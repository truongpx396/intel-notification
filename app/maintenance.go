package app

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	notify "github.com/truongpx396/intel-notification"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// The notify_job_leases rows the maintenance jobs take (D22). Digest flush is
// JobDigestFlush, which the DigestFlusher takes, and not one of these.
const (
	JobRetention        = "retention"
	JobPartitions       = "partition_provision"
	JobIdemExpiry       = "idem_expiry"
	JobDigestExpiry     = "digest_expiry"
	JobBroadcastCleanup = "broadcast_cleanup"
	JobPreflight        = "preflight"
)

// Maintenance runs the single-owner jobs that keep the engine's storage bounded and tell
// an operator when its database is not as it should be (NR-022, NR-037, D33, D38):
//
//   - retention retires the partitions of the inbox, the delivery history and the dead
//     letters that have aged past their windows;
//   - partition provisioning creates the months ahead, and then checks the catalog and
//     raises notify.partition.missing and notify.partition.unscoped;
//   - idempotency expiry, digest expiry and broadcast cleanup delete what is past the
//     idempotency window, in batches;
//   - the preflight reads the database's conditions and reports the ones the queue
//     depends on as warnings.
//
// Each job is single-owner (D22). It is run only while holding its own lease in
// notify_job_leases, taken or renewed first, and before every batch of a job that works
// in batches, so any number of workers may run a Maintenance and each job has one runner.
// What a worker that died leaves behind is a lease that lapses, after which another
// worker's pass takes the job over. A job keeps no memory of what it meant to do: each
// run derives its cutoffs from the clock, so a job that did not run for a day is late and
// not wrong, and a re-run changes nothing the run before it did not already settle.
type Maintenance struct {
	store   ports.Maintenance
	clock   ports.Clock
	metrics ports.Metrics

	cfg     notify.Config // with defaults applied
	owner   string
	tick    time.Duration
	batch   int
	every   map[string]time.Duration
	limits  domain.PreflightLimits
	onError func(error)
	onWarn  func(domain.PreflightWarning)

	mu      sync.Mutex // guards lastRun and wal
	lastRun map[string]time.Time
	wal     domain.WALRate
}

var _ ports.Jobs = (*Maintenance)(nil)

// The defaults of the schedule. Retention and provisioning work in months, so an hour is
// finer than they need; the expiries delete a steady trickle and are cheap to run often;
// the preflight watches for a transaction that is pinning vacuum now, and a minute is
// finer than the alarm it feeds.
var defaultJobIntervals = map[string]time.Duration{
	JobRetention:        time.Hour,
	JobPartitions:       time.Hour,
	JobIdemExpiry:       5 * time.Minute,
	JobDigestExpiry:     5 * time.Minute,
	JobBroadcastCleanup: time.Hour,
	JobPreflight:        time.Minute,
}

const (
	defaultMaintenanceTick  = 30 * time.Second
	defaultMaintenanceBatch = 1000
	minJobLease             = 30 * time.Second

	// partitionsAhead is how many months, counting this one, provisioning keeps. The
	// check wants this month and the next, so the rest is slack for a job that lapses.
	partitionsAhead = 3
)

// MaintenanceOption changes how a Maintenance is built.
type MaintenanceOption func(*Maintenance)

// WithMaintenanceOwner sets the name the jobs' leases are held under, which
// notify_job_leases shows an operator. The default names the host and process and adds a
// random suffix, so two workers never share one: a shared name would let each renew the
// other's lease, and a job would run twice.
func WithMaintenanceOwner(owner string) MaintenanceOption {
	return func(m *Maintenance) { m.owner = owner }
}

// WithMaintenanceTick sets how often Run looks for a job that is due. The default is 30
// seconds. A job runs no more often than its own interval, so this is the resolution of
// the schedule and not a rate.
func WithMaintenanceTick(d time.Duration) MaintenanceOption {
	return func(m *Maintenance) { m.tick = d }
}

// WithMaintenanceBatch sets how many rows a batch of an expiry job deletes. The default
// is 1000.
func WithMaintenanceBatch(n int) MaintenanceOption {
	return func(m *Maintenance) { m.batch = n }
}

// WithJobInterval sets how often a job runs, for one of the Job constants. The defaults
// are an hour for retention, provisioning and broadcast cleanup, five minutes for the
// two expiries and a minute for the preflight.
func WithJobInterval(job string, d time.Duration) MaintenanceOption {
	return func(m *Maintenance) { m.every[job] = d }
}

// WithMaintenanceErrorHandler sets where a fault a pass survives is reported. The default
// discards them.
func WithMaintenanceErrorHandler(h func(error)) MaintenanceOption {
	return func(m *Maintenance) { m.onError = h }
}

// WithPreflightWarningHandler sets where the preflight's warnings are reported, each as
// it is found. The default discards them; Preflight returns them as well.
func WithPreflightWarningHandler(h func(domain.PreflightWarning)) MaintenanceOption {
	return func(m *Maintenance) { m.onWarn = h }
}

// WithPreflightLimits sets the thresholds the preflight judges by, for a deployment whose
// write rate makes the defaults wrong (domain.DefaultPreflightLimits).
func WithPreflightLimits(l domain.PreflightLimits) MaintenanceOption {
	return func(m *Maintenance) { m.limits = l }
}

// NewMaintenance returns a Maintenance for cfg over d. It refuses a configuration that
// does not validate, names every port it needs and d leaves nil, and refuses an option
// that could not work. Deps.Clock and Deps.Metrics are optional.
func NewMaintenance(cfg notify.Config, d Deps, opts ...MaintenanceOption) (*Maintenance, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	var g gaps
	g.need("Maintenance", d.Maintenance == nil)
	if err := g.err("Maintenance"); err != nil {
		return nil, err
	}
	m := &Maintenance{
		store: d.Maintenance, clock: d.clockOf(), metrics: d.Metrics,
		cfg: cfg.WithDefaults(), owner: defaultMaintenanceOwner(),
		tick: defaultMaintenanceTick, batch: defaultMaintenanceBatch,
		every: map[string]time.Duration{}, limits: domain.DefaultPreflightLimits(),
		onError: func(error) {}, onWarn: func(domain.PreflightWarning) {},
		lastRun: map[string]time.Time{},
	}
	for job, every := range defaultJobIntervals {
		m.every[job] = every
	}
	for _, opt := range opts {
		opt(m)
	}
	switch {
	case m.tick <= 0:
		return nil, fmt.Errorf("app: Maintenance tick %v is not positive", m.tick)
	case m.batch < 1:
		return nil, fmt.Errorf("app: Maintenance batch %d is not at least one row", m.batch)
	case m.owner == "":
		return nil, errors.New("app: Maintenance owner has no name, and the jobs' leases are held under one")
	case m.limits.OldestTransaction <= 0 || m.limits.WALHeadroom <= 0:
		return nil, fmt.Errorf("app: Maintenance preflight limits %+v must both be positive, or they judge nothing", m.limits)
	}
	for job, every := range m.every {
		if _, known := defaultJobIntervals[job]; !known {
			return nil, fmt.Errorf("app: Maintenance has no job %q", job)
		}
		if every <= 0 {
			return nil, fmt.Errorf("app: Maintenance interval %v for the %s job is not positive", every, job)
		}
	}
	if m.onError == nil {
		m.onError = func(error) {}
	}
	if m.onWarn == nil {
		m.onWarn = func(domain.PreflightWarning) {}
	}
	return m, nil
}

func defaultMaintenanceOwner() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	var suffix [4]byte
	_, _ = rand.Read(suffix[:])
	return fmt.Sprintf("%s/%d/%s", host, os.Getpid(), hex.EncodeToString(suffix[:]))
}

// Pass runs, once, every job that is due, each under its lease, and returns the names of
// the jobs it ran, in the order it ran them. A job whose lease another worker holds is
// skipped without error and is not in the list. A job that fails is reported in the error,
// joined with the others' and does not stop them, and still counts as having run: it is
// tried again at its next interval, and not at every tick until it works.
func (m *Maintenance) Pass(ctx context.Context) ([]string, error) {
	var (
		ran  []string
		errs []error
	)
	for _, j := range m.jobs() {
		if ctx.Err() != nil {
			break
		}
		now := m.clock.Now()
		if !m.due(j.name, now) {
			continue
		}
		held := false
		lease := func() (bool, error) {
			ok, err := m.store.TryLeaseJob(ctx, j.name, m.owner, m.leaseTTL(j.name))
			if err != nil {
				return false, fmt.Errorf("lease the %s job: %w", j.name, err)
			}
			held = held || ok
			return ok, nil
		}
		err := j.run(ctx, now, lease)
		if held {
			m.mu.Lock()
			m.lastRun[j.name] = now
			m.mu.Unlock()
			ran = append(ran, j.name)
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", j.name, err))
		}
	}
	return ran, errors.Join(errs...)
}

// maintJob is one job. lease takes or renews its lease and says whether the worker holds
// it: a job asks before it works, and a job that works in batches asks before each.
type maintJob struct {
	name string
	run  func(ctx context.Context, now time.Time, lease func() (bool, error)) error
}

// jobs lists the jobs in the order a pass runs them. Provisioning is first, because a
// fresh deployment cannot accept a notification until the month exists.
func (m *Maintenance) jobs() []maintJob {
	return []maintJob{
		{JobPartitions, m.partitions},
		{JobRetention, m.retention},
		{JobIdemExpiry, m.expiry(m.store.ExpireIdem)},
		// A flushed window is read by Status as long as the key it was accepted under is
		// remembered, so it is kept for the same window.
		{JobDigestExpiry, m.expiry(m.store.ExpireDigests)},
		{JobBroadcastCleanup, m.expiry(m.store.ExpireBroadcasts)},
		{JobPreflight, func(ctx context.Context, _ time.Time, lease func() (bool, error)) error {
			if ok, err := lease(); err != nil || !ok {
				return err
			}
			_, err := m.Preflight(ctx)
			return err
		}},
	}
}

// due is whether the job's interval has passed since this worker last ran it. A lease the
// worker was refused is not a run, so a job that has never run is always due.
func (m *Maintenance) due(job string, now time.Time) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	last, ran := m.lastRun[job]
	return !ran || now.Sub(last) >= m.every[job]
}

// leaseTTL outlives the gap between a job's runs. The holder takes the lease again at each
// run, and until then it is what keeps every other worker out: a lease that lapsed
// between runs would let a second worker in, and the job would run once per worker per
// interval. The price is that a worker that dies leaves its jobs unrun for up to twice
// their interval, which is harmless for work measured in months and minutes.
func (m *Maintenance) leaseTTL(job string) time.Duration { return max(minJobLease, 2*m.every[job]) }

// retention retires the partitions that have aged past each table's window, counted back
// from now. A partition goes only when its whole range is older than the window, so a
// notification outlives RetentionWindow by up to a month, read or unread (D33). A table
// that fails does not stop the others.
func (m *Maintenance) retention(ctx context.Context, now time.Time, lease func() (bool, error)) error {
	if ok, err := lease(); err != nil || !ok {
		return err
	}
	var errs []error
	for _, t := range []struct {
		table  string
		window time.Duration
	}{
		{domain.TableNotifications, m.cfg.RetentionWindow},
		{domain.TableDeliveries, m.cfg.DeliveryLogRetention},
		{domain.TableDeadLetters, m.cfg.DeadLetterRetention},
	} {
		if _, err := m.store.RetirePartitions(ctx, t.table, now.Add(-t.window)); err != nil {
			errs = append(errs, fmt.Errorf("retire %s: %w", t.table, err))
		}
	}
	return errors.Join(errs...)
}

// partitions creates the months ahead and then checks the catalog, so what it reports is
// what is there once provisioning has done what it could. A provisioning that failed (a
// role that cannot create tables) does not skip the check: that is when a month is
// missing and the alarm is wanted.
func (m *Maintenance) partitions(ctx context.Context, now time.Time, lease func() (bool, error)) error {
	if ok, err := lease(); err != nil || !ok {
		return err
	}
	var errs []error
	if _, err := m.store.EnsurePartitions(ctx, now, partitionsAhead); err != nil {
		errs = append(errs, fmt.Errorf("provision partitions: %w", err))
	}
	health, err := m.store.CheckPartitions(ctx, now)
	if err != nil {
		return errors.Join(append(errs, fmt.Errorf("check partitions: %w", err))...)
	}
	for _, gap := range health.Missing {
		m.inc(domain.MetricPartitionMissing, map[string]string{
			domain.LabelTable: gap.Table, domain.LabelMonth: gap.Month.UTC().Format("2006-01"),
		})
	}
	for _, name := range health.Unscoped {
		m.inc(domain.MetricPartitionUnscoped, map[string]string{domain.LabelPartition: name})
	}
	return errors.Join(errs...)
}

// expiry is the job that deletes, through del, what is older than the idempotency window.
func (m *Maintenance) expiry(del func(ctx context.Context, olderThan time.Time, batch int) (int, error)) func(context.Context, time.Time, func() (bool, error)) error {
	return func(ctx context.Context, now time.Time, lease func() (bool, error)) error {
		return m.expire(ctx, lease, del, now.Add(-m.cfg.IdempotencyWindow))
	}
}

// expire deletes in batches until one comes back short, taking the lease again before each:
// a job that takes long keeps it, and one that loses it stops where it is, because the
// worker that holds it now owns the rest. A fault ends this job's loop, not the pass.
func (m *Maintenance) expire(ctx context.Context, lease func() (bool, error),
	del func(ctx context.Context, olderThan time.Time, batch int) (int, error), olderThan time.Time,
) error {
	for ctx.Err() == nil {
		if ok, err := lease(); err != nil || !ok {
			return err
		}
		n, err := del(ctx, olderThan, m.batch)
		if err != nil {
			return err
		}
		if n < m.batch {
			return nil
		}
	}
	return nil
}

// inc counts an alarm, when there is anything to count it.
func (m *Maintenance) inc(name string, labels map[string]string) {
	if m.metrics != nil {
		m.metrics.Inc(name, labels)
	}
}

// Run makes a pass at once, so a fresh deployment has its partitions and its preflight
// before it takes traffic, and then one every tick, until ctx ends. It returns nil: a
// fault is reported to the error handler and the loop goes on, and a warning never
// stops a service starting. Run is one loop; more workers are more loops, and the leases
// keep them from running the same job.
func (m *Maintenance) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		if _, err := m.Pass(ctx); err != nil && ctx.Err() == nil {
			m.onError(err)
		}
		timer := time.NewTimer(m.tick)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
	return nil
}

// Preflight reads the database's conditions and reports, as warnings and never as
// failures, those the queue depends on that are not as they should be: the role's
// timeouts, a transaction old enough to pin vacuum, and max_wal_size against the peak WAL
// rate seen so far (NR-037, D38). Each warning goes to the warning handler as well as the
// return. It also sets the gauges domain.MetricDBOldestXminAge and
// domain.MetricDBCheckpointsRequested, when the Metrics can hold them. An error means the
// conditions could not be read, which is a fault and not a warning, and the caller
// carries on: a database that cannot be asked is not a reason to stop.
func (m *Maintenance) Preflight(ctx context.Context) ([]domain.PreflightWarning, error) {
	c, err := m.store.DatabaseConditions(ctx)
	if err != nil {
		return nil, fmt.Errorf("read the database's conditions: %w", err)
	}
	m.mu.Lock()
	rate := m.wal.Observe(c)
	m.mu.Unlock()

	if g, ok := m.metrics.(ports.GaugeMetrics); ok {
		g.Gauge(domain.MetricDBOldestXminAge, c.OldestTransactionAge.Seconds(), nil)
		g.Gauge(domain.MetricDBCheckpointsRequested, float64(c.CheckpointsRequested), nil)
	}
	warnings := domain.AssessDatabase(c, rate, m.limits)
	for _, w := range warnings {
		m.onWarn(w)
	}
	return warnings, nil
}
