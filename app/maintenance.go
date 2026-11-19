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
// the jobs it ran. A job whose lease another worker holds is skipped without error and is
// not in the list. A job that fails is reported in the error, joined with the others'
// and does not stop them.
func (m *Maintenance) Pass(ctx context.Context) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, _, _ = m.wal, m.lastRun, partitionsAhead // the jobs' state, until they are built
	return nil, errors.New("not implemented")
}

// Run makes a pass at once, so a fresh deployment has its partitions and its preflight
// before it takes traffic, and then one every tick, until ctx ends. It returns nil: a
// fault is reported to the error handler and the loop goes on, and a warning never
// stops a service starting. Run is one loop; more workers are more loops, and the leases
// keep them from running the same job.
func (m *Maintenance) Run(ctx context.Context) error {
	return errors.New("not implemented")
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
	return nil, errors.New("not implemented")
}
