package app_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	notify "github.com/truongpx396/intel-notification"
	"github.com/truongpx396/intel-notification/app"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/notifytest"
	"github.com/truongpx396/intel-notification/ports"
)

// T038, T040a: the maintenance jobs and the preflight, with no database. The Maintenance
// behind them is a recording stand-in that keeps real lease semantics over a clock, so
// "two workers contending run a job once" is something the test shows and not something it
// scripts. What each operation does to a table is the PostgreSQL suite's
// (TestRetirePartitions, TestExpireIdem, TestDatabaseConditions, ...), and the jobs
// end to end are in maintenance_integration_test.go.

var maintEpoch = time.Date(2031, 3, 1, 12, 0, 0, 0, time.UTC)

// The jobs in the order a pass runs them: provisioning first, because a fresh deployment
// cannot accept a notification until the month exists.
var maintJobs = []string{
	app.JobPartitions, app.JobRetention, app.JobIdemExpiry, app.JobDigestExpiry,
	app.JobBroadcastCleanup, app.JobPreflight,
}

// The operations each job performs on the Maintenance port.
var maintOps = map[string][]string{
	app.JobPartitions:       {"ensure", "check"},
	app.JobRetention:        {"retire"},
	app.JobIdemExpiry:       {"expire_idem"},
	app.JobDigestExpiry:     {"expire_digests"},
	app.JobBroadcastCleanup: {"expire_broadcasts"},
	app.JobPreflight:        {"conditions"},
}

type maintCall struct {
	op    string // "lease", "retire", "ensure", "check", "expire_idem", "expire_digests", "expire_broadcasts", "conditions"
	when  time.Time
	job   string        // lease: the job asked for
	owner string        // lease
	ttl   time.Duration // lease
	table string        // retire
	at    time.Time     // retire: the cutoff; expire: olderThan; ensure, check: the instant
	n     int           // expire: the batch; ensure: the months
}

type maintLease struct {
	owner   string
	expires time.Time
}

type maintStore struct {
	ports.Maintenance
	now func() time.Time

	mu        sync.Mutex
	calls     []maintCall
	leases    map[string]maintLease
	denied    map[string]bool // jobs whose lease is refused to everyone
	grants    map[string]int
	loseAfter map[string]int // a job's lease is refused once it has been granted this many times

	retireErr map[string]error // by table
	ensureErr error
	health    domain.PartitionHealth
	expire    map[string][]int // op → rows deleted by each call, in order; once spent, 0
	expireErr map[string]int   // op → the call (1-based) that fails
	expired   map[string]int

	conditions []domain.DatabaseConditions // read in order, the last one again and again
	reads      int
	condErr    error
}

func newMaintStore(now func() time.Time) *maintStore {
	return &maintStore{
		now: now, leases: map[string]maintLease{}, denied: map[string]bool{}, grants: map[string]int{},
		loseAfter: map[string]int{}, retireErr: map[string]error{}, expire: map[string][]int{},
		expireErr: map[string]int{}, expired: map[string]int{},
	}
}

func (s *maintStore) record(c maintCall) {
	c.when = time.Now()
	s.calls = append(s.calls, c)
}

func (s *maintStore) TryLeaseJob(_ context.Context, job, owner string, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.record(maintCall{op: "lease", job: job, owner: owner, ttl: ttl})
	if s.denied[job] {
		return false, nil
	}
	if n, ok := s.loseAfter[job]; ok && s.grants[job] >= n {
		return false, nil
	}
	if l, ok := s.leases[job]; ok && l.owner != owner && l.expires.After(s.now()) {
		return false, nil
	}
	s.leases[job] = maintLease{owner: owner, expires: s.now().Add(ttl)}
	s.grants[job]++
	return true, nil
}

func (s *maintStore) RetirePartitions(_ context.Context, table string, before time.Time) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.record(maintCall{op: "retire", table: table, at: before})
	return nil, s.retireErr[table]
}

func (s *maintStore) EnsurePartitions(_ context.Context, from time.Time, months int) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.record(maintCall{op: "ensure", at: from, n: months})
	return nil, s.ensureErr
}

func (s *maintStore) CheckPartitions(_ context.Context, at time.Time) (domain.PartitionHealth, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.record(maintCall{op: "check", at: at})
	return s.health, nil
}

func (s *maintStore) expireOp(op string, olderThan time.Time, batch int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.record(maintCall{op: op, at: olderThan, n: batch})
	s.expired[op]++
	if s.expireErr[op] == s.expired[op] {
		return 0, fmt.Errorf("%s: the database is away", op)
	}
	if counts := s.expire[op]; s.expired[op] <= len(counts) {
		return counts[s.expired[op]-1], nil
	}
	return 0, nil
}

func (s *maintStore) ExpireIdem(_ context.Context, olderThan time.Time, batch int) (int, error) {
	return s.expireOp("expire_idem", olderThan, batch)
}

func (s *maintStore) ExpireDigests(_ context.Context, olderThan time.Time, batch int) (int, error) {
	return s.expireOp("expire_digests", olderThan, batch)
}

func (s *maintStore) ExpireBroadcasts(_ context.Context, olderThan time.Time, batch int) (int, error) {
	return s.expireOp("expire_broadcasts", olderThan, batch)
}

func (s *maintStore) DatabaseConditions(context.Context) (domain.DatabaseConditions, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.record(maintCall{op: "conditions"})
	if s.condErr != nil {
		return domain.DatabaseConditions{}, s.condErr
	}
	s.reads++
	if len(s.conditions) == 0 {
		return maintHealthy(), nil
	}
	return s.conditions[min(s.reads, len(s.conditions))-1], nil
}

func (s *maintStore) log() []maintCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.calls)
}

func (s *maintStore) ops(op string) []maintCall {
	var out []maintCall
	for _, c := range s.log() {
		if c.op == op {
			out = append(out, c)
		}
	}
	return out
}

func (s *maintStore) leaseAsks(job string) []maintCall {
	var out []maintCall
	for _, c := range s.ops("lease") {
		if c.job == job {
			out = append(out, c)
		}
	}
	return out
}

func (s *maintStore) setDenied(job string, denied bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.denied[job] = denied
}

// maintHealthy is a reading with nothing to report.
func maintHealthy() domain.DatabaseConditions {
	return domain.DatabaseConditions{
		At:                       maintEpoch,
		MaxWALSize:               16 << 30,
		IdleInTransactionTimeout: 30 * time.Second,
		StatementTimeout:         time.Minute,
		SeesAllSessions:          true,
	}
}

type warnLog struct {
	mu   sync.Mutex
	list []domain.PreflightWarning
}

func (l *warnLog) add(w domain.PreflightWarning) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.list = append(l.list, w)
}

func (l *warnLog) codes() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []string
	for _, w := range l.list {
		out = append(out, w.Code)
	}
	return out
}

type maintFx struct {
	cfg     notify.Config
	clock   *notifytest.Clock
	store   *maintStore
	metrics *notifytest.Metrics
	errs    *errLog
	warns   *warnLog
}

func newMaintFx() *maintFx {
	clock := notifytest.NewClock(maintEpoch)
	return &maintFx{
		cfg:     notify.Config{Realm: planRealm, StoreDSN: "x", RedisURL: "y"},
		clock:   clock,
		store:   newMaintStore(clock.Now),
		metrics: notifytest.NewMetrics(),
		errs:    &errLog{},
		warns:   &warnLog{},
	}
}

func (f *maintFx) build(t *testing.T, owner string, opts ...app.MaintenanceOption) *app.Maintenance {
	t.Helper()
	opts = append([]app.MaintenanceOption{
		app.WithMaintenanceOwner(owner), app.WithMaintenanceErrorHandler(f.errs.add),
		app.WithPreflightWarningHandler(f.warns.add),
	}, opts...)
	m, err := app.NewMaintenance(f.cfg, app.Deps{Maintenance: f.store, Clock: f.clock, Metrics: f.metrics}, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func (f *maintFx) pass(t *testing.T, m *app.Maintenance) []string {
	t.Helper()
	ran, err := m.Pass(t.Context())
	if err != nil {
		t.Fatalf("Pass: %v", err)
	}
	return ran
}

func indexOf(log []maintCall, match func(maintCall) bool) int {
	return slices.IndexFunc(log, match)
}

// ------------------------------------------------------------ the lease --

// Each job asks for its own lease, under the worker's own name and before it does any
// of its work (D22): the lease is what makes a job single-owner, and a job that worked
// first would have done its work whether or not it was the owner.
func TestEveryJobTakesItsLeaseBeforeItWorks(t *testing.T) {
	t.Parallel()
	f := newMaintFx()
	if ran := f.pass(t, f.build(t, "me")); !slices.Equal(ran, maintJobs) {
		t.Fatalf("Pass ran %v, want every job, in order: %v", ran, maintJobs)
	}
	log := f.store.log()
	for job, ops := range maintOps {
		lease := indexOf(log, func(c maintCall) bool { return c.op == "lease" && c.job == job && c.owner == "me" })
		if lease < 0 {
			t.Errorf("%s never asked for its lease under the worker's name", job)
			continue
		}
		for _, op := range ops {
			if at := indexOf(log, func(c maintCall) bool { return c.op == op }); at < lease {
				t.Errorf("%s: %s ran at call %d, before the lease was asked for at %d", job, op, at, lease)
			}
		}
	}
}

// A job whose lease another worker holds does not run, does not appear to have run, and
// does not stop the jobs it does not share.
func TestAJobWhoseLeaseIsHeldElsewhereDoesNotRun(t *testing.T) {
	t.Parallel()
	for _, held := range maintJobs {
		t.Run(held, func(t *testing.T) {
			t.Parallel()
			f := newMaintFx()
			f.store.setDenied(held, true)

			ran, err := f.build(t, "me").Pass(t.Context())
			if err != nil {
				t.Fatalf("Pass = %v, want no error: another worker holding a lease is not a fault", err)
			}
			want := slices.DeleteFunc(slices.Clone(maintJobs), func(j string) bool { return j == held })
			if !slices.Equal(ran, want) {
				t.Fatalf("Pass ran %v, want %v", ran, want)
			}
			for _, op := range maintOps[held] {
				if n := len(f.store.ops(op)); n != 0 {
					t.Errorf("%s ran %d times without its lease", op, n)
				}
			}
			for job, ops := range maintOps {
				if job == held {
					continue
				}
				for _, op := range ops {
					if len(f.store.ops(op)) == 0 {
						t.Errorf("%s never ran, though only %s was denied", op, held)
					}
				}
			}
		})
	}
}

// The lease outlives the gap between a job's runs. The holder takes it again at each run,
// and until then it is what keeps every other worker out: one that lapsed between runs
// would let a second worker in, and the job would run once per worker per interval.
func TestALeaseOutlivesTheGapBetweenRuns(t *testing.T) {
	t.Parallel()
	f := newMaintFx()
	var opts []app.MaintenanceOption
	for _, job := range maintJobs {
		opts = append(opts, app.WithJobInterval(job, 10*time.Minute))
	}
	f.pass(t, f.build(t, "me", opts...))
	for _, job := range maintJobs {
		asks := f.store.leaseAsks(job)
		if len(asks) == 0 || asks[0].ttl < 10*time.Minute {
			t.Errorf("%s: lease asks %+v, want a ttl of at least the job's 10 minute interval", job, asks)
		}
	}
}

// Two workers contending at once run each job once between them, and when the holder's
// lease lapses, the other takes the jobs over.
func TestTwoWorkersContendingForTheJobsRunEachOnce(t *testing.T) {
	t.Parallel()
	f := newMaintFx()
	a, b := f.build(t, "worker-a"), f.build(t, "worker-b")

	ran := make([][]string, 2)
	var wg sync.WaitGroup
	for i, m := range []*app.Maintenance{a, b} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ran[i], _ = m.Pass(t.Context())
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("two workers did not finish a pass within 10 seconds")
	}

	counts := map[string]int{}
	for _, r := range ran {
		for _, job := range r {
			counts[job]++
		}
	}
	for _, job := range maintJobs {
		if counts[job] != 1 {
			t.Errorf("%s ran %d times across two workers, want once (a ran %v, b ran %v)", job, counts[job], ran[0], ran[1])
		}
	}
	for op, want := range map[string]int{
		"retire": 3, "ensure": 1, "check": 1, "expire_idem": 1, "expire_digests": 1, "expire_broadcasts": 1, "conditions": 1,
	} {
		if got := len(f.store.ops(op)); got != want {
			t.Errorf("%s was called %d times, want %d", op, got, want)
		}
	}
}

func TestALapsedLeasePassesTheJobsToAnotherWorker(t *testing.T) {
	t.Parallel()
	f := newMaintFx()
	a, b := f.build(t, "worker-a"), f.build(t, "worker-b")

	if ran := f.pass(t, a); len(ran) != len(maintJobs) {
		t.Fatalf("the first worker ran %v, want every job", ran)
	}
	if ran := f.pass(t, b); len(ran) != 0 {
		t.Fatalf("the second worker ran %v while the first held every lease", ran)
	}
	f.clock.Advance(3 * time.Hour) // longer than any lease
	if ran := f.pass(t, b); !slices.Equal(ran, maintJobs) {
		t.Fatalf("the second worker ran %v after the first's leases lapsed, want every job", ran)
	}
	if ran := f.pass(t, a); len(ran) != 0 {
		t.Fatalf("the first worker ran %v after the second took the leases", ran)
	}
}

// ------------------------------------------------------------- the cadence --

// A job runs no oftener than its interval, whatever the tick. A lease that was refused
// is not a run: the worker asks again at the next pass and does not wait out an interval
// for work it never did.
func TestAJobRunsNoOftenerThanItsInterval(t *testing.T) {
	t.Parallel()
	f := newMaintFx()
	m := f.build(t, "me")
	steps := []struct {
		advance time.Duration
		want    []string
	}{
		{0, maintJobs},
		{30 * time.Second, nil},
		{30 * time.Second, []string{app.JobPreflight}},                                         // a minute
		{4 * time.Minute, []string{app.JobIdemExpiry, app.JobDigestExpiry, app.JobPreflight}},  // five
		{4 * time.Minute, []string{app.JobPreflight}},                                          // nine
		{51 * time.Minute, maintJobs},                                                          // an hour
		{59 * time.Minute, []string{app.JobIdemExpiry, app.JobDigestExpiry, app.JobPreflight}}, // not yet two
		{time.Minute, maintJobs},                                                               // two hours
	}
	for _, st := range steps {
		f.clock.Advance(st.advance)
		if ran := f.pass(t, m); !slices.Equal(ran, st.want) {
			t.Fatalf("at %s the pass ran %v, want %v", f.clock.Now().Sub(maintEpoch), ran, st.want)
		}
	}
}

func TestARefusedLeaseIsNotARun(t *testing.T) {
	t.Parallel()
	f := newMaintFx()
	m := f.build(t, "me")
	f.store.setDenied(app.JobRetention, true)
	f.pass(t, m)
	f.store.setDenied(app.JobRetention, false)
	f.clock.Advance(30 * time.Second)
	if ran := f.pass(t, m); !slices.Equal(ran, []string{app.JobRetention}) {
		t.Fatalf("the next pass ran %v, want retention, which never ran: an hour has not passed, but it has not run either", ran)
	}
}

// ---------------------------------------------------------------- retention --

// Retention retires each table at its own window, the inbox by RetentionWindow, the
// delivery history by DeliveryLogRetention and the dead letters by DeadLetterRetention,
// every one counted back from now (D33).
func TestRetentionRetiresEachTableAtItsOwnWindow(t *testing.T) {
	t.Parallel()
	day := 24 * time.Hour
	cases := []struct {
		name string
		cfg  notify.Config
		want map[string]time.Duration
	}{
		{"configured windows",
			notify.Config{RetentionWindow: 100 * day, DeliveryLogRetention: 40 * day, DeadLetterRetention: 20 * day},
			map[string]time.Duration{domain.TableNotifications: 100 * day, domain.TableDeliveries: 40 * day, domain.TableDeadLetters: 20 * day}},
		{"the defaults", notify.Config{},
			map[string]time.Duration{domain.TableNotifications: 90 * day, domain.TableDeliveries: 30 * day, domain.TableDeadLetters: 30 * day}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newMaintFx()
			tc.cfg.Realm, tc.cfg.StoreDSN, tc.cfg.RedisURL = f.cfg.Realm, "x", "y"
			f.cfg = tc.cfg
			f.pass(t, f.build(t, "me"))

			got := map[string]time.Time{}
			var order []string
			for _, c := range f.store.ops("retire") {
				got[c.table] = c.at
				order = append(order, c.table)
			}
			for table, window := range tc.want {
				if want := maintEpoch.Add(-window); !got[table].Equal(want) {
					t.Errorf("%s retired before %v, want %v (now less %s)", table, got[table], want, window)
				}
			}
			wantOrder := []string{domain.TableNotifications, domain.TableDeliveries, domain.TableDeadLetters}
			if !slices.Equal(order, wantOrder) {
				t.Errorf("retired %v, want %v", order, wantOrder)
			}
		})
	}
}

// A failure on one table does not leave the others unretired, and is reported.
func TestARetirementThatFailsDoesNotStopTheOtherTables(t *testing.T) {
	t.Parallel()
	f := newMaintFx()
	boom := errors.New("lock timeout on the inbox partition")
	f.store.retireErr[domain.TableNotifications] = boom

	ran, err := f.build(t, "me").Pass(t.Context())
	if !errors.Is(err, boom) {
		t.Fatalf("Pass = %v, want the fault returned", err)
	}
	if !slices.Equal(ran, maintJobs) {
		t.Fatalf("Pass ran %v, want every job: a job that failed ran, and the others go on", ran)
	}
	if n := len(f.store.ops("retire")); n != 3 {
		t.Fatalf("%d tables retired, want all 3 attempted", n)
	}
	if n := len(f.store.ops("expire_idem")); n != 1 {
		t.Fatalf("the job after retention ran %d times, want 1", n)
	}
}

// -------------------------------------------------------------- the expiries --

// The three expiries cut at the idempotency window, in batches of the size configured,
// and a pass keeps taking batches until one comes back short.
func TestTheExpiriesCutAtTheIdempotencyWindowInBatches(t *testing.T) {
	t.Parallel()
	for _, op := range []string{"expire_idem", "expire_digests", "expire_broadcasts"} {
		t.Run(op, func(t *testing.T) {
			t.Parallel()
			f := newMaintFx()
			f.cfg.IdempotencyWindow = 10 * 24 * time.Hour
			f.cfg.RetentionWindow = 400 * 24 * time.Hour // so no other window is 10 days
			f.store.expire[op] = []int{50, 50, 7}
			f.pass(t, f.build(t, "me", app.WithMaintenanceBatch(50)))

			calls := f.store.ops(op)
			if len(calls) != 3 {
				t.Fatalf("%d batches, want 3: two full and a short one", len(calls))
			}
			for i, c := range calls {
				if want := maintEpoch.Add(-10 * 24 * time.Hour); !c.at.Equal(want) {
					t.Errorf("batch %d expired before %v, want the idempotency window's %v", i+1, c.at, want)
				}
				if c.n != 50 {
					t.Errorf("batch %d asked for %d rows, want the configured 50", i+1, c.n)
				}
			}
		})
	}
}

// The lease is taken again before each batch, so a job that takes long keeps it, and one
// that loses it stops where it is: the worker that holds it now owns the rest.
func TestAnExpiryRenewsItsLeaseBeforeEachBatchAndStopsWhenItLosesIt(t *testing.T) {
	t.Parallel()
	f := newMaintFx()
	f.store.expire["expire_idem"] = []int{10, 10, 10, 10}
	f.store.loseAfter[app.JobIdemExpiry] = 2

	ran, err := f.build(t, "me", app.WithMaintenanceBatch(10)).Pass(t.Context())
	if err != nil {
		t.Fatalf("Pass = %v, want no error: a lost lease is not a fault", err)
	}
	if !slices.Contains(ran, app.JobIdemExpiry) {
		t.Fatalf("Pass ran %v, want it to include the expiry that began", ran)
	}
	if n := len(f.store.ops("expire_idem")); n != 2 {
		t.Fatalf("%d batches, want 2: the lease was lost before the third", n)
	}
	if n := len(f.store.leaseAsks(app.JobIdemExpiry)); n != 3 {
		t.Fatalf("%d lease asks, want 3: one before each batch, the third refused", n)
	}
}

// A fault in a batch ends that job's loop, is reported, and does not stop the other jobs.
func TestAnExpiryThatFailsEndsItsLoopAndNotThePass(t *testing.T) {
	t.Parallel()
	f := newMaintFx()
	f.store.expire["expire_idem"] = []int{10, 10, 10}
	f.store.expireErr["expire_idem"] = 2

	ran, err := f.build(t, "me", app.WithMaintenanceBatch(10)).Pass(t.Context())
	if err == nil || !strings.Contains(err.Error(), "expire_idem") {
		t.Fatalf("Pass = %v, want the batch's fault returned", err)
	}
	if n := len(f.store.ops("expire_idem")); n != 2 {
		t.Fatalf("%d batches, want 2: the fault ends the loop, and it is not tried again in the pass", n)
	}
	if !slices.Equal(ran, maintJobs) || len(f.store.ops("expire_digests")) != 1 {
		t.Fatalf("Pass ran %v and the digest expiry %d times, want the other jobs to go on", ran, len(f.store.ops("expire_digests")))
	}
}

// ------------------------------------------------------------- partitions --

// Provisioning creates the months ahead and the check reads the catalog after it, so what
// it reports is what is there once provisioning has done what it could.
func TestTheCheckFollowsTheProvisioning(t *testing.T) {
	t.Parallel()
	f := newMaintFx()
	f.pass(t, f.build(t, "me"))

	log := f.store.log()
	ensure := indexOf(log, func(c maintCall) bool { return c.op == "ensure" })
	check := indexOf(log, func(c maintCall) bool { return c.op == "check" })
	if ensure < 0 || check < 0 || ensure > check {
		t.Fatalf("ensure at %d, check at %d, want both, in that order", ensure, check)
	}
	if c := log[ensure]; !c.at.Equal(maintEpoch) || c.n < 2 {
		t.Fatalf("ensure(%v, %d months), want from now, and at least this month and the next", c.at, c.n)
	}
	if c := log[check]; !c.at.Equal(maintEpoch) {
		t.Fatalf("check at %v, want now", c.at)
	}
}

// A report becomes the two alarms and nothing else does: one notify.partition.missing for
// each gap, naming its table and its month, one notify.partition.unscoped for each
// partition, naming it, and neither for a healthy report. Detecting the conditions is the
// catalog reader's (TestCheckPartitions); this is that the job turns what it reads
// into what an operator is paged on.
func TestThePartitionJobTurnsAReportIntoItsAlarms(t *testing.T) {
	t.Parallel()
	mar := time.Date(2031, 3, 1, 0, 0, 0, 0, time.UTC)
	apr := time.Date(2031, 4, 1, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		name     string
		health   domain.PartitionHealth
		missing  []map[string]string
		unscoped []map[string]string
	}{
		{name: "a healthy report raises neither"},
		{name: "a missing partition names its table and month",
			health:  domain.PartitionHealth{Missing: []domain.PartitionGap{{Table: domain.TableDeadLetters, Month: apr}}},
			missing: []map[string]string{{domain.LabelTable: "dead_letters", domain.LabelMonth: "2031-04"}}},
		{name: "each gap is its own alarm",
			health: domain.PartitionHealth{Missing: []domain.PartitionGap{
				{Table: domain.TableNotifications, Month: mar}, {Table: domain.TableDeliveries, Month: mar},
				{Table: domain.TableNotifications, Month: apr}}},
			missing: []map[string]string{
				{domain.LabelTable: "notifications", domain.LabelMonth: "2031-03"},
				{domain.LabelTable: "notification_deliveries", domain.LabelMonth: "2031-03"},
				{domain.LabelTable: "notifications", domain.LabelMonth: "2031-04"}}},
		{name: "an unscoped partition names itself",
			health:   domain.PartitionHealth{Unscoped: []string{"notifications_2031m03"}},
			unscoped: []map[string]string{{domain.LabelPartition: "notifications_2031m03"}}},
		{name: "each unscoped partition is its own alarm",
			health:   domain.PartitionHealth{Unscoped: []string{"notifications", "notifications_2031m04"}},
			unscoped: []map[string]string{{domain.LabelPartition: "notifications"}, {domain.LabelPartition: "notifications_2031m04"}}},
		{name: "both at once",
			health: domain.PartitionHealth{
				Missing:  []domain.PartitionGap{{Table: domain.TableNotifications, Month: mar}},
				Unscoped: []string{"notifications_2031m04"}},
			missing:  []map[string]string{{domain.LabelTable: "notifications", domain.LabelMonth: "2031-03"}},
			unscoped: []map[string]string{{domain.LabelPartition: "notifications_2031m04"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newMaintFx()
			f.store.health = tc.health
			f.pass(t, f.build(t, "me"))

			got := map[string][]map[string]string{}
			for _, e := range f.metrics.Events() {
				switch e.Name {
				case domain.MetricPartitionMissing, domain.MetricPartitionUnscoped:
					got[e.Name] = append(got[e.Name], e.Labels)
				}
			}
			for name, want := range map[string][]map[string]string{
				domain.MetricPartitionMissing: tc.missing, domain.MetricPartitionUnscoped: tc.unscoped,
			} {
				if fmt.Sprint(got[name]) != fmt.Sprint(want) && (len(got[name]) != 0 || len(want) != 0) {
					t.Errorf("%s raised %v, want %v", name, got[name], want)
				}
			}
		})
	}
}

// Provisioning that fails (a role that cannot create tables) is reported, and the check
// still runs after it: that is exactly when a month is missing and the alarm is wanted.
func TestAProvisioningFailureIsReportedAndTheCheckStillAlarms(t *testing.T) {
	t.Parallel()
	f := newMaintFx()
	boom := errors.New("permission denied for schema public")
	f.store.ensureErr = boom
	f.store.health = domain.PartitionHealth{Missing: []domain.PartitionGap{
		{Table: domain.TableNotifications, Month: time.Date(2031, 4, 1, 0, 0, 0, 0, time.UTC)}}}

	_, err := f.build(t, "me").Pass(t.Context())
	if !errors.Is(err, boom) {
		t.Fatalf("Pass = %v, want the provisioning fault returned", err)
	}
	if f.metrics.Count(domain.MetricPartitionMissing) != 1 {
		t.Fatalf("%d missing-partition alarms, want 1: the check runs after a provisioning that failed", f.metrics.Count(domain.MetricPartitionMissing))
	}
}

// --------------------------------------------------------------- the loop --

// Run makes a pass at once and then one every tick, until its context ends, and returns nil.
func TestRunPassesAtOnceAndThenEveryTick(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newMaintFx()
		f.store.now = time.Now
		m, err := app.NewMaintenance(f.cfg, app.Deps{Maintenance: f.store, Metrics: f.metrics},
			app.WithMaintenanceOwner("me"), app.WithMaintenanceTick(7*time.Second),
			app.WithJobInterval(app.JobPreflight, time.Second))
		if err != nil {
			t.Fatal(err)
		}
		start := time.Now()

		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- m.Run(ctx) }()
		time.Sleep(15 * time.Second)
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("Run = %v, want nil when its context ends", err)
		}

		var offsets []time.Duration
		for _, c := range f.store.ops("conditions") {
			offsets = append(offsets, c.when.Sub(start))
		}
		if want := []time.Duration{0, 7 * time.Second, 14 * time.Second}; !slices.Equal(offsets, want) {
			t.Fatalf("passes began at %v, want %v: at once, then every tick", offsets, want)
		}
		if n := len(f.store.ops("ensure")); n != 1 {
			t.Fatalf("provisioning ran %d times in 15 seconds, want once, at startup: its interval is an hour", n)
		}
	})
}

// A fault is reported and the loop goes on; a preflight that cannot read the database is
// a fault and not a reason to stop, and the other jobs have run regardless.
func TestRunReportsAFaultAndGoesOn(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newMaintFx()
		f.store.now = time.Now
		boom := errors.New("pg_stat_activity is away")
		f.store.condErr = boom
		m, err := app.NewMaintenance(f.cfg, app.Deps{Maintenance: f.store, Metrics: f.metrics},
			app.WithMaintenanceOwner("me"), app.WithMaintenanceTick(time.Second),
			app.WithJobInterval(app.JobPreflight, time.Second),
			app.WithMaintenanceErrorHandler(f.errs.add))
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- m.Run(ctx) }()
		time.Sleep(2500 * time.Millisecond)
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("Run = %v, want nil: a preflight that cannot read the database does not stop the service", err)
		}

		if errs := f.errs.all(); len(errs) != 3 || !errors.Is(errs[0], boom) {
			t.Fatalf("reported %v, want the fault once per pass, three passes", errs)
		}
		if n := len(f.store.ops("retire")); n != 3 {
			t.Fatalf("retention retired %d tables, want 3: the jobs before the preflight ran", n)
		}
	})
}

func TestMaintenanceRunStopsAtOnceWhenItsContextHasEnded(t *testing.T) {
	t.Parallel()
	f := newMaintFx()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := f.build(t, "me").Run(ctx); err != nil {
		t.Fatalf("Run = %v, want nil", err)
	}
	if n := len(f.store.log()); n != 0 {
		t.Fatalf("%d calls after the context ended, want none", n)
	}
}

// ---------------------------------------------------------------- the wiring --

func TestNewMaintenanceRefusesWhatCouldNotWork(t *testing.T) {
	t.Parallel()
	valid := notify.Config{Realm: planRealm, StoreDSN: "x", RedisURL: "y"}
	deps := app.Deps{Maintenance: newMaintStore(time.Now)}
	cases := []struct {
		name string
		cfg  notify.Config
		deps app.Deps
		opts []app.MaintenanceOption
		want string
	}{
		{"no maintenance port", valid, app.Deps{}, nil, "Deps.Maintenance"},
		{"a configuration that does not validate", notify.Config{StoreDSN: "x", RedisURL: "y"}, deps, nil, "Realm"},
		{"a tick of zero", valid, deps, []app.MaintenanceOption{app.WithMaintenanceTick(0)}, "tick"},
		{"a batch of zero", valid, deps, []app.MaintenanceOption{app.WithMaintenanceBatch(0)}, "batch"},
		{"no owner", valid, deps, []app.MaintenanceOption{app.WithMaintenanceOwner("")}, "owner"},
		{"a job nobody runs", valid, deps, []app.MaintenanceOption{app.WithJobInterval("shred", time.Hour)}, "shred"},
		{"an interval of zero", valid, deps, []app.MaintenanceOption{app.WithJobInterval(app.JobRetention, 0)}, app.JobRetention},
		{"limits that judge nothing", valid, deps, []app.MaintenanceOption{app.WithPreflightLimits(domain.PreflightLimits{})}, "limits"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m, err := app.NewMaintenance(tc.cfg, tc.deps, tc.opts...)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("NewMaintenance = %v, %v, want an error naming %q", m, err, tc.want)
			}
		})
	}
	if _, err := app.NewMaintenance(valid, deps); err != nil {
		t.Fatalf("NewMaintenance with what it needs = %v", err)
	}
}

// ---------------------------------------------------------------- preflight --

// Each condition is a warning, returned and handed to the handler, and never an error:
// the engine runs on a database that has every one of them, and the operator learns of
// it before its effect (NR-037).
func TestPreflightReportsEachConditionAsAWarningAndNeverAnError(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		mutate func(*domain.DatabaseConditions)
		want   []string
	}{
		{name: "a healthy configuration produces none"},
		{name: "a role with no timeouts produces its warnings",
			mutate: func(c *domain.DatabaseConditions) { c.IdleInTransactionTimeout, c.StatementTimeout = 0, 0 },
			want:   []string{domain.WarnNoIdleInTransactionTimeout, domain.WarnNoStatementTimeout}},
		{name: "an open old transaction produces its warning",
			mutate: func(c *domain.DatabaseConditions) { c.OldestTransactionAge = 5 * time.Minute },
			want:   []string{domain.WarnOldTransaction}},
		{name: "a role that cannot see other sessions says so",
			mutate: func(c *domain.DatabaseConditions) { c.SeesAllSessions = false },
			want:   []string{domain.WarnBlindToOtherSessions}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newMaintFx()
			c := maintHealthy()
			if tc.mutate != nil {
				tc.mutate(&c)
			}
			f.store.conditions = []domain.DatabaseConditions{c}

			ws, err := f.build(t, "me").Preflight(t.Context())
			if err != nil {
				t.Fatalf("Preflight = %v, want no error: a condition is a warning", err)
			}
			var got []string
			for _, w := range ws {
				got = append(got, w.Code)
			}
			if !slices.Equal(got, tc.want) {
				t.Fatalf("Preflight warned %v, want %v", got, tc.want)
			}
			if handled := f.warns.codes(); !slices.Equal(handled, tc.want) {
				t.Fatalf("the handler was given %v, want %v", handled, tc.want)
			}
		})
	}
}

// max_wal_size is judged against the peak rate the readings show, so it takes two:
// the first has an average to go on, and the next an interval.
func TestPreflightJudgesMaxWALSizeAgainstTheRateItHasSeen(t *testing.T) {
	t.Parallel()
	f := newMaintFx()
	const mib = int64(1) << 20
	reset := maintEpoch.Add(-10 * time.Minute)
	first := maintHealthy()
	first.MaxWALSize = 1024 * mib
	first.At, first.WALBytes, first.WALStatsReset = maintEpoch, 600*mib, reset // 1 MiB/s: 900 MiB in fifteen minutes
	second := first
	second.At, second.WALBytes = maintEpoch.Add(time.Minute), 900*mib // 5 MiB/s: 4500 MiB
	f.store.conditions = []domain.DatabaseConditions{first, second}
	m := f.build(t, "me")

	if ws, err := m.Preflight(t.Context()); err != nil || len(ws) != 0 {
		t.Fatalf("first reading: %v, %v, want no warning: 1 GiB holds fifteen minutes of 1 MiB/s", ws, err)
	}
	ws, err := m.Preflight(t.Context())
	if err != nil || len(ws) != 1 || ws[0].Code != domain.WarnMaxWALSizeUndersized {
		t.Fatalf("second reading: %v, %v, want the undersized warning: 5 MiB/s is 4500 MiB for fifteen minutes", ws, err)
	}
}

// The two gauges are set from the reading, whatever it says: the age in seconds, zero
// when no transaction is open, and the checkpoints the WAL forced.
func TestPreflightSetsItsGauges(t *testing.T) {
	t.Parallel()
	f := newMaintFx()
	c := maintHealthy()
	c.OldestTransactionAge = 95 * time.Second
	c.CheckpointsRequested, c.CheckpointsTimed = 41, 300
	f.store.conditions = []domain.DatabaseConditions{c}
	m := f.build(t, "me")

	if _, err := m.Preflight(t.Context()); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]float64{domain.MetricDBOldestXminAge: 95, domain.MetricDBCheckpointsRequested: 41} {
		if got, ok := f.metrics.GaugeValue(name); !ok || got != want {
			t.Errorf("gauge %s = %v, %v, want %v", name, got, ok, want)
		}
	}

	f.store.mu.Lock()
	f.store.conditions = []domain.DatabaseConditions{maintHealthy()}
	f.store.reads = 0
	f.store.mu.Unlock()
	if _, err := m.Preflight(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got, ok := f.metrics.GaugeValue(domain.MetricDBOldestXminAge); !ok || got != 0 {
		t.Errorf("gauge %s = %v, %v after the transaction closed, want 0: no transaction is a reading", domain.MetricDBOldestXminAge, got, ok)
	}
}

// countingMetrics is a Metrics that cannot hold a gauge.
type countingMetrics struct {
	mu sync.Mutex
	n  int
}

func (c *countingMetrics) Inc(string, map[string]string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n++
}

// A Metrics that cannot hold a level is not an error and is not given one.
func TestPreflightWithAMetricsThatCannotHoldAGauge(t *testing.T) {
	t.Parallel()
	f := newMaintFx()
	cm := &countingMetrics{}
	m, err := app.NewMaintenance(f.cfg, app.Deps{Maintenance: f.store, Clock: f.clock, Metrics: cm}, app.WithMaintenanceOwner("me"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Preflight(t.Context()); err != nil {
		t.Fatalf("Preflight = %v, want no error without gauges", err)
	}
	if cm.n != 0 {
		t.Fatalf("a healthy preflight incremented a counter %d times", cm.n)
	}
}

// A reading that cannot be taken is a fault the caller sees, with no warning made up.
func TestPreflightThatCannotReadReturnsAFaultAndNoWarnings(t *testing.T) {
	t.Parallel()
	f := newMaintFx()
	boom := errors.New("permission denied for pg_stat_wal")
	f.store.condErr = boom
	ws, err := f.build(t, "me").Preflight(t.Context())
	if !errors.Is(err, boom) || len(ws) != 0 {
		t.Fatalf("Preflight = %v, %v, want the fault and no warnings", ws, err)
	}
	if _, ok := f.metrics.GaugeValue(domain.MetricDBOldestXminAge); ok {
		t.Fatal("a gauge was set from a reading that was never taken")
	}
}

// The job carries the warnings to the handler, and the pass that found them is not a
// failed pass.
func TestThePreflightJobHandsItsWarningsToTheHandler(t *testing.T) {
	t.Parallel()
	f := newMaintFx()
	c := maintHealthy()
	c.StatementTimeout = 0
	f.store.conditions = []domain.DatabaseConditions{c}

	ran, err := f.build(t, "me").Pass(t.Context())
	if err != nil {
		t.Fatalf("Pass = %v, want no error: a warning is not a failure", err)
	}
	if !slices.Contains(ran, app.JobPreflight) {
		t.Fatalf("Pass ran %v, want the preflight among them", ran)
	}
	if got := f.warns.codes(); !slices.Equal(got, []string{domain.WarnNoStatementTimeout}) {
		t.Fatalf("the handler was given %v, want the one warning", got)
	}
}
