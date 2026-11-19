//go:build integration

package app_test

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/truongpx396/intel-notification/app"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/notifytest"
	"github.com/truongpx396/intel-notification/ports"
)

// T038: the maintenance jobs end to end, on the Env's real store, with real leases in
// notify_job_leases. The cutoffs come from the Env's fake clock (Epoch, 2031-03-01), the
// leases and the partitions' dates from the database, so the rows these tests age are
// dated by the fixture and never by the day the suite runs.

// countingMaintenance is the real store behind a count of what the jobs asked of it, and,
// when noProvision is set, a provisioning that does nothing, for the test that wants the
// catalog to stay as the test left it.
type countingMaintenance struct {
	ports.Maintenance

	mu          sync.Mutex
	calls       map[string]int
	noProvision bool
}

func newCounting(m ports.Maintenance) *countingMaintenance {
	return &countingMaintenance{Maintenance: m, calls: map[string]int{}}
}

func (c *countingMaintenance) note(op string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls[op]++
}

func (c *countingMaintenance) count(op string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls[op]
}

func (c *countingMaintenance) RetirePartitions(ctx context.Context, table string, before time.Time) ([]string, error) {
	c.note("retire")
	return c.Maintenance.RetirePartitions(ctx, table, before)
}

func (c *countingMaintenance) ExpireIdem(ctx context.Context, olderThan time.Time, batch int) (int, error) {
	c.note("expire_idem")
	return c.Maintenance.ExpireIdem(ctx, olderThan, batch)
}

func (c *countingMaintenance) ExpireDigests(ctx context.Context, olderThan time.Time, batch int) (int, error) {
	c.note("expire_digests")
	return c.Maintenance.ExpireDigests(ctx, olderThan, batch)
}

func (c *countingMaintenance) ExpireBroadcasts(ctx context.Context, olderThan time.Time, batch int) (int, error) {
	c.note("expire_broadcasts")
	return c.Maintenance.ExpireBroadcasts(ctx, olderThan, batch)
}

func (c *countingMaintenance) EnsurePartitions(ctx context.Context, from time.Time, months int) ([]string, error) {
	c.note("ensure")
	if c.noProvision {
		return nil, nil
	}
	return c.Maintenance.EnsurePartitions(ctx, from, months)
}

func (c *countingMaintenance) CheckPartitions(ctx context.Context, at time.Time) (domain.PartitionHealth, error) {
	c.note("check")
	return c.Maintenance.CheckPartitions(ctx, at)
}

func (c *countingMaintenance) DatabaseConditions(ctx context.Context) (domain.DatabaseConditions, error) {
	c.note("conditions")
	return c.Maintenance.DatabaseConditions(ctx)
}

func maintainer(t *testing.T, e *notifytest.Env, store ports.Maintenance, owner string) *app.Maintenance {
	t.Helper()
	m, err := app.NewMaintenance(e.Config, app.Deps{Maintenance: store, Clock: e.Clock, Metrics: e.Metrics},
		app.WithMaintenanceOwner(owner), app.WithMaintenanceBatch(2))
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func partitionsOf(t *testing.T, e *notifytest.Env, table string) []string {
	t.Helper()
	rows, err := e.DB.Admin.Query(t.Context(), `
SELECT c.relname::text FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
 WHERE i.inhparent = $1::regclass ORDER BY c.relname COLLATE "C"`, table)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		names = append(names, n)
	}
	return names
}

// seedAged writes what the jobs should clear and what they should leave: idempotency keys
// older and newer than the window, broadcasts completed long ago, recently, and not at all,
// and flushed digest windows whose delivery has finished and whose has not.
func seedAged(t *testing.T, e *notifytest.Env, id domain.Identity) {
	t.Helper()
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := e.DB.Admin.Exec(t.Context(), sql, args...); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	for _, k := range []string{"aged-1", "aged-2", "aged-3"} {
		send(t, e, id, k)
	}
	send(t, e, id, "fresh")
	exec(`UPDATE notify_idem SET created_at = '2031-02-01' WHERE idem_key LIKE 'aged-%'`)
	for _, row := range []struct {
		key, status string
		completed   *time.Time
	}{
		{"b-old-1", "completed", ptr(time.Date(2031, 2, 1, 0, 0, 0, 0, time.UTC))},
		{"b-old-2", "completed", ptr(time.Date(2031, 2, 1, 0, 0, 0, 0, time.UTC))},
		{"b-old-3", "completed", ptr(time.Date(2031, 2, 1, 0, 0, 0, 0, time.UTC))},
		{"b-recent", "completed", ptr(time.Date(2031, 2, 28, 0, 0, 0, 0, time.UTC))},
		{"b-pending", "pending", nil},
	} {
		exec(`INSERT INTO notification_broadcasts (realm, tenant_kind, tenant_id, idem_key, audience, request, status, completed_at)
              VALUES ('aisat', 'workspace', 'w1', $1, 'everyone', '{}'::jsonb, $2, $3)`, row.key, row.status, row.completed)
	}
}

func ptr[T any](v T) *T { return &v }

// snapshot is the state the jobs act on: what a re-run must leave exactly as it found it.
type snapshot struct {
	idem, broadcasts, digests int
	partitions                map[string][]string
}

func takeSnapshot(t *testing.T, e *notifytest.Env) snapshot {
	t.Helper()
	s := snapshot{
		idem:       count(t, e, `SELECT count(*) FROM notify_idem`),
		broadcasts: count(t, e, `SELECT count(*) FROM notification_broadcasts`),
		digests:    count(t, e, `SELECT count(*) FROM digest_buffer`),
		partitions: map[string][]string{},
	}
	for _, table := range []string{domain.TableNotifications, domain.TableDeliveries, domain.TableDeadLetters} {
		s.partitions[table] = partitionsOf(t, e, table)
	}
	return s
}

// The jobs do what they are for on a real database: the aged keys and the completed
// broadcasts go and the rest stays, the aged partitions are retired and the months ahead
// are provisioned, every job leaves its lease row naming the worker that ran it, and the
// check of a healthy catalog raises neither alarm.
func TestTheJobsClearWhatIsAgedAndLeaveTheRest(t *testing.T) {
	t.Parallel()
	e, id := emailEnv(t)
	e.AddChannel("email", domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupProvider, DedupWindow: time.Hour})
	seedAged(t, e, id)
	if got := partitionsOf(t, e, domain.TableNotifications); !slices.Contains(got, "notifications_2026m09") {
		t.Fatalf("the template has no aged partition to retire: %v", got)
	}

	m := maintainer(t, e, e.Maintenance, "worker-a")
	ran, err := m.Pass(t.Context())
	if err != nil {
		t.Fatalf("Pass = %v", err)
	}
	if !slices.Equal(ran, []string{app.JobPartitions, app.JobRetention, app.JobIdemExpiry, app.JobDigestExpiry, app.JobBroadcastCleanup, app.JobPreflight}) {
		t.Fatalf("Pass ran %v, want every job", ran)
	}

	if n := count(t, e, `SELECT count(*) FROM notify_idem WHERE idem_key LIKE 'aged-%'`); n != 0 {
		t.Errorf("%d aged idempotency keys remain, want none: they are past the window", n)
	}
	if n := count(t, e, `SELECT count(*) FROM notify_idem WHERE idem_key = 'fresh'`); n != 1 {
		t.Errorf("%d fresh keys, want the 1 inside the window", n)
	}
	for key, want := range map[string]int{"b-old-1": 0, "b-old-2": 0, "b-old-3": 0, "b-recent": 1, "b-pending": 1} {
		if n := count(t, e, `SELECT count(*) FROM notification_broadcasts WHERE idem_key = $1`, key); n != want {
			t.Errorf("broadcast %s: %d rows, want %d", key, n, want)
		}
	}
	for _, table := range []string{domain.TableNotifications, domain.TableDeliveries, domain.TableDeadLetters} {
		got := partitionsOf(t, e, table)
		if slices.Contains(got, table+"_2026m09") {
			t.Errorf("%s still has its 2026-09 partition, which is far past its window: %v", table, got)
		}
		for _, month := range []string{"2031m03", "2031m04", "2031m05"} {
			if !slices.Contains(got, table+"_"+month) {
				t.Errorf("%s has no %s partition after provisioning: %v", table, month, got)
			}
		}
	}
	for _, job := range []string{app.JobPartitions, app.JobRetention, app.JobIdemExpiry, app.JobDigestExpiry, app.JobBroadcastCleanup, app.JobPreflight} {
		var owner string
		if err := e.DB.Admin.QueryRow(t.Context(), `SELECT owner FROM notify_job_leases WHERE job = $1`, job).Scan(&owner); err != nil || owner != "worker-a" {
			t.Errorf("the lease row of %s names %q (%v), want worker-a: notify_job_leases is how an operator sees who runs what", job, owner, err)
		}
	}
	if n := e.Metrics.Count(domain.MetricPartitionMissing) + e.Metrics.Count(domain.MetricPartitionUnscoped); n != 0 {
		t.Errorf("%d partition alarms on a healthy catalog, want none", n)
	}
	if _, ok := e.Metrics.GaugeValue(domain.MetricDBOldestXminAge); !ok {
		t.Error("the preflight set no notify.db.oldest_xmin_age gauge")
	}
}

// A re-run of every job changes nothing: the second pass finds the first pass's work
// done, and the database is as the first left it.
func TestAReRunOfEveryJobChangesNothing(t *testing.T) {
	t.Parallel()
	e, id := emailEnv(t)
	e.AddChannel("email", domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupProvider, DedupWindow: time.Hour})
	seedAged(t, e, id)
	m := maintainer(t, e, e.Maintenance, "worker-a")

	if _, err := m.Pass(t.Context()); err != nil {
		t.Fatalf("first Pass = %v", err)
	}
	first := takeSnapshot(t, e)
	e.Clock.Advance(2 * time.Hour) // every job is due again
	ran, err := m.Pass(t.Context())
	if err != nil || len(ran) != 6 {
		t.Fatalf("second Pass ran %v, %v, want every job again", ran, err)
	}
	second := takeSnapshot(t, e)
	if first.idem != second.idem || first.broadcasts != second.broadcasts || first.digests != second.digests {
		t.Errorf("a re-run changed the rows: %+v then %+v", first, second)
	}
	for table, was := range first.partitions {
		if !slices.Equal(was, second.partitions[table]) {
			t.Errorf("a re-run changed the partitions of %s: %v then %v", table, was, second.partitions[table])
		}
	}
}

// Two workers on one database contend for the jobs through the real lease table, and
// each job runs once between them, however the race falls.
func TestTwoWorkersOnOneDatabaseRunEachJobOnce(t *testing.T) {
	t.Parallel()
	e, id := emailEnv(t)
	e.AddChannel("email", domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupProvider, DedupWindow: time.Hour})
	seedAged(t, e, id)
	store := newCounting(e.Maintenance)
	a, b := maintainer(t, e, store, "worker-a"), maintainer(t, e, store, "worker-b")

	ran := make([][]string, 2)
	var wg sync.WaitGroup
	for i, m := range []*app.Maintenance{a, b} {
		wg.Go(func() {
			var err error
			if ran[i], err = m.Pass(t.Context()); err != nil {
				t.Errorf("worker %d: Pass = %v", i, err)
			}
		})
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("two workers did not finish a pass within 60 seconds")
	}

	counts := map[string]int{}
	for _, r := range ran {
		for _, job := range r {
			counts[job]++
		}
	}
	for _, job := range []string{app.JobPartitions, app.JobRetention, app.JobIdemExpiry, app.JobDigestExpiry, app.JobBroadcastCleanup, app.JobPreflight} {
		if counts[job] != 1 {
			t.Errorf("%s ran %d times across two workers, want once (a ran %v, b ran %v)", job, counts[job], ran[0], ran[1])
		}
	}
	// The retention job asks once for each of its three tables, and a job that works in
	// batches asks until a batch comes back short: three aged rows in batches of two.
	for op, want := range map[string]int{"retire": 3, "ensure": 1, "check": 1, "conditions": 1, "expire_idem": 2, "expire_broadcasts": 2, "expire_digests": 1} {
		if got := store.count(op); got != want {
			t.Errorf("%s was called %d times, want %d", op, got, want)
		}
	}
}

// The alarms are read from a real catalog. With provisioning that does nothing, a month
// dropped from one table is reported as missing, naming the table and the month, and a
// notifications partition created without its recipient scope is reported as unscoped,
// naming it: the two conditions the alarms exist for, produced by the conditions and not
// by a script.
func TestThePartitionAlarmsFireOnARealCatalog(t *testing.T) {
	t.Parallel()
	e, _ := emailEnv(t)
	for _, sql := range []string{
		`DROP TABLE dead_letters_2031m04`,
		`CREATE TABLE notifications_2031m05 PARTITION OF notifications
             FOR VALUES FROM ('2031-05-01 00:00:00+00') TO ('2031-06-01 00:00:00+00')`,
	} {
		if _, err := e.DB.Admin.Exec(t.Context(), sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	store := newCounting(e.Maintenance)
	store.noProvision = true

	if _, err := maintainer(t, e, store, "worker-a").Pass(t.Context()); err != nil {
		t.Fatalf("Pass = %v", err)
	}
	if n := e.Metrics.CountWith(domain.MetricPartitionMissing, map[string]string{domain.LabelTable: "dead_letters", domain.LabelMonth: "2031-04"}); n != 1 {
		t.Errorf("%d missing-partition alarms for dead_letters 2031-04, want 1 (all: %v)", n, e.Metrics.Events())
	}
	if n := e.Metrics.Count(domain.MetricPartitionMissing); n != 1 {
		t.Errorf("%d missing-partition alarms in all, want only the one gap", n)
	}
	if n := e.Metrics.CountWith(domain.MetricPartitionUnscoped, map[string]string{domain.LabelPartition: "notifications_2031m05"}); n != 1 {
		t.Errorf("%d unscoped-partition alarms for notifications_2031m05, want 1 (all: %v)", n, e.Metrics.Events())
	}
}

// With provisioning working, the same lost month is made good before the check reads the
// catalog, so the gap never alarms: the jobs repair what they can and report what they
// cannot.
func TestProvisioningMakesGoodALostMonthBeforeTheCheckSeesIt(t *testing.T) {
	t.Parallel()
	e, _ := emailEnv(t)
	if _, err := e.DB.Admin.Exec(t.Context(), `DROP TABLE dead_letters_2031m04`); err != nil {
		t.Fatal(err)
	}
	if _, err := maintainer(t, e, e.Maintenance, "worker-a").Pass(t.Context()); err != nil {
		t.Fatalf("Pass = %v", err)
	}
	if got := partitionsOf(t, e, domain.TableDeadLetters); !slices.Contains(got, "dead_letters_2031m04") {
		t.Fatalf("dead_letters has %v, want 2031m04 provisioned again", got)
	}
	if n := e.Metrics.Count(domain.MetricPartitionMissing); n != 0 {
		t.Fatalf("%d missing-partition alarms, want none: the month was provisioned before the check", n)
	}
}
