//go:build integration

package postgres

import (
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/truongpx396/intel-notification/domain"
)

// T039: retention, against PostgreSQL as the table owner. The fixtures sit in the
// bootstrap partitions (2026-09 and after), and every cutoff is a fixed instant, so the
// suite does not depend on the day it runs; the one assertion that reads the real
// clock, that a partition whose range has not ended is never retired, computes its
// expectation from the partitions' own names.

// witnessSQL records, in a table of its own, every row a DELETE removes from a partition
// it is attached to. A partition retired by DROP removes no row, so the witness stays
// empty; a retirement that deleted first and dropped after would be a mass purge (NR-022)
// and fills it. The function runs with its creator's rights, so the owner role the engine
// runs as needs no privilege on the witness. n_tup_del is not used for this: a partitioned
// parent keeps no statistics of its own, a dropped partition takes its with it, and the
// statistics are reported to the collector after the statement and not with it.
const witnessSQL = `
CREATE TABLE retire_witness (tbl text NOT NULL);
CREATE FUNCTION retire_witness_fn() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
    INSERT INTO retire_witness VALUES (TG_TABLE_NAME);
    RETURN OLD;
END $$`

func (e *env) admin(sql string, args ...any) {
	e.t.Helper()
	if _, err := e.db.Admin.Exec(e.t.Context(), sql, args...); err != nil {
		e.t.Fatalf("fixture %q: %v", sql, err)
	}
}

// watch attaches the witness to each named partition.
func (e *env) watch(partitions ...string) {
	e.t.Helper()
	e.admin(witnessSQL)
	for _, p := range partitions {
		e.admin(`CREATE TRIGGER witness AFTER DELETE ON ` + p + ` FOR EACH ROW EXECUTE FUNCTION retire_witness_fn()`)
	}
}

// partitions lists the partitions that exist under a table, oldest name first.
func (e *env) partitions(table string) []string {
	e.t.Helper()
	rows, err := e.db.Admin.Query(e.t.Context(), `
SELECT c.relname::text FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
 WHERE i.inhparent = $1::regclass ORDER BY c.relname COLLATE "C"`, table)
	if err != nil {
		e.t.Fatal(err)
	}
	names, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		e.t.Fatal(err)
	}
	return names
}

func (e *env) oct(recipient, key string) {
	e.t.Helper()
	e.admin(`
INSERT INTO notifications (id, realm, tenant_kind, tenant_id, recipient_kind, recipient_id, topic, idem_key, created_at)
VALUES (gen_random_uuid(), 'aisat', 'workspace', 'w1', 'user', $1, 't', $2, '2026-10-15')`, recipient, key)
}

func utc(y int, m time.Month, d int) time.Time { return time.Date(y, m, d, 0, 0, 0, 0, time.UTC) }

// NR-022, D33. Retiring is dropping whole partitions: no row is deleted, whatever the
// partition held, and what is not retired is not touched.
func TestRetirePartitionsDropsWholePartitionsAndDeletesNoRow(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	u1 := ident("w1", "u1")
	for i := range 3 {
		e.notification(u1, "sept-"+string(rune('a'+i))) // 2026-09-15, in notifications_2026m09
	}
	e.oct("u1", "oct-a")
	e.oct("u1", "oct-b")
	e.watch("notifications_2026m09", "notifications_2026m10")

	retired, err := e.s.RetirePartitions(t.Context(), domain.TableNotifications, utc(2026, 10, 1))
	if err != nil || !slices.Equal(retired, []string{"notifications_2026m09"}) {
		t.Fatalf("RetirePartitions = %v, %v, want exactly notifications_2026m09: its whole range ends at the cutoff", retired, err)
	}
	if got := e.partitions(domain.TableNotifications); slices.Contains(got, "notifications_2026m09") || !slices.Contains(got, "notifications_2026m10") {
		t.Fatalf("partitions left %v, want 2026m09 gone and 2026m10 kept", got)
	}
	if n := e.count(`SELECT count(*) FROM retire_witness`); n != 0 {
		t.Fatalf("%d rows were deleted on the way to retiring a partition: that is a purge, not a metadata operation (NR-022)", n)
	}
	if n := e.count(`SELECT count(*) FROM notifications WHERE created_at >= '2026-10-01' AND created_at < '2026-11-01'`); n != 2 {
		t.Fatalf("%d October rows left, want the 2 of the partition that was not retired", n)
	}
	for _, other := range []string{domain.TableDeliveries, domain.TableDeadLetters} {
		if got := e.partitions(other); !slices.Contains(got, other+"_2026m09") {
			t.Fatalf("retiring the inbox dropped a partition of %s: %v", other, got)
		}
	}
	if again, err := e.s.RetirePartitions(t.Context(), domain.TableNotifications, utc(2026, 10, 1)); err != nil || len(again) != 0 {
		t.Fatalf("a second run retired %v (%v): a re-run changes nothing", again, err)
	}
}

// A partition goes only when its whole range is before the cutoff, so a cutoff inside a
// month keeps that month, and one on its first instant has retired the month before.
func TestRetirePartitionsRetiresOnlyWhatTheCutoffHasWhollyPassed(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		before time.Time
		want   []string
		why    string
	}{
		{"before the first partition ends", utc(2026, 9, 1), nil, ""},
		{"a cutoff inside the month", time.Date(2026, 9, 30, 23, 59, 59, 0, time.UTC), nil,
			"the month's last second has not passed, so a notification of the 30th is still inside the window"},
		{"a cutoff on the first instant after the month", utc(2026, 10, 1), []string{"2026m09"},
			"a partition's upper bound is exclusive, so a cutoff equal to it has passed the whole range"},
		{"a cutoff inside the next month", utc(2026, 10, 20), []string{"2026m09"}, ""},
		{"two months passed", utc(2026, 11, 1), []string{"2026m09", "2026m10"}, "oldest first"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t).with(t)
			var want []string
			for _, month := range tc.want {
				want = append(want, domain.TableNotifications+"_"+month)
			}
			retired, err := e.s.RetirePartitions(t.Context(), domain.TableNotifications, tc.before)
			if err != nil || !slices.Equal(retired, want) {
				t.Fatalf("RetirePartitions(before %v) = %v, %v, want %v: %s", tc.before, retired, err, want, tc.why)
			}
		})
	}
}

// Each table is retired on its own: a table's partitions are named by the call, and the
// others' are not looked at, because each has its own window (D33).
func TestRetirePartitionsTouchesOnlyTheTableItIsGiven(t *testing.T) {
	t.Parallel()
	for _, table := range []string{domain.TableNotifications, domain.TableDeliveries, domain.TableDeadLetters} {
		t.Run(table, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t).with(t)
			retired, err := e.s.RetirePartitions(t.Context(), table, utc(2026, 10, 1))
			if err != nil || !slices.Equal(retired, []string{table + "_2026m09"}) {
				t.Fatalf("RetirePartitions(%s) = %v, %v, want %s_2026m09", table, retired, err, table)
			}
			for _, other := range []string{domain.TableNotifications, domain.TableDeliveries, domain.TableDeadLetters} {
				if other != table && !slices.Contains(e.partitions(other), other+"_2026m09") {
					t.Errorf("retiring %s dropped %s_2026m09", table, other)
				}
			}
		})
	}
}

// Whatever cutoff it is given, a partition whose range has not ended is not retired: it
// holds this month's notifications and next month's, and a cutoff in the future (a
// caller's bug, a unit mistaken) must not be able to drop them.
func TestRetirePartitionsNeverRetiresAPartitionWhoseRangeHasNotEnded(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	now := time.Now()
	var wantGone, wantKept []string
	monthName := regexp.MustCompile(`_(\d{4})m(\d{2})$`)
	for _, p := range e.partitions(domain.TableNotifications) {
		m := monthName.FindStringSubmatch(p)
		if m == nil {
			t.Fatalf("partition %q does not follow the <table>_<yyyy>m<mm> naming", p)
		}
		start, err := time.Parse("2006m01", m[1]+"m"+m[2])
		if err != nil {
			t.Fatal(err)
		}
		if end := start.AddDate(0, 1, 0); !end.After(now) {
			wantGone = append(wantGone, p)
		} else {
			wantKept = append(wantKept, p)
		}
	}
	if len(wantKept) == 0 {
		t.Fatal("no partition covers a month that has not ended; the template is meant to provision this month and the next")
	}

	retired, err := e.s.RetirePartitions(t.Context(), domain.TableNotifications, utc(2100, 1, 1))
	if err != nil || !slices.Equal(retired, wantGone) {
		t.Fatalf("RetirePartitions(before 2100) = %v, %v, want %v: only those whose range has ended", retired, err, wantGone)
	}
	got := e.partitions(domain.TableNotifications)
	for _, p := range wantKept {
		if !slices.Contains(got, p) {
			t.Errorf("%s was retired by a cutoff in the future, though its range has not ended", p)
		}
	}
}

// Only the range-partitioned tables can be retired. Any other name, a queue table or a
// catalog, is refused before anything is dropped.
func TestRetirePartitionsRefusesATableItDoesNotRetire(t *testing.T) {
	t.Parallel()
	for _, table := range []string{"notification_outbox", "notify_idem", "notify_job_leases", "pg_class", "notifications; DROP TABLE notify_job_leases", ""} {
		t.Run(table, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t).with(t)
			retired, err := e.s.RetirePartitions(t.Context(), table, utc(2100, 1, 1))
			if err == nil || len(retired) != 0 {
				t.Fatalf("RetirePartitions(%q) = %v, %v, want an error and nothing retired", table, retired, err)
			}
			if n := e.count(`SELECT count(*) FROM pg_class WHERE relname = 'notify_job_leases'`); n != 1 {
				t.Fatalf("a table the engine runs on is gone after RetirePartitions(%q)", table)
			}
			if got := e.partitions(domain.TableNotifications); !slices.Contains(got, "notifications_2026m09") {
				t.Fatalf("a partition was dropped by a refused call: %v", got)
			}
		})
	}
}

// D6, NR-022. Retention is not blocked by a dead letter that names a notification in the
// partition: nothing references an aged partition, so dropping it cannot fail while a row
// somewhere still points at it, and the dead letter outlives the notification it was about.
func TestRetentionSucceedsWhileADeadLetterReferencesAnAgedPartition(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	u1 := ident("w1", "u1")
	p := e.park(u1, "retire-dl", row{shard: 3, channel: "email"}, domain.OutcomeRejected, "the mailbox refuses")
	var inAged int
	if err := e.db.Admin.QueryRow(t.Context(), `SELECT count(*) FROM notifications_2026m09 WHERE id = $1::uuid`, p.nid).Scan(&inAged); err != nil || inAged != 1 {
		t.Fatalf("the dead letter's notification is not in the aged partition: %d rows, %v", inAged, err)
	}

	retired, err := e.s.RetirePartitions(t.Context(), domain.TableNotifications, utc(2026, 10, 1))
	if err != nil || !slices.Equal(retired, []string{"notifications_2026m09"}) {
		t.Fatalf("RetirePartitions = %v, %v, want the aged partition retired though a dead letter names a notification in it", retired, err)
	}
	if n := e.count(`SELECT count(*) FROM dead_letters WHERE id = $1::uuid AND notification_id = $2::uuid`, p.deadLetter, p.nid); n != 1 {
		t.Fatalf("the dead letter did not survive the retirement of the notification it names: %d rows", n)
	}
}

// The case moved from T020: a delivery still pending when its notification's partition is
// retired is still claimable, and the dispatcher's own read of it says there is nothing
// to send, so it is finished as canceled and leaves the queue. Retrying it would only
// dead-letter a delivery that nothing is wrong with.
func TestRetentionLeavesAPendingDeliveryDrivable(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	u1 := ident("w1", "u1")
	nid := e.notification(u1, "retire-pending")
	oid := e.enqueue(row{id: u1, notification: nid, shard: 5, channel: "email"})

	retired, err := e.s.RetirePartitions(t.Context(), domain.TableNotifications, utc(2026, 10, 1))
	if err != nil || !slices.Equal(retired, []string{"notifications_2026m09"}) {
		t.Fatalf("RetirePartitions = %v, %v, want the aged partition retired", retired, err)
	}
	if !e.queued(oid) {
		t.Fatal("retiring the inbox partition removed a pending delivery: the queue holds pending work only, and this was some")
	}

	c := e.claimOne(5)
	n, members, canceled, err := e.s.Load(t.Context(), c)
	if err != nil || !canceled || n != nil || len(members) != 0 {
		t.Fatalf("Load = %v, %v, canceled=%v, %v, want canceled with nothing to send and no error: the row is gone, and that is not the delivery's fault",
			n, members, canceled, err)
	}
	if ok, err := e.s.Finish(t.Context(), c, mustFinish(t, domain.OutcomeCanceled, "", "")); err != nil || !ok {
		t.Fatalf("Finish = %v, %v, want the canceled delivery finished", ok, err)
	}
	if e.queued(oid) {
		t.Fatal("the delivery is still queued after it was finished")
	}
	if n := e.count(`SELECT count(*) FROM notification_deliveries WHERE notification_id = $1::uuid AND outcome = 'canceled'`, nid); n != 1 {
		t.Fatalf("%d canceled history rows, want 1: the delivery's end is recorded", n)
	}
	if n := e.count(`SELECT count(*) FROM dead_letters WHERE notification_id = $1::uuid`, nid); n != 0 {
		t.Fatalf("%d dead letters: a delivery whose notification aged out is a correct outcome, not a failure (NR-007)", n)
	}
}

// D47. Dropping a partition needs a lock that a long transaction can hold up, and a
// request for it queues every reader and writer of the table behind it. So retirement
// waits a bounded time, gives up on the partition it could not lock, goes on to the next,
// says which it skipped, and finds the skipped one again at its next call.
func TestRetirementWaitsOnlyBrieflyForALockAndSkipsThePartitionItCannotLock(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	s := New(e.db.Owner, WithClock(e.clock), WithRetireLockTimeout(200*time.Millisecond))

	// A reader holds notifications_2026m09, as a long report would.
	holder, err := e.db.Owner.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	released := false
	release := func() {
		if !released {
			released = true
			_ = holder.Rollback(t.Context())
		}
	}
	defer release()
	if _, err := holder.Exec(t.Context(), `SELECT count(*) FROM notifications_2026m09`); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	retired, err := s.RetirePartitions(t.Context(), domain.TableNotifications, utc(2026, 11, 1))
	if waited := time.Since(start); waited > 20*time.Second {
		t.Fatalf("RetirePartitions waited %v for a lock: the wait is bounded", waited)
	}
	if !slices.Equal(retired, []string{"notifications_2026m10"}) {
		t.Fatalf("retired %v, want only notifications_2026m10: the locked partition is skipped and the next is not held up by it", retired)
	}
	if err == nil || !strings.Contains(err.Error(), "notifications_2026m09") {
		t.Fatalf("err = %v, want it to name the partition that was skipped", err)
	}
	if !slices.Contains(e.partitions(domain.TableNotifications), "notifications_2026m09") {
		t.Fatal("the locked partition was dropped")
	}

	release()
	retired, err = s.RetirePartitions(t.Context(), domain.TableNotifications, utc(2026, 11, 1))
	if err != nil || !slices.Equal(retired, []string{"notifications_2026m09"}) {
		t.Fatalf("after the lock was released, RetirePartitions = %v, %v, want the skipped partition retired", retired, err)
	}
}
