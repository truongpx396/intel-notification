//go:build integration

package postgres

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/truongpx396/intel-notification/domain"
)

// D22. A job has one runner at a time; a lapsed lease passes to another.
func TestTryLeaseJob(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	steps := []struct { // sequential on purpose: each step depends on the last
		name   string
		owner  string
		lapse  bool
		wantOK bool
	}{
		{"the first worker takes the lease", "worker-1", false, true},
		{"a second worker cannot take a live lease", "worker-2", false, false},
		{"the holder renews its own lease", "worker-1", false, true},
		{"once the lease lapses another worker takes over", "worker-2", true, true},
		{"and the former holder is now locked out", "worker-1", false, false},
	}
	for _, st := range steps {
		if st.lapse {
			if _, err := e.db.Admin.Exec(t.Context(),
				`UPDATE notify_job_leases SET lease_expires_at = now() - interval '1 second'`); err != nil {
				t.Fatal(err)
			}
		}
		ok, err := e.s.TryLeaseJob(t.Context(), "retention", st.owner, time.Minute)
		if err != nil || ok != st.wantOK {
			t.Fatalf("%s: ok=%v err=%v", st.name, ok, err)
		}
	}
}

// D50. The stale-job alarm (operations.md) reads last_started_at, so it has to say when the
// job last ran for the worker that keeps its lease as much as for one that has just taken it:
// every grant moves it, the holder's renewal included, and a refusal moves nothing. Before
// this, a renewal kept the column and the alarm fired for every healthy job. The age is made
// by backdating the column, and the test never waits for it.
func TestTryLeaseJobRecordsEveryGrantAsTheJobsLastRun(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()
	const job = "retention"
	startedRecently := func() bool {
		return e.count(`SELECT count(*) FROM notify_job_leases WHERE job = 'retention'
                        AND last_started_at > now() - interval '1 minute'`) == 1
	}
	backdate := func() {
		if _, err := e.db.Admin.Exec(ctx,
			`UPDATE notify_job_leases SET last_started_at = now() - interval '1 hour' WHERE job = 'retention'`); err != nil {
			t.Fatal(err)
		}
	}
	ask := func(owner string, want bool) {
		t.Helper()
		if ok, err := e.s.TryLeaseJob(ctx, job, owner, time.Minute); err != nil || ok != want {
			t.Fatalf("TryLeaseJob(%s) = %v, %v, want %v", owner, ok, err, want)
		}
	}

	// A sequence: each step starts from what the last left.
	ask("worker-1", true)
	if !startedRecently() {
		t.Fatal("the first grant did not record the job's start")
	}

	backdate()
	ask("worker-2", false) // the lease is live and worker-1's
	if startedRecently() {
		t.Fatal("a refused ask moved last_started_at: the job did not run")
	}

	ask("worker-1", true) // the holder renews, as every run of a job does
	if !startedRecently() {
		t.Fatal("the holder's renewal kept last_started_at, so the column shows the job last ran when worker-1 took it, and notify.job.stale fires for a healthy job (D50)")
	}

	backdate()
	if _, err := e.db.Admin.Exec(ctx, `UPDATE notify_job_leases SET lease_expires_at = now() - interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	ask("worker-2", true) // the lease lapsed and passed on
	if !startedRecently() {
		t.Fatal("a grant that changed the owner did not record the job's start")
	}
}

// D51. IdemKeys names the keys an erasure is about to remove, so the pre-check's entries for
// them can be forgotten first. It lists one recipient's keys and no one else's, in ascending
// bytewise order whatever the database's collation (the caller's loop compares them as bytes),
// and a caller pages through them by passing back the last key it got. "B" sorts before "a" in
// bytes and after it in most locales, which is why one of the keys is "B".
func TestIdemKeys(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()
	u1 := ident("w1", "u1")
	for _, k := range []string{"b", "a", "B", "invoice:42", "c"} {
		e.notification(u1, k)
	}
	e.notification(ident("w1", "u2"), "of-another-recipient")
	e.notification(ident("w2", "u1"), "of-another-tenant")
	want := []string{"B", "a", "b", "c", "invoice:42"}

	got, err := e.s.IdemKeys(ctx, u1, "", 100)
	if err != nil || !slices.Equal(got, want) {
		t.Fatalf("IdemKeys = %q, %v, want %q: the recipient's keys, in bytewise order, and no one else's", got, err, want)
	}

	// Paged: each ask begins after the last key of the page before, and the end is an empty page.
	var paged []string
	after := ""
	for range 10 {
		page, err := e.s.IdemKeys(ctx, u1, after, 2)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		if len(page) > 2 {
			t.Fatalf("a page of %d for a limit of 2", len(page))
		}
		paged = append(paged, page...)
		after = page[len(page)-1]
	}
	if !slices.Equal(paged, want) {
		t.Fatalf("paged through by two, got %q, want %q", paged, want)
	}

	if none, err := e.s.IdemKeys(ctx, ident("w1", "nobody"), "", 10); err != nil || len(none) != 0 {
		t.Fatalf("IdemKeys for a recipient with none = %q, %v, want an empty list", none, err)
	}
	if _, err := e.s.IdemKeys(ctx, u1, "", 0); err == nil {
		t.Fatal("IdemKeys with a limit of 0 succeeded: a page of nothing would never end a loop")
	}
	if _, err := e.s.IdemKeys(ctx, domain.Identity{}, "", 10); err == nil {
		t.Fatal("IdemKeys for an identity with an empty component succeeded: it cannot be scoped")
	}
}

// D11. Lowering the shard count folds rows from retired shards into live ones,
// and the rows stay claimable.
func TestRehomeShards(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	id := ident("w1", "u1")
	nid := e.notification(id, "k")
	for shard := range domain.Shard(16) {
		e.enqueue(row{id: id, notification: nid, shard: shard, channel: "c" + string(rune('a'+shard))})
	}
	if _, err := e.s.AppendDigest(t.Context(), domain.DigestMember{Identity: id, Topic: "t",
		Channel: "email", Shard: 12, NotificationID: nid}, time.Hour, 10); err != nil {
		t.Fatal(err)
	}

	moved, err := e.s.RehomeShards(t.Context(), 8)
	if err != nil || moved != 9 { // shards 8..15 in the queue, plus the digest window in 12
		t.Fatalf("moved %d rows (%v), want 9", moved, err)
	}
	if n := e.count(`SELECT count(*) FROM notification_outbox WHERE shard >= 8`) +
		e.count(`SELECT count(*) FROM digest_buffer WHERE shard >= 8`); n != 0 {
		t.Fatalf("%d rows left in retired shards", n)
	}
	claimed := 0
	for shard := range domain.Shard(8) {
		claims, err := e.s.Claim(t.Context(), shard, 10, lease)
		if err != nil {
			t.Fatal(err)
		}
		claimed += len(claims)
	}
	if claimed != 16 {
		t.Fatalf("claimed %d rows across the live shards, want all 16", claimed)
	}
	if _, err := e.s.RehomeShards(t.Context(), 0); err == nil {
		t.Fatal("a shard count of zero was accepted")
	}
}

// D21. Keys older than the window expire in batches; keys inside it are kept.
func TestExpireIdem(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	fresh := ident("w1", "fresh")
	e.notification(fresh, "fresh-key")
	for i := range 5 {
		aged := ident("w1", "aged"+string(rune('a'+i)))
		e.notification(aged, "aged-key")
		if _, err := e.db.Admin.Exec(t.Context(),
			`UPDATE notify_idem SET created_at = now() - interval '8 days' WHERE recipient_id = $1`,
			aged.Recipient.ID); err != nil {
			t.Fatal(err)
		}
	}

	window := time.Now().Add(-7 * 24 * time.Hour)
	batches := []struct{ batch, want int }{{2, 2}, {2, 2}, {2, 1}, {2, 0}}
	for i, b := range batches {
		if n, err := e.s.ExpireIdem(t.Context(), window, b.batch); err != nil || n != b.want {
			t.Fatalf("batch %d: expired %d (%v), want %d", i+1, n, err, b.want)
		}
	}
	if e.count(`SELECT count(*) FROM notify_idem WHERE idem_key = 'fresh-key'`) != 1 {
		t.Fatal("a key inside the window expired")
	}
}

// Flushed windows are deleted once their delivery has finished, never before.
func TestExpireDigests(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()
	window := func(recipient string) string {
		w, err := e.s.AppendDigest(ctx, member(recipient), time.Hour, 1) // max 1: sealed at once
		if err != nil {
			t.Fatal(err)
		}
		return w
	}
	finished, pending, unflushed := window("u-finished"), window("u-pending"), window("u-open")
	for _, w := range []string{finished, pending} {
		if ok, err := e.s.FlushDigest(ctx, w); err != nil || !ok {
			t.Fatalf("flush %s: ok=%v err=%v", w, ok, err)
		}
	}
	// Finish the first window's delivery; leave the second's queued.
	if _, err := e.db.Admin.Exec(ctx, `DELETE FROM notification_outbox WHERE digest_id = $1::uuid`, finished); err != nil {
		t.Fatal(err)
	}

	n, err := e.s.ExpireDigests(ctx, time.Now().Add(time.Minute), 100)
	if err != nil || n != 1 {
		t.Fatalf("expired %d windows (%v), want only the finished one", n, err)
	}
	for w, want := range map[string]int{finished: 0, pending: 1, unflushed: 1} {
		if got := e.count(`SELECT count(*) FROM digest_buffer WHERE id = $1::uuid`, w); got != want {
			t.Errorf("window %s: %d rows, want %d", w, got, want)
		}
	}
}

// D35 / NS-012. Erasure removes one recipient from every table that holds it,
// under its own scope, leaves everyone else alone, keeps the suppression and
// records the erasure by hash.
func TestErase(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()
	u9, u1 := ident("w1", "u9"), ident("w1", "u1")
	keep := e.notification(u1, "keep")
	nid := e.notification(u9, "erase:me")
	e.enqueue(row{id: u9, notification: nid, shard: 90, channel: "email"})
	if _, err := e.s.AppendDigest(ctx, member("u9"), time.Hour, 100); err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{
		`INSERT INTO notification_preferences (realm, tenant_kind, tenant_id, recipient_kind, recipient_id, topic, channel, enabled)
         VALUES ('aisat', 'workspace', 'w1', 'user', 'u9', 't', 'email', false)`,
		`INSERT INTO notification_schedules (realm, tenant_kind, tenant_id, recipient_kind, recipient_id)
         VALUES ('aisat', 'workspace', 'w1', 'user', 'u9')`,
		`INSERT INTO recipient_addresses (realm, tenant_kind, tenant_id, recipient_kind, recipient_id, channel, address_key, value)
         VALUES ('aisat', 'workspace', 'w1', 'user', 'u9', 'email', 'k9', 'u9@example.com')`,
		`INSERT INTO notification_deliveries (id, notification_id, realm, tenant_kind, tenant_id, recipient_kind,
                                              recipient_id, topic, channel, address_key, outcome, attempts)
         VALUES (gen_random_uuid(), '` + nid + `', 'aisat', 'workspace', 'w1', 'user', 'u9', 't', 'sms', 'k', 'delivered', 1)`,
		`INSERT INTO dead_letters (id, realm, tenant_kind, tenant_id, recipient_kind, recipient_id, source, reason,
                                   payload, attempts, last_error)
         VALUES (gen_random_uuid(), 'aisat', 'workspace', 'w1', 'user', 'u9', 'outbox', 'max_attempts', '{}', 5, 'timeout')`,
	} {
		if _, err := e.db.Admin.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := e.db.Admin.Exec(ctx, `INSERT INTO channel_suppressions (realm, channel, address_hash, reason)
                                       VALUES ('aisat', 'email', $1, 'complaint')`,
		domain.SuppressionHash("email", "u9@example.com")); err != nil {
		t.Fatal(err)
	}

	counts, err := e.s.Erase(ctx, u9)
	if err != nil {
		t.Fatal(err)
	}
	// Every table that CAN hold a recipient, read from the catalog rather than
	// from erasedTables, so a table the code forgot — or one added later — fails
	// here instead of keeping personal data.
	rows, err := e.db.Admin.Query(ctx, `
SELECT c.relname FROM pg_class c
 WHERE c.relnamespace = 'public'::regnamespace AND c.relkind IN ('r', 'p') AND NOT c.relispartition
   AND EXISTS (SELECT 1 FROM pg_attribute a
                WHERE a.attrelid = c.oid AND a.attname = 'recipient_id' AND NOT a.attisdropped)`)
	if err != nil {
		t.Fatal(err)
	}
	holders, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil || len(holders) == 0 {
		t.Fatalf("listing tables that hold recipients: %v", err)
	}
	for _, table := range holders {
		if n := e.count(`SELECT count(*) FROM ` + pgx.Identifier{table}.Sanitize() + ` WHERE recipient_id = 'u9'`); n != 0 {
			t.Errorf("%s: %d rows of u9 remain after erasure", table, n)
		}
		if counts[table] != 1 {
			t.Errorf("%s: erasure reported %d rows, want the 1 fixture row", table, counts[table])
		}
	}
	if e.count(`SELECT count(*) FROM notifications WHERE id = $1::uuid`, keep) != 1 {
		t.Error("erasing u9 touched u1's notification")
	}
	if e.count(`SELECT count(*) FROM channel_suppressions WHERE reason = 'complaint'`) != 1 {
		t.Error("the complaint suppression did not survive; u9 would be mailed again")
	}
	if e.count(`SELECT count(*) FROM erasure_requests WHERE subject_hash = $1`, domain.SubjectHash(u9)) != 1 {
		t.Error("the erasure was not recorded by the identity's hash")
	}
	if _, err := e.s.Erase(ctx, domain.Identity{Realm: "aisat"}); err == nil {
		t.Error("an identity with empty components was accepted")
	}
}

// D17. Provisioning creates each missing monthly partition once, and every new
// notifications partition carries the recipient-scope policy — so reading it by
// name, unscoped, as the owner, shows nothing.
func TestEnsurePartitions(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()
	from := time.Date(2031, 1, 10, 0, 0, 0, 0, time.UTC)

	created, err := e.s.EnsurePartitions(ctx, from, 2)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"notifications_2031m01", "notification_deliveries_2031m01", "dead_letters_2031m01",
		"notifications_2031m02", "notification_deliveries_2031m02", "dead_letters_2031m02",
	}
	if len(created) != len(want) {
		t.Fatalf("created %v, want %v", created, want)
	}
	for i := range want {
		if created[i] != want[i] {
			t.Fatalf("created %v, want %v", created, want)
		}
	}
	if again, err := e.s.EnsurePartitions(ctx, from, 2); err != nil || len(again) != 0 {
		t.Fatalf("a second run created %v (%v); provisioning must be idempotent", again, err)
	}

	for _, p := range []string{"notifications_2031m01", "notifications_2031m02"} {
		if n := e.count(`
SELECT count(*) FROM pg_class c
 WHERE c.oid = $1::regclass AND c.relrowsecurity AND c.relforcerowsecurity
   AND EXISTS (SELECT 1 FROM pg_policy p WHERE p.polrelid = c.oid AND p.polname = 'recipient_scope')`, p); n != 1 {
			t.Errorf("%s lacks the forced recipient_scope policy", p)
		}
	}

	if _, err := e.db.Admin.Exec(ctx, `
INSERT INTO notifications (id, realm, tenant_kind, tenant_id, recipient_kind, recipient_id, topic, idem_key, created_at)
VALUES (gen_random_uuid(), 'aisat', 'workspace', 'w1', 'user', 'u1', 't', 'k', '2031-01-20')`); err != nil {
		t.Fatalf("insert into a provisioned month: %v", err)
	}
	var visible int
	if err := e.db.Owner.QueryRow(ctx, `SELECT count(*) FROM notifications_2031m01`).Scan(&visible); err != nil {
		t.Fatal(err)
	}
	if visible != 0 {
		t.Fatalf("the owner read %d rows of a new partition without a scope", visible)
	}
}

// scopePredicate is recipient_scope's USING clause as notify_apply_recipient_scope
// writes it. Cases that recreate the policy with one thing changed use it, so
// the one change is the only difference.
const scopePredicate = `
    realm          = current_setting('notify.realm', true)
AND tenant_kind    = current_setting('notify.tenant_kind', true)
AND tenant_id      = current_setting('notify.tenant_id', true)
AND recipient_kind = current_setting('notify.recipient_kind', true)
AND recipient_id   = current_setting('notify.recipient_id', true)`

// NR-022, D17, D33. CheckPartitions reads, from the catalog alone, what the two
// partition alarms fire on: a month, this one or the next, that a
// range-partitioned table has no partition for, so inserts dated in it fail;
// and notifications or a partition of it that recipient scoping does not hold
// on, so reading it by name shows every recipient's rows. The months are 2031's,
// clear of the partitions the template carries for the current month.
func TestCheckPartitions(t *testing.T) {
	t.Parallel()
	mar := time.Date(2031, 3, 1, 0, 0, 0, 0, time.UTC)
	apr := time.Date(2031, 4, 1, 0, 0, 0, 0, time.UTC)
	may := time.Date(2031, 5, 1, 0, 0, 0, 0, time.UTC)
	midMarch := time.Date(2031, 3, 17, 13, 0, 0, 0, time.UTC)
	const (
		n  = "notifications"
		nd = "notification_deliveries"
		dl = "dead_letters"
	)
	gap := func(table string, month time.Time) domain.PartitionGap {
		return domain.PartitionGap{Table: table, Month: month}
	}
	recreate := func(partition, policy string) []string {
		return []string{
			`DROP POLICY recipient_scope ON ` + partition,
			`CREATE POLICY recipient_scope ON ` + partition + ` ` + policy,
		}
	}

	cases := []struct {
		name      string
		zone      string    // the database's session time zone, set before any connection
		provision bool      // EnsurePartitions for March and April 2031 first
		setup     []string  // then these, as the superuser
		at        time.Time // zero: mid-March
		want      domain.PartitionHealth
	}{
		// Missing.
		{name: "every table has this month and the next", provision: true},
		{name: "nothing provisioned: every table misses both months",
			want: domain.PartitionHealth{Missing: []domain.PartitionGap{
				gap(n, mar), gap(nd, mar), gap(dl, mar), gap(n, apr), gap(nd, apr), gap(dl, apr)}}},
		{name: "next month missing from one table", provision: true,
			setup: []string{`DROP TABLE dead_letters_2031m04`},
			want:  domain.PartitionHealth{Missing: []domain.PartitionGap{gap(dl, apr)}}},
		{name: "this month missing from one table", provision: true,
			setup: []string{`DROP TABLE notification_deliveries_2031m03`},
			want:  domain.PartitionHealth{Missing: []domain.PartitionGap{gap(nd, mar)}}},
		{name: "a month split across two partitions is covered", provision: true,
			setup: []string{
				`DROP TABLE notifications_2031m03`,
				`CREATE TABLE notifications_2031m03a PARTITION OF notifications
                     FOR VALUES FROM ('2031-03-01 00:00+00') TO ('2031-03-16 00:00+00')`,
				`CREATE TABLE notifications_2031m03b PARTITION OF notifications
                     FOR VALUES FROM ('2031-03-16 00:00+00') TO ('2031-04-01 00:00+00')`,
				`SELECT notify_apply_recipient_scope('notifications_2031m03a')`,
				`SELECT notify_apply_recipient_scope('notifications_2031m03b')`,
			}},
		{name: "half a month is missing", provision: true,
			setup: []string{
				`DROP TABLE notifications_2031m03`,
				`CREATE TABLE notifications_2031m03a PARTITION OF notifications
                     FOR VALUES FROM ('2031-03-01 00:00+00') TO ('2031-03-16 00:00+00')`,
				`SELECT notify_apply_recipient_scope('notifications_2031m03a')`,
			},
			want: domain.PartitionHealth{Missing: []domain.PartitionGap{gap(n, mar)}}},
		{name: "one partition spanning several months covers each", provision: true,
			setup: []string{
				`DROP TABLE notification_deliveries_2031m03`,
				`DROP TABLE notification_deliveries_2031m04`,
				`CREATE TABLE notification_deliveries_2031h1 PARTITION OF notification_deliveries
                     FOR VALUES FROM ('2031-01-01 00:00+00') TO ('2031-07-01 00:00+00')`,
			}},
		{name: "an unbounded upper bound is read", provision: true,
			setup: []string{
				`DROP TABLE dead_letters_2031m03`,
				`DROP TABLE dead_letters_2031m04`,
				`CREATE TABLE dead_letters_from_2031 PARTITION OF dead_letters
                     FOR VALUES FROM ('2031-01-01 00:00+00') TO (MAXVALUE)`,
			}},
		{name: "an unbounded lower bound is read",
			at: time.Date(2020, 5, 10, 0, 0, 0, 0, time.UTC),
			setup: []string{
				`CREATE TABLE notifications_to_2021 PARTITION OF notifications
                     FOR VALUES FROM (MINVALUE) TO ('2021-01-01 00:00+00')`,
				`SELECT notify_apply_recipient_scope('notifications_to_2021')`,
				`CREATE TABLE notification_deliveries_to_2021 PARTITION OF notification_deliveries
                     FOR VALUES FROM (MINVALUE) TO ('2021-01-01 00:00+00')`,
				`CREATE TABLE dead_letters_to_2021 PARTITION OF dead_letters
                     FOR VALUES FROM (MINVALUE) TO ('2021-01-01 00:00+00')`,
			}},
		{name: "bounds are read in any session time zone", zone: "Pacific/Pago_Pago", provision: true},
		{name: "a default partition is not coverage", provision: true,
			setup: []string{
				`DROP TABLE dead_letters_2031m03`,
				`CREATE TABLE dead_letters_default PARTITION OF dead_letters DEFAULT`,
			},
			want: domain.PartitionHealth{Missing: []domain.PartitionGap{gap(dl, mar)}}},
		{name: "a detached partition is missing although its table remains", provision: true,
			setup: []string{`ALTER TABLE notification_deliveries DETACH PARTITION notification_deliveries_2031m04`},
			want:  domain.PartitionHealth{Missing: []domain.PartitionGap{gap(nd, apr)}}},
		{name: "a table with no partitions at all", provision: true,
			setup: []string{`
DO $$
DECLARE r record;
BEGIN
    FOR r IN SELECT inhrelid::regclass AS p FROM pg_inherits WHERE inhparent = 'dead_letters'::regclass
    LOOP
        EXECUTE format('ALTER TABLE dead_letters DETACH PARTITION %s', r.p);
    END LOOP;
END $$`},
			want: domain.PartitionHealth{Missing: []domain.PartitionGap{gap(dl, mar), gap(dl, apr)}}},
		{name: "the month is the UTC month", provision: true,
			// 22:00 on 31 March at UTC-5 is already April in UTC, so the months are April and May.
			at: time.Date(2031, 3, 31, 22, 0, 0, 0, time.FixedZone("UTC-5", -5*60*60)),
			want: domain.PartitionHealth{Missing: []domain.PartitionGap{
				gap(n, may), gap(nd, may), gap(dl, may)}}},

		// Unscoped.
		{name: "a partition created by hand without the scope", provision: true,
			setup: []string{`CREATE TABLE notifications_2031m05 PARTITION OF notifications
                                 FOR VALUES FROM ('2031-05-01 00:00+00') TO ('2031-06-01 00:00+00')`},
			want: domain.PartitionHealth{Unscoped: []string{"notifications_2031m05"}}},
		{name: "row-level security enabled but not forced", provision: true,
			setup: []string{`ALTER TABLE notifications_2031m03 NO FORCE ROW LEVEL SECURITY`},
			want:  domain.PartitionHealth{Unscoped: []string{"notifications_2031m03"}}},
		{name: "row-level security disabled", provision: true,
			setup: []string{`ALTER TABLE notifications_2031m04 DISABLE ROW LEVEL SECURITY`},
			want:  domain.PartitionHealth{Unscoped: []string{"notifications_2031m04"}}},
		{name: "the parent not forced", provision: true,
			setup: []string{`ALTER TABLE notifications NO FORCE ROW LEVEL SECURITY`},
			want:  domain.PartitionHealth{Unscoped: []string{"notifications"}}},
		{name: "the scope policy dropped from a bootstrap partition", provision: true,
			setup: []string{`DROP POLICY recipient_scope ON notifications_2026m09`},
			want:  domain.PartitionHealth{Unscoped: []string{"notifications_2026m09"}}},
		{name: "the scope policy recreated as it was", provision: true,
			setup: recreate("notifications_2031m03", `USING (`+scopePredicate+`)`)},
		{name: "a looser predicate", provision: true,
			setup: recreate("notifications_2031m03", `USING (realm = current_setting('notify.realm', true))`),
			want:  domain.PartitionHealth{Unscoped: []string{"notifications_2031m03"}}},
		{name: "a looser check on writes", provision: true,
			setup: recreate("notifications_2031m03", `USING (`+scopePredicate+`) WITH CHECK (true)`),
			want:  domain.PartitionHealth{Unscoped: []string{"notifications_2031m03"}}},
		{name: "the scope for reads only", provision: true,
			setup: recreate("notifications_2031m03", `FOR SELECT USING (`+scopePredicate+`)`),
			want:  domain.PartitionHealth{Unscoped: []string{"notifications_2031m03"}}},
		{name: "the scope restrictive, with no permissive policy", provision: true,
			setup: recreate("notifications_2031m03", `AS RESTRICTIVE USING (`+scopePredicate+`)`),
			want:  domain.PartitionHealth{Unscoped: []string{"notifications_2031m03"}}},
		{name: "another permissive policy widens the scope", provision: true,
			setup: []string{`CREATE POLICY peek ON notifications_2031m03 FOR SELECT USING (true)`},
			want:  domain.PartitionHealth{Unscoped: []string{"notifications_2031m03"}}},
		{name: "another restrictive policy only narrows it", provision: true,
			setup: []string{`CREATE POLICY narrow ON notifications_2031m03 AS RESTRICTIVE USING (topic <> 'hidden')`}},
		{name: "an unscoped partition of a partition", provision: true,
			setup: []string{
				`DROP TABLE notifications_2031m04`,
				`CREATE TABLE notifications_2031m04 PARTITION OF notifications
                     FOR VALUES FROM ('2031-04-01 00:00+00') TO ('2031-05-01 00:00+00')
                     PARTITION BY RANGE (created_at)`,
				`SELECT notify_apply_recipient_scope('notifications_2031m04')`,
				`CREATE TABLE notifications_2031m04_all PARTITION OF notifications_2031m04
                     FOR VALUES FROM ('2031-04-01 00:00+00') TO ('2031-05-01 00:00+00')`,
			},
			want: domain.PartitionHealth{Unscoped: []string{"notifications_2031m04_all"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			ctx := t.Context()
			if c.zone != "" {
				if _, err := e.db.Admin.Exec(ctx, `ALTER DATABASE `+pgx.Identifier{e.db.Name}.Sanitize()+
					` SET TimeZone = '`+c.zone+`'`); err != nil {
					t.Fatal(err)
				}
			}
			if c.provision {
				if _, err := e.s.EnsurePartitions(ctx, mar, 2); err != nil {
					t.Fatal(err)
				}
			}
			for _, sql := range c.setup {
				if _, err := e.db.Admin.Exec(ctx, sql); err != nil {
					t.Fatalf("setup %q: %v", sql, err)
				}
			}
			at := c.at
			if at.IsZero() {
				at = midMarch
			}

			got, err := e.s.CheckPartitions(ctx, at)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.EqualFunc(got.Missing, c.want.Missing, func(a, b domain.PartitionGap) bool {
				return a.Table == b.Table && a.Month.Equal(b.Month)
			}) {
				t.Errorf("missing %v, want %v", got.Missing, c.want.Missing)
			}
			if !slices.Equal(got.Unscoped, c.want.Unscoped) {
				t.Errorf("unscoped %v, want %v", got.Unscoped, c.want.Unscoped)
			}
		})
	}
}

// NR-022, D33. A completed broadcast is deleted once it finished before the cutoff, in
// batches; one that is pending is never deleted, however old, and one inside the window
// is kept. The broadcast table is not row-level scoped (it holds no recipient), so the
// store runs this as the worker, with no scope.
func TestExpireBroadcasts(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	insert := func(key, status string, completed *time.Time) {
		t.Helper()
		e.admin(`
INSERT INTO notification_broadcasts (realm, tenant_kind, tenant_id, idem_key, audience, request, status, completed_at, created_at)
VALUES ('aisat', 'workspace', 'w1', $1, 'everyone', '{}'::jsonb, $2, $3, '2026-06-01')`, key, status, completed)
	}
	old, recent := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	for _, k := range []string{"old-1", "old-2", "old-3"} {
		insert(k, "completed", &old)
	}
	insert("recent", "completed", &recent)
	insert("pending-and-ancient", "pending", nil)

	cutoff := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	batches := []struct{ batch, want int }{{2, 2}, {2, 1}, {2, 0}}
	for i, b := range batches {
		if n, err := e.s.ExpireBroadcasts(t.Context(), cutoff, b.batch); err != nil || n != b.want {
			t.Fatalf("batch %d: expired %d (%v), want %d", i+1, n, err, b.want)
		}
	}
	for key, want := range map[string]int{"old-1": 0, "old-2": 0, "old-3": 0, "recent": 1, "pending-and-ancient": 1} {
		if got := e.count(`SELECT count(*) FROM notification_broadcasts WHERE idem_key = $1`, key); got != want {
			t.Errorf("broadcast %s: %d rows, want %d", key, got, want)
		}
	}
}

// storeWith is a store over a pool of its own, whose every session has been given the
// settings, so a reading of "the connected role's" timeouts is of connections that have
// them. ALTER ROLE ... IN DATABASE reaches only this test's database, which every test
// has to itself, so no other test's role is touched.
func (e *env) storeWith(settings ...string) *Store {
	e.t.Helper()
	role := e.db.Owner.Config().ConnConfig.User
	for _, s := range settings {
		e.admin(`ALTER ROLE ` + pgx.Identifier{role}.Sanitize() + ` IN DATABASE ` + pgx.Identifier{e.db.Name}.Sanitize() + ` SET ` + s)
	}
	pool, err := pgxpool.NewWithConfig(e.t.Context(), e.db.Owner.Config().Copy())
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(pool.Close)
	return New(pool, WithClock(e.clock))
}

// NR-037, D38. DatabaseConditions reads what the queue depends on and judges none of it.
// The domain's AssessDatabase is the judge (TestAssessDatabase); this is that the
// reading is true.
func TestDatabaseConditionsReadsTheRolesTimeoutsAndTheServersSizing(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	plain := e.storeWith()
	c, err := plain.DatabaseConditions(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if c.IdleInTransactionTimeout != 0 || c.StatementTimeout != 0 {
		t.Fatalf("timeouts = %v and %v for a role with none, want off (zero)", c.IdleInTransactionTimeout, c.StatementTimeout)
	}
	if c.MaxWALSize < 32<<20 {
		t.Fatalf("MaxWALSize = %d, want the server's max_wal_size in bytes (at least its 32 MiB minimum)", c.MaxWALSize)
	}
	if d := time.Since(c.At); d < -time.Minute || d > time.Minute {
		t.Fatalf("At = %v, want the database's clock, which is now", c.At)
	}

	set := e.storeWith("idle_in_transaction_session_timeout = '30s'", "statement_timeout = '45s'")
	c, err = set.DatabaseConditions(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if c.IdleInTransactionTimeout != 30*time.Second || c.StatementTimeout != 45*time.Second {
		t.Fatalf("timeouts = %v and %v, want 30s and 45s as the role has them", c.IdleInTransactionTimeout, c.StatementTimeout)
	}
}

// A transaction pins vacuum when it holds a snapshot or has an id, and not merely by
// being open: a read-committed session idle in a transaction holds neither between
// statements, so it is not what the alarm is for, and reporting it would cry wolf at every
// host that keeps a connection in a transaction for a moment.
func TestDatabaseConditionsReportsTheAgeOfATransactionThatPinsVacuum(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		open func(*testing.T, *env) pgx.Tx
		pins bool
	}{
		{"no transaction is open", nil, false},
		{"a repeatable-read transaction holds its snapshot", func(t *testing.T, e *env) pgx.Tx {
			t.Helper()
			tx, err := e.db.Owner.BeginTx(t.Context(), pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec(t.Context(), `SELECT count(*) FROM notification_outbox`); err != nil {
				t.Fatal(err)
			}
			return tx
		}, true},
		{"a read-committed transaction with an id holds it", func(t *testing.T, e *env) pgx.Tx {
			t.Helper()
			tx, err := e.db.Owner.Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec(t.Context(), `SELECT txid_current()`); err != nil {
				t.Fatal(err)
			}
			return tx
		}, true},
		{"a read-committed transaction idle between statements pins nothing", func(t *testing.T, e *env) pgx.Tx {
			t.Helper()
			tx, err := e.db.Owner.Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec(t.Context(), `SELECT 1`); err != nil {
				t.Fatal(err)
			}
			return tx
		}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t).with(t)
			if tc.open != nil {
				tx := tc.open(t, e)
				defer func() { _ = tx.Rollback(t.Context()) }()
			}
			c, err := e.storeWith().DatabaseConditions(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if tc.pins && c.OldestTransactionAge <= 0 {
				t.Fatalf("OldestTransactionAge = %v with a transaction pinning vacuum, want its age", c.OldestTransactionAge)
			}
			if !tc.pins && c.OldestTransactionAge != 0 {
				t.Fatalf("OldestTransactionAge = %v, want zero: nothing here pins vacuum", c.OldestTransactionAge)
			}
		})
	}
}

// The role that cannot see other roles' sessions says so, so a clean reading is not
// taken for a clean database; one that can, does not.
func TestDatabaseConditionsSaysWhetherTheRoleCanSeeOtherSessions(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	owner, err := e.storeWith().DatabaseConditions(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if owner.SeesAllSessions {
		t.Fatal("the non-superuser owner is reported as seeing every session")
	}
	super, err := New(e.db.Admin, WithClock(e.clock)).DatabaseConditions(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !super.SeesAllSessions {
		t.Fatal("a superuser is reported as not seeing every session")
	}
}

// The WAL counter moves with writes, and a checkpoint the command asked for is counted
// as requested, not as timed: the preflight alarms on requested checkpoints.
func TestDatabaseConditionsCountsTheWALAndTheCheckpointsItsWritesCause(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	s := e.storeWith()
	before, err := s.DatabaseConditions(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if before.WALBytes <= 0 {
		t.Fatalf("WALBytes = %d, want the WAL the server has written", before.WALBytes)
	}
	for i := range 20 {
		e.notification(ident("w1", "u"+string(rune('a'+i))), "wal")
	}
	e.admin(`CHECKPOINT`)

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	for {
		after, err := s.DatabaseConditions(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if after.WALBytes < before.WALBytes {
			t.Fatalf("WALBytes went from %d to %d with writes between", before.WALBytes, after.WALBytes)
		}
		if after.CheckpointsRequested > before.CheckpointsRequested {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("CheckpointsRequested stayed at %d for 15s after a CHECKPOINT: the statistics read are not the checkpointer's requested count",
				before.CheckpointsRequested)
		case <-time.After(50 * time.Millisecond):
		}
	}
}
