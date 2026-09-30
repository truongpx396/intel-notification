//go:build integration

package postgres

import (
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

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
		{name: "unbounded bounds are read", provision: true,
			setup: []string{
				`DROP TABLE dead_letters_2031m03`,
				`DROP TABLE dead_letters_2031m04`,
				`CREATE TABLE dead_letters_from_2031 PARTITION OF dead_letters
                     FOR VALUES FROM ('2031-01-01 00:00+00') TO (MAXVALUE)`,
				`CREATE TABLE notification_deliveries_before_2026m09 PARTITION OF notification_deliveries
                     FOR VALUES FROM (MINVALUE) TO ('2026-09-01 00:00+00')`,
			}},
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
