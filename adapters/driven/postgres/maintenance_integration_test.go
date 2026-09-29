//go:build integration

package postgres

import (
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
