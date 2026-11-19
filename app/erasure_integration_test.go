//go:build integration

package app_test

import (
	"strings"
	"testing"
	"time"

	"github.com/truongpx396/intel-notification/app"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/notifytest"
)

// NS-012, NR-034, D35, end to end: a recipient the engine has served (accepted
// notifications, a delivery, a dead letter, a pending one, a digest window, preferences,
// a schedule and an address) is erased through Admin, and then
//
//   - no table the database catalog says can hold a recipient holds a row of theirs;
//   - the pre-check no longer answers "replay" for any key of theirs (D51), and still does
//     for another recipient's;
//   - the erasure is recorded, by a hash, with nothing that says who it was;
//   - a suppression recorded for one of their addresses still prevents delivery to it.
//
// The tables are listed from the catalog and not from the adapter's own list, so a table
// the adapter forgot, or one a later migration adds, fails here instead of keeping personal
// data. For the same reason the test insists on a fixture row in every table the catalog
// lists: a table with none would pass the scan for want of anything to find.

// recipientHolders lists every table in the database that has a recipient_id column,
// partitions excluded, whose rows are read through the parent.
func recipientHolders(t *testing.T, e *notifytest.Env) []string {
	t.Helper()
	rows, err := e.DB.Admin.Query(t.Context(), `
SELECT c.relname::text FROM pg_class c
 WHERE c.relnamespace = 'public'::regnamespace AND c.relkind IN ('r', 'p') AND NOT c.relispartition
   AND EXISTS (SELECT 1 FROM pg_attribute a
                WHERE a.attrelid = c.oid AND a.attname = 'recipient_id' AND NOT a.attisdropped)
 ORDER BY 1`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			t.Fatal(err)
		}
		tables = append(tables, table)
	}
	if err := rows.Err(); err != nil || len(tables) == 0 {
		t.Fatalf("listing the tables that hold recipients: %v", err)
	}
	return tables
}

// quoteIdent quotes a table name the catalog gave, so one with an odd character is read as
// itself.
func quoteIdent(name string) string { return `"` + strings.ReplaceAll(name, `"`, `""`) + `"` }

func rowsOf(t *testing.T, e *notifytest.Env, table string, id domain.Identity) int {
	t.Helper()
	return count(t, e, `SELECT count(*) FROM `+quoteIdent(table)+
		` WHERE realm = $1 AND tenant_id = $2 AND recipient_id = $3`, string(id.Realm), id.Tenant.ID, id.Recipient.ID)
}

func TestNS012AnErasedRecipientLeavesNothingAndTheirSuppressionStays(t *testing.T) {
	t.Parallel()
	e, id := emailEnv(t)
	e.AddChannel("email", domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupProvider, DedupWindow: time.Hour})
	other := domain.Identity{Realm: notifytest.Realm, Tenant: itTenant, Recipient: domain.Recipient{Kind: "user", ID: "u2"}}
	e.Addresses.Put(other, "email", domain.Address{Channel: "email", Value: "u2@example.com"})
	const address = "u1@example.com"

	// What the engine writes for the recipient as it serves them.
	send(t, e, id, "k1") // delivered: a history row
	if n, err := drainShard(t, e.Dispatcher, e, id); err != nil || n != 1 {
		t.Fatalf("Drain = %d, %v", n, err)
	}
	send(t, e, id, "k2") // refused for good: a dead letter and a history row
	e.Probe.FailNext(notifytest.Permanent)
	if n, err := drainShard(t, e.Dispatcher, e, id); err != nil || n != 1 {
		t.Fatalf("Drain = %d, %v", n, err)
	}
	pending, err := e.Notifier.Notify(t.Context(), domain.Notification{Tenant: id.Tenant, Recipient: id.Recipient,
		Topic: "t", IdemKey: "k3", Title: "Done", OccurredAt: notifytest.Epoch}) // left queued
	if err != nil || !pending.Applied {
		t.Fatalf("Notify(k3) = %+v, %v", pending, err)
	}
	if _, err := e.Digests.AppendDigest(t.Context(), domain.DigestMember{Identity: id, Topic: "t", Channel: "email",
		Shard: domain.ShardFor(id, e.Config.WithDefaults().Shards), NotificationID: pending.NotificationID}, time.Hour, 100); err != nil {
		t.Fatal(err)
	}
	send(t, e, other, "o1") // someone else's, which must come through untouched

	// What a host writes about them, which no flow here reaches.
	for _, sql := range []string{
		`INSERT INTO notification_preferences (realm, tenant_kind, tenant_id, recipient_kind, recipient_id, topic, channel, enabled)
         VALUES ('aisat', 'workspace', 'w1', 'user', 'u1', 't', 'sms', false)`,
		`INSERT INTO notification_schedules (realm, tenant_kind, tenant_id, recipient_kind, recipient_id)
         VALUES ('aisat', 'workspace', 'w1', 'user', 'u1')`,
		`INSERT INTO recipient_addresses (realm, tenant_kind, tenant_id, recipient_kind, recipient_id, channel, address_key, value)
         VALUES ('aisat', 'workspace', 'w1', 'user', 'u1', 'email', 'k-u1', '` + address + `')`,
	} {
		if _, err := e.DB.Admin.Exec(t.Context(), sql); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.Suppressions.Suppress(t.Context(), notifytest.Realm, "email", domain.SuppressionHash("email", address),
		domain.SuppressComplaint, time.Time{}); err != nil {
		t.Fatal(err)
	}

	// The control: every table the catalog lists has something of the recipient's in it,
	// so a clean scan afterwards is a table emptied and not a table never filled.
	holders := recipientHolders(t, e)
	before := map[string]int{}
	for _, table := range holders {
		before[table] = rowsOf(t, e, table, id)
		if before[table] == 0 {
			t.Errorf("%s can hold a recipient and the test put none of u1's in it: add a fixture, or the scan after erasure proves nothing about it", table)
		}
	}
	untouched := map[string]int{}
	for _, table := range holders {
		untouched[table] = rowsOf(t, e, table, other)
	}

	// The pre-check remembers what the Notifier accepted, under each key, for the recipient and
	// for the other one. The control: it does, so an entry gone afterwards was forgotten and not
	// never written (D51).
	for _, key := range []string{"k1", "k2", "k3"} {
		if _, hit, err := e.PreCheck.Seen(t.Context(), domain.PreCheckKey(id, key)); err != nil || !hit {
			t.Fatalf("the pre-check holds no entry for u1's %s (%v): the check below would prove nothing", key, err)
		}
	}
	if _, hit, err := e.PreCheck.Seen(t.Context(), domain.PreCheckKey(other, "o1")); err != nil || !hit {
		t.Fatalf("the pre-check holds no entry for u2's o1 (%v): the check below would prove nothing", err)
	}

	admin, err := app.NewAdmin(e.Config, app.Deps{Maintenance: e.Maintenance, Waker: e.Dispatcher, PreCheck: e.PreCheck})
	if err != nil {
		t.Fatal(err)
	}
	counts, err := admin.Erase(t.Context(), id)
	if err != nil {
		t.Fatalf("Erase = %v", err)
	}

	for _, table := range holders {
		if n := rowsOf(t, e, table, id); n != 0 {
			t.Errorf("%s: %d rows of the erased recipient remain (NS-012)", table, n)
		}
		if counts[table] != before[table] {
			t.Errorf("%s: Erase reported %d rows removed, and there were %d", table, counts[table], before[table])
		}
		if n := rowsOf(t, e, table, other); n != untouched[table] {
			t.Errorf("%s: erasing u1 changed u2's rows from %d to %d", table, untouched[table], n)
		}
	}

	// Nothing of theirs answers "replay" any more: an entry left for a key whose guard row was
	// erased would tell a producer that a notification which no longer exists was delivered
	// (D51). The other recipient's entry is untouched.
	for _, key := range []string{"k1", "k2", "k3"} {
		if nid, hit, _ := e.PreCheck.Seen(t.Context(), domain.PreCheckKey(id, key)); hit {
			t.Errorf("the pre-check still answers %q for the erased recipient's key %s", nid, key)
		}
	}
	if _, hit, _ := e.PreCheck.Seen(t.Context(), domain.PreCheckKey(other, "o1")); !hit {
		t.Error("erasing u1 forgot u2's entry in the pre-check")
	}

	// The erasure is on record by a hash, and the record says nothing of who.
	var record string
	if err := e.DB.Admin.QueryRow(t.Context(), `SELECT row_to_json(r)::text FROM erasure_requests r WHERE subject_hash = $1`,
		domain.SubjectHash(id)).Scan(&record); err != nil {
		t.Fatalf("the erasure was not recorded by the identity's hash: %v", err)
	}
	for _, who := range []string{"u1", "w1", address} {
		if strings.Contains(record, who) {
			t.Errorf("the erasure record %s names %q: it keeps no identity", record, who)
		}
	}

	// The suppression survived, so the address is still refused when the recipient is
	// notified again: delivery is suppressed and the channel is never asked.
	send(t, e, id, "k4")
	if n, err := drainShard(t, e.Dispatcher, e, id); err != nil || n < 1 { // u2's delivery may share the shard
		t.Fatalf("Drain after the erasure = %d, %v", n, err)
	}
	key := domain.DeliveryIdemKey(id, "k4", "", "email", domain.AddressKey("email", address))
	if e.Probe.Calls(key) != 0 || e.Probe.Sends(key) != 0 {
		t.Fatalf("the channel was asked %d times and sent %d to an address suppressed before the erasure, want neither (NR-034)",
			e.Probe.Calls(key), e.Probe.Sends(key))
	}
	if n := count(t, e, `SELECT count(*) FROM notification_deliveries WHERE outcome = 'suppressed' AND recipient_id = 'u1'`); n != 1 {
		t.Fatalf("%d suppressed deliveries recorded for the address after the erasure, want 1", n)
	}
}
