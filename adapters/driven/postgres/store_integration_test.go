//go:build integration

package postgres

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/truongpx396/intel-notification/domain"
)

// StoreContract (T020): the persist and read path of ports.Store, as the table
// owner against PostgreSQL 16. Every test below is a case of it. The three
// broadcast methods belong to T042 and have no case here.

// ------------------------------------------------------------------ fixtures --

// notif is a valid notification for id that carries every field the inbox row
// stores, so a round trip shows any that is lost.
func notif(id domain.Identity, idemKey string) domain.Notification {
	return domain.Notification{
		Tenant:     id.Tenant,
		Recipient:  id.Recipient,
		Topic:      "ingestion_complete",
		Priority:   domain.PriorityInfo,
		Data:       map[string]any{"doc": "d1", "pages": 12.0, "tags": []any{"a", "b"}},
		Title:      "Done",
		Body:       "Your upload finished.",
		Payload:    map[string]string{"doc_id": "d1"},
		Attributes: map[string]string{"trace_id": "t-1"},
		IdemKey:    idemKey,
		OccurredAt: fixtureDay.Add(-time.Minute),
	}
}

func immediate(ch domain.ChannelKind) domain.PlannedDelivery {
	return domain.PlannedDelivery{Channel: ch, Mode: domain.PlanImmediate}
}

// planOf is a plan that makes the notification inbox-visible, with the digest
// settings a digested channel needs.
func planOf(shard domain.Shard, ds ...domain.PlannedDelivery) domain.DeliveryPlan {
	return domain.DeliveryPlan{
		InboxVisible: true, Shard: shard, Deliveries: ds,
		DigestWindow: 15 * time.Minute, DigestMax: 10,
	}
}

// farFuture is a due time the database's clock will not reach during a test.
func farFuture() time.Time { return time.Now().Add(24 * time.Hour).Truncate(time.Microsecond) }

// fullPlan touches every table PersistAndEnqueue writes: an immediate and a
// deferred queue row, a digest append and a dropped-quota record.
func fullPlan(shard domain.Shard) domain.DeliveryPlan {
	return planOf(shard,
		immediate("email"),
		domain.PlannedDelivery{Channel: "sms", Mode: domain.PlanDeferred, NotBefore: farFuture(), Reason: "quiet_hours"},
		domain.PlannedDelivery{Channel: "push", Mode: domain.PlanDigest},
		domain.PlannedDelivery{Channel: "webhook", Mode: domain.PlanDropped, Reason: "quota"},
	)
}

// persistTables are the tables PersistAndEnqueue writes.
var persistTables = []string{"notifications", "notify_idem", "notification_outbox", "digest_buffer", "notification_deliveries"}

// persist applies a notification and fails the test unless it was applied.
func (e *env) persist(id domain.Identity, n domain.Notification, p domain.DeliveryPlan) domain.Receipt {
	e.t.Helper()
	r, err := e.s.PersistAndEnqueue(e.t.Context(), nil, id, n, p)
	if err != nil {
		e.t.Fatalf("PersistAndEnqueue: %v", err)
	}
	if !r.Applied {
		e.t.Fatalf("PersistAndEnqueue(%q) was reported as a replay", n.IdemKey)
	}
	return r
}

// assertNothingWritten fails unless every table PersistAndEnqueue writes is empty.
func (e *env) assertNothingWritten() {
	e.t.Helper()
	for _, table := range persistTables {
		if n := e.count("SELECT count(*) FROM " + table); n != 0 {
			e.t.Errorf("%s holds %d rows, want none", table, n)
		}
	}
}

// rowCounts is how many rows each table PersistAndEnqueue writes holds.
func (e *env) rowCounts() map[string]int {
	e.t.Helper()
	counts := make(map[string]int, len(persistTables))
	for _, table := range persistTables {
		counts[table] = e.count("SELECT count(*) FROM " + table)
	}
	return counts
}

// failInserts makes every insert into the tables fail inside the database, as a
// full disk or a lost connection would. A trigger is a real fault: it fires on
// whichever transaction does the write, so it needs no seam in the code under test
// and works the same for the store's own transaction and a caller's.
func (e *env) failInserts(tables ...string) {
	e.t.Helper()
	ctx := e.t.Context()
	if _, err := e.db.Admin.Exec(ctx, `
CREATE OR REPLACE FUNCTION test_inject_failure() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'injected failure on %', TG_TABLE_NAME; END $$`); err != nil {
		e.t.Fatal(err)
	}
	for _, table := range tables {
		if _, err := e.db.Admin.Exec(ctx, `CREATE TRIGGER test_inject_failure BEFORE INSERT ON `+table+
			` FOR EACH ROW EXECUTE FUNCTION test_inject_failure()`); err != nil {
			e.t.Fatal(err)
		}
	}
}

// ownerSees counts the rows of table the table owner can read once its scope is
// set to id, or, with a nil identity, with no scope at all.
func (e *env) ownerSees(table string, id *domain.Identity) int {
	e.t.Helper()
	var n int
	err := pgx.BeginFunc(e.t.Context(), e.db.Owner, func(tx pgx.Tx) error {
		if id != nil {
			if err := setScope(e.t.Context(), tx, *id); err != nil {
				return err
			}
		}
		return tx.QueryRow(e.t.Context(), "SELECT count(*) FROM "+table).Scan(&n)
	})
	if err != nil {
		e.t.Fatal(err)
	}
	return n
}

// claimAll claims everything due in one shard.
func (e *env) claimAll(shard domain.Shard) []domain.Claim {
	e.t.Helper()
	claims, err := e.s.Claim(e.t.Context(), shard, 50, lease)
	if err != nil {
		e.t.Fatal(err)
	}
	return claims
}

// claimFor claims everything due in shard and returns the claim on channel ch.
func (e *env) claimFor(shard domain.Shard, ch domain.ChannelKind) domain.Claim {
	e.t.Helper()
	for _, c := range e.claimAll(shard) {
		if c.Entry.Channel == ch {
			return c
		}
	}
	e.t.Fatalf("no due delivery on %s in shard %d", ch, shard)
	return domain.Claim{}
}

// ----------------------------------------------------------- what is written --

type inboxRow struct {
	Topic, Priority, Title, Body, IdemKey                   string
	Data                                                    map[string]any
	Payload, Attributes                                     map[string]string
	InboxVisible                                            bool
	VisibleFrom, OccurredAt, CreatedAt                      time.Time
	CanceledAt                                              *time.Time
	Realm, TenantKind, TenantID, RecipientKind, RecipientID string
}

func (e *env) inboxRow(nid string) inboxRow {
	e.t.Helper()
	var r inboxRow
	err := e.db.Admin.QueryRow(e.t.Context(), `
SELECT topic, priority, title, body, idem_key, data, payload, attributes, inbox_visible,
       visible_from, occurred_at, created_at, canceled_at,
       realm, tenant_kind, tenant_id, recipient_kind, recipient_id
  FROM notifications WHERE id = $1::uuid`, nid).
		Scan(&r.Topic, &r.Priority, &r.Title, &r.Body, &r.IdemKey, &r.Data, &r.Payload, &r.Attributes,
			&r.InboxVisible, &r.VisibleFrom, &r.OccurredAt, &r.CreatedAt, &r.CanceledAt,
			&r.Realm, &r.TenantKind, &r.TenantID, &r.RecipientKind, &r.RecipientID)
	if err != nil {
		e.t.Fatalf("inbox row %s: %v", nid, err)
	}
	return r
}

type queueRow struct {
	Priority, AddressKey  string
	Fallback              []string
	Shard                 int16
	DeliverBefore         *time.Time
	NextAttemptAt         time.Time
	Due                   bool
	QuietHoursOverride    bool
	NotificationCreatedAt time.Time
	Identity              domain.Identity
}

// queueRows returns a notification's pending deliveries by channel.
func (e *env) queueRows(nid string) map[string]queueRow {
	e.t.Helper()
	rows, err := e.db.Admin.Query(e.t.Context(), `
SELECT channel, priority, address_key, fallback, shard, deliver_before, next_attempt_at,
       next_attempt_at <= now(), quiet_hours_override, notification_created_at,
       realm, tenant_kind, tenant_id, recipient_kind, recipient_id
  FROM notification_outbox WHERE notification_id = $1::uuid`, nid)
	if err != nil {
		e.t.Fatal(err)
	}
	out := map[string]queueRow{}
	for rows.Next() {
		var ch, realm string
		var r queueRow
		if err := rows.Scan(&ch, &r.Priority, &r.AddressKey, &r.Fallback, &r.Shard, &r.DeliverBefore,
			&r.NextAttemptAt, &r.Due, &r.QuietHoursOverride, &r.NotificationCreatedAt, &realm,
			&r.Identity.Tenant.Kind, &r.Identity.Tenant.ID, &r.Identity.Recipient.Kind,
			&r.Identity.Recipient.ID); err != nil {
			e.t.Fatal(err)
		}
		r.Identity.Realm = domain.Realm(realm)
		out[ch] = r
	}
	if err := rows.Err(); err != nil {
		e.t.Fatal(err)
	}
	return out
}

// ------------------------------------------------------------ the happy path --

// NR-001, NR-003, D26, D29. One accepted notification is a guard, an inbox row, a
// queue row per immediate or deferred delivery, a digest append per digested
// channel and a record per dropped one, and the receipt says so.
func TestPersistAndEnqueue(t *testing.T) {
	t.Parallel()

	t.Run("writes the guard, the inbox row and one queue row per immediate or deferred delivery", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t)
		id := ident("w1", "u1")
		n := notif(id, "invoice:42:overdue")
		n.Priority = domain.PriorityWarning
		n.DeliverBefore = fixtureDay.Add(48 * time.Hour)
		later := farFuture()
		p := planOf(5,
			immediate("in_app"),
			domain.PlannedDelivery{Channel: "push", Mode: domain.PlanImmediate,
				Fallback: []domain.ChannelKind{"sms", "email"}},
			domain.PlannedDelivery{Channel: "slack", Mode: domain.PlanDeferred, NotBefore: later, Reason: "quiet_hours"},
			domain.PlannedDelivery{Channel: "pager", Mode: domain.PlanImmediate, QuietHoursOverride: true},
		)

		r := e.persist(id, n, p)

		if r.NotificationID == "" || r.IdemKey != n.IdemKey || !r.InboxVisible || !reflect.DeepEqual(r.Deliveries, p.Deliveries) {
			t.Errorf("receipt = %+v, want the new id, the key, visibility, and the plan it applied", r)
		}

		in := e.inboxRow(r.NotificationID)
		if in.Topic != "ingestion_complete" || in.Priority != "warning" || in.Title != "Done" ||
			in.Body != "Your upload finished." || in.IdemKey != n.IdemKey || !in.InboxVisible {
			t.Errorf("inbox row = %+v", in)
		}
		if !reflect.DeepEqual(in.Data, n.Data) || !reflect.DeepEqual(in.Payload, n.Payload) ||
			!reflect.DeepEqual(in.Attributes, n.Attributes) {
			t.Errorf("inbox row data/payload/attributes = %v / %v / %v", in.Data, in.Payload, in.Attributes)
		}
		if !in.CreatedAt.Equal(fixtureDay) || !in.VisibleFrom.Equal(fixtureDay) || !in.OccurredAt.Equal(n.OccurredAt) {
			t.Errorf("created %v, visible from %v, occurred %v: want the store clock for the first two and the producer's time for the third",
				in.CreatedAt, in.VisibleFrom, in.OccurredAt)
		}
		if in.Realm != "aisat" || in.TenantKind != "workspace" || in.TenantID != "w1" ||
			in.RecipientKind != "user" || in.RecipientID != "u1" {
			t.Errorf("inbox row identity = %+v", in)
		}

		if got := e.count(`SELECT count(*) FROM notify_idem
                            WHERE realm = 'aisat' AND tenant_id = 'w1' AND recipient_id = 'u1'
                              AND idem_key = $1 AND notification_id = $2::uuid
                              AND notification_created_at = $3`, n.IdemKey, r.NotificationID, fixtureDay); got != 1 {
			t.Errorf("guard rows naming the notification and its partition key: %d, want 1", got)
		}

		q := e.queueRows(r.NotificationID)
		if len(q) != 4 {
			t.Fatalf("%d queue rows, want one per immediate or deferred delivery: %+v", len(q), q)
		}
		for ch, row := range q {
			if row.Shard != 5 || row.Priority != "warning" || row.AddressKey != "" ||
				!row.NotificationCreatedAt.Equal(fixtureDay) || row.Identity != id {
				t.Errorf("%s row = %+v: want shard 5, the notification's priority, unbound, its partition key and identity", ch, row)
			}
			if row.DeliverBefore == nil || !row.DeliverBefore.Equal(n.DeliverBefore) {
				t.Errorf("%s deliver_before = %v, want %v", ch, row.DeliverBefore, n.DeliverBefore)
			}
		}
		if !reflect.DeepEqual(q["push"].Fallback, []string{"sms", "email"}) || len(q["in_app"].Fallback) != 0 {
			t.Errorf("fallback chains: push %v, in_app %v", q["push"].Fallback, q["in_app"].Fallback)
		}
		if !q["in_app"].Due || !q["push"].Due {
			t.Error("an immediate delivery is not due at once")
		}
		if q["slack"].Due || !q["slack"].NextAttemptAt.Equal(later) {
			t.Errorf("deferred delivery due=%v at %v, want not due, at %v", q["slack"].Due, q["slack"].NextAttemptAt, later)
		}
		if !q["pager"].QuietHoursOverride || q["push"].QuietHoursOverride {
			t.Error("quiet_hours_override was not carried to exactly the delivery that has it")
		}
	})

	t.Run("an empty plan still writes the durable record, hidden from the inbox (NR-001, D26)", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t)
		id := ident("w1", "u1")
		r := e.persist(id, notif(id, "k"), domain.DeliveryPlan{Shard: 1})
		if r.InboxVisible || len(r.Deliveries) != 0 {
			t.Errorf("receipt = %+v", r)
		}
		if in := e.inboxRow(r.NotificationID); in.InboxVisible {
			t.Error("the row is visible although the plan made it invisible")
		}
		if e.count(`SELECT count(*) FROM notify_idem`) != 1 || e.count(`SELECT count(*) FROM notification_outbox`) != 0 {
			t.Error("want the guard and no queue row")
		}
	})

	t.Run("a scheduled notification is invisible and not due until its send time (D29)", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t)
		id := ident("w1", "u1")
		send := farFuture()
		n := notif(id, "meeting:m1:start")
		n.DeliverAfter = send
		p := planOf(2, domain.PlannedDelivery{Channel: "push", Mode: domain.PlanImmediate, NotBefore: send})
		p.VisibleFrom = send
		r := e.persist(id, n, p)
		if in := e.inboxRow(r.NotificationID); !in.VisibleFrom.Equal(send) {
			t.Errorf("visible_from = %v, want %v", in.VisibleFrom, send)
		}
		if q := e.queueRows(r.NotificationID)["push"]; q.Due || !q.NextAttemptAt.Equal(send) {
			t.Errorf("queue row due=%v at %v, want not due, at %v", q.Due, q.NextAttemptAt, send)
		}
	})

	t.Run("a digested channel is folded into the recipient's open window and gets no queue row (D4)", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t)
		id := ident("w1", "u1")
		p := planOf(7, immediate("in_app"), domain.PlannedDelivery{Channel: "email", Mode: domain.PlanDigest})
		first := e.persist(id, notif(id, "k1"), p)
		second := e.persist(id, notif(id, "k2"), p)

		if q := e.queueRows(first.NotificationID); len(q) != 1 {
			t.Errorf("queue rows %v: the digested channel must not have one", q)
		}
		var windows, members int
		var ids []string
		var shard int16
		var channel string
		if err := e.db.Admin.QueryRow(t.Context(), `
SELECT count(*) OVER (), member_count, member_ids::text[], shard, channel FROM digest_buffer`).
			Scan(&windows, &members, &ids, &shard, &channel); err != nil {
			t.Fatal(err)
		}
		if windows != 1 || members != 2 || shard != 7 || channel != "email" ||
			len(ids) != 2 || ids[0] != first.NotificationID || ids[1] != second.NotificationID {
			t.Errorf("windows=%d members=%d ids=%v shard=%d channel=%s, want one email window in shard 7 holding both, oldest first",
				windows, members, ids, shard, channel)
		}
	})

	t.Run("a dropped channel is recorded as dropped_quota and nothing is queued for it (D5)", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t)
		id := ident("w1", "u1")
		r := e.persist(id, notif(id, "k"), planOf(3,
			immediate("in_app"), domain.PlannedDelivery{Channel: "sms", Mode: domain.PlanDropped, Reason: "quota"}))
		if q := e.queueRows(r.NotificationID); len(q) != 1 {
			t.Errorf("queue rows %v: a dropped channel must not be queued", q)
		}
		if n := e.count(`SELECT count(*) FROM notification_deliveries
                          WHERE notification_id = $1::uuid AND channel = 'sms' AND outcome = 'dropped_quota'
                            AND detail = 'quota' AND attempts = 0 AND address_key = ''
                            AND tenant_id = 'w1' AND recipient_id = 'u1'`, r.NotificationID); n != 1 {
			t.Errorf("dropped_quota records for sms: %d, want 1", n)
		}
	})
}

// NR-003 rejects what cannot be persisted truthfully. Each is refused before any
// write, and none of them leaves a row behind.
func TestPersistAndEnqueueRejects(t *testing.T) {
	t.Parallel()
	id := ident("w1", "u1")
	cases := []struct {
		name   string
		mutate func(*domain.Identity, *domain.Notification, *domain.DeliveryPlan)
		is     error
		why    string
	}{
		{"an identity whose tenant is not the notification's", func(i *domain.Identity, _ *domain.Notification, _ *domain.DeliveryPlan) {
			i.Tenant.ID = "w2"
		}, nil, "the identity is authoritative, and a mismatch would file the row under the wrong tenant (NR-009)"},
		{"an identity whose recipient is not the notification's", func(i *domain.Identity, _ *domain.Notification, _ *domain.DeliveryPlan) {
			i.Recipient.ID = "u2"
		}, nil, ""},
		{"an identity with no realm", func(i *domain.Identity, _ *domain.Notification, _ *domain.DeliveryPlan) {
			i.Realm = ""
		}, domain.ErrEmptyIdentity, "an empty component would equal an unset scope (D17)"},
		{"no idempotency key", func(_ *domain.Identity, n *domain.Notification, _ *domain.DeliveryPlan) {
			n.IdemKey = ""
		}, domain.ErrMissingIdem, ""},
		{"a priority the notifier did not resolve", func(_ *domain.Identity, n *domain.Notification, _ *domain.DeliveryPlan) {
			n.Priority = ""
		}, nil, "the store cannot know the topic's default, and storing info for a critical topic would quietly defer it"},
		{"a channel planned twice", func(_ *domain.Identity, _ *domain.Notification, p *domain.DeliveryPlan) {
			p.Deliveries = append(p.Deliveries, immediate("email"))
		}, nil, "one pending delivery per notification and channel"},
		{"a deferred delivery with no time", func(_ *domain.Identity, _ *domain.Notification, p *domain.DeliveryPlan) {
			p.Deliveries = append(p.Deliveries, domain.PlannedDelivery{Channel: "slack", Mode: domain.PlanDeferred})
		}, nil, "it would be due at once, which is the opposite of deferred"},
		{"a delivery with no mode", func(_ *domain.Identity, _ *domain.Notification, p *domain.DeliveryPlan) {
			p.Deliveries = append(p.Deliveries, domain.PlannedDelivery{Channel: "slack"})
		}, nil, ""},
		{"a digest with no window", func(_ *domain.Identity, _ *domain.Notification, p *domain.DeliveryPlan) {
			p.Deliveries = append(p.Deliveries, domain.PlannedDelivery{Channel: "slack", Mode: domain.PlanDigest})
			p.DigestWindow = 0
		}, nil, ""},
		{"a digest with no member limit", func(_ *domain.Identity, _ *domain.Notification, p *domain.DeliveryPlan) {
			p.Deliveries = append(p.Deliveries, domain.PlannedDelivery{Channel: "slack", Mode: domain.PlanDigest})
			p.DigestMax = 0
		}, nil, ""},
		{"a negative shard", func(_ *domain.Identity, _ *domain.Notification, p *domain.DeliveryPlan) {
			p.Shard = -1
		}, nil, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			// A control first: the same call, unchanged, is accepted. Without it a
			// store that accepts nothing would pass every case below.
			control := ident("w1", "control")
			e.persist(control, notif(control, "k"), planOf(1, immediate("email")))
			before := e.rowCounts()

			i, n, p := id, notif(id, "k"), planOf(1, immediate("email"))
			tc.mutate(&i, &n, &p)
			_, err := e.s.PersistAndEnqueue(t.Context(), nil, i, n, p)
			if err == nil {
				t.Fatalf("accepted: %s", tc.why)
			}
			if tc.is != nil && !errors.Is(err, tc.is) {
				t.Errorf("error %v does not wrap %v", err, tc.is)
			}
			if after := e.rowCounts(); !reflect.DeepEqual(before, after) {
				t.Errorf("a refused call changed the tables: %v -> %v", before, after)
			}
		})
	}
}

// ------------------------------------------------------------------- replays --

// NR-002, NS-002, D18. A replay is reported, not an error, and writes nothing.
func TestPersistAndEnqueueReplay(t *testing.T) {
	t.Parallel()

	t.Run("a replay is Applied == false, names the first notification and writes nothing", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t)
		id := ident("w1", "u1")
		first := e.persist(id, notif(id, "invoice:42:overdue"), fullPlan(4))

		again := notif(id, "invoice:42:overdue")
		again.Title, again.Data = "A different title", map[string]any{"changed": true}
		r, err := e.s.PersistAndEnqueue(t.Context(), nil, id, again, fullPlan(4))
		if err != nil {
			t.Fatalf("a replay is not an error: %v", err)
		}
		if r.Applied || r.NotificationID != first.NotificationID || r.IdemKey != "invoice:42:overdue" ||
			!r.InboxVisible || len(r.Deliveries) != 0 {
			t.Errorf("replay receipt = %+v, want the first notification's id, visible, with no deliveries and Applied false", r)
		}
		for table, want := range map[string]int{"notifications": 1, "notify_idem": 1, "notification_outbox": 2,
			"digest_buffer": 1, "notification_deliveries": 1} {
			if got := e.count("SELECT count(*) FROM " + table); got != want {
				t.Errorf("%s holds %d rows after a replay, want %d", table, got, want)
			}
		}
		if in := e.inboxRow(first.NotificationID); in.Title != "Done" {
			t.Errorf("a replay rewrote the inbox row: title %q", in.Title)
		}
	})

	t.Run("the guard is written first: a replay attempts no other write", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t)
		id := ident("w1", "u1")
		e.persist(id, notif(id, "k"), fullPlan(4))
		// Every table but the guard now refuses inserts. A store that wrote the
		// inbox row before the guard would fail here instead of reporting a replay.
		e.failInserts("notifications", "notification_outbox", "digest_buffer", "notification_deliveries")
		r, err := e.s.PersistAndEnqueue(t.Context(), nil, id, notif(id, "k"), fullPlan(4))
		if err != nil || r.Applied {
			t.Fatalf("applied=%v err=%v: a replay reached a table after the guard", r.Applied, err)
		}
	})

	t.Run("concurrent replays apply once (NS-002)", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t)
		id := ident("w1", "u1")
		const callers = 16
		var wg sync.WaitGroup
		applied := make(chan bool, callers)
		ids := make(chan string, callers)
		errs := make(chan error, callers)
		for range callers {
			wg.Go(func() {
				ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
				defer cancel()
				r, err := e.s.PersistAndEnqueue(ctx, nil, id, notif(id, "k"), fullPlan(4))
				if err != nil {
					errs <- err
					return
				}
				applied <- r.Applied
				ids <- r.NotificationID
			})
		}
		wg.Wait()
		close(errs)
		close(applied)
		close(ids)
		for err := range errs {
			t.Fatal(err)
		}
		wins := 0
		for a := range applied {
			if a {
				wins++
			}
		}
		if wins != 1 {
			t.Errorf("%d of %d concurrent callers applied the notification, want exactly 1", wins, callers)
		}
		seen := map[string]bool{}
		for id := range ids {
			seen[id] = true
		}
		if len(seen) != 1 {
			t.Errorf("callers were told %d different notification ids, want 1", len(seen))
		}
		for table, want := range map[string]int{"notifications": 1, "notify_idem": 1, "notification_outbox": 2,
			"digest_buffer": 1, "notification_deliveries": 1} {
			if got := e.count("SELECT count(*) FROM " + table); got != want {
				t.Errorf("%s holds %d rows, want %d", table, got, want)
			}
		}
	})
}

// NR-010, NS-001, D1, D18. The idempotency key space is (realm, tenant,
// recipient, key): the same recipient id and the same key in another tenant, or
// another realm, is another notification, and the table owner sees each only
// through its own scope.
func TestStoreKeepsIdentitiesApart(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	other := domain.Identity{Realm: "other", Tenant: ident("w1", "u1").Tenant, Recipient: ident("w1", "u1").Recipient}
	identities := []domain.Identity{ident("w1", "u1"), ident("w2", "u1"), other}

	receipts := map[domain.Identity]domain.Receipt{}
	for _, id := range identities {
		n := notif(id, "weekly_digest:W39")
		n.Title = string(id.Realm) + "/" + id.Tenant.ID // tells the three apart on a read
		receipts[id] = e.persist(id, n, planOf(3, immediate("email")))
	}

	for _, id := range identities {
		if got := e.ownerSees("notifications", &id); got != 1 {
			t.Errorf("%+v: the owner, scoped to it, sees %d notifications, want only its own", id, got)
		}
		st, err := e.s.Status(t.Context(), id, "weekly_digest:W39")
		if err != nil || !st.Found || st.NotificationID != receipts[id].NotificationID {
			t.Errorf("%+v: status = %+v (%v), want its own notification %s", id, st, err, receipts[id].NotificationID)
		}
	}
	if got := e.ownerSees("notifications", nil); got != 0 {
		t.Errorf("the table owner with no scope sees %d notifications; forced row-level security must hold for the owner", got)
	}

	claims := e.claimAll(3)
	if len(claims) != 3 {
		t.Fatalf("%d claims, want one per identity", len(claims))
	}
	for _, c := range claims {
		n, _, canceled, err := e.s.Load(t.Context(), c)
		want := string(c.Entry.Identity.Realm) + "/" + c.Entry.Identity.Tenant.ID
		if err != nil || canceled || n == nil || n.Title != want {
			t.Errorf("claim of %+v loaded %+v (canceled=%v, %v), want the notification titled %q", c.Entry.Identity, n, canceled, err, want)
		}
		// The same claim presented under another tenant's identity reads nothing.
		forged := c
		forged.Entry.Identity.Tenant.ID = "w3"
		if n, _, canceled, err := e.s.Load(t.Context(), forged); err != nil || n != nil || !canceled {
			t.Errorf("a claim forged into another tenant loaded %+v (canceled=%v, %v), want nothing", n, canceled, err)
		}
	}
}

// ------------------------------------------------------------- atomicity -----

// NR-003, invariant 4. The guard, the inbox row, the queue rows, the digest append
// and the drop record commit together or not at all: a failure at ANY of the five
// writes leaves none of the others behind, whether the store opened the
// transaction or the caller did.
func TestPersistAndEnqueueIsAtomic(t *testing.T) {
	t.Parallel()
	id := ident("w1", "u1")
	for _, table := range persistTables {
		for _, owner := range []string{"its own transaction", "the caller's transaction"} {
			t.Run(table+" fails in "+owner, func(t *testing.T) {
				t.Parallel()
				e := newEnv(t)
				e.failInserts(table)
				var err error
				if owner == "its own transaction" {
					_, err = e.s.PersistAndEnqueue(t.Context(), nil, id, notif(id, "k"), fullPlan(4))
				} else {
					tx, berr := e.db.Owner.Begin(t.Context())
					if berr != nil {
						t.Fatal(berr)
					}
					_, err = e.s.PersistAndEnqueue(t.Context(), tx, id, notif(id, "k"), fullPlan(4))
					_ = tx.Rollback(context.Background()) // what the host does with an error
				}
				if err == nil || !strings.Contains(err.Error(), "injected failure on "+table) {
					t.Fatalf("err = %v, want the injected failure on %s", err, table)
				}
				e.assertNothingWritten()
			})
		}
	}
}

// NR-027, D28. NotifyTx joins the host's transaction: the store neither commits
// it nor rolls it back, so the notification commits with the host's change or not
// at all.
func TestPersistAndEnqueueInCallersTransaction(t *testing.T) {
	t.Parallel()
	id := ident("w1", "u1")

	t.Run("rolled back by the host leaves nothing", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t)
		tx, err := e.db.Owner.Begin(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(context.Background()) }() // a failed assertion must not strand the connection
		if _, err := e.s.PersistAndEnqueue(t.Context(), tx, id, notif(id, "k"), fullPlan(4)); err != nil {
			t.Fatal(err)
		}
		if err := tx.Rollback(t.Context()); err != nil {
			t.Fatalf("the host could not roll back its own transaction: %v", err)
		}
		e.assertNothingWritten()
	})

	t.Run("committed by the host leaves work a claim can drive", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t)
		tx, err := e.db.Owner.Begin(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(context.Background()) }()
		r, err := e.s.PersistAndEnqueue(t.Context(), tx, id, notif(id, "k"), fullPlan(4))
		if err != nil || !r.Applied {
			t.Fatalf("applied=%v err=%v", r.Applied, err)
		}
		if err := tx.Commit(t.Context()); err != nil {
			t.Fatalf("the host could not commit its own transaction: %v", err)
		}
		c := e.claimFor(4, "email")
		if c.Entry.NotificationID != r.NotificationID || c.Entry.Identity != id {
			t.Errorf("claimed %+v, want the committed notification %s", c.Entry, r.NotificationID)
		}
	})

	t.Run("the store neither commits nor rolls back what it was given", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t)
		tx, err := e.db.Owner.Begin(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(context.Background()) }()
		if _, err := e.s.PersistAndEnqueue(t.Context(), tx, id, notif(id, "k"), fullPlan(4)); err != nil {
			t.Fatal(err)
		}
		e.assertNothingWritten() // not yet visible to anyone else: the host has not committed

		// Still the host's, and still open: it can read its own writes, and commit.
		var inTx int
		if err := tx.QueryRow(t.Context(), `SELECT count(*) FROM notification_outbox`).Scan(&inTx); err != nil || inTx != 2 {
			t.Fatalf("the host reads %d queue rows in its own transaction (%v), want 2", inTx, err)
		}
		if err := tx.Commit(t.Context()); err != nil {
			t.Fatalf("the host's commit: %v", err)
		}
		if got := e.count(`SELECT count(*) FROM notification_outbox`); got != 2 {
			t.Errorf("%d queue rows after the host's commit, want 2", got)
		}
	})

	t.Run("a transaction that is not a pgx.Tx is refused", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t)
		e.persist(ident("w1", "control"), notif(ident("w1", "control"), "k"), planOf(4, immediate("email")))
		before := e.rowCounts()
		if _, err := e.s.PersistAndEnqueue(t.Context(), "not a transaction", id, notif(id, "k"), fullPlan(4)); err == nil {
			t.Fatal("accepted a Tx the store cannot use")
		}
		if after := e.rowCounts(); !reflect.DeepEqual(before, after) {
			t.Errorf("a refused call changed the tables: %v -> %v", before, after)
		}
	})
}

// NS-003, NR-035. Acceptance is the commit: once PersistAndEnqueue has returned,
// everything a claim needs is in the database, so a process that dies the next
// instant leaves work a fresh process can drive end to end. A new Store over the
// same database stands in for the restart: nothing but the database remembers.
func TestCrashAfterCommitLeavesDrivableWork(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	id := ident("w1", "u1")
	r := e.persist(id, notif(id, "invoice:42:overdue"), fullPlan(4))

	restarted := New(e.db.Owner)
	claims, err := restarted.Claim(t.Context(), 4, 10, lease)
	if err != nil || len(claims) != 1 {
		t.Fatalf("a restarted process claimed %d deliveries (%v), want the one immediate email", len(claims), err)
	}
	c := claims[0]
	if c.Entry.NotificationID != r.NotificationID || c.Entry.Channel != "email" || c.Entry.Identity != id || c.Entry.Attempts != 1 {
		t.Fatalf("claim = %+v", c.Entry)
	}
	n, digest, canceled, err := restarted.Load(t.Context(), c)
	if err != nil || canceled || digest != nil || n == nil || n.IdemKey != "invoice:42:overdue" || n.Title != "Done" {
		t.Fatalf("load = %+v, %v, canceled=%v, %v: a claim must be able to read what it delivers", n, digest, canceled, err)
	}
	f, _ := domain.NewFinish(domain.OutcomeDelivered, "prov-1", "")
	if ok, err := restarted.Finish(t.Context(), c, f); err != nil || !ok {
		t.Fatalf("finish: ok=%v err=%v", ok, err)
	}
}

// ---------------------------------------------------------------------- Load --

// D17. Load reads what a claim delivers, under the claim's own identity.
func TestLoad(t *testing.T) {
	t.Parallel()
	id := ident("w1", "u1")

	t.Run("returns the notification as it was persisted", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t)
		want := notif(id, "k")
		want.Priority = domain.PriorityCritical
		e.persist(id, want, planOf(6, immediate("email")))
		n, digest, canceled, err := e.s.Load(t.Context(), e.claimFor(6, "email"))
		if err != nil || canceled || digest != nil || n == nil {
			t.Fatalf("n=%+v digest=%v canceled=%v err=%v", n, digest, canceled, err)
		}
		if !n.OccurredAt.Equal(want.OccurredAt) {
			t.Errorf("occurred at %v, want %v", n.OccurredAt, want.OccurredAt)
		}
		n.OccurredAt, want.OccurredAt = time.Time{}, time.Time{}
		if !reflect.DeepEqual(*n, want) {
			t.Errorf("loaded %+v\nwant   %+v", *n, want)
		}
	})

	t.Run("a notification with no data, payload or attributes loads with none", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t)
		bare := domain.Notification{Tenant: id.Tenant, Recipient: id.Recipient, Topic: "t",
			Priority: domain.PriorityInfo, IdemKey: "k"}
		e.persist(id, bare, planOf(6, immediate("email")))
		n, _, _, err := e.s.Load(t.Context(), e.claimFor(6, "email"))
		if err != nil || n == nil || n.Data != nil || n.Payload != nil || n.Attributes != nil {
			t.Fatalf("loaded %+v (%v): empty maps should come back nil", n, err)
		}
	})

	t.Run("a canceled notification is reported as canceled, with nothing to send (D29)", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t)
		e.persist(id, notif(id, "k"), planOf(6, immediate("email"), immediate("sms")))
		held := e.claimFor(6, "email") // a worker is mid-send when the cancel arrives
		if r, err := e.s.Cancel(t.Context(), id, "k"); err != nil || r.InFlight != 1 {
			t.Fatalf("cancel: %+v %v", r, err)
		}
		n, digest, canceled, err := e.s.Load(t.Context(), held)
		if err != nil || !canceled || n != nil || digest != nil {
			t.Fatalf("n=%+v digest=%v canceled=%v err=%v, want canceled with nothing to send", n, digest, canceled, err)
		}
	})

	t.Run("a notification that no longer exists is reported as canceled, not as an error", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t)
		e.persist(id, notif(id, "k"), planOf(6, immediate("email")))
		held := e.claimFor(6, "email")
		if _, err := e.s.Erase(t.Context(), id); err != nil { // the recipient is erased under the claim
			t.Fatal(err)
		}
		n, _, canceled, err := e.s.Load(t.Context(), held)
		if err != nil || !canceled || n != nil {
			t.Fatalf("n=%+v canceled=%v err=%v: an erased recipient's claim has nothing left to send", n, canceled, err)
		}
	})

	// NR-014, D4. A digest delivery loads the window's members, oldest first,
	// leaving out any that were canceled, and is itself canceled once none is left.
	t.Run("a digest loads its members, omits the canceled ones, and is canceled when none is left (D4)", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t)
		p := planOf(9, domain.PlannedDelivery{Channel: "email", Mode: domain.PlanDigest})
		var receipts []domain.Receipt
		for _, k := range []string{"k1", "k2", "k3"} {
			n := notif(id, k)
			n.Title = "title " + k
			receipts = append(receipts, e.persist(id, n, p))
		}
		var window string
		if err := e.db.Admin.QueryRow(t.Context(), `SELECT id::text FROM digest_buffer`).Scan(&window); err != nil {
			t.Fatal(err)
		}
		if queued, err := e.s.FlushDigest(t.Context(), window); err != nil || !queued {
			t.Fatalf("flush: queued=%v err=%v", queued, err)
		}
		c := e.claimFor(9, "email")
		if c.Entry.DigestID != window || c.Entry.NotificationID != "" {
			t.Fatalf("claim = %+v, want the digest's delivery", c.Entry)
		}

		titles := func() []string {
			t.Helper()
			n, members, canceled, err := e.s.Load(t.Context(), c)
			if err != nil || n != nil {
				t.Fatalf("n=%+v canceled=%v err=%v: a digest delivery has members, not a notification", n, canceled, err)
			}
			var got []string
			for _, m := range members {
				got = append(got, m.Title)
			}
			if canceled != (len(members) == 0) {
				t.Fatalf("canceled=%v with %d members left", canceled, len(members))
			}
			return got
		}
		steps := []struct { // sequential on purpose: each cancel builds on the last
			cancel string
			want   []string
		}{
			{"", []string{"title k1", "title k2", "title k3"}},
			{"k2", []string{"title k1", "title k3"}},
			{"k1", []string{"title k3"}},
			{"k3", nil},
		}
		for _, st := range steps {
			if st.cancel != "" {
				if r, err := e.s.Cancel(t.Context(), id, st.cancel); err != nil || !r.Matched {
					t.Fatalf("cancel %s: %+v %v", st.cancel, r, err)
				}
			}
			if got := titles(); !reflect.DeepEqual(got, st.want) {
				t.Fatalf("after canceling %q the digest loads %v, want %v", st.cancel, got, st.want)
			}
		}
		_ = receipts
	})

	t.Run("a digest never loads another recipient's notification", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t)
		p := planOf(9, domain.PlannedDelivery{Channel: "email", Mode: domain.PlanDigest})
		e.persist(id, notif(id, "k"), p)
		var window string
		if err := e.db.Admin.QueryRow(t.Context(), `SELECT id::text FROM digest_buffer`).Scan(&window); err != nil {
			t.Fatal(err)
		}
		if _, err := e.s.FlushDigest(t.Context(), window); err != nil {
			t.Fatal(err)
		}
		c := e.claimFor(9, "email")
		c.Entry.Identity = ident("w1", "someone-else")
		if _, members, canceled, err := e.s.Load(t.Context(), c); err != nil || len(members) != 0 || !canceled {
			t.Fatalf("members=%v canceled=%v err=%v: a claim under another identity read this window", members, canceled, err)
		}
	})
}

// ------------------------------------------------------------------- Status --

// NR-032. Status answers "was it delivered?" for every delivery of a
// notification, by channel and address: the pending, the finished, a channel
// that fanned out into several addresses, and one that was dropped.
func TestStatus(t *testing.T) {
	t.Parallel()
	id := ident("w1", "u1")

	t.Run("reports every delivery by channel and address", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t)
		r := e.persist(id, notif(id, "k"), planOf(11,
			immediate("email"), immediate("push"),
			domain.PlannedDelivery{Channel: "sms", Mode: domain.PlanDeferred, NotBefore: farFuture(), Reason: "quiet_hours"},
			domain.PlannedDelivery{Channel: "webhook", Mode: domain.PlanDropped, Reason: "quota"},
		))
		// email binds one address and is delivered; push fans out to three devices;
		// sms is still deferred; webhook was dropped when the notification was accepted.
		email := e.claimFor(11, "email")
		if _, ok, err := e.s.BindAddresses(t.Context(), email, []domain.Address{{Channel: "email", Value: "a@example.com"}}, []string{"addr-email"}); err != nil || !ok {
			t.Fatalf("bind email: %v %v", ok, err)
		}
		if ok, err := e.s.Finish(t.Context(), email, mustFinish(t, domain.OutcomeDelivered, "prov-email", "")); err != nil || !ok {
			t.Fatalf("finish email: %v %v", ok, err)
		}
		push := e.claimFor(11, "push")
		devices := []domain.Address{{Channel: "push", Value: "a"}, {Channel: "push", Value: "b"}, {Channel: "push", Value: "c"}}
		if _, ok, err := e.s.BindAddresses(t.Context(), push, devices, []string{"dev-a", "dev-b", "dev-c"}); err != nil || !ok {
			t.Fatalf("fan out push: %v %v", ok, err)
		}

		st, err := e.s.Status(t.Context(), id, "k")
		if err != nil {
			t.Fatal(err)
		}
		if !st.Found || st.NotificationID != r.NotificationID || st.Canceled {
			t.Fatalf("status = %+v", st)
		}
		type line struct {
			Channel  domain.ChannelKind
			Address  string
			State    string
			Attempts int
			Provider string
		}
		var got []line
		for _, d := range st.Deliveries {
			got = append(got, line{d.Channel, d.AddressKey, d.State, d.Attempts, d.ProviderMessageID})
			finished := d.State != "pending"
			if finished == d.CompletedAt.IsZero() {
				t.Errorf("%s/%s is %s but CompletedAt = %v", d.Channel, d.AddressKey, d.State, d.CompletedAt)
			}
		}
		want := []line{
			{"email", "addr-email", "delivered", 1, "prov-email"},
			{"push", "", "fanned_out", 1, ""},
			{"push", "dev-a", "pending", 0, ""},
			{"push", "dev-b", "pending", 0, ""},
			{"push", "dev-c", "pending", 0, ""},
			{"sms", "", "pending", 0, ""},
			{"webhook", "", "dropped_quota", 0, ""},
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("deliveries:\n got  %+v\n want %+v", got, want)
		}
	})

	t.Run("an unknown key, or another tenant's identical key, is not found", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t)
		e.persist(id, notif(id, "k"), planOf(11, immediate("email")))
		for name, c := range map[string]struct {
			id  domain.Identity
			key string
		}{
			"an unknown key":             {id, "no-such-key"},
			"another tenant's, same key": {ident("w2", "u1"), "k"},
			"another recipient's":        {ident("w1", "u2"), "k"},
		} {
			if st, err := e.s.Status(t.Context(), c.id, c.key); err != nil || st.Found || len(st.Deliveries) != 0 {
				t.Errorf("%s: status = %+v (%v), want not found", name, st, err)
			}
		}
	})

	t.Run("a canceled notification says so, and its canceled deliveries are listed", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t)
		e.persist(id, notif(id, "k"), planOf(11, immediate("email")))
		if _, err := e.s.Cancel(t.Context(), id, "k"); err != nil {
			t.Fatal(err)
		}
		st, err := e.s.Status(t.Context(), id, "k")
		if err != nil || !st.Found || !st.Canceled || len(st.Deliveries) != 1 || st.Deliveries[0].State != "canceled" {
			t.Fatalf("status = %+v (%v)", st, err)
		}
	})

	t.Run("a digested channel is pending while its window is open, then follows the digest's delivery", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t)
		e.persist(id, notif(id, "k"), planOf(11, domain.PlannedDelivery{Channel: "email", Mode: domain.PlanDigest}))
		pending, err := e.s.Status(t.Context(), id, "k")
		if err != nil || len(pending.Deliveries) != 1 || pending.Deliveries[0].Channel != "email" || pending.Deliveries[0].State != "pending" {
			t.Fatalf("before the flush: %+v (%v), want email pending", pending, err)
		}
		var window string
		if err := e.db.Admin.QueryRow(t.Context(), `SELECT id::text FROM digest_buffer`).Scan(&window); err != nil {
			t.Fatal(err)
		}
		if _, err := e.s.FlushDigest(t.Context(), window); err != nil {
			t.Fatal(err)
		}
		c := e.claimFor(11, "email")
		if ok, err := e.s.Finish(t.Context(), c, mustFinish(t, domain.OutcomeDelivered, "prov-digest", "")); err != nil || !ok {
			t.Fatalf("finish: %v %v", ok, err)
		}
		done, err := e.s.Status(t.Context(), id, "k")
		if err != nil || len(done.Deliveries) != 1 || done.Deliveries[0].State != "delivered" ||
			done.Deliveries[0].ProviderMessageID != "prov-digest" {
			t.Fatalf("after the digest was delivered: %+v (%v)", done, err)
		}
	})
}

// -------------------------------------------------- RecordProviderStatus -----

// callbackFixture is three finished deliveries. email/p1 and sms/p1 share a
// provider message id, which two providers are free to do; email/p2 shares a
// channel with the first.
type callbackFixture struct {
	e        *env
	id       domain.Identity
	finished map[string]string // "channel/provider id" -> the delivery's id
}

func newCallbackFixture(t *testing.T) *callbackFixture {
	t.Helper()
	f := &callbackFixture{e: newEnv(t), id: ident("w1", "u1"), finished: map[string]string{}}
	for i, d := range []struct{ channel, provider string }{{"email", "p1"}, {"email", "p2"}, {"sms", "p1"}} {
		shard := domain.Shard(40 + i)
		nid := f.e.notification(f.id, fmt.Sprintf("k%d", i))
		oid := f.e.enqueue(row{id: f.id, notification: nid, shard: shard, channel: d.channel})
		c := f.e.claimOne(shard)
		if ok, err := f.e.s.Finish(t.Context(), c, mustFinish(t, domain.OutcomeDelivered, d.provider, "")); err != nil || !ok {
			t.Fatalf("finish %s/%s: %v %v", d.channel, d.provider, ok, err)
		}
		f.finished[d.channel+"/"+d.provider] = oid
	}
	return f
}

// statusOf is the provider status recorded on a delivery, and whether the
// callback's time was recorded with it.
func (f *callbackFixture) statusOf(key string) (status string, stamped bool) {
	f.e.t.Helper()
	var s *string
	var at *time.Time
	if err := f.e.db.Admin.QueryRow(f.e.t.Context(),
		`SELECT provider_status, provider_status_at FROM notification_deliveries WHERE id = $1::uuid`,
		f.finished[key]).Scan(&s, &at); err != nil {
		f.e.t.Fatal(err)
	}
	if s == nil {
		return "", at != nil
	}
	return *s, at != nil
}

// NR-032. A provider's callback names its own message id: it is recorded on the
// delivery with that id on that channel, and on no other.
func TestRecordProviderStatus(t *testing.T) {
	t.Parallel()

	t.Run("a callback lands on its channel and message id and on no other delivery", func(t *testing.T) {
		t.Parallel()
		f := newCallbackFixture(t)
		if err := f.e.s.RecordProviderStatus(t.Context(), "email", "p1", "bounced"); err != nil {
			t.Fatal(err)
		}
		for key, want := range map[string]string{"email/p1": "bounced", "email/p2": "", "sms/p1": ""} {
			if got, _ := f.statusOf(key); got != want {
				t.Errorf("%s has provider status %q, want %q", key, got, want)
			}
		}
		if _, stamped := f.statusOf("email/p1"); !stamped {
			t.Error("the callback's time was not recorded")
		}
	})

	t.Run("a later callback replaces the earlier status", func(t *testing.T) {
		t.Parallel()
		f := newCallbackFixture(t)
		for _, status := range []string{"delivered", "complained"} {
			if err := f.e.s.RecordProviderStatus(t.Context(), "email", "p1", status); err != nil {
				t.Fatal(err)
			}
		}
		if got, _ := f.statusOf("email/p1"); got != "complained" {
			t.Errorf("status %q, want the later callback", got)
		}
	})

	t.Run("a message id nobody recorded is reported, and changes nothing", func(t *testing.T) {
		t.Parallel()
		f := newCallbackFixture(t)
		for _, c := range []struct {
			ch domain.ChannelKind
			id string
		}{{"email", "p9"}, {"push", "p1"}} {
			if err := f.e.s.RecordProviderStatus(t.Context(), c.ch, c.id, "bounced"); !errors.Is(err, domain.ErrUnknownDelivery) {
				t.Errorf("%s/%s: err = %v, want ErrUnknownDelivery", c.ch, c.id, err)
			}
		}
		for key := range f.finished {
			if got, _ := f.statusOf(key); got != "" {
				t.Errorf("%s has status %q after callbacks for deliveries that do not exist", key, got)
			}
		}
	})

	t.Run("the recorded status is what Status reports", func(t *testing.T) {
		t.Parallel()
		f := newCallbackFixture(t)
		if err := f.e.s.RecordProviderStatus(t.Context(), "sms", "p1", "undelivered"); err != nil {
			t.Fatal(err)
		}
		st, err := f.e.s.Status(t.Context(), f.id, "k2")
		if err != nil || len(st.Deliveries) != 1 || st.Deliveries[0].ProviderStatus != "undelivered" ||
			st.Deliveries[0].ProviderMessageID != "p1" {
			t.Fatalf("status = %+v (%v), want the recorded callback", st, err)
		}
	})
}
