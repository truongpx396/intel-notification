//go:build integration

package postgres

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/truongpx396/intel-notification/domain"
)

// T032. A dead letter is replayed once. The transition is one statement, so the
// guarantees are the statement's, and they are asserted here against a real
// PostgreSQL, as the table owner, with real concurrency for the one that only
// matters under it.

// parked is a dead letter made the way the engine makes one: a queue row is claimed,
// its address bound, and it is finished as a genuine failure.
type parked struct {
	deadLetter string
	outbox     string
	nid        string
	address    domain.Address
	key        string
}

func (e *env) park(id domain.Identity, idemKey string, r row, outcome domain.TerminalOutcome, detail string) parked {
	e.t.Helper()
	p := parked{nid: e.notification(id, idemKey), address: domain.Address{Channel: domain.ChannelKind(r.channel), Value: "u1@example.com", Locale: "vi"}}
	r.id, r.notification = id, p.nid
	p.outbox = e.enqueue(r)
	p.key = domain.AddressKey(p.address.Channel, p.address.Value)
	c := e.claimOne(r.shard)
	if _, ok, err := e.s.BindAddresses(e.t.Context(), c, []domain.Address{p.address}, []string{p.key}); err != nil || !ok {
		e.t.Fatalf("bind: %v, %v", ok, err)
	}
	if ok, err := e.s.Finish(e.t.Context(), c, mustFinish(e.t, outcome, "", detail)); err != nil || !ok {
		e.t.Fatalf("finish: %v, %v", ok, err)
	}
	if err := e.db.Admin.QueryRow(e.t.Context(), `SELECT id::text FROM dead_letters WHERE outbox_id = $1::uuid`, p.outbox).
		Scan(&p.deadLetter); err != nil {
		e.t.Fatalf("the finish left no dead letter: %v", err)
	}
	return p
}

// newUUID is an id no row has, drawn by the database.
func (e *env) newUUID() string {
	e.t.Helper()
	var id string
	if err := e.db.Admin.QueryRow(e.t.Context(), `SELECT gen_random_uuid()::text`).Scan(&id); err != nil {
		e.t.Fatal(err)
	}
	return id
}

// pendingRow is a queue row of one channel, as a replay leaves it.
type pendingRow struct {
	ID, NotificationID, DigestID, AddressKey string
	Attempts, Deferrals                      int
	Leased                                   bool
	Fallback                                 []string
	Due                                      bool
	HasAddress                               bool
	Shard                                    int
	DeliverBefore                            *time.Time
	Topic, Priority                          string
	Quiet                                    bool
	Identity                                 domain.Identity
}

func (e *env) pending(channel string) []pendingRow {
	e.t.Helper()
	rows, err := e.db.Admin.Query(e.t.Context(), `
		SELECT id::text, coalesce(notification_id::text, ''), coalesce(digest_id::text, ''), address_key, attempts, deferrals,
		       lease_token IS NOT NULL, fallback, next_attempt_at <= now(), address IS NOT NULL, shard, deliver_before,
		       topic, priority, quiet_hours_override, realm, tenant_kind, tenant_id, recipient_kind, recipient_id
		  FROM notification_outbox WHERE channel = $1 ORDER BY created_at, id`, channel)
	if err != nil {
		e.t.Fatal(err)
	}
	defer rows.Close()
	var out []pendingRow
	for rows.Next() {
		var p pendingRow
		var realm string
		if err := rows.Scan(&p.ID, &p.NotificationID, &p.DigestID, &p.AddressKey, &p.Attempts, &p.Deferrals, &p.Leased,
			&p.Fallback, &p.Due, &p.HasAddress, &p.Shard, &p.DeliverBefore, &p.Topic, &p.Priority, &p.Quiet,
			&realm, &p.Identity.Tenant.Kind, &p.Identity.Tenant.ID, &p.Identity.Recipient.Kind, &p.Identity.Recipient.ID); err != nil {
			e.t.Fatal(err)
		}
		p.Identity.Realm = domain.Realm(realm)
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		e.t.Fatal(err)
	}
	return out
}

func (e *env) replayedAt(deadLetter string) *time.Time {
	e.t.Helper()
	var at *time.Time
	if err := e.db.Admin.QueryRow(e.t.Context(), `SELECT replayed_at FROM dead_letters WHERE id = $1::uuid`, deadLetter).Scan(&at); err != nil {
		e.t.Fatal(err)
	}
	return at
}

// A dead letter is inspectable with its reason and its last error (NS-006), and
// replaying it enqueues one fresh delivery that is the original, made new: attempts
// start again, nothing holds it, it is due now, it keeps the address it was bound to
// so a channel dedupes it on the key it had, and it no longer carries a fallback
// chain, which ran when the delivery was parked.
func TestReplayDeadLetterEnqueuesOneFreshDelivery(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	id := ident("w1", "u1")
	deadline := time.Now().Add(48 * time.Hour).Truncate(time.Microsecond)
	p := e.park(id, "k1", row{channel: "email", shard: 4, fallback: []string{"sms"}}, domain.OutcomeRejected, "payload too large")

	// Inspectable: why, and what the last error was.
	var reason, lastError, channel string
	var attempts int
	if err := e.db.Admin.QueryRow(t.Context(),
		`SELECT reason, last_error, channel, attempts FROM dead_letters WHERE id = $1::uuid`, p.deadLetter).
		Scan(&reason, &lastError, &channel, &attempts); err != nil {
		t.Fatal(err)
	}
	if reason != "rejected" || lastError != "payload too large" || channel != "email" || attempts != 1 {
		t.Fatalf("dead letter = %q, %q, %q, %d, want rejected, its detail, email and attempt 1", reason, lastError, channel, attempts)
	}
	if e.replayedAt(p.deadLetter) != nil {
		t.Fatal("a dead letter nobody replayed says it was")
	}
	if got := e.pending("email"); len(got) != 0 {
		t.Fatalf("%d email rows pending before the replay, want none: the original left the queue", len(got))
	}
	if _, err := e.db.Admin.Exec(t.Context(), `UPDATE dead_letters SET payload = jsonb_set(payload, '{deliver_before}', to_jsonb($2::timestamptz)) WHERE id = $1::uuid`,
		p.deadLetter, deadline); err != nil {
		t.Fatal(err)
	}
	// A critical message sent through quiet hours: the row said so, and the replay keeps it.
	if _, err := e.db.Admin.Exec(t.Context(), `UPDATE dead_letters SET payload = payload || '{"priority": "critical", "quiet_hours_override": true}'::jsonb WHERE id = $1::uuid`,
		p.deadLetter); err != nil {
		t.Fatal(err)
	}

	replayed, err := e.s.ReplayDeadLetter(t.Context(), "aisat", p.deadLetter)
	if err != nil || !replayed {
		t.Fatalf("ReplayDeadLetter = %v, %v, want it replayed", replayed, err)
	}

	got := e.pending("email")
	if len(got) != 1 {
		t.Fatalf("%d email rows pending after the replay, want one", len(got))
	}
	r := got[0]
	if r.ID == p.outbox || r.NotificationID != p.nid || r.AddressKey != p.key || !r.HasAddress || r.Shard != 4 {
		t.Errorf("row = %+v, want a new row for notification %s bound to %s on shard 4", r, p.nid, p.key)
	}
	if r.Attempts != 0 || r.Deferrals != 0 || r.Leased || !r.Due {
		t.Errorf("row = %+v, want it fresh: no attempts, no lease, due now", r)
	}
	if r.Topic != "ingestion_complete" || r.Priority != "critical" || !r.Quiet || r.Identity != id {
		t.Errorf("row = %+v, want the same topic, priority and audit flag, for the same identity", r)
	}
	if len(r.Fallback) != 0 {
		t.Errorf("fallback = %v, want none: the chain ran when the delivery was parked, and replaying must not run it twice", r.Fallback)
	}
	if r.DeliverBefore == nil || !r.DeliverBefore.Equal(deadline) {
		t.Errorf("deliver_before = %v, want the original deadline %v: a replayed message is not sent late", r.DeliverBefore, deadline)
	}
	if at := e.replayedAt(p.deadLetter); at == nil {
		t.Error("replayed_at is not recorded")
	}

	// The fresh row is a delivery like any other: it is claimed, and counted once.
	claims, err := e.s.Claim(t.Context(), 4, 10, lease)
	var email *domain.Claim
	for i := range claims {
		if claims[i].Entry.Channel == "email" {
			email = &claims[i]
		}
	}
	if err != nil || email == nil || email.Entry.Attempts != 1 || email.Entry.Address == nil || email.Entry.Address.Value != "u1@example.com" || email.Entry.AddressKey != p.key {
		t.Fatalf("Claim = %+v, %v, want the replayed delivery at attempt 1, bound to its address", claims, err)
	}
}

// A second replay enqueues nothing more (NS-006): the operator who clicks twice, and
// the script that retries after a timeout, get one delivery.
func TestReplayDeadLetterTwiceEnqueuesOnce(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	p := e.park(ident("w1", "u1"), "k1", row{channel: "email", shard: 4}, domain.OutcomeMaxAttempts, "timeout")

	for i := range 3 {
		replayed, err := e.s.ReplayDeadLetter(t.Context(), "aisat", p.deadLetter)
		if err != nil {
			t.Fatalf("replay %d: %v", i+1, err)
		}
		if replayed != (i == 0) {
			t.Fatalf("replay %d reported replayed = %v, want true only for the first", i+1, replayed)
		}
	}
	if got := e.pending("email"); len(got) != 1 {
		t.Fatalf("%d email rows pending after three replays, want one", len(got))
	}
}

// Two replays at once enqueue one delivery. Only the database can say which of them
// came first; if the statement did not decide, both would enqueue, and the
// notification would be sent twice on a channel that cannot tell. Real concurrency,
// with a deadline.
func TestReplayDeadLetterConcurrentlyEnqueuesOnce(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	p := e.park(ident("w1", "u1"), "k1", row{channel: "email", shard: 4}, domain.OutcomeMaxAttempts, "timeout")

	const replayers = 8
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		replayed int
		errs     []error
		gate     = make(chan struct{})
	)
	for range replayers {
		wg.Go(func() {
			<-gate
			ok, err := e.s.ReplayDeadLetter(ctx, "aisat", p.deadLetter)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
			}
			if ok {
				replayed++
			}
		})
	}
	close(gate)
	wg.Wait()
	if len(errs) != 0 || replayed != 1 {
		t.Fatalf("%d replayers said they replayed it (errors %v), want exactly one", replayed, errs)
	}
	if got := e.pending("email"); len(got) != 1 {
		t.Fatalf("%d email rows pending, want one", len(got))
	}
}

// What cannot be replayed says why, and enqueues nothing: an id that names no dead
// letter, one that is another realm's (which must read as absent, or an id could be
// probed across products), one that is not an id, and one parked by the bus.
func TestReplayDeadLetterRefusals(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	p := e.park(ident("w1", "u1"), "k1", row{channel: "email", shard: 4}, domain.OutcomeMaxAttempts, "timeout")
	busID := e.newUUID()
	if _, err := e.db.Admin.Exec(t.Context(), `
		INSERT INTO dead_letters (id, realm, source, reason, payload, attempts, last_error)
		VALUES ($1::uuid, 'aisat', 'bus', 'poison', '{}', 0, 'not a notification')`, busID); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name  string
		realm domain.Realm
		id    string
		want  error
	}{
		{"an id that names no dead letter", "aisat", e.newUUID(), domain.ErrUnknownDeadLetter},
		{"another realm's dead letter", "other-product", p.deadLetter, domain.ErrUnknownDeadLetter},
		{"an id that is not an id", "aisat", "not-a-uuid", domain.ErrUnknownDeadLetter},
		{"an empty id", "aisat", "", domain.ErrUnknownDeadLetter},
		{"a dead letter parked by the bus", "aisat", busID, domain.ErrNotReplayable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			replayed, err := e.with(t).s.ReplayDeadLetter(t.Context(), tc.realm, tc.id)
			if !errors.Is(err, tc.want) || replayed {
				t.Fatalf("ReplayDeadLetter = %v, %v, want %v", replayed, err, tc.want)
			}
		})
	}
	if got := e.pending("email"); len(got) != 0 {
		t.Fatalf("%d rows pending after the refusals, want none", len(got))
	}
	if e.replayedAt(p.deadLetter) != nil {
		t.Fatal("another realm's replay marked the dead letter replayed")
	}
}

// The statement is all or nothing: a delivery for the same notification, channel and
// address that is already pending refuses the replay, and replayed_at stays unset, so
// the dead letter is not lost to a replay that enqueued nothing.
func TestReplayDeadLetterThatConflictsLeavesTheDeadLetterUnreplayed(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	id := ident("w1", "u1")
	p := e.park(id, "k1", row{channel: "email", shard: 4}, domain.OutcomeMaxAttempts, "timeout")
	// Something else re-enqueued the same delivery meanwhile.
	if _, err := e.db.Admin.Exec(t.Context(), `
		INSERT INTO notification_outbox (notification_id, notification_created_at, realm, tenant_kind, tenant_id,
		                                 recipient_kind, recipient_id, topic, channel, address_key, shard)
		VALUES ($1::uuid, $2, 'aisat', 'workspace', 'w1', 'user', 'u1', 'ingestion_complete', 'email', $3, 4)`,
		p.nid, fixtureDay, p.key); err != nil {
		t.Fatal(err)
	}

	replayed, err := e.s.ReplayDeadLetter(t.Context(), "aisat", p.deadLetter)
	if err == nil || replayed {
		t.Fatalf("ReplayDeadLetter = %v, %v, want an error: the delivery is already pending", replayed, err)
	}
	if e.replayedAt(p.deadLetter) != nil {
		t.Fatal("replayed_at was set although nothing was enqueued")
	}
	if got := e.pending("email"); len(got) != 1 {
		t.Fatalf("%d rows pending, want only the one that was there", len(got))
	}
}

// A dead letter of a digest delivery replays the digest's delivery: the window, not
// a notification.
func TestReplayDeadLetterOfADigestReplaysTheWindow(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	id := ident("w1", "u1")
	window := e.newUUID()
	if _, err := e.db.Admin.Exec(t.Context(), `
		INSERT INTO notification_outbox (digest_id, realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
		                                 topic, channel, shard)
		VALUES ($1::uuid, 'aisat', 'workspace', 'w1', 'user', 'u1', 'ingestion_complete', 'email', 4)`, window); err != nil {
		t.Fatal(err)
	}
	c := e.claimOne(4)
	if ok, err := e.s.Finish(t.Context(), c, mustFinish(t, domain.OutcomeMaxAttempts, "", "timeout")); err != nil || !ok {
		t.Fatalf("finish: %v, %v", ok, err)
	}
	var deadLetter string
	if err := e.db.Admin.QueryRow(t.Context(), `SELECT id::text FROM dead_letters WHERE digest_id = $1::uuid`, window).Scan(&deadLetter); err != nil {
		t.Fatal(err)
	}

	if replayed, err := e.s.ReplayDeadLetter(t.Context(), id.Realm, deadLetter); err != nil || !replayed {
		t.Fatalf("ReplayDeadLetter = %v, %v", replayed, err)
	}
	got := e.pending("email")
	if len(got) != 1 || got[0].DigestID != window || got[0].NotificationID != "" || got[0].Attempts != 0 {
		t.Fatalf("pending = %+v, want one fresh delivery for the window %s and no notification", got, window)
	}
}
