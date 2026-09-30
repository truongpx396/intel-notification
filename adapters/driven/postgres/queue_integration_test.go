//go:build integration

package postgres

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/truongpx396/intel-notification/domain"
)

// D19. The claim is the one piece of this engine that is only correct under
// concurrency, so this test uses real concurrency: session A holds rows in an
// open transaction while session B claims from the same shard. Remove SKIP
// LOCKED from claimSQL and B blocks until its deadline, failing the test
// instead of hanging it.
func TestClaimLeasesAndSkipsLockedRows(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()
	nid := e.notification(ident("w1", "u1"), "k1")
	for _, ch := range []string{"email", "sms", "push"} {
		e.enqueue(row{id: ident("w1", "u1"), notification: nid, shard: 5, channel: ch})
	}

	txA, err := e.db.Owner.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = txA.Rollback(context.Background()) }()
	rowsA, err := txA.Query(ctx, claimSQL, int16(5), 2, micros(lease))
	if err != nil {
		t.Fatal(err)
	}
	heldByA, err := pgx.CollectRows(rowsA, scanClaim)
	if err != nil || len(heldByA) != 2 {
		t.Fatalf("session A claimed %d rows (%v), want 2", len(heldByA), err)
	}

	ctxB, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	takenByB, err := e.s.Claim(ctxB, 5, 10, lease)
	if err != nil {
		t.Fatalf("session B blocked on rows A holds instead of skipping them: %v", err)
	}
	if len(takenByB) != 1 {
		t.Fatalf("session B claimed %d rows, want only the one A does not hold", len(takenByB))
	}
	if err := txA.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	if again, err := e.s.Claim(ctx, 5, 10, lease); err != nil || len(again) != 0 {
		t.Fatalf("leased rows were claimable again (%d, %v)", len(again), err)
	}

	e.lapseLeases(5) // both workers die holding their leases
	reclaimed, err := e.s.Claim(ctx, 5, 10, lease)
	if err != nil || len(reclaimed) != 3 {
		t.Fatalf("after the leases lapsed, claimed %d rows (%v), want 3", len(reclaimed), err)
	}
	for _, c := range reclaimed {
		if c.Entry.Attempts != 2 {
			t.Errorf("row %s: attempts = %d, want 2 — every claim counts, so a crash does too",
				c.Entry.ID, c.Entry.Attempts)
		}
		if c.LeaseToken == "" || c.Entry.Identity != ident("w1", "u1") {
			t.Errorf("claim %+v lacks its lease token or its identity", c)
		}
	}
}

// D19. Every state change is fenced by the lease token. The realistic case is
// the last one: a worker stalls past its lease, another worker re-claims the row,
// and the first worker's late write must change nothing.
func TestFencedWrites(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	later := time.Now().Add(time.Minute)
	delivered := mustFinish(t, domain.OutcomeDelivered, "p", "")
	bindOne := func(ctx context.Context, c domain.Claim) (bool, error) {
		_, ok, err := e.s.BindAddresses(ctx, c,
			[]domain.Address{{Channel: "email", Value: "u1@example.com"}}, []string{"k"})
		return ok, err
	}
	writes := map[string]func(context.Context, domain.Claim) (bool, error){
		"retry": func(ctx context.Context, c domain.Claim) (bool, error) {
			return e.s.Retry(ctx, c, later, "timeout")
		},
		"defer": func(ctx context.Context, c domain.Claim) (bool, error) {
			return e.s.Defer(ctx, c, later, "quota")
		},
		"finish": func(ctx context.Context, c domain.Claim) (bool, error) {
			return e.s.Finish(ctx, c, delivered)
		},
		"bind": bindOne,
	}

	next := domain.Shard(10)
	for name, write := range writes {
		for _, stale := range []string{"forged token", "token of a lapsed lease"} {
			next++
			shard := next // each parallel subtest owns one shard
			t.Run(name+" with "+stale, func(t *testing.T) {
				t.Parallel()
				nid := e.notification(ident("w1", "u-"+name), "k-"+stale)
				oid := e.enqueue(row{id: ident("w1", "u-"+name), notification: nid, shard: shard, channel: "email"})
				c := e.claimOne(shard)

				staleClaim := c
				if stale == "forged token" {
					staleClaim.LeaseToken = "00000000-0000-4000-8000-000000000000"
				} else {
					e.lapseLeases(shard)
					c = e.claimOne(shard) // another worker takes over
				}
				if ok, err := write(t.Context(), staleClaim); err != nil || ok {
					t.Fatalf("stale %s: ok=%v err=%v — the write should have been discarded", name, ok, err)
				}
				if !e.queued(oid) {
					t.Fatal("the stale write changed the row")
				}
				if ok, err := write(t.Context(), c); err != nil || !ok {
					t.Fatalf("current holder's %s: ok=%v err=%v", name, ok, err)
				}
			})
		}
	}
}

func TestRetryAndDefer(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	cases := []struct {
		name          string
		shard         domain.Shard
		act           func(context.Context, domain.Claim, time.Time) (bool, error)
		wantAttempts  int
		wantDeferrals int
	}{
		{"retry keeps the attempt the claim counted", 20,
			func(ctx context.Context, c domain.Claim, at time.Time) (bool, error) {
				return e.s.Retry(ctx, c, at, "timeout")
			},
			1, 0},
		{"defer gives the attempt back, so it can never dead-letter", 21,
			func(ctx context.Context, c domain.Claim, at time.Time) (bool, error) {
				return e.s.Defer(ctx, c, at, "quota")
			},
			0, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			nid := e.notification(ident("w1", tc.name), "k")
			oid := e.enqueue(row{id: ident("w1", tc.name), notification: nid, shard: tc.shard, channel: "email"})
			c := e.claimOne(tc.shard)
			if ok, err := tc.act(t.Context(), c, time.Now().Add(time.Hour)); err != nil || !ok {
				t.Fatalf("ok=%v err=%v", ok, err)
			}
			var attempts, deferrals int
			var leased, due bool
			if err := e.db.Admin.QueryRow(t.Context(), `
SELECT attempts, deferrals, lease_token IS NOT NULL, next_attempt_at <= now()
  FROM notification_outbox WHERE id = $1::uuid`, oid).Scan(&attempts, &deferrals, &leased, &due); err != nil {
				t.Fatal(err)
			}
			if attempts != tc.wantAttempts || deferrals != tc.wantDeferrals {
				t.Errorf("attempts=%d deferrals=%d, want %d and %d", attempts, deferrals, tc.wantAttempts, tc.wantDeferrals)
			}
			if leased || due {
				t.Errorf("leased=%v due=%v: the lease should be released and the row due in an hour", leased, due)
			}
		})
	}
}

// D19. The lease lives in lease_expires_at and a claim leaves the due time alone:
// next_attempt_at is indexed, so moving it would make every claim a non-HOT update
// that writes an entry into every index.
func TestClaimLeavesTheDueTimeAlone(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	id := ident("w1", "u1")
	oid := e.enqueue(row{id: id, notification: e.notification(id, "k"), shard: 90, channel: "email",
		dueIn: -time.Minute})
	dueTime := func() (due time.Time, leaseEnds *time.Time) {
		t.Helper()
		if err := e.db.Admin.QueryRow(t.Context(),
			`SELECT next_attempt_at, lease_expires_at FROM notification_outbox WHERE id = $1::uuid`, oid).
			Scan(&due, &leaseEnds); err != nil {
			t.Fatal(err)
		}
		return due, leaseEnds
	}
	before, leaseEnds := dueTime()
	if leaseEnds != nil {
		t.Fatalf("an unclaimed row has a lease ending %v", leaseEnds)
	}

	e.claimOne(90)

	after, leaseEnds := dueTime()
	if !after.Equal(before) {
		t.Errorf("the claim moved next_attempt_at from %v to %v", before, after)
	}
	if leaseEnds == nil || time.Until(*leaseEnds) < lease-time.Minute {
		t.Errorf("lease_expires_at = %v, want about %v from now", leaseEnds, lease)
	}
}

// D19. Retry and Defer end the lease along with the token. A row released and due
// again at once must be claimable at once, not after the lease it no longer holds
// would have expired.
func TestReleasedRowIsClaimableAtOnce(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	past := time.Now().Add(-time.Second)
	cases := []struct {
		name    string
		shard   domain.Shard
		release func(context.Context, domain.Claim) (bool, error)
	}{
		{"retry", 91, func(ctx context.Context, c domain.Claim) (bool, error) {
			return e.s.Retry(ctx, c, past, "timeout")
		}},
		{"defer", 92, func(ctx context.Context, c domain.Claim) (bool, error) {
			return e.s.Defer(ctx, c, past, "quota")
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			id := ident("w1", "u-"+tc.name)
			e.enqueue(row{id: id, notification: e.notification(id, "k"), shard: tc.shard, channel: "email"})
			c := e.claimOne(tc.shard)
			if ok, err := tc.release(t.Context(), c); err != nil || !ok {
				t.Fatalf("release: ok=%v err=%v", ok, err)
			}
			if claims, err := e.s.Claim(t.Context(), tc.shard, 10, lease); err != nil || len(claims) != 1 {
				t.Fatalf("claimed %d rows (%v) after the %s, want the released row at once", len(claims), err, tc.name)
			}
		})
	}
}

// D12, D20, D29, D37. The terminal transition for every outcome: the queue row
// leaves, the history row lands, and the dead letter and next fallback channel
// follow the disposition the domain decided.
func TestFinish(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	cases := []struct {
		outcome        domain.TerminalOutcome
		fallback       []string
		wantDeadLetter bool
		wantNext       string // "" ⇒ no fallback enqueued
		wantRest       []string
	}{
		{outcome: domain.OutcomeDelivered, fallback: []string{"sms_fb"}},
		{outcome: domain.OutcomeMaxAttempts, fallback: []string{"sms_fb", "email_fb"},
			wantDeadLetter: true, wantNext: "sms_fb", wantRest: []string{"email_fb"}},
		{outcome: domain.OutcomeRejected, wantDeadLetter: true},
		{outcome: domain.OutcomeSuppressed, fallback: []string{"sms_fb"}, wantNext: "sms_fb", wantRest: []string{}},
		{outcome: domain.OutcomeNoAddress, fallback: []string{"sms_fb"}, wantNext: "sms_fb", wantRest: []string{}},
		{outcome: domain.OutcomeExpired, fallback: []string{"sms_fb"}},
		{outcome: domain.OutcomeCanceled, fallback: []string{"sms_fb"}},
		{outcome: domain.OutcomeDroppedQuota, fallback: []string{"sms_fb"}},
	}
	for i, tc := range cases {
		shard := domain.Shard(30 + i)
		t.Run(string(tc.outcome), func(t *testing.T) {
			t.Parallel()
			id := ident("w1", "u-"+string(tc.outcome))
			nid := e.notification(id, "k")
			oid := e.enqueue(row{id: id, notification: nid, shard: shard, channel: "push", fallback: tc.fallback})
			c := e.claimOne(shard)

			ok, err := e.s.Finish(t.Context(), c, mustFinish(t, tc.outcome, "prov-"+string(tc.outcome), "detail"))
			if err != nil || !ok {
				t.Fatalf("ok=%v err=%v", ok, err)
			}
			if e.queued(oid) {
				t.Error("the finished delivery is still in the queue")
			}
			if n := e.count(`SELECT count(*) FROM notification_deliveries
                              WHERE id = $1::uuid AND outcome = $2 AND detail = 'detail'`, oid, string(tc.outcome)); n != 1 {
				t.Errorf("history rows with outcome %s: %d, want 1", tc.outcome, n)
			}
			if tc.outcome == domain.OutcomeDelivered {
				// A bounce or delivery callback names the provider's id, not ours.
				if n := e.count(`SELECT count(*) FROM notification_deliveries
                                  WHERE channel = 'push' AND provider_message_id = 'prov-delivered'`); n != 1 {
					t.Error("a provider callback cannot find its delivery by (channel, provider_message_id)")
				}
			}
			if n := e.count(`SELECT count(*) FROM dead_letters
                              WHERE outbox_id = $1::uuid AND reason = $2`, oid, string(tc.outcome)); (n == 1) != tc.wantDeadLetter {
				t.Errorf("dead letters: %d, want dead letter = %v", n, tc.wantDeadLetter)
			}
			var next []string
			err = e.db.Admin.QueryRow(t.Context(), `
SELECT fallback FROM notification_outbox WHERE notification_id = $1::uuid AND channel = $2`,
				nid, tc.wantNext).Scan(&next)
			switch {
			case tc.wantNext == "":
				if n := e.count(`SELECT count(*) FROM notification_outbox WHERE notification_id = $1::uuid`, nid); n != 0 {
					t.Errorf("%d rows enqueued; %s must not continue the fallback chain", n, tc.outcome)
				}
			case err != nil:
				t.Errorf("next fallback channel %s not enqueued: %v", tc.wantNext, err)
			case !slices.Equal(next, tc.wantRest):
				t.Errorf("next channel carries fallback %q, want %q", next, tc.wantRest)
			}
		})
	}

	t.Run("the store executes the disposition it is given, and decides nothing", func(t *testing.T) {
		t.Parallel()
		id := ident("w1", "u-mechanism")
		nid := e.notification(id, "k")
		oid := e.enqueue(row{id: id, notification: nid, shard: 50, channel: "email"})
		c := e.claimOne(50)
		f := domain.Finish{Outcome: domain.OutcomeMaxAttempts} // disposition deliberately empty
		if ok, err := e.s.Finish(t.Context(), c, f); err != nil || !ok {
			t.Fatalf("ok=%v err=%v", ok, err)
		}
		if n := e.count(`SELECT count(*) FROM dead_letters WHERE outbox_id = $1::uuid`, oid); n != 0 {
			t.Fatal("the store dead-lettered on its own judgement")
		}
	})

	t.Run("the transition is one statement: a refused dead letter leaves the row queued", func(t *testing.T) {
		t.Parallel()
		id := ident("w1", "u-atomic")
		nid := e.notification(id, "k")
		oid := e.enqueue(row{id: id, notification: nid, shard: 51, channel: "email"})
		c := e.claimOne(51)
		// The schema refuses a correct outcome as a dead letter. Because the whole
		// transition is one statement, the refusal undoes the delete and the
		// history row with it.
		f := domain.Finish{Outcome: domain.OutcomeSuppressed, Disposition: domain.Disposition{DeadLetter: true}}
		if _, err := e.s.Finish(t.Context(), c, f); err == nil {
			t.Fatal("a dead letter for a correct outcome was accepted")
		}
		if !e.queued(oid) || e.count(`SELECT count(*) FROM notification_deliveries WHERE id = $1::uuid`, oid) != 0 {
			t.Fatal("a failed transition left a partial write behind")
		}
	})

	t.Run("fanned_out is not a finish", func(t *testing.T) {
		t.Parallel()
		if _, err := e.s.Finish(t.Context(), domain.Claim{}, domain.Finish{Outcome: domain.OutcomeFannedOut}); err == nil {
			t.Fatal("fanned_out accepted")
		}
	})
}

// D25. One address binds in place; several fan out into one delivery per
// address; binding happens once.
func TestBindAddresses(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	devices := []domain.Address{
		{Channel: "push", Value: "tok-a"}, {Channel: "push", Value: "tok-b"}, {Channel: "push", Value: "tok-c"},
	}
	keys := []string{"dev-a", "dev-b", "dev-c"}

	t.Run("one address binds in place and the claim stays held", func(t *testing.T) {
		t.Parallel()
		nid := e.notification(ident("w1", "u-one"), "k")
		oid := e.enqueue(row{id: ident("w1", "u-one"), notification: nid, shard: 60, channel: "email"})
		c := e.claimOne(60)
		b, ok, err := e.s.BindAddresses(t.Context(), c,
			[]domain.Address{{Channel: "email", Value: "u1@example.com"}}, []string{"addr-x"})
		if err != nil || !ok || b != (domain.Binding{Addresses: 1, StillHeld: true}) {
			t.Fatalf("binding=%+v ok=%v err=%v", b, ok, err)
		}
		// Still held: the claimer can go on to finish it with the same token.
		if ok, err := e.s.Finish(t.Context(), c, mustFinish(t, domain.OutcomeDelivered, "p", "")); err != nil || !ok {
			t.Fatalf("finishing the bound claim: ok=%v err=%v", ok, err)
		}
		if n := e.count(`SELECT count(*) FROM notification_deliveries
                          WHERE id = $1::uuid AND address_key = 'addr-x'`, oid); n != 1 {
			t.Fatal("the delivery was not recorded against its bound address")
		}
	})

	t.Run("binding is one-shot, so the delivery key never changes", func(t *testing.T) {
		t.Parallel()
		nid := e.notification(ident("w1", "u-once"), "k")
		e.enqueue(row{id: ident("w1", "u-once"), notification: nid, shard: 61, channel: "email"})
		c := e.claimOne(61)
		addr := []domain.Address{{Channel: "email", Value: "a@example.com"}}
		if _, ok, _ := e.s.BindAddresses(t.Context(), c, addr, []string{"first"}); !ok {
			t.Fatal("first bind failed")
		}
		if _, ok, err := e.s.BindAddresses(t.Context(), c, addr, []string{"second"}); err != nil || ok {
			t.Fatalf("second bind: ok=%v err=%v — it must be refused", ok, err)
		}
	})

	t.Run("several addresses fan out into unleased deliveries", func(t *testing.T) {
		t.Parallel()
		nid := e.notification(ident("w1", "u-many"), "k")
		oid := e.enqueue(row{id: ident("w1", "u-many"), notification: nid, shard: 62, channel: "push",
			fallback: []string{"sms"}})
		c := e.claimOne(62)
		b, ok, err := e.s.BindAddresses(t.Context(), c, devices, keys)
		if err != nil || !ok || b != (domain.Binding{Addresses: 3, StillHeld: false}) {
			t.Fatalf("binding=%+v ok=%v err=%v", b, ok, err)
		}
		if n := e.count(`SELECT count(*) FROM notification_outbox
                          WHERE notification_id = $1::uuid AND channel = 'push' AND lease_token IS NULL
                            AND address_key = ANY($2) AND cardinality(fallback) = 0`, nid, keys); n != 3 {
			t.Fatalf("%d child deliveries, want 3 unleased ones without a fallback chain", n)
		}
		if n := e.count(`SELECT count(*) FROM notification_deliveries
                          WHERE id = $1::uuid AND outcome = 'fanned_out'`, oid); n != 1 {
			t.Fatal("the channel-level delivery was not recorded as fanned out")
		}
		if _, ok, err := e.s.BindAddresses(t.Context(), c, devices, keys); err != nil || ok {
			t.Fatalf("binding with the spent claim: ok=%v err=%v", ok, err)
		}
		// Each device is now claimed, retried and finished on its own.
		if claims, err := e.s.Claim(t.Context(), 62, 10, lease); err != nil || len(claims) != 3 {
			t.Fatalf("claimed %d device deliveries (%v), want 3", len(claims), err)
		}
	})

	invalid := []struct {
		name  string
		addrs []domain.Address
		keys  []string
	}{
		{"no addresses", nil, nil},
		{"fewer keys than addresses", devices, keys[:2]},
		{"duplicate keys", devices[:2], []string{"k", "k"}},
		{"an empty key", devices[:1], []string{""}},
	}
	for _, tc := range invalid {
		t.Run("rejects "+tc.name, func(t *testing.T) {
			t.Parallel()
			if _, _, err := e.s.BindAddresses(t.Context(), domain.Claim{}, tc.addrs, tc.keys); err == nil {
				t.Fatal("accepted")
			}
		})
	}
}

// D24 / NS-007. An exhausted tenant's whole backlog leaves the claim range in
// one statement, and the next claim goes to someone else.
func TestDeferTenantChannel(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	for i := range 50 {
		id := ident("w-noisy", "u"+string(rune('a'+i%26))+string(rune('a'+i/26)))
		e.enqueue(row{id: id, notification: e.notification(id, "k"), shard: 70, channel: "email",
			dueIn: -10 * time.Minute})
	}
	quiet := ident("w-quiet", "q1")
	e.enqueue(row{id: quiet, notification: e.notification(quiet, "k"), shard: 70, channel: "email",
		dueIn: -time.Minute})
	// A noisy-tenant delivery a worker is sending right now: deferral must leave
	// it to that worker rather than steal its lease.
	inFlight := ident("w-noisy", "in-flight")
	inFlightRow := e.enqueue(row{id: inFlight, notification: e.notification(inFlight, "k"), shard: 72,
		channel: "email", dueIn: -10 * time.Minute})
	held := e.claimOne(72)
	noisySMS := ident("w-noisy", "sms-user")
	e.enqueue(row{id: noisySMS, notification: e.notification(noisySMS, "k"), shard: 71, channel: "sms",
		dueIn: -10 * time.Minute})

	n, err := e.s.DeferTenantChannel(t.Context(), "aisat", domain.Tenant{Kind: "workspace", ID: "w-noisy"},
		"email", time.Now().Add(time.Hour))
	if err != nil || n != 50 {
		t.Fatalf("deferred %d rows (%v), want the noisy tenant's 50 email deliveries", n, err)
	}
	claims, err := e.s.Claim(t.Context(), 70, 5, lease)
	if err != nil || len(claims) != 1 || claims[0].Entry.Identity != quiet {
		t.Fatalf("the next claim took %d rows (%v); it should go straight to the quiet tenant", len(claims), err)
	}
	if claims, _ := e.s.Claim(t.Context(), 71, 5, lease); len(claims) != 1 {
		t.Fatal("the noisy tenant's other channel was deferred too; quotas are per channel")
	}
	if n := e.count(`SELECT count(*) FROM notification_outbox
                      WHERE id = $1::uuid AND lease_token = $2::uuid`, inFlightRow, held.LeaseToken); n != 1 {
		t.Fatal("deferral took a delivery from the worker holding its lease")
	}
}

// D29. Cancel stops what is pending and reports, truthfully, what is in flight.
func TestCancel(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	u3 := ident("w1", "u3")
	nid := e.notification(u3, "remind:m1:start")
	email := e.enqueue(row{id: u3, notification: nid, shard: 80, channel: "email"})
	sms := e.enqueue(row{id: u3, notification: nid, shard: 81, channel: "sms"})
	e.claimOne(81) // a worker is mid-send on the SMS

	cases := []struct {
		name    string
		id      domain.Identity
		idemKey string
		want    domain.CancelReceipt
	}{
		{"unknown key", u3, "no-such-key", domain.CancelReceipt{}},
		{"another tenant's identical key", ident("w2", "u3"), "remind:m1:start", domain.CancelReceipt{}},
	}
	for _, tc := range cases {
		t.Run(tc.name+" matches nothing", func(t *testing.T) {
			t.Parallel()
			if r, err := e.s.Cancel(t.Context(), tc.id, tc.idemKey); err != nil || r != tc.want {
				t.Fatalf("receipt=%+v err=%v", r, err)
			}
		})
	}

	t.Run("pending stops, in-flight is reported, the inbox row is canceled", func(t *testing.T) {
		t.Parallel()
		r, err := e.s.Cancel(t.Context(), u3, "remind:m1:start")
		if err != nil {
			t.Fatal(err)
		}
		if r != (domain.CancelReceipt{Matched: true, Canceled: 1, InFlight: 1}) {
			t.Fatalf("receipt = %+v, want 1 canceled and 1 in flight", r)
		}
		if e.queued(email) || e.count(`SELECT count(*) FROM notification_deliveries
                                        WHERE id = $1::uuid AND outcome = 'canceled'`, email) != 1 {
			t.Error("the pending email was not recorded as canceled")
		}
		if !e.queued(sms) {
			t.Error("the in-flight SMS was taken from its worker")
		}
		if e.count(`SELECT count(*) FROM notifications WHERE id = $1::uuid AND canceled_at IS NOT NULL`, nid) != 1 {
			t.Error("the inbox row was not marked canceled")
		}
	})
}
