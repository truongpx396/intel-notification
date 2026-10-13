//go:build integration

package notifytest

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/truongpx396/intel-notification/domain"
)

// deadline bounds everything a case waits on that is concurrent, so a regression
// fails instead of hanging the suite.
const deadline = 15 * time.Second

// dispatcherFixture is one case's world: an Env, the contract's recipient and the
// helpers that read the queue, the history and the dead letters as the superuser
// (the engine's own connection would see only its scope, and a case asks what is
// really there).
type dispatcherFixture struct {
	t  *testing.T
	e  *Env
	id domain.Identity
}

func newDispatcherFixture(t *testing.T, newEnv func(*testing.T) *Env) *dispatcherFixture {
	t.Helper()
	e := newEnv(t)
	return &dispatcherFixture{t: t, e: e,
		id: domain.Identity{Realm: Realm, Tenant: contractTenant, Recipient: contractUser}}
}

// inApp registers the inbox-backed channel that needs no address.
func (f *dispatcherFixture) inApp() *Channel {
	return f.e.AddChannel("in_app", domain.ChannelCapabilities{InboxBacked: true, Dedup: domain.DedupLocal})
}

// email registers a channel that needs an address and dedupes at the provider, and
// gives the recipient one.
func (f *dispatcherFixture) email() *Channel {
	ch := f.e.AddChannel("email", domain.ChannelCapabilities{NeedsSubject: true, NeedsAddress: true,
		Dedup: domain.DedupProvider, DedupWindow: 24 * time.Hour})
	f.address("email", "u1@example.com")
	return ch
}

// address gives the recipient the addresses (values) on a channel.
func (f *dispatcherFixture) address(ch domain.ChannelKind, values ...string) {
	f.t.Helper()
	addrs := make([]domain.Address, len(values))
	for i, v := range values {
		addrs[i] = domain.Address{Channel: ch, Value: v}
	}
	f.e.Addresses.Put(f.id, ch, addrs...)
}

// topic registers a topic that fans out on channels, with an optional fallback chain.
func (f *dispatcherFixture) topic(name domain.Topic, channels []domain.ChannelKind, fallback ...domain.ChannelKind) {
	f.t.Helper()
	f.e.Topics.Register(Realm, name, domain.TopicDef{DefaultChannels: channels, Fallback: fallback,
		DefaultPriority: domain.PriorityInfo})
}

// note is a valid notification for id on topic.
func (f *dispatcherFixture) note(topic domain.Topic, key string) domain.Notification {
	return domain.Notification{Tenant: f.id.Tenant, Recipient: f.id.Recipient, Topic: topic, IdemKey: key,
		Title: "Done", Body: "Your upload finished.", OccurredAt: Epoch}
}

// notify sends n and fails the case unless it was accepted.
func (f *dispatcherFixture) notify(n domain.Notification) domain.Receipt {
	f.t.Helper()
	r, err := f.e.Notifier.Notify(f.t.Context(), n)
	if err != nil || !r.Applied {
		f.t.Fatalf("Notify(%s) = %+v, %v, want it applied", n.IdemKey, r, err)
	}
	return r
}

func (f *dispatcherFixture) cfg() domainConfig {
	c := f.e.Config.WithDefaults()
	return domainConfig{shards: c.Shards, batch: c.ClaimBatch, maxAttempts: c.MaxAttempts,
		base: c.BackoffBase, ceiling: c.BackoffCeiling}
}

type domainConfig struct {
	shards, batch, maxAttempts int
	base, ceiling              time.Duration
}

func (f *dispatcherFixture) shardOf(id domain.Identity) domain.Shard {
	return domain.ShardFor(id, f.cfg().shards)
}

// drain runs one batch on the recipient's shard and fails the case on an error.
func (f *dispatcherFixture) drain() int {
	f.t.Helper()
	n, err := f.e.Dispatcher.Drain(f.t.Context(), f.shardOf(f.id), f.cfg().batch)
	if err != nil {
		f.t.Fatalf("Drain: %v", err)
	}
	return n
}

// drainAll runs one batch on every shard and returns how many claims were handled.
func (f *dispatcherFixture) drainAll() int {
	f.t.Helper()
	total := 0
	for s := range f.cfg().shards {
		n, err := f.e.Dispatcher.Drain(f.t.Context(), domain.Shard(s), f.cfg().batch)
		if err != nil {
			f.t.Fatalf("Drain(shard %d): %v", s, err)
		}
		total += n
	}
	return total
}

// crashDrain runs one batch and returns the crash it died of, or nil. It stands in
// for the worker process being killed at that step.
func (f *dispatcherFixture) crashDrain() *Crash {
	f.t.Helper()
	return Survive(func() { _, _ = f.e.Dispatcher.Drain(f.t.Context(), f.shardOf(f.id), f.cfg().batch) })
}

func (f *dispatcherFixture) exec(sql string, args ...any) {
	f.t.Helper()
	if _, err := f.e.DB.Admin.Exec(f.t.Context(), sql, args...); err != nil {
		f.t.Fatalf("%s: %v", sql, err)
	}
}

// lapse ends every lease at once: the workers holding them died, or stalled, long
// enough. A case cannot wait out ClaimLease, and does not need to.
func (f *dispatcherFixture) lapse() {
	f.t.Helper()
	f.exec(`UPDATE notification_outbox SET lease_expires_at = now() - interval '1 second'
	         WHERE lease_expires_at IS NOT NULL`)
}

// makeDue makes every pending delivery claimable now. A retry is scheduled on the
// Env's fake clock, which is years from the database's own, so a case that has read
// the due time back moves it to the present to drive the next attempt.
func (f *dispatcherFixture) makeDue() {
	f.t.Helper()
	f.exec(`UPDATE notification_outbox SET next_attempt_at = now() - interval '1 second'`)
}

type queued struct {
	ID, Channel, AddressKey, LastError string
	Attempts, Deferrals                int
	NextAttemptAt                      time.Time
	Leased                             bool
	Fallback                           []string
	HasAddress                         bool
}

// queue lists the pending deliveries, ordered.
func (f *dispatcherFixture) queue() []queued {
	f.t.Helper()
	rows, err := f.e.DB.Admin.Query(f.t.Context(), `
		SELECT id::text, channel, address_key, coalesce(last_error, ''), attempts, deferrals, next_attempt_at,
		       coalesce(lease_expires_at > now(), false), fallback, address IS NOT NULL
		  FROM notification_outbox ORDER BY channel, address_key, id`)
	if err != nil {
		f.t.Fatalf("read the queue: %v", err)
	}
	defer rows.Close()
	var out []queued
	for rows.Next() {
		var q queued
		if err := rows.Scan(&q.ID, &q.Channel, &q.AddressKey, &q.LastError, &q.Attempts, &q.Deferrals,
			&q.NextAttemptAt, &q.Leased, &q.Fallback, &q.HasAddress); err != nil {
			f.t.Fatal(err)
		}
		out = append(out, q)
	}
	if err := rows.Err(); err != nil {
		f.t.Fatal(err)
	}
	return out
}

type finished struct {
	Channel, AddressKey, Outcome, ProviderMessageID, Detail string
	Attempts                                                int
}

// history lists the finished deliveries, ordered, without the rows that record an
// address fan-out unless withFanOut.
func (f *dispatcherFixture) history(withFanOut bool) []finished {
	f.t.Helper()
	rows, err := f.e.DB.Admin.Query(f.t.Context(), `
		SELECT channel, address_key, outcome, coalesce(provider_message_id, ''), coalesce(detail, ''), attempts
		  FROM notification_deliveries WHERE $1 OR outcome <> 'fanned_out'
		 ORDER BY channel, address_key, outcome, completed_at`, withFanOut)
	if err != nil {
		f.t.Fatalf("read the history: %v", err)
	}
	defer rows.Close()
	var out []finished
	for rows.Next() {
		var h finished
		if err := rows.Scan(&h.Channel, &h.AddressKey, &h.Outcome, &h.ProviderMessageID, &h.Detail, &h.Attempts); err != nil {
			f.t.Fatal(err)
		}
		out = append(out, h)
	}
	if err := rows.Err(); err != nil {
		f.t.Fatal(err)
	}
	return out
}

// outcomes lists the outcome of each finished delivery on a channel.
func (f *dispatcherFixture) outcomes(ch domain.ChannelKind) []string {
	f.t.Helper()
	var out []string
	for _, h := range f.history(false) {
		if h.Channel == string(ch) {
			out = append(out, h.Outcome)
		}
	}
	return out
}

type parked struct {
	Channel, Reason, LastError string
	Attempts                   int
}

func (f *dispatcherFixture) deadLetters() []parked {
	f.t.Helper()
	rows, err := f.e.DB.Admin.Query(f.t.Context(),
		`SELECT coalesce(channel, ''), reason, last_error, attempts FROM dead_letters ORDER BY created_at, id`)
	if err != nil {
		f.t.Fatalf("read the dead letters: %v", err)
	}
	defer rows.Close()
	var out []parked
	for rows.Next() {
		var p parked
		if err := rows.Scan(&p.Channel, &p.Reason, &p.LastError, &p.Attempts); err != nil {
			f.t.Fatal(err)
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		f.t.Fatal(err)
	}
	return out
}

// wantNoAlarm fails if the dead-letter alarm was raised: a correct ending is not a
// failure, and the alarm that means "someone was not told" must stay meaningful (D8).
func (f *dispatcherFixture) wantNoAlarm(because string) {
	f.t.Helper()
	if n := f.e.Metrics.Count(domain.MetricDeadLetter); n != 0 {
		f.t.Errorf("%s raised %s %d times: %s", domain.MetricDeadLetter, domain.MetricDeadLetter, n, because)
	}
	if dl := f.deadLetters(); len(dl) != 0 {
		f.t.Errorf("dead letters = %+v, want none: %s", dl, because)
	}
}

// wantEmpty fails if anything is still pending.
func (f *dispatcherFixture) wantEmpty(because string) {
	f.t.Helper()
	if q := f.queue(); len(q) != 0 {
		f.t.Fatalf("queue = %+v, want it empty: %s", q, because)
	}
}

// emailKey is the key a channel dedupes the recipient's email on (D25).
func (f *dispatcherFixture) emailKey(idemKey, address string) string {
	return domain.DeliveryIdemKey(f.id, idemKey, "", "email", domain.AddressKey("email", address))
}

// backoff is the delay the Env's dispatcher chooses after the given attempt.
func (f *dispatcherFixture) backoff(attempt int, retryAfter time.Duration) time.Duration {
	c := f.cfg()
	return domain.Backoff(attempt, c.base, c.ceiling, retryAfter, func() float64 { return Jitter })
}

// await waits for ch to close, or fails the case at the deadline.
func await(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(deadline):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func sameStrings(got []string, want ...string) bool {
	return slices.Equal(slices.Sorted(slices.Values(got)), slices.Sorted(slices.Values(want)))
}

// DispatcherContract drives the Dispatcher through every crash point and every
// ending (T033), against the real queue and store on the Env's PostgreSQL. Each
// case builds its own Env. It asserts NS-002, NS-003 and NS-006 through the worker:
// that a delivery is sent once at the level its channel declares however the worker
// dies, that poison terminates, that every ending is the right one and only a
// genuine failure raises the alarm, and that a stale worker's write is discarded.
func DispatcherContract(t *testing.T, newEnv func(t *testing.T) *Env) {
	t.Helper()

	t.Run("a delivery is sent once and recorded as delivered (NR-004)", func(t *testing.T) {
		t.Parallel()
		f := newDispatcherFixture(t, newEnv)
		f.inApp()
		email := NewScripted("email", domain.ChannelCapabilities{NeedsSubject: true, NeedsAddress: true, Dedup: domain.DedupProvider, DedupWindow: time.Hour})
		f.e.Channels.Register(email)
		f.address("email", "u1@example.com")
		f.e.Templates.Put(Realm, "ingestion_complete", "email", "en", domain.RenderedContent{Subject: "Upload done", Body: "It finished.", TemplateVersion: 3})
		f.topic("ingestion_complete", []domain.ChannelKind{"in_app", "email"})
		receipt := f.notify(f.note("ingestion_complete", "k1"))

		if n := f.drain(); n != 2 {
			t.Fatalf("Drain handled %d deliveries, want the two channels", n)
		}
		f.wantEmpty("both deliveries finished")
		got := f.history(false)
		if len(got) != 2 || got[0].Channel != "email" || got[1].Channel != "in_app" {
			t.Fatalf("history = %+v, want email and in_app", got)
		}
		key := f.emailKey("k1", "u1@example.com")
		wantAddressKey := domain.AddressKey("email", "u1@example.com")
		if got[0].Outcome != "delivered" || got[0].ProviderMessageID != "msg-"+key || got[0].AddressKey != wantAddressKey || got[0].Attempts != 1 {
			t.Errorf("email history = %+v, want delivered as msg-%s on the bound address %s at attempt 1", got[0], key, wantAddressKey)
		}
		if got[1].Outcome != "delivered" || got[1].AddressKey != "" {
			t.Errorf("in_app history = %+v, want delivered with no address", got[1])
		}

		sent := email.Deliveries()
		if len(sent) != 1 {
			t.Fatalf("the email channel was asked %d times, want once", len(sent))
		}
		d := sent[0]
		if d.IdemKey != key {
			t.Errorf("IdemKey = %s, want %s: the key is per address and per notification, not the producer's (D25)", d.IdemKey, key)
		}
		if d.ID == "" || d.Identity != f.id || d.NotificationID != receipt.NotificationID || d.DigestID != "" ||
			d.Topic != "ingestion_complete" || d.Channel != "email" || d.Attempt != 1 {
			t.Errorf("Delivery = %+v, want the queue row, the identity, the notification %s and attempt 1", d, receipt.NotificationID)
		}
		if d.Notification.IdemKey != "k1" || d.Notification.Title != "Done" || len(d.Digest) != 0 {
			t.Errorf("Delivery.Notification = %+v and %d members, want the notification itself and no digest", d.Notification, len(d.Digest))
		}
		if d.Address.Value != "u1@example.com" || d.Address.Channel != "email" {
			t.Errorf("Delivery.Address = %+v, want the recipient's address", d.Address)
		}
		if d.Content.Subject != "Upload done" || d.Content.Body != "It finished." || d.Content.TemplateVersion != 3 {
			t.Errorf("Delivery.Content = %+v, want what the template rendered", d.Content)
		}
		f.wantNoAlarm("a delivery that went out is not a failure")
	})

	// NS-003: every enabled channel is eventually delivered whatever step the worker
	// dies at, and a channel that dedupes sees one send. In-app has no address, so it
	// shows the steps with nothing to do at the address step and a fence with nothing
	// before it.
	for _, step := range domain.AllDispatchSteps() {
		for _, ch := range []domain.ChannelKind{"email", "in_app"} {
			t.Run(fmt.Sprintf("a crash at %s on %s is re-driven after the lease lapses and sends once (NS-003)", step, ch), func(t *testing.T) {
				t.Parallel()
				f := newDispatcherFixture(t, newEnv)
				var probeKey string
				if ch == "email" {
					f.email()
					f.topic("t", []domain.ChannelKind{"email"})
					probeKey = f.emailKey("k1", "u1@example.com")
				} else {
					f.inApp()
					f.topic("t", []domain.ChannelKind{"in_app"})
					probeKey = domain.DeliveryIdemKey(f.id, "k1", "", "in_app", "")
				}
				f.notify(f.note("t", "k1"))

				f.e.Faults.PanicAt(step)
				crash := f.crashDrain()
				if crash == nil || crash.Step != step || f.e.Faults.Pending() != 0 {
					t.Fatalf("the worker did not die at %s: crash %v, %d faults pending", step, crash, f.e.Faults.Pending())
				}
				// A delivery whose only write was the last one is already finished: the
				// worker died after it, with nothing left to drive.
				finishedBeforeDying := ch == "in_app" && step == domain.StepFence
				if finishedBeforeDying {
					f.wantEmpty("the worker died after recording the outcome")
				} else if n := f.drain(); n != 0 {
					t.Fatalf("a second worker handled %d deliveries while the dead worker's lease was live, want none", n)
				}

				f.lapse()
				f.drain()
				f.wantEmpty("the re-drive finished it")
				got := f.outcomes(ch)
				if !sameStrings(got, "delivered") {
					t.Fatalf("outcomes on %s = %v, want one delivered", ch, got)
				}
				if sends := f.e.Probe.Sends(probeKey); sends != 1 {
					t.Fatalf("the provider side saw %d sends of %s, want 1: the channel dedupes and the worker re-drove with the same key", sends, probeKey)
				}
				wantAttempts := 2
				if finishedBeforeDying {
					wantAttempts = 1
				}
				if h := f.history(false); len(h) != 1 || h[0].Attempts != wantAttempts {
					t.Fatalf("history = %+v, want one row at attempt %d: every claim counts, a crash included", h, wantAttempts)
				}
				f.wantNoAlarm("a re-driven delivery is not a failure")
			})
		}
	}

	t.Run("crash after send, before record: re-driven with the SAME Delivery.IdemKey (D25)", func(t *testing.T) {
		t.Parallel()
		f := newDispatcherFixture(t, newEnv)
		email := NewScripted("email", domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupNone})
		f.e.Channels.Register(email)
		f.address("email", "u1@example.com")
		f.topic("t", []domain.ChannelKind{"email"})
		f.e.Templates.Put(Realm, "t", "email", "en", domain.RenderedContent{Subject: "S", Body: "B"})
		f.notify(f.note("t", "k1"))

		f.e.Faults.PanicAt(domain.StepRecord)
		if crash := f.crashDrain(); crash == nil || crash.Step != domain.StepRecord {
			t.Fatalf("the worker did not die after sending: %v", crash)
		}
		if got := len(email.Deliveries()); got != 1 {
			t.Fatalf("the channel was asked %d times before the crash, want once", got)
		}
		f.lapse()
		f.drain()

		sent := email.Deliveries()
		if len(sent) != 2 {
			t.Fatalf("the channel was asked %d times, want twice: a DedupNone channel is at-least-once by declaration", len(sent))
		}
		if sent[0].IdemKey != sent[1].IdemKey || sent[0].IdemKey != f.emailKey("k1", "u1@example.com") {
			t.Fatalf("keys %s and %s: the re-drive must present the same key, or no provider could collapse it", sent[0].IdemKey, sent[1].IdemKey)
		}
		if sent[0].Attempt != 1 || sent[1].Attempt != 2 {
			t.Fatalf("attempts %d and %d, want 1 and 2", sent[0].Attempt, sent[1].Attempt)
		}
		if !reflect.DeepEqual(sent[0].Address, sent[1].Address) || sent[0].Address.Value != "u1@example.com" {
			t.Fatalf("addresses %+v and %+v, want the one bound at the first claim (D25)", sent[0].Address, sent[1].Address)
		}
		if !sameStrings(f.outcomes("email"), "delivered") {
			t.Fatalf("outcomes = %v, want one delivered", f.outcomes("email"))
		}
	})

	t.Run("a delivery that crashes its worker every time terminates unsent at the ceiling (D19, NS-006)", func(t *testing.T) {
		t.Parallel()
		f := newDispatcherFixture(t, newEnv)
		email := NewScripted("email", domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupNone})
		f.e.Channels.Register(email)
		f.address("email", "u1@example.com")
		f.topic("t", []domain.ChannelKind{"email"})
		f.e.Templates.Put(Realm, "t", "email", "en", domain.RenderedContent{Subject: "S", Body: "B"})
		f.notify(f.note("t", "k1"))

		ceiling := f.cfg().maxAttempts
		for i := 1; i <= ceiling; i++ {
			f.e.Faults.PanicAt(domain.StepRecord) // the poison kills the worker right after sending
			if crash := f.crashDrain(); crash == nil {
				t.Fatalf("attempt %d did not kill the worker", i)
			}
			f.lapse()
		}
		if got := len(email.Deliveries()); got != ceiling {
			t.Fatalf("%d sends before the ceiling, want %d", got, ceiling)
		}

		f.drain()
		if got := len(email.Deliveries()); got != ceiling {
			t.Fatalf("%d sends after the ceiling, want %d: a claim beyond MaxAttempts must finish the delivery, not send it again", got, ceiling)
		}
		f.wantEmpty("the poison was parked")
		if !sameStrings(f.outcomes("email"), "max_attempts") {
			t.Fatalf("outcomes = %v, want max_attempts", f.outcomes("email"))
		}
		dl := f.deadLetters()
		if len(dl) != 1 || dl[0].Reason != "max_attempts" || dl[0].Channel != "email" || dl[0].LastError == "" {
			t.Fatalf("dead letters = %+v, want one max_attempts on email with a last error", dl)
		}
		if n := f.e.Metrics.CountWith(domain.MetricDeadLetter, map[string]string{domain.LabelChannel: "email", domain.LabelReason: "max_attempts"}); n != 1 {
			t.Fatalf("%s{email, max_attempts} = %d, want 1", domain.MetricDeadLetter, n)
		}
	})

	t.Run("a channel that panics is a retry, and its poison still terminates (D19)", func(t *testing.T) {
		t.Parallel()
		f := newDispatcherFixture(t, newEnv)
		email := NewScripted("email", domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupNone}).
			OnDeliver(func(context.Context, domain.Delivery) { panic("template engine blew up") })
		f.e.Channels.Register(email)
		f.address("email", "u1@example.com")
		f.topic("t", []domain.ChannelKind{"email"})
		f.e.Templates.Put(Realm, "t", "email", "en", domain.RenderedContent{Subject: "S", Body: "B"})
		f.notify(f.note("t", "k1"))

		f.drain() // the worker survives the channel's panic
		q := f.queue()
		if len(q) != 1 || q[0].Attempts != 1 || !strings.Contains(q[0].LastError, "template engine blew up") {
			t.Fatalf("queue = %+v, want the delivery retried once, naming the panic", q)
		}
		for range f.cfg().maxAttempts - 1 {
			f.makeDue()
			f.drain()
		}
		f.wantEmpty("the ceiling was reached")
		dl := f.deadLetters()
		if len(dl) != 1 || dl[0].Reason != "max_attempts" || !strings.Contains(dl[0].LastError, "template engine blew up") {
			t.Fatalf("dead letters = %+v, want one max_attempts whose last error names the panic", dl)
		}
		if got := len(email.Deliveries()); got != f.cfg().maxAttempts {
			t.Fatalf("%d sends, want %d", got, f.cfg().maxAttempts)
		}
	})

	t.Run("a transient failure retries under full-jitter backoff, floored by the provider's hint (D10)", func(t *testing.T) {
		t.Parallel()
		f := newDispatcherFixture(t, newEnv)
		email := NewScripted("email", domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupNone}).
			Then(domain.DeliveryResult{Outcome: domain.Retry, Detail: "greylisted"}, nil).
			Then(domain.DeliveryResult{Outcome: domain.Retry, Detail: "greylisted again"}, nil).
			Then(domain.DeliveryResult{Outcome: domain.Retry, RetryAfter: 2 * time.Hour, Detail: "slow down"}, nil)
		f.e.Channels.Register(email)
		f.address("email", "u1@example.com")
		f.topic("t", []domain.ChannelKind{"email"})
		f.e.Templates.Put(Realm, "t", "email", "en", domain.RenderedContent{Subject: "S", Body: "B"})
		f.notify(f.note("t", "k1"))

		cases := []struct {
			attempt    int
			retryAfter time.Duration
			lastError  string
			why        string
		}{
			{1, 0, "greylisted", "the first retry is jittered inside the base window"},
			{2, 0, "greylisted again", "the window doubles with the attempt"},
			{3, 2 * time.Hour, "slow down", "a provider's Retry-After is a floor above the jittered delay"},
		}
		for _, tc := range cases {
			f.drain()
			q := f.queue()
			if len(q) != 1 {
				t.Fatalf("attempt %d: queue = %+v, want the one delivery waiting", tc.attempt, q)
			}
			want := Epoch.Add(f.backoff(tc.attempt, tc.retryAfter))
			if !q[0].NextAttemptAt.Equal(want) || q[0].Attempts != tc.attempt || q[0].Leased || q[0].LastError != tc.lastError {
				t.Fatalf("attempt %d: row = %+v, want due at %v with attempt %d, no lease and last error %q: %s",
					tc.attempt, q[0], want, tc.attempt, tc.lastError, tc.why)
			}
			f.makeDue()
		}
		f.drain()
		if !sameStrings(f.outcomes("email"), "delivered") || len(email.Deliveries()) != 4 {
			t.Fatalf("outcomes %v after %d sends, want delivered after the fourth", f.outcomes("email"), len(email.Deliveries()))
		}
		f.wantNoAlarm("a delivery that needed retries is not a failure")
	})

	t.Run("the attempt ceiling parks a delivery that keeps failing, with its last error (NR-007)", func(t *testing.T) {
		t.Parallel()
		f := newDispatcherFixture(t, newEnv)
		f.email()
		f.topic("t", []domain.ChannelKind{"email"})
		f.notify(f.note("t", "k1"))
		key := f.emailKey("k1", "u1@example.com")

		ceiling := f.cfg().maxAttempts
		for i := 1; i <= ceiling; i++ {
			f.e.Probe.FailNext(Transient)
			f.drain()
			if i < ceiling {
				if q := f.queue(); len(q) != 1 || q[0].Attempts != i {
					t.Fatalf("after attempt %d the queue = %+v, want the delivery waiting at attempt %d", i, q, i)
				}
				f.makeDue()
			}
		}
		f.wantEmpty("the ceiling was reached")
		if f.e.Probe.Calls(key) != ceiling {
			t.Fatalf("%d sends, want %d: no more than the ceiling", f.e.Probe.Calls(key), ceiling)
		}
		dl := f.deadLetters()
		if len(dl) != 1 || dl[0].Reason != "max_attempts" || dl[0].Attempts != ceiling || dl[0].LastError != "injected transient failure" {
			t.Fatalf("dead letters = %+v, want one max_attempts at attempt %d whose last error is the channel's", dl, ceiling)
		}
		if n := f.e.Metrics.CountWith(domain.MetricDeadLetter, map[string]string{domain.LabelChannel: "email", domain.LabelReason: "max_attempts"}); n != 1 {
			t.Fatalf("%s = %d, want 1", domain.MetricDeadLetter, n)
		}
	})

	t.Run("a permanent refusal is parked at once with its reason and last error (D12)", func(t *testing.T) {
		t.Parallel()
		f := newDispatcherFixture(t, newEnv)
		f.email()
		f.topic("t", []domain.ChannelKind{"email"})
		f.notify(f.note("t", "k1"))
		key := f.emailKey("k1", "u1@example.com")

		f.e.Probe.FailNext(Permanent)
		f.drain()
		f.wantEmpty("a refusal is terminal")
		if f.e.Probe.Calls(key) != 1 {
			t.Fatalf("%d sends, want 1: a permanent refusal is not retried", f.e.Probe.Calls(key))
		}
		dl := f.deadLetters()
		if len(dl) != 1 || dl[0].Reason != "rejected" || dl[0].Attempts != 1 || dl[0].LastError != "injected permanent refusal" {
			t.Fatalf("dead letters = %+v, want one rejected at attempt 1 with the channel's detail", dl)
		}
		if n := f.e.Metrics.CountWith(domain.MetricDeadLetter, map[string]string{domain.LabelChannel: "email", domain.LabelReason: "rejected"}); n != 1 {
			t.Fatalf("%s = %d, want 1", domain.MetricDeadLetter, n)
		}
	})

	t.Run("a suppressed ending and an unresolvable address raise no alarm (NR-006, NR-007)", func(t *testing.T) {
		t.Parallel()
		f := newDispatcherFixture(t, newEnv)
		f.email()
		f.topic("t", []domain.ChannelKind{"email"})
		f.notify(f.note("t", "k1"))
		f.e.Probe.FailNext(DeadAddress)
		f.drain()
		if !sameStrings(f.outcomes("email"), "suppressed") {
			t.Fatalf("outcomes = %v, want suppressed", f.outcomes("email"))
		}
		f.wantNoAlarm("a hard bounce is the engine working")

		g := newDispatcherFixture(t, newEnv)
		g.e.AddChannel("email", domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupProvider, DedupWindow: time.Hour})
		g.topic("t", []domain.ChannelKind{"email"}) // and the recipient has no address
		g.notify(g.note("t", "k1"))
		g.drain()
		if !sameStrings(g.outcomes("email"), "no_address") {
			t.Fatalf("outcomes = %v, want no_address", g.outcomes("email"))
		}
		g.wantNoAlarm("a recipient with no address is not a failure")
	})

	t.Run("a channel's Suppressed is recorded with its expiry, then finished (D13, D14)", func(t *testing.T) {
		t.Parallel()
		f := newDispatcherFixture(t, newEnv)
		email := NewScripted("email", domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupNone}).
			Then(domain.DeliveryResult{Outcome: domain.Suppressed, SuppressReason: domain.SuppressSoftBounce, SuppressFor: 6 * time.Hour}, nil)
		f.e.Channels.Register(email)
		f.address("email", "u1@example.com")
		f.topic("t", []domain.ChannelKind{"email"})
		f.e.Templates.Put(Realm, "t", "email", "en", domain.RenderedContent{Subject: "S", Body: "B"})
		f.notify(f.note("t", "k1"))
		f.drain()

		hash := domain.SuppressionHash("email", "u1@example.com")
		live := func() bool {
			ok, err := f.e.Suppressions.Suppressed(f.t.Context(), Realm, "email", hash)
			if err != nil {
				t.Fatal(err)
			}
			return ok
		}
		if !live() {
			t.Fatal("the address is not suppressed after the channel reported it dead")
		}
		f.e.Clock.Advance(5*time.Hour + 59*time.Minute)
		if !live() {
			t.Fatal("the suppression lapsed before SuppressFor")
		}
		f.e.Clock.Advance(2 * time.Minute)
		if live() {
			t.Fatal("the suppression outlived SuppressFor")
		}
		f.wantNoAlarm("a suppression is a correct ending")
	})

	t.Run("a suppressed address is never handed to the channel (D13)", func(t *testing.T) {
		t.Parallel()
		f := newDispatcherFixture(t, newEnv)
		f.email()
		f.topic("t", []domain.ChannelKind{"email"})
		if err := f.e.Suppressions.Suppress(t.Context(), Realm, "email", domain.SuppressionHash("email", "u1@example.com"),
			domain.SuppressHardBounce, time.Time{}); err != nil {
			t.Fatal(err)
		}
		f.notify(f.note("t", "k1"))
		f.drain()

		if calls := f.e.Probe.Calls(f.emailKey("k1", "u1@example.com")); calls != 0 {
			t.Fatalf("the channel was asked %d times to send to a suppressed address, want never", calls)
		}
		if !sameStrings(f.outcomes("email"), "suppressed") {
			t.Fatalf("outcomes = %v, want suppressed", f.outcomes("email"))
		}
		f.wantEmpty("suppressed is terminal")
		f.wantNoAlarm("a suppressed address is a correct ending")
	})

	t.Run("a suppression written after the address was bound is still honoured (D13)", func(t *testing.T) {
		t.Parallel()
		f := newDispatcherFixture(t, newEnv)
		f.email()
		f.topic("t", []domain.ChannelKind{"email"})
		f.notify(f.note("t", "k1"))
		f.e.Probe.FailNext(Transient)
		f.drain() // binds the address, and the send fails
		if q := f.queue(); len(q) != 1 || !q[0].HasAddress {
			t.Fatalf("queue = %+v, want the delivery waiting with its address bound", q)
		}
		// A bounce lands while the delivery waits for its retry.
		if err := f.e.Suppressions.Suppress(t.Context(), Realm, "email", domain.SuppressionHash("email", "u1@example.com"),
			domain.SuppressComplaint, time.Time{}); err != nil {
			t.Fatal(err)
		}
		f.makeDue()
		f.drain()
		if calls := f.e.Probe.Calls(f.emailKey("k1", "u1@example.com")); calls != 1 {
			t.Fatalf("the channel was asked %d times, want only the first: the retry must see the suppression", calls)
		}
		if !sameStrings(f.outcomes("email"), "suppressed") {
			t.Fatalf("outcomes = %v, want suppressed", f.outcomes("email"))
		}
	})

	// T031. The dispatcher checks suppressions before every delivery, on every
	// channel, and never by name: a channel written next year is checked the same.
	for _, kind := range []domain.ChannelKind{"email", "sms", "push", "slack", "pager"} {
		t.Run("a live suppression means the "+string(kind)+" channel is never called (NR-006, D13)", func(t *testing.T) {
			t.Parallel()
			f := newDispatcherFixture(t, newEnv)
			ch := f.e.AddChannel(kind, domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupNone})
			_ = ch
			f.address(kind, "target-1")
			f.topic("t", []domain.ChannelKind{kind})
			f.notify(f.note("t", "k1"))
			hash := domain.SuppressionHash(kind, "target-1")
			if err := f.e.Suppressions.Suppress(t.Context(), Realm, kind, hash, domain.SuppressUnsubscribe, time.Time{}); err != nil {
				t.Fatal(err)
			}

			f.drain()
			if f.e.Probe.Total() != 0 || len(f.e.Probe.Keys()) != 0 {
				t.Fatalf("the %s channel was asked to send to a suppressed address: keys %v", kind, f.e.Probe.Keys())
			}
			if !sameStrings(f.outcomes(kind), "suppressed") {
				t.Fatalf("outcomes = %v, want suppressed", f.outcomes(kind))
			}
			f.wantNoAlarm("a suppressed address is a correct ending")
		})
	}

	t.Run("a suppression of another channel, or another realm, does not stop a delivery (D13)", func(t *testing.T) {
		t.Parallel()
		f := newDispatcherFixture(t, newEnv)
		f.email()
		f.topic("t", []domain.ChannelKind{"email"})
		f.notify(f.note("t", "k1"))
		for _, k := range []struct {
			realm domain.Realm
			ch    domain.ChannelKind
		}{{Realm, "sms"}, {"other-product", "email"}} {
			if err := f.e.Suppressions.Suppress(t.Context(), k.realm, k.ch, domain.SuppressionHash("email", "u1@example.com"),
				domain.SuppressHardBounce, time.Time{}); err != nil {
				t.Fatal(err)
			}
		}
		f.drain()
		if !sameStrings(f.outcomes("email"), "delivered") {
			t.Fatalf("outcomes = %v, want delivered: a suppression is per realm and per channel", f.outcomes("email"))
		}
	})

	t.Run("an expired suppression no longer stops a delivery (D14)", func(t *testing.T) {
		t.Parallel()
		f := newDispatcherFixture(t, newEnv)
		f.email()
		f.topic("t", []domain.ChannelKind{"email"})
		f.notify(f.note("t", "k1"))
		if err := f.e.Suppressions.Suppress(t.Context(), Realm, "email", domain.SuppressionHash("email", "u1@example.com"),
			domain.SuppressSoftBounce, Epoch.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		f.e.Clock.Set(Epoch.Add(time.Hour)) // a suppression ends at its expiry
		f.drain()
		if !sameStrings(f.outcomes("email"), "delivered") {
			t.Fatalf("outcomes = %v, want delivered once the cooldown is over", f.outcomes("email"))
		}
	})

	t.Run("one suppressed device does not stop the recipient's others (D25)", func(t *testing.T) {
		t.Parallel()
		f := newDispatcherFixture(t, newEnv)
		f.e.AddChannel("push", domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupNone})
		f.address("push", "phone", "tablet", "watch")
		f.topic("t", []domain.ChannelKind{"push"})
		if err := f.e.Suppressions.Suppress(t.Context(), Realm, "push", domain.SuppressionHash("push", "tablet"),
			domain.SuppressTokenRevoked, time.Time{}); err != nil {
			t.Fatal(err)
		}
		f.notify(f.note("t", "k1"))
		f.drain()
		if q := f.queue(); len(q) != 2 {
			t.Fatalf("queue = %+v, want a row for each device still reachable", q)
		}
		f.drain()
		if got := f.outcomes("push"); !sameStrings(got, "delivered", "delivered") {
			t.Fatalf("outcomes = %v, want the two live devices delivered", got)
		}
		tabletKey := domain.DeliveryIdemKey(f.id, "k1", "", "push", domain.AddressKey("push", "tablet"))
		if f.e.Probe.Calls(tabletKey) != 0 {
			t.Fatal("the suppressed device was sent to")
		}
	})

	t.Run("two spellings of one address are one address, not a fan-out (D25)", func(t *testing.T) {
		t.Parallel()
		f := newDispatcherFixture(t, newEnv)
		f.email()
		f.address("email", "u1@example.com", " u1@example.com") // the channel trims, so these are equal
		f.topic("t", []domain.ChannelKind{"email"})
		f.notify(f.note("t", "k1"))
		f.drain()
		if got := f.outcomes("email"); !sameStrings(got, "delivered") {
			t.Fatalf("outcomes = %v, want one delivery", got)
		}
		if h := f.history(true); len(h) != 1 {
			t.Fatalf("history = %+v, want the one delivery and no fan-out record", h)
		}
	})

	t.Run("a channel with no address that reports Suppressed does not silence the realm (D13)", func(t *testing.T) {
		t.Parallel()
		f := newDispatcherFixture(t, newEnv)
		feed := NewScripted("feed", domain.ChannelCapabilities{InboxBacked: true, Dedup: domain.DedupLocal}).
			Then(domain.DeliveryResult{Outcome: domain.Suppressed, SuppressReason: domain.SuppressHardBounce}, nil)
		f.e.Channels.Register(feed)
		f.topic("t", []domain.ChannelKind{"feed"})
		f.notify(f.note("t", "k1"))
		f.drain()
		if !sameStrings(f.outcomes("feed"), "suppressed") {
			t.Fatalf("outcomes = %v, want suppressed", f.outcomes("feed"))
		}
		silenced, err := f.e.Suppressions.Suppressed(t.Context(), Realm, "feed", domain.SuppressionHash("feed", ""))
		if err != nil || silenced {
			t.Fatalf("the empty address hash is suppressed (%v, %v): it would silence the channel for every recipient of the realm", silenced, err)
		}
	})

	t.Run("a worker that dies right after a Suppressed ending has already recorded the suppression (D13)", func(t *testing.T) {
		t.Parallel()
		f := newDispatcherFixture(t, newEnv)
		email := NewScripted("email", domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupNone}).
			Then(domain.DeliveryResult{Outcome: domain.Retry, Detail: "greylisted"}, nil).
			Then(domain.DeliveryResult{Outcome: domain.Suppressed, SuppressReason: domain.SuppressHardBounce}, nil)
		f.e.Channels.Register(email)
		f.address("email", "u1@example.com")
		f.topic("t", []domain.ChannelKind{"email"})
		f.e.Templates.Put(Realm, "t", "email", "en", domain.RenderedContent{Subject: "S", Body: "B"})
		f.notify(f.note("t", "k1"))

		f.drain() // binds the address (a write the fence step sees) and is told to retry
		f.makeDue()
		f.e.Faults.PanicAt(domain.StepFence)
		if crash := f.crashDrain(); crash == nil || crash.Step != domain.StepFence {
			t.Fatalf("the worker did not die at the fence: %v", crash)
		}
		// Whichever write came last, the worker died after it. A suppression written
		// after the finish would be the one that is missing: the delivery is over and
		// nothing would ever write it, so the next notification mails a dead address.
		f.wantEmpty("the delivery was finished before the worker died")
		ok, err := f.e.Suppressions.Suppressed(t.Context(), Realm, "email", domain.SuppressionHash("email", "u1@example.com"))
		if err != nil || !ok {
			t.Fatalf("Suppressed = %v, %v: a delivery that ended as suppressed left no suppression behind", ok, err)
		}
	})

	t.Run("address equality is the channel's: for the key, the suppression check and the record (D25, D13)", func(t *testing.T) {
		t.Parallel()
		f := newDispatcherFixture(t, newEnv)
		lower := func(v string) string { return strings.ToLower(strings.TrimSpace(v)) }
		scripted := NewScripted("email", domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupNone}).
			Then(domain.DeliveryResult{Outcome: domain.Suppressed, SuppressReason: domain.SuppressHardBounce}, nil)
		f.e.Channels.Register(NewNormalizing(scripted, lower))
		f.address("email", "U1@Example.COM", " u1@example.com") // two spellings of one address
		f.topic("t", []domain.ChannelKind{"email"})
		f.e.Templates.Put(Realm, "t", "email", "en", domain.RenderedContent{Subject: "S", Body: "B"})
		f.notify(f.note("t", "k1"))
		f.drain()

		// One address, not a fan-out; keyed by the normalized value.
		h := f.history(true)
		wantKey := domain.AddressKey("email", "u1@example.com")
		if len(h) != 1 || h[0].Outcome != "suppressed" || h[0].AddressKey != wantKey {
			t.Fatalf("history = %+v, want one suppressed delivery on the address keyed %s", h, wantKey)
		}
		// Recorded under the normalized address, so that spelling and every other is
		// found by the check.
		for value, want := range map[string]bool{"u1@example.com": true, "U1@Example.COM": false} {
			got, err := f.e.Suppressions.Suppressed(t.Context(), Realm, "email", domain.SuppressionHash("email", value))
			if err != nil || got != want {
				t.Fatalf("Suppressed(hash of %q) = %v, %v, want %v: the suppression is written under the channel's normalized address", value, got, err, want)
			}
		}
		// And checked under it: the next notification is stopped without the channel.
		f.notify(f.note("t", "k2"))
		f.drain()
		if n := len(scripted.Deliveries()); n != 1 {
			t.Fatalf("the channel was asked %d times, want only for the first notification: the second address spelling must be found suppressed", n)
		}
		if got := f.outcomes("email"); !sameStrings(got, "suppressed", "suppressed") {
			t.Fatalf("outcomes = %v, want both suppressed", got)
		}
	})

	t.Run("a Suppressed with no valid reason is recorded as an invalid address", func(t *testing.T) {
		t.Parallel()
		f := newDispatcherFixture(t, newEnv)
		email := NewScripted("email", domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupNone}).
			Then(domain.DeliveryResult{Outcome: domain.Suppressed, SuppressReason: "made_up"}, nil)
		f.e.Channels.Register(email)
		f.address("email", "u1@example.com")
		f.topic("t", []domain.ChannelKind{"email"})
		f.e.Templates.Put(Realm, "t", "email", "en", domain.RenderedContent{Subject: "S", Body: "B"})
		f.notify(f.note("t", "k1"))
		f.drain()

		var reason string
		err := f.e.DB.Admin.QueryRow(t.Context(),
			`SELECT reason FROM channel_suppressions WHERE realm = $1 AND channel = 'email' AND address_hash = $2`,
			string(Realm), domain.SuppressionHash("email", "u1@example.com")).Scan(&reason)
		if err != nil || reason != "invalid_address" {
			t.Fatalf("suppression reason = %q, %v, want invalid_address: the dead address must still stop the next send", reason, err)
		}
		if !sameStrings(f.outcomes("email"), "suppressed") {
			t.Fatalf("outcomes = %v, want suppressed", f.outcomes("email"))
		}
	})

	t.Run("three devices: each is its own delivery, and one failing retries alone (D25, NR-028)", func(t *testing.T) {
		t.Parallel()
		f := newDispatcherFixture(t, newEnv)
		f.e.AddChannel("push", domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupNone})
		devices := []string{"phone-token", "tablet-token", "watch-token"}
		f.address("push", devices...)
		f.topic("t", []domain.ChannelKind{"push"})
		f.e.Templates.Put(Realm, "t", "push", "en", domain.RenderedContent{Subject: "S", Body: "B"})
		f.notify(f.note("t", "k1"))

		if n := f.drain(); n != 1 {
			t.Fatalf("the first Drain handled %d claims, want the one row that fans out", n)
		}
		q := f.queue()
		if len(q) != 3 {
			t.Fatalf("queue = %+v, want one row per device", q)
		}
		for _, row := range q {
			if row.Leased || row.Attempts != 0 || !row.HasAddress || row.AddressKey == "" || len(row.Fallback) != 0 {
				t.Fatalf("row %+v: a fanned-out row is unleased, unattempted, bound, and carries no fallback", row)
			}
		}
		if h := f.history(true); len(h) != 1 || h[0].Outcome != "fanned_out" {
			t.Fatalf("history = %+v, want the one fanned_out record", h)
		}

		f.e.Probe.FailNext(Transient)
		if n := f.drain(); n != 3 {
			t.Fatalf("the second Drain handled %d claims, want the three devices", n)
		}
		q = f.queue()
		if len(q) != 1 || q[0].Attempts != 1 {
			t.Fatalf("queue = %+v, want only the failing device waiting, at attempt 1", q)
		}
		if got := f.outcomes("push"); !sameStrings(got, "delivered", "delivered") {
			t.Fatalf("outcomes = %v, want the other two delivered", got)
		}

		f.makeDue()
		f.drain()
		f.wantEmpty("every device was delivered")
		keys := map[string]bool{}
		for _, device := range devices {
			key := domain.DeliveryIdemKey(f.id, "k1", "", "push", domain.AddressKey("push", device))
			keys[key] = true
			calls := f.e.Probe.Calls(key)
			if calls < 1 || calls > 2 {
				t.Fatalf("device %s was asked %d times", device, calls)
			}
		}
		total := 0
		for key := range keys {
			total += f.e.Probe.Calls(key)
		}
		if len(keys) != 3 || total != 4 {
			t.Fatalf("%d keys and %d sends, want 3 distinct keys and 4 sends: only the failing device was retried", len(keys), total)
		}
		if got := f.outcomes("push"); !sameStrings(got, "delivered", "delivered", "delivered") {
			t.Fatalf("outcomes = %v, want three delivered", got)
		}
	})

	t.Run("expired is never sent late (D29, NR-029)", func(t *testing.T) {
		t.Parallel()
		f := newDispatcherFixture(t, newEnv)
		f.email()
		f.topic("t", []domain.ChannelKind{"email"})
		n := f.note("t", "k1")
		n.DeliverBefore = Epoch.Add(time.Hour)
		f.notify(n)

		f.e.Clock.Set(Epoch.Add(2 * time.Hour))
		f.drain()
		if calls := f.e.Probe.Calls(f.emailKey("k1", "u1@example.com")); calls != 0 {
			t.Fatalf("the channel was asked %d times for an expired delivery, want never", calls)
		}
		if !sameStrings(f.outcomes("email"), "expired") {
			t.Fatalf("outcomes = %v, want expired", f.outcomes("email"))
		}
		f.wantNoAlarm("expiry is the engine working: nobody is paged for a late send it refused")
	})

	t.Run("a delivery that expires while it waits for a retry is expired, not parked (D29)", func(t *testing.T) {
		t.Parallel()
		f := newDispatcherFixture(t, newEnv)
		f.email()
		f.topic("t", []domain.ChannelKind{"email"})
		n := f.note("t", "k1")
		n.DeliverBefore = Epoch.Add(time.Hour)
		f.notify(n)

		f.e.Probe.FailNext(Transient)
		f.drain()
		f.e.Clock.Set(Epoch.Add(time.Hour)) // the instant itself is already not before the deadline
		f.makeDue()
		f.drain()
		if !sameStrings(f.outcomes("email"), "expired") {
			t.Fatalf("outcomes = %v, want expired", f.outcomes("email"))
		}
		if calls := f.e.Probe.Calls(f.emailKey("k1", "u1@example.com")); calls != 1 {
			t.Fatalf("%d sends, want only the one before the deadline", calls)
		}
		f.wantNoAlarm("an expired delivery is not a failure")
	})

	t.Run("no_address on push enqueues the sms fallback (D29, NR-031)", func(t *testing.T) {
		t.Parallel()
		f := newDispatcherFixture(t, newEnv)
		f.e.AddChannel("push", domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupNone})
		f.e.AddChannel("sms", domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupProvider, DedupWindow: time.Hour})
		f.address("sms", "+15551230000")
		f.topic("t", []domain.ChannelKind{"push"}, "sms")
		f.notify(f.note("t", "k1"))

		if q := f.queue(); len(q) != 1 || q[0].Channel != "push" || !slices.Equal(q[0].Fallback, []string{"sms"}) {
			t.Fatalf("queue = %+v, want the push delivery carrying the sms fallback", q)
		}
		f.drain()
		if !sameStrings(f.outcomes("push"), "no_address") {
			t.Fatalf("push outcomes = %v, want no_address", f.outcomes("push"))
		}
		q := f.queue()
		if len(q) != 1 || q[0].Channel != "sms" || len(q[0].Fallback) != 0 {
			t.Fatalf("queue = %+v, want the sms fallback pending with the chain spent", q)
		}
		f.drain()
		f.wantEmpty("sms was delivered")
		if !sameStrings(f.outcomes("sms"), "delivered") {
			t.Fatalf("sms outcomes = %v, want delivered", f.outcomes("sms"))
		}
		f.wantNoAlarm("a missing address is not a failure, and the fallback made up for it")
	})

	t.Run("exhausted tenant: its backlog moves with one deferral and the quiet tenant goes next (D24, NR-023)", func(t *testing.T) {
		t.Parallel()
		f := newDispatcherFixture(t, newEnv)
		f.email()
		f.topic("t", []domain.ChannelKind{"email"})
		noisy := f.id
		quiet := domain.Identity{Realm: Realm, Tenant: domain.Tenant{Kind: "workspace", ID: "w2"}, Recipient: contractUser}
		f.e.Addresses.Put(quiet, "email", domain.Address{Channel: "email", Value: "q@example.com"})
		note := func(id domain.Identity, key string) domain.Notification {
			n := f.note("t", key)
			n.Tenant, n.Recipient = id.Tenant, id.Recipient
			return n
		}
		f.notify(note(noisy, "n1"))
		f.notify(note(noisy, "n2"))
		f.notify(note(quiet, "q1"))

		// The budget runs out after the rows were queued, so the planner did not see it.
		reset := Epoch.Add(time.Hour)
		f.e.Quotas.SetBudget(domain.QuotaKey{Realm: Realm, Tenant: noisy.Tenant, Channel: "email"}, 0, reset)

		if n, err := f.e.Dispatcher.Drain(t.Context(), f.shardOf(noisy), 1); err != nil || n != 1 {
			t.Fatalf("Drain(noisy) = %d, %v", n, err)
		}
		pending := f.queue()
		if len(pending) != 3 {
			t.Fatalf("queue = %+v, want all three still pending", pending)
		}
		moved := 0
		for _, row := range pending {
			if row.NextAttemptAt.Equal(reset) {
				moved++
				if row.Attempts != 0 || row.Leased {
					t.Errorf("row %+v: a deferral gives the attempt back and releases the lease, or it walks a delivery to the dead letter", row)
				}
			}
		}
		if moved != 2 {
			t.Fatalf("%d rows are due at the reset, want the noisy tenant's two: one deferred, and the other moved with it before it was ever claimed", moved)
		}
		if row := pending[0]; row.Deferrals != 1 && pending[1].Deferrals != 1 && pending[2].Deferrals != 1 {
			t.Fatalf("queue = %+v, want a deferral counted", pending)
		}

		f.drainAll()
		if calls := f.e.Probe.Calls(f.emailKey("n1", "u1@example.com")) + f.e.Probe.Calls(f.emailKey("n2", "u1@example.com")); calls != 0 {
			t.Fatalf("the exhausted tenant was sent %d times", calls)
		}
		quietKey := domain.DeliveryIdemKey(quiet, "q1", "", "email", domain.AddressKey("email", "q@example.com"))
		if f.e.Probe.Sends(quietKey) != 1 {
			t.Fatalf("the quiet tenant's delivery was sent %d times, want once", f.e.Probe.Sends(quietKey))
		}
		if q := f.queue(); len(q) != 2 {
			t.Fatalf("queue = %+v, want the noisy tenant's two waiting for the reset", q)
		}
		f.wantNoAlarm("a deferral is not a failure")
	})

	t.Run("a spent budget drops only what the policy allows, and never an essential or critical notification (D5)", func(t *testing.T) {
		t.Parallel()
		cases := []struct {
			name      string
			essential bool
			priority  domain.Priority
			want      string // the outcome of the first drain, or "" for deferred
		}{
			{"info on an ordinary topic is dropped", false, domain.PriorityInfo, "dropped_quota"},
			{"warning on an ordinary topic is dropped", false, domain.PriorityWarning, "dropped_quota"},
			{"critical is deferred", false, domain.PriorityCritical, ""},
			{"info on an essential topic is deferred", true, domain.PriorityInfo, ""},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				f := newDispatcherFixture(t, newEnv)
				f.email()
				f.e.Topics.Register(Realm, "t", domain.TopicDef{DefaultChannels: []domain.ChannelKind{"email"},
					DefaultPriority: domain.PriorityInfo, Essential: tc.essential})
				n := f.note("t", "k1")
				n.Priority = tc.priority
				f.notify(n)
				key := domain.QuotaKey{Realm: Realm, Tenant: f.id.Tenant, Channel: "email"}
				f.e.Quotas.SetBudget(key, 0, Epoch.Add(time.Hour))
				f.e.Quotas.SetPolicy(key, domain.QuotaDropNonEssential)

				f.drain()
				if calls := f.e.Probe.Calls(f.emailKey("k1", "u1@example.com")); calls != 0 {
					t.Fatalf("the channel was asked %d times with the budget spent", calls)
				}
				if tc.want == "" {
					if q := f.queue(); len(q) != 1 || !q[0].NextAttemptAt.Equal(Epoch.Add(time.Hour)) {
						t.Fatalf("queue = %+v, want it deferred to the reset", q)
					}
					return
				}
				f.wantEmpty("it was dropped")
				if !sameStrings(f.outcomes("email"), tc.want) {
					t.Fatalf("outcomes = %v, want %s", f.outcomes("email"), tc.want)
				}
				f.wantNoAlarm("a quota drop is a policy, not a failure")
			})
		}
	})

	t.Run("losing the quota counter fails open: the delivery is sent (D5)", func(t *testing.T) {
		t.Parallel()
		f := newDispatcherFixture(t, newEnv)
		f.email()
		f.topic("t", []domain.ChannelKind{"email"})
		f.notify(f.note("t", "k1"))
		key := domain.QuotaKey{Realm: Realm, Tenant: f.id.Tenant, Channel: "email"}
		f.e.Quotas.SetBudget(key, 0, Epoch.Add(time.Hour))
		f.e.Quotas.Lose() // Redis restarted

		f.drain()
		if !sameStrings(f.outcomes("email"), "delivered") {
			t.Fatalf("outcomes = %v, want delivered: a quota counter that forgot must not hold a delivery back", f.outcomes("email"))
		}
	})

	t.Run("a stale worker's outcome is discarded and counted (D19)", func(t *testing.T) {
		t.Parallel()
		f := newDispatcherFixture(t, newEnv)
		email := NewScripted("email", domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupNone})
		f.e.Channels.Register(email)
		f.address("email", "u1@example.com")
		f.topic("t", []domain.ChannelKind{"email"})
		f.e.Templates.Put(Realm, "t", "email", "en", domain.RenderedContent{Subject: "S", Body: "B"})
		f.notify(f.note("t", "k1"))

		// Worker A sends, then stalls before it records.
		stall := f.e.Faults.StallAt(domain.StepRecord)
		type result struct {
			n   int
			err error
		}
		done := make(chan result, 1)
		go func() {
			n, err := f.e.Dispatcher.Drain(t.Context(), f.shardOf(f.id), f.cfg().batch)
			done <- result{n, err}
		}()
		await(t, stall.Reached(), "worker A to stall after sending")

		// Its lease lapses and worker B takes the delivery over.
		f.lapse()
		if n := f.drain(); n != 1 {
			t.Fatalf("worker B handled %d deliveries, want the one A abandoned", n)
		}
		before := f.history(false)
		stall.Release()
		select {
		case r := <-done:
			if r.err != nil || r.n != 1 {
				t.Fatalf("worker A's Drain = %d, %v: a lost lease is not an error", r.n, r.err)
			}
		case <-time.After(deadline):
			t.Fatal("timed out waiting for worker A to finish")
		}

		if after := f.history(false); !slices.Equal(before, after) || len(after) != 1 || after[0].Outcome != "delivered" || after[0].Attempts != 2 {
			t.Fatalf("history %+v then %+v: the stale worker wrote, or B's outcome is not the one recorded", before, after)
		}
		if n := f.e.Metrics.CountWith(domain.MetricLeaseLost, map[string]string{domain.LabelChannel: "email"}); n != 1 {
			t.Fatalf("%s = %d, want 1", domain.MetricLeaseLost, n)
		}
		if n := len(email.Deliveries()); n != 2 {
			t.Fatalf("%d sends, want 2: a DedupNone channel is sent by both, which is what the counter warns of", n)
		}
		f.wantNoAlarm("a lost lease is counted on its own metric")
	})

	t.Run("a worker whose lease was lost during a retry records nothing either (D19)", func(t *testing.T) {
		t.Parallel()
		f := newDispatcherFixture(t, newEnv)
		email := NewScripted("email", domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupNone}).
			Then(domain.DeliveryResult{Outcome: domain.Retry, Detail: "A saw a timeout"}, nil)
		f.e.Channels.Register(email)
		f.address("email", "u1@example.com")
		f.topic("t", []domain.ChannelKind{"email"})
		f.e.Templates.Put(Realm, "t", "email", "en", domain.RenderedContent{Subject: "S", Body: "B"})
		f.notify(f.note("t", "k1"))

		stall := f.e.Faults.StallAt(domain.StepRecord)
		done := make(chan struct{})
		go func() {
			defer close(done)
			_, _ = f.e.Dispatcher.Drain(t.Context(), f.shardOf(f.id), f.cfg().batch)
		}()
		await(t, stall.Reached(), "worker A to stall")
		f.lapse()
		f.drain() // B delivers: the script is spent
		stall.Release()
		await(t, done, "worker A to finish")

		if h := f.history(false); len(h) != 1 || h[0].Outcome != "delivered" {
			t.Fatalf("history = %+v, want B's delivered alone", h)
		}
		f.wantEmpty("A's retry must not have put the delivery back")
		if n := f.e.Metrics.Count(domain.MetricLeaseLost); n != 1 {
			t.Fatalf("%s = %d, want 1", domain.MetricLeaseLost, n)
		}
	})

	t.Run("a stale worker's refusal is discarded, and raises no alarm for a failure that was not recorded (D19, NR-007)", func(t *testing.T) {
		t.Parallel()
		f := newDispatcherFixture(t, newEnv)
		email := NewScripted("email", domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupNone}).
			Then(domain.DeliveryResult{Outcome: domain.Rejected, Detail: "A was refused"}, nil)
		f.e.Channels.Register(email)
		f.address("email", "u1@example.com")
		f.topic("t", []domain.ChannelKind{"email"})
		f.e.Templates.Put(Realm, "t", "email", "en", domain.RenderedContent{Subject: "S", Body: "B"})
		f.notify(f.note("t", "k1"))

		stall := f.e.Faults.StallAt(domain.StepRecord)
		done := make(chan struct{})
		go func() {
			defer close(done)
			_, _ = f.e.Dispatcher.Drain(t.Context(), f.shardOf(f.id), f.cfg().batch)
		}()
		await(t, stall.Reached(), "worker A to stall")
		f.lapse()
		f.drain() // B delivers: the script is spent
		stall.Release()
		await(t, done, "worker A to finish")

		if !sameStrings(f.outcomes("email"), "delivered") {
			t.Fatalf("outcomes = %v, want B's delivered alone", f.outcomes("email"))
		}
		f.wantNoAlarm("the dead letter A would have written was never written, so nothing was parked")
		if n := f.e.Metrics.Count(domain.MetricLeaseLost); n != 1 {
			t.Fatalf("%s = %d, want 1", domain.MetricLeaseLost, n)
		}
	})

	t.Run("a budget whose reset is already past is deferred by at least one poll, not to a moment that has gone (D24)", func(t *testing.T) {
		t.Parallel()
		f := newDispatcherFixture(t, newEnv)
		f.email()
		f.topic("t", []domain.ChannelKind{"email"})
		f.notify(f.note("t", "k1"))
		f.e.Quotas.SetBudget(domain.QuotaKey{Realm: Realm, Tenant: f.id.Tenant, Channel: "email"}, 0, Epoch.Add(-time.Hour))

		f.drain()
		q := f.queue()
		want := Epoch.Add(f.e.Config.WithDefaults().PollInterval)
		if len(q) != 1 || !q[0].NextAttemptAt.Equal(want) || q[0].Attempts != 0 {
			t.Fatalf("queue = %+v, want one delivery due at %v, a poll from now: due at the stale reset it would be claimed again at once, in a loop", q, want)
		}
	})

	t.Run("a notification canceled while its delivery is in flight is finished as canceled and never sent (D29)", func(t *testing.T) {
		t.Parallel()
		f := newDispatcherFixture(t, newEnv)
		f.email()
		f.topic("t", []domain.ChannelKind{"email"})
		f.notify(f.note("t", "k1"))

		stall := f.e.Faults.StallAt(domain.StepLoad)
		done := make(chan struct{})
		go func() {
			defer close(done)
			_, _ = f.e.Dispatcher.Drain(t.Context(), f.shardOf(f.id), f.cfg().batch)
		}()
		await(t, stall.Reached(), "the worker to hold the claim")
		receipt, err := f.e.Notifier.Cancel(t.Context(), domain.CancelRequest{Tenant: f.id.Tenant, Recipient: f.id.Recipient, IdemKey: "k1"})
		if err != nil || !receipt.Matched || receipt.Canceled != 0 || receipt.InFlight != 1 {
			t.Fatalf("Cancel = %+v, %v, want the delivery reported in flight and not recorded as canceled", receipt, err)
		}
		stall.Release()
		await(t, done, "the worker to finish")

		if !sameStrings(f.outcomes("email"), "canceled") {
			t.Fatalf("outcomes = %v, want canceled", f.outcomes("email"))
		}
		if calls := f.e.Probe.Calls(f.emailKey("k1", "u1@example.com")); calls != 0 {
			t.Fatalf("the channel was asked %d times for a canceled notification", calls)
		}
		f.wantNoAlarm("a cancel is not a failure")
	})

	t.Run("a claim whose notification is gone is finished as canceled, not retried to the dead letter", func(t *testing.T) {
		t.Parallel()
		f := newDispatcherFixture(t, newEnv)
		f.email()
		f.topic("t", []domain.ChannelKind{"email"})
		f.notify(f.note("t", "k1"))
		f.exec(`DELETE FROM notifications`) // an erased recipient, or a retired partition

		f.drain()
		f.wantEmpty("nothing is left to send")
		if !sameStrings(f.outcomes("email"), "canceled") {
			t.Fatalf("outcomes = %v, want canceled", f.outcomes("email"))
		}
		f.wantNoAlarm("a vanished notification is not the dispatcher's failure")
	})

	t.Run("a template is found by the topic's template ref, then the address's locale, then the default (D27)", func(t *testing.T) {
		t.Parallel()
		cases := []struct {
			name        string
			locale      string
			templates   map[string]domain.RenderedContent // locale → content, under the ref "dunning"
			title, body string                            // the notification's own copy
			want        domain.RenderedContent
			wantOutcome string
		}{
			{"the address's own locale", "vi", map[string]domain.RenderedContent{
				"vi": {Subject: "Xong", Body: "Tai len xong."}, "en": {Subject: "Done", Body: "Uploaded."}},
				"", "", domain.RenderedContent{Subject: "Xong", Body: "Tai len xong."}, "delivered"},
			{"a locale with no template falls back to the default", "fr", map[string]domain.RenderedContent{
				"en": {Subject: "Done", Body: "Uploaded."}},
				"", "", domain.RenderedContent{Subject: "Done", Body: "Uploaded."}, "delivered"},
			{"an address with no locale takes the default", "", map[string]domain.RenderedContent{
				"en": {Subject: "Done", Body: "Uploaded."}},
				"", "", domain.RenderedContent{Subject: "Done", Body: "Uploaded."}, "delivered"},
			{"no template at all uses the notification's own copy", "en", nil,
				"Fallback title", "Fallback body", domain.RenderedContent{Subject: "Fallback title", Body: "Fallback body"}, "delivered"},
			{"no template and no copy is rejected", "en", nil, "", "", domain.RenderedContent{}, "rejected"},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				f := newDispatcherFixture(t, newEnv)
				email := NewScripted("email", domain.ChannelCapabilities{NeedsSubject: true, NeedsAddress: true, Dedup: domain.DedupProvider, DedupWindow: time.Hour})
				f.e.Channels.Register(email)
				f.e.Addresses.Put(f.id, "email", domain.Address{Channel: "email", Value: "u1@example.com", Locale: tc.locale})
				f.e.Topics.Register(Realm, "t", domain.TopicDef{DefaultChannels: []domain.ChannelKind{"email"},
					DefaultPriority: domain.PriorityInfo, TemplateRef: "dunning"})
				for locale, c := range tc.templates {
					f.e.Templates.Put(Realm, "dunning", "email", locale, c)
				}
				n := f.note("t", "k1")
				n.Title, n.Body = tc.title, tc.body
				f.notify(n)
				f.drain()

				if !sameStrings(f.outcomes("email"), tc.wantOutcome) {
					t.Fatalf("outcomes = %v, want %s", f.outcomes("email"), tc.wantOutcome)
				}
				if tc.wantOutcome == "rejected" {
					if len(email.Deliveries()) != 0 {
						t.Fatal("a delivery with nothing to say was sent")
					}
					dl := f.deadLetters()
					if len(dl) != 1 || dl[0].Reason != "rejected" || !strings.Contains(dl[0].LastError, "template") {
						t.Fatalf("dead letters = %+v, want one rejected whose last error says there was no template", dl)
					}
					return
				}
				sent := email.Deliveries()
				if len(sent) != 1 || sent[0].Content.Subject != tc.want.Subject || sent[0].Content.Body != tc.want.Body {
					t.Fatalf("sent %+v, want the content %+v", sent, tc.want)
				}
				f.wantNoAlarm("a delivery that rendered is not a failure")
			})
		}
	})

	t.Run("a channel the engine has no registration for is retried, not lost or rejected", func(t *testing.T) {
		t.Parallel()
		f := newDispatcherFixture(t, newEnv)
		f.topic("t", []domain.ChannelKind{"fax"}) // no channel of that kind is registered here
		f.notify(f.note("t", "k1"))
		f.drain()

		q := f.queue()
		if len(q) != 1 || q[0].Attempts != 1 || !strings.Contains(q[0].LastError, "fax") {
			t.Fatalf("queue = %+v, want the delivery waiting, its last error naming the channel: a worker without the channel is a deploy in progress, and replaying later must be possible", q)
		}
		f.wantNoAlarm("not yet a failure")
	})

	t.Run("a batch is sent concurrently, so ClaimLease need only outlast one send", func(t *testing.T) {
		t.Parallel()
		f := newDispatcherFixture(t, newEnv)
		const sends = 3
		var (
			mu      sync.Mutex
			arrived int
			all     = make(chan struct{})
		)
		email := NewScripted("email", domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupNone}).
			OnDeliver(func(ctx context.Context, _ domain.Delivery) {
				mu.Lock()
				arrived++
				if arrived == sends {
					close(all)
				}
				mu.Unlock()
				select {
				case <-all:
				case <-ctx.Done():
				case <-time.After(deadline):
				}
			})
		f.e.Channels.Register(email)
		f.address("email", "u1@example.com")
		for i := range sends {
			topic := domain.Topic(fmt.Sprintf("t%d", i))
			f.topic(topic, []domain.ChannelKind{"email"})
			f.e.Templates.Put(Realm, string(topic), "email", "en", domain.RenderedContent{Subject: "S", Body: "B"})
			f.notify(f.note(topic, fmt.Sprintf("k%d", i)))
		}
		if n := f.drain(); n != sends {
			t.Fatalf("Drain handled %d, want %d", n, sends)
		}
		select {
		case <-all:
		default:
			t.Fatalf("only %d of %d sends were in flight together: a batch claimed under one lease and sent one by one outlives it", arrived, sends)
		}
		if got := f.outcomes("email"); len(got) != sends {
			t.Fatalf("outcomes = %v, want %d delivered", got, sends)
		}
	})

	t.Run("Drain handles at most max, and says how many it handled", func(t *testing.T) {
		t.Parallel()
		f := newDispatcherFixture(t, newEnv)
		f.inApp()
		for i := range 3 {
			topic := domain.Topic(fmt.Sprintf("t%d", i))
			f.topic(topic, []domain.ChannelKind{"in_app"})
			f.notify(f.note(topic, fmt.Sprintf("k%d", i)))
		}
		shard := f.shardOf(f.id)
		for _, want := range []int{2, 1, 0} {
			n, err := f.e.Dispatcher.Drain(t.Context(), shard, 2)
			if err != nil || n != want {
				t.Fatalf("Drain(max 2) = %d, %v, want %d", n, err, want)
			}
		}
		if got := f.outcomes("in_app"); len(got) != 3 {
			t.Fatalf("outcomes = %v, want three delivered", got)
		}
	})

	t.Run("a digest is delivered to its members, keyed by the window, and skips the canceled ones (D4, D25)", func(t *testing.T) {
		t.Parallel()
		f := newDispatcherFixture(t, newEnv)
		email := NewScripted("email", domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupProvider, DedupWindow: time.Hour})
		f.e.Channels.Register(email)
		f.address("email", "u1@example.com")
		f.topic("t", []domain.ChannelKind{"email"})
		f.e.Templates.Put(Realm, "t", "email", "en", domain.RenderedContent{Subject: "Digest", Body: "Several things happened."})
		if err := f.e.Prefs.SetSchedule(t.Context(), f.id, domain.DeliverySchedule{Digest: time.Hour}); err != nil {
			t.Fatal(err)
		}
		for _, key := range []string{"k1", "k2", "k3"} {
			n := f.note("t", key)
			n.Title = "Upload " + key
			f.notify(n)
		}
		if q := f.queue(); len(q) != 0 {
			t.Fatalf("queue = %+v, want the digested notifications held in a window, not queued", q)
		}
		if _, err := f.e.Notifier.Cancel(t.Context(), domain.CancelRequest{Tenant: f.id.Tenant, Recipient: f.id.Recipient, IdemKey: "k2"}); err != nil {
			t.Fatal(err)
		}

		var window string
		if err := f.e.DB.Admin.QueryRow(t.Context(), `SELECT id::text FROM digest_buffer`).Scan(&window); err != nil {
			t.Fatal(err)
		}
		if queuedNow, err := f.e.Digests.FlushDigest(t.Context(), window); err != nil || !queuedNow {
			t.Fatalf("FlushDigest = %v, %v, want the window flushed", queuedNow, err)
		}
		f.drain()

		sent := email.Deliveries()
		if len(sent) != 1 {
			t.Fatalf("the channel was asked %d times, want once for the whole digest", len(sent))
		}
		d := sent[0]
		var titles []string
		for _, m := range d.Digest {
			titles = append(titles, m.Title)
		}
		if d.DigestID != window || d.NotificationID != "" || d.Notification.IdemKey != "" {
			t.Errorf("Delivery names notification %q, digest %q and a notification %+v, want the window %s alone", d.NotificationID, d.DigestID, d.Notification, window)
		}
		if !slices.Equal(titles, []string{"Upload k1", "Upload k3"}) {
			t.Errorf("members = %v, want k1 and k3 in the order they joined, without the canceled k2", titles)
		}
		wantKey := domain.DeliveryIdemKey(f.id, "", window, "email", domain.AddressKey("email", "u1@example.com"))
		if d.IdemKey != wantKey {
			t.Errorf("IdemKey = %s, want %s: a digest is keyed by its window (D25)", d.IdemKey, wantKey)
		}
		f.wantNoAlarm("a digest that went out is not a failure")
	})

	t.Run("a digest whose members are all canceled is finished as canceled (D4)", func(t *testing.T) {
		t.Parallel()
		f := newDispatcherFixture(t, newEnv)
		f.email()
		f.topic("t", []domain.ChannelKind{"email"})
		if err := f.e.Prefs.SetSchedule(t.Context(), f.id, domain.DeliverySchedule{Digest: time.Hour}); err != nil {
			t.Fatal(err)
		}
		f.notify(f.note("t", "k1"))
		if _, err := f.e.Notifier.Cancel(t.Context(), domain.CancelRequest{Tenant: f.id.Tenant, Recipient: f.id.Recipient, IdemKey: "k1"}); err != nil {
			t.Fatal(err)
		}
		var window string
		if err := f.e.DB.Admin.QueryRow(t.Context(), `SELECT id::text FROM digest_buffer`).Scan(&window); err != nil {
			t.Fatal(err)
		}
		if _, err := f.e.Digests.FlushDigest(t.Context(), window); err != nil {
			t.Fatal(err)
		}
		f.drain()

		if !sameStrings(f.outcomes("email"), "canceled") {
			t.Fatalf("outcomes = %v, want canceled", f.outcomes("email"))
		}
		if f.e.Probe.Total() != 0 {
			t.Fatalf("%d sends, want none: an empty digest is not sent", f.e.Probe.Total())
		}
		f.wantNoAlarm("a canceled digest is not a failure")
	})
}
