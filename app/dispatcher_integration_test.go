//go:build integration

package app_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/truongpx396/intel-notification/adapters/driven/postgres"
	"github.com/truongpx396/intel-notification/app"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/notifytest"
	"github.com/truongpx396/intel-notification/ports"
)

// What DispatcherContract does not reach, on the same Env: the replay of a dead letter
// end to end (T032), the policy for a port that faults, and Run itself.

var (
	itTenant = domain.Tenant{Kind: "workspace", ID: "w1"}
	itUser   = domain.Recipient{Kind: "user", ID: "u1"}
)

// emailEnv is an Env with an email channel the recipient has an address for, and a
// topic that sends on it.
func emailEnv(t *testing.T) (*notifytest.Env, domain.Identity) {
	t.Helper()
	e := notifytest.NewEnv(t)
	id := domain.Identity{Realm: notifytest.Realm, Tenant: itTenant, Recipient: itUser}
	e.Topics.Register(notifytest.Realm, "t", domain.TopicDef{DefaultChannels: []domain.ChannelKind{"email"}, DefaultPriority: domain.PriorityInfo})
	e.Templates.Put(notifytest.Realm, "t", "email", "en", domain.RenderedContent{Subject: "S", Body: "B"})
	e.Addresses.Put(id, "email", domain.Address{Channel: "email", Value: "u1@example.com"})
	return e, id
}

func send(t *testing.T, e *notifytest.Env, id domain.Identity, key string) {
	t.Helper()
	r, err := e.Notifier.Notify(t.Context(), domain.Notification{Tenant: id.Tenant, Recipient: id.Recipient,
		Topic: "t", IdemKey: key, Title: "Done", OccurredAt: notifytest.Epoch})
	if err != nil || !r.Applied {
		t.Fatalf("Notify(%s) = %+v, %v", key, r, err)
	}
}

func count(t *testing.T, e *notifytest.Env, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := e.DB.Admin.QueryRow(t.Context(), sql, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return n
}

func drainShard(t *testing.T, d *app.Dispatcher, e *notifytest.Env, id domain.Identity) (int, error) {
	t.Helper()
	return d.Drain(t.Context(), domain.ShardFor(id, e.Config.WithDefaults().Shards), 100)
}

// ------------------------------------------------------------------ T032 --

// A delivery that failed for good is parked, and an operator replays it from the
// dead letter: it is delivered, the replay is once however many times it is asked for,
// and the channel sees the one send (NS-006).
func TestAParkedDeliveryIsReplayedAndDeliveredOnce(t *testing.T) {
	t.Parallel()
	e, id := emailEnv(t)
	e.AddChannel("email", domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupProvider, DedupWindow: time.Hour})
	send(t, e, id, "k1")
	key := domain.DeliveryIdemKey(id, "k1", "", "email", domain.AddressKey("email", "u1@example.com"))

	e.Probe.FailNext(notifytest.Permanent)
	if n, err := drainShard(t, e.Dispatcher, e, id); err != nil || n != 1 {
		t.Fatalf("Drain = %d, %v", n, err)
	}
	var deadLetter, reason, lastError string
	if err := e.DB.Admin.QueryRow(t.Context(), `SELECT id::text, reason, last_error FROM dead_letters`).Scan(&deadLetter, &reason, &lastError); err != nil {
		t.Fatalf("the refusal left no dead letter: %v", err)
	}
	if reason != "rejected" || lastError != "injected permanent refusal" {
		t.Fatalf("dead letter = %q, %q, want it inspectable with its reason and last error", reason, lastError)
	}

	admin, err := app.NewAdmin(e.Config, app.Deps{Maintenance: e.Maintenance, Waker: e.Dispatcher})
	if err != nil {
		t.Fatal(err)
	}
	// Once, however many times and however many at once.
	var wg sync.WaitGroup
	for range 6 {
		wg.Go(func() {
			if err := admin.ReplayDeadLetter(t.Context(), notifytest.Realm, deadLetter); err != nil {
				t.Errorf("ReplayDeadLetter: %v", err)
			}
		})
	}
	wg.Wait()
	if err := admin.ReplayDeadLetter(t.Context(), notifytest.Realm, deadLetter); err != nil {
		t.Fatalf("a further replay: %v", err)
	}
	if n := count(t, e, `SELECT count(*) FROM notification_outbox`); n != 1 {
		t.Fatalf("%d deliveries pending after seven replays, want one", n)
	}

	if n, err := drainShard(t, e.Dispatcher, e, id); err != nil || n != 1 {
		t.Fatalf("Drain after the replay = %d, %v", n, err)
	}
	if got := count(t, e, `SELECT count(*) FROM notification_deliveries WHERE outcome = 'delivered'`); got != 1 {
		t.Fatalf("%d delivered, want 1", got)
	}
	if e.Probe.Calls(key) != 2 || e.Probe.Sends(key) != 1 {
		t.Fatalf("the channel was asked %d times and sent %d, want 2 and 1: the refusal sent nothing, the replay sent once, on the same key", e.Probe.Calls(key), e.Probe.Sends(key))
	}
	if n := count(t, e, `SELECT count(*) FROM notification_outbox`); n != 0 {
		t.Fatalf("%d deliveries still pending", n)
	}
}

// A replayed message that has expired is not sent late (D29): the replay keeps the
// deadline, and the dispatcher honours it like any other.
func TestAReplayedDeliveryThatHasExpiredIsNotSentLate(t *testing.T) {
	t.Parallel()
	e, id := emailEnv(t)
	e.AddChannel("email", domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupNone})
	r, err := e.Notifier.Notify(t.Context(), domain.Notification{Tenant: id.Tenant, Recipient: id.Recipient, Topic: "t", IdemKey: "k1",
		DeliverBefore: notifytest.Epoch.Add(time.Hour), OccurredAt: notifytest.Epoch})
	if err != nil || !r.Applied {
		t.Fatal(r, err)
	}
	e.Probe.FailNext(notifytest.Permanent)
	if _, err := drainShard(t, e.Dispatcher, e, id); err != nil {
		t.Fatal(err)
	}
	var deadLetter string
	if err := e.DB.Admin.QueryRow(t.Context(), `SELECT id::text FROM dead_letters`).Scan(&deadLetter); err != nil {
		t.Fatal(err)
	}

	e.Clock.Set(notifytest.Epoch.Add(2 * time.Hour))
	admin, _ := app.NewAdmin(e.Config, app.Deps{Maintenance: e.Maintenance})
	if err := admin.ReplayDeadLetter(t.Context(), notifytest.Realm, deadLetter); err != nil {
		t.Fatal(err)
	}
	if _, err := drainShard(t, e.Dispatcher, e, id); err != nil {
		t.Fatal(err)
	}
	if got := count(t, e, `SELECT count(*) FROM notification_deliveries WHERE outcome = 'expired'`); got != 1 {
		t.Fatalf("%d expired, want the replayed delivery finished as expired", got)
	}
	if e.Probe.Total() != 0 {
		t.Fatalf("%d sends, want none", e.Probe.Total())
	}
}

// ------------------------------------------------------- the faults of ports --

type (
	faultyAddresses struct {
		ports.AddressBook
		err error
	}
	faultySuppressions struct {
		ports.SuppressionStore
		checkErr, writeErr error
	}
	faultyRenderer struct {
		ports.TemplateRenderer
		err error
	}
	faultyStore struct {
		ports.Store
		err error
	}
	faultyTopics struct {
		ports.TopicRegistry
		err error
	}
	faultyQuota struct {
		ports.QuotaCounter
		err error
	}
	faultyQueue struct {
		ports.Queue
		finishErr error
	}
)

func (f faultyAddresses) Resolve(context.Context, domain.Identity, domain.ChannelKind) ([]domain.Address, error) {
	return nil, f.err
}

func (f faultySuppressions) Suppressed(ctx context.Context, r domain.Realm, ch domain.ChannelKind, h []byte) (bool, error) {
	if f.checkErr != nil {
		return false, f.checkErr
	}
	return f.SuppressionStore.Suppressed(ctx, r, ch, h)
}

func (f faultySuppressions) Suppress(ctx context.Context, r domain.Realm, ch domain.ChannelKind, h []byte, reason domain.SuppressionReason, until time.Time) error {
	if f.writeErr != nil {
		return f.writeErr
	}
	return f.SuppressionStore.Suppress(ctx, r, ch, h, reason, until)
}

func (f faultyRenderer) Render(context.Context, domain.RenderRequest) (domain.RenderedContent, error) {
	return domain.RenderedContent{}, f.err
}

func (f faultyStore) Load(context.Context, domain.Claim) (*domain.Notification, []domain.Notification, bool, error) {
	return nil, nil, false, f.err
}

// Lookup answers that the topic is known, and fails: a registry that reports both is
// the case where reading "known" without the error would be a guess.
func (f faultyTopics) Lookup(context.Context, domain.Realm, domain.Topic) (domain.TopicDef, bool, error) {
	return domain.TopicDef{}, true, f.err
}

func (f faultyQuota) Take(context.Context, domain.QuotaKey) (bool, time.Time, error) {
	return false, time.Time{}, f.err
}

func (f faultyQueue) Finish(context.Context, domain.Claim, domain.Finish) (bool, error) {
	return false, f.finishErr
}

// A port the engine does not own that faults is a delivery to try again, never one
// lost, rejected or parked on the first failure; and the ceiling still parks it. Quota
// is the exception that proves the rule: a counter that cannot answer fails open, so a
// Redis outage does not hold back the whole system (D5).
func TestAFaultingPortIsARetryAndNeverALostDelivery(t *testing.T) {
	t.Parallel()
	boom := errors.New("connection refused")
	scripted := func(res domain.DeliveryResult, err error) func(*notifytest.Env) ports.Channel {
		return func(*notifytest.Env) ports.Channel {
			return notifytest.NewScripted("email", domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupNone}).Then(res, err)
		}
	}
	cases := []struct {
		name      string
		wrap      func(d *app.Deps)
		channel   func(*notifytest.Env) ports.Channel // nil: the kit's, which delivers
		wantError string                              // the retry's last error, or "" when the delivery should go through
	}{
		{"the address book", func(d *app.Deps) { d.Addresses = faultyAddresses{d.Addresses, boom} }, nil, "resolve addresses"},
		{"the suppression check", func(d *app.Deps) { d.Suppressions = faultySuppressions{d.Suppressions, boom, nil} }, nil, "suppression check"},
		{"the renderer", func(d *app.Deps) { d.Renderer = faultyRenderer{d.Renderer, boom} }, nil, "render"},
		{"the topic registry", func(d *app.Deps) { d.Topics = faultyTopics{d.Topics, boom} }, nil, "topic"},
		{"the store's load", func(d *app.Deps) { d.Store = faultyStore{d.Store, boom} }, nil, "load"},
		{"the quota counter fails open", func(d *app.Deps) { d.Quotas = faultyQuota{d.Quotas, boom} }, nil, ""},
		{"recording a suppression", func(d *app.Deps) { d.Suppressions = faultySuppressions{d.Suppressions, nil, boom} },
			scripted(domain.DeliveryResult{Outcome: domain.Suppressed, SuppressReason: domain.SuppressHardBounce}, nil), "record the suppression"},
		{"a channel that errors", func(*app.Deps) {}, scripted(domain.DeliveryResult{}, boom), "connection refused"},
		{"a channel that answers with an outcome it does not have", func(*app.Deps) {},
			scripted(domain.DeliveryResult{Outcome: 99}, nil), "unknown outcome 99"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e, id := emailEnv(t)
			if tc.channel != nil {
				e.Channels.Register(tc.channel(e))
			} else {
				e.AddChannel("email", domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupNone})
			}
			send(t, e, id, "k1")

			var handled []error
			var mu sync.Mutex
			deps := e.DispatcherDeps()
			tc.wrap(&deps)
			d, err := app.NewDispatcher(e.Config, deps, app.WithRand(func() float64 { return notifytest.Jitter }),
				app.WithErrorHandler(func(err error) { mu.Lock(); defer mu.Unlock(); handled = append(handled, err) }))
			if err != nil {
				t.Fatal(err)
			}
			if n, err := drainShard(t, d, e, id); err != nil || n != 1 {
				t.Fatalf("Drain = %d, %v: a port that faults is the delivery's retry, not the batch's error", n, err)
			}

			if tc.wantError == "" {
				if got := count(t, e, `SELECT count(*) FROM notification_deliveries WHERE outcome = 'delivered'`); got != 1 {
					t.Fatalf("%d delivered, want the delivery to go ahead", got)
				}
				mu.Lock()
				defer mu.Unlock()
				if len(handled) != 1 || !errors.Is(handled[0], boom) {
					t.Fatalf("the error handler saw %v, want the fault reported once", handled)
				}
				return
			}
			var attempts int
			var lastError string
			if err := e.DB.Admin.QueryRow(t.Context(), `SELECT attempts, last_error FROM notification_outbox`).Scan(&attempts, &lastError); err != nil {
				t.Fatalf("the delivery is not waiting in the queue: %v", err)
			}
			if attempts != 1 || !strings.Contains(lastError, tc.wantError) {
				t.Fatalf("attempts %d, last error %q, want a retry whose error mentions %q", attempts, lastError, tc.wantError)
			}
			if got := count(t, e, `SELECT count(*) FROM notification_deliveries`); got != 0 {
				t.Fatalf("%d deliveries finished, want none", got)
			}
		})
	}
}

// An outcome that could not be written is the batch's error, and leaves the claim
// leased to lapse: another worker drives it. Nothing is counted as handled.
func TestAnOutcomeThatCannotBeWrittenIsTheBatchsError(t *testing.T) {
	t.Parallel()
	e, id := emailEnv(t)
	e.AddChannel("email", domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupProvider, DedupWindow: time.Hour})
	send(t, e, id, "k1")
	boom := errors.New("connection reset")
	deps := e.DispatcherDeps()
	deps.Queue = faultyQueue{deps.Queue, boom}
	d, err := app.NewDispatcher(e.Config, deps)
	if err != nil {
		t.Fatal(err)
	}

	n, err := drainShard(t, d, e, id)
	if !errors.Is(err, boom) || n != 0 {
		t.Fatalf("Drain = %d, %v, want nothing handled and the write's error", n, err)
	}
	if got := count(t, e, `SELECT count(*) FROM notification_outbox WHERE lease_expires_at > now()`); got != 1 {
		t.Fatalf("%d leased deliveries, want the one: it must stay held until the lease lapses, or two workers would send it", got)
	}
}

// ---------------------------------------------------------------------- Run --

// Run drains every shard, and a delivery in flight when it is stopped is still
// recorded: the outcome is written on a context that outlives the worker's, so
// shutdown never turns a sent message into one that is sent again.
func TestRunDrainsEveryShardAndRecordsWhatWasInFlightAtShutdown(t *testing.T) {
	t.Parallel()
	e := notifytest.NewEnv(t)
	const recipients = 24
	e.Topics.Register(notifytest.Realm, "t", domain.TopicDef{DefaultChannels: []domain.ChannelKind{"email"}, DefaultPriority: domain.PriorityInfo})
	e.Templates.Put(notifytest.Realm, "t", "email", "en", domain.RenderedContent{Subject: "S", Body: "B"})

	ctx, stopRun := context.WithCancel(t.Context())
	var started atomic.Int32
	all := make(chan struct{})
	email := notifytest.NewScripted("email", domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupNone}).
		OnDeliver(func(context.Context, domain.Delivery) {
			if started.Add(1) == recipients {
				stopRun() // the worker is told to stop while this send is in flight
				close(all)
			}
		})
	e.Channels.Register(email)

	notifier, err := app.NewNotifier(e.Config, app.Deps{Channels: e.Channels, Topics: e.Topics, Prefs: e.Prefs, Quotas: e.Quotas,
		Store: e.Store, Queue: e.Queue, Clock: e.Clock, Waker: e.Dispatcher})
	if err != nil {
		t.Fatal(err)
	}
	shards := map[domain.Shard]bool{}
	for i := range recipients {
		id := domain.Identity{Realm: notifytest.Realm, Tenant: itTenant, Recipient: domain.Recipient{Kind: "user", ID: fmt.Sprintf("u%d", i)}}
		shards[domain.ShardFor(id, e.Config.WithDefaults().Shards)] = true
		e.Addresses.Put(id, "email", domain.Address{Channel: "email", Value: fmt.Sprintf("u%d@example.com", i)})
		r, err := notifier.Notify(t.Context(), domain.Notification{Tenant: id.Tenant, Recipient: id.Recipient, Topic: "t", IdemKey: "k1", Title: "Done"})
		if err != nil || !r.Applied {
			t.Fatalf("Notify = %+v, %v", r, err)
		}
	}
	if len(shards) < 4 {
		t.Fatalf("the recipients fall in %d shards, want enough to show Run visiting them all", len(shards))
	}

	done := make(chan error, 1)
	go func() { done <- e.Dispatcher.Run(ctx) }()
	select {
	case <-all:
	case <-time.After(30 * time.Second):
		t.Fatalf("only %d of %d deliveries were sent", started.Load(), recipients)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run = %v, want nil", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Run did not return after its context ended")
	}

	if got := count(t, e, `SELECT count(*) FROM notification_deliveries WHERE outcome = 'delivered'`); got != recipients {
		t.Fatalf("%d delivered, want %d: a send in flight at shutdown must still be recorded", got, recipients)
	}
	if n := count(t, e, `SELECT count(*) FROM notification_outbox`); n != 0 {
		t.Fatalf("%d deliveries left in the queue", n)
	}
}

// countingQueue closes reached once Claim has returned n times.
type countingQueue struct {
	ports.Queue
	n       int64
	calls   atomic.Int64
	reached chan struct{}
}

func (q *countingQueue) Claim(ctx context.Context, shard domain.Shard, max int, lease time.Duration) ([]domain.Claim, error) {
	claims, err := q.Queue.Claim(ctx, shard, max, lease)
	if q.calls.Add(1) == q.n {
		close(q.reached)
	}
	return claims, err
}

// With LISTEN/NOTIFY on, a commit in one session gets an idle dispatcher in another
// claiming at once (D22). The dispatcher is let go idle until its next poll is two and a
// half seconds away, a notification is committed by a store that announces its work, and
// it is delivered within a second and a half: polling alone could not have found it
// before then. Which of the dispatcher's passes it has reached is counted in claims, and
// the wait is on the delivery itself.
func TestListenNotifyGetsAnIdleDispatcherClaimingAtOnce(t *testing.T) {
	t.Parallel()
	e, id := emailEnv(t)
	got := make(chan struct{})
	var once sync.Once
	e.Channels.Register(notifytest.NewScripted("email", domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupNone}).
		OnDeliver(func(context.Context, domain.Delivery) { once.Do(func() { close(got) }) }))

	cfg := e.Config
	cfg.ListenNotify = true
	cfg.PollInterval = time.Hour // the ceiling is far away: the wait doubles from 10ms and is not capped here
	shards := cfg.WithDefaults().Shards
	idle := &countingQueue{Queue: e.Queue, n: int64(9 * shards), reached: make(chan struct{})}
	deps := e.DispatcherDeps()
	deps.Queue = idle
	deps.Listener = postgres.NewListener(e.DB.Owner)
	d, err := app.NewDispatcher(cfg, deps)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()
	defer func() {
		cancel()
		<-done
	}()

	select {
	case <-idle.reached: // nine passes: the next is 2.5 s away
	case <-time.After(30 * time.Second):
		t.Fatal("the dispatcher did not poll")
	}
	announcing := postgres.New(e.DB.Owner, postgres.WithClock(e.Clock), postgres.WithNotify())
	notifier, err := app.NewNotifier(e.Config, app.Deps{Channels: e.Channels, Topics: e.Topics, Prefs: e.Prefs, Quotas: e.Quotas,
		Store: announcing, Queue: e.Queue, Clock: e.Clock}) // no Waker: this commit is another process's
	if err != nil {
		t.Fatal(err)
	}
	if r, err := notifier.Notify(t.Context(), domain.Notification{Tenant: id.Tenant, Recipient: id.Recipient, Topic: "t", IdemKey: "k1", Title: "Done"}); err != nil || !r.Applied {
		t.Fatalf("Notify = %+v, %v", r, err)
	}
	select {
	case <-got:
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("the delivery was not sent within 1.5 s of the commit: the dispatcher heard nothing and waited for its poll")
	}
}

// recordingRenderer remembers what the dispatcher asked it to render.
type recordingRenderer struct {
	ports.TemplateRenderer
	mu   sync.Mutex
	reqs []domain.RenderRequest
}

func (r *recordingRenderer) Render(ctx context.Context, req domain.RenderRequest) (domain.RenderedContent, error) {
	r.mu.Lock()
	r.reqs = append(r.reqs, req)
	r.mu.Unlock()
	return r.TemplateRenderer.Render(ctx, req)
}

// The renderer is asked for the right template, in the right locale, with the right
// data (D27): the topic's own template ref, the address's locale or the configured
// default, the notification's data, and for a digest its members and no data.
func TestTheRendererIsAskedForTheRightTemplateInTheRightLocale(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		locale string
		want   string
	}{
		{"the address's own locale", "vi", "vi"},
		{"an address with no locale takes the configured default", "", "en"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e, id := emailEnv(t)
			e.AddChannel("email", domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupNone})
			e.Topics.Register(notifytest.Realm, "t", domain.TopicDef{DefaultChannels: []domain.ChannelKind{"email"},
				DefaultPriority: domain.PriorityInfo, TemplateRef: "dunning"})
			e.Addresses.Put(id, "email", domain.Address{Channel: "email", Value: "u1@example.com", Locale: tc.locale})
			e.Templates.Put(notifytest.Realm, "dunning", "email", "en", domain.RenderedContent{Subject: "S", Body: "B"})
			rec := &recordingRenderer{TemplateRenderer: e.Templates}
			deps := e.DispatcherDeps()
			deps.Renderer = rec
			d, err := app.NewDispatcher(e.Config, deps)
			if err != nil {
				t.Fatal(err)
			}
			if r, err := e.Notifier.Notify(t.Context(), domain.Notification{Tenant: id.Tenant, Recipient: id.Recipient, Topic: "t",
				IdemKey: "k1", Data: map[string]any{"amount": 42}}); err != nil || !r.Applied {
				t.Fatal(r, err)
			}
			if n, err := drainShard(t, d, e, id); err != nil || n != 1 {
				t.Fatalf("Drain = %d, %v", n, err)
			}

			if len(rec.reqs) != 1 {
				t.Fatalf("the renderer was asked %d times, want once", len(rec.reqs))
			}
			got := rec.reqs[0]
			if got.Realm != notifytest.Realm || got.Tenant != id.Tenant || got.Topic != "t" || got.TemplateRef != "dunning" ||
				got.Channel != "email" || got.Locale != tc.want || len(got.Digest) != 0 {
				t.Fatalf("RenderRequest = %+v, want realm, tenant, topic t, ref dunning, channel email and locale %s", got, tc.want)
			}
			if v, ok := got.Data["amount"].(float64); !ok || v != 42 || len(got.Data) != 1 {
				t.Fatalf("Data = %v, want the notification's data", got.Data)
			}
		})
	}
}

// lookupCounter counts the topic lookups it is asked for.
type lookupCounter struct {
	ports.TopicRegistry
	n atomic.Int64
}

func (c *lookupCounter) Lookup(ctx context.Context, realm domain.Realm, topic domain.Topic) (domain.TopicDef, bool, error) {
	c.n.Add(1)
	return c.TopicRegistry.Lookup(ctx, realm, topic)
}

// A batch reads each topic once, however many of its deliveries are in it: in service
// mode a lookup is a query.
func TestABatchReadsEachTopicOnce(t *testing.T) {
	t.Parallel()
	e, id := emailEnv(t)
	e.AddChannel("email", domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupNone})
	for i := range 3 { // three notifications of one topic, one recipient: one shard
		send(t, e, id, fmt.Sprintf("k%d", i))
	}
	counting := &lookupCounter{TopicRegistry: e.Topics}
	deps := e.DispatcherDeps()
	deps.Topics = counting
	d, err := app.NewDispatcher(e.Config, deps)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := drainShard(t, d, e, id); err != nil || n != 3 {
		t.Fatalf("Drain = %d, %v, want the three deliveries", n, err)
	}
	if got := counting.n.Load(); got != 1 {
		t.Fatalf("the topic registry was asked %d times for three deliveries of one topic, want once", got)
	}
}

// A spent budget under drop_non_essential drops what it is sure may be dropped. A topic
// the dispatcher cannot read is not known to be non-essential, and dropping discards,
// so the delivery waits for the reset instead (D5).
func TestATopicThatCannotBeReadIsDeferredUnderASpentBudgetNotDropped(t *testing.T) {
	t.Parallel()
	e, id := emailEnv(t)
	e.AddChannel("email", domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupNone})
	send(t, e, id, "k1")
	key := domain.QuotaKey{Realm: notifytest.Realm, Tenant: id.Tenant, Channel: "email"}
	reset := notifytest.Epoch.Add(time.Hour)
	e.Quotas.SetBudget(key, 0, reset)
	e.Quotas.SetPolicy(key, domain.QuotaDropNonEssential)

	deps := e.DispatcherDeps()
	deps.Topics = faultyTopics{deps.Topics, errors.New("connection refused")}
	d, err := app.NewDispatcher(e.Config, deps)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := drainShard(t, d, e, id); err != nil {
		t.Fatal(err)
	}
	var next time.Time
	if err := e.DB.Admin.QueryRow(t.Context(), `SELECT next_attempt_at FROM notification_outbox`).Scan(&next); err != nil || !next.Equal(reset) {
		t.Fatalf("queue row due %v (%v), want it deferred to the reset %v", next, err, reset)
	}
	if got := count(t, e, `SELECT count(*) FROM notification_deliveries`); got != 0 {
		t.Fatalf("%d deliveries finished, want none: it must not be dropped on a guess", got)
	}
}

// A digest is rendered from its members, and a digest has no data of its own.
func TestADigestIsRenderedFromItsMembers(t *testing.T) {
	t.Parallel()
	e, id := emailEnv(t)
	e.AddChannel("email", domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupNone})
	if err := e.Prefs.SetSchedule(t.Context(), id, domain.DeliverySchedule{Digest: time.Hour}); err != nil {
		t.Fatal(err)
	}
	send(t, e, id, "k1")
	send(t, e, id, "k2")
	var window string
	if err := e.DB.Admin.QueryRow(t.Context(), `SELECT id::text FROM digest_buffer`).Scan(&window); err != nil {
		t.Fatal(err)
	}
	if queued, err := e.Digests.FlushDigest(t.Context(), window); err != nil || !queued {
		t.Fatalf("FlushDigest = %v, %v", queued, err)
	}
	rec := &recordingRenderer{TemplateRenderer: e.Templates}
	deps := e.DispatcherDeps()
	deps.Renderer = rec
	d, err := app.NewDispatcher(e.Config, deps)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := drainShard(t, d, e, id); err != nil || n != 1 {
		t.Fatalf("Drain = %d, %v", n, err)
	}
	if len(rec.reqs) != 1 || len(rec.reqs[0].Digest) != 2 || rec.reqs[0].Data != nil || rec.reqs[0].Topic != "t" {
		t.Fatalf("RenderRequests = %+v, want one, for the topic, with the digest's two members and no data", rec.reqs)
	}
}
