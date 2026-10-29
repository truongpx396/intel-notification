//go:build integration

package app_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/truongpx396/intel-notification/adapters/driven/postgres"
	"github.com/truongpx396/intel-notification/adapters/driven/redis"
	"github.com/truongpx396/intel-notification/app"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/notifytest"
	"github.com/truongpx396/intel-notification/ports"
)

// T036: the quota, in the use-cases and the adapters together. The budget's unit is a
// send: a peek at enqueue consumes none, the dispatcher's take consumes one, and a
// delivery deferred for want of a unit gives its attempt back and takes nothing.

// A delivery takes one unit, and one deferred for want of a unit takes none and returns
// its attempt, so deferral can never walk a delivery to the dead letters (NR-023, D24).
func TestADeliveryTakesOneUnitAndADeferralTakesNone(t *testing.T) {
	t.Parallel()
	e, id := emailEnv(t)
	e.AddChannel("email", domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupProvider, DedupWindow: time.Hour})
	key := domain.QuotaKey{Realm: notifytest.Realm, Tenant: id.Tenant, Channel: "email"}
	reset := notifytest.Epoch.Add(time.Hour)
	e.Quotas.SetBudget(key, 1, reset)

	send(t, e, id, "k1")
	send(t, e, id, "k2")
	if n := e.Quotas.Taken(key); n != 0 {
		t.Fatalf("%d units taken by accepting two notifications, want none: the planner peeks and never takes", n)
	}

	if n, err := drainShard(t, e.Dispatcher, e, id); err != nil || n != 2 {
		t.Fatalf("Drain = %d, %v, want both handled", n, err)
	}
	if n := e.Quotas.Taken(key); n != 1 {
		t.Fatalf("%d units taken by one send and one deferral, want 1: a take consumes once, and a deferral consumes none", n)
	}
	if got := count(t, e, `SELECT count(*) FROM notification_deliveries WHERE outcome = 'delivered'`); got != 1 {
		t.Fatalf("%d delivered, want the one the budget allowed", got)
	}
	var attempts, deferrals int
	var due time.Time
	if err := e.DB.Admin.QueryRow(t.Context(), `SELECT attempts, deferrals, next_attempt_at FROM notification_outbox`).Scan(&attempts, &deferrals, &due); err != nil {
		t.Fatalf("the deferred delivery is not in the queue: %v", err)
	}
	if attempts != 0 || deferrals != 1 || !due.Equal(reset) {
		t.Fatalf("deferred row: attempts %d, deferrals %d, due %v, want 0, 1, %v: the attempt goes back, and it waits for the reset", attempts, deferrals, due, reset)
	}
}

// realBudgets is an Env on Redis whose Notifier and Dispatcher are built over the real
// counter and the real reader of channel_quotas, instead of the kit's fake.
type realBudgets struct {
	e          *notifytest.Env
	id         domain.Identity
	key        domain.QuotaKey
	notifier   ports.Notifier
	dispatcher *app.Dispatcher
}

func newRealBudgets(t *testing.T) *realBudgets {
	t.Helper()
	e := notifytest.NewEnv(t, notifytest.WithRedis())
	id := domain.Identity{Realm: notifytest.Realm, Tenant: itTenant, Recipient: itUser}
	e.AddChannel("email", domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupProvider, DedupWindow: time.Hour})
	e.Addresses.Put(id, "email", domain.Address{Channel: "email", Value: "u1@example.com"})
	for topic, def := range map[domain.Topic]domain.TopicDef{
		"t":    {DefaultChannels: []domain.ChannelKind{"email"}, DefaultPriority: domain.PriorityInfo},
		"must": {DefaultChannels: []domain.ChannelKind{"email"}, DefaultPriority: domain.PriorityInfo, Essential: true},
	} {
		e.Topics.Register(notifytest.Realm, topic, def)
		e.Templates.Put(notifytest.Realm, string(topic), "email", "en", domain.RenderedContent{Subject: "S", Body: "B"})
	}

	table := postgres.NewQuotas(e.DB.Owner)
	counter := redis.NewQuota(e.Redis.Client, table, redis.WithQuotaClock(e.Clock), redis.WithLimitsTTL(0))
	deps := e.DispatcherDeps()
	deps.Quotas, deps.QuotaPolicies = counter, table
	dispatcher, err := app.NewDispatcher(e.Config, deps, app.WithRand(func() float64 { return notifytest.Jitter }))
	if err != nil {
		t.Fatal(err)
	}
	notifier, err := app.NewNotifier(e.Config, app.Deps{Channels: e.Channels, Topics: e.Topics, Prefs: e.Prefs,
		Quotas: counter, QuotaPolicies: table, Store: e.Store, Queue: e.Queue, PreCheck: e.PreCheck, Clock: e.Clock})
	if err != nil {
		t.Fatal(err)
	}
	return &realBudgets{e: e, id: id, notifier: notifier, dispatcher: dispatcher,
		key: domain.QuotaKey{Realm: notifytest.Realm, Tenant: id.Tenant, Channel: "email"}}
}

// quotaRow configures the realm-wide budget on email.
func (r *realBudgets) quotaRow(t *testing.T, hour, day int, onExhausted string) {
	t.Helper()
	if _, err := r.e.DB.Admin.Exec(t.Context(), `
INSERT INTO channel_quotas (realm, channel, max_per_hour, max_per_day, on_exhausted) VALUES ($1, 'email', $2, $3, $4)`,
		string(notifytest.Realm), hour, day, onExhausted); err != nil {
		t.Fatal(err)
	}
}

func (r *realBudgets) drain(t *testing.T) int {
	t.Helper()
	n, err := r.dispatcher.Drain(t.Context(), domain.ShardFor(r.id, r.e.Config.WithDefaults().Shards), 100)
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	return n
}

// counted is what Redis has counted for the budget in the hour containing at.
func (r *realBudgets) counted(t *testing.T, at time.Time) int {
	t.Helper()
	key := domain.QuotaCounterKey(r.key, domain.QuotaHour, at)
	if exists, err := r.e.Redis.Client.Exists(t.Context(), key).Result(); err != nil || exists == 0 {
		if err != nil {
			t.Fatal(err)
		}
		return 0
	}
	n, err := r.e.Redis.Client.Get(t.Context(), key).Int()
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// The planner peeks at enqueue and the dispatcher takes at dispatch, over the real
// counter and the real table: a notification accepted after the budget is spent is
// queued for the reset and not for now, accepting it spent nothing, and it is sent once
// the window has rolled (NR-023, D24).
func TestARealBudgetIsPeekedAtEnqueueAndTakenAtDispatch(t *testing.T) {
	t.Parallel()
	r := newRealBudgets(t)
	r.quotaRow(t, 2, 10, "defer")
	e, reset := r.e, notifytest.Epoch.Add(time.Hour)

	for _, k := range []string{"k1", "k2"} {
		if rec, err := r.notifier.Notify(t.Context(), domain.Notification{Tenant: r.id.Tenant, Recipient: r.id.Recipient,
			Topic: "t", IdemKey: k, Title: "Done", OccurredAt: notifytest.Epoch}); err != nil || !rec.Applied {
			t.Fatalf("Notify(%s) = %+v, %v", k, rec, err)
		}
	}
	if n := r.counted(t, notifytest.Epoch); n != 0 {
		t.Fatalf("%d units counted by accepting two notifications, want none: the planner only peeks", n)
	}
	if n := r.drain(t); n != 2 {
		t.Fatalf("Drain = %d, want both sent", n)
	}
	if n := r.counted(t, notifytest.Epoch); n != 2 {
		t.Fatalf("%d units counted after two sends, want 2", n)
	}

	// The budget is spent, so the planner sees it: the third is queued for the reset.
	if rec, err := r.notifier.Notify(t.Context(), domain.Notification{Tenant: r.id.Tenant, Recipient: r.id.Recipient,
		Topic: "t", IdemKey: "k3", Title: "Done", OccurredAt: notifytest.Epoch}); err != nil || !rec.Applied {
		t.Fatalf("Notify(k3) = %+v, %v", rec, err)
	}
	var due time.Time
	if err := e.DB.Admin.QueryRow(t.Context(), `SELECT next_attempt_at FROM notification_outbox`).Scan(&due); err != nil {
		t.Fatal(err)
	}
	if !due.Equal(reset) {
		t.Fatalf("the third delivery is due at %v, want the reset %v: accepted, and held until the budget returns", due, reset)
	}
	if n := r.counted(t, notifytest.Epoch); n != 2 {
		t.Fatalf("%d units counted after the peek of the third, want still 2", n)
	}
	if n := r.drain(t); n != 0 {
		t.Fatalf("Drain = %d before the reset, want nothing due", n)
	}

	// The window rolls. The queue's due times are the database's clock, which a test cannot
	// move, so the delivery is made due by hand and the budget's clock is moved past the hour.
	if _, err := e.DB.Admin.Exec(t.Context(), `UPDATE notification_outbox SET next_attempt_at = now()`); err != nil {
		t.Fatal(err)
	}
	e.Clock.Set(reset)
	if n := r.drain(t); n != 1 {
		t.Fatalf("Drain after the reset = %d, want the third sent", n)
	}
	if got := count(t, e, `SELECT count(*) FROM notification_deliveries WHERE outcome = 'delivered'`); got != 3 {
		t.Fatalf("%d delivered, want all three", got)
	}
	if n := r.counted(t, reset); n != 1 {
		t.Fatalf("%d units counted in the new hour, want 1", n)
	}
}

// A delivery the planner could not know to hold, because the budget ran out after it was
// queued, is deferred by the dispatcher to the real reset with its attempt returned, and
// the next hour sends it.
func TestTheDispatcherDefersToTheRealBudgetsReset(t *testing.T) {
	t.Parallel()
	r := newRealBudgets(t)
	r.quotaRow(t, 2, 10, "defer")
	e, reset := r.e, notifytest.Epoch.Add(time.Hour)

	for i := range 3 {
		send(t, e, r.id, fmt.Sprintf("k%d", i)) // through the Env's own Notifier, whose budget is unlimited
	}
	if n := r.drain(t); n != 3 {
		t.Fatalf("Drain = %d, want all three handled", n)
	}
	if got := count(t, e, `SELECT count(*) FROM notification_deliveries WHERE outcome = 'delivered'`); got != 2 {
		t.Fatalf("%d delivered, want the two the budget allowed", got)
	}
	var attempts int
	var due time.Time
	if err := e.DB.Admin.QueryRow(t.Context(), `SELECT attempts, next_attempt_at FROM notification_outbox`).Scan(&attempts, &due); err != nil {
		t.Fatalf("the third is not waiting in the queue: %v", err)
	}
	if attempts != 0 || !due.Equal(reset) {
		t.Fatalf("deferred row: attempts %d, due %v, want 0 and the end of the hour %v", attempts, due, reset)
	}
	if n := r.counted(t, notifytest.Epoch); n != 2 {
		t.Fatalf("%d units counted, want 2: the refused take counted nothing", n)
	}
}

// drop_non_essential, read from the table, drops only what may be dropped: an ordinary
// notification is finished as dropped_quota, and an essential one and a critical one wait
// for the reset (NR-023, D5).
func TestADropPolicyInTheTableDropsOnlyWhatMayBeDropped(t *testing.T) {
	t.Parallel()
	r := newRealBudgets(t)
	r.quotaRow(t, 0, 5, "drop_non_essential") // the hour is spent from the start
	e, reset := r.e, notifytest.Epoch.Add(time.Hour)

	for _, n := range []domain.Notification{
		{Topic: "t", IdemKey: "ordinary"},
		{Topic: "must", IdemKey: "essential"},
		{Topic: "t", IdemKey: "critical", Priority: domain.PriorityCritical},
	} {
		n.Tenant, n.Recipient, n.Title, n.OccurredAt = r.id.Tenant, r.id.Recipient, "Done", notifytest.Epoch
		if rec, err := e.Notifier.Notify(t.Context(), n); err != nil || !rec.Applied {
			t.Fatalf("Notify(%s) = %+v, %v", n.IdemKey, rec, err)
		}
	}
	r.drain(t)

	if got := count(t, e, `SELECT count(*) FROM notification_deliveries WHERE outcome = 'dropped_quota'`); got != 1 {
		t.Fatalf("%d deliveries ended dropped_quota, want only the ordinary one", got)
	}
	if got := count(t, e, `SELECT count(*) FROM notification_outbox WHERE next_attempt_at = $1`, reset); got != 2 {
		t.Fatalf("%d deliveries wait for the reset, want the essential and the critical", got)
	}
	if e.Probe.Total() != 0 {
		t.Fatalf("%d sends, want none: the budget is spent", e.Probe.Total())
	}
}
