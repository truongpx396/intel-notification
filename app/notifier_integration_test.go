//go:build integration

package app_test

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/truongpx396/intel-notification/app"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/notifytest"
	"github.com/truongpx396/intel-notification/ports"
)

func TestMain(m *testing.M) { os.Exit(notifytest.Main(m, notifytest.WithRedis())) }

// NotifierContract (T024) against the in-process Notifier, on an Env whose
// pre-check is the in-memory one.
func TestNotifierContract(t *testing.T) {
	t.Parallel()
	notifytest.NotifierContract(t, func(t *testing.T) *notifytest.Env {
		t.Helper()
		return notifytest.NewEnv(t)
	})
}

// ...and again on the Redis pre-check (T023), over a Redis database of each case's
// own: a failed commit leaves no entry in Redis either.
func TestNotifierContractOnRedis(t *testing.T) {
	t.Parallel()
	notifytest.NotifierContract(t, func(t *testing.T) *notifytest.Env {
		t.Helper()
		return notifytest.NewEnv(t, notifytest.WithRedis())
	})
}

// redisFixture is a Redis Env with the two channels and the topic registered.
func redisFixture(t *testing.T) (*notifytest.Env, domain.Identity) {
	t.Helper()
	e := notifytest.NewEnv(t, notifytest.WithRedis())
	e.AddChannel("in_app", domain.ChannelCapabilities{InboxBacked: true, Dedup: domain.DedupLocal})
	e.AddChannel("email", domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupProvider})
	e.Topics.Register(notifytest.Realm, "ingestion_complete", domain.TopicDef{
		DefaultChannels: []domain.ChannelKind{"in_app", "email"}, DefaultPriority: domain.PriorityInfo,
	})
	id := domain.Identity{Realm: notifytest.Realm, Tenant: domain.Tenant{Kind: "workspace", ID: "w1"},
		Recipient: domain.Recipient{Kind: "user", ID: "u1"}}
	return e, id
}

func noteFor(id domain.Identity, key string) domain.Notification {
	return domain.Notification{Tenant: id.Tenant, Recipient: id.Recipient, Topic: "ingestion_complete", IdemKey: key}
}

// The entry in Redis is under the frozen notify:applied key (T008), holds the
// notification's id, and lives for the configured PreCheckTTL, which is under the
// idempotency window (D18).
func TestNotifierRemembersUnderTheFrozenKeyInRedis(t *testing.T) {
	t.Parallel()
	e, id := redisFixture(t)
	r, err := e.Notifier.Notify(t.Context(), noteFor(id, "invoice:42:overdue"))
	if err != nil || !r.Applied {
		t.Fatalf("Notify = %+v, %v", r, err)
	}
	const key = "notify:applied:aisat:2006f991ca0e66c48051540129023c7fc5c3f7f77455b4f8a3b70bade98c8df1"
	got, err := e.Redis.Client.Get(t.Context(), key).Result()
	if err != nil || got != r.NotificationID {
		t.Fatalf("GET %s = %q, %v, want the notification id %s", key, got, err, r.NotificationID)
	}
	ttl, err := e.Redis.Client.PTTL(t.Context(), key).Result()
	if want := e.Config.WithDefaults().PreCheckTTL; err != nil || ttl <= want-time.Minute || ttl > want {
		t.Fatalf("PTTL = %v, %v, want about the configured PreCheckTTL %v", ttl, err, want)
	}
	if n, _ := e.Redis.Client.DBSize(t.Context()).Result(); n != 1 {
		t.Fatalf("%d keys in Redis, want the one", n)
	}
}

// The pre-check can only miss: lose Redis, and a replay is still a replay, found by
// the durable guard (D18). A miss never decides that a key is new.
func TestNotifierStillFindsAReplayAfterRedisIsLost(t *testing.T) {
	t.Parallel()
	e, id := redisFixture(t)
	first, err := e.Notifier.Notify(t.Context(), noteFor(id, "k1"))
	if err != nil || !first.Applied {
		t.Fatalf("first Notify = %+v, %v", first, err)
	}
	if err := e.Redis.Client.FlushDB(t.Context()).Err(); err != nil {
		t.Fatal(err)
	}
	again, err := e.Notifier.Notify(t.Context(), noteFor(id, "k1"))
	if err != nil {
		t.Fatalf("replay Notify: %v", err)
	}
	if again.Applied || again.NotificationID != first.NotificationID {
		t.Fatalf("replay with Redis empty = %+v, want a replay of %s", again, first.NotificationID)
	}
	var rows int
	if err := e.DB.Admin.QueryRow(t.Context(), `SELECT count(*) FROM notifications WHERE realm = $1`, string(id.Realm)).Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("%d notifications, %v, want exactly one", rows, err)
	}
}

// countingStore counts the transactions the Notifier asks the store to run.
type countingStore struct {
	ports.Store
	persists atomic.Int64
}

func (s *countingStore) PersistAndEnqueue(ctx context.Context, tx ports.Tx, id domain.Identity, n domain.Notification, p domain.DeliveryPlan) (domain.Receipt, error) {
	s.persists.Add(1)
	return s.Store.PersistAndEnqueue(ctx, tx, id, n, p)
}

// A replay the pre-check knows costs the database nothing: no plan, no transaction.
// One it does not know goes to the guard and costs one.
func TestNotifierReplayFromThePreCheckOpensNoTransaction(t *testing.T) {
	t.Parallel()
	e, id := redisFixture(t)
	counting := &countingStore{Store: e.Store}
	n, err := app.NewNotifier(e.Config, app.Deps{
		Channels: e.Channels, Topics: e.Topics, Prefs: e.Prefs, Quotas: e.Quotas, QuotaPolicies: e.Quotas,
		Store: counting, Queue: e.Queue, PreCheck: e.PreCheck, Clock: e.Clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := n.Notify(t.Context(), noteFor(id, "k1"))
	if err != nil || !first.Applied {
		t.Fatalf("first Notify = %+v, %v", first, err)
	}
	for range 3 {
		r, err := n.Notify(t.Context(), noteFor(id, "k1"))
		if err != nil || r.Applied || r.NotificationID != first.NotificationID {
			t.Fatalf("replay = %+v, %v, want a replay of %s", r, err, first.NotificationID)
		}
	}
	if got := counting.persists.Load(); got != 1 {
		t.Fatalf("the store ran %d transactions for one notification and three replays the pre-check knew, want 1", got)
	}
}
