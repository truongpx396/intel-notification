//go:build integration

package notifytest

import (
	"os"
	"testing"
	"time"

	"github.com/truongpx396/intel-notification/adapters/driven/postgres"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/pgtest"
	"github.com/truongpx396/intel-notification/ports"
)

func TestMain(m *testing.M) { os.Exit(Main(m, WithRedis())) }

// Every Env has a database of its own, so two tests can lease the same job name
// and neither sees the other's rows. Within one Env a second owner is refused,
// which shows the lease is real and the first two successes are not vacuous.
func TestEnvGivesEachTestItsOwnDatabase(t *testing.T) {
	t.Parallel()
	a, b := NewEnv(t), NewEnv(t)
	if a.DB.Name == b.DB.Name {
		t.Fatalf("two Envs share the database %s", a.DB.Name)
	}
	lease := func(e *Env, owner string) bool {
		t.Helper()
		ok, err := e.Maintenance.TryLeaseJob(t.Context(), "retention", owner, time.Minute)
		if err != nil {
			t.Fatalf("TryLeaseJob: %v", err)
		}
		return ok
	}
	if !lease(a, "owner-a") || !lease(b, "owner-b") {
		t.Fatal("an Env could not lease a job that only another Env holds: they share state")
	}
	if lease(a, "owner-c") {
		t.Fatal("a second owner took a lease the first still holds")
	}
}

// The clock starts at Epoch, so a test's times do not depend on the day it runs.
func TestEnvStartsAtItsEpoch(t *testing.T) {
	t.Parallel()
	e := NewEnv(t)
	if got := e.Clock.Now(); !got.Equal(Epoch) {
		t.Fatalf("Clock.Now() = %v, want Epoch %v", got, Epoch)
	}
}

// The template is prepared with the months around the real now and around Epoch,
// so a row the database dates itself and a row a test dates from its clock both
// have a partition to land in. The migrations cover a few fixed months and stop
// covering the real now one day, so this runs prepare with a date far past them:
// it must not depend on the calendar.
func TestPrepareProvisionsTheMonthsAroundNowAndTheEpoch(t *testing.T) {
	t.Parallel()
	db := pgtest.New(t)
	future := time.Date(2040, 6, 15, 9, 0, 0, 0, time.UTC)
	if err := prepare(t.Context(), db.Owner, future); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	store := postgres.New(db.Owner)
	for name, at := range map[string]time.Time{"now": future, "the epoch": Epoch} {
		health, err := store.CheckPartitions(t.Context(), at)
		if err != nil {
			t.Fatalf("CheckPartitions(%s): %v", name, err)
		}
		if len(health.Missing) != 0 || len(health.Unscoped) != 0 {
			t.Fatalf("CheckPartitions(%s) = %+v, want this month and the next provisioned and scoped", name, health)
		}
	}
}

// The queue is the real one: a claim on an empty database is empty, not an error.
func TestEnvWiresTheRealQueue(t *testing.T) {
	t.Parallel()
	e := NewEnv(t)
	claims, err := e.Queue.Claim(t.Context(), 0, 10, time.Minute)
	if err != nil || len(claims) != 0 {
		t.Fatalf("Claim on an empty database = %v, %v, want none and no error", claims, err)
	}
}

// The store is the real one, and it reads the Env's clock: what it persists is dated
// from the fake clock, so a test controls the partition a row lands in and the
// instant a scheduled notification becomes visible.
func TestEnvWiresTheRealStoreOnItsClock(t *testing.T) {
	t.Parallel()
	e := NewEnv(t)
	id := domain.Identity{Realm: Realm, Tenant: domain.Tenant{Kind: "workspace", ID: "w1"},
		Recipient: domain.Recipient{Kind: "user", ID: "u1"}}
	persist := func(key string) string {
		t.Helper()
		n := domain.Notification{Tenant: id.Tenant, Recipient: id.Recipient, Topic: "t",
			Priority: domain.PriorityInfo, IdemKey: key}
		r, err := e.Store.PersistAndEnqueue(t.Context(), nil, id, n, domain.DeliveryPlan{Shard: 1})
		if err != nil || !r.Applied {
			t.Fatalf("PersistAndEnqueue(%s) = %+v, %v", key, r, err)
		}
		return r.NotificationID
	}
	createdAt := func(nid string) time.Time {
		t.Helper()
		var at time.Time
		if err := e.DB.Admin.QueryRow(t.Context(),
			`SELECT created_at FROM notifications WHERE id = $1::uuid`, nid).Scan(&at); err != nil {
			t.Fatal(err)
		}
		return at
	}

	first := persist("k1")
	e.Clock.Advance(90 * time.Minute)
	second := persist("k2")
	if got := createdAt(first); !got.Equal(Epoch) {
		t.Errorf("created_at = %v, want the Env's clock, Epoch %v", got, Epoch)
	}
	if got := createdAt(second); !got.Equal(Epoch.Add(90 * time.Minute)) {
		t.Errorf("created_at = %v after advancing the clock, want %v", got, Epoch.Add(90*time.Minute))
	}
}

// Registering a channel is one line, and what it sends is counted by the Env's
// probe.
func TestEnvAddChannelRegistersOneAndReportsToItsProbe(t *testing.T) {
	t.Parallel()
	e := NewEnv(t)
	ch := e.AddChannel("webhook", domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupLocal})

	got, ok := e.Channels.Get("webhook")
	if !ok || got == nil || got.Kind() != "webhook" {
		t.Fatalf("Get(webhook) = %v, %v, want the added channel", got, ok)
	}
	if _, err := ch.Deliver(t.Context(), domain.Delivery{IdemKey: "k1"}); err != nil {
		t.Fatal(err)
	}
	if e.Probe.Sends("k1") != 1 {
		t.Fatalf("Sends(k1) = %d, want 1: the channel reports to the Env's probe", e.Probe.Sends("k1"))
	}
}

// Without WithRedis the Notifier's pre-check is the in-memory one, and Memory is that
// same object; with it the pre-check is Redis, over a database of the test's own, and
// there is no Memory. Either way PreCheck is what the Notifier was built with.
func TestEnvPreCheckIsInMemoryUnlessRedisIsAsked(t *testing.T) {
	t.Parallel()
	mem := NewEnv(t)
	if mem.Redis != nil || mem.Memory == nil || mem.PreCheck != ports.PreCheck(mem.Memory) {
		t.Fatalf("a plain Env has Redis %v, Memory %v, PreCheck %T, want the in-memory pre-check alone", mem.Redis, mem.Memory, mem.PreCheck)
	}

	a, b := NewEnv(t, WithRedis()), NewEnv(t, WithRedis())
	if a.Redis == nil || a.Memory != nil {
		t.Fatalf("a Redis Env has Redis %v and Memory %v, want Redis and no Memory", a.Redis, a.Memory)
	}
	if a.Redis.Index == b.Redis.Index {
		t.Fatalf("two Envs share Redis database %d", a.Redis.Index)
	}
	if err := a.PreCheck.Remember(t.Context(), "k", "n-1", time.Hour); err != nil {
		t.Fatal(err)
	}
	if got, err := a.Redis.Client.Get(t.Context(), "k").Result(); err != nil || got != "n-1" {
		t.Fatalf("Redis holds %q, %v after the Env's pre-check remembered, want n-1", got, err)
	}
	if _, seen, _ := b.PreCheck.Seen(t.Context(), "k"); seen {
		t.Fatal("another Env's pre-check sees the entry: the databases are shared")
	}
	// The Env's pre-check is capped at the idempotency window, as a deployment's is.
	if err := a.PreCheck.Remember(t.Context(), "long", "n-2", 30*24*time.Hour); err != nil {
		t.Fatal(err)
	}
	window := a.Config.WithDefaults().IdempotencyWindow
	if ttl, _ := a.Redis.Client.PTTL(t.Context(), "long").Result(); ttl <= 0 || ttl > window {
		t.Fatalf("an entry lives %v, want at most the idempotency window %v", ttl, window)
	}
}
