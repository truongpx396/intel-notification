//go:build integration

package notifytest

import (
	"os"
	"testing"
	"time"

	"github.com/truongpx396/intel-notification/adapters/driven/postgres"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/pgtest"
)

func TestMain(m *testing.M) { os.Exit(Main(m)) }

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
