//go:build integration

package notifytest

import (
	"os"
	"testing"
	"time"

	"github.com/truongpx396/intel-notification/domain"
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

// The clock starts at Epoch, and the months around it are provisioned, so a row a
// test dates from its clock has a partition to land in. The months around the real
// now are provisioned too, for rows the database dates itself.
func TestEnvStartsAtItsEpochWithItsPartitions(t *testing.T) {
	t.Parallel()
	e := NewEnv(t)
	if got := e.Clock.Now(); !got.Equal(Epoch) {
		t.Fatalf("Clock.Now() = %v, want Epoch %v", got, Epoch)
	}
	for name, at := range map[string]time.Time{"the epoch": Epoch, "now": time.Now()} {
		health, err := e.Maintenance.CheckPartitions(t.Context(), at)
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
