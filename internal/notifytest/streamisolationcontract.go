//go:build integration

package notifytest

import (
	"testing"
	"time"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// RelayWorld is one case's world for StreamIsolationContract: an Env whose registry holds the real
// in-app channel, publishing to the stream the relay subscribes from, and a Relay client of a server
// over the Env's real inbox. The builder wires them; the suite registers the topic and drives the rest.
type RelayWorld struct {
	Env *Env
	// Relay is a client of the relay under test.
	Relay *Relay
	// Publisher is the stream the relay subscribes from, as the in-app channel publishes to it: what a
	// forged nudge is published on.
	Publisher ports.StreamPublisher
}

// streamTopic is the topic every case sends on, delivered on in_app alone.
const streamTopic domain.Topic = "ingestion_complete"

// StreamIsolationContract proves the live path is isolated as strictly as the data layer (D16, NS-001):
// a recipient's stream carries that recipient's notifications and nobody else's, however the nudge got
// there. It runs against the relay a host deploys, through the real in-app channel, the real stream and
// the real row-level-security-scoped inbox, so the property that holds is the one a recipient sees.
func StreamIsolationContract(t *testing.T, newWorld func(t *testing.T) *RelayWorld) {
	t.Helper()

	id := func(tenant, recipient string) domain.Identity {
		return domain.Identity{Realm: Realm, Tenant: domain.Tenant{Kind: "workspace", ID: tenant}, Recipient: domain.Recipient{Kind: "user", ID: recipient}}
	}
	setup := func(t *testing.T) *RelayWorld {
		t.Helper()
		w := newWorld(t)
		w.Env.Topics.Register(Realm, streamTopic, domain.TopicDef{DefaultChannels: []domain.ChannelKind{"in_app"}, DefaultPriority: domain.PriorityInfo})
		return w
	}
	// notifyAndDrain accepts a notification for who and lets the dispatcher deliver it, which is what
	// publishes the nudge.
	notifyAndDrain := func(t *testing.T, w *RelayWorld, who domain.Identity, key string) domain.Receipt {
		t.Helper()
		r, err := w.Env.Notifier.Notify(t.Context(), domain.Notification{
			Tenant: who.Tenant, Recipient: who.Recipient, Topic: streamTopic, IdemKey: key, Title: "Done", Body: key, OccurredAt: Epoch,
		})
		if err != nil || !r.Applied {
			t.Fatalf("Notify(%s) = %+v, %v", key, r, err)
		}
		for s := range w.Env.Config.WithDefaults().Shards {
			if _, err := w.Env.Dispatcher.Drain(t.Context(), domain.Shard(s), 100); err != nil {
				t.Fatalf("Drain(shard %d): %v", s, err)
			}
		}
		return r
	}

	t.Run("the same recipient id in two tenants: each stream sees only its own (D16)", func(t *testing.T) {
		t.Parallel()
		w := setup(t)
		a := w.Relay.Connect(t, id("w1", "u1"))
		b := w.Relay.Connect(t, id("w2", "u1")) // same recipient id, other tenant
		first := notifyAndDrain(t, w, id("w1", "u1"), "k1")

		a.MustReceive(t, first.NotificationID)
		b.MustReceiveNothing(t, time.Second)
	})

	t.Run("a forged nudge naming another recipient's notification shows nothing (D16)", func(t *testing.T) {
		t.Parallel()
		w := setup(t)
		victim := w.Relay.Connect(t, id("w1", "u2"))
		other := notifyAndDrain(t, w, id("w1", "u1"), "k2")

		// The nudge is published to the victim's own stream, naming a notification that is not the victim's:
		// the push path is reached, and the load under the victim's scope returns zero rows.
		if err := w.Publisher.Publish(t.Context(), id("w1", "u2"), other.NotificationID); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		victim.MustReceiveNothing(t, time.Second)

		// ...and the stream is not poisoned: a notification that is the victim's still arrives.
		own := notifyAndDrain(t, w, id("w1", "u2"), "k3")
		victim.MustReceive(t, own.NotificationID)
	})

	t.Run("a nudge for a notification of another realm's recipient shows nothing", func(t *testing.T) {
		t.Parallel()
		w := setup(t)
		victim := w.Relay.Connect(t, id("w1", "u1"))
		// The same tenant and recipient ids under another realm: a different identity, a different key.
		foreign := id("w1", "u1")
		foreign.Realm = "rival"
		if err := w.Publisher.Publish(t.Context(), foreign, "0195f3a0-0000-7000-8000-000000000001"); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		victim.MustReceiveNothing(t, time.Second)
	})

	t.Run("a reconnect delivers the bounded authoritative count first, even after a missed push (D15)", func(t *testing.T) {
		t.Parallel()
		w := setup(t)
		me := id("w1", "u3")
		notifyAndDrain(t, w, me, "m1") // nobody is connected: the nudges are lost, as pub/sub loses them
		notifyAndDrain(t, w, me, "m2")

		conn := w.Relay.Connect(t, me)
		if got := conn.NextCount(t); got != (Count{Count: 2}) {
			t.Fatalf("first event = %+v, want the count of 2 the push never delivered", got)
		}
		third := notifyAndDrain(t, w, me, "m3")
		if got := conn.MustReceive(t, third.NotificationID); got.Unread.Count != 3 {
			t.Errorf("the notification came with a count of %d, want 3", got.Unread.Count)
		}
	})
}
