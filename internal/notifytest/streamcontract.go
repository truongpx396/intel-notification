package notifytest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// LiveStream is both ends of the live stream: what the in-app channel publishes to and what
// the relay subscribes from.
type LiveStream interface {
	ports.StreamPublisher
	ports.StreamSubscriber
}

// StreamFactory returns a stream for t alone. Redis pub/sub is shared by every database on the
// server, so the cases below publish only as identities of their own (a realm derived from the
// test's name), and a factory need not isolate anything else.
type StreamFactory func(t *testing.T) LiveStream

// streamWait bounds a wait for a nudge, so a regression fails and does not hang.
const streamWait = 10 * time.Second

// StreamContract is the table every pair of ports.StreamPublisher and ports.StreamSubscriber
// runs: the in-memory [Stream] in this package and the Redis adapter alike, so neither can
// drift from what the other decided where the port is silent. It asserts the live path's
// isolation (D16, NS-001), through the ports, with no waiting for silence: a nudge published to
// another identity would be a listener's first message, ahead of the sentinel published to it
// afterwards.
func StreamContract(t *testing.T, newStream StreamFactory) {
	t.Helper()

	t.Run("a subscriber hears what is published to its identity, in order", func(t *testing.T) {
		t.Parallel()
		s, me := newStream(t), streamIdentity(t, "w1", "u1")
		sub := mustSubscribe(t, s, me)
		for _, id := range []string{"n-1", "n-2", "n-3"} {
			if err := s.Publish(t.Context(), me, id); err != nil {
				t.Fatalf("Publish %s: %v", id, err)
			}
		}
		for _, want := range []string{"n-1", "n-2", "n-3"} {
			if got := nextNudge(t, sub); got != want {
				t.Fatalf("heard %q, want %q: nudges arrive as published, in order", got, want)
			}
		}
	})

	t.Run("a subscriber hears no other identity, whichever component differs", func(t *testing.T) {
		t.Parallel()
		others := []struct {
			name  string
			other func(domain.Identity) domain.Identity
		}{
			{"the same recipient id in another tenant", func(i domain.Identity) domain.Identity { i.Tenant.ID = "w2"; return i }},
			{"another kind of tenant", func(i domain.Identity) domain.Identity { i.Tenant.Kind = "organization"; return i }},
			{"another recipient in the tenant", func(i domain.Identity) domain.Identity { i.Recipient.ID = "u2"; return i }},
			{"another kind of recipient", func(i domain.Identity) domain.Identity { i.Recipient.Kind = "device"; return i }},
			{"the same tenant and recipient in another realm", func(i domain.Identity) domain.Identity { i.Realm += "-x"; return i }},
		}
		for _, tc := range others {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				s, me := newStream(t), streamIdentity(t, "w1", "u1")
				other := tc.other(me)
				mine, theirs := mustSubscribe(t, s, me), mustSubscribe(t, s, other)
				if err := s.Publish(t.Context(), me, "n-mine"); err != nil {
					t.Fatalf("Publish mine: %v", err)
				}
				if err := s.Publish(t.Context(), other, "n-sentinel"); err != nil {
					t.Fatalf("Publish theirs: %v", err)
				}
				if got := nextNudge(t, mine); got != "n-mine" {
					t.Fatalf("the subscriber heard %q, want n-mine", got)
				}
				if got := nextNudge(t, theirs); got != "n-sentinel" {
					t.Fatalf("the other identity's subscriber heard %q first, want only its own n-sentinel: a nudge crossed an identity boundary", got)
				}
			})
		}
	})

	t.Run("every subscriber of an identity hears it", func(t *testing.T) {
		t.Parallel()
		s, me := newStream(t), streamIdentity(t, "w1", "u1")
		var subs []ports.Subscription
		for range 3 {
			subs = append(subs, mustSubscribe(t, s, me))
		}
		if err := s.Publish(t.Context(), me, "n-1"); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		for i, sub := range subs {
			if got := nextNudge(t, sub); got != "n-1" {
				t.Fatalf("subscriber %d heard %q, want n-1: a recipient may have several devices connected", i, got)
			}
		}
	})

	t.Run("a nudge nobody hears is not an error", func(t *testing.T) {
		t.Parallel()
		s, me := newStream(t), streamIdentity(t, "w1", "u1")
		if err := s.Publish(t.Context(), me, "n-1"); err != nil {
			t.Fatalf("Publish with no subscriber = %v, want nil: the stream is at-most-once (D15)", err)
		}
	})

	t.Run("a nudge published before the subscription is not replayed", func(t *testing.T) {
		t.Parallel()
		s, me := newStream(t), streamIdentity(t, "w1", "u1")
		if err := s.Publish(t.Context(), me, "n-old"); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		sub := mustSubscribe(t, s, me)
		if err := s.Publish(t.Context(), me, "n-new"); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		if got := nextNudge(t, sub); got != "n-new" {
			t.Fatalf("heard %q first, want n-new: the stream keeps no history, a reconnect recomputes the count from the store (D15)", got)
		}
	})

	t.Run("Close ends the subscription and returns with it stopped, twice or more", func(t *testing.T) {
		t.Parallel()
		s, me := newStream(t), streamIdentity(t, "w1", "u1")
		sub := mustSubscribe(t, s, me)
		for i := range 2 {
			if err := sub.Close(); err != nil {
				t.Fatalf("Close #%d = %v, want nil", i+1, err)
			}
		}
		select {
		case id, ok := <-sub.IDs():
			if ok {
				t.Fatalf("IDs delivered %q after Close", id)
			}
		default:
			t.Fatal("IDs was still open when Close returned: Close waits for the subscription to stop")
		}
		if err := s.Publish(t.Context(), me, "n-1"); err != nil {
			t.Fatalf("Publish after Close = %v, want nil", err)
		}
	})

	t.Run("a subscription ends with its context", func(t *testing.T) {
		t.Parallel()
		s, me := newStream(t), streamIdentity(t, "w1", "u1")
		ctx, cancel := context.WithCancel(t.Context())
		sub, err := s.Subscribe(ctx, me)
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		cancel()
		select {
		case id, ok := <-sub.IDs():
			if ok {
				t.Fatalf("IDs delivered %q after the context ended", id)
			}
		case <-time.After(streamWait):
			t.Fatal("IDs still open after the subscription's context ended")
		}
	})

	t.Run("an identity or an id with nothing to nudge is permanent", func(t *testing.T) {
		t.Parallel()
		s, me := newStream(t), streamIdentity(t, "w1", "u1")
		noTenant, noRecipient := me, me
		noTenant.Tenant = domain.Tenant{}
		noRecipient.Recipient = domain.Recipient{}
		for _, tc := range []struct {
			name string
			id   domain.Identity
			nid  string
		}{
			{"no notification id", me, ""},
			{"no tenant", noTenant, "n-1"},
			{"no recipient", noRecipient, "n-1"},
		} {
			if err := s.Publish(t.Context(), tc.id, tc.nid); !errors.Is(err, domain.ErrPermanent) {
				t.Errorf("Publish with %s = %v, want an error wrapping ErrPermanent: no retry makes a nudge of nothing", tc.name, err)
			}
		}
		for name, id := range map[string]domain.Identity{"no tenant": noTenant, "no recipient": noRecipient} {
			if sub, err := s.Subscribe(t.Context(), id); !errors.Is(err, domain.ErrPermanent) {
				if err == nil {
					_ = sub.Close()
				}
				t.Errorf("Subscribe with %s = %v, want an error wrapping ErrPermanent: a stream of nobody is every stream", name, err)
			}
		}
	})
}

// streamIdentity is an identity no other test publishes as: the realm is derived from the
// test's name, which is unique within a test binary.
func streamIdentity(t *testing.T, tenant, recipient string) domain.Identity {
	t.Helper()
	sum := sha256.Sum256([]byte(t.Name()))
	return domain.Identity{
		Realm:     domain.Realm("t" + hex.EncodeToString(sum[:6])),
		Tenant:    domain.Tenant{Kind: "workspace", ID: tenant},
		Recipient: domain.Recipient{Kind: "user", ID: recipient},
	}
}

func mustSubscribe(t *testing.T, s LiveStream, id domain.Identity) ports.Subscription {
	t.Helper()
	sub, err := s.Subscribe(t.Context(), id)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	return sub
}

func nextNudge(t *testing.T, sub ports.Subscription) string {
	t.Helper()
	select {
	case id, ok := <-sub.IDs():
		if !ok {
			t.Fatal("the subscription closed before a nudge arrived")
		}
		return id
	case <-time.After(streamWait):
		t.Fatalf("no nudge within %v", streamWait)
		return ""
	}
}
