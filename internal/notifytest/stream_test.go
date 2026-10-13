package notifytest

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/truongpx396/intel-notification/domain"
)

var streamID = domain.Identity{
	Realm:     "aisat",
	Tenant:    domain.Tenant{Kind: "workspace", ID: "w1"},
	Recipient: domain.Recipient{Kind: "user", ID: "u1"},
}

// The fake refuses what the Redis stream refuses, and answers a fault the way the
// in-app channel is told to read it: a refusal is permanent, anything else is not.
func TestStreamAnswersAsTheRedisStreamDoes(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		id        domain.Identity
		nid       string
		arm       Failure
		permanent bool
		fails     bool
	}{
		{name: "a nudge", id: streamID, nid: "n1"},
		{name: "no notification id", id: streamID, nid: "", permanent: true, fails: true},
		{name: "an identity with no tenant", id: domain.Identity{Realm: "aisat", Recipient: streamID.Recipient}, nid: "n1", permanent: true, fails: true},
		{name: "a refusal", id: streamID, nid: "n1", arm: Permanent, permanent: true, fails: true},
		{name: "a transient fault", id: streamID, nid: "n1", arm: Transient, fails: true},
		{name: "a dead address, which a stream cannot have", id: streamID, nid: "n1", arm: DeadAddress, fails: true},
		{name: "an infrastructure fault", id: streamID, nid: "n1", arm: Infrastructure, fails: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := NewProbe()
			s := NewStream(p)
			if tc.arm != 0 {
				p.FailNext(tc.arm)
			}
			err := s.Publish(t.Context(), tc.id, tc.nid)
			if (err != nil) != tc.fails {
				t.Fatalf("Publish = %v, want failure %v", err, tc.fails)
			}
			if got := errors.Is(err, domain.ErrPermanent); got != tc.permanent {
				t.Fatalf("Publish = %v, permanent %v, want %v", err, got, tc.permanent)
			}
			if tc.fails && len(s.Nudges()) != 0 {
				t.Fatalf("a refused publish recorded %v", s.Nudges())
			}
		})
	}
}

// Nudges are kept in order with whom they were for, and the probe sees one send
// per notification id, the relay's one item however often it is nudged.
func TestStreamRecordsNudgesAndCollapsesARepeatForTheProbe(t *testing.T) {
	t.Parallel()
	p := NewProbe()
	s := NewStream(p)
	other := streamID
	other.Tenant.ID = "w2"
	for _, step := range []struct {
		id  domain.Identity
		nid string
	}{{streamID, "n1"}, {other, "n2"}, {streamID, "n1"}} {
		if err := s.Publish(t.Context(), step.id, step.nid); err != nil {
			t.Fatalf("Publish(%s): %v", step.nid, err)
		}
	}
	want := []Nudge{{streamID, "n1"}, {other, "n2"}, {streamID, "n1"}}
	got := s.Nudges()
	if len(got) != len(want) {
		t.Fatalf("Nudges = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Nudges[%d] = %v, want %v", i, got[i], want[i])
		}
	}
	if p.Sends("n1") != 1 || p.Calls("n1") != 2 || p.Sends("n2") != 1 {
		t.Fatalf("probe saw n1 sends=%d calls=%d, n2 sends=%d, want 1, 2 and 1", p.Sends("n1"), p.Calls("n1"), p.Sends("n2"))
	}
}

// The in-memory stream runs the table the Redis adapter runs, so what a relay's unit test sees
// of the live path is what Redis gives it (T044).
func TestStreamRunsTheStreamContract(t *testing.T) {
	t.Parallel()
	StreamContract(t, func(*testing.T) LiveStream { return NewStream(NewProbe()) })
}

// A context that has already ended closes the subscription it was given. Under -race this
// also fails if arming the context watch races the subscription's registration.
func TestStreamSubscribeWithAnEndedContext(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	s := NewStream(NewProbe())
	for range 50 {
		sub, err := s.Subscribe(ctx, streamID)
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		select {
		case _, ok := <-sub.IDs():
			if ok {
				t.Fatal("a nudge arrived on a subscription whose context had ended")
			}
		case <-time.After(10 * time.Second):
			t.Fatal("IDs still open for a subscription whose context had ended")
		}
		if err := s.Publish(t.Context(), streamID, "n1"); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
}

// A subscriber that has stopped reading misses nudges, as a pub/sub message is missed, and never
// holds up the publisher: a stalled relay must not stall the dispatcher that publishes to it.
func TestStreamPublishNeverWaitsForASlowSubscriber(t *testing.T) {
	t.Parallel()
	s := NewStream(NewProbe())
	sub, err := s.Subscribe(t.Context(), streamID)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	// No Close: if Publish does block it holds the stream's lock, and a Close here would wait
	// on it and turn the failure below into a hang. The stream is this test's alone.

	const published = 3 * subscriptionBuffer
	done := make(chan error, 1)
	go func() {
		for i := range published {
			if err := s.Publish(t.Context(), streamID, fmt.Sprintf("n%d", i)); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Publish: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Publish blocked on a subscriber that was not reading")
	}
	heard := 0
	for {
		select {
		case <-sub.IDs():
			heard++
			continue
		default:
		}
		break
	}
	if heard != subscriptionBuffer {
		t.Fatalf("the slow subscriber heard %d of %d, want the %d its buffer held and none of the rest", heard, published, subscriptionBuffer)
	}
}
