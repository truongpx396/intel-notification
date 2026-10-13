package notifytest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/truongpx396/intel-notification/domain"
)

func scriptedDelivery(key string) domain.Delivery {
	return domain.Delivery{
		ID: "d-" + key, IdemKey: key, Channel: "email",
		Address: domain.Address{Channel: "email", Value: "a@example.com", Meta: map[string]string{"k": "v"}},
		Digest:  []domain.Notification{{Title: "one", Data: map[string]any{"n": 1}}},
	}
}

// With no script the channel delivers, and says which key it delivered.
func TestScriptedDeliversWhenItsScriptIsEmpty(t *testing.T) {
	t.Parallel()
	s := NewScripted("email", domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupLocal})
	r, err := s.Deliver(t.Context(), scriptedDelivery("k1"))
	if err != nil || r.Outcome != domain.Delivered || r.ProviderMessageID != "msg-k1" {
		t.Fatalf("Deliver = %+v, %v, want Delivered as msg-k1", r, err)
	}
	if s.Kind() != "email" || !s.Capabilities().NeedsAddress || s.Capabilities().Dedup != domain.DedupLocal {
		t.Fatalf("Kind %q, Capabilities %+v: not what it was built with", s.Kind(), s.Capabilities())
	}
}

// Answers are used in order, one per send, and then the channel delivers again.
func TestScriptedAnswersInOrderThenDelivers(t *testing.T) {
	t.Parallel()
	boom := errors.New("boom")
	s := NewScripted("email", domain.ChannelCapabilities{}).
		Then(domain.DeliveryResult{Outcome: domain.Retry, RetryAfter: time.Hour, Detail: "slow down"}, nil).
		Then(domain.DeliveryResult{}, boom).
		Then(domain.DeliveryResult{Outcome: domain.Suppressed, SuppressReason: domain.SuppressHardBounce, SuppressFor: time.Minute}, nil)

	r, err := s.Deliver(t.Context(), scriptedDelivery("k1"))
	if err != nil || r.Outcome != domain.Retry || r.RetryAfter != time.Hour || r.Detail != "slow down" {
		t.Fatalf("first answer = %+v, %v", r, err)
	}
	if _, err := s.Deliver(t.Context(), scriptedDelivery("k1")); !errors.Is(err, boom) {
		t.Fatalf("second answer error = %v, want the scripted one", err)
	}
	r, err = s.Deliver(t.Context(), scriptedDelivery("k1"))
	if err != nil || r.Outcome != domain.Suppressed || r.SuppressReason != domain.SuppressHardBounce || r.SuppressFor != time.Minute {
		t.Fatalf("third answer = %+v, %v", r, err)
	}
	if r, err := s.Deliver(t.Context(), scriptedDelivery("k1")); err != nil || r.Outcome != domain.Delivered {
		t.Fatalf("after the script, Deliver = %+v, %v, want Delivered", r, err)
	}
}

// It keeps what it was asked, in order, as copies: the dispatcher owns the
// delivery it built, and a test reading it must not alias what it can still change.
func TestScriptedRemembersWhatItWasAsked(t *testing.T) {
	t.Parallel()
	s := NewScripted("email", domain.ChannelCapabilities{})
	for _, key := range []string{"k1", "k2"} {
		if _, err := s.Deliver(t.Context(), scriptedDelivery(key)); err != nil {
			t.Fatal(err)
		}
	}
	got := s.Deliveries()
	if len(got) != 2 || got[0].IdemKey != "k1" || got[1].IdemKey != "k2" {
		t.Fatalf("Deliveries() = %+v, want k1 then k2", got)
	}
	got[0].Address.Meta["k"] = "changed"
	got[0].Digest[0].Title = "changed"
	got[0].Digest[0].Data["n"] = 99
	again := s.Deliveries()
	if again[0].Address.Meta["k"] != "v" || again[0].Digest[0].Title != "one" || again[0].Digest[0].Data["n"] != 1 {
		t.Fatalf("Deliveries() shares its maps and slices with the caller: %+v", again[0])
	}
}

// And what it keeps is a copy of what the dispatcher handed it: the dispatcher goes on
// owning the delivery it built, and a change it makes afterwards must not rewrite what
// the channel was asked.
func TestScriptedKeepsACopyOfWhatItWasAsked(t *testing.T) {
	t.Parallel()
	s := NewScripted("email", domain.ChannelCapabilities{})
	d := scriptedDelivery("k1")
	if _, err := s.Deliver(t.Context(), d); err != nil {
		t.Fatal(err)
	}
	d.Address.Meta["k"] = "changed"
	d.Digest[0].Data["n"] = 99
	d.Digest[0].Title = "changed"
	got := s.Deliveries()[0]
	if got.Address.Meta["k"] != "v" || got.Digest[0].Data["n"] != 1 || got.Digest[0].Title != "one" {
		t.Fatalf("the channel's record changed with the caller's delivery: %+v", got)
	}
}

// The hook runs on every send before it is answered, with the context and the
// delivery; it may hold the send in flight or panic as a dead worker would.
func TestScriptedHookRunsBeforeTheAnswer(t *testing.T) {
	t.Parallel()
	var order []string
	s := NewScripted("email", domain.ChannelCapabilities{}).
		OnDeliver(func(_ context.Context, d domain.Delivery) { order = append(order, "hook:"+d.IdemKey) }).
		Then(domain.DeliveryResult{Outcome: domain.Retry}, nil)
	r, err := s.Deliver(t.Context(), scriptedDelivery("k1"))
	order = append(order, "answered")
	if err != nil || r.Outcome != domain.Retry {
		t.Fatalf("Deliver = %+v, %v", r, err)
	}
	if len(order) != 2 || order[0] != "hook:k1" || order[1] != "answered" {
		t.Fatalf("order = %v, want the hook, then the answer", order)
	}
	if got := len(s.Deliveries()); got != 1 {
		t.Fatalf("%d deliveries remembered, want 1", got)
	}
}
