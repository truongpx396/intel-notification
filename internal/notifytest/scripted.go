package notifytest

import (
	"context"
	"maps"
	"sync"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// Scripted is a ports.Channel that answers from a script and remembers what it was
// asked. The kit's [Channel] reports to a [Probe] and cannot say what the
// dispatcher handed it, nor answer with a Retry-After or a chosen suppression; this
// one is for the cases that assert on the Delivery itself (its key, its content, its
// address, a digest's members) or that need an outcome the probe cannot arm.
type Scripted struct {
	kind domain.ChannelKind
	caps domain.ChannelCapabilities

	mu     sync.Mutex
	script []scriptedAnswer
	hook   func(context.Context, domain.Delivery)
	got    []domain.Delivery
}

type scriptedAnswer struct {
	res domain.DeliveryResult
	err error
}

var _ ports.Channel = (*Scripted)(nil)

// NewScripted returns a channel of the given kind and capabilities with an empty
// script. Until it is given answers it delivers every send.
func NewScripted(kind domain.ChannelKind, caps domain.ChannelCapabilities) *Scripted {
	return &Scripted{kind: kind, caps: caps}
}

// Then queues the answer to a coming send. Answers are used in order, one per send;
// when they run out the channel delivers.
func (s *Scripted) Then(res domain.DeliveryResult, err error) *Scripted {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.script = append(s.script, scriptedAnswer{res: res, err: err})
	return s
}

// OnDeliver sets a function called on every send, before it is answered. It may
// block, to hold a send in flight, or panic.
func (s *Scripted) OnDeliver(fn func(context.Context, domain.Delivery)) *Scripted {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hook = fn
	return s
}

// Deliveries lists what the channel was asked to send, in order.
func (s *Scripted) Deliveries() []domain.Delivery {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]domain.Delivery, len(s.got))
	for i, d := range s.got {
		out[i] = cloneDelivery(d)
	}
	return out
}

// Kind implements ports.Channel.
func (s *Scripted) Kind() domain.ChannelKind { return s.kind }

// Capabilities implements ports.Channel.
func (s *Scripted) Capabilities() domain.ChannelCapabilities { return s.caps }

// Deliver implements ports.Channel.
func (s *Scripted) Deliver(ctx context.Context, d domain.Delivery) (domain.DeliveryResult, error) {
	s.mu.Lock()
	s.got = append(s.got, cloneDelivery(d))
	hook := s.hook
	s.mu.Unlock()

	// The hook runs outside the lock: it may hold the send in flight, and a second
	// send must not wait behind it.
	if hook != nil {
		hook(ctx, d)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.script) == 0 {
		return domain.DeliveryResult{Outcome: domain.Delivered, ProviderMessageID: "msg-" + d.IdemKey}, nil
	}
	a := s.script[0]
	s.script = s.script[1:]
	return a.res, a.err
}

// cloneDelivery copies what a test could otherwise change under the dispatcher: the
// address's hints, and a digest's members.
func cloneDelivery(d domain.Delivery) domain.Delivery {
	d.Address.Meta = maps.Clone(d.Address.Meta)
	d.Content.Data = maps.Clone(d.Content.Data)
	d.Notification = cloneNotification(d.Notification)
	if d.Digest != nil {
		members := make([]domain.Notification, len(d.Digest))
		for i, m := range d.Digest {
			members[i] = cloneNotification(m)
		}
		d.Digest = members
	}
	return d
}

func cloneNotification(n domain.Notification) domain.Notification {
	n.Data = maps.Clone(n.Data)
	n.Payload = maps.Clone(n.Payload)
	n.Attributes = maps.Clone(n.Attributes)
	return n
}
