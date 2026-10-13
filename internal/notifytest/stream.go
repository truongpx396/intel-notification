package notifytest

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// Nudge is one publish a [Stream] received: whom it was addressed to, and the
// notification id it carried, which is all a nudge carries (D16).
type Nudge struct {
	Identity       domain.Identity
	NotificationID string
}

// Stream is an in-memory ports.StreamPublisher that reports to a [Probe]. It
// refuses what the Redis stream refuses, an identity with an empty component or a
// nudge with no id, as a permanent fault, so a channel is tested against the same
// verdicts it meets in production.
//
// The probe counts a send per notification id and collapses a repeat: that is the
// relay's behaviour, which loads one inbox row per id, however many nudges name it.
// It is what makes a re-publish "only a re-nudge" (D16), and it is not something
// the in-app channel could break.
//
// It is also the subscribing end (ports.StreamSubscriber), so a relay's unit test has a live path
// without Redis. A nudge goes to the subscribers of exactly the identity it names, and is dropped
// for one that has stopped reading, as a pub/sub message is: the stream is at-most-once (D15).
type Stream struct {
	probe *Probe

	mu     sync.Mutex
	nudges []Nudge
	subs   map[domain.Identity]map[*subscription]struct{}
}

var (
	_ ports.StreamPublisher  = (*Stream)(nil)
	_ ports.StreamSubscriber = (*Stream)(nil)
)

// NewStream returns a stream reporting to probe.
func NewStream(probe *Probe) *Stream {
	return &Stream{probe: probe, subs: map[domain.Identity]map[*subscription]struct{}{}}
}

// Publish implements ports.StreamPublisher.
func (s *Stream) Publish(_ context.Context, id domain.Identity, notificationID string) error {
	if err := id.Validate(); err != nil {
		return fmt.Errorf("notifytest: %w: %w", domain.ErrPermanent, err)
	}
	if notificationID == "" {
		return fmt.Errorf("notifytest: %w: no notification id to nudge", domain.ErrPermanent)
	}
	if f, failed := s.probe.call(notificationID); failed {
		if f == Permanent {
			return fmt.Errorf("notifytest: injected refusal: %w", domain.ErrPermanent)
		}
		// A stream has no address to be dead and no verdict short of a refusal, so
		// every other fault is the transient kind.
		return ErrInfrastructure
	}
	s.probe.send(notificationID, true)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nudges = append(s.nudges, Nudge{Identity: id, NotificationID: notificationID})
	for sub := range s.subs[id] {
		select {
		case sub.ids <- notificationID:
		default: // a subscriber that has stopped reading misses it
		}
	}
	return nil
}

// Nudges lists what was published, in order.
func (s *Stream) Nudges() []Nudge {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.nudges)
}

// subscriptionBuffer is how many nudges a subscriber may fall behind by before it misses one.
const subscriptionBuffer = 256

// subscription is one subscriber of one identity.
type subscription struct {
	stream *Stream
	id     domain.Identity
	ids    chan string
	once   sync.Once
	stop   func() bool // releases the context watch; guarded by the stream's lock
}

// IDs implements ports.Subscription.
func (u *subscription) IDs() <-chan string { return u.ids }

// Close implements ports.Subscription. The subscriber leaves and its channel closes under the lock
// a publish holds, so a publish never sends on a closed channel.
func (u *subscription) Close() error {
	u.once.Do(func() {
		u.stream.mu.Lock()
		defer u.stream.mu.Unlock()
		if u.stop != nil {
			u.stop()
		}
		delete(u.stream.subs[u.id], u)
		if len(u.stream.subs[u.id]) == 0 {
			delete(u.stream.subs, u.id)
		}
		close(u.ids)
	})
	return nil
}

// Subscribe implements ports.StreamSubscriber. It refuses an identity with an empty component, as
// the Redis stream does, and ends the subscription with ctx.
func (s *Stream) Subscribe(ctx context.Context, id domain.Identity) (ports.Subscription, error) {
	if err := id.Validate(); err != nil {
		return nil, fmt.Errorf("notifytest: %w: %w", domain.ErrPermanent, err)
	}
	sub := &subscription{stream: s, id: id, ids: make(chan string, subscriptionBuffer)}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.subs[id] == nil {
		s.subs[id] = map[*subscription]struct{}{}
	}
	s.subs[id][sub] = struct{}{}
	// Armed under the lock Close takes, so a context that has already ended cannot close the
	// subscription before it is registered, or race the assignment.
	sub.stop = context.AfterFunc(ctx, func() { _ = sub.Close() })
	return sub, nil
}
