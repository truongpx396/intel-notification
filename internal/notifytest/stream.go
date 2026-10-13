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
type Stream struct {
	probe *Probe

	mu     sync.Mutex
	nudges []Nudge
}

var _ ports.StreamPublisher = (*Stream)(nil)

// NewStream returns a stream reporting to probe.
func NewStream(probe *Probe) *Stream { return &Stream{probe: probe} }

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
	return nil
}

// Nudges lists what was published, in order.
func (s *Stream) Nudges() []Nudge {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.nudges)
}
