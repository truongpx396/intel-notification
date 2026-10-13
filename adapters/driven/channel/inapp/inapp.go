// Package inapp is the reference in-app channel: a nudge on the identity's live
// stream, naming a notification the inbox already holds.
//
// The inbox row is the delivery. A notification is visible in the inbox because a
// channel with the InboxBacked capability was enabled when it was accepted (D26),
// and the channel's own work is the courtesy of saying so to a connected client: a
// nudge naming the row, which the relay re-reads under the session's identity (D16).
// That is why the channel is idempotent by construction, and keeps no record of its
// own: a re-drive names the same row again, and the client shows one item.
package inapp

import (
	"context"
	"errors"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// Kind is the channel kind a topic's DefaultChannels and a preference name.
const Kind domain.ChannelKind = "in_app"

// Channel publishes a nudge for a delivery.
type Channel struct {
	stream ports.StreamPublisher
}

var _ ports.Channel = (*Channel)(nil)

// New returns the in-app channel over stream. A nil stream is a wiring mistake and
// panics, as registering a channel with no kind does, so it is found when the host
// starts and not on the first delivery.
func New(stream ports.StreamPublisher) *Channel {
	if stream == nil {
		panic("inapp: New needs a ports.StreamPublisher")
	}
	return &Channel{stream: stream}
}

// Kind implements ports.Channel.
func (*Channel) Kind() domain.ChannelKind { return Kind }

// Capabilities implements ports.Channel. The channel is addressed by the
// recipient's identity alone, so it needs no address and is never suppressed; it
// makes a notification visible in the inbox; and a re-drive is only a re-nudge
// (DedupLocal). RichContent is true because the item the relay loads carries the
// notification's structured data to the client.
func (*Channel) Capabilities() domain.ChannelCapabilities {
	return domain.ChannelCapabilities{
		NeedsAddress: false,
		RichContent:  true,
		InboxBacked:  true,
		Dedup:        domain.DedupLocal,
	}
}

// Deliver implements ports.Channel. The nudge is the notification id, published
// to the delivery's own identity, and nothing else: no title, body or count
// (NR-024). What the stream reports decides the outcome: a refusal wrapping
// domain.ErrPermanent is Rejected, so it is dead-lettered at once with its reason,
// and any other fault is Retry (D10, D12).
//
// The planner never digests an inbox-backed channel (D39), so a digest delivery
// should not reach this channel. One that does, from a queue planned before that
// rule or by a host whose channel set changed, names no notification: its members
// carry no ids, and their inbox rows are visible already. There is nothing to point
// a nudge at, and dead-lettering the delivery would lose nothing the inbox does not
// hold, so it succeeds without one. A delivery that names neither a notification nor
// a digest is malformed, and no retry changes that.
func (c *Channel) Deliver(ctx context.Context, d domain.Delivery) (domain.DeliveryResult, error) {
	if d.NotificationID == "" {
		if d.DigestID != "" {
			return domain.DeliveryResult{Outcome: domain.Delivered}, nil
		}
		return domain.DeliveryResult{Outcome: domain.Rejected, Detail: "inapp: the delivery names neither a notification nor a digest"}, nil
	}
	if err := c.stream.Publish(ctx, d.Identity, d.NotificationID); err != nil {
		if errors.Is(err, domain.ErrPermanent) {
			return domain.DeliveryResult{Outcome: domain.Rejected, Detail: err.Error()}, nil
		}
		return domain.DeliveryResult{Outcome: domain.Retry, Detail: err.Error()}, nil
	}
	return domain.DeliveryResult{Outcome: domain.Delivered}, nil
}
