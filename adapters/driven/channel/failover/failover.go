// Package failover is a composite channel over an ordered list of providers for one
// kind: when one reports a transient failure, the next is tried inside the same
// attempt. Provider failover is a channel and not an engine feature, so the core
// stays channel-agnostic (D29).
package failover

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// Channel tries its members in order, starting at the first on every attempt. It
// keeps no memory of which one answered, so the order is the operator's preference
// at every attempt and nothing about a delivery lives in the channel.
//
// What ends the chain is what each result means. A delivery is done. A Suppressed
// is a verdict on the address, which no other provider can reach, and a Rejected is
// a verdict on the message. Only a transient failure, a Retry or an error, which
// the port says is the same thing, tries the next provider. A provider that cannot
// send for its own reasons (a revoked key, an unverified domain) must report that
// as transient too, and not as a refusal of the message, or the chain cannot route
// around it.
//
// Its dedup level is the weakest of its members' (D29, D32), and that is a limit
// and not a promise: a delivery that the first provider failed and the second sent
// may, when its record is lost to a crash, be re-driven from the first provider,
// which has never seen the key. The composite can be no more truthful than that,
// since a record of which provider sent would be state it deliberately does not have.
type Channel struct {
	members []ports.Channel
	kind    domain.ChannelKind
}

var (
	_ ports.Channel           = (*Channel)(nil)
	_ ports.AddressNormalizer = (*Channel)(nil)
)

// New returns a channel over members, tried in the order given. Members must be
// channels of one kind. Anything else is a wiring mistake and panics, so it is found
// when the host starts.
func New(members ...ports.Channel) *Channel {
	if len(members) == 0 {
		panic("failover: New needs at least one provider")
	}
	for i, m := range members {
		if m == nil {
			panic(fmt.Sprintf("failover: provider %d is nil", i+1))
		}
		if m.Kind() != members[0].Kind() {
			panic(fmt.Sprintf("failover: providers must be of one kind, got %q and %q", members[0].Kind(), m.Kind()))
		}
	}
	return &Channel{members: append([]ports.Channel(nil), members...), kind: members[0].Kind()}
}

// Kind implements ports.Channel.
func (c *Channel) Kind() domain.ChannelKind { return c.kind }

// Capabilities implements ports.Channel. The composite needs what any member needs,
// so the renderer supplies it, and offers what every member offers, so a notification
// is not called inbox-visible on one member's strength. Its dedup level is the
// weakest of its members', and its window, which means something only for
// DedupProvider, the shortest.
func (c *Channel) Capabilities() domain.ChannelCapabilities {
	out := c.members[0].Capabilities()
	window := out.DedupWindow
	for _, m := range c.members[1:] {
		mc := m.Capabilities()
		out.NeedsSubject = out.NeedsSubject || mc.NeedsSubject
		out.NeedsAddress = out.NeedsAddress || mc.NeedsAddress
		out.RichContent = out.RichContent || mc.RichContent
		out.InboxBacked = out.InboxBacked && mc.InboxBacked
		out.Dedup = min(out.Dedup, mc.Dedup)
		window = min(window, mc.DedupWindow)
	}
	if out.Dedup == domain.DedupProvider {
		out.DedupWindow = window
	} else {
		out.DedupWindow = 0
	}
	return out
}

// NormalizeAddress implements ports.AddressNormalizer. Address equality belongs to
// the kind, so the first member that defines it answers for all, and a value is
// left as it is when none does.
func (c *Channel) NormalizeAddress(v string) string {
	for _, m := range c.members {
		if n, ok := m.(ports.AddressNormalizer); ok {
			return n.NormalizeAddress(v)
		}
	}
	return v
}

// Deliver implements ports.Channel. Every member is handed the same delivery, key
// included. When none can send, the result is Retry, whose RetryAfter is the
// earliest any member offered (none, if any offered none: one provider's hint does
// not bind a delivery another may carry) and whose Detail says what each said. A
// context that ends stops the chain, and the error is the context's.
func (c *Channel) Deliver(ctx context.Context, d domain.Delivery) (domain.DeliveryResult, error) {
	said := make([]string, 0, len(c.members))
	var floor time.Duration
	for i, m := range c.members {
		if err := ctx.Err(); err != nil {
			return domain.DeliveryResult{}, err
		}
		r, err := m.Deliver(ctx, d)
		var hint time.Duration
		var detail string
		switch {
		case err != nil:
			detail = err.Error()
		case r.Outcome == domain.Delivered, r.Outcome == domain.Suppressed, r.Outcome == domain.Rejected:
			return r, nil
		case r.Outcome == domain.Retry:
			hint, detail = r.RetryAfter, r.Detail
		default:
			detail = fmt.Sprintf("unknown outcome %d", int(r.Outcome))
		}
		if i == 0 || hint < floor {
			floor = hint
		}
		said = append(said, fmt.Sprintf("provider %d: %s", i+1, detail))
	}
	return domain.DeliveryResult{Outcome: domain.Retry, RetryAfter: floor, Detail: strings.Join(said, "; ")}, nil
}
