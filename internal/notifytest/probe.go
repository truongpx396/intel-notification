package notifytest

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// Failure is a way a send can go wrong, armed on a [Probe] with FailNext. The first
// three are what the ChannelContract injects; Infrastructure is a fault the channel
// reports as a Go error rather than as an outcome.
type Failure int

const (
	Transient      Failure = iota + 1 // a temporary refusal: the channel reports Retry
	DeadAddress                       // the address is dead: the channel reports Suppressed
	Permanent                         // a refusal unrelated to the address: the channel reports Rejected
	Infrastructure                    // the channel cannot reach its provider: it returns an error
)

// ErrInfrastructure is the error a channel returns for an [Infrastructure] failure.
var ErrInfrastructure = errors.New("notifytest: injected infrastructure fault")

// Probe stands where a provider would: it counts the sends that reached it per
// idempotency key, and it injects failures into the next sends. It is how a test
// shows that a delivery re-driven after a crash was sent once (NS-002), and that a
// channel sends what it says it does.
type Probe struct {
	mu       sync.Mutex
	sends    map[string]int
	calls    map[string]int
	failures []Failure
}

// NewProbe returns a probe that has seen nothing.
func NewProbe() *Probe {
	return &Probe{sends: map[string]int{}, calls: map[string]int{}}
}

// Sends is how many sends with this key reached the provider. A provider that
// dedupes collapses a re-drive into one.
func (p *Probe) Sends(key string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.sends[key]
}

// Calls is how many times a channel was asked to send with this key, including the
// re-drives a deduping provider collapsed, and the attempts that failed.
func (p *Probe) Calls(key string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls[key]
}

// Total is the number of sends that reached the provider, over every key.
func (p *Probe) Total() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, sends := range p.sends {
		n += sends
	}
	return n
}

// Keys lists every key a channel was asked to send, sorted.
func (p *Probe) Keys() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Sorted(maps.Keys(p.calls))
}

// FailNext arms one failure for a coming send. Failures are consumed in the order
// armed, one per send attempt.
func (p *Probe) FailNext(f Failure) {
	if f < Transient || f > Infrastructure {
		panic(fmt.Sprintf("notifytest: %d is not a failure a probe can inject", int(f)))
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failures = append(p.failures, f)
}

// call records that a channel was asked to send key, and takes the next armed
// failure, if any, in the same step so concurrent calls each get their own.
func (p *Probe) call(key string) (Failure, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls[key]++
	if len(p.failures) == 0 {
		return 0, false
	}
	f := p.failures[0]
	p.failures = p.failures[1:]
	return f, true
}

// send records that a send with key reached the provider. A provider that dedupes
// counts it only the first time.
func (p *Probe) send(key string, dedupes bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if dedupes && p.sends[key] > 0 {
		return
	}
	p.sends[key]++
}

// Channel is a fake ports.Channel that reports to a [Probe]. It honours the Dedup
// level it was given the way a real channel at that level would, and it turns an
// armed failure into the outcome the contract prescribes, so a dispatcher suite
// can drive every branch of step 8 without a provider.
type Channel struct {
	kind  domain.ChannelKind
	caps  domain.ChannelCapabilities
	probe *Probe
}

var _ ports.Channel = (*Channel)(nil)

// NewChannel returns a channel of the given kind and capabilities that reports to
// probe.
func NewChannel(kind domain.ChannelKind, caps domain.ChannelCapabilities, probe *Probe) *Channel {
	return &Channel{kind: kind, caps: caps, probe: probe}
}

// Kind implements ports.Channel.
func (c *Channel) Kind() domain.ChannelKind { return c.kind }

// Capabilities implements ports.Channel.
func (c *Channel) Capabilities() domain.ChannelCapabilities { return c.caps }

// Deliver implements ports.Channel.
func (c *Channel) Deliver(_ context.Context, d domain.Delivery) (domain.DeliveryResult, error) {
	if f, failed := c.probe.call(d.IdemKey); failed {
		switch f {
		case Transient:
			return domain.DeliveryResult{Outcome: domain.Retry, Detail: "injected transient failure"}, nil
		case DeadAddress:
			return domain.DeliveryResult{Outcome: domain.Suppressed, SuppressReason: domain.SuppressHardBounce,
				Detail: "injected dead address"}, nil
		case Permanent:
			return domain.DeliveryResult{Outcome: domain.Rejected, Detail: "injected permanent refusal"}, nil
		default:
			return domain.DeliveryResult{}, ErrInfrastructure
		}
	}
	c.probe.send(d.IdemKey, c.caps.Dedup >= domain.DedupLocal)
	return domain.DeliveryResult{Outcome: domain.Delivered, ProviderMessageID: "msg-" + d.IdemKey}, nil
}
