package notifytest

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/truongpx396/intel-notification/domain"
)

// A contract that has never failed proves nothing (T029). So the suite is run,
// here and for good, against channels each broken in one way, and has to name the
// case that catches it. A broken channel that passes is a hole in the contract.

// caseFailed is what the recorder panics with to stop a case at its first
// failure, as t.Fatalf does.
type caseFailed struct{}

// recorder is a contractT that remembers the first failure instead of failing a
// test.
type recorder struct {
	msg string
}

func (*recorder) Helper()                     {}
func (*recorder) Context() context.Context    { return context.Background() }
func (r *recorder) Fatalf(f string, a ...any) { r.msg = fmt.Sprintf(f, a...); panic(caseFailed{}) }

// failures runs every case against a channel from newChannel and returns the
// failure message of each one that failed, by case name.
func failures(t *testing.T, newChannel ChannelFactory) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, c := range channelCases() {
		r := &recorder{}
		func() {
			defer func() {
				if p := recover(); p != nil {
					if _, ok := p.(caseFailed); !ok {
						panic(p)
					}
				}
			}()
			c.run(r, newChannel(t))
		}()
		if r.msg != "" {
			out[c.name] = r.msg
		}
	}
	return out
}

// fnChannel is a channel whose behaviour a test writes inline.
type fnChannel struct {
	caps    domain.ChannelCapabilities
	deliver func(context.Context, domain.Delivery) (domain.DeliveryResult, error)
}

func (fnChannel) Kind() domain.ChannelKind                   { return "x" }
func (c fnChannel) Capabilities() domain.ChannelCapabilities { return c.caps }
func (c fnChannel) Deliver(ctx context.Context, d domain.Delivery) (domain.DeliveryResult, error) {
	return c.deliver(ctx, d)
}

// conforming is the kit's own channel at a dedup level, which honours the contract
// by construction.
func conforming(dedup domain.DedupLevel) ChannelFactory {
	return func(*testing.T) ChannelUnderTest {
		p := NewProbe()
		return ChannelUnderTest{
			Channel: NewChannel("x", domain.ChannelCapabilities{NeedsAddress: true, NeedsSubject: true, Dedup: dedup}, p),
			Probe:   p,
		}
	}
}

// broken wraps the conforming channel at dedup: caps overrides what it declares
// when set, and mutate rewrites what it does.
func broken(dedup domain.DedupLevel, caps func(*domain.ChannelCapabilities),
	mutate func(inner fnChannel, ctx context.Context, d domain.Delivery) (domain.DeliveryResult, error)) ChannelFactory {
	return func(t *testing.T) ChannelUnderTest {
		t.Helper()
		u := conforming(dedup)(t)
		c := fnChannel{caps: u.Channel.Capabilities(), deliver: u.Channel.Deliver}
		inner := c
		if caps != nil {
			caps(&c.caps)
		}
		c.deliver = func(ctx context.Context, d domain.Delivery) (domain.DeliveryResult, error) {
			return mutate(inner, ctx, d)
		}
		u.Channel = c
		return u
	}
}

// afterTransient is a conforming channel that, once the provider has reported a
// transient failure, does one more thing to the provider side before reporting it.
func afterTransient(extra func(p *Probe, d domain.Delivery)) ChannelFactory {
	return func(t *testing.T) ChannelUnderTest {
		t.Helper()
		u := conforming(domain.DedupLocal)(t)
		in := u.Channel
		u.Channel = fnChannel{caps: in.Capabilities(), deliver: func(ctx context.Context, d domain.Delivery) (domain.DeliveryResult, error) {
			r, err := in.Deliver(ctx, d)
			if err == nil && r.Outcome == domain.Retry {
				extra(u.Probe, d)
			}
			return r, err
		}}
		return u
	}
}

func TestChannelContractFailsABrokenChannel(t *testing.T) {
	t.Parallel()
	pass := func(inner fnChannel, ctx context.Context, d domain.Delivery) (domain.DeliveryResult, error) {
		return inner.deliver(ctx, d)
	}
	cases := []struct {
		name    string
		channel ChannelFactory
		want    []string // the cases that must fail, and no others
	}{
		{
			name: "declares DedupProvider and sends twice",
			channel: broken(domain.DedupNone, func(c *domain.ChannelCapabilities) {
				c.Dedup, c.DedupWindow = domain.DedupProvider, 24*time.Hour
			}, pass),
			want: []string{caseRedrive},
		},
		{
			name:    "declares DedupLocal and sends twice",
			channel: broken(domain.DedupNone, func(c *domain.ChannelCapabilities) { c.Dedup = domain.DedupLocal }, pass),
			want:    []string{caseRedrive},
		},
		{
			name: "returns an error for a transient failure",
			channel: broken(domain.DedupLocal, nil, func(in fnChannel, ctx context.Context, d domain.Delivery) (domain.DeliveryResult, error) {
				r, err := in.deliver(ctx, d)
				if r.Outcome == domain.Retry {
					return domain.DeliveryResult{}, errors.New("provider unavailable")
				}
				return r, err
			}),
			want: []string{caseTransient},
		},
		{
			name: "reports a transient failure as delivered",
			channel: broken(domain.DedupLocal, nil, func(in fnChannel, ctx context.Context, d domain.Delivery) (domain.DeliveryResult, error) {
				r, err := in.deliver(ctx, d)
				if r.Outcome == domain.Retry {
					r.Outcome = domain.Delivered
				}
				return r, err
			}),
			want: []string{caseTransient},
		},
		{
			name: "retries a transient failure itself",
			channel: broken(domain.DedupLocal, nil, func(in fnChannel, ctx context.Context, d domain.Delivery) (domain.DeliveryResult, error) {
				r, err := in.deliver(ctx, d)
				if r.Outcome == domain.Retry {
					return in.deliver(ctx, d)
				}
				return r, err
			}),
			want: []string{caseTransient},
		},
		{
			name: "reports every send as delivered and sends nothing",
			channel: broken(domain.DedupLocal, nil, func(fnChannel, context.Context, domain.Delivery) (domain.DeliveryResult, error) {
				return domain.DeliveryResult{Outcome: domain.Delivered}, nil
			}),
			want: []string{caseRedrive, caseDistinct, caseTransient, caseUnclassified, caseDead, casePermanent},
		},
		{
			name: "collapses two notifications to one address",
			channel: func(t *testing.T) ChannelUnderTest {
				t.Helper()
				u := conforming(domain.DedupLocal)(t)
				in, seen := u.Channel, map[string]bool{}
				u.Channel = fnChannel{caps: in.Capabilities(), deliver: func(ctx context.Context, d domain.Delivery) (domain.DeliveryResult, error) {
					if seen[d.Address.Value] {
						return domain.DeliveryResult{Outcome: domain.Delivered}, nil
					}
					r, err := in.Deliver(ctx, d)
					seen[d.Address.Value] = seen[d.Address.Value] || (err == nil && r.Outcome == domain.Delivered)
					return r, err
				}}
				return u
			},
			want: []string{caseDistinct},
		},
		{
			name: "remembers a send that failed, so its retry is swallowed",
			channel: func(t *testing.T) ChannelUnderTest {
				t.Helper()
				u := conforming(domain.DedupLocal)(t)
				in, seen := u.Channel, map[string]bool{}
				u.Channel = fnChannel{caps: in.Capabilities(), deliver: func(ctx context.Context, d domain.Delivery) (domain.DeliveryResult, error) {
					if seen[d.IdemKey] {
						return domain.DeliveryResult{Outcome: domain.Delivered}, nil
					}
					seen[d.IdemKey] = true
					return in.Deliver(ctx, d)
				}}
				return u
			},
			want: []string{caseTransient},
		},
		{
			name: "reports a send it never made",
			channel: func(*testing.T) ChannelUnderTest {
				p := NewProbe()
				caps := domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupNone}
				return ChannelUnderTest{Probe: p, Channel: fnChannel{caps: caps, deliver: func(_ context.Context, d domain.Delivery) (domain.DeliveryResult, error) {
					// Honest about every failure it is armed with, and never sends.
					if f, failed := p.call(d.IdemKey); failed {
						switch f {
						case Transient:
							return domain.DeliveryResult{Outcome: domain.Retry}, nil
						case DeadAddress:
							return domain.DeliveryResult{Outcome: domain.Suppressed, SuppressReason: domain.SuppressHardBounce}, nil
						case Permanent:
							return domain.DeliveryResult{Outcome: domain.Rejected, Detail: "refused"}, nil
						default:
							return domain.DeliveryResult{}, ErrInfrastructure
						}
					}
					return domain.DeliveryResult{Outcome: domain.Delivered}, nil
				}}}
			},
			want: []string{caseRedrive, caseDistinct, caseTransient},
		},
		{
			name:    "calls the provider again after a transient failure and ignores the answer",
			channel: afterTransient(func(p *Probe, d domain.Delivery) { p.call(d.IdemKey) }),
			want:    []string{caseTransient},
		},
		{
			name:    "sends despite reporting a transient failure",
			channel: afterTransient(func(p *Probe, d domain.Delivery) { p.send(d.IdemKey, false) }),
			want:    []string{caseTransient},
		},
		{
			name: "reports a transient failure as a result and an error",
			channel: broken(domain.DedupLocal, nil, func(in fnChannel, ctx context.Context, d domain.Delivery) (domain.DeliveryResult, error) {
				r, err := in.deliver(ctx, d)
				if r.Outcome == domain.Retry {
					return r, errors.New("provider unavailable")
				}
				return r, err
			}),
			want: []string{caseTransient},
		},
		{
			name: "reports a dead address as a transient failure",
			channel: broken(domain.DedupLocal, nil, func(in fnChannel, ctx context.Context, d domain.Delivery) (domain.DeliveryResult, error) {
				r, err := in.deliver(ctx, d)
				if r.Outcome == domain.Suppressed {
					return domain.DeliveryResult{Outcome: domain.Retry}, nil
				}
				return r, err
			}),
			want: []string{caseDead},
		},
		{
			name: "suppresses without a reason",
			channel: broken(domain.DedupLocal, nil, func(in fnChannel, ctx context.Context, d domain.Delivery) (domain.DeliveryResult, error) {
				r, err := in.deliver(ctx, d)
				r.SuppressReason = ""
				return r, err
			}),
			want: []string{caseDead},
		},
		{
			name: "reports a permanent refusal as a transient failure",
			channel: broken(domain.DedupLocal, nil, func(in fnChannel, ctx context.Context, d domain.Delivery) (domain.DeliveryResult, error) {
				r, err := in.deliver(ctx, d)
				if r.Outcome == domain.Rejected {
					return domain.DeliveryResult{Outcome: domain.Retry, Detail: r.Detail}, nil
				}
				return r, err
			}),
			want: []string{casePermanent},
		},
		{
			name: "rejects without saying why",
			channel: broken(domain.DedupLocal, nil, func(in fnChannel, ctx context.Context, d domain.Delivery) (domain.DeliveryResult, error) {
				r, err := in.deliver(ctx, d)
				r.Detail = ""
				return r, err
			}),
			want: []string{casePermanent},
		},
		{
			name: "rejects a fault it cannot classify",
			channel: broken(domain.DedupLocal, nil, func(in fnChannel, ctx context.Context, d domain.Delivery) (domain.DeliveryResult, error) {
				r, err := in.deliver(ctx, d)
				if err != nil {
					return domain.DeliveryResult{Outcome: domain.Rejected, Detail: err.Error()}, nil
				}
				return r, err
			}),
			want: []string{caseUnclassified},
		},
		{
			name: "suppresses an address on an unclassified fault",
			channel: broken(domain.DedupLocal, nil, func(in fnChannel, ctx context.Context, d domain.Delivery) (domain.DeliveryResult, error) {
				r, err := in.deliver(ctx, d)
				if err != nil {
					return domain.DeliveryResult{Outcome: domain.Suppressed, SuppressReason: domain.SuppressHardBounce}, nil
				}
				return r, err
			}),
			want: []string{caseUnclassified},
		},
		{
			name:    "declares a window it has no provider to honour",
			channel: broken(domain.DedupLocal, func(c *domain.ChannelCapabilities) { c.DedupWindow = 24 * time.Hour }, pass),
			want:    []string{caseDeclares},
		},
		{
			name:    "declares DedupProvider with no window",
			channel: broken(domain.DedupProvider, func(c *domain.ChannelCapabilities) { c.DedupWindow = 0 }, pass),
			want:    []string{caseDeclares},
		},
		{
			name:    "declares a level that does not exist",
			channel: broken(domain.DedupLocal, func(c *domain.ChannelCapabilities) { c.Dedup = 7 }, pass),
			want:    []string{caseDeclares},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := slices.Sorted(maps.Keys(failures(t, tc.channel)))
			if want := slices.Sorted(slices.Values(tc.want)); !slices.Equal(got, want) {
				t.Fatalf("the contract failed %q, want exactly %q: a broken channel it does not reject is a hole in it", got, want)
			}
		})
	}
}

// A channel with no address has none to be dead, and a Suppressed from it would
// make the engine suppress the empty address hash for the whole realm.
func TestChannelContractHoldsAnAddresslessChannelToNeverSuppressing(t *testing.T) {
	t.Parallel()
	addressless := func(suppresses bool) ChannelFactory {
		return func(t *testing.T) ChannelUnderTest {
			t.Helper()
			u := conforming(domain.DedupLocal)(t)
			in := u.Channel
			caps := in.Capabilities()
			caps.NeedsAddress = false
			u.Channel = fnChannel{caps: caps, deliver: func(ctx context.Context, d domain.Delivery) (domain.DeliveryResult, error) {
				r, err := in.Deliver(ctx, d)
				if r.Outcome == domain.Suppressed && !suppresses {
					return domain.DeliveryResult{Outcome: domain.Retry, Detail: "the stream is down"}, nil
				}
				return r, err
			}}
			return u
		}
	}
	if got := failures(t, addressless(false)); len(got) != 0 {
		t.Fatalf("an addressless channel that retries when its provider is down failed %v", got)
	}
	got := failures(t, addressless(true))
	if _, ok := got[caseDead]; !ok || len(got) != 1 {
		t.Fatalf("an addressless channel that suppresses failed %v, want exactly %q", got, caseDead)
	}
}

// The suite must pass a channel that honours every level it can declare, or it is
// rejecting good channels.
func TestChannelContractPassesAConformingChannel(t *testing.T) {
	t.Parallel()
	for _, level := range []domain.DedupLevel{domain.DedupNone, domain.DedupLocal, domain.DedupProvider} {
		t.Run(fmt.Sprintf("dedup %d", level), func(t *testing.T) {
			t.Parallel()
			newChannel := conforming(level)
			if level == domain.DedupProvider {
				newChannel = func(t *testing.T) ChannelUnderTest {
					t.Helper()
					u := conforming(level)(t)
					u.Channel = fnChannel{
						caps:    domain.ChannelCapabilities{NeedsAddress: true, NeedsSubject: true, Dedup: level, DedupWindow: 24 * time.Hour},
						deliver: u.Channel.Deliver,
					}
					return u
				}
			}
			if got := failures(t, newChannel); len(got) != 0 {
				t.Fatalf("a conforming channel failed %v", got)
			}
			ChannelContract(t, newChannel)
		})
	}
}
