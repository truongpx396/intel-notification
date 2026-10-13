package notifytest

import (
	"context"
	"fmt"
	"testing"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// ChannelUnderTest is one channel with the provider side the suite watches it
// through. The probe stands where the provider would: it counts the sends that
// reached it and injects the failures a case arms.
type ChannelUnderTest struct {
	Channel ports.Channel
	Probe   *Probe

	// Key is the key the provider side counts a delivery's send under. A channel
	// that dedupes on Delivery.IdemKey leaves it nil; one whose effect is keyed by
	// something else (the in-app nudge is keyed by the notification) says what.
	Key func(domain.Delivery) string
	// Fail arms f for the next send. Nil arms it on Probe. A composite arms it on
	// every member, since one armed failure is consumed by the first.
	Fail func(Failure)
	// ProviderCalls is how many provider calls one failed delivery may make: 1 for
	// a channel, one per member for a composite. Zero means 1.
	ProviderCalls int
}

// ChannelFactory returns a fresh channel for t alone.
type ChannelFactory func(t *testing.T) ChannelUnderTest

// contractT is the part of *testing.T a case needs. Cases are written against it
// so the suite can be run against a channel it is meant to reject and the failure
// read back, instead of being believed.
type contractT interface {
	Helper()
	Context() context.Context
	Fatalf(format string, args ...any)
}

type channelCase struct {
	name string
	// dedupOnly marks a case that needs nothing injected, so a real provider's
	// sandbox, which cannot be made to fail on demand, can run it.
	dedupOnly bool
	run       func(t contractT, u ChannelUnderTest)
}

// The contract's cases by name, so the suite's own test can say which one a
// broken channel must fail.
const (
	caseDeclares     = "declares its dedup level, and with DedupProvider a window"
	caseRedrive      = "re-drive collapses at the declared dedup level"
	caseDistinct     = "distinct delivery keys are distinct sends"
	caseTransient    = "transient failure is Retry, never an error or a drop"
	caseUnclassified = "an unclassified fault is never a verdict on the delivery"
	caseDead         = "dead address is Suppressed with a reason"
	casePermanent    = "permanent refusal is Rejected, with a detail"
)

// contractIdentity is whom every contract delivery is for.
var contractIdentity = domain.Identity{
	Realm:     "aisat",
	Tenant:    domain.Tenant{Kind: "workspace", ID: "w1"},
	Recipient: domain.Recipient{Kind: "user", ID: "u1"},
}

// contractDelivery is the n-th delivery of the suite: its own key, notification
// and address, as the dispatcher binds them (D25).
func contractDelivery(u ChannelUnderTest, n int) domain.Delivery {
	kind := u.Channel.Kind()
	return domain.Delivery{
		ID:             fmt.Sprintf("d%d", n),
		IdemKey:        fmt.Sprintf("k%d", n),
		Identity:       contractIdentity,
		NotificationID: fmt.Sprintf("n%d", n),
		Topic:          "ingestion_complete",
		Channel:        kind,
		Address:        domain.Address{Channel: kind, Value: fmt.Sprintf("a%d@example.com", n)},
		Content:        domain.RenderedContent{Subject: "Done", Body: "Your upload finished."},
		Attempt:        1,
	}
}

func (u ChannelUnderTest) key(d domain.Delivery) string {
	if u.Key != nil {
		return u.Key(d)
	}
	return d.IdemKey
}

func (u ChannelUnderTest) fail(f Failure) {
	if u.Fail != nil {
		u.Fail(f)
		return
	}
	u.Probe.FailNext(f)
}

func (u ChannelUnderTest) providerCalls() int {
	if u.ProviderCalls > 0 {
		return u.ProviderCalls
	}
	return 1
}

// deliver sends d and fails the case on an error: the cases that expect a
// returned error (none do) would not call this.
func deliver(t contractT, u ChannelUnderTest, d domain.Delivery) domain.DeliveryResult {
	t.Helper()
	r, err := u.Channel.Deliver(t.Context(), d)
	if err != nil {
		t.Fatalf("Deliver returned the error %v, want a result: an error is an infrastructure fault, and a failure the provider reported is an outcome", err)
	}
	return r
}

// channelCases is the contract, in the order it reads.
func channelCases() []channelCase {
	return []channelCase{
		{name: caseDeclares, dedupOnly: true, run: func(t contractT, u ChannelUnderTest) {
			t.Helper()
			c := u.Channel.Capabilities()
			if u.Channel.Kind() == "" {
				t.Fatalf("Kind is empty: the registry would refuse the channel")
			}
			switch c.Dedup {
			case domain.DedupNone, domain.DedupLocal, domain.DedupProvider:
			default:
				t.Fatalf("Dedup = %d, want DedupNone, DedupLocal or DedupProvider: the engine exports the level an operator reasons from (D32)", int(c.Dedup))
			}
			if c.Dedup == domain.DedupProvider && c.DedupWindow <= 0 {
				t.Fatalf("DedupProvider with DedupWindow %v: a provider remembers a key for a time, and a replay past it sends again", c.DedupWindow)
			}
			if c.Dedup != domain.DedupProvider && c.DedupWindow != 0 {
				t.Fatalf("DedupWindow %v declared with Dedup %d: a window means something only for DedupProvider", c.DedupWindow, int(c.Dedup))
			}
		}},

		{name: caseRedrive, dedupOnly: true, run: func(t contractT, u ChannelUnderTest) {
			t.Helper()
			d := contractDelivery(u, 1)
			if r := deliver(t, u, d); r.Outcome != domain.Delivered {
				t.Fatalf("first delivery = %+v, want Delivered", r)
			}
			if got := u.Probe.Sends(u.key(d)); got != 1 {
				t.Fatalf("after one delivery the provider side saw %d sends, want 1: a channel that reports Delivered must have sent", got)
			}
			r := deliver(t, u, d)
			switch u.Channel.Capabilities().Dedup {
			case domain.DedupProvider, domain.DedupLocal:
				if r.Outcome != domain.Delivered || u.Probe.Sends(u.key(d)) != 1 {
					t.Fatalf("declared dedup %d, but a re-drive was %v and the provider side saw %d sends, want Delivered and 1",
						int(u.Channel.Capabilities().Dedup), r.Outcome, u.Probe.Sends(u.key(d)))
				}
			default:
				// At-least-once by declaration: a second send is permitted, and the
				// engine reports this channel's level. A re-drive must still succeed.
				if r.Outcome != domain.Delivered {
					t.Fatalf("re-drive = %+v, want Delivered", r)
				}
			}
		}},

		{name: caseDistinct, dedupOnly: true, run: func(t contractT, u ChannelUnderTest) {
			t.Helper()
			a, b := contractDelivery(u, 1), contractDelivery(u, 2)
			sameAddress := contractDelivery(u, 3)
			sameAddress.Address = a.Address
			for _, d := range []domain.Delivery{a, b, sameAddress} {
				if r := deliver(t, u, d); r.Outcome != domain.Delivered {
					t.Fatalf("delivery %s = %+v, want Delivered", d.IdemKey, r)
				}
			}
			for _, d := range []domain.Delivery{a, b, sameAddress} {
				if got := u.Probe.Sends(u.key(d)); got != 1 {
					t.Fatalf("delivery %s was sent %d times, want 1: keys that differ per address or per notification must not collapse (D25)", d.IdemKey, got)
				}
			}
		}},

		{name: caseTransient, run: func(t contractT, u ChannelUnderTest) {
			t.Helper()
			d := contractDelivery(u, 1)
			u.fail(Transient)
			r, err := u.Channel.Deliver(t.Context(), d)
			if err != nil || r.Outcome != domain.Retry {
				t.Fatalf("a transient failure gave %v, %v, want Retry and no error: the dispatcher owns backoff, and an error or a drop is not that", r.Outcome, err)
			}
			if got := u.Probe.Calls(u.key(d)); got > u.providerCalls() {
				t.Fatalf("the provider was called %d times for one failed delivery, want at most %d: a channel that retries inside defeats the jitter (D10)", got, u.providerCalls())
			}
			if got := u.Probe.Sends(u.key(d)); got != 0 {
				t.Fatalf("a failed delivery counted %d sends, want 0", got)
			}
			if r := deliver(t, u, d); r.Outcome != domain.Delivered || u.Probe.Sends(u.key(d)) != 1 {
				t.Fatalf("the next attempt = %v with %d sends, want Delivered and 1: a failure must leave nothing that blocks the retry", r.Outcome, u.Probe.Sends(u.key(d)))
			}
		}},

		{name: caseUnclassified, run: func(t contractT, u ChannelUnderTest) {
			t.Helper()
			u.fail(Infrastructure)
			r, err := u.Channel.Deliver(t.Context(), contractDelivery(u, 1))
			if err == nil && r.Outcome != domain.Retry {
				t.Fatalf("a fault the provider did not classify gave %v, want Retry or an error: only a provider's own verdict ends a delivery, and a guess dead-letters or suppresses a deliverable address", r.Outcome)
			}
		}},

		{name: caseDead, run: func(t contractT, u ChannelUnderTest) {
			t.Helper()
			u.fail(DeadAddress)
			r := deliver(t, u, contractDelivery(u, 1))
			if u.Channel.Capabilities().NeedsAddress {
				if r.Outcome != domain.Suppressed || !r.SuppressReason.Valid() {
					t.Fatalf("a dead address gave %v with reason %q, want Suppressed with one of the six reasons", r.Outcome, r.SuppressReason)
				}
				return
			}
			// There is no address to be dead, and the engine would write the empty
			// address hash as a suppression, silencing the channel for the realm (D13).
			if r.Outcome == domain.Suppressed {
				t.Fatalf("a channel with no address reported Suppressed: it would suppress the empty address hash for every recipient")
			}
		}},

		{name: casePermanent, run: func(t contractT, u ChannelUnderTest) {
			t.Helper()
			u.fail(Permanent)
			r := deliver(t, u, contractDelivery(u, 1))
			if r.Outcome != domain.Rejected {
				t.Fatalf("a permanent refusal gave %v, want Rejected: retrying it burns every attempt before the dead letter", r.Outcome)
			}
			if r.Detail == "" {
				t.Fatalf("Rejected with no Detail: the dead letter keeps the last error, and an operator has nothing to read (D12)")
			}
		}},
	}
}

// ChannelContract runs against ANY ports.Channel. No case is skippable: a channel
// declares its guarantees through Capabilities and the suite holds it to them
// (D32). Every case builds its own channel with newChannel.
func ChannelContract(t *testing.T, newChannel ChannelFactory) {
	t.Helper()
	runChannelCases(t, newChannel, false)
}

// ChannelDedupContract is the part of ChannelContract that injects no failure:
// what a channel claims about re-drives, held to what the provider side shows. It
// is what a real provider's sandbox is asked, under -tags provider, because a
// sandbox cannot be made to time out or bounce on demand but can be made to see
// the same idempotency key twice.
func ChannelDedupContract(t *testing.T, newChannel ChannelFactory) {
	t.Helper()
	runChannelCases(t, newChannel, true)
}

func runChannelCases(t *testing.T, newChannel ChannelFactory, dedupOnly bool) {
	t.Helper()
	for _, c := range channelCases() {
		if dedupOnly && !c.dedupOnly {
			continue
		}
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			c.run(t, newChannel(t))
		})
	}
}
