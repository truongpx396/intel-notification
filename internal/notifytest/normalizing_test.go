package notifytest

import (
	"strings"
	"testing"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// The wrapper is the channel it wraps with a normalizer added: the dispatcher finds
// the normalizer by interface, and everything else is the wrapped channel's.
func TestNormalizingAddsANormalizerAndKeepsTheChannel(t *testing.T) {
	t.Parallel()
	inner := NewScripted("email", domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupLocal})
	var ch ports.Channel = NewNormalizing(inner, strings.ToUpper)

	n, ok := ch.(ports.AddressNormalizer)
	if !ok {
		t.Fatal("the wrapper is not a ports.AddressNormalizer")
	}
	if got := n.NormalizeAddress("a@b.c"); got != "A@B.C" {
		t.Fatalf("NormalizeAddress = %q, want the function's answer", got)
	}
	if ch.Kind() != "email" || ch.Capabilities().Dedup != domain.DedupLocal {
		t.Fatalf("Kind %q, Capabilities %+v: not the wrapped channel's", ch.Kind(), ch.Capabilities())
	}
	if res, err := ch.Deliver(t.Context(), domain.Delivery{IdemKey: "k"}); err != nil || res.Outcome != domain.Delivered {
		t.Fatalf("Deliver = %+v, %v", res, err)
	}
	if len(inner.Deliveries()) != 1 {
		t.Fatal("the wrapped channel was not the one asked")
	}
	if _, plain := ports.Channel(inner).(ports.AddressNormalizer); plain {
		t.Fatal("the plain channel is a normalizer: the wrapper adds nothing")
	}
}
