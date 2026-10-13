package notifytest

import "github.com/truongpx396/intel-notification/ports"

// Normalizing is a channel that defines address equality, as one does by implementing
// ports.AddressNormalizer (a lower-cased email domain, an E.164 number, a URL's host).
// It wraps any channel, so a case can give the channel it uses a normalizer without
// writing another channel: the dispatcher asks for the normalizer by interface, so a
// channel that has none is not asked.
type Normalizing struct {
	ports.Channel
	normalize func(string) string
}

var (
	_ ports.Channel           = (*Normalizing)(nil)
	_ ports.AddressNormalizer = (*Normalizing)(nil)
)

// NewNormalizing returns ch with normalize as its definition of address equality.
func NewNormalizing(ch ports.Channel, normalize func(string) string) *Normalizing {
	return &Normalizing{Channel: ch, normalize: normalize}
}

// NormalizeAddress implements ports.AddressNormalizer.
func (n *Normalizing) NormalizeAddress(value string) string { return n.normalize(value) }
