package notifytest

import (
	"context"
	"fmt"
	"maps"
	"sync"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

type addressKey struct {
	id      domain.Identity
	channel domain.ChannelKind
}

// AddressBook is an in-memory ports.AddressBook: zero, one or many addresses per
// (recipient, channel) (NR-028), scoped by the full identity as the real table is.
type AddressBook struct {
	mu sync.Mutex
	m  map[addressKey][]domain.Address
}

var _ ports.AddressBook = (*AddressBook)(nil)

// NewAddressBook returns an empty address book.
func NewAddressBook() *AddressBook {
	return &AddressBook{m: map[addressKey][]domain.Address{}}
}

// Put replaces id's addresses on channel ch with addrs, as Catalog.PutAddresses
// replaces one channel's set. An address with no Channel takes ch; one naming
// another channel is a test's mistake and panics.
func (b *AddressBook) Put(id domain.Identity, ch domain.ChannelKind, addrs ...domain.Address) {
	stored := make([]domain.Address, len(addrs))
	for i, a := range addrs {
		if a.Channel != "" && a.Channel != ch {
			panic(fmt.Sprintf("notifytest: an address for channel %q put on channel %q", a.Channel, ch))
		}
		a.Channel = ch
		stored[i] = cloneAddress(a)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.m[addressKey{id, ch}] = stored
}

// Resolve implements ports.AddressBook. It returns copies, in the order they were
// put, and no addresses, with no error, for an identity that has none.
func (b *AddressBook) Resolve(_ context.Context, id domain.Identity, ch domain.ChannelKind) ([]domain.Address, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	stored := b.m[addressKey{id, ch}]
	if len(stored) == 0 {
		return nil, nil
	}
	out := make([]domain.Address, len(stored))
	for i, a := range stored {
		out[i] = cloneAddress(a)
	}
	return out, nil
}

func cloneAddress(a domain.Address) domain.Address {
	a.Meta = maps.Clone(a.Meta)
	return a
}
