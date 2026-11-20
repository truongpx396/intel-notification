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

var (
	_ ports.AddressBook   = (*AddressBook)(nil)
	_ ports.AddressWriter = (*AddressBook)(nil)
)

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

// putChecked replaces id's addresses on ch with addrs, as the table does. It refuses what the
// table's constraints refuse: an identity with an empty component, keys that do not pair with the
// addresses, two addresses with one key, and an address of another channel. Nothing is stored when it
// refuses.
func (b *AddressBook) putChecked(id domain.Identity, ch domain.ChannelKind, addrs []domain.Address, keys []string) error {
	if err := id.Validate(); err != nil {
		return err
	}
	if ch == "" {
		return fmt.Errorf("notifytest: an address set with no channel")
	}
	if len(keys) != len(addrs) {
		return fmt.Errorf("notifytest: %d addresses with %d keys", len(addrs), len(keys))
	}
	seen := map[string]bool{}
	for i, a := range addrs {
		if a.Channel != "" && a.Channel != ch {
			return fmt.Errorf("notifytest: an address for channel %q put on channel %q", a.Channel, ch)
		}
		if a.Value == "" {
			return fmt.Errorf("notifytest: address %d has no value", i)
		}
		if seen[keys[i]] {
			return fmt.Errorf("notifytest: addresses %d and an earlier one have the same key", i)
		}
		seen[keys[i]] = true
	}
	stored := make([]domain.Address, len(addrs))
	for i, a := range addrs {
		a.Channel = ch
		stored[i] = cloneAddress(a)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(stored) == 0 {
		delete(b.m, addressKey{id, ch})
		return nil
	}
	b.m[addressKey{id, ch}] = stored
	return nil
}

// PutAddresses implements ports.AddressWriter.
func (b *AddressBook) PutAddresses(_ context.Context, id domain.Identity, ch domain.ChannelKind, addrs []domain.Address, keys []string) error {
	return b.putChecked(id, ch, addrs, keys)
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
