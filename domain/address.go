package domain

import (
	"fmt"
	"maps"
	"slices"
	"strings"
)

// Bounds on what a producer may store for one recipient and channel. They are generous for a person
// (every device they own) and bound what an authenticated producer's mistake can write.
const (
	MaxAddressesPerChannel = 50
	MaxAddressValueBytes   = 2048
	MaxAddressMetaEntries  = 20
	MaxAddressMetaBytes    = 512
)

// PrepareAddresses checks a producer's set of addresses for one channel and returns what to store: the
// addresses with the channel set, and each one's AddressKey, computed over normalize(value), the
// channel's definition of address equality (nil: the value as it is). Two addresses with one key are one
// address, and the first is kept, so a set that names a mailbox in two cases stores it once. It returns a
// *ValidationError naming every problem found, and stores nothing, when the set is not one the engine
// can use: too many addresses, an empty or oversized value, a locale that is not a language tag, a time
// zone the database does not know, or meta over its bounds (NR-028, D31).
func PrepareAddresses(ch ChannelKind, addrs []Address, normalize func(string) string) ([]Address, []string, error) {
	var v ValidationError
	if len(addrs) > MaxAddressesPerChannel {
		v.add("Addresses", fmt.Sprintf("%d addresses on one channel, the limit is %d", len(addrs), MaxAddressesPerChannel), nil)
	}
	if normalize == nil {
		normalize = func(s string) string { return s }
	}
	var (
		kept []Address
		keys []string
		seen = map[string]bool{}
	)
	for i, a := range addrs {
		field := func(name string) string { return fmt.Sprintf("Addresses[%d].%s", i, name) }
		switch {
		case strings.TrimSpace(a.Value) == "":
			v.add(field("Value"), "is required", nil)
		case len(a.Value) > MaxAddressValueBytes:
			v.add(field("Value"), fmt.Sprintf("is %d bytes, the limit is %d", len(a.Value), MaxAddressValueBytes), nil)
		}
		if a.Channel != "" && a.Channel != ch {
			v.add(field("Channel"), fmt.Sprintf("%q is not the channel the set is for, %q", a.Channel, ch), nil)
		}
		if a.Locale != "" && NormalizeLocale(a.Locale) == "" {
			v.add(field("Locale"), fmt.Sprintf("%q is not a language tag such as en or fr-CA", a.Locale), nil)
		}
		if a.Timezone != "" {
			if _, err := (DeliverySchedule{Timezone: a.Timezone}).zone(); err != nil {
				v.add(field("Timezone"), fmt.Sprintf("%q is not an IANA time zone name", a.Timezone), nil)
			}
		}
		if problem := metaProblem(a.Meta); problem != "" {
			v.add(field("Meta"), problem, nil)
		}
		if len(v.Problems) > 0 {
			continue // report every problem, store nothing
		}
		key := AddressKey(ch, normalize(a.Value))
		if seen[key] {
			continue
		}
		seen[key] = true
		stored := a
		stored.Channel = ch
		stored.Meta = maps.Clone(a.Meta)
		kept = append(kept, stored)
		keys = append(keys, key)
	}
	if err := v.err(); err != nil {
		return nil, nil, err
	}
	return kept, keys, nil
}

// metaProblem describes what is wrong with an address's meta, or returns "".
func metaProblem(meta map[string]string) string {
	if len(meta) > MaxAddressMetaEntries {
		return fmt.Sprintf("%d entries, the limit is %d", len(meta), MaxAddressMetaEntries)
	}
	for _, k := range slices.Sorted(maps.Keys(meta)) {
		if len(k) > MaxAddressMetaBytes || len(meta[k]) > MaxAddressMetaBytes {
			return fmt.Sprintf("the entry %q is over %d bytes", k[:min(len(k), 32)], MaxAddressMetaBytes)
		}
	}
	return ""
}
