package domain

import (
	"fmt"
	"slices"
	"strings"
)

// RealmBindings says which realm each producer principal may act in (D31, NR-009). A principal is what
// the transport authenticated: a client certificate's SAN, or a token's subject. A call from a principal
// with no binding is refused, and a call never names its own realm, so a producer cannot write into
// another product's key space or read another product's status.
type RealmBindings struct{ byPrincipal map[string]Realm }

// ParseRealmBindings reads "principal=realm" pairs, comma-separated. It refuses a pair with no "=", an
// empty principal, a realm that is not a realm, and a principal bound twice, even to the same realm:
// that is a typo, and a silent last-one-wins would bind a producer to the wrong product.
func ParseRealmBindings(s string) (RealmBindings, error) {
	b := RealmBindings{byPrincipal: map[string]Realm{}}
	for _, pair := range splitPairs(s) {
		// A principal may be a URI that holds "=", and a realm never does, so the realm is what follows
		// the last one.
		at := strings.LastIndexByte(pair, '=')
		if at < 0 {
			return RealmBindings{}, fmt.Errorf("realm binding %q is not principal=realm", pair)
		}
		principal, realm := strings.TrimSpace(pair[:at]), Realm(strings.TrimSpace(pair[at+1:]))
		if principal == "" {
			return RealmBindings{}, fmt.Errorf("realm binding %q has no principal", pair)
		}
		if err := realm.Validate(); err != nil {
			return RealmBindings{}, fmt.Errorf("realm binding for principal %q: %w", principal, err)
		}
		if _, dup := b.byPrincipal[principal]; dup {
			return RealmBindings{}, fmt.Errorf("principal %q is bound twice", principal)
		}
		b.byPrincipal[principal] = realm
	}
	return b, nil
}

// splitPairs splits a comma-separated setting into its non-empty, trimmed entries.
func splitPairs(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

// RealmOf is the realm principal is bound to. A principal is matched whole: no prefix, no case folding.
func (b RealmBindings) RealmOf(principal string) (Realm, bool) {
	realm, ok := b.byPrincipal[principal]
	return realm, ok
}

// Realms lists every realm any principal is bound to, sorted, without repeats.
func (b RealmBindings) Realms() []Realm {
	var out []Realm
	for _, realm := range b.byPrincipal {
		if !slices.Contains(out, realm) {
			out = append(out, realm)
		}
	}
	slices.Sort(out)
	return out
}

// ParseRealmPairs reads "realm=value" pairs, comma-separated, as the settings that carry one value per
// realm are written (a JWKS URL, a Directory address). The value may itself hold "=" and is everything
// after the first one. A realm that is not a realm, a realm named twice and a pair with no value are
// refused.
func ParseRealmPairs(s string) (map[Realm]string, error) {
	out := map[Realm]string{}
	for _, pair := range splitPairs(s) {
		name, value, ok := strings.Cut(pair, "=")
		realm := Realm(strings.TrimSpace(name))
		if !ok {
			return nil, fmt.Errorf("setting %q is not realm=value", pair)
		}
		if err := realm.Validate(); err != nil {
			return nil, err
		}
		if value = strings.TrimSpace(value); value == "" {
			return nil, fmt.Errorf("realm %q has no value", realm)
		}
		if _, dup := out[realm]; dup {
			return nil, fmt.Errorf("realm %q is named twice", realm)
		}
		out[realm] = value
	}
	return out, nil
}
