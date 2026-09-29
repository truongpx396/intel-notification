// Package domain holds the engine's pure types and rules: identities and their
// canonical encoding, the keys derived from them, the shard, backoff, and the
// policy that decides what a terminal outcome means. It imports nothing outside
// the standard library, so every rule here is testable without a database.
package domain

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Realm is WHICH PRODUCT — the outermost isolation axis (D1). It is never taken
// from a request: library mode reads it from configuration, service mode derives
// it from the authenticated producer (D31).
type Realm string

var realmPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)

// Validate reports whether r is a well-formed realm. The pattern keeps a realm
// usable verbatim as a bus subject token and a Redis key segment.
func (r Realm) Validate() error {
	if !realmPattern.MatchString(string(r)) {
		return fmt.Errorf("realm %q must match %s", r, realmPattern)
	}
	return nil
}

// Tenant is the host's isolation boundary: a workspace, organization, account.
// Opaque — the engine scopes by it and derives keys from it, and never parses it.
type Tenant struct {
	Kind string
	ID   string
}

// Recipient is the host's delivery subject: a user, device, Slack channel.
// Opaque. The same user in two tenants is two recipients-within-tenant.
type Recipient struct {
	Kind string
	ID   string
}

// Identity is the full scoping tuple. Every row, key and stream derives from it.
type Identity struct {
	Realm     Realm
	Tenant    Tenant
	Recipient Recipient
}

// ErrEmptyIdentity is returned for an identity with an empty component. The
// schema rejects one too: an empty component would equal an unset scope setting
// and defeat the row-level security predicate.
var ErrEmptyIdentity = errors.New("identity has an empty component")

// Validate reports whether every component of i is non-empty.
func (i Identity) Validate() error {
	if i.Realm == "" || i.Tenant.Kind == "" || i.Tenant.ID == "" ||
		i.Recipient.Kind == "" || i.Recipient.ID == "" {
		return ErrEmptyIdentity
	}
	return nil
}

// Canonical returns the unambiguous encoding of i: see [Canonical].
func (i Identity) Canonical() string {
	return Canonical(string(i.Realm), i.Tenant.Kind, i.Tenant.ID, i.Recipient.Kind, i.Recipient.ID)
}

// Canonical encodes parts unambiguously: each part is written as
// <octet length>:<part>, and parts are joined by ",". It is the only way the
// engine turns identities into strings (D18). Joining opaque ids with a bare
// separator collides as soon as an id contains it — ("a:b","c") and ("a","b:c")
// — and a collision in a dedup key drops a notification while one in a stream
// key leaks it.
//
//	Canonical("aisat", "workspace", "w1", "user", "u1") == "5:aisat,9:workspace,2:w1,4:user,2:u1"
func Canonical(parts ...string) string {
	var b strings.Builder
	for i, p := range parts {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.Itoa(len(p)))
		b.WriteByte(':')
		b.WriteString(p)
	}
	return b.String()
}
