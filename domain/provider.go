package domain

import (
	"fmt"
	"regexp"
	"strings"
)

// ChannelProvider is one row of channel_providers: which provider backs a channel for a realm, or for one
// tenant of it, and where its credential is (D29, D31). The credential itself is never here, only a
// reference to it.
type ChannelProvider struct {
	Realm Realm
	// Tenant is the zero value for a realm-wide provider, and a tenant for the provider that tenant
	// brings: it replaces the realm's for that tenant.
	Tenant   Tenant
	Channel  ChannelKind
	Provider string // resend, smtp, webhook, ...
	// Priority orders the providers of one channel in one scope: the lowest is tried first, and the next
	// is tried when it fails transiently (provider failover, D29).
	Priority int
	// Config is the provider's non-secret settings: a region, a from-address, a relay's address.
	Config map[string]any
	// SecretRef is a reference to the credential, such as env:NAME or file:/run/secrets/key. Empty for a
	// provider that needs none.
	SecretRef string
}

// secretRefPattern is the schema's own check on channel_providers.secret_ref: a scheme, a colon and
// something after it, which catches a key pasted where a reference belongs.
var secretRefPattern = regexp.MustCompile(`^[a-z][a-z0-9+.-]*:.+`)

// ParseSecretRef splits a secret reference into its scheme and what follows it. A value with no scheme
// is refused with an error that does not repeat it, because the likeliest mistake is the credential
// itself pasted where its reference belongs.
func ParseSecretRef(ref string) (scheme, rest string, err error) {
	if !secretRefPattern.MatchString(ref) {
		return "", "", fmt.Errorf("a secret reference is scheme:locator, such as env:NAME or file:/run/secrets/key: this value (%d characters) is not one", len(ref))
	}
	scheme, rest, _ = strings.Cut(ref, ":")
	return scheme, rest, nil
}
