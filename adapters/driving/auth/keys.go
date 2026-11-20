package auth

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/truongpx396/intel-notification/domain"
)

// Keys holds each realm's JWKS: the keys that realm's host signs recipient and producer tokens with. It
// fetches a realm's set on first use, keeps it for a while, and fetches again when a token names a key id
// the set does not hold, once and bounded, because the host may have rotated its keys (T050).
type Keys struct{}

// KeysOption configures Keys.
type KeysOption func(*Keys)

// WithHTTPClient sets the client the JWKS are fetched with.
func WithHTTPClient(c *http.Client) KeysOption { return func(*Keys) {} }

// WithClock sets the clock the cache and the refetch bound are read against.
func WithClock(now func() time.Time) KeysOption { return func(*Keys) {} }

// WithTTL sets how long a fetched set is used before it is fetched again.
func WithTTL(d time.Duration) KeysOption { return func(*Keys) {} }

// WithMinRefetch sets the shortest interval between two fetches of one realm's set that an unknown key id
// may cause. It is what keeps a flood of tokens with invented key ids from becoming a flood of requests
// to the host.
func WithMinRefetch(d time.Duration) KeysOption { return func(*Keys) {} }

// AllowInsecureHTTP permits a JWKS URL that is plain HTTP, for development and the e2e stack.
func AllowInsecureHTTP() KeysOption { return func(*Keys) {} }

// NewKeys returns Keys over one JWKS URL per realm. A URL that cannot work is refused here, when the
// service starts: it must be absolute HTTPS (or HTTP with AllowInsecureHTTP), with a host.
func NewKeys(urls map[domain.Realm]string, opts ...KeysOption) (*Keys, error) {
	return &Keys{}, nil
}

// Realms lists the realms this holds keys for.
func (k *Keys) Realms() []domain.Realm { return nil }

// For returns realm's key set: the cached one while it is fresh, else a fetch. A fetch that fails keeps
// the last good set and uses it, so a host whose JWKS endpoint blinks does not sign everybody out; a realm
// that has never been fetched has no keys, which is an error and not an empty set.
func (k *Keys) For(ctx context.Context, realm domain.Realm) (domain.KeySet, error) {
	return domain.KeySet{}, errors.New("not implemented")
}

// Refetch fetches realm's set again because a token named a key id it did not hold. It does so at most once
// per minimum interval: within it, it reports refetched=false and the set it has.
func (k *Keys) Refetch(ctx context.Context, realm domain.Realm) (set domain.KeySet, refetched bool, err error) {
	return domain.KeySet{}, false, errors.New("not implemented")
}
