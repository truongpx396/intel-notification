package auth

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"sync"
	"time"

	"github.com/truongpx396/intel-notification/domain"
)

// maxJWKSBytes bounds what is read from a host's JWKS endpoint. A real key set is a few kilobytes.
const maxJWKSBytes = 1 << 20

// Keys holds each realm's JWKS: the keys that realm's host signs recipient and producer tokens with. It
// fetches a realm's set on first use, keeps it for a while, and fetches again when a token names a key id
// the set does not hold, once and bounded, because the host may have rotated its keys (T050).
type Keys struct {
	client     *http.Client
	now        func() time.Time
	ttl        time.Duration
	minRefetch time.Duration
	allowHTTP  bool
	realms     map[domain.Realm]*realmKeys
}

// realmKeys is one realm's cached set. Its mutex is held across a fetch, so simultaneous first uses make
// one request and not one each.
type realmKeys struct {
	// gen counts the sets this has held: a caller that verified against one generation can tell a later one is there.
	gen       uint64
	url       string
	mu        sync.Mutex
	set       domain.KeySet
	have      bool
	lastTry   time.Time // the last fetch attempted, whether or not it succeeded
	lastError error
}

// KeysOption configures Keys.
type KeysOption func(*Keys)

// WithHTTPClient sets the client the JWKS are fetched with. A redirect is never followed by the default
// client, and a replacement should not either: an endpoint that moves is the operator's to repoint.
func WithHTTPClient(c *http.Client) KeysOption { return func(k *Keys) { k.client = c } }

// WithClock sets the clock the cache and the refetch bound are read against.
func WithClock(now func() time.Time) KeysOption { return func(k *Keys) { k.now = now } }

// WithTTL sets how long a fetched set is used before it is fetched again. The default is ten minutes.
func WithTTL(d time.Duration) KeysOption { return func(k *Keys) { k.ttl = d } }

// WithMinRefetch sets the shortest interval between two fetches of one realm's set, however they come
// about: the ttl, an unknown key id, or a retry after a failure. It is what keeps a flood of tokens with
// invented key ids from becoming a flood of requests to the host, and an outage from becoming one. The
// default is one minute.
func WithMinRefetch(d time.Duration) KeysOption { return func(k *Keys) { k.minRefetch = d } }

// AllowInsecureHTTP permits a JWKS URL that is plain HTTP, for development and the e2e stack. Keys read in
// the clear can be replaced by anyone on the path, which would let them mint tokens for any recipient.
func AllowInsecureHTTP() KeysOption { return func(k *Keys) { k.allowHTTP = true } }

// NewKeys returns Keys over one JWKS URL per realm. A URL that cannot work is refused here, when the
// service starts: it must be absolute HTTPS (or HTTP with AllowInsecureHTTP), with a host and no
// credentials.
func NewKeys(urls map[domain.Realm]string, opts ...KeysOption) (*Keys, error) {
	k := &Keys{now: time.Now, ttl: 10 * time.Minute, minRefetch: time.Minute, realms: map[domain.Realm]*realmKeys{}}
	for _, opt := range opts {
		opt(k)
	}
	if k.client == nil {
		k.client = &http.Client{
			Timeout:       5 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}
	}
	for realm, raw := range urls {
		if err := realm.Validate(); err != nil {
			return nil, err
		}
		u, err := url.Parse(raw)
		switch {
		case err != nil:
			return nil, fmt.Errorf("the JWKS URL of realm %s is not a URL: %w", realm, err)
		case u.Scheme == "https", u.Scheme == "http" && k.allowHTTP:
		case u.Scheme == "http":
			return nil, fmt.Errorf("the JWKS URL of realm %s is plain HTTP: keys read in the clear can be replaced by anyone on the path", realm)
		default:
			return nil, fmt.Errorf("the JWKS URL of realm %s is not an absolute HTTPS URL", realm)
		}
		if u.Hostname() == "" || u.User != nil {
			return nil, fmt.Errorf("the JWKS URL of realm %s has no host, or carries credentials", realm)
		}
		k.realms[realm] = &realmKeys{url: raw}
	}
	return k, nil
}

// Realms lists the realms this holds keys for, sorted.
func (k *Keys) Realms() []domain.Realm {
	out := make([]domain.Realm, 0, len(k.realms))
	for realm := range k.realms {
		out = append(out, realm)
	}
	slices.Sort(out)
	return out
}

// Has reports whether realm is one this holds keys for.
func (k *Keys) Has(realm domain.Realm) bool { _, ok := k.realms[realm]; return ok }

// fetch reads and parses realm's JWKS. A set with no usable key is a failure, and says so: it would
// otherwise verify no one, silently.
func (k *Keys) fetch(ctx context.Context, rk *realmKeys) (domain.KeySet, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rk.url, nil)
	if err != nil {
		return domain.KeySet{}, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := k.client.Do(req)
	if err != nil {
		return domain.KeySet{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return domain.KeySet{}, fmt.Errorf("the endpoint answered %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxJWKSBytes+1))
	if err != nil {
		return domain.KeySet{}, err
	}
	if len(body) > maxJWKSBytes {
		return domain.KeySet{}, fmt.Errorf("the key set is over %d bytes", maxJWKSBytes)
	}
	set, err := domain.ParseJWKS(body)
	if err != nil {
		return domain.KeySet{}, err
	}
	if set.Len() == 0 {
		return domain.KeySet{}, errors.New("the key set holds no key a token can be verified with")
	}
	return set, nil
}

// refresh fetches, records the attempt, and keeps the last good set on a failure. The caller holds rk.mu.
func (k *Keys) refresh(ctx context.Context, rk *realmKeys) {
	rk.lastTry = k.now()
	set, err := k.fetch(ctx, rk)
	if err != nil {
		rk.lastError = err
		return
	}
	rk.set, rk.have, rk.lastError = set, true, nil
	rk.gen++
}

// For returns realm's key set: the cached one while it is fresh, else a fetch. A fetch that fails keeps
// the last good set and uses it, so a host whose JWKS endpoint blinks does not sign everybody out, and is
// not tried again until the refetch interval has passed. A realm that has never been fetched has no keys,
// which is an error and not an empty set.
func (k *Keys) For(ctx context.Context, realm domain.Realm) (domain.KeySet, error) {
	set, _, err := k.forGen(ctx, realm)
	return set, err
}

// forGen is For with the generation of the set it returns.
func (k *Keys) forGen(ctx context.Context, realm domain.Realm) (domain.KeySet, uint64, error) {
	rk, ok := k.realms[realm]
	if !ok {
		return domain.KeySet{}, 0, fmt.Errorf("no JWKS is configured for realm %q", realm)
	}
	rk.mu.Lock()
	defer rk.mu.Unlock()
	now := k.now()
	fresh := rk.have && now.Sub(rk.lastTry) < k.ttl && rk.lastError == nil
	waiting := !rk.lastTry.IsZero() && now.Sub(rk.lastTry) < k.minRefetch
	if !fresh && !waiting {
		k.refresh(ctx, rk)
	}
	if rk.have {
		return rk.set, rk.gen, nil
	}
	if rk.lastError != nil {
		return domain.KeySet{}, 0, fmt.Errorf("the keys of realm %q could not be fetched: %w", realm, rk.lastError)
	}
	return domain.KeySet{}, 0, fmt.Errorf("the keys of realm %q are not available", realm)
}

// Refetch fetches realm's set again because a token named a key id it did not hold. It does so at most
// once per refetch interval, counted from the last fetch of any kind: within it, it reports
// refetched=false and the set it has.
func (k *Keys) Refetch(ctx context.Context, realm domain.Realm) (set domain.KeySet, refetched bool, err error) {
	set, _, refetched, err = k.refetchGen(ctx, realm)
	return set, refetched, err
}

// refetchGen is Refetch with the generation of the set it returns.
func (k *Keys) refetchGen(ctx context.Context, realm domain.Realm) (set domain.KeySet, gen uint64, refetched bool, err error) {
	rk, ok := k.realms[realm]
	if !ok {
		return domain.KeySet{}, 0, false, fmt.Errorf("no JWKS is configured for realm %q", realm)
	}
	rk.mu.Lock()
	defer rk.mu.Unlock()
	if !rk.lastTry.IsZero() && k.now().Sub(rk.lastTry) < k.minRefetch {
		return rk.set, rk.gen, false, nil
	}
	k.refresh(ctx, rk)
	if rk.lastError != nil {
		return rk.set, rk.gen, true, fmt.Errorf("the keys of realm %q could not be fetched: %w", realm, rk.lastError)
	}
	return rk.set, rk.gen, true, nil
}
