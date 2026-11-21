package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"google.golang.org/grpc/metadata"

	"github.com/truongpx396/intel-notification/domain"
)

// bearerToken is the token of an "Authorization: Bearer <token>" header. The scheme is matched without
// regard to case (RFC 9110), and a header that is anything else is not a bearer token.
func bearerToken(header string) (string, bool) {
	scheme, token, ok := strings.Cut(strings.TrimSpace(header), " ")
	token = strings.TrimSpace(token)
	if !ok || !strings.EqualFold(scheme, "Bearer") || token == "" || strings.ContainsAny(token, " \t") {
		return "", false
	}
	return token, true
}

// errBadToken is what a refusal says: that it was refused, and never what was presented or why in a
// way that would help forge another.
var errBadToken = fmt.Errorf("%w: not accepted", domain.ErrInvalidToken)

// keySource is what verifyWithRefetch needs of the key cache: a realm's set and its generation, and a bounded
// fetch of it again. *Keys is one; a test is another.
type keySource interface {
	forGen(ctx context.Context, realm domain.Realm) (domain.KeySet, uint64, error)
	refetchGen(ctx context.Context, realm domain.Realm) (set domain.KeySet, gen uint64, refetched bool, err error)
}

// verifyWithRefetch runs verify against realm's keys and, when the token names a key id the set does not hold
// or does not verify under the one it names, fetches the set once more (bounded by Keys) and runs it again:
// the host may have added a key, or replaced one under the same id.
func verifyWithRefetch[T any](ctx context.Context, keys keySource, realm domain.Realm, verify func(domain.KeySet) (T, error)) (T, error) {
	var zero T
	set, gen, err := keys.forGen(ctx, realm)
	if err != nil {
		return zero, err
	}
	got, err := verify(set)
	if !errors.Is(err, domain.ErrUnknownKeyID) && !errors.Is(err, domain.ErrBadSignature) {
		return got, err
	}
	set, now, refetched, ferr := keys.refetchGen(ctx, realm)
	if ferr != nil {
		return zero, err
	}
	// Only one request may fetch in an interval, and when several arrive together after a rotation the
	// others are throttled. A throttled one that read the set before the fetch still has a try left: the set
	// is newer than the one it failed with. One that finds the same set has nothing new to try, so a forged
	// token costs one verification and no fetch.
	if !refetched && now == gen {
		return zero, err
	}
	return verify(set)
}

// Recipients authenticates a recipient from the token the host signed for its signed-in user (T050,
// rest-api.md): a bearer token, verified against the keys of the realm its iss names. The identity is the
// verified claims' and nothing else, and the realm is the one whose keys verified it.
type Recipients struct {
	keys     *Keys
	audience string
	now      func() time.Time
	onError  func(error)
}

// RecipientOption configures Recipients.
type RecipientOption func(*Recipients)

// WithAudience requires every token to name this audience.
func WithAudience(aud string) RecipientOption { return func(r *Recipients) { r.audience = aud } }

// WithNow sets the clock tokens are judged by.
func WithNow(now func() time.Time) RecipientOption { return func(r *Recipients) { r.now = now } }

// WithRefusalHandler is given the reason each token was refused, for an operator's log: the caller is
// answered with an empty 401 and told nothing.
func WithRefusalHandler(f func(error)) RecipientOption { return func(r *Recipients) { r.onError = f } }

// NewRecipients returns Recipients over keys.
func NewRecipients(keys *Keys, opts ...RecipientOption) *Recipients {
	r := &Recipients{keys: keys, now: time.Now, onError: func(error) {}}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Identify is an httpapi.IdentityFunc: it returns the identity a request's bearer token opens, or an
// error, which is a 401. Only the Authorization header is read, so a token in a query string, which is
// logged, is not one (the stream route moves its own into the header first).
func (r *Recipients) Identify(req *http.Request) (domain.Identity, error) {
	token, ok := bearerToken(req.Header.Get("Authorization"))
	if !ok {
		return domain.Identity{}, errBadToken
	}
	iss, ok := domain.PeekIssuer(token)
	realm := domain.Realm(iss)
	if !ok || !r.keys.Has(realm) {
		return domain.Identity{}, errBadToken
	}
	claims, err := verifyWithRefetch(req.Context(), r.keys, realm, func(ks domain.KeySet) (domain.RecipientClaims, error) {
		return domain.VerifyRecipientToken(token, ks, domain.TokenPolicy{Issuer: realm, Audience: r.audience, Now: r.now()})
	})
	if err != nil {
		r.onError(fmt.Errorf("recipient token for realm %q refused: %w", realm, err))
		return domain.Identity{}, errBadToken
	}
	return domain.Identity{Realm: realm, Tenant: claims.Tenant, Recipient: claims.Recipient}, nil
}

// ProducerOption configures ProducerTokens.
type ProducerOption func(*producerOptions)

type producerOptions struct {
	audience string
	now      func() time.Time
}

// WithProducerNow sets the clock producer tokens are judged by.
func WithProducerNow(now func() time.Time) ProducerOption {
	return func(o *producerOptions) { o.now = now }
}

// WithProducerAudience requires every producer token to name this audience.
func WithProducerAudience(aud string) ProducerOption {
	return func(o *producerOptions) { o.audience = aud }
}

// ProducerTokens is the PrincipalFunc of producers that authenticate with a token: a bearer token in the
// call's metadata whose subject is the principal. The token is verified with the keys of the realm its
// subject is bound to, so a subject bound to one realm cannot be claimed with another realm's keys, and a
// subject with no binding is not a principal at all (D31, NR-009). A call with no bearer token finds no
// credentials, so mTLS or another way may be tried; one with a token that is not good is an error.
func ProducerTokens(b domain.RealmBindings, keys *Keys, opts ...ProducerOption) PrincipalFunc {
	o := producerOptions{now: time.Now}
	for _, opt := range opts {
		opt(&o)
	}
	return func(ctx context.Context) (string, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		var token string
		for _, h := range md.Get("authorization") {
			if t, ok := bearerToken(h); ok {
				token = t
				break
			}
		}
		if token == "" {
			return "", ErrNoCredentials
		}
		subject, ok := domain.PeekSubject(token)
		if !ok {
			return "", errBadToken
		}
		realm, bound := b.RealmOf(subject)
		if !bound || !keys.Has(realm) {
			return "", errBadToken
		}
		claims, err := verifyWithRefetch(ctx, keys, realm, func(ks domain.KeySet) (domain.ProducerClaims, error) {
			return domain.VerifyProducerToken(token, ks, domain.TokenPolicy{Issuer: realm, Audience: o.audience, Now: o.now()})
		})
		if err != nil {
			return "", fmt.Errorf("producer token for %q refused: %w", subject, err)
		}
		return claims.Subject, nil
	}
}
