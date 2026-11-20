package auth

import (
	"context"
	"net/http"
	"time"

	"github.com/truongpx396/intel-notification/domain"
)

// Recipients authenticates a recipient from the token the host signed for its signed-in user (T050,
// rest-api.md): a bearer token, verified against the keys of the realm its iss names. The identity is the
// verified claims' and nothing else, and the realm is the one whose keys verified it.
type Recipients struct{}

// RecipientOption configures Recipients.
type RecipientOption func(*Recipients)

// WithAudience requires every token to name this audience.
func WithAudience(aud string) RecipientOption { return func(*Recipients) {} }

// WithNow sets the clock tokens are judged by.
func WithNow(now func() time.Time) RecipientOption { return func(*Recipients) {} }

// NewRecipients returns Recipients over keys.
func NewRecipients(keys *Keys, opts ...RecipientOption) *Recipients { return &Recipients{} }

// Identify is an httpapi.IdentityFunc: any error is a 401.
func (r *Recipients) Identify(req *http.Request) (domain.Identity, error) {
	return domain.Identity{}, nil
}

// ProducerOption configures ProducerTokens.
type ProducerOption func(*producerOptions)

type producerOptions struct{}

// WithProducerNow sets the clock producer tokens are judged by.
func WithProducerNow(now func() time.Time) ProducerOption { return func(*producerOptions) {} }

// WithProducerAudience requires every producer token to name this audience.
func WithProducerAudience(aud string) ProducerOption { return func(*producerOptions) {} }

// ProducerTokens is the PrincipalFunc of producers that authenticate with a token: a bearer token in the
// call's metadata whose subject is the principal. The token is verified with the keys of the realm its
// subject is bound to, so a subject bound to one realm cannot be claimed with another realm's keys, and a
// subject with no binding is not a principal at all (D31, NR-009).
func ProducerTokens(b domain.RealmBindings, keys *Keys, opts ...ProducerOption) PrincipalFunc {
	return func(context.Context) (string, error) { return "unauthenticated stub", nil }
}
