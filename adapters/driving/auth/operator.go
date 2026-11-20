package auth

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"net/http"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// Operators authenticates an operator of /admin routes by a bearer token, and says which realm it
// administers: the realm of an administrative call is the token's and never a request's (NR-009). A token
// is for local stacks and the e2e suite; production prefers mTLS (docs/security.md).
type Operators struct {
	tokens []operatorToken
}

type operatorToken struct {
	digest [sha256.Size]byte
	realm  domain.Realm
}

// NewOperators returns Operators over the tokens, one realm each. An empty token is refused: it would
// authenticate a request that sent none. Two tokens that are the same value are refused too, because which
// realm they administer would then be a guess.
func NewOperators(tokens map[string]domain.Realm) (*Operators, error) {
	o := &Operators{}
	seen := map[[sha256.Size]byte]bool{}
	for token, realm := range tokens {
		if token == "" {
			return nil, errors.New("auth: an operator token is empty")
		}
		if err := realm.Validate(); err != nil {
			return nil, err
		}
		d := sha256.Sum256([]byte(token))
		if seen[d] {
			return nil, errors.New("auth: two operator tokens are the same value")
		}
		seen[d] = true
		o.tokens = append(o.tokens, operatorToken{digest: d, realm: realm})
	}
	return o, nil
}

// Authenticate is an httpapi.OperatorFunc. Every configured token is compared, and each comparison is
// constant-time over the digests, so how long the answer takes says nothing of how much of a token was
// right.
func (o *Operators) Authenticate(r *http.Request) (domain.Realm, error) {
	presented, ok := bearerToken(r.Header.Get("Authorization"))
	if !ok {
		return "", errBadToken
	}
	digest := sha256.Sum256([]byte(presented))
	var realm domain.Realm
	matched := 0
	for _, t := range o.tokens {
		match := subtle.ConstantTimeCompare(digest[:], t.digest[:])
		matched |= match
		if match == 1 {
			realm = t.realm
		}
	}
	if matched != 1 {
		return "", errBadToken
	}
	return realm, nil
}

// WithRealm returns ctx carrying realm as the realm of the call. The REST operator routes use it, as the
// gRPC interceptors do, so one Route picks the engine of the realm for both.
func WithRealm(ctx context.Context, realm domain.Realm) context.Context {
	return context.WithValue(ctx, realmKey{}, realm)
}

// RoutedNotifier is a ports.Notifier that sends every call to the Notifier of the realm the context
// carries: service mode's, where one process serves several realms and each has an engine of its own.
type RoutedNotifier struct {
	Route func(context.Context) (ports.Notifier, error)
}

var _ ports.Notifier = (*RoutedNotifier)(nil)

// Notify implements ports.Notifier.
func (n *RoutedNotifier) Notify(ctx context.Context, nt domain.Notification) (domain.Receipt, error) {
	e, err := n.Route(ctx)
	if err != nil {
		return domain.Receipt{}, err
	}
	return e.Notify(ctx, nt)
}

// NotifyTx implements ports.Notifier. A service cannot join a caller's transaction.
func (n *RoutedNotifier) NotifyTx(context.Context, ports.Tx, domain.Notification) (domain.Receipt, error) {
	return domain.Receipt{}, domain.ErrTxUnsupported
}

// Broadcast implements ports.Notifier.
func (n *RoutedNotifier) Broadcast(ctx context.Context, b domain.BroadcastRequest) (domain.BroadcastReceipt, error) {
	e, err := n.Route(ctx)
	if err != nil {
		return domain.BroadcastReceipt{}, err
	}
	return e.Broadcast(ctx, b)
}

// Cancel implements ports.Notifier.
func (n *RoutedNotifier) Cancel(ctx context.Context, c domain.CancelRequest) (domain.CancelReceipt, error) {
	e, err := n.Route(ctx)
	if err != nil {
		return domain.CancelReceipt{}, err
	}
	return e.Cancel(ctx, c)
}

// Status implements ports.Notifier.
func (n *RoutedNotifier) Status(ctx context.Context, s domain.StatusRequest) (domain.NotificationStatus, error) {
	e, err := n.Route(ctx)
	if err != nil {
		return domain.NotificationStatus{}, err
	}
	return e.Status(ctx, s)
}
