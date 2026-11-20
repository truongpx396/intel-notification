// Package auth establishes who is calling the service and what they may act as: the producer's realm
// from its authenticated principal (D31, NR-009), and, with the token verifier, the recipient's identity
// from a token the host signed (T050).
//
// Nothing here trusts a request. The realm of a call is the one its principal is bound to, so it cannot
// be named by a message field, a header or a query string; a principal the bindings do not know is
// refused; and what authenticates a principal (a client certificate the TLS layer verified, or a token
// whose signature was verified) is the only thing that can make one.
package auth

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/truongpx396/intel-notification/domain"
)

// ErrNoCredentials is what a PrincipalFunc returns for a call that carries none of what it looks for,
// so the next one may be tried. Any other error is a credential that was presented and is not good.
var ErrNoCredentials = errors.New("auth: the call carries no credentials")

// PrincipalFunc says who is calling, from the call alone.
type PrincipalFunc func(ctx context.Context) (principal string, err error)

// Option configures the interceptors.
type Option func(*options)

type options struct {
	onDenied func(principal string)
	onFault  func(error)
}

// WithDenied is told the principal of every call refused for having no binding, so an operator can see
// who tried: the caller is told only that it is not bound.
func WithDenied(f func(principal string)) Option { return func(o *options) { o.onDenied = f } }

// WithFault is given every error a PrincipalFunc returned for a credential that was presented and was
// not good, which the caller is shown only as "unauthenticated".
func WithFault(f func(error)) Option { return func(o *options) { o.onFault = f } }

// RealmInterceptors returns the unary and stream interceptors that derive each call's realm from its
// principal through bindings and put it in the call's context. A call with no credentials is
// Unauthenticated, one whose credentials are not good is Unauthenticated, and one whose principal has no
// binding is PermissionDenied: none reaches the handler.
func RealmInterceptors(b domain.RealmBindings, principal PrincipalFunc, opts ...Option) (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	o := options{onDenied: func(string) {}, onFault: func(error) {}}
	for _, opt := range opts {
		opt(&o)
	}
	derive := func(ctx context.Context) (context.Context, error) {
		who, err := principal(ctx)
		switch {
		case errors.Is(err, ErrNoCredentials):
			return nil, status.Error(codes.Unauthenticated, "authentication is required")
		case err != nil:
			// What the verifier said can hold what was presented, so the caller is told no more than that.
			o.onFault(err)
			return nil, status.Error(codes.Unauthenticated, "the credentials were not accepted")
		}
		realm, ok := b.RealmOf(who)
		if !ok {
			o.onDenied(who)
			return nil, status.Error(codes.PermissionDenied, "the caller is not bound to a realm")
		}
		return context.WithValue(ctx, realmKey{}, realm), nil
	}
	unary := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		ctx, err := derive(ctx)
		if err != nil {
			return nil, err
		}
		return h(ctx, req)
	}
	stream := func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, h grpc.StreamHandler) error {
		ctx, err := derive(ss.Context())
		if err != nil {
			return err
		}
		return h(srv, &realmStream{ServerStream: ss, ctx: ctx})
	}
	return unary, stream
}

type realmKey struct{}

// realmStream is a stream whose context carries the realm.
type realmStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *realmStream) Context() context.Context { return s.ctx }

// RealmFrom is the realm the interceptor derived for the call.
func RealmFrom(ctx context.Context) (domain.Realm, bool) {
	realm, ok := ctx.Value(realmKey{}).(domain.Realm)
	return realm, ok && realm != ""
}

// RealmOf is RealmFrom for a handler that must have one: a call with none went through no interceptor,
// which is a wiring fault and is reported as one, never answered as the caller's.
func RealmOf(ctx context.Context) (domain.Realm, error) {
	realm, ok := RealmFrom(ctx)
	if !ok {
		return "", errors.New("auth: the call carries no realm: it did not pass the realm interceptor")
	}
	return realm, nil
}

// Route picks what serves a call from the realm the interceptor derived for it: the engine of that
// realm, built once per bound realm at start. A realm with no engine is a wiring fault and is never
// answered by another realm's.
func Route[T any](byRealm map[domain.Realm]T) func(context.Context) (T, error) {
	return func(ctx context.Context) (T, error) {
		var zero T
		realm, err := RealmOf(ctx)
		if err != nil {
			return zero, err
		}
		engine, ok := byRealm[realm]
		if !ok {
			return zero, fmt.Errorf("auth: no engine serves realm %q", realm)
		}
		return engine, nil
	}
}

// MTLSPrincipal is the principal of a call made over mutual TLS: the first URI SAN of the client's
// certificate, else its first DNS SAN. It reads only a chain the TLS layer verified, so a server that
// does not require and verify client certificates never has a principal from here.
func MTLSPrincipal(ctx context.Context) (string, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return "", ErrNoCredentials
	}
	info, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || len(info.State.VerifiedChains) == 0 || len(info.State.VerifiedChains[0]) == 0 {
		return "", ErrNoCredentials
	}
	leaf := info.State.VerifiedChains[0][0]
	switch {
	case len(leaf.URIs) > 0:
		return leaf.URIs[0].String(), nil
	case len(leaf.DNSNames) > 0:
		return leaf.DNSNames[0], nil
	}
	return "", ErrNoCredentials
}

// FirstPrincipal tries each func in order and uses the first that finds credentials. A func that finds
// bad ones ends the search with its error: a presented credential that fails is never papered over by
// another.
func FirstPrincipal(funcs ...PrincipalFunc) PrincipalFunc {
	return func(ctx context.Context) (string, error) {
		for _, f := range funcs {
			who, err := f(ctx)
			if errors.Is(err, ErrNoCredentials) {
				continue
			}
			return who, err
		}
		return "", ErrNoCredentials
	}
}
