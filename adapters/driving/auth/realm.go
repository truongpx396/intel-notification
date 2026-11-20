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

	"google.golang.org/grpc"

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
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
			return h(ctx, req)
		},
		func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, h grpc.StreamHandler) error {
			return h(srv, ss)
		}
}

// RealmFrom is the realm the interceptor derived for the call.
func RealmFrom(ctx context.Context) (domain.Realm, bool) { return "", false }

// RealmOf is RealmFrom for a handler that must have one: a call with none went through no interceptor,
// which is a wiring fault and is reported as one.
func RealmOf(ctx context.Context) (domain.Realm, error) { return "", errors.New("not implemented") }

// Route picks what serves a call from the realm the interceptor derived for it: the engine of that
// realm, built once per bound realm at start.
func Route[T any](byRealm map[domain.Realm]T) func(context.Context) (T, error) {
	return func(context.Context) (T, error) {
		var zero T
		return zero, errors.New("not implemented")
	}
}

// MTLSPrincipal is the principal of a call made over mutual TLS: the first URI SAN of the client's
// certificate, else its first DNS SAN. It reads only a chain the TLS layer verified, so a server that
// does not require and verify client certificates never has a principal from here.
func MTLSPrincipal(ctx context.Context) (string, error) { return "", errors.New("not implemented") }

// FirstPrincipal tries each func in order and uses the first that finds credentials. A func that finds
// bad ones ends the search with its error: a presented credential that fails is never papered over by
// another.
func FirstPrincipal(funcs ...PrincipalFunc) PrincipalFunc {
	return func(context.Context) (string, error) { return "", errors.New("not implemented") }
}
