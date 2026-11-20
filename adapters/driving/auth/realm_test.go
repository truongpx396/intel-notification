package auth_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/url"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/truongpx396/intel-notification/adapters/driving/auth"
	"github.com/truongpx396/intel-notification/domain"
)

// T049, NR-009, D31. The realm of a call is its principal's binding, and nothing the request says.

func bindings(t *testing.T) domain.RealmBindings {
	t.Helper()
	b, err := domain.ParseRealmBindings("producer-a=alpha,producer-b=beta")
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// principalFrom reads the principal from a metadata key a test sets, standing in for what a verified
// certificate or token would say.
func principalFrom(ctx context.Context) (string, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if v := md.Get("x-test-principal"); len(v) > 0 {
		return v[0], nil
	}
	if v := md.Get("x-test-bad"); len(v) > 0 {
		return "", errors.New("the credential is not good: signature secret=hunter2")
	}
	return "", auth.ErrNoCredentials
}

func incoming(kv ...string) context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs(kv...))
}

// call runs the unary interceptor over a handler that records the realm it saw.
func call(t *testing.T, ctx context.Context, opts ...auth.Option) (reached bool, realm domain.Realm, err error) {
	t.Helper()
	unary, _ := auth.RealmInterceptors(bindings(t), principalFrom, opts...)
	_, err = unary(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/notify.v1.Notifier/Notify"}, func(ctx context.Context, _ any) (any, error) {
		reached = true
		realm, _ = auth.RealmFrom(ctx)
		return nil, nil
	})
	return reached, realm, err
}

func TestTheRealmIsTheOneThePrincipalIsBoundTo(t *testing.T) {
	t.Parallel()
	for principal, want := range map[string]domain.Realm{"producer-a": "alpha", "producer-b": "beta"} {
		reached, realm, err := call(t, incoming("x-test-principal", principal))
		if err != nil || !reached || realm != want {
			t.Errorf("%s: reached=%v realm=%q err=%v, want realm %q", principal, reached, realm, err, want)
		}
	}
}

// NR-009. A realm the caller names, in a header or in a metadata key by any spelling, is not read.
func TestARealmTheCallerNamesIsNeverRead(t *testing.T) {
	t.Parallel()
	for _, key := range []string{"x-notify-realm", "realm", "x-realm", "notify-realm", "x-forwarded-realm"} {
		reached, realm, err := call(t, incoming("x-test-principal", "producer-a", key, "beta"))
		if err != nil || !reached || realm != "alpha" {
			t.Errorf("metadata %s=beta: reached=%v realm=%q err=%v, want alpha: the realm is the principal's binding", key, reached, realm, err)
		}
	}
}

func TestAnUnboundPrincipalIsRefusedBeforeTheHandler(t *testing.T) {
	t.Parallel()
	var denied []string
	reached, _, err := call(t, incoming("x-test-principal", "mallory"), auth.WithDenied(func(p string) { denied = append(denied, p) }))
	if reached || status.Code(err) != codes.PermissionDenied {
		t.Fatalf("reached=%v err=%v, want PermissionDenied and no handler", reached, err)
	}
	if len(denied) != 1 || denied[0] != "mallory" {
		t.Fatalf("the denial hook was told %v, want the principal, so an operator can see who tried", denied)
	}
	if msg := status.Convert(err).Message(); contains(msg, "alpha") || contains(msg, "beta") || contains(msg, "producer-a") {
		t.Fatalf("the refusal %q names a binding: a caller is told it is not bound and nothing more", msg)
	}
}

func contains(s, sub string) bool {
	return len(sub) > 0 && len(s) >= len(sub) && (func() bool { return indexOf(s, sub) >= 0 })()
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func TestACallWithNoCredentialsIsUnauthenticated(t *testing.T) {
	t.Parallel()
	reached, _, err := call(t, incoming())
	if reached || status.Code(err) != codes.Unauthenticated {
		t.Fatalf("reached=%v err=%v, want Unauthenticated and no handler", reached, err)
	}
}

func TestACredentialThatIsNotGoodIsUnauthenticatedAndSaysNothingOfWhy(t *testing.T) {
	t.Parallel()
	var faults []error
	reached, _, err := call(t, incoming("x-test-bad", "1"), auth.WithFault(func(e error) { faults = append(faults, e) }))
	if reached || status.Code(err) != codes.Unauthenticated {
		t.Fatalf("reached=%v err=%v, want Unauthenticated and no handler", reached, err)
	}
	if contains(status.Convert(err).Message(), "hunter2") {
		t.Fatalf("the refusal %q repeats what the verifier said, which can hold a secret", status.Convert(err).Message())
	}
	if len(faults) != 1 {
		t.Fatalf("the fault hook was given %d errors, want the one the verifier returned, for an operator", len(faults))
	}
}

type fakeStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (f fakeStream) Context() context.Context { return f.ctx }

func TestTheStreamInterceptorDerivesTheRealmToo(t *testing.T) {
	t.Parallel()
	_, stream := auth.RealmInterceptors(bindings(t), principalFrom)
	var seen domain.Realm
	err := stream(nil, fakeStream{ctx: incoming("x-test-principal", "producer-b")}, &grpc.StreamServerInfo{FullMethod: "/x"}, func(_ any, ss grpc.ServerStream) error {
		seen, _ = auth.RealmFrom(ss.Context())
		return nil
	})
	if err != nil || seen != "beta" {
		t.Fatalf("stream: realm=%q err=%v, want beta", seen, err)
	}
	called := false
	err = stream(nil, fakeStream{ctx: incoming("x-test-principal", "mallory")}, &grpc.StreamServerInfo{FullMethod: "/x"}, func(any, grpc.ServerStream) error { called = true; return nil })
	if called || status.Code(err) != codes.PermissionDenied {
		t.Fatalf("stream for an unbound principal: called=%v err=%v, want PermissionDenied", called, err)
	}
}

func TestRealmOfAnUnauthenticatedContextIsAFaultAndNotARealm(t *testing.T) {
	t.Parallel()
	if realm, err := auth.RealmOf(context.Background()); err == nil || realm != "" {
		t.Fatalf("RealmOf = %q, %v, want an error: no interceptor ran", realm, err)
	}
}

func TestRoutePicksByTheRealmOfTheCall(t *testing.T) {
	t.Parallel()
	route := auth.Route(map[domain.Realm]string{"alpha": "engine-a", "beta": "engine-b"})
	for realm, want := range map[domain.Realm]string{"alpha": "engine-a", "beta": "engine-b"} {
		unary, _ := auth.RealmInterceptors(bindings(t), func(context.Context) (string, error) {
			return map[domain.Realm]string{"alpha": "producer-a", "beta": "producer-b"}[realm], nil
		})
		var got string
		_, err := unary(context.Background(), nil, &grpc.UnaryServerInfo{}, func(ctx context.Context, _ any) (any, error) {
			var err error
			got, err = route(ctx)
			return nil, err
		})
		if err != nil || got != want {
			t.Errorf("realm %s routed to %q, %v, want %q", realm, got, err, want)
		}
	}
	if got, err := route(context.Background()); err == nil {
		t.Errorf("a context with no realm routed to %q, want an error", got)
	}
}

func TestRouteOfARealmNothingServesIsAFault(t *testing.T) {
	t.Parallel()
	b, _ := domain.ParseRealmBindings("p=gamma")
	unary, _ := auth.RealmInterceptors(b, func(context.Context) (string, error) { return "p", nil })
	route := auth.Route(map[domain.Realm]string{"alpha": "engine-a"})
	_, err := unary(context.Background(), nil, &grpc.UnaryServerInfo{}, func(ctx context.Context, _ any) (any, error) { return route(ctx) })
	if err == nil {
		t.Fatal("a bound realm with no engine routed, want an error: a wiring fault must not serve another realm's engine")
	}
}

// ------------------------------------------------------------------------------------------ mTLS

func tlsCtx(state tls.ConnectionState) context.Context {
	return peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: state}})
}

func leaf(uri string, dns ...string) *x509.Certificate {
	c := &x509.Certificate{DNSNames: dns}
	if uri != "" {
		u, _ := url.Parse(uri)
		c.URIs = []*url.URL{u}
	}
	return c
}

func TestMTLSPrincipalIsTheVerifiedCertificatesSAN(t *testing.T) {
	t.Parallel()
	verified := func(c *x509.Certificate) tls.ConnectionState {
		return tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{c}}, PeerCertificates: []*x509.Certificate{c}}
	}
	cases := []struct {
		name string
		ctx  context.Context
		want string
		err  error
	}{
		{"a URI SAN", tlsCtx(verified(leaf("spiffe://example.test/billing", "billing.internal"))), "spiffe://example.test/billing", nil},
		{"a DNS SAN when there is no URI", tlsCtx(verified(leaf("", "billing.internal"))), "billing.internal", nil},
		{"a certificate that was presented and not verified", tlsCtx(tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf("spiffe://example.test/billing")}}), "", auth.ErrNoCredentials},
		{"a verified certificate with no SAN", tlsCtx(verified(leaf(""))), "", auth.ErrNoCredentials},
		{"a call with no TLS", context.Background(), "", auth.ErrNoCredentials},
		{"a call over TLS with no client certificate", tlsCtx(tls.ConnectionState{}), "", auth.ErrNoCredentials},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := auth.MTLSPrincipal(tc.ctx)
			if got != tc.want || !errors.Is(err, tc.err) {
				t.Fatalf("MTLSPrincipal = %q, %v, want %q, %v", got, err, tc.want, tc.err)
			}
		})
	}
}

func TestFirstPrincipalUsesTheFirstThatFindsCredentials(t *testing.T) {
	t.Parallel()
	none := func(context.Context) (string, error) { return "", auth.ErrNoCredentials }
	named := func(n string) auth.PrincipalFunc { return func(context.Context) (string, error) { return n, nil } }
	bad := func(context.Context) (string, error) { return "", errors.New("bad credential") }
	if got, err := auth.FirstPrincipal(none, named("second"), named("third"))(context.Background()); err != nil || got != "second" {
		t.Errorf("= %q, %v, want second", got, err)
	}
	if got, err := auth.FirstPrincipal(none, none)(context.Background()); !errors.Is(err, auth.ErrNoCredentials) || got != "" {
		t.Errorf("= %q, %v, want ErrNoCredentials", got, err)
	}
	if got, err := auth.FirstPrincipal(bad, named("papered-over"))(context.Background()); err == nil || errors.Is(err, auth.ErrNoCredentials) || got != "" {
		t.Errorf("= %q, %v, want the bad credential's own error: it must not be papered over by the next func", got, err)
	}
}
