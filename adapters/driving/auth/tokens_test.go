package auth_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"

	"github.com/truongpx396/intel-notification/adapters/driving/auth"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/notifytest"
)

// T050, NR-008, NR-009, D31. The recipient's identity is the verified claims of a token the realm's own
// host signed, and nothing else.

var (
	tenantW1 = domain.Tenant{Kind: "workspace", ID: "w1"}
	userU1   = domain.Recipient{Kind: "user", ID: "u1"}
)

type tokenWorld struct {
	h      *host
	clk    *clock
	keys   *auth.Keys
	alpha  *notifytest.TokenSigner
	beta   *notifytest.TokenSigner
	recips *auth.Recipients
}

func newTokenWorld(t *testing.T, opts ...auth.RecipientOption) *tokenWorld {
	t.Helper()
	w := &tokenWorld{h: newHost(t), clk: newClock(), alpha: notifytest.NewTokenSigner("alpha-1"), beta: notifytest.NewTokenSigner("beta-1")}
	w.h.serve("/alpha.json", notifytest.JWKS(w.alpha))
	w.h.serve("/beta.json", notifytest.JWKS(w.beta))
	w.keys = keysFor(t, w.h, w.clk)
	w.recips = auth.NewRecipients(w.keys, append([]auth.RecipientOption{auth.WithNow(w.clk.Now)}, opts...)...)
	return w
}

func bearer(token string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/notifications", nil)
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	return r
}

func (w *tokenWorld) recipientToken(s *notifytest.TokenSigner, realm domain.Realm) string {
	return s.Recipient(realm, tenantW1, userU1, w.clk.Now(), 10*time.Minute)
}

func TestARecipientTokenOpensTheInboxItNamesInTheRealmThatSignedIt(t *testing.T) {
	t.Parallel()
	w := newTokenWorld(t)
	got, err := w.recips.Identify(bearer(w.recipientToken(w.alpha, "alpha")))
	want := domain.Identity{Realm: "alpha", Tenant: tenantW1, Recipient: userU1}
	if err != nil || got != want {
		t.Fatalf("Identify = %+v, %v, want %+v", got, err, want)
	}
	got, err = w.recips.Identify(bearer(w.recipientToken(w.beta, "beta")))
	if err != nil || got.Realm != "beta" {
		t.Fatalf("Identify = %+v, %v, want realm beta", got, err)
	}
}

// NR-009. A token is for the realm whose keys signed it. One realm's host cannot mint a token that opens
// another realm's inbox, whatever it puts in iss.
func TestOneRealmsKeysNeverOpenAnotherRealmsInbox(t *testing.T) {
	t.Parallel()
	w := newTokenWorld(t)
	cases := map[string]string{
		"a token alpha's host signed, claiming to be beta's": w.recipientToken(w.alpha, "beta"),
		"a token beta's host signed, claiming to be alpha's": w.recipientToken(w.beta, "alpha"),
		"a token for a realm nobody configured":              w.recipientToken(w.alpha, "gamma"),
	}
	for name, token := range cases {
		if got, err := w.recips.Identify(bearer(token)); err == nil {
			t.Errorf("%s: Identify = %+v, nil, want an error (a 401)", name, got)
		}
	}
	if n := w.h.fetches("/gamma.json"); n != 0 {
		t.Errorf("%d fetches for a realm nobody configured, want none", n)
	}
}

func TestTheRequestIsRefusedWithoutABearerToken(t *testing.T) {
	t.Parallel()
	w := newTokenWorld(t)
	good := w.recipientToken(w.alpha, "alpha")
	for name, r := range map[string]*http.Request{
		"no header": bearer(""),
		"another scheme": func() *http.Request {
			r := bearer("")
			r.Header.Set("Authorization", "Basic dXNlcjpwYXNz")
			return r
		}(),
		"a bare token, no scheme": func() *http.Request {
			r := bearer("")
			r.Header.Set("Authorization", good)
			return r
		}(),
		"an empty bearer": func() *http.Request {
			r := bearer("")
			r.Header.Set("Authorization", "Bearer ")
			return r
		}(),
		"a token in a query parameter, which is not read": func() *http.Request {
			return httptest.NewRequest(http.MethodGet, "/notifications?token="+good, nil)
		}(),
		"a token in a header that is not Authorization": func() *http.Request {
			r := bearer("")
			r.Header.Set("X-Token", good)
			return r
		}(),
	} {
		if got, err := w.recips.Identify(r); err == nil {
			t.Errorf("%s: Identify = %+v, nil, want an error", name, got)
		}
	}
	// The scheme's case is the client's business (RFC 9110).
	r := bearer("")
	r.Header.Set("Authorization", "bearer "+good)
	if _, err := w.recips.Identify(r); err != nil {
		t.Errorf("a lower-case scheme: %v, want it accepted", err)
	}
}

func TestTheIdentityIsTheTokensClaimsAndNotItsSubjectOrRealmClaim(t *testing.T) {
	t.Parallel()
	w := newTokenWorld(t)
	token := w.alpha.Sign(map[string]any{
		"iss": "alpha", "iat": w.clk.Now().Unix(), "exp": w.clk.Now().Add(time.Minute).Unix(),
		"sub": "user:someone-else", "realm": "beta",
		"tenant": map[string]string{"kind": "workspace", "id": "w1"}, "recipient": map[string]string{"kind": "user", "id": "u1"},
	})
	got, err := w.recips.Identify(bearer(token))
	if err != nil || got != (domain.Identity{Realm: "alpha", Tenant: tenantW1, Recipient: userU1}) {
		t.Fatalf("Identify = %+v, %v, want the tenant and recipient objects in the realm that signed it", got, err)
	}
}

func TestAnExpiredTokenIsRefusedByTheClockItIsGiven(t *testing.T) {
	t.Parallel()
	w := newTokenWorld(t)
	token := w.recipientToken(w.alpha, "alpha")
	if _, err := w.recips.Identify(bearer(token)); err != nil {
		t.Fatal(err)
	}
	w.clk.Advance(11 * time.Minute)
	if got, err := w.recips.Identify(bearer(token)); err == nil {
		t.Fatalf("Identify after expiry = %+v, nil, want an error", got)
	}
}

func TestARequiredAudienceIsRequired(t *testing.T) {
	t.Parallel()
	w := newTokenWorld(t, auth.WithAudience("intel-notification"))
	if _, err := w.recips.Identify(bearer(w.recipientToken(w.alpha, "alpha"))); err == nil {
		t.Fatal("a token with no audience, where one is required, was accepted")
	}
	claims := map[string]any{
		"iss": "alpha", "aud": "intel-notification", "exp": w.clk.Now().Add(time.Minute).Unix(),
		"tenant": map[string]string{"kind": "workspace", "id": "w1"}, "recipient": map[string]string{"kind": "user", "id": "u1"},
	}
	if _, err := w.recips.Identify(bearer(w.alpha.Sign(claims))); err != nil {
		t.Fatalf("a token for the audience: %v", err)
	}
}

// T050: an unknown kid is refetched once. A host that rotated its keys is followed, and a stranger's
// invented key ids cost one request an interval.
func TestAHostsRotatedKeyIsFollowedAndInventedKeyIDsAreBounded(t *testing.T) {
	t.Parallel()
	w := newTokenWorld(t)
	if _, err := w.recips.Identify(bearer(w.recipientToken(w.alpha, "alpha"))); err != nil {
		t.Fatal(err)
	}
	rotated := notifytest.NewTokenSigner("alpha-2")
	w.h.serve("/alpha.json", notifytest.JWKS(w.alpha, rotated))
	w.clk.Advance(2 * time.Minute) // past the refetch bound, within the cache's ttl
	if got, err := w.recips.Identify(bearer(rotated.Recipient("alpha", tenantW1, userU1, w.clk.Now(), time.Minute))); err != nil || got.Realm != "alpha" {
		t.Fatalf("a token under the host's new key = %+v, %v, want it accepted after one refetch", got, err)
	}
	before := w.h.fetches("/alpha.json")
	for i := range 40 {
		stranger := notifytest.NewTokenSigner("invented-" + strings.Repeat("x", i%5))
		if _, err := w.recips.Identify(bearer(stranger.Recipient("alpha", tenantW1, userU1, w.clk.Now(), time.Minute))); err == nil {
			t.Fatal("a token signed by a stranger's key was accepted")
		}
	}
	if extra := w.h.fetches("/alpha.json") - before; extra > 1 {
		t.Fatalf("%d fetches caused by 40 tokens with invented key ids, want at most 1 per interval", extra)
	}
}

func TestNoErrorFromIdentifyRepeatsTheToken(t *testing.T) {
	t.Parallel()
	w := newTokenWorld(t)
	token := w.recipientToken(w.beta, "alpha") // wrong key for the realm it names
	_, err := w.recips.Identify(bearer(token))
	if err == nil || strings.Contains(err.Error(), token) || strings.Contains(err.Error(), token[:20]) {
		t.Fatalf("err = %v, want a refusal that does not repeat what was presented", err)
	}
}

// ------------------------------------------------------------------------------------- producers

func producerWorld(t *testing.T) (*tokenWorld, auth.PrincipalFunc) {
	t.Helper()
	w := newTokenWorld(t)
	b, err := domain.ParseRealmBindings("producer-a=alpha,producer-b=beta")
	if err != nil {
		t.Fatal(err)
	}
	return w, auth.ProducerTokens(b, w.keys, auth.WithProducerNow(w.clk.Now))
}

func withBearer(token string) context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token))
}

func TestAProducerTokenIsItsPrincipalWhenTheRealmsOwnKeysSignedIt(t *testing.T) {
	t.Parallel()
	w, principal := producerWorld(t)
	for _, c := range []struct {
		signer *notifytest.TokenSigner
		realm  domain.Realm
		sub    string
	}{{w.alpha, "alpha", "producer-a"}, {w.beta, "beta", "producer-b"}} {
		got, err := principal(withBearer(c.signer.Producer(c.realm, c.sub, w.clk.Now(), 5*time.Minute)))
		if err != nil || got != c.sub {
			t.Errorf("principal = %q, %v, want %q", got, err, c.sub)
		}
	}
}

// NR-009. A producer bound to alpha cannot be impersonated with beta's keys, or any other.
func TestAProducerTokenFromAnotherRealmsKeysOrAnUnboundSubjectIsNotAPrincipal(t *testing.T) {
	t.Parallel()
	w, principal := producerWorld(t)
	stranger := notifytest.NewTokenSigner("alpha-1") // the same kid as alpha's, another key
	cases := map[string]string{
		"producer-a, signed by beta's host": w.beta.Producer("alpha", "producer-a", w.clk.Now(), time.Minute),
		"producer-a, signed by a stranger":  stranger.Producer("alpha", "producer-a", w.clk.Now(), time.Minute),
		"a subject nothing binds":           w.alpha.Producer("alpha", "mallory", w.clk.Now(), time.Minute),
		"producer-a, claiming beta's realm": w.alpha.Producer("beta", "producer-a", w.clk.Now(), time.Minute),
		"expired":                           w.alpha.Producer("alpha", "producer-a", w.clk.Now().Add(-time.Hour), time.Minute),
		"a recipient token":                 w.recipientToken(w.alpha, "alpha"),
	}
	for name, token := range cases {
		got, err := principal(withBearer(token))
		if err == nil || errors.Is(err, auth.ErrNoCredentials) || got != "" {
			t.Errorf("%s: principal = %q, %v, want a credential that was presented and is not good", name, got, err)
		}
	}
}

func TestAProducerPrincipalFuncFindsNoCredentialsWhereThereAreNone(t *testing.T) {
	t.Parallel()
	_, principal := producerWorld(t)
	for name, ctx := range map[string]context.Context{
		"no metadata":      context.Background(),
		"no authorization": metadata.NewIncomingContext(context.Background(), metadata.Pairs("x", "y")),
		"another scheme":   metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Basic dXNlcjpwYXNz")),
	} {
		if _, err := principal(ctx); !errors.Is(err, auth.ErrNoCredentials) {
			t.Errorf("%s: err = %v, want ErrNoCredentials, so the next way of authenticating may be tried", name, err)
		}
	}
}

// A host that rotates a key in place, under the same key id, is followed like one that adds a key: the old key
// no longer verifies its tokens, which is as good a sign as an unknown key id that the keys moved, and a
// refetch, once and bounded, finds the new one. Without it every token is refused until the cache expires.
func TestAKeyRotatedInPlaceUnderTheSameKeyIDIsFollowedAndStillBounded(t *testing.T) {
	t.Parallel()
	w := newTokenWorld(t)
	if _, err := w.recips.Identify(bearer(w.recipientToken(w.alpha, "alpha"))); err != nil {
		t.Fatal(err)
	}
	rotated := notifytest.NewTokenSigner("alpha-1") // the same key id, a new key
	w.h.serve("/alpha.json", notifytest.JWKS(rotated))
	w.clk.Advance(2 * time.Minute) // past the refetch bound, within the cache's ttl
	if got, err := w.recips.Identify(bearer(rotated.Recipient("alpha", tenantW1, userU1, w.clk.Now(), time.Minute))); err != nil || got.Realm != "alpha" {
		t.Fatalf("a token under the key rotated in place = %+v, %v, want it accepted after one refetch", got, err)
	}
	before := w.h.fetches("/alpha.json")
	stranger := notifytest.NewTokenSigner("alpha-1") // yet another key under that id: not the host's
	for range 40 {
		if _, err := w.recips.Identify(bearer(stranger.Recipient("alpha", tenantW1, userU1, w.clk.Now(), time.Minute))); err == nil {
			t.Fatal("a token signed by a stranger's key under the host's key id was accepted")
		}
	}
	if extra := w.h.fetches("/alpha.json") - before; extra > 1 {
		t.Fatalf("%d fetches caused by 40 tokens that do not verify, want at most 1 per interval", extra)
	}
}
