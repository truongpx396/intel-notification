package domain_test

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/big"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/truongpx396/intel-notification/domain"
)

// T050, NR-008, D31. A recipient token opens one inbox, and only when the realm's own keys signed it. The
// table is the task's: expired, wrong audience, another realm's key, alg none, tampered, an unknown kid,
// and the recipient identity from verified claims and nothing else.

var now = time.Date(2031, 3, 1, 12, 0, 0, 0, time.UTC)

type signer struct {
	kid string
	alg string
	key crypto.Signer
}

var (
	keysOnce sync.Once
	ecKey    *ecdsa.PrivateKey
	edKey    ed25519.PrivateKey
	rsaKey   *rsa.PrivateKey
	otherEC  *ecdsa.PrivateKey
)

func testKeys(t *testing.T) {
	t.Helper()
	keysOnce.Do(func() {
		var err error
		if ecKey, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader); err != nil {
			panic(err)
		}
		if otherEC, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader); err != nil {
			panic(err)
		}
		if _, edKey, err = ed25519.GenerateKey(rand.Reader); err != nil {
			panic(err)
		}
		if rsaKey, err = rsa.GenerateKey(rand.Reader, 2048); err != nil {
			panic(err)
		}
	})
}

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

func jsonOf(t *testing.T, v any) []byte {
	t.Helper()
	out, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// jwksOf is the JWKS document for keys, as a host would publish it.
func jwksOf(t *testing.T, ks ...map[string]any) []byte {
	t.Helper()
	return jsonOf(t, map[string]any{"keys": ks})
}

func ecJWK(kid string, k *ecdsa.PublicKey) map[string]any {
	return map[string]any{"kty": "EC", "crv": "P-256", "kid": kid, "use": "sig", "alg": "ES256",
		"x": b64(k.X.FillBytes(make([]byte, 32))), "y": b64(k.Y.FillBytes(make([]byte, 32)))}
}
func edJWK(kid string, k ed25519.PublicKey) map[string]any {
	return map[string]any{"kty": "OKP", "crv": "Ed25519", "kid": kid, "x": b64(k)}
}
func rsaJWK(kid string, k *rsa.PublicKey) map[string]any {
	return map[string]any{"kty": "RSA", "kid": kid, "alg": "RS256", "n": b64(k.N.Bytes()), "e": b64(big.NewInt(int64(k.E)).Bytes())}
}

// validClaims is a good token's claims at now.
func validClaims() map[string]any {
	return map[string]any{
		"iss": "alpha", "iat": now.Add(-time.Minute).Unix(), "exp": now.Add(10 * time.Minute).Unix(),
		"tenant": map[string]string{"kind": "workspace", "id": "w1"}, "recipient": map[string]string{"kind": "user", "id": "u1"},
	}
}

// mint signs claims. header overrides the header the signer would write, so a case can lie about alg.
func mint(t *testing.T, s signer, claims map[string]any, header map[string]any) string {
	t.Helper()
	h := map[string]any{"alg": s.alg, "typ": "JWT", "kid": s.kid}
	for k, v := range header {
		if v == nil {
			delete(h, k)
			continue
		}
		h[k] = v
	}
	input := b64(jsonOf(t, h)) + "." + b64(jsonOf(t, claims))
	return input + "." + b64(sign(t, s, input))
}

func sign(t *testing.T, s signer, input string) []byte {
	t.Helper()
	switch k := s.key.(type) {
	case *ecdsa.PrivateKey:
		sum := sha256.Sum256([]byte(input))
		r, ss, err := ecdsa.Sign(rand.Reader, k, sum[:])
		if err != nil {
			t.Fatal(err)
		}
		return append(r.FillBytes(make([]byte, 32)), ss.FillBytes(make([]byte, 32))...)
	case ed25519.PrivateKey:
		return ed25519.Sign(k, []byte(input))
	case *rsa.PrivateKey:
		sum := sha256.Sum256([]byte(input))
		sig, err := rsa.SignPKCS1v15(rand.Reader, k, crypto.SHA256, sum[:])
		if err != nil {
			t.Fatal(err)
		}
		return sig
	}
	t.Fatalf("no signer for %T", s.key)
	return nil
}

func policy(mutate ...func(*domain.TokenPolicy)) domain.TokenPolicy {
	p := domain.TokenPolicy{Issuer: "alpha", Now: now}
	for _, m := range mutate {
		m(&p)
	}
	return p
}

func realmKeys(t *testing.T) domain.KeySet {
	t.Helper()
	testKeys(t)
	ks, err := domain.ParseJWKS(jwksOf(t, ecJWK("ec1", &ecKey.PublicKey), edJWK("ed1", edKey.Public().(ed25519.PublicKey)), rsaJWK("rsa1", &rsaKey.PublicKey)))
	if err != nil {
		t.Fatalf("ParseJWKS: %v", err)
	}
	return ks
}

func TestAGoodTokenOpensTheInboxItNames(t *testing.T) {
	t.Parallel()
	testKeys(t)
	ks := realmKeys(t)
	for _, s := range []signer{{"ec1", "ES256", ecKey}, {"ed1", "EdDSA", edKey}, {"rsa1", "RS256", rsaKey}} {
		got, err := domain.VerifyRecipientToken(mint(t, s, validClaims(), nil), ks, policy())
		if err != nil {
			t.Errorf("%s: %v", s.alg, err)
			continue
		}
		if got.Tenant != (domain.Tenant{Kind: "workspace", ID: "w1"}) || got.Recipient != (domain.Recipient{Kind: "user", ID: "u1"}) {
			t.Errorf("%s: claims = %+v, want the tenant and recipient the token names", s.alg, got)
		}
		if !got.ExpiresAt.Equal(now.Add(10 * time.Minute)) {
			t.Errorf("%s: ExpiresAt = %v", s.alg, got.ExpiresAt)
		}
	}
}

func TestATokenIsRefused(t *testing.T) {
	t.Parallel()
	testKeys(t)
	ks := realmKeys(t)
	ec := signer{"ec1", "ES256", ecKey}
	with := func(f func(map[string]any)) map[string]any {
		c := validClaims()
		f(c)
		return c
	}
	cases := []struct {
		name  string
		token func(t *testing.T) string
		pol   domain.TokenPolicy
		why   string
	}{
		{"expired", func(t *testing.T) string {
			return mint(t, ec, with(func(c map[string]any) { c["exp"] = now.Add(-time.Minute).Unix() }), nil)
		}, policy(), "a token past its exp opens nothing"},
		{"expired a moment ago, beyond the leeway", func(t *testing.T) string {
			return mint(t, ec, with(func(c map[string]any) { c["exp"] = now.Add(-time.Minute).Unix() }), nil)
		}, policy(func(p *domain.TokenPolicy) { p.Leeway = time.Second }), "the leeway is what is configured"},
		{"no exp", func(t *testing.T) string {
			return mint(t, ec, with(func(c map[string]any) { delete(c, "exp") }), nil)
		}, policy(), "a token that never expires is not one a host mints (rest-api.md: exp ≤ 1 hour)"},
		{"an exp more than an hour away", func(t *testing.T) string {
			return mint(t, ec, with(func(c map[string]any) { c["exp"] = now.Add(25 * time.Hour).Unix() }), nil)
		}, policy(), "the lifetime is bounded, so a leaked token is bounded too"},
		{"not yet valid", func(t *testing.T) string {
			return mint(t, ec, with(func(c map[string]any) { c["nbf"] = now.Add(time.Hour).Unix() }), nil)
		}, policy(), "nbf in the future"},
		{"issued in the future", func(t *testing.T) string {
			return mint(t, ec, with(func(c map[string]any) { c["iat"] = now.Add(time.Hour).Unix() }), nil)
		}, policy(), "a clock that far off is not skew"},
		{"wrong audience", func(t *testing.T) string {
			return mint(t, ec, with(func(c map[string]any) { c["aud"] = "some-other-service" }), nil)
		}, policy(func(p *domain.TokenPolicy) { p.Audience = "intel-notification" }), "a token minted for another service"},
		{"no audience where one is required", func(t *testing.T) string {
			return mint(t, ec, validClaims(), nil)
		}, policy(func(p *domain.TokenPolicy) { p.Audience = "intel-notification" }), "an audience, once configured, is required"},
		{"another realm's iss", func(t *testing.T) string {
			return mint(t, ec, with(func(c map[string]any) { c["iss"] = "beta" }), nil)
		}, policy(), "a token for realm beta is not realm alpha's, even signed by a key alpha holds (NR-009)"},
		{"signed by another realm's key under the same kid", func(t *testing.T) string {
			return mint(t, signer{"ec1", "ES256", otherEC}, validClaims(), nil)
		}, policy(), "the signature is checked against the realm's own key"},
		{"signed by a key the realm does not hold", func(t *testing.T) string {
			return mint(t, signer{"not-mine", "ES256", otherEC}, validClaims(), nil)
		}, policy(), "an unknown kid"},
		{"alg none with no signature", func(t *testing.T) string {
			return b64(jsonOf(t, map[string]any{"alg": "none", "kid": "ec1"})) + "." + b64(jsonOf(t, validClaims())) + "."
		}, policy(), "alg none is no signature"},
		{"alg none with a signature", func(t *testing.T) string {
			good := mint(t, ec, validClaims(), nil)
			parts := strings.Split(good, ".")
			return b64(jsonOf(t, map[string]any{"alg": "none", "kid": "ec1"})) + "." + parts[1] + "." + parts[2]
		}, policy(), "alg none, whatever follows"},
		{"alg None in another case", func(t *testing.T) string {
			return mint(t, ec, validClaims(), map[string]any{"alg": "NoNe"})
		}, policy(), "the header's alg is not matched case-insensitively against an allow list"},
		{"HS256 signed with the public key as the secret", func(t *testing.T) string {
			input := b64(jsonOf(t, map[string]any{"alg": "HS256", "kid": "ec1", "typ": "JWT"})) + "." + b64(jsonOf(t, validClaims()))
			mac := hmac.New(sha256.New, jsonOf(t, ecJWK("ec1", &ecKey.PublicKey)))
			mac.Write([]byte(input))
			return input + "." + b64(mac.Sum(nil))
		}, policy(), "the classic algorithm confusion: a shared secret either side can mint with"},
		{"RS256 in the header over an EC key", func(t *testing.T) string {
			return mint(t, ec, validClaims(), map[string]any{"alg": "RS256"})
		}, policy(), "the key decides the algorithm, not the header"},
		{"ES256 in the header over an RSA key", func(t *testing.T) string {
			return mint(t, signer{"rsa1", "RS256", rsaKey}, validClaims(), map[string]any{"alg": "ES256"})
		}, policy(), "the key decides the algorithm, not the header"},
		{"EdDSA in the header over an EC key", func(t *testing.T) string {
			return mint(t, ec, validClaims(), map[string]any{"alg": "EdDSA"})
		}, policy(), "the key decides the algorithm, not the header"},
		{"no kid", func(t *testing.T) string {
			return mint(t, ec, validClaims(), map[string]any{"kid": nil})
		}, policy(), "which key is a guess without one"},
		{"a critical header the verifier does not know", func(t *testing.T) string {
			return mint(t, ec, validClaims(), map[string]any{"crit": []string{"exp-extension"}})
		}, policy(), "RFC 7515: a token that requires an extension we do not implement is refused"},
		{"tampered payload", func(t *testing.T) string {
			good := mint(t, ec, validClaims(), nil)
			parts := strings.Split(good, ".")
			evil := validClaims()
			evil["recipient"] = map[string]string{"kind": "user", "id": "someone-else"}
			return parts[0] + "." + b64(jsonOf(t, evil)) + "." + parts[2]
		}, policy(), "a claim changed after signing"},
		{"tampered header", func(t *testing.T) string {
			good := mint(t, ec, validClaims(), nil)
			parts := strings.Split(good, ".")
			return b64(jsonOf(t, map[string]any{"alg": "ES256", "kid": "ec1", "extra": "x"})) + "." + parts[1] + "." + parts[2]
		}, policy(), "the header is signed too"},
		{"a truncated signature", func(t *testing.T) string {
			good := mint(t, ec, validClaims(), nil)
			return good[:len(good)-6]
		}, policy(), ""},
		{"an ES256 signature in DER form", func(t *testing.T) string {
			input := b64(jsonOf(t, map[string]any{"alg": "ES256", "kid": "ec1"})) + "." + b64(jsonOf(t, validClaims()))
			sum := sha256.Sum256([]byte(input))
			der, err := ecdsa.SignASN1(rand.Reader, ecKey, sum[:])
			if err != nil {
				t.Fatal(err)
			}
			return input + "." + b64(der)
		}, policy(), "JWS ES256 is the raw r||s, 64 bytes"},
		{"no tenant", func(t *testing.T) string {
			return mint(t, ec, with(func(c map[string]any) { delete(c, "tenant") }), nil)
		}, policy(), "a token must name whose inbox it opens"},
		{"a tenant with an empty id", func(t *testing.T) string {
			return mint(t, ec, with(func(c map[string]any) { c["tenant"] = map[string]string{"kind": "workspace", "id": ""} }), nil)
		}, policy(), "an empty component would equal an unset scope (D17)"},
		{"no recipient", func(t *testing.T) string {
			return mint(t, ec, with(func(c map[string]any) { delete(c, "recipient") }), nil)
		}, policy(), ""},
		{"a recipient with an empty kind", func(t *testing.T) string {
			return mint(t, ec, with(func(c map[string]any) { c["recipient"] = map[string]string{"kind": "", "id": "u1"} }), nil)
		}, policy(), ""},
		{"not three parts", func(*testing.T) string { return "a.b" }, policy(), ""},
		{"four parts", func(*testing.T) string { return "a.b.c.d" }, policy(), ""},
		{"empty", func(*testing.T) string { return "" }, policy(), ""},
		{"a payload that is not base64", func(t *testing.T) string {
			good := mint(t, ec, validClaims(), nil)
			parts := strings.Split(good, ".")
			return parts[0] + ".!!!." + parts[2]
		}, policy(), ""},
		{"padded base64, which JWS does not use", func(t *testing.T) string {
			good := mint(t, ec, validClaims(), nil)
			parts := strings.Split(good, ".")
			return parts[0] + "=." + parts[1] + "." + parts[2]
		}, policy(), ""},
		{"a payload that is not an object", func(t *testing.T) string {
			input := b64(jsonOf(t, map[string]any{"alg": "ES256", "kid": "ec1"})) + "." + b64([]byte(`"just a string"`))
			sum := sha256.Sum256([]byte(input))
			r, s, _ := ecdsa.Sign(rand.Reader, ecKey, sum[:])
			return input + "." + b64(append(r.FillBytes(make([]byte, 32)), s.FillBytes(make([]byte, 32))...))
		}, policy(), ""},
		{"too large", func(t *testing.T) string {
			return mint(t, ec, with(func(c map[string]any) { c["pad"] = strings.Repeat("x", 9000) }), nil)
		}, policy(), "no token is that large, and the verifier reads it all"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := domain.VerifyRecipientToken(tc.token(t), ks, tc.pol)
			if !errors.Is(err, domain.ErrInvalidToken) {
				t.Fatalf("VerifyRecipientToken = %+v, %v, want ErrInvalidToken: %s", got, err, tc.why)
			}
			if got != (domain.RecipientClaims{}) {
				t.Fatalf("a refused token returned claims %+v: nothing in it may be read as an identity", got)
			}
		})
	}
}

func TestAnUnknownKeyIDIsToldApart(t *testing.T) {
	t.Parallel()
	testKeys(t)
	ks := realmKeys(t)
	_, err := domain.VerifyRecipientToken(mint(t, signer{"rotated-in", "ES256", otherEC}, validClaims(), nil), ks, policy())
	if !errors.Is(err, domain.ErrUnknownKeyID) || !errors.Is(err, domain.ErrInvalidToken) {
		t.Fatalf("err = %v, want ErrUnknownKeyID, which is also ErrInvalidToken: it is the one refusal a caller answers by fetching the keys again", err)
	}
	_, err = domain.VerifyRecipientToken(mint(t, signer{"ec1", "ES256", otherEC}, validClaims(), nil), ks, policy())
	if errors.Is(err, domain.ErrUnknownKeyID) {
		t.Fatalf("err = %v: a known kid with a bad signature is not an unknown kid, and fetching again would not help", err)
	}
}

// The identity is the verified claims', and the realm is the policy's. Whatever else the token says, a
// subject or a realm included, is not an identity.
func TestTheIdentityComesFromVerifiedClaimsOnly(t *testing.T) {
	t.Parallel()
	testKeys(t)
	ks := realmKeys(t)
	claims := validClaims()
	claims["sub"] = "user:someone-else"
	claims["realm"] = "beta"
	claims["tenant_id"] = "w-other"
	claims["recipient_id"] = "u-other"
	got, err := domain.VerifyRecipientToken(mint(t, signer{"ec1", "ES256", ecKey}, claims, nil), ks, policy())
	if err != nil {
		t.Fatal(err)
	}
	if got.Recipient.ID != "u1" || got.Tenant.ID != "w1" {
		t.Fatalf("claims = %+v, want the `tenant` and `recipient` objects and nothing else: sub is not an identity", got)
	}
}

func TestTheLeewayAndTheLifetimeAreConfigurable(t *testing.T) {
	t.Parallel()
	testKeys(t)
	ks := realmKeys(t)
	ec := signer{"ec1", "ES256", ecKey}
	recent := validClaims()
	recent["exp"] = now.Add(-10 * time.Second).Unix()
	if _, err := domain.VerifyRecipientToken(mint(t, ec, recent, nil), ks, policy()); err != nil {
		t.Errorf("a token 10 seconds past exp, under the default leeway: %v, want it accepted: clocks differ", err)
	}
	long := validClaims()
	long["exp"] = now.Add(3 * time.Hour).Unix()
	if _, err := domain.VerifyRecipientToken(mint(t, ec, long, nil), ks, policy()); err == nil {
		t.Error("a 3-hour token under the default lifetime was accepted")
	}
	if _, err := domain.VerifyRecipientToken(mint(t, ec, long, nil), ks, policy(func(p *domain.TokenPolicy) { p.MaxLifetime = 4 * time.Hour })); err != nil {
		t.Errorf("a 3-hour token under a 4-hour lifetime: %v", err)
	}
	aud := validClaims()
	aud["aud"] = []string{"other", "intel-notification"}
	if _, err := domain.VerifyRecipientToken(mint(t, ec, aud, nil), ks, policy(func(p *domain.TokenPolicy) { p.Audience = "intel-notification" })); err != nil {
		t.Errorf("an aud list that holds the audience: %v, want it accepted", err)
	}
	if _, err := domain.VerifyRecipientToken(mint(t, ec, aud, nil), ks, policy()); err != nil {
		t.Errorf("an aud when none is configured: %v, want it ignored", err)
	}
}

func TestParseJWKS(t *testing.T) {
	t.Parallel()
	testKeys(t)
	t.Run("keeps the three types and skips what it cannot use", func(t *testing.T) {
		t.Parallel()
		small, _ := rsa.GenerateKey(rand.Reader, 1024)
		doc := jwksOf(t,
			ecJWK("ec1", &ecKey.PublicKey),
			map[string]any{"kty": "oct", "kid": "shared", "k": b64([]byte("secret"))},
			map[string]any{"kty": "EC", "crv": "P-384", "kid": "p384", "x": "AA", "y": "AA"},
			map[string]any{"kty": "EC", "crv": "P-256", "use": "enc", "kid": "enc", "x": b64(ecKey.X.FillBytes(make([]byte, 32))), "y": b64(ecKey.Y.FillBytes(make([]byte, 32)))},
			map[string]any{"kty": "EC", "crv": "P-256", "x": b64(ecKey.X.FillBytes(make([]byte, 32))), "y": b64(ecKey.Y.FillBytes(make([]byte, 32)))}, // no kid
			rsaJWK("weak", &small.PublicKey),
			map[string]any{"kty": "EC", "crv": "P-256", "kid": "off-curve", "x": b64(make([]byte, 32)), "y": b64(make([]byte, 32))},
		)
		ks, err := domain.ParseJWKS(doc)
		if err != nil {
			t.Fatal(err)
		}
		if !ks.Has("ec1") || ks.Len() != 1 {
			t.Fatalf("the set holds %d keys, want only ec1: a shared secret, an unsupported curve, an encryption key, a key with no id, a 1024-bit RSA key and a point off the curve are not keys a token may be verified with", ks.Len())
		}
	})
	t.Run("refuses a key id twice", func(t *testing.T) {
		t.Parallel()
		if _, err := domain.ParseJWKS(jwksOf(t, ecJWK("k", &ecKey.PublicKey), ecJWK("k", &otherEC.PublicKey))); err == nil {
			t.Fatal("ParseJWKS with a kid twice = nil, want an error: which key a token meant is a guess")
		}
	})
	t.Run("refuses a document that is not a JWKS", func(t *testing.T) {
		t.Parallel()
		for _, doc := range []string{``, `not json`, `[]`, `{"keys": "x"}`, `{"nokeys": []}`} {
			if _, err := domain.ParseJWKS([]byte(doc)); err == nil {
				t.Errorf("ParseJWKS(%q) = nil, want an error", doc)
			}
		}
	})
	t.Run("an empty set is a set, and verifies nothing", func(t *testing.T) {
		t.Parallel()
		ks, err := domain.ParseJWKS([]byte(`{"keys": []}`))
		if err != nil || ks.Len() != 0 {
			t.Fatalf("ParseJWKS = %v, %v", ks.Len(), err)
		}
		if _, err := domain.VerifyRecipientToken(mint(t, signer{"ec1", "ES256", ecKey}, validClaims(), nil), ks, policy()); !errors.Is(err, domain.ErrUnknownKeyID) {
			t.Fatalf("err = %v, want ErrUnknownKeyID", err)
		}
	})
}

func TestPeekIssuer(t *testing.T) {
	t.Parallel()
	testKeys(t)
	tok := mint(t, signer{"ec1", "ES256", ecKey}, validClaims(), nil)
	if iss, ok := domain.PeekIssuer(tok); !ok || iss != "alpha" {
		t.Fatalf("PeekIssuer = %q, %v, want alpha", iss, ok)
	}
	for _, bad := range []string{"", "a.b", "a.!!!.c", "a." + b64([]byte(`{}`)) + ".c", "a." + b64([]byte(`[1]`)) + ".c"} {
		if iss, ok := domain.PeekIssuer(bad); ok {
			t.Errorf("PeekIssuer(%q) = %q, true, want no issuer", bad, iss)
		}
	}
}

// D31. A producer presents a token whose subject is its principal; the realm is the policy's, and the keys
// that verify it are that realm's.
func TestAProducerTokenNamesAPrincipalAndIsJudgedLikeARecipientToken(t *testing.T) {
	t.Parallel()
	testKeys(t)
	ks := realmKeys(t)
	ec := signer{"ec1", "ES256", ecKey}
	producer := func(f func(map[string]any)) map[string]any {
		c := map[string]any{"iss": "alpha", "sub": "billing-service", "iat": now.Add(-time.Minute).Unix(), "exp": now.Add(10 * time.Minute).Unix()}
		if f != nil {
			f(c)
		}
		return c
	}
	got, err := domain.VerifyProducerToken(mint(t, ec, producer(nil), nil), ks, policy())
	if err != nil || got.Subject != "billing-service" || !got.ExpiresAt.Equal(now.Add(10*time.Minute)) {
		t.Fatalf("VerifyProducerToken = %+v, %v, want the principal the token names", got, err)
	}
	if sub, ok := domain.PeekSubject(mint(t, ec, producer(nil), nil)); !ok || sub != "billing-service" {
		t.Fatalf("PeekSubject = %q, %v", sub, ok)
	}
	refused := map[string]string{
		mint(t, ec, producer(func(c map[string]any) { delete(c, "sub") }), nil):                      "no subject",
		mint(t, ec, producer(func(c map[string]any) { c["sub"] = "" }), nil):                         "an empty subject",
		mint(t, ec, producer(func(c map[string]any) { c["iss"] = "beta" }), nil):                     "another realm's iss",
		mint(t, ec, producer(func(c map[string]any) { c["exp"] = now.Add(-time.Hour).Unix() }), nil): "expired",
		mint(t, ec, producer(func(c map[string]any) { delete(c, "exp") }), nil):                      "no expiry",
		mint(t, signer{"ec1", "ES256", otherEC}, producer(nil), nil):                                 "another key",
		mint(t, signer{"ec1", "ES256", ecKey}, producer(nil), map[string]any{"alg": "none"}):         "alg none",
	}
	for token, why := range refused {
		if got, err := domain.VerifyProducerToken(token, ks, policy()); !errors.Is(err, domain.ErrInvalidToken) || got != (domain.ProducerClaims{}) {
			t.Errorf("%s: VerifyProducerToken = %+v, %v, want ErrInvalidToken", why, got, err)
		}
	}
	// A recipient token is not a producer's, and a producer token opens no inbox.
	if _, err := domain.VerifyRecipientToken(mint(t, ec, producer(nil), nil), ks, policy()); !errors.Is(err, domain.ErrInvalidToken) {
		t.Errorf("a producer token verified as a recipient token: %v: it names no tenant and recipient", err)
	}
}
