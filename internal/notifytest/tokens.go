package notifytest

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"time"

	"github.com/truongpx396/intel-notification/domain"
)

// TokenSigner is a host's signing key, for tests: it mints the recipient and producer tokens a host would
// (ES256, a key id, claims as rest-api.md states them) and publishes its public half as a JWKS. Each
// signer has a key of its own, so a test can have two realms' hosts and show that one's tokens do not
// open the other's service.
type TokenSigner struct {
	Kid string
	key *ecdsa.PrivateKey
}

// NewTokenSigner returns a signer with a fresh P-256 key under kid.
func NewTokenSigner(kid string) *TokenSigner {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		panic("notifytest: " + err.Error())
	}
	return &TokenSigner{Kid: kid, key: key}
}

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

// JWK is the signer's public key as a JWKS entry.
func (s *TokenSigner) JWK() map[string]any {
	point, err := s.key.PublicKey.Bytes() // 0x04 || X || Y
	if err != nil {
		panic("notifytest: " + err.Error())
	}
	return map[string]any{"kty": "EC", "crv": "P-256", "kid": s.Kid, "use": "sig", "alg": "ES256", "x": b64(point[1:33]), "y": b64(point[33:65])}
}

// JWKS is a JWKS document holding the public keys of signers.
func JWKS(signers ...*TokenSigner) []byte {
	keys := make([]map[string]any, len(signers))
	for i, s := range signers {
		keys[i] = s.JWK()
	}
	out, err := json.Marshal(map[string]any{"keys": keys})
	if err != nil {
		panic("notifytest: " + err.Error())
	}
	return out
}

// Sign signs claims as they are, with the signer's key id in the header.
func (s *TokenSigner) Sign(claims map[string]any) string {
	header, _ := json.Marshal(map[string]any{"alg": "ES256", "typ": "JWT", "kid": s.Kid})
	payload, err := json.Marshal(claims)
	if err != nil {
		panic("notifytest: " + err.Error())
	}
	input := b64(header) + "." + b64(payload)
	sum := sha256.Sum256([]byte(input))
	r, ss, err := ecdsa.Sign(rand.Reader, s.key, sum[:])
	if err != nil {
		panic("notifytest: " + err.Error())
	}
	return input + "." + b64(append(r.FillBytes(make([]byte, 32)), ss.FillBytes(make([]byte, 32))...))
}

// Recipient mints the token a host gives its signed-in user: issued by realm, for one tenant and one
// recipient, valid for ttl from now.
func (s *TokenSigner) Recipient(realm domain.Realm, tenant domain.Tenant, recipient domain.Recipient, now time.Time, ttl time.Duration) string {
	return s.Sign(map[string]any{
		"iss": string(realm), "iat": now.Unix(), "exp": now.Add(ttl).Unix(),
		"tenant":    map[string]string{"kind": tenant.Kind, "id": tenant.ID},
		"recipient": map[string]string{"kind": recipient.Kind, "id": recipient.ID},
	})
}

// Producer mints the token a producer presents to the gRPC surface: issued by realm, naming principal as
// its subject.
func (s *TokenSigner) Producer(realm domain.Realm, principal string, now time.Time, ttl time.Duration) string {
	return s.Sign(map[string]any{"iss": string(realm), "sub": principal, "iat": now.Unix(), "exp": now.Add(ttl).Unix()})
}
