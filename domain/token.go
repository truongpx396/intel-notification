package domain

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"
)

// ErrInvalidToken is wrapped by every refusal of a recipient token: expired, signed by a key the realm
// does not hold, an algorithm it does not accept, a claim that is missing or wrong. The caller answers it
// with an empty 401, and says nothing of which (rest-api.md).
var ErrInvalidToken = errors.New("notify: invalid recipient token")

// ErrUnknownKeyID is a token whose key id the realm's key set does not hold. It wraps ErrInvalidToken. It
// is the one refusal a caller may answer by fetching the key set again, once and bounded, because the host
// may have rotated its keys since the set was read (T050).
var ErrUnknownKeyID = errors.New("notify: the token's key id is not in the realm's key set")

// ErrBadSignature is a token whose signature does not verify under the key its header names. It wraps
// ErrInvalidToken. Like ErrUnknownKeyID it is a refusal a caller may answer by fetching the key set again,
// once and bounded: a host that rotates a key in place, under the same key id, produces exactly this, and a
// stranger's forgery produces it too, which is why the fetch is bounded and its result is only ever used to
// verify again.
var ErrBadSignature = errors.New("notify: the token's signature does not verify")

// KeySet is the keys one realm's host signs recipient tokens with, by key id. It is parsed from the JWKS
// the host publishes (RFC 7517) and holds only keys of the three types the verifier accepts.
type KeySet struct{ keys map[string]any }

// jwk is the part of a JSON Web Key this engine reads (RFC 7517, 7518).
type jwk struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	Use string `json:"use"`
	Crv string `json:"crv"`
	X   string `json:"x"`
	Y   string `json:"y"`
	N   string `json:"n"`
	E   string `json:"e"`
}

// minRSABits is the smallest RSA modulus a recipient token's key may have.
const minRSABits = 2048

// ParseJWKS reads a JWKS document. It keeps the keys of the types a recipient token may be signed with:
// an EC key on P-256 (ES256), an Ed25519 key (EdDSA) and an RSA key of at least 2048 bits (RS256). It
// skips, and does not fail on, a key of another type (a shared secret above all: a JWKS never makes one
// a verification key), a key meant for encryption, a key with no id and a point that is not on its curve,
// so a host that publishes more than this engine reads is not refused; and it refuses a document in which
// a key id appears twice, because which key a token meant is then a guess.
func ParseJWKS(data []byte) (KeySet, error) {
	var doc struct {
		Keys *[]jwk `json:"keys"`
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		return KeySet{}, fmt.Errorf("the key set is not a JWKS document: %w", err)
	}
	if doc.Keys == nil {
		return KeySet{}, errors.New("the key set is not a JWKS document: it has no \"keys\"")
	}
	set := KeySet{keys: map[string]any{}}
	seen := map[string]bool{}
	for _, k := range *doc.Keys {
		if k.Kid != "" {
			if seen[k.Kid] {
				return KeySet{}, fmt.Errorf("the key id %q appears twice in the key set: which key a token means is a guess", k.Kid)
			}
			seen[k.Kid] = true
		}
		if k.Kid == "" || (k.Use != "" && k.Use != "sig") {
			continue
		}
		if key := k.publicKey(); key != nil {
			set.keys[k.Kid] = key
		}
	}
	return set, nil
}

// publicKey is the key a jwk describes, or nil when it is not one the verifier accepts.
func (k jwk) publicKey() any {
	dec := base64.RawURLEncoding.Strict().DecodeString
	switch k.Kty {
	case "EC":
		x, errX := dec(k.X)
		y, errY := dec(k.Y)
		if k.Crv != "P-256" || errX != nil || errY != nil || len(x) != 32 || len(y) != 32 {
			return nil
		}
		// The uncompressed point, which the standard library refuses when it is not on the curve.
		pub, err := ecdsa.ParseUncompressedPublicKey(elliptic.P256(), append(append([]byte{4}, x...), y...))
		if err != nil {
			return nil
		}
		return pub
	case "OKP":
		x, err := dec(k.X)
		if k.Crv != "Ed25519" || err != nil || len(x) != ed25519.PublicKeySize {
			return nil
		}
		return ed25519.PublicKey(x)
	case "RSA":
		n, errN := dec(k.N)
		e, errE := dec(k.E)
		if errN != nil || errE != nil || len(e) == 0 || len(e) > 4 {
			return nil
		}
		pub := &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: int(new(big.Int).SetBytes(e).Int64())}
		if pub.N.BitLen() < minRSABits || pub.E < 3 || pub.E%2 == 0 {
			return nil
		}
		return pub
	}
	return nil
}

// Len is the number of keys in the set.
func (k KeySet) Len() int { return len(k.keys) }

// Has reports whether the set holds a key with this id.
func (k KeySet) Has(kid string) bool { _, ok := k.keys[kid]; return ok }

// TokenPolicy is what a token is judged against.
type TokenPolicy struct {
	// Issuer is the realm the token must be for: its iss claim is this realm's id and no other.
	Issuer Realm
	// Audience, when not empty, is required: the token's aud must name it. Empty: aud is not read.
	Audience string
	// Now is the instant the token is judged at.
	Now time.Time
	// MaxLifetime bounds how far in the future exp may be. Zero is an hour (rest-api.md).
	MaxLifetime time.Duration
	// Leeway is the clock skew tolerated on exp, nbf and iat. Zero is 30 seconds.
	Leeway time.Duration
}

// RecipientClaims is what a verified token says: whose inbox it opens. The realm is not here: it is the
// policy's, and a token cannot name another.
type RecipientClaims struct {
	Tenant    Tenant
	Recipient Recipient
	ExpiresAt time.Time
}

// PeekIssuer reads a token's iss claim WITHOUT verifying anything, so the caller can choose which
// realm's key set to verify against. What it returns is attacker-controlled and decides nothing but
// which keys are tried: a token that names a realm it was not signed for fails verification under that
// realm's keys.
func PeekIssuer(token string) (string, bool) { return peekClaim(token, "iss") }

// PeekSubject reads a token's sub claim WITHOUT verifying anything, to find which producer principal it
// claims to be and so which realm's keys to verify it with. Like PeekIssuer it chooses keys and nothing
// else: the subject a caller acts on is the one VerifyProducerToken returns.
func PeekSubject(token string) (string, bool) { return peekClaim(token, "sub") }

func peekClaim(token, name string) (string, bool) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "", false
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", false
	}
	var claims map[string]json.RawMessage
	if json.Unmarshal(raw, &claims) != nil {
		return "", false
	}
	var value string
	if json.Unmarshal(claims[name], &value) != nil || value == "" {
		return "", false
	}
	return value, true
}

// maxTokenBytes is the largest token the verifier reads. No token a host mints is anywhere near it.
const maxTokenBytes = 8 << 10

func invalidToken(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidToken, fmt.Sprintf(format, args...))
}

// VerifyRecipientToken verifies a recipient token against a realm's key set and returns the identity it
// carries (T050). The signature is checked first and with the algorithm the KEY allows, never the one
// the header asks for; then the claims. Nothing in the token is read as an identity until both hold, and
// nothing a token says about its subject or its realm is read as one at all: the tenant and the
// recipient are its two objects, and the realm is the policy's.
//
// ES256, EdDSA and RS256 are accepted. alg none is not, and HS256 is not: a secret both sides hold lets
// either mint a token for any recipient (security.md), so it is not implemented even for development.
func VerifyRecipientToken(token string, keys KeySet, p TokenPolicy) (RecipientClaims, error) {
	payload, err := verifyJWS(token, keys)
	if err != nil {
		return RecipientClaims{}, err
	}
	return recipientClaims(payload, p)
}

// verifyJWS checks the structure and the signature of a compact JWS against a key set and returns its
// payload. It is everything that must hold before a claim may be read.
func verifyJWS(token string, keys KeySet) ([]byte, error) {
	if len(token) > maxTokenBytes {
		return nil, invalidToken("the token is over %d bytes", maxTokenBytes)
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return nil, invalidToken("not a compact JWS")
	}
	dec := base64.RawURLEncoding.Strict().DecodeString
	rawHeader, errH := dec(parts[0])
	rawPayload, errP := dec(parts[1])
	sig, errS := dec(parts[2])
	if errH != nil || errP != nil || errS != nil {
		return nil, invalidToken("a part is not unpadded base64url")
	}

	var header map[string]json.RawMessage
	if err := json.Unmarshal(rawHeader, &header); err != nil {
		return nil, invalidToken("the header is not a JSON object")
	}
	if _, critical := header["crit"]; critical {
		return nil, invalidToken("the header requires an extension this verifier does not implement")
	}
	var alg, kid string
	if json.Unmarshal(header["alg"], &alg) != nil || json.Unmarshal(header["kid"], &kid) != nil || kid == "" {
		return nil, invalidToken("the header names no algorithm or no key id")
	}

	key, ok := keys.keys[kid]
	if !ok {
		return nil, fmt.Errorf("%w: %w", ErrInvalidToken, ErrUnknownKeyID)
	}
	signed := []byte(parts[0] + "." + parts[1])
	sum := sha256.Sum256(signed)
	valid := false
	// The key decides which algorithm may be used with it. A header that asks for another is lying or
	// confused, and the classic confusions (HS256 over a public key, RS256 over an EC key) die here.
	switch k := key.(type) {
	case *ecdsa.PublicKey:
		if alg == "ES256" && len(sig) == 64 { // raw r||s, not DER (RFC 7518 §3.4)
			valid = ecdsa.Verify(k, sum[:], new(big.Int).SetBytes(sig[:32]), new(big.Int).SetBytes(sig[32:]))
		}
	case ed25519.PublicKey:
		if alg == "EdDSA" && len(sig) == ed25519.SignatureSize {
			valid = ed25519.Verify(k, signed, sig)
		}
	case *rsa.PublicKey:
		if alg == "RS256" {
			valid = rsa.VerifyPKCS1v15(k, crypto.SHA256, sum[:], sig) == nil
		}
	}
	if !valid {
		return nil, fmt.Errorf("%w: %w: under the key %q", ErrInvalidToken, ErrBadSignature, kid)
	}
	return rawPayload, nil
}

// registered is the registered claims every token of the engine is judged on.
type registered struct {
	Iss string          `json:"iss"`
	Sub string          `json:"sub"`
	Aud json.RawMessage `json:"aud"`
	Exp *float64        `json:"exp"`
	Nbf *float64        `json:"nbf"`
	Iat *float64        `json:"iat"`
}

// check judges the registered claims against the policy and returns the expiry.
func (c registered) check(p TokenPolicy) (time.Time, error) {
	leeway, maxLife := p.Leeway, p.MaxLifetime
	if leeway == 0 {
		leeway = 30 * time.Second
	}
	if maxLife == 0 {
		maxLife = time.Hour
	}
	at := func(v *float64) time.Time { return time.Unix(int64(*v), 0).UTC() }

	if c.Iss != string(p.Issuer) {
		return time.Time{}, invalidToken("the token is for another realm")
	}
	if c.Exp == nil {
		return time.Time{}, invalidToken("the token has no expiry")
	}
	exp := at(c.Exp)
	switch {
	case !p.Now.Before(exp.Add(leeway)):
		return time.Time{}, invalidToken("the token has expired")
	case exp.After(p.Now.Add(maxLife).Add(leeway)):
		return time.Time{}, invalidToken("the token expires more than %s from now", maxLife)
	case c.Nbf != nil && at(c.Nbf).After(p.Now.Add(leeway)):
		return time.Time{}, invalidToken("the token is not valid yet")
	case c.Iat != nil && at(c.Iat).After(p.Now.Add(leeway)):
		return time.Time{}, invalidToken("the token was issued in the future")
	}
	if p.Audience != "" && !audienceHolds(c.Aud, p.Audience) {
		return time.Time{}, invalidToken("the token is not for this audience")
	}
	return exp, nil
}

// recipientClaims judges the claims of a recipient token whose signature verified.
func recipientClaims(raw []byte, p TokenPolicy) (RecipientClaims, error) {
	var c struct {
		registered
		Tenant *struct {
			Kind string `json:"kind"`
			ID   string `json:"id"`
		} `json:"tenant"`
		Recipient *struct {
			Kind string `json:"kind"`
			ID   string `json:"id"`
		} `json:"recipient"`
	}
	if err := json.Unmarshal(raw, &c); err != nil {
		return RecipientClaims{}, invalidToken("the claims are not a JSON object of the expected shape")
	}
	exp, err := c.check(p)
	if err != nil {
		return RecipientClaims{}, err
	}
	if c.Tenant == nil || c.Tenant.Kind == "" || c.Tenant.ID == "" || c.Recipient == nil || c.Recipient.Kind == "" || c.Recipient.ID == "" {
		return RecipientClaims{}, invalidToken("the token does not name a tenant and a recipient")
	}
	return RecipientClaims{
		Tenant:    Tenant{Kind: c.Tenant.Kind, ID: c.Tenant.ID},
		Recipient: Recipient{Kind: c.Recipient.Kind, ID: c.Recipient.ID},
		ExpiresAt: exp,
	}, nil
}

// ProducerClaims is what a verified producer token says: which principal is calling.
type ProducerClaims struct {
	Subject   string
	ExpiresAt time.Time
}

// VerifyProducerToken verifies a token a producer presents to the gRPC surface, signed by the host of the
// realm it is bound to (D31). It is judged exactly as a recipient token is, and carries a subject, the
// producer principal, where a recipient token carries a tenant and a recipient. The realm is the
// policy's and not the token's, so the subject is bound to a realm by RealmBindings and by the keys that
// signed the token, and by nothing the token says.
func VerifyProducerToken(token string, keys KeySet, p TokenPolicy) (ProducerClaims, error) {
	payload, err := verifyJWS(token, keys)
	if err != nil {
		return ProducerClaims{}, err
	}
	var c registered
	if err := json.Unmarshal(payload, &c); err != nil {
		return ProducerClaims{}, invalidToken("the claims are not a JSON object of the expected shape")
	}
	exp, err := c.check(p)
	if err != nil {
		return ProducerClaims{}, err
	}
	if c.Sub == "" {
		return ProducerClaims{}, invalidToken("the token has no subject")
	}
	return ProducerClaims{Subject: c.Sub, ExpiresAt: exp}, nil
}

// audienceHolds reports whether an aud claim, a string or a list of them, names want.
func audienceHolds(raw json.RawMessage, want string) bool {
	var one string
	if json.Unmarshal(raw, &one) == nil {
		return one == want
	}
	var many []string
	if json.Unmarshal(raw, &many) == nil {
		for _, a := range many {
			if a == want {
				return true
			}
		}
	}
	return false
}
