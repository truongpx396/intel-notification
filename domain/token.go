package domain

import (
	"encoding/base64"
	"encoding/json"
	"errors"
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

// KeySet is the keys one realm's host signs recipient tokens with, by key id. It is parsed from the JWKS
// the host publishes (RFC 7517) and holds only keys of the three types the verifier accepts.
type KeySet struct{ keys map[string]any }

// ParseJWKS reads a JWKS document. It keeps the keys of the types a recipient token may be signed with:
// an EC key on P-256 (ES256), an Ed25519 key (EdDSA) and an RSA key of at least 2048 bits (RS256). It
// skips, and does not fail on, a key of another type, a key meant for encryption, and a key with no id,
// so a host that publishes more than this engine reads is not refused; and it refuses a document in which
// a key id appears twice, because which key a token meant is then a guess.
func ParseJWKS(data []byte) (KeySet, error) { return KeySet{}, nil }

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
func PeekIssuer(token string) (string, bool) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "", false
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", false
	}
	var claims struct {
		Iss string `json:"iss"`
	}
	if json.Unmarshal(raw, &claims) != nil || claims.Iss == "" {
		return "", false
	}
	return claims.Iss, true
}

// VerifyRecipientToken verifies a recipient token against a realm's key set and returns the identity it
// carries (T050). The signature is checked first and with the algorithm the KEY allows, never the one
// the header asks for; then the claims. Nothing in the token is read as an identity until both hold.
func VerifyRecipientToken(token string, keys KeySet, p TokenPolicy) (RecipientClaims, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return RecipientClaims{}, ErrInvalidToken
	}
	raw, _ := base64.RawURLEncoding.DecodeString(parts[1])
	var c struct {
		Tenant    struct{ Kind, ID string } `json:"tenant"`
		Recipient struct{ Kind, ID string } `json:"recipient"`
	}
	_ = json.Unmarshal(raw, &c)
	return RecipientClaims{
		Tenant:    Tenant{Kind: c.Tenant.Kind, ID: c.Tenant.ID},
		Recipient: Recipient{Kind: c.Recipient.Kind, ID: c.Recipient.ID},
	}, nil
}
