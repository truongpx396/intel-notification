package domain

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

// MinUnsubscribeTTL is how long an unsubscribe link must keep working: D34 says at least 60 days,
// and CAN-SPAM wants 30.
const MinUnsubscribeTTL = 60 * 24 * time.Hour

// MinUnsubscribeKeyBytes is the shortest signing key accepted: an HMAC-SHA256 key shorter than its
// output weakens it.
const MinUnsubscribeKeyBytes = 32

// maxUnsubscribeToken bounds a token, and with it the work a hostile query string can cause. A
// token that would not fit is a scope too large for a URL.
const maxUnsubscribeToken = 2048

var (
	// ErrInvalidUnsubscribeToken is returned for a token that is malformed, signed by a key the signer
	// does not hold, or altered. The REST surface answers it with a 410 (D34).
	ErrInvalidUnsubscribeToken = errors.New("notify: invalid unsubscribe token")
	// ErrExpiredUnsubscribeToken is returned for a token that was genuine and has lapsed. It is told
	// apart so a host can offer a route to preferences; the REST surface answers it with a 410 too.
	ErrExpiredUnsubscribeToken = errors.New("notify: unsubscribe token has expired")
)

// UnsubscribeScope is the one preference a token may disable: a leaked link turns off a single
// (realm, tenant, recipient, topic, channel) pair and nothing else (D34).
type UnsubscribeScope struct {
	Identity Identity
	Topic    Topic
	Channel  ChannelKind
}

// unsubscribePayload is what a token signs. The names are short because the token is a URL, and the
// order is the order they are written in, which the frozen vector fixes.
type unsubscribePayload struct {
	Realm         string `json:"r"`
	TenantKind    string `json:"tk"`
	TenantID      string `json:"ti"`
	RecipientKind string `json:"rk"`
	RecipientID   string `json:"ri"`
	Topic         string `json:"t"`
	Channel       string `json:"c"`
	Expires       int64  `json:"x"` // Unix seconds; the token is good while now is before it
}

const (
	unsubscribeVersion = "v1"
	// unsubscribeDomain separates this MAC from any other the same key might make.
	unsubscribeDomain = "notify.unsubscribe.v1\n"
)

// tokenAlphabet is every character a token holds: base64url, the version, key ids and the dots.
var tokenAlphabet = regexp.MustCompile(`^[A-Za-z0-9_.-]*$`)

var keyIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,32}$`)

var tokenEncoding = base64.RawURLEncoding.Strict()

// UnsubscribeSigner makes and checks unsubscribe tokens (D34):
//
//	v1.<key id>.<base64url payload>.<base64url HMAC-SHA256>
//
// The MAC covers the key id and the payload, under a domain separator, and is checked before the
// payload is read. The payload is the pair a token disables and the second it lapses. A signer
// signs with one key and verifies with every key it holds, so a key rotates by adding the new one
// and making it current, and retires when the mail signed with the old one has outlived its life.
type UnsubscribeSigner struct {
	current string
	keys    map[string][]byte
	ttl     time.Duration
}

// NewUnsubscribeSigner returns a signer that signs with the key named currentKeyID and verifies with
// any key in keys. ttl is how long a token works from the moment it is signed, and may not be less
// than [MinUnsubscribeTTL]. The keys are copied.
func NewUnsubscribeSigner(currentKeyID string, keys map[string][]byte, ttl time.Duration) (*UnsubscribeSigner, error) {
	if ttl < MinUnsubscribeTTL {
		return nil, fmt.Errorf("domain: unsubscribe token life %v is under the %v D34 requires", ttl, MinUnsubscribeTTL)
	}
	if len(keys) == 0 {
		return nil, errors.New("domain: an unsubscribe signer needs at least one key")
	}
	held := make(map[string][]byte, len(keys))
	for id, key := range keys {
		if !keyIDPattern.MatchString(id) {
			return nil, fmt.Errorf("domain: unsubscribe key id %q must match %s: a dot would split the token", id, keyIDPattern)
		}
		if len(key) < MinUnsubscribeKeyBytes {
			return nil, fmt.Errorf("domain: unsubscribe key %q is %d bytes, want at least %d", id, len(key), MinUnsubscribeKeyBytes)
		}
		held[id] = bytes.Clone(key)
	}
	if _, ok := held[currentKeyID]; !ok {
		return nil, fmt.Errorf("domain: the current unsubscribe key %q is not among the keys held", currentKeyID)
	}
	return &UnsubscribeSigner{current: currentKeyID, keys: held, ttl: ttl}, nil
}

func (s *UnsubscribeSigner) mac(keyID string, key, payload []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(unsubscribeDomain + keyID + "\n"))
	h.Write(payload)
	return h.Sum(nil)
}

// Sign makes the token for scope, good from now for the signer's life. A scope with an empty
// component is refused, as is one whose strings are not valid UTF-8 (the payload is JSON, which
// would rewrite them, and the token would then name another pair), or that makes a token too long
// for a URL.
func (s *UnsubscribeSigner) Sign(sc UnsubscribeScope, now time.Time) (string, error) {
	if err := sc.Identity.Validate(); err != nil {
		return "", fmt.Errorf("domain: unsubscribe scope: %w", err)
	}
	if sc.Topic == "" || sc.Channel == "" {
		return "", errors.New("domain: unsubscribe scope needs a topic and a channel")
	}
	p := unsubscribePayload{
		Realm: string(sc.Identity.Realm), TenantKind: sc.Identity.Tenant.Kind, TenantID: sc.Identity.Tenant.ID,
		RecipientKind: sc.Identity.Recipient.Kind, RecipientID: sc.Identity.Recipient.ID,
		Topic: string(sc.Topic), Channel: string(sc.Channel),
		Expires: now.Add(s.ttl).Unix(),
	}
	for _, field := range []string{p.Realm, p.TenantKind, p.TenantID, p.RecipientKind, p.RecipientID, p.Topic, p.Channel} {
		if !utf8.ValidString(field) {
			return "", errors.New("domain: unsubscribe scope holds a string that is not valid UTF-8")
		}
	}
	body, err := json.Marshal(p)
	if err != nil {
		return "", fmt.Errorf("domain: encode the unsubscribe payload: %w", err)
	}
	token := unsubscribeVersion + "." + s.current + "." + tokenEncoding.EncodeToString(body) + "." +
		tokenEncoding.EncodeToString(s.mac(s.current, s.keys[s.current], body))
	if len(token) > maxUnsubscribeToken {
		return "", fmt.Errorf("domain: the unsubscribe token for this scope is %d bytes, over the %d a link can carry", len(token), maxUnsubscribeToken)
	}
	return token, nil
}

// Verify checks a token at now and returns the scope it was made for. A token that is not one this
// signer made is [ErrInvalidUnsubscribeToken], whatever is wrong with it, and one it made that has
// lapsed is [ErrExpiredUnsubscribeToken]; neither returns a scope. The MAC is checked in constant
// time, and before the payload is parsed.
func (s *UnsubscribeSigner) Verify(token string, now time.Time) (UnsubscribeScope, error) {
	invalid := func(why string) (UnsubscribeScope, error) {
		return UnsubscribeScope{}, fmt.Errorf("%w: %s", ErrInvalidUnsubscribeToken, why)
	}
	if len(token) > maxUnsubscribeToken {
		return invalid("too long")
	}
	// The decoder skips CR and LF, so without this several spellings would verify as one token.
	if !tokenAlphabet.MatchString(token) {
		return invalid("not in the token alphabet")
	}
	parts := strings.Split(token, ".")
	if len(parts) != 4 || parts[0] != unsubscribeVersion {
		return invalid("not a v1 token")
	}
	key, ok := s.keys[parts[1]]
	if !ok {
		return invalid("unknown key")
	}
	body, err := tokenEncoding.DecodeString(parts[2])
	if err != nil || len(body) == 0 {
		return invalid("payload is not base64url")
	}
	sig, err := tokenEncoding.DecodeString(parts[3])
	if err != nil {
		return invalid("signature is not base64url")
	}
	if !hmac.Equal(sig, s.mac(parts[1], key, body)) {
		return invalid("signature does not match")
	}

	var p unsubscribePayload
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p); err != nil {
		return invalid("payload is not a scope")
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return invalid("payload has trailing data")
	}
	scope := UnsubscribeScope{
		Identity: Identity{
			Realm:     Realm(p.Realm),
			Tenant:    Tenant{Kind: p.TenantKind, ID: p.TenantID},
			Recipient: Recipient{Kind: p.RecipientKind, ID: p.RecipientID},
		},
		Topic:   Topic(p.Topic),
		Channel: ChannelKind(p.Channel),
	}
	if scope.Identity.Validate() != nil || scope.Topic == "" || scope.Channel == "" || p.Expires <= 0 {
		return invalid("payload is not a whole scope")
	}
	if now.Unix() >= p.Expires {
		return UnsubscribeScope{}, ErrExpiredUnsubscribeToken
	}
	return scope, nil
}
