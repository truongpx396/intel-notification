package domain

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
	"time"
)

var (
	unsubNow   = time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC)
	unsubKey   = []byte("0123456789abcdef0123456789abcdef") // 32 bytes
	unsubScope = UnsubscribeScope{
		Identity: Identity{Realm: "aisat", Tenant: Tenant{Kind: "workspace", ID: "w1"}, Recipient: Recipient{Kind: "user", ID: "u1"}},
		Topic:    "ingestion_complete",
		Channel:  "email",
	}
)

// frozenUnsubToken was computed with Python's hmac, hashlib, base64 and json from the format in
// D34's notes: "v1." + key id + "." + base64url(payload) + "." + base64url(HMAC-SHA256(key,
// "notify.unsubscribe.v1\n" + key id + "\n" + payload)), the payload being the scope and the expiry
// in seconds, in that order. A token that is mailed is a stored encoding: changing it breaks every
// link already sent, so it is frozen.
const frozenUnsubToken = "v1.k1.eyJyIjoiYWlzYXQiLCJ0ayI6IndvcmtzcGFjZSIsInRpIjoidzEiLCJyayI6InVzZXIiLCJyaSI6InUxIiwidCI6ImluZ2VzdGlvbl9jb21wbGV0ZSIsImMiOiJlbWFpbCIsIngiOjE3OTcwMzM2MDB9.RWd57YKRHBK2RY66RFzc5Wydeo8BWtIVlQCODyoJ-Lg"

func newUnsubSigner(t *testing.T) *UnsubscribeSigner {
	t.Helper()
	s, err := NewUnsubscribeSigner("k1", map[string][]byte{"k1": unsubKey}, MinUnsubscribeTTL)
	if err != nil {
		t.Fatalf("NewUnsubscribeSigner: %v", err)
	}
	return s
}

func TestUnsubscribeTokenMatchesTheFrozenVector(t *testing.T) {
	t.Parallel()
	s := newUnsubSigner(t)
	got, err := s.Sign(unsubScope, unsubNow)
	if err != nil || got != frozenUnsubToken {
		t.Fatalf("Sign = %q, %v, want the frozen token %q", got, err, frozenUnsubToken)
	}
	scope, err := s.Verify(frozenUnsubToken, unsubNow)
	if err != nil || scope != unsubScope {
		t.Fatalf("Verify(frozen) = %+v, %v, want %+v", scope, err, unsubScope)
	}
}

// An id is opaque to the engine (NR-018): whatever bytes a host chooses must come back as they went
// in, and none may be able to forge a boundary between two fields.
func TestUnsubscribeTokenRoundTripsOpaqueIdentities(t *testing.T) {
	t.Parallel()
	odd := []string{`a.b`, `a:b`, `a,b`, `"quoted"`, `<>&`, `back\slash`, `ünï-ç0dé`, `with space`, `tab	tab`, `emoji 🔔`, `{"x":1}`, strings.Repeat("x", 200)}
	for _, id := range odd {
		t.Run(id[:min(len(id), 20)], func(t *testing.T) {
			t.Parallel()
			s := newUnsubSigner(t)
			sc := unsubScope
			sc.Identity.Recipient.ID, sc.Identity.Tenant.ID, sc.Topic = id, id, Topic(id)
			tok, err := s.Sign(sc, unsubNow)
			if err != nil {
				t.Fatalf("Sign: %v", err)
			}
			got, err := s.Verify(tok, unsubNow)
			if err != nil || got != sc {
				t.Fatalf("Verify = %+v, %v, want %+v back unchanged", got, err, sc)
			}
		})
	}
}

// parts splits a token for the tests that alter one part and keep the rest.
func parts(t *testing.T, tok string) (version, kid, payload, mac string) {
	t.Helper()
	p := strings.Split(tok, ".")
	if len(p) != 4 {
		t.Fatalf("token %q has %d parts, want 4", tok, len(p))
	}
	return p[0], p[1], p[2], p[3]
}

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

// Every field of the scope is under the MAC: changing any one, with the signature left as it was,
// is a token the signer did not make (D34). The pair is what a leaked link is limited to.
func TestUnsubscribeTokenRefusesAnyAlteredField(t *testing.T) {
	t.Parallel()
	_, kid, payload, mac := parts(t, frozenUnsubToken)
	raw, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	for field := range fields {
		t.Run(field, func(t *testing.T) {
			t.Parallel()
			altered := map[string]any{}
			for k, v := range fields {
				altered[k] = v
			}
			if field == "x" {
				altered[field] = fields[field].(float64) + 86400*365 // a longer life
			} else {
				altered[field] = fields[field].(string) + "x"
			}
			body, _ := json.Marshal(altered)
			tok := "v1." + kid + "." + b64(body) + "." + mac
			if _, err := newUnsubSigner(t).Verify(tok, unsubNow); !errors.Is(err, ErrInvalidUnsubscribeToken) {
				t.Fatalf("Verify with %q altered = %v, want ErrInvalidUnsubscribeToken", field, err)
			}
		})
	}
	t.Run("the signature", func(t *testing.T) {
		t.Parallel()
		sig, _ := base64.RawURLEncoding.DecodeString(mac)
		sig[0] ^= 1
		tok := "v1." + kid + "." + payload + "." + b64(sig)
		if _, err := newUnsubSigner(t).Verify(tok, unsubNow); !errors.Is(err, ErrInvalidUnsubscribeToken) {
			t.Fatalf("Verify = %v, want ErrInvalidUnsubscribeToken", err)
		}
	})
}

// The key id is under the MAC too, and a key the signer does not hold verifies nothing.
func TestUnsubscribeTokenKeys(t *testing.T) {
	t.Parallel()
	other := []byte("ffffffffffffffffffffffffffffffff")
	both := map[string][]byte{"k1": unsubKey, "k2": other}
	mk := func(current string, keys map[string][]byte) *UnsubscribeSigner {
		s, err := NewUnsubscribeSigner(current, keys, MinUnsubscribeTTL)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	oldTok, err := mk("k1", both).Sign(unsubScope, unsubNow)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("a rotated key still verifies the mail already sent", func(t *testing.T) {
		t.Parallel()
		rotated := mk("k2", both)
		if got, err := rotated.Verify(oldTok, unsubNow); err != nil || got != unsubScope {
			t.Fatalf("Verify after rotation = %+v, %v, want the old token to hold", got, err)
		}
		newTok, _ := rotated.Sign(unsubScope, unsubNow)
		if !strings.HasPrefix(newTok, "v1.k2.") {
			t.Fatalf("a rotated signer signed %q, want it signed with the current key k2", newTok)
		}
	})
	t.Run("a key that is retired verifies nothing", func(t *testing.T) {
		t.Parallel()
		retired := mk("k2", map[string][]byte{"k2": other})
		if _, err := retired.Verify(oldTok, unsubNow); !errors.Is(err, ErrInvalidUnsubscribeToken) {
			t.Fatalf("Verify = %v, want ErrInvalidUnsubscribeToken", err)
		}
	})
	t.Run("another secret under the same id", func(t *testing.T) {
		t.Parallel()
		imposter := mk("k1", map[string][]byte{"k1": other})
		if _, err := imposter.Verify(oldTok, unsubNow); !errors.Is(err, ErrInvalidUnsubscribeToken) {
			t.Fatalf("Verify = %v, want ErrInvalidUnsubscribeToken", err)
		}
	})
	t.Run("the key id relabelled to another key that is held", func(t *testing.T) {
		t.Parallel()
		_, _, payload, mac := parts(t, oldTok)
		relabelled := "v1.k2." + payload + "." + mac
		if _, err := mk("k1", both).Verify(relabelled, unsubNow); !errors.Is(err, ErrInvalidUnsubscribeToken) {
			t.Fatalf("Verify = %v, want ErrInvalidUnsubscribeToken: the id is under the signature", err)
		}
	})
	t.Run("a signature made without the domain separator", func(t *testing.T) {
		t.Parallel()
		_, kid, payload, _ := parts(t, oldTok)
		raw, _ := base64.RawURLEncoding.DecodeString(payload)
		h := hmac.New(sha256.New, unsubKey)
		h.Write(raw)
		forged := "v1." + kid + "." + payload + "." + b64(h.Sum(nil))
		if _, err := mk("k1", both).Verify(forged, unsubNow); !errors.Is(err, ErrInvalidUnsubscribeToken) {
			t.Fatalf("Verify = %v, want ErrInvalidUnsubscribeToken: a MAC from the same key for another purpose must not pass", err)
		}
	})
}

// A link works for at least 60 days from the send (D34), and not a moment beyond what was signed.
func TestUnsubscribeTokenExpiry(t *testing.T) {
	t.Parallel()
	s := newUnsubSigner(t)
	tok, err := s.Sign(unsubScope, unsubNow)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		at   time.Time
		want error
	}{
		{"at the send", unsubNow, nil},
		{"after a month", unsubNow.Add(30 * 24 * time.Hour), nil},
		{"59 days and 23 hours later", unsubNow.Add(MinUnsubscribeTTL - time.Hour), nil},
		{"one second before it lapses", unsubNow.Add(MinUnsubscribeTTL - time.Second), nil},
		{"the second it lapses", unsubNow.Add(MinUnsubscribeTTL), ErrExpiredUnsubscribeToken},
		{"a day later", unsubNow.Add(MinUnsubscribeTTL + 24*time.Hour), ErrExpiredUnsubscribeToken},
		{"a year later", unsubNow.Add(365 * 24 * time.Hour), ErrExpiredUnsubscribeToken},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := s.Verify(tok, tc.at)
			if !errors.Is(err, tc.want) || (tc.want == nil && got != unsubScope) {
				t.Fatalf("Verify = %+v, %v, want %v", got, err, tc.want)
			}
			if tc.want != nil && (got != UnsubscribeScope{} || errors.Is(err, ErrInvalidUnsubscribeToken)) {
				t.Fatalf("a lapsed token returned %+v / %v: expired is its own error, and returns no scope", got, err)
			}
		})
	}
	t.Run("a longer life is signed as given", func(t *testing.T) {
		t.Parallel()
		long, _ := NewUnsubscribeSigner("k1", map[string][]byte{"k1": unsubKey}, 90*24*time.Hour)
		tok, _ := long.Sign(unsubScope, unsubNow)
		if _, err := long.Verify(tok, unsubNow.Add(89*24*time.Hour)); err != nil {
			t.Fatalf("Verify = %v, want a 90-day token to hold at day 89", err)
		}
	})
}

// Whatever arrives in a query string is hostile input: nothing here may panic, and none of it may
// verify.
func TestUnsubscribeTokenRefusesWhatIsNotAToken(t *testing.T) {
	t.Parallel()
	_, _, payload, mac := parts(t, frozenUnsubToken)
	okJSON := b64([]byte(`{"r":"aisat","tk":"workspace","ti":"w1","rk":"user","ri":"u1","t":"x","c":"email","x":1797033600}`))
	cases := map[string]string{
		"empty":                               "",
		"a word":                              "unsubscribe",
		"one dot":                             "v1.k1",
		"three parts":                         "v1.k1." + payload,
		"five parts":                          frozenUnsubToken + ".extra",
		"another version":                     "v2" + strings.TrimPrefix(frozenUnsubToken, "v1"),
		"no version":                          strings.TrimPrefix(frozenUnsubToken, "v1."),
		"empty key id":                        "v1.." + payload + "." + mac,
		"empty payload":                       "v1.k1.." + mac,
		"empty signature":                     "v1.k1." + payload + ".",
		"payload not base64":                  "v1.k1.!!!." + mac,
		"signature not base64":                "v1.k1." + payload + ".!!!",
		"padded base64":                       "v1.k1." + payload + "==." + mac,
		"payload is not JSON":                 "v1.k1." + b64([]byte("not json")) + "." + mac,
		"leading whitespace":                  " " + frozenUnsubToken,
		"trailing newline":                    frozenUnsubToken + "\n",
		"a signature spelled non-canonically": frozenUnsubToken[:len(frozenUnsubToken)-1] + "h",
		"a newline inside":                    strings.Replace(frozenUnsubToken, ".eyJ", ".\neyJ", 1),
		"a carriage return inside":            strings.Replace(frozenUnsubToken, ".RWd", ".R\rWd", 1),
		"a NUL":                               frozenUnsubToken + "\x00",
		"a long run of dots":                  strings.Repeat(".", 5000),
		"a very long token":                   "v1.k1." + strings.Repeat("A", 100_000) + "." + mac,
		"a key id with a bad char":            "v1.k 1." + payload + "." + mac,
		"JSON with another shape":             "v1.k1." + b64([]byte(`[1,2,3]`)) + "." + mac,
		"JSON that is signed wrong":           "v1.k1." + okJSON + "." + mac,
	}
	s := newUnsubSigner(t)
	for name, tok := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if tok == frozenUnsubToken {
				t.Fatal("this row built the valid token, so it proves nothing: its replacement matched nothing")
			}
			got, err := s.Verify(tok, unsubNow)
			if !errors.Is(err, ErrInvalidUnsubscribeToken) || got != (UnsubscribeScope{}) {
				t.Fatalf("Verify = %+v, %v, want ErrInvalidUnsubscribeToken and no scope", got, err)
			}
		})
	}
}

// A signature is not enough: the payload it covers must be a whole scope. A signer is only trusted
// to sign one, but a key that leaked could sign anything, and a token for nobody is still refused.
func TestUnsubscribeTokenRefusesAnIncompleteSignedPayload(t *testing.T) {
	t.Parallel()
	sign := func(payload string) string {
		h := hmac.New(sha256.New, unsubKey)
		h.Write([]byte("notify.unsubscribe.v1\nk1\n" + payload))
		return "v1.k1." + b64([]byte(payload)) + "." + b64(h.Sum(nil))
	}
	cases := map[string]string{
		"no realm":         `{"r":"","tk":"workspace","ti":"w1","rk":"user","ri":"u1","t":"x","c":"email","x":1797033600}`,
		"no tenant id":     `{"r":"aisat","tk":"workspace","ti":"","rk":"user","ri":"u1","t":"x","c":"email","x":1797033600}`,
		"no recipient":     `{"r":"aisat","tk":"workspace","ti":"w1","rk":"user","ri":"","t":"x","c":"email","x":1797033600}`,
		"no topic":         `{"r":"aisat","tk":"workspace","ti":"w1","rk":"user","ri":"u1","t":"","c":"email","x":1797033600}`,
		"no channel":       `{"r":"aisat","tk":"workspace","ti":"w1","rk":"user","ri":"u1","t":"x","c":"","x":1797033600}`,
		"no expiry":        `{"r":"aisat","tk":"workspace","ti":"w1","rk":"user","ri":"u1","t":"x","c":"email"}`,
		"an unknown field": `{"r":"aisat","tk":"workspace","ti":"w1","rk":"user","ri":"u1","t":"x","c":"email","x":1797033600,"admin":true}`,
		"trailing data":    `{"r":"aisat","tk":"workspace","ti":"w1","rk":"user","ri":"u1","t":"x","c":"email","x":1797033600}{}`,
	}
	s := newUnsubSigner(t)
	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := s.Verify(sign(payload), unsubNow); !errors.Is(err, ErrInvalidUnsubscribeToken) {
				t.Fatalf("Verify = %v, want ErrInvalidUnsubscribeToken", err)
			}
		})
	}
	t.Run("the whole scope, signed, verifies", func(t *testing.T) {
		t.Parallel()
		ok := `{"r":"aisat","tk":"workspace","ti":"w1","rk":"user","ri":"u1","t":"x","c":"email","x":1797033600}`
		if _, err := s.Verify(sign(ok), unsubNow); err != nil {
			t.Fatalf("Verify = %v, want the control to hold, or the rows above prove nothing", err)
		}
	})
}

func TestUnsubscribeSignerConfiguration(t *testing.T) {
	t.Parallel()
	k := func(n int) []byte { return []byte(strings.Repeat("k", n)) }
	cases := []struct {
		name    string
		current string
		keys    map[string][]byte
		ttl     time.Duration
		ok      bool
		why     string
	}{
		{"exactly the minimum life", "k1", map[string][]byte{"k1": k(32)}, MinUnsubscribeTTL, true, ""},
		{"a longer life", "k1", map[string][]byte{"k1": k(32)}, 90 * 24 * time.Hour, true, ""},
		{"a life one second short of 60 days", "k1", map[string][]byte{"k1": k(32)}, MinUnsubscribeTTL - time.Second, false, "D34: at least 60 days, or a link dies before the law says it may"},
		{"a month", "k1", map[string][]byte{"k1": k(32)}, 30 * 24 * time.Hour, false, "D34"},
		{"no life", "k1", map[string][]byte{"k1": k(32)}, 0, false, ""},
		{"a key one byte short", "k1", map[string][]byte{"k1": k(31)}, MinUnsubscribeTTL, false, "HMAC-SHA256 wants a key as long as its output"},
		{"a retired key that is too short", "k1", map[string][]byte{"k1": k(32), "old": k(8)}, MinUnsubscribeTTL, false, "a weak key that still verifies is a weak key"},
		{"no keys", "k1", nil, MinUnsubscribeTTL, false, ""},
		{"a current key that is not held", "k2", map[string][]byte{"k1": k(32)}, MinUnsubscribeTTL, false, ""},
		{"no current key id", "", map[string][]byte{"k1": k(32)}, MinUnsubscribeTTL, false, ""},
		{"a key id with a dot", "k.1", map[string][]byte{"k.1": k(32)}, MinUnsubscribeTTL, false, "a dot splits the token"},
		{"a key id with a space", "k 1", map[string][]byte{"k 1": k(32)}, MinUnsubscribeTTL, false, ""},
		{"a key id over 32 characters", strings.Repeat("a", 33), map[string][]byte{strings.Repeat("a", 33): k(32)}, MinUnsubscribeTTL, false, ""},
		{"a key id of 32 characters", strings.Repeat("a", 32), map[string][]byte{strings.Repeat("a", 32): k(32)}, MinUnsubscribeTTL, true, ""},
		{"a key id with - and _", "k_1-a", map[string][]byte{"k_1-a": k(32)}, MinUnsubscribeTTL, true, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := NewUnsubscribeSigner(tc.current, tc.keys, tc.ttl)
			if (err == nil) != tc.ok {
				t.Fatalf("NewUnsubscribeSigner = %v, want ok=%v: %s", err, tc.ok, tc.why)
			}
		})
	}
	t.Run("the keys are copied", func(t *testing.T) {
		t.Parallel()
		keys := map[string][]byte{"k1": append([]byte(nil), unsubKey...)}
		s, _ := NewUnsubscribeSigner("k1", keys, MinUnsubscribeTTL)
		want, _ := s.Sign(unsubScope, unsubNow)
		keys["k1"][0] ^= 0xff // the host reuses its buffers
		delete(keys, "k1")
		if got, _ := s.Sign(unsubScope, unsubNow); got != want {
			t.Fatalf("a signer's tokens changed when the host changed its key map: %q, want %q", got, want)
		}
	})
}

func TestUnsubscribeSignRefusesWhatIsNotAScope(t *testing.T) {
	t.Parallel()
	with := func(f func(*UnsubscribeScope)) UnsubscribeScope { sc := unsubScope; f(&sc); return sc }
	cases := map[string]UnsubscribeScope{
		"no realm":                    with(func(s *UnsubscribeScope) { s.Identity.Realm = "" }),
		"no tenant kind":              with(func(s *UnsubscribeScope) { s.Identity.Tenant.Kind = "" }),
		"no tenant id":                with(func(s *UnsubscribeScope) { s.Identity.Tenant.ID = "" }),
		"no recipient":                with(func(s *UnsubscribeScope) { s.Identity.Recipient.ID = "" }),
		"no topic":                    with(func(s *UnsubscribeScope) { s.Topic = "" }),
		"no channel":                  with(func(s *UnsubscribeScope) { s.Channel = "" }),
		"a string that is not UTF-8":  with(func(s *UnsubscribeScope) { s.Identity.Recipient.ID = "u\xff1" }),
		"a scope too large for a URL": with(func(s *UnsubscribeScope) { s.Identity.Recipient.ID = strings.Repeat("x", 4000) }),
	}
	s := newUnsubSigner(t)
	for name, sc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if tok, err := s.Sign(sc, unsubNow); err == nil {
				t.Fatalf("Sign = %q, want an error: a token must be for one whole pair", tok)
			}
		})
	}
}

// A token the signer made is never longer than a link can carry, so a longer one that carries a
// valid signature (a key that leaked, a bug elsewhere) is refused for its size alone: the bound is
// on the work a query string can cause, and is not observable by any token that fails its MAC.
func TestUnsubscribeVerifyRefusesAnOversizeSignedToken(t *testing.T) {
	t.Parallel()
	payload := `{"r":"aisat","tk":"workspace","ti":"w1","rk":"user","ri":"` + strings.Repeat("u", 3000) + `","t":"x","c":"email","x":1797033600}`
	h := hmac.New(sha256.New, unsubKey)
	h.Write([]byte("notify.unsubscribe.v1\nk1\n" + payload))
	tok := "v1.k1." + b64([]byte(payload)) + "." + b64(h.Sum(nil))
	if len(tok) <= 2048 {
		t.Fatalf("the fixture is %d bytes, want one over the cap", len(tok))
	}
	if _, err := newUnsubSigner(t).Verify(tok, unsubNow); !errors.Is(err, ErrInvalidUnsubscribeToken) {
		t.Fatalf("Verify = %v, want ErrInvalidUnsubscribeToken: a signed token over the cap is still refused", err)
	}
}

// How a MAC is compared is a security property no behaviour test can see: a comparison that exits at
// the first differing byte returns the same answers as one that does not, and differs only in time. The
// timing itself cannot be asserted in a unit test, where a 32-byte comparison differs by nanoseconds
// and a measurement would be noise or blind. What can be asserted is the choice of primitive, and that
// is what a careless edit changes. So the source is read: Verify must compare the presented signature
// with the one it computes using hmac.Equal (or subtle.ConstantTimeCompare), and nothing in the file
// may compare a MAC another way. The checker is itself held to sources that get it right and wrong.

// macComparisonProblems reports what is wrong with how src compares a MAC in its Verify method.
func macComparisonProblems(t *testing.T, src string) []string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "src.go", src, 0)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	var problems []string
	callee := func(c *ast.CallExpr) string {
		if sel, ok := c.Fun.(*ast.SelectorExpr); ok {
			if pkg, ok := sel.X.(*ast.Ident); ok {
				return pkg.Name + "." + sel.Sel.Name
			}
		}
		return ""
	}
	// touchesMAC: the expression is, or computes, a signature. The result of a constant-time compare, and
	// a length (which is not secret), are not the signature, so a comparison of those is fine.
	touchesMAC := func(e ast.Expr) bool {
		found := false
		ast.Inspect(e, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.CallExpr:
				if id, ok := n.Fun.(*ast.Ident); ok && id.Name == "len" {
					return false
				}
				if name := callee(n); name == "hmac.Equal" || name == "subtle.ConstantTimeCompare" {
					return false
				}
			case *ast.Ident:
				if n.Name == "sig" || n.Name == "mac" || n.Name == "expected" {
					found = true
				}
			case *ast.SelectorExpr:
				if n.Sel.Name == "mac" {
					found = true
				}
			}
			return !found
		})
		return found
	}
	forbidden := map[string]bool{"bytes.Equal": true, "bytes.Compare": true, "strings.Compare": true, "strings.EqualFold": true, "reflect.DeepEqual": true}
	ast.Inspect(file, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.CallExpr:
			if name := callee(n); forbidden[name] {
				problems = append(problems, name+" is not constant-time")
			}
		case *ast.BinaryExpr:
			if (n.Op == token.EQL || n.Op == token.NEQ) && (touchesMAC(n.X) || touchesMAC(n.Y)) {
				problems = append(problems, "a MAC is compared with "+n.Op.String())
			}
		}
		return true
	})
	var verify *ast.FuncDecl
	for _, d := range file.Decls {
		if fn, ok := d.(*ast.FuncDecl); ok && fn.Recv != nil && fn.Name.Name == "Verify" {
			verify = fn
		}
	}
	if verify == nil {
		return append(problems, "there is no Verify method")
	}
	compared := false
	ast.Inspect(verify, func(n ast.Node) bool {
		c, ok := n.(*ast.CallExpr)
		if !ok || len(c.Args) != 2 {
			return true
		}
		if name := callee(c); name != "hmac.Equal" && name != "subtle.ConstantTimeCompare" {
			return true
		}
		isSig := func(e ast.Expr) bool { id, ok := e.(*ast.Ident); return ok && id.Name == "sig" }
		isComputed := func(e ast.Expr) bool {
			call, ok := e.(*ast.CallExpr)
			if !ok {
				return false
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			return ok && sel.Sel.Name == "mac"
		}
		if (isSig(c.Args[0]) && isComputed(c.Args[1])) || (isComputed(c.Args[0]) && isSig(c.Args[1])) {
			compared = true
		}
		return true
	})
	if !compared {
		problems = append(problems, "Verify does not compare the presented signature with the computed one using hmac.Equal or subtle.ConstantTimeCompare")
	}
	return problems
}

func TestUnsubscribeVerifyComparesTheMACInConstantTime(t *testing.T) {
	t.Parallel()
	src, err := os.ReadFile("unsubscribe.go")
	if err != nil {
		t.Fatal(err)
	}
	if problems := macComparisonProblems(t, string(src)); len(problems) != 0 {
		t.Fatalf("unsubscribe.go compares a MAC in a way that is not constant-time: %q", problems)
	}
}

// The checker has to fail what it is for, or the test above proves nothing.
func TestMACComparisonCheckerTellsRightFromWrong(t *testing.T) {
	t.Parallel()
	wrap := func(body string) string {
		return "package p\nimport (\"bytes\"; \"crypto/hmac\"; \"crypto/subtle\")\nvar _, _, _ = bytes.Equal, hmac.Equal, subtle.ConstantTimeCompare\n" +
			"type S struct{}\nfunc (s *S) mac(k string, b []byte) []byte { return nil }\nfunc (s *S) Verify(tok string) bool {\n\tvar sig, body []byte\n" + body + "\n\treturn true\n}\n"
	}
	cases := []struct {
		name string
		src  string
		bad  bool
	}{
		{"hmac.Equal against the computed MAC", wrap(`if !hmac.Equal(sig, s.mac("k", body)) { return false }`), false},
		{"the arguments the other way round", wrap(`if !hmac.Equal(s.mac("k", body), sig) { return false }`), false},
		{"subtle.ConstantTimeCompare against the computed MAC", wrap(`if subtle.ConstantTimeCompare(sig, s.mac("k", body)) != 1 { return false }`), false},
		{"a length check before the comparison", wrap(`if len(sig) != 32 || !hmac.Equal(sig, s.mac("k", body)) { return false }`), false},
		{"bytes.Equal", wrap(`if !bytes.Equal(sig, s.mac("k", body)) { return false }`), true},
		{"string comparison", wrap(`if string(sig) != string(s.mac("k", body)) { return false }`), true},
		{"== on a computed MAC", wrap(`expected := s.mac("k", body); if len(sig) == len(expected) && string(sig) == string(expected) { return false }`), true},
		{"hmac.Equal that is not the MAC check", wrap(`if !hmac.Equal(body, body) { return false }`), true},
		{"hmac.Equal against a signature it did not compute", wrap(`if !hmac.Equal(sig, body) { return false }`), true},
		{"no comparison at all", wrap(`_ = sig`), true},
		{"bytes.Compare alongside a correct check", wrap(`_ = bytes.Compare(sig, body); if !hmac.Equal(sig, s.mac("k", body)) { return false }`), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			problems := macComparisonProblems(t, tc.src)
			if (len(problems) != 0) != tc.bad {
				t.Fatalf("problems = %q, want bad=%v", problems, tc.bad)
			}
		})
	}
	t.Run("a source with no Verify", func(t *testing.T) {
		t.Parallel()
		if problems := macComparisonProblems(t, "package p\n"); len(problems) == 0 {
			t.Fatal("a file with no Verify passed")
		}
	})
}
