package auth

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/truongpx396/intel-notification/domain"
)

// T050. A host that rotates a key in place under one key id is followed by one bounded refetch per interval. Many
// requests arrive at once when it does, and only one of them may fetch: the others have to be able to use what it
// fetched, or a valid token is refused because another request got there first.

// keySetOf is a JWKS with one Ed25519 key under kid.
func keySetOf(t *testing.T, kid string) domain.KeySet {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	doc, err := json.Marshal(map[string]any{"keys": []any{map[string]any{
		"kty": "OKP", "crv": "Ed25519", "kid": kid, "use": "sig", "alg": "EdDSA", "x": base64.RawURLEncoding.EncodeToString(pub),
	}}})
	if err != nil {
		t.Fatal(err)
	}
	set, err := domain.ParseJWKS(doc)
	if err != nil {
		t.Fatal(err)
	}
	return set
}

// racedSource is a cache whose caller read generation 1 and then found generation 2 there, put by another request.
type racedSource struct {
	stale, fresh domain.KeySet
	refetched    bool // whether this call is the one that fetched, or was throttled because another did
	freshGen     uint64
}

func (s *racedSource) forGen(context.Context, domain.Realm) (domain.KeySet, uint64, error) {
	return s.stale, 1, nil
}

func (s *racedSource) refetchGen(context.Context, domain.Realm) (domain.KeySet, uint64, bool, error) {
	return s.fresh, s.freshGen, s.refetched, nil
}

func TestARequestThatReadTheStaleSetUsesTheOneAnotherRequestFetched(t *testing.T) {
	t.Parallel()
	src := &racedSource{stale: keySetOf(t, "old"), fresh: keySetOf(t, "new"), refetched: false, freshGen: 2}
	var tried []bool
	got, err := verifyWithRefetch(t.Context(), src, "realm", func(ks domain.KeySet) (string, error) {
		tried = append(tried, ks.Has("new"))
		if !ks.Has("new") {
			return "", domain.ErrBadSignature
		}
		return "accepted", nil
	})
	if err != nil || got != "accepted" {
		t.Fatalf("verify = %q, %v (tried with fresh=%v), want accepted: the token is valid under the key another request fetched a moment ago, and refusing it because that request got there first signs a user out", got, err, tried)
	}
}

func TestNothingNewMeansNoSecondTry(t *testing.T) {
	t.Parallel()
	stale := keySetOf(t, "old")
	// Throttled, and the set is still the one this request read: a forged token gets one try and no more.
	src := &racedSource{stale: stale, fresh: stale, refetched: false, freshGen: 1}
	calls := 0
	_, err := verifyWithRefetch(t.Context(), src, "realm", func(domain.KeySet) (string, error) {
		calls++
		return "", domain.ErrBadSignature
	})
	if !errors.Is(err, domain.ErrBadSignature) || calls != 1 {
		t.Fatalf("verify ran %d times and returned %v, want once and the signature failure: a refetch that was throttled and brought nothing is no reason to try again", calls, err)
	}
}

func TestAFetchThatBroughtAKeyIsTriedOnce(t *testing.T) {
	t.Parallel()
	src := &racedSource{stale: keySetOf(t, "old"), fresh: keySetOf(t, "new"), refetched: true, freshGen: 2}
	calls := 0
	_, err := verifyWithRefetch(t.Context(), src, "realm", func(domain.KeySet) (string, error) {
		calls++
		return "", domain.ErrUnknownKeyID
	})
	if !errors.Is(err, domain.ErrUnknownKeyID) || calls != 2 {
		t.Fatalf("verify ran %d times and returned %v, want twice (before and after the fetch) and the failure", calls, err)
	}
}

// The generation is what lets a request that read an old set know a newer one is there, so it advances when, and
// only when, the cache comes to hold a different fetched set.
func TestTheGenerationAdvancesOnlyWhenANewSetIsFetched(t *testing.T) {
	t.Parallel()
	var serving atomic.Value
	var failing atomic.Bool
	serving.Store("old")
	docs := map[string][]byte{}
	for _, kid := range []string{"old", "new"} {
		pub, _, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		docs[kid], _ = json.Marshal(map[string]any{"keys": []any{map[string]any{"kty": "OKP", "crv": "Ed25519", "kid": kid, "use": "sig", "alg": "EdDSA", "x": base64.RawURLEncoding.EncodeToString(pub)}}})
	}
	host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if failing.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = w.Write(docs[serving.Load().(string)])
	}))
	t.Cleanup(host.Close)
	now := time.Unix(1_700_000_000, 0)
	keys, err := NewKeys(map[domain.Realm]string{"r": host.URL}, AllowInsecureHTTP(), WithClock(func() time.Time { return now }), WithMinRefetch(time.Minute))
	if err != nil {
		t.Fatal(err)
	}

	set, g1, err := keys.forGen(t.Context(), "r")
	if err != nil || !set.Has("old") || g1 == 0 {
		t.Fatalf("first use = %v, generation %d, %v, want the old set at a generation above zero", set.Has("old"), g1, err)
	}
	if _, g, err := keys.forGen(t.Context(), "r"); err != nil || g != g1 {
		t.Fatalf("a second use is generation %d, %v, want %d: the cache did not change", g, err, g1)
	}

	// Throttled: inside the interval nothing is fetched, so nothing is new.
	serving.Store("new")
	if _, g, refetched, err := keys.refetchGen(t.Context(), "r"); err != nil || refetched || g != g1 {
		t.Fatalf("a throttled refetch = generation %d, refetched=%v, %v, want %d, false", g, refetched, err, g1)
	}

	// After the interval it fetches, and the generation is new.
	now = now.Add(2 * time.Minute)
	set, g2, refetched, err := keys.refetchGen(t.Context(), "r")
	if err != nil || !refetched || !set.Has("new") || g2 <= g1 {
		t.Fatalf("refetch = new=%v, generation %d, refetched=%v, %v, want the new set at a generation above %d", set.Has("new"), g2, refetched, err, g1)
	}

	// A fetch that fails keeps the set, and is not a new generation.
	failing.Store(true)
	now = now.Add(2 * time.Minute)
	if set, g3, refetched, err := keys.refetchGen(t.Context(), "r"); err == nil || !refetched || g3 != g2 || !set.Has("new") {
		t.Fatalf("a failed refetch = generation %d, refetched=%v, %v, want the set kept at %d and the failure reported", g3, refetched, err, g2)
	}
}
