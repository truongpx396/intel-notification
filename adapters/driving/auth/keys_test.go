package auth_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/truongpx396/intel-notification/adapters/driving/auth"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/notifytest"
)

// T050, D31. A realm's keys are the host's, fetched when first needed and no more often than they must be.

// host is a host's JWKS endpoint: it serves whatever document it is told to, and counts the requests.
type host struct {
	srv  *httptest.Server
	mu   sync.Mutex
	docs map[string][]byte // by path
	code map[string]int
	hits map[string]*atomic.Int64
}

func newHost(t *testing.T) *host {
	t.Helper()
	h := &host{docs: map[string][]byte{}, code: map[string]int{}, hits: map[string]*atomic.Int64{}}
	h.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.mu.Lock()
		doc, code, counter := h.docs[r.URL.Path], h.code[r.URL.Path], h.hits[r.URL.Path]
		h.mu.Unlock()
		if counter == nil {
			http.NotFound(w, r)
			return
		}
		counter.Add(1)
		if code == 302 {
			http.Redirect(w, r, "/elsewhere", http.StatusFound)
			return
		}
		if code != 0 {
			w.WriteHeader(code)
			return
		}
		_, _ = w.Write(doc)
	}))
	t.Cleanup(h.srv.Close)
	return h
}

func (h *host) serve(path string, doc []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.docs[path] = doc
	if h.hits[path] == nil {
		h.hits[path] = new(atomic.Int64)
	}
}

func (h *host) fail(path string, code int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.code[path] = code
}

func (h *host) fetches(path string) int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	if c := h.hits[path]; c != nil {
		return c.Load()
	}
	return 0
}

func (h *host) url(path string) string { return h.srv.URL + path }

// clock is a time that moves when told.
type clock struct {
	mu  sync.Mutex
	now time.Time
}

func newClock() *clock { return &clock{now: time.Date(2031, 3, 1, 12, 0, 0, 0, time.UTC)} }

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func keysFor(t *testing.T, h *host, clk *clock, opts ...auth.KeysOption) *auth.Keys {
	t.Helper()
	all := append([]auth.KeysOption{auth.AllowInsecureHTTP(), auth.WithClock(clk.Now), auth.WithTTL(10 * time.Minute), auth.WithMinRefetch(time.Minute)}, opts...)
	k, err := auth.NewKeys(map[domain.Realm]string{"alpha": h.url("/alpha.json"), "beta": h.url("/beta.json")}, all...)
	if err != nil {
		t.Fatalf("NewKeys: %v", err)
	}
	return k
}

func TestKeysAreFetchedOnFirstUseAndKept(t *testing.T) {
	t.Parallel()
	h, clk := newHost(t), newClock()
	signer := notifytest.NewTokenSigner("k1")
	h.serve("/alpha.json", notifytest.JWKS(signer))
	k := keysFor(t, h, clk)
	if n := h.fetches("/alpha.json"); n != 0 {
		t.Fatalf("%d fetches at construction, want none: the host may not be up yet", n)
	}
	for range 3 {
		set, err := k.For(t.Context(), "alpha")
		if err != nil || !set.Has("k1") {
			t.Fatalf("For = %v keys, %v, want the host's key", set.Len(), err)
		}
	}
	if n := h.fetches("/alpha.json"); n != 1 {
		t.Fatalf("%d fetches for three uses, want 1", n)
	}
	if n := h.fetches("/beta.json"); n != 0 {
		t.Fatalf("%d fetches of another realm's set, want none", n)
	}
	clk.Advance(11 * time.Minute)
	if _, err := k.For(t.Context(), "alpha"); err != nil {
		t.Fatal(err)
	}
	if n := h.fetches("/alpha.json"); n != 2 {
		t.Fatalf("%d fetches after the ttl, want 2: a rotated key is picked up", n)
	}
}

func TestAHostWhoseEndpointBlinksDoesNotSignEveryoneOut(t *testing.T) {
	t.Parallel()
	h, clk := newHost(t), newClock()
	signer := notifytest.NewTokenSigner("k1")
	h.serve("/alpha.json", notifytest.JWKS(signer))
	k := keysFor(t, h, clk)
	if _, err := k.For(t.Context(), "alpha"); err != nil {
		t.Fatal(err)
	}
	h.fail("/alpha.json", http.StatusInternalServerError)
	clk.Advance(11 * time.Minute)
	set, err := k.For(t.Context(), "alpha")
	if err != nil || !set.Has("k1") {
		t.Fatalf("For during an outage = %d keys, %v, want the last good set", set.Len(), err)
	}
}

func TestARealmThatWasNeverFetchedHasNoKeysAndThatIsAnError(t *testing.T) {
	t.Parallel()
	for name, prep := range map[string]func(*host){
		"a server error":                func(h *host) { h.serve("/alpha.json", nil); h.fail("/alpha.json", 500) },
		"not found":                     func(h *host) {},
		"a document that is not a JWKS": func(h *host) { h.serve("/alpha.json", []byte("<html>")) },
		"a redirect, even to a good key set": func(h *host) {
			h.serve("/alpha.json", nil)
			h.fail("/alpha.json", 302)
			h.serve("/elsewhere", notifytest.JWKS(notifytest.NewTokenSigner("k1")))
		},
		"a valid document too large": func(h *host) {
			// Valid JSON with a usable key: only its size can refuse it.
			pad := strings.Repeat("x", 2<<20)
			doc := notifytest.JWKS(notifytest.NewTokenSigner("k1"))
			h.serve("/alpha.json", append(doc[:len(doc)-1], []byte(`,"pad":"`+pad+`"}`)...))
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h := newHost(t)
			prep(h)
			set, err := keysFor(t, h, newClock()).For(t.Context(), "alpha")
			if err == nil {
				t.Fatalf("For = %d keys, nil, want an error: a realm with no keys verifies no one, and an empty set must not look like success", set.Len())
			}
		})
	}
	t.Run("a realm nobody configured", func(t *testing.T) {
		t.Parallel()
		if _, err := keysFor(t, newHost(t), newClock()).For(t.Context(), "gamma"); err == nil {
			t.Fatal("For of an unconfigured realm = nil, want an error")
		}
	})
}

// T050: an unknown kid is refetched once, bounded.
func TestAnUnknownKeyIDCausesAtMostOneRefetchPerInterval(t *testing.T) {
	t.Parallel()
	h, clk := newHost(t), newClock()
	old, rotated := notifytest.NewTokenSigner("old"), notifytest.NewTokenSigner("new")
	h.serve("/alpha.json", notifytest.JWKS(old))
	k := keysFor(t, h, clk)
	if _, err := k.For(t.Context(), "alpha"); err != nil {
		t.Fatal(err)
	}
	// Just fetched: a refetch now would learn nothing, so none is made.
	if _, refetched, _ := k.Refetch(t.Context(), "alpha"); refetched || h.fetches("/alpha.json") != 1 {
		t.Fatalf("refetched=%v after %d fetches, want no refetch within the interval of the last fetch", refetched, h.fetches("/alpha.json"))
	}
	clk.Advance(2 * time.Minute)
	h.serve("/alpha.json", notifytest.JWKS(old, rotated))
	set, refetched, err := k.Refetch(t.Context(), "alpha")
	if err != nil || !refetched || !set.Has("new") {
		t.Fatalf("Refetch = %d keys, refetched=%v, %v, want the host's rotated key", set.Len(), refetched, err)
	}
	// A flood of invented key ids is one request per interval, not one each.
	for range 50 {
		if _, refetched, _ := k.Refetch(t.Context(), "alpha"); refetched {
			t.Fatal("a second refetch within the interval: a flood of tokens with invented key ids would flood the host")
		}
	}
	if n := h.fetches("/alpha.json"); n != 2 {
		t.Fatalf("%d fetches in all, want 2", n)
	}
	clk.Advance(time.Minute)
	if _, refetched, _ := k.Refetch(t.Context(), "alpha"); !refetched {
		t.Fatal("no refetch after the interval passed")
	}
}

// An endpoint that is down is not asked again by every request: the interval bounds retries too.
func TestAnEndpointThatIsDownIsNotHammered(t *testing.T) {
	t.Parallel()
	h, clk := newHost(t), newClock()
	h.serve("/alpha.json", nil)
	h.fail("/alpha.json", http.StatusInternalServerError)
	k := keysFor(t, h, clk)
	for range 20 {
		if _, err := k.For(t.Context(), "alpha"); err == nil {
			t.Fatal("For = nil with the endpoint down and no keys held, want an error")
		}
	}
	if n := h.fetches("/alpha.json"); n != 1 {
		t.Fatalf("%d requests to a down endpoint for 20 uses, want 1 within the interval", n)
	}
	clk.Advance(2 * time.Minute)
	h.fail("/alpha.json", 0)
	h.serve("/alpha.json", notifytest.JWKS(notifytest.NewTokenSigner("k1")))
	if set, err := k.For(t.Context(), "alpha"); err != nil || !set.Has("k1") {
		t.Fatalf("For after the endpoint recovered = %d keys, %v, want it found", set.Len(), err)
	}
}

// A host that answers with an endless body costs the service the bytes it is willing to read, and no more.
func TestAnEndlessAnswerIsNotReadToTheEnd(t *testing.T) {
	t.Parallel()
	var written atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		chunk := make([]byte, 64<<10)
		for range 4096 { // 256 MiB on offer
			n, err := w.Write(chunk)
			written.Add(int64(n))
			if err != nil || r.Context().Err() != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)
	k, err := auth.NewKeys(map[domain.Realm]string{"alpha": srv.URL + "/jwks.json"}, auth.AllowInsecureHTTP())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.For(t.Context(), "alpha"); err == nil {
		t.Fatal("For of an endless answer = nil, want an error")
	}
	if got := written.Load(); got > 64<<20 {
		t.Fatalf("the host got to write %d MiB before the client stopped reading, want a client that reads about a MiB and hangs up", got>>20)
	}
}

func TestColdUsesFetchOnce(t *testing.T) {
	t.Parallel()
	h := newHost(t)
	h.serve("/alpha.json", notifytest.JWKS(notifytest.NewTokenSigner("k1")))
	k := keysFor(t, h, newClock())
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := k.For(context.Background(), "alpha"); err != nil {
				t.Errorf("For: %v", err)
			}
		}()
	}
	wg.Wait()
	if n := h.fetches("/alpha.json"); n != 1 {
		t.Fatalf("%d fetches for 16 simultaneous first uses, want 1", n)
	}
}

func TestNewKeysRefusesAURLThatCannotWork(t *testing.T) {
	t.Parallel()
	for name, url := range map[string]string{
		"relative":                  "/jwks.json",
		"a scheme that is not http": "ftp://h.example/jwks.json",
		"no host":                   "https:///jwks.json",
		"plain http, not permitted": "http://h.example/jwks.json",
		"credentials in the URL":    "https://user:pass@h.example/jwks.json",
	} {
		if _, err := auth.NewKeys(map[domain.Realm]string{"alpha": url}); err == nil {
			t.Errorf("%s: NewKeys(%q) = nil, want an error at start and not on the first request", name, url)
		}
	}
	if _, err := auth.NewKeys(map[domain.Realm]string{"Not A Realm": "https://h.example/jwks.json"}); err == nil {
		t.Error("NewKeys with a realm that is no realm = nil, want an error")
	}
	if k, err := auth.NewKeys(map[domain.Realm]string{"beta": "https://h.example/b", "alpha": "https://h.example/a"}); err != nil || len(k.Realms()) != 2 || k.Realms()[0] != "alpha" {
		t.Errorf("NewKeys = %v, %v, want the realms, sorted", k.Realms(), err)
	}
}
