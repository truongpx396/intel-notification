package notifytest

import (
	"testing"
	"time"

	"github.com/truongpx396/intel-notification/ports"
)

// PreCheckFactory returns an empty ports.PreCheck for t alone: a fresh fake, or
// an adapter over a Redis database no other test uses.
type PreCheckFactory func(t *testing.T) ports.PreCheck

// PreCheckContract is the table every ports.PreCheck runs: the in-memory fake in
// this package and the Redis adapter alike, so neither can drift from what the
// other decided where the contract leaves room (T013a, T023). It asserts what the
// port promises through the port: a remembered key is seen with its id, Seen only
// reads, a key is the key it was given, and an entry that would never lapse is
// refused. A lapse needs a clock the test can move, so it is the fake's own test;
// Redis's is shown from the TTL it stores.
//
// Every case builds its own pre-check with newPreCheck.
func PreCheckContract(t *testing.T, newPreCheck PreCheckFactory) {
	t.Helper()

	t.Run("remember then seen returns the notification id", func(t *testing.T) {
		t.Parallel()
		p := newPreCheck(t)
		if err := p.Remember(t.Context(), "k1", "n1", time.Hour); err != nil {
			t.Fatalf("Remember: %v", err)
		}
		id, ok, err := p.Seen(t.Context(), "k1")
		if err != nil || !ok || id != "n1" {
			t.Fatalf("Seen = %q, %v, %v, want n1, true, nil", id, ok, err)
		}
	})

	t.Run("an unknown key is a miss, never an error", func(t *testing.T) {
		t.Parallel()
		p := newPreCheck(t)
		id, ok, err := p.Seen(t.Context(), "nobody-remembered-this")
		if err != nil || ok || id != "" {
			t.Fatalf("Seen = %q, %v, %v, want a miss: the pre-check can only miss (D18)", id, ok, err)
		}
	})

	t.Run("Seen creates nothing", func(t *testing.T) {
		t.Parallel()
		p := newPreCheck(t)
		for i := range 3 {
			if id, ok, err := p.Seen(t.Context(), "k1"); err != nil || ok {
				t.Fatalf("Seen #%d = %q, %v, %v, want a miss: a read that stored the key made the next Notify a replay", i+1, id, ok, err)
			}
		}
	})

	t.Run("a key is stored exactly as given", func(t *testing.T) {
		t.Parallel()
		p := newPreCheck(t)
		// Each key differs from another only in case, padding or punctuation. The
		// pre-check shares the durable guard's key space, so it may not fold any of it.
		keys := []string{"k", "K", "k ", " k", "k:", "notify:applied:aisat:ab", "notify:applied:AISAT:ab", "ключ"}
		for i, k := range keys {
			if err := p.Remember(t.Context(), k, "n"+string(rune('A'+i)), time.Hour); err != nil {
				t.Fatalf("Remember(%q): %v", k, err)
			}
		}
		for i, k := range keys {
			want := "n" + string(rune('A'+i))
			if id, ok, err := p.Seen(t.Context(), k); err != nil || !ok || id != want {
				t.Fatalf("Seen(%q) = %q, %v, %v, want %q: keys %q collided", k, id, ok, err, want, keys)
			}
		}
		if _, ok, _ := p.Seen(t.Context(), "k  "); ok {
			t.Fatal(`Seen("k  ") hit an entry stored under a different key`)
		}
	})

	t.Run("remembering one key makes no other key seen", func(t *testing.T) {
		t.Parallel()
		p := newPreCheck(t)
		if err := p.Remember(t.Context(), "k1", "n1", time.Hour); err != nil {
			t.Fatalf("Remember: %v", err)
		}
		if id, ok, _ := p.Seen(t.Context(), "k2"); ok {
			t.Fatalf("Seen(k2) = %q, true after remembering k1", id)
		}
	})

	t.Run("a ttl that is not positive is refused and stores nothing", func(t *testing.T) {
		t.Parallel()
		for _, ttl := range []time.Duration{0, -time.Second} {
			t.Run(ttl.String(), func(t *testing.T) {
				t.Parallel()
				p := newPreCheck(t)
				// A zero TTL is "no expiry" to Redis. An entry that never lapses would
				// outlive the idempotency window and drop a notification the guard
				// would accept (D18).
				if err := p.Remember(t.Context(), "k1", "n1", ttl); err == nil {
					t.Fatalf("Remember with ttl %v succeeded, want an error", ttl)
				}
				if id, ok, _ := p.Seen(t.Context(), "k1"); ok {
					t.Fatalf("a refused Remember left an entry: Seen = %q, true", id)
				}
			})
		}
	})
}
