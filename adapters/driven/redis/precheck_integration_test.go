//go:build integration

package redis_test

import (
	"os"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/truongpx396/intel-notification/adapters/driven/redis"
	"github.com/truongpx396/intel-notification/internal/notifytest"
	"github.com/truongpx396/intel-notification/internal/redistest"
	"github.com/truongpx396/intel-notification/ports"
)

func TestMain(m *testing.M) { os.Exit(redistest.Main(m)) }

// newPreCheck is a pre-check over a Redis database of the test's own.
func newPreCheck(t *testing.T, opts ...redis.Option) (*redis.PreCheck, *goredis.Client) {
	t.Helper()
	db := redistest.New(t)
	return redis.NewPreCheck(db.Client, opts...), db.Client
}

// The Redis adapter runs the same table as the in-memory fake (T023), so what the
// fake decides where the port is silent is decided for both.
func TestPreCheckRunsTheContract(t *testing.T) {
	t.Parallel()
	notifytest.PreCheckContract(t, func(t *testing.T) ports.PreCheck {
		t.Helper()
		p, _ := newPreCheck(t)
		return p
	})
}

// The key is stored as the caller built it: the pre-check shares the durable
// guard's key space, and a prefix or a fold would put the two out of step (D18).
func TestPreCheckStoresTheKeyAsGivenAndHoldsTheID(t *testing.T) {
	t.Parallel()
	p, rdb := newPreCheck(t)
	const key = "notify:applied:aisat:2006f991ca0e66c48051540129023c7fc5c3f7f77455b4f8a3b70bade98c8df1"
	if err := p.Remember(t.Context(), key, "n-1", time.Hour); err != nil {
		t.Fatalf("Remember: %v", err)
	}
	got, err := rdb.Get(t.Context(), key).Result()
	if err != nil || got != "n-1" {
		t.Fatalf("GET %s = %q, %v, want the notification id", key, got, err)
	}
	if keys, _ := rdb.Keys(t.Context(), "*").Result(); len(keys) != 1 || keys[0] != key {
		t.Fatalf("keys in Redis = %q, want only %q", keys, key)
	}
}

// An entry lives as long as it was asked to, and never longer than the idempotency
// window it is capped at: an entry that outlived the guard would drop a
// notification the guard would accept (D18).
func TestPreCheckTTLNeverExceedsTheWindow(t *testing.T) {
	t.Parallel()
	const window = time.Hour
	cases := []struct {
		name     string
		ttl      time.Duration
		wantUpTo time.Duration
		wantOver time.Duration
	}{
		{"a ttl under the window is kept", 30 * time.Minute, 30 * time.Minute, 29 * time.Minute},
		{"a ttl equal to the window is kept", window, window, window - time.Minute},
		{"a ttl above the window is clamped to it", 48 * time.Hour, window, window - time.Minute},
		{"a ttl of milliseconds is kept", 1500 * time.Millisecond, 1500 * time.Millisecond, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p, rdb := newPreCheck(t, redis.WithMaxTTL(window))
			if err := p.Remember(t.Context(), "k", "n-1", tc.ttl); err != nil {
				t.Fatalf("Remember: %v", err)
			}
			got, err := rdb.PTTL(t.Context(), "k").Result()
			if err != nil || got <= tc.wantOver || got > tc.wantUpTo {
				t.Fatalf("PTTL = %v, %v, want in (%v, %v]", got, err, tc.wantOver, tc.wantUpTo)
			}
		})
	}
}

// With no cap configured an entry keeps the ttl it was given: the cap is the
// composition root's to set from the idempotency window.
func TestPreCheckWithNoCapKeepsTheTTLItWasGiven(t *testing.T) {
	t.Parallel()
	p, rdb := newPreCheck(t)
	if err := p.Remember(t.Context(), "k", "n-1", 48*time.Hour); err != nil {
		t.Fatal(err)
	}
	if got, _ := rdb.PTTL(t.Context(), "k").Result(); got <= 47*time.Hour {
		t.Fatalf("PTTL = %v, want about 48h", got)
	}
}

// A Redis outage is a miss, and a write that could not be made is a write nobody
// needed: neither is an error that blocks Notify (NR-004, D18). The fault is
// reported to the handler, so an operator can see it.
func TestPreCheckRedisOutageIsAMissNotAnError(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var ops []string
	// Nothing listens on port 1, so every command is refused at once.
	dead := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1", MaxRetries: -1, DialTimeout: time.Second})
	t.Cleanup(func() { _ = dead.Close() })
	p := redis.NewPreCheck(dead, redis.WithErrorHandler(func(op string, err error) {
		mu.Lock()
		defer mu.Unlock()
		ops = append(ops, op)
		if err == nil {
			t.Error("the error handler was called with a nil error")
		}
	}))

	id, ok, err := p.Seen(t.Context(), "k")
	if err != nil || ok || id != "" {
		t.Fatalf("Seen with Redis down = %q, %v, %v, want a miss and no error", id, ok, err)
	}
	if err := p.Remember(t.Context(), "k", "n-1", time.Hour); err != nil {
		t.Fatalf("Remember with Redis down = %v, want nil: the entry only saves a later round trip", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(ops) != 2 || ops[0] != "seen" || ops[1] != "remember" {
		t.Fatalf("the handler heard %v, want [seen remember]", ops)
	}
}

// A refused ttl is a programming error, not an outage: it is returned, and nothing
// is stored, so Redis never holds an entry with no expiry.
func TestPreCheckRefusesATTLThatWouldNeverLapse(t *testing.T) {
	t.Parallel()
	p, rdb := newPreCheck(t)
	// A control first, so a pre-check that stores nothing cannot pass.
	if err := p.Remember(t.Context(), "control", "n-0", time.Hour); err != nil {
		t.Fatalf("Remember with a good ttl: %v", err)
	}
	for _, ttl := range []time.Duration{0, -time.Second} {
		if err := p.Remember(t.Context(), "k", "n-1", ttl); err == nil {
			t.Fatalf("Remember with ttl %v succeeded", ttl)
		}
	}
	if keys, _ := rdb.Keys(t.Context(), "*").Result(); len(keys) != 1 || keys[0] != "control" {
		t.Fatalf("keys in Redis = %q, want only the control: a refused write stored something", keys)
	}
}
