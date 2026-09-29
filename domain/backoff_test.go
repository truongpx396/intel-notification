package domain

import (
	"math/rand/v2"
	"testing"
	"time"
)

const (
	base    = 10 * time.Second
	ceiling = time.Hour
)

func fixed(v float64) func() float64 { return func() float64 { return v } }

func TestBackoff(t *testing.T) {
	t.Parallel()
	top := fixed(0.999999999) // the top of the jitter window
	cases := []struct {
		name       string
		attempt    int
		retryAfter time.Duration
		rnd        func() float64
		want       time.Duration
		tolerance  time.Duration
	}{
		{"first retry tops out at base", 1, 0, top, 10 * time.Second, time.Second},
		{"doubles each attempt", 3, 0, top, 40 * time.Second, time.Second},
		{"still doubling at attempt 9", 9, 0, top, 2560 * time.Second, time.Second},
		{"capped at the ceiling", 10, 0, top, time.Hour, time.Second},
		{"no overflow far past the ceiling", 100, 0, top, time.Hour, time.Second},
		{"attempt 0 is the first attempt", 0, 0, top, 10 * time.Second, time.Second},
		{"full jitter reaches zero", 5, 0, fixed(0), 0, 0},
		{"jitter is uniform in the window", 3, 0, fixed(0.5), 20 * time.Second, 0},
		{"a provider's Retry-After is a floor", 1, 90 * time.Second, fixed(0.5), 90 * time.Second, 0},
		{"a smaller Retry-After does not shorten the delay", 3, time.Second, fixed(0.5), 20 * time.Second, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := Backoff(tc.attempt, base, ceiling, tc.retryAfter, tc.rnd)
			if got < tc.want-tc.tolerance || got > tc.want {
				t.Fatalf("Backoff(%d) = %v, want %v (−%v)", tc.attempt, got, tc.want, tc.tolerance)
			}
		})
	}
}

// A provider outage fails every in-flight delivery at once. Their retries must
// spread across the window rather than land together (D10).
func TestBackoffSpreadsAHerd(t *testing.T) {
	t.Parallel()
	r := rand.New(rand.NewPCG(1, 2))
	lo, hi := time.Duration(1<<62), time.Duration(0)
	for range 1000 {
		d := Backoff(4, base, ceiling, 0, r.Float64)
		lo, hi = min(lo, d), max(hi, d)
	}
	window := 80 * time.Second
	if lo > window/10 || hi < window*9/10 {
		t.Fatalf("1000 retries spread only over [%v, %v] of an %v window", lo, hi, window)
	}
}
