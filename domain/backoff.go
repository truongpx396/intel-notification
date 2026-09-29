package domain

import "time"

// Backoff returns the delay before retrying after the given attempt, with full
// jitter (D10): uniform in [0, min(base·2^(attempt-1), ceiling)], never below a
// provider's retryAfter hint. rnd must return a value in [0, 1); inject a fixed
// one to test.
//
// Full jitter rather than plain exponential backoff because a provider outage
// fails every in-flight delivery at once: without jitter they all retry at the
// same computed instant, and the recovering provider takes the whole backlog in
// one hit.
func Backoff(attempt int, base, ceiling, retryAfter time.Duration, rnd func() float64) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	computed := ceiling
	// base·2^(attempt-1), stopping as soon as it would pass the ceiling so the
	// shift cannot overflow.
	if d := base; d > 0 {
		for i := 1; i < attempt && d < ceiling; i++ {
			d *= 2
		}
		computed = min(d, ceiling)
	}
	delay := time.Duration(rnd() * float64(computed))
	return max(delay, retryAfter)
}
