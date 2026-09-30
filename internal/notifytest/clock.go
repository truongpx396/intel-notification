// Package notifytest is the kit every suite above the store is written against,
// so a Red test can exist before the code it drives (T013a).
//
// It holds a fake clock, fault injection (a hold or a crash at any of the
// dispatcher's nine steps, and a commit that fails once), a probe that counts
// sends per idempotency key with a channel that reports to it, a channel
// registry, and in-memory fakes for the read-side ports a unit test needs:
// topics, preferences, quota, addresses and templates. With the integration tag,
// Env adds a private PostgreSQL database and the real queue, digest and
// maintenance ports over it.
//
// There is deliberately no fake Queue or Store. Their leases, fencing and
// row-level security only PostgreSQL can hold, and a fake that passed where the
// real one fails would be worse than none.
package notifytest

import (
	"sync"
	"time"

	"github.com/truongpx396/intel-notification/ports"
)

// Clock is a fake ports.Clock. It moves only when told, so a test that depends on
// time never sleeps and never flakes.
type Clock struct {
	mu  sync.Mutex
	now time.Time
}

var _ ports.Clock = (*Clock)(nil)

// NewClock returns a clock reading start.
func NewClock(start time.Time) *Clock { return &Clock{now: start} }

// Now returns the clock's time. Reading it does not move it.
func (c *Clock) Now() time.Time { return time.Time{} }

// Advance moves the clock forward by d and returns the new time. A negative d
// panics: a clock that runs backwards hides the bugs a test is there to find; use
// Set when a test really means to jump.
func (c *Clock) Advance(d time.Duration) time.Time { return time.Time{} }

// Set moves the clock to t, forwards or backwards.
func (c *Clock) Set(t time.Time) {}
