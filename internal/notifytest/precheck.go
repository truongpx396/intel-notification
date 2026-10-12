package notifytest

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/truongpx396/intel-notification/ports"
)

// PreCheckWrite is one Remember a [PreCheck] received: what it was asked to
// store, and the instant on its clock that it happened. A test reads these to see
// whether a write came before or after the commit it was meant to follow (D18).
type PreCheckWrite struct {
	Key            string
	NotificationID string
	TTL            time.Duration
	At             time.Time
}

type precheckEntry struct {
	id      string
	expires time.Time
}

// PreCheck is an in-memory ports.PreCheck. It stands where Redis will, so the
// Notifier's contract suite can be written, and can fail on its assertions,
// before any Redis adapter exists (T023a); T023 later runs the same table against
// both, so the two cannot drift.
//
// Seen only reads. Remember writes, an entry lapses once its ttl has passed on
// the clock the pre-check was built with, and each Remember is kept with the
// instant it happened. A key is whatever string it is given.
type PreCheck struct {
	clock ports.Clock

	mu      sync.Mutex
	entries map[string]precheckEntry
	writes  []PreCheckWrite
}

var _ ports.PreCheck = (*PreCheck)(nil)

// NewPreCheck returns an empty pre-check that reads time from clock.
func NewPreCheck(clock ports.Clock) *PreCheck {
	return &PreCheck{clock: clock, entries: map[string]precheckEntry{}}
}

// Seen implements ports.PreCheck. It creates nothing and removes nothing: an
// entry past its ttl is ignored, not deleted, so a read never changes state.
func (p *PreCheck) Seen(_ context.Context, key string) (string, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.entries[key]
	if !ok || !p.clock.Now().Before(e.expires) {
		return "", false, nil
	}
	return e.id, true, nil
}

// Remember implements ports.PreCheck. A ttl that is not positive is refused: it
// would be an entry that never lapses, and one that outlives the idempotency
// window could drop a notification the durable guard would accept (D18).
func (p *PreCheck) Remember(_ context.Context, key, notificationID string, ttl time.Duration) error {
	if ttl <= 0 {
		return fmt.Errorf("notifytest: pre-check ttl %v is not positive", ttl)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.clock.Now()
	p.entries[key] = precheckEntry{id: notificationID, expires: now.Add(ttl)}
	p.writes = append(p.writes, PreCheckWrite{Key: key, NotificationID: notificationID, TTL: ttl, At: now})
	return nil
}

// Writes lists every Remember the pre-check stored, in order. A read and a
// refused write are not among them.
func (p *PreCheck) Writes() []PreCheckWrite {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.writes)
}
