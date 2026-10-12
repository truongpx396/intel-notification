package notifytest

import (
	"context"
	"errors"
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

// PreCheck is an in-memory ports.PreCheck. It stands where Redis will, so the
// Notifier's contract suite can be written, and can fail on its assertions,
// before any Redis adapter exists (T023a); T023 later runs the same table against
// both, so the two cannot drift.
type PreCheck struct{}

var _ ports.PreCheck = (*PreCheck)(nil)

// NewPreCheck returns an empty pre-check that reads time from clock.
func NewPreCheck(clock ports.Clock) *PreCheck { return &PreCheck{} }

// Seen implements ports.PreCheck.
func (p *PreCheck) Seen(context.Context, string) (string, bool, error) {
	return "", false, nil
}

// Remember implements ports.PreCheck.
func (p *PreCheck) Remember(context.Context, string, string, time.Duration) error {
	return errors.New("not implemented")
}

// Writes lists every Remember the pre-check received, in order.
func (p *PreCheck) Writes() []PreCheckWrite { return nil }
