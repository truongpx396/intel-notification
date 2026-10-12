// Package redis is the Redis implementation of the driven ports that are hot and
// reconstructible: the pre-check today, the live stream and quota counters next.
// Redis here is a fast path, never the record: losing it costs speed, not
// correctness (D5, D16, D18).
package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/truongpx396/intel-notification/ports"
)

// PreCheck is the Redis ports.PreCheck (D18). An entry is the notification's id
// under the durable guard's key, written after the commit it reports, so it can
// only ever be a miss for work that was not committed.
//
// It is a fast path and stays one: a Redis fault is a miss. Seen answers "not seen"
// and Remember answers nil when Redis cannot be reached, and either says so to the
// error handler, so an operator can see it and Notify is never blocked by it
// (NR-004). What it does return as an error is a call that is wrong in itself, a
// ttl that is not positive, which Redis would read as an entry that never lapses.
type PreCheck struct {
	rdb     goredis.Cmdable
	maxTTL  time.Duration // 0: no cap
	onError func(op string, err error)
}

var _ ports.PreCheck = (*PreCheck)(nil)

// Option configures a PreCheck.
type Option func(*PreCheck)

// WithMaxTTL caps every entry's lifetime at d, which the composition root sets to
// the idempotency window: an entry that outlived the guard row it reports would
// answer "replay" for a key the guard would accept again (D18). A d that is not
// positive sets no cap.
func WithMaxTTL(d time.Duration) Option { return func(p *PreCheck) { p.maxTTL = d } }

// WithErrorHandler reports each Redis fault the pre-check turned into a miss, with
// the operation ("seen" or "remember") that met it. The default ignores them.
func WithErrorHandler(f func(op string, err error)) Option {
	return func(p *PreCheck) {
		if f != nil {
			p.onError = f
		}
	}
}

// NewPreCheck returns a pre-check over rdb.
func NewPreCheck(rdb goredis.Cmdable, opts ...Option) *PreCheck {
	p := &PreCheck{rdb: rdb, onError: func(string, error) {}}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// Seen implements ports.PreCheck. It reads and creates nothing.
func (p *PreCheck) Seen(ctx context.Context, key string) (string, bool, error) {
	id, err := p.rdb.Get(ctx, key).Result()
	switch {
	case err == nil:
		return id, true, nil
	case errors.Is(err, goredis.Nil):
		return "", false, nil
	default:
		p.onError("seen", err)
		return "", false, nil
	}
}

// Remember implements ports.PreCheck. The entry lives for ttl, or for the cap when
// that is shorter.
func (p *PreCheck) Remember(ctx context.Context, key, notificationID string, ttl time.Duration) error {
	if ttl <= 0 {
		return fmt.Errorf("redis pre-check: ttl %v is not positive, so the entry would never lapse", ttl)
	}
	if p.maxTTL > 0 && ttl > p.maxTTL {
		ttl = p.maxTTL
	}
	if err := p.rdb.Set(ctx, key, notificationID, ttl).Err(); err != nil {
		p.onError("remember", err)
	}
	return nil
}
