// Package redis is the Redis implementation of the driven ports that are hot and
// reconstructible: the pre-check today, the live stream and quota counters next.
// Redis here is a fast path, never the record: losing it costs speed, not
// correctness (D5, D16, D18).
package redis

import (
	"context"
	"errors"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/truongpx396/intel-notification/ports"
)

// PreCheck is the Redis ports.PreCheck (D18).
type PreCheck struct{}

var _ ports.PreCheck = (*PreCheck)(nil)

// Option configures a PreCheck.
type Option func(*PreCheck)

// WithMaxTTL caps every entry's lifetime at d.
func WithMaxTTL(d time.Duration) Option { return func(*PreCheck) {} }

// WithErrorHandler reports each Redis fault the pre-check turned into a miss.
func WithErrorHandler(f func(op string, err error)) Option { return func(*PreCheck) {} }

// NewPreCheck returns a pre-check over rdb.
func NewPreCheck(rdb goredis.Cmdable, opts ...Option) *PreCheck { return &PreCheck{} }

// Seen implements ports.PreCheck.
func (p *PreCheck) Seen(ctx context.Context, key string) (string, bool, error) {
	return "", false, errors.New("not implemented")
}

// Remember implements ports.PreCheck.
func (p *PreCheck) Remember(ctx context.Context, key, notificationID string, ttl time.Duration) error {
	return errors.New("not implemented")
}
