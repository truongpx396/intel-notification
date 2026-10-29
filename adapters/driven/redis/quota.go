package redis

import (
	"context"
	"errors"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// Quota is the Redis ports.QuotaCounter.
type Quota struct{}

var _ ports.QuotaCounter = (*Quota)(nil)

// QuotaOption configures a Quota.
type QuotaOption func(*Quota)

// WithQuotaClock sets the clock that decides which window a call falls in.
func WithQuotaClock(ports.Clock) QuotaOption { return func(*Quota) {} }

// WithLimitsTTL sets how long a budget's ceilings are remembered before they are read again.
func WithLimitsTTL(time.Duration) QuotaOption { return func(*Quota) {} }

// WithQuotaErrorHandler reports each fault the counter survived.
func WithQuotaErrorHandler(func(op string, err error)) QuotaOption { return func(*Quota) {} }

// NewQuota returns a counter over rdb that reads its ceilings from limits.
func NewQuota(goredis.Cmdable, ports.QuotaLimits, ...QuotaOption) *Quota { return &Quota{} }

// Peek implements ports.QuotaCounter.
func (*Quota) Peek(context.Context, domain.QuotaKey) (bool, time.Time, error) {
	return false, time.Time{}, errors.New("not implemented")
}

// Take implements ports.QuotaCounter.
func (*Quota) Take(context.Context, domain.QuotaKey) (bool, time.Time, error) {
	return false, time.Time{}, errors.New("not implemented")
}
