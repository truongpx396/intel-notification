package redis

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

const (
	// defaultLimitsTTL is how long a budget's ceilings are remembered. A Take is on the
	// path of every delivery and the ceilings are rows in another database, so they are
	// read once in a while and not once per send; a tenant's override is seen within this.
	defaultLimitsTTL = 30 * time.Second

	// maxCachedLimits bounds the ceilings remembered. A deployment with more budgets than
	// this empties the memory and reads them again, which costs reads, never correctness.
	maxCachedLimits = 1 << 16

	// counterGrace is how long a window's counter outlives the window. The window is part
	// of the key, so a window that has ended is never read again; the grace only keeps a
	// clock that runs a little behind Redis's from losing a count it still needs.
	counterGrace = time.Hour
)

// takeScript spends one unit in the hour and in the day, or neither. It runs in Redis as
// one step, so concurrent takers cannot spend what is not there, and a take that is
// refused counts nowhere. It returns {1 or 0, hour count, day count}.
var takeScript = goredis.NewScript(`
local h = tonumber(redis.call('GET', KEYS[1]) or '0')
local d = tonumber(redis.call('GET', KEYS[2]) or '0')
if h >= tonumber(ARGV[1]) or d >= tonumber(ARGV[2]) then
  return {0, h, d}
end
h = redis.call('INCR', KEYS[1])
if h == 1 then redis.call('EXPIRE', KEYS[1], ARGV[3]) end
d = redis.call('INCR', KEYS[2])
if d == 1 then redis.call('EXPIRE', KEYS[2], ARGV[4]) end
return {1, h, d}
`)

// Quota is the Redis ports.QuotaCounter (D5, D24): the hot count of how much of a
// (realm, tenant, channel) budget is spent, in the current UTC hour and the current UTC
// day. The ceilings are not its own; it reads them from a ports.QuotaLimits, the table
// that holds them.
//
// It is a fast path and fails open on quota, never on correctness. Redis forgetting
// everything is a budget with all its units back, and no error. A Redis that cannot be
// reached is an answer of "allowed" with the fault returned, so a caller that ignores the
// error still sends and one that reads it can say so. A table that cannot be read goes on
// with the ceilings last read for a budget already known, and allows a budget not known.
//
// A budget with no row has no ceiling and keeps no count.
type Quota struct {
	rdb     goredis.Cmdable
	src     ports.QuotaLimits
	clock   ports.Clock
	ttl     time.Duration
	onError func(op string, err error)

	mu    sync.Mutex
	cache map[domain.QuotaKey]cachedLimits
}

var _ ports.QuotaCounter = (*Quota)(nil)

type cachedLimits struct {
	limits  domain.QuotaLimits
	limited bool // false: nothing configures the key
	readAt  time.Time
}

// QuotaOption configures a Quota.
type QuotaOption func(*Quota)

// WithQuotaClock sets the clock that decides which window a call falls in. The default
// is the system clock.
func WithQuotaClock(c ports.Clock) QuotaOption {
	return func(q *Quota) {
		if c != nil {
			q.clock = c
		}
	}
}

// WithLimitsTTL sets how long a budget's ceilings are remembered before they are read
// again. The default is 30 seconds; zero or less reads them on every call.
func WithLimitsTTL(d time.Duration) QuotaOption { return func(q *Quota) { q.ttl = d } }

// WithQuotaErrorHandler reports each fault the counter survived, with the operation that
// met it: "limits", when the table could not be read and the ceilings last read were used.
// A fault the counter did not survive is returned to the caller instead.
func WithQuotaErrorHandler(f func(op string, err error)) QuotaOption {
	return func(q *Quota) {
		if f != nil {
			q.onError = f
		}
	}
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

// NewQuota returns a counter over rdb that reads its ceilings from limits.
func NewQuota(rdb goredis.Cmdable, limits ports.QuotaLimits, opts ...QuotaOption) *Quota {
	q := &Quota{rdb: rdb, src: limits, clock: systemClock{}, ttl: defaultLimitsTTL,
		onError: func(string, error) {}, cache: map[domain.QuotaKey]cachedLimits{}}
	for _, opt := range opts {
		opt(q)
	}
	return q
}

// limitsOf returns the ceilings of k, from memory while they are fresh.
func (q *Quota) limitsOf(ctx context.Context, k domain.QuotaKey, now time.Time) (domain.QuotaLimits, bool, error) {
	q.mu.Lock()
	c, have := q.cache[k]
	q.mu.Unlock()
	if have && now.Sub(c.readAt) < q.ttl {
		return c.limits, c.limited, nil
	}

	limits, limited, err := q.src.Limits(ctx, k)
	if err != nil {
		if have {
			// A table briefly away must not turn quotas off for a budget already known.
			q.onError("limits", err)
			return c.limits, c.limited, nil
		}
		return domain.QuotaLimits{}, false, fmt.Errorf("read the ceilings of %s on %s: %w", k.Tenant.ID, k.Channel, err)
	}
	if q.ttl > 0 {
		q.mu.Lock()
		if len(q.cache) >= maxCachedLimits {
			clear(q.cache)
		}
		q.cache[k] = cachedLimits{limits: limits, limited: limited, readAt: now}
		q.mu.Unlock()
	}
	return limits, limited, nil
}

// Peek implements ports.QuotaCounter. It reads the two counts and writes nothing.
func (q *Quota) Peek(ctx context.Context, k domain.QuotaKey) (bool, time.Time, error) {
	now := q.clock.Now()
	limits, limited, err := q.limitsOf(ctx, k, now)
	if err != nil {
		return false, time.Time{}, err
	}
	if !limited {
		return false, time.Time{}, nil
	}
	vals, err := q.rdb.MGet(ctx, domain.QuotaCounterKey(k, domain.QuotaHour, now), domain.QuotaCounterKey(k, domain.QuotaDay, now)).Result()
	if err != nil {
		return false, time.Time{}, fmt.Errorf("peek the quota of %s on %s: %w", k.Tenant.ID, k.Channel, err)
	}
	var counts [2]int
	for i, v := range vals {
		if v == nil {
			continue // not counted yet in this window
		}
		s, _ := v.(string)
		if counts[i], err = strconv.Atoi(s); err != nil {
			return false, time.Time{}, fmt.Errorf("peek the quota of %s on %s: counter %v: %w", k.Tenant.ID, k.Channel, v, err)
		}
	}
	exhausted, resetAt := limits.Status(now, counts[0], counts[1])
	return exhausted, resetAt, nil
}

// Take implements ports.QuotaCounter. A refused take reports when units return: the end
// of the longest window that is spent. An allowed one reports the end of the hour.
func (q *Quota) Take(ctx context.Context, k domain.QuotaKey) (bool, time.Time, error) {
	now := q.clock.Now()
	limits, limited, err := q.limitsOf(ctx, k, now)
	if err != nil {
		return true, time.Time{}, err
	}
	if !limited {
		return true, time.Time{}, nil
	}
	keys := []string{domain.QuotaCounterKey(k, domain.QuotaHour, now), domain.QuotaCounterKey(k, domain.QuotaDay, now)}
	res, err := takeScript.Run(ctx, q.rdb, keys, limits.PerHour, limits.PerDay, ttlSeconds(domain.QuotaHour, now), ttlSeconds(domain.QuotaDay, now)).Int64Slice()
	if err != nil || len(res) != 3 {
		if err == nil {
			err = fmt.Errorf("the take script answered %v, want three numbers", res)
		}
		return true, time.Time{}, fmt.Errorf("take from the quota of %s on %s: %w", k.Tenant.ID, k.Channel, err)
	}
	if res[0] == 1 {
		return true, domain.QuotaHour.End(now), nil
	}
	_, resetAt := limits.Status(now, int(res[1]), int(res[2]))
	return false, resetAt, nil
}

// ttlSeconds is how long a window's counter lives for, from now: the rest of the window
// and the grace, in whole seconds.
func ttlSeconds(w domain.QuotaWindow, now time.Time) int64 {
	return int64((w.End(now).Sub(now) + counterGrace) / time.Second)
}
