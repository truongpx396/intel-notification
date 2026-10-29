//go:build integration

package redis_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/truongpx396/intel-notification/adapters/driven/redis"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/notifytest"
	"github.com/truongpx396/intel-notification/internal/redistest"
	"github.com/truongpx396/intel-notification/ports"
)

// limitSource is the ceilings the counter reads, as a table a test edits, and a count of
// how often it was asked.
type limitSource struct {
	mu     sync.Mutex
	limits map[domain.QuotaKey]domain.QuotaLimits
	err    error
	reads  int
}

var _ ports.QuotaLimits = (*limitSource)(nil)

func newLimitSource() *limitSource {
	return &limitSource{limits: map[domain.QuotaKey]domain.QuotaLimits{}}
}

func (s *limitSource) Limits(_ context.Context, k domain.QuotaKey) (domain.QuotaLimits, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads++
	if s.err != nil {
		return domain.QuotaLimits{}, false, s.err
	}
	l, ok := s.limits[k]
	return l, ok, nil
}

func (s *limitSource) set(k domain.QuotaKey, l domain.QuotaLimits) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.limits[k] = l
}

func (s *limitSource) fail(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
}

func (s *limitSource) readCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reads
}

type quotaFx struct {
	q     *redis.Quota
	src   *limitSource
	clock *notifytest.Clock
	rdb   *goredis.Client
	errs  *opLog
}

type opLog struct {
	mu  sync.Mutex
	ops []string
}

func (l *opLog) add(op string, _ error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ops = append(l.ops, op)
}

func (l *opLog) all() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.ops...)
}

// newQuotaFx is a counter over a Redis database of the test's own, on a clock that
// starts at the Env's epoch (2031-03-01T12:00:00Z), with ceilings that are never cached
// unless the test passes a ttl.
func newQuotaFx(t *testing.T, opts ...redis.QuotaOption) *quotaFx {
	t.Helper()
	db := redistest.New(t)
	fx := &quotaFx{src: newLimitSource(), clock: notifytest.NewClock(notifytest.Epoch), rdb: db.Client, errs: &opLog{}}
	opts = append([]redis.QuotaOption{redis.WithQuotaClock(fx.clock), redis.WithQuotaErrorHandler(fx.errs.add)}, opts...)
	fx.q = redis.NewQuota(db.Client, fx.src, opts...)
	return fx
}

var (
	w1Email = domain.QuotaKey{Realm: "aisat", Tenant: domain.Tenant{Kind: "workspace", ID: "w1"}, Channel: "email"}
	hour1   = notifytest.Epoch.Add(time.Hour) // the end of the epoch's hour
)

func (fx *quotaFx) take(t *testing.T, k domain.QuotaKey) (bool, time.Time) {
	t.Helper()
	allowed, resetAt, err := fx.q.Take(t.Context(), k)
	if err != nil {
		t.Fatalf("Take: %v", err)
	}
	return allowed, resetAt
}

// redisWorld is the counter as a notifytest.QuotaWorld: a budget of n is n an hour, with
// a day so large it never binds, so the budget resets at the end of the hour.
type redisWorld struct{ *quotaFx }

func (w redisWorld) Grant(k domain.QuotaKey, n int) time.Time {
	w.src.set(k, domain.QuotaLimits{PerHour: n, PerDay: 1 << 30})
	return domain.QuotaHour.End(w.clock.Now())
}

func (w redisWorld) Lose() { w.rdb.FlushDB(context.Background()) }

func (w redisWorld) Peek(ctx context.Context, k domain.QuotaKey) (bool, time.Time, error) {
	return w.q.Peek(ctx, k)
}

func (w redisWorld) Take(ctx context.Context, k domain.QuotaKey) (bool, time.Time, error) {
	return w.q.Take(ctx, k)
}

// The Redis counter runs the same table as the in-memory fake, so what the fake decides
// where the port is silent is decided for both (T036).
func TestQuotaRunsTheContract(t *testing.T) {
	t.Parallel()
	notifytest.QuotaContract(t, func(t *testing.T) notifytest.QuotaWorld {
		t.Helper()
		return redisWorld{newQuotaFx(t, redis.WithLimitsTTL(0))}
	})
}

// ---------------------------------------------------------------- windows --

// The hour's count starts again at the hour, so a budget spent at noon is back at one.
func TestTheHourlyBudgetReturnsAtTheNextHour(t *testing.T) {
	t.Parallel()
	fx := newQuotaFx(t)
	fx.src.set(w1Email, domain.QuotaLimits{PerHour: 2, PerDay: 100})

	for range 2 {
		fx.take(t, w1Email)
	}
	allowed, resetAt := fx.take(t, w1Email)
	if allowed || !resetAt.Equal(hour1) {
		t.Fatalf("the third Take = %v, resetting at %v, want refused until %v: the end of the hour", allowed, resetAt, hour1)
	}

	fx.clock.Set(hour1.Add(-time.Nanosecond))
	if allowed, _ := fx.take(t, w1Email); allowed {
		t.Fatal("a Take in the last instant of the hour was allowed")
	}
	fx.clock.Set(hour1)
	for i := range 2 {
		if allowed, _ := fx.take(t, w1Email); !allowed {
			t.Fatalf("Take %d in the next hour was refused: the budget did not return at %v", i+1, hour1)
		}
	}
	if allowed, _ := fx.take(t, w1Email); allowed {
		t.Fatal("the next hour allowed a third Take: the ceiling is per hour")
	}
}

// The day's ceiling binds when the hour has room, and its reset is the day's: the hour
// returning gives nothing back, so a delivery deferred to the next hour would be deferred
// again and again until midnight.
func TestTheDailyBudgetBindsAndResetsAtMidnight(t *testing.T) {
	t.Parallel()
	fx := newQuotaFx(t)
	fx.src.set(w1Email, domain.QuotaLimits{PerHour: 5, PerDay: 6})
	midnight := time.Date(2031, 3, 2, 0, 0, 0, 0, time.UTC)

	for range 5 {
		fx.take(t, w1Email)
	}
	fx.clock.Set(hour1)
	if allowed, _ := fx.take(t, w1Email); !allowed {
		t.Fatal("the sixth Take was refused with a unit left in the day")
	}
	allowed, resetAt := fx.take(t, w1Email)
	if allowed || !resetAt.Equal(midnight) {
		t.Fatalf("the seventh Take = %v, resetting at %v, want refused until midnight %v: the hour has room, the day has none", allowed, resetAt, midnight)
	}
	if exhausted, got, _ := fx.q.Peek(t.Context(), w1Email); !exhausted || !got.Equal(midnight) {
		t.Fatalf("Peek = %v at %v, want exhausted until %v", exhausted, got, midnight)
	}

	fx.clock.Set(midnight)
	if allowed, _ := fx.take(t, w1Email); !allowed {
		t.Fatal("the first Take of the next day was refused")
	}
}

// A refused take counts nowhere: not in the hour, and not in the day. A refusal that
// counted in the day would spend it on deliveries that were never made.
func TestARefusedTakeSpendsNeitherWindow(t *testing.T) {
	t.Parallel()
	fx := newQuotaFx(t)
	fx.src.set(w1Email, domain.QuotaLimits{PerHour: 2, PerDay: 3})
	for range 2 {
		fx.take(t, w1Email)
	}
	for range 10 {
		fx.take(t, w1Email) // refused
	}
	fx.clock.Set(hour1)
	if allowed, _ := fx.take(t, w1Email); !allowed {
		t.Fatal("the third unit of the day was refused: the refusals were counted against it")
	}
	if allowed, _ := fx.take(t, w1Email); allowed {
		t.Fatal("a fourth unit of the day was allowed")
	}
}

// A ceiling lowered under what has already been counted is spent, not unlimited.
func TestACeilingLoweredUnderTheCountIsSpent(t *testing.T) {
	t.Parallel()
	fx := newQuotaFx(t, redis.WithLimitsTTL(0))
	fx.src.set(w1Email, domain.QuotaLimits{PerHour: 10, PerDay: 100})
	for range 5 {
		fx.take(t, w1Email)
	}
	fx.src.set(w1Email, domain.QuotaLimits{PerHour: 3, PerDay: 100})
	if allowed, _ := fx.take(t, w1Email); allowed {
		t.Fatal("a Take was allowed with five counted against a ceiling of three")
	}
}

// ------------------------------------------------------------------- keys --

// A budget is two keys on one hash tag, each expiring after its window, so a window that
// has ended leaves nothing behind for long and nothing for a reader to find.
func TestTheCountersAreOnePairOfKeysThatExpire(t *testing.T) {
	t.Parallel()
	fx := newQuotaFx(t)
	fx.src.set(w1Email, domain.QuotaLimits{PerHour: 10, PerDay: 100})
	now := fx.clock.Now()
	fx.take(t, w1Email)

	hourKey := domain.QuotaCounterKey(w1Email, domain.QuotaHour, now)
	dayKey := domain.QuotaCounterKey(w1Email, domain.QuotaDay, now)
	keys, err := fx.rdb.Keys(t.Context(), "*").Result()
	if err != nil || len(keys) != 2 {
		t.Fatalf("keys in Redis = %q, %v, want the hour's and the day's and nothing else", keys, err)
	}
	for key, window := range map[string]domain.QuotaWindow{hourKey: domain.QuotaHour, dayKey: domain.QuotaDay} {
		if n, err := fx.rdb.Get(t.Context(), key).Int(); err != nil || n != 1 {
			t.Fatalf("GET %s = %d, %v, want 1", key, n, err)
		}
		ttl, err := fx.rdb.TTL(t.Context(), key).Result()
		remaining := window.End(now).Sub(now)
		// The counter outlives its window by a grace, so a clock a little behind Redis's
		// does not lose a count it still needs; and it does not outlive it by much.
		if err != nil || ttl <= remaining || ttl > remaining+2*time.Hour {
			t.Fatalf("TTL of %s = %v, %v, want more than the %v left in its window, and a bounded more", key, ttl, err, remaining)
		}
	}
}

// A Peek reads the counts and writes nothing: no key, and no expiry.
func TestPeekCreatesNoKey(t *testing.T) {
	t.Parallel()
	fx := newQuotaFx(t)
	fx.src.set(w1Email, domain.QuotaLimits{PerHour: 10, PerDay: 100})
	for range 3 {
		if exhausted, _, err := fx.q.Peek(t.Context(), w1Email); exhausted || err != nil {
			t.Fatalf("Peek = %v, %v", exhausted, err)
		}
	}
	if keys, _ := fx.rdb.Keys(t.Context(), "*").Result(); len(keys) != 0 {
		t.Fatalf("keys in Redis after peeks = %q, want none", keys)
	}
}

// An unlimited budget costs Redis nothing: there is no count to keep.
func TestAnUnlimitedBudgetWritesNothing(t *testing.T) {
	t.Parallel()
	fx := newQuotaFx(t)
	for range 3 {
		fx.take(t, w1Email)
	}
	if keys, _ := fx.rdb.Keys(t.Context(), "*").Result(); len(keys) != 0 {
		t.Fatalf("keys in Redis for an unlimited budget = %q, want none", keys)
	}
}

// -------------------------------------------------------------- ceilings --

// The ceilings are read once and remembered for the ttl, because a Take is on the hot
// path of every delivery and the table is in another database; a change is seen when the
// ttl has passed.
func TestCeilingsAreReadOncePerTTL(t *testing.T) {
	t.Parallel()
	fx := newQuotaFx(t, redis.WithLimitsTTL(time.Minute))
	fx.src.set(w1Email, domain.QuotaLimits{PerHour: 2, PerDay: 100})

	for range 5 {
		fx.q.Peek(t.Context(), w1Email) //nolint:errcheck // the reads are counted
		fx.q.Take(t.Context(), w1Email) //nolint:errcheck
	}
	if got := fx.src.readCount(); got != 1 {
		t.Fatalf("the ceilings were read %d times in a minute, want once", got)
	}

	// A tenant's own row now overrides the default: the counter sees it after the ttl.
	fx.src.set(w1Email, domain.QuotaLimits{PerHour: 50, PerDay: 100})
	if allowed, _ := fx.take(t, w1Email); allowed {
		t.Fatal("a Take was allowed within the ttl of the old ceiling, which was spent")
	}
	fx.clock.Advance(time.Minute)
	if allowed, _ := fx.take(t, w1Email); !allowed {
		t.Fatal("a Take was refused after the ttl, with the new ceiling of fifty")
	}
	if got := fx.src.readCount(); got != 2 {
		t.Fatalf("the ceilings were read %d times, want 2: once, and again after the ttl", got)
	}
}

// A key nothing configures is remembered as unlimited too, so a tenant with no row does
// not cost a read of the table per delivery.
func TestAnUnconfiguredKeyIsRememberedAsUnlimited(t *testing.T) {
	t.Parallel()
	fx := newQuotaFx(t, redis.WithLimitsTTL(time.Minute))
	for range 5 {
		fx.take(t, w1Email)
	}
	if got := fx.src.readCount(); got != 1 {
		t.Fatalf("the ceilings of an unconfigured key were read %d times, want once", got)
	}
}

// A table that cannot be read does not turn quotas off for a budget already known: the
// counter goes on with the last ceilings it read, and says so. With nothing known, a
// Take is allowed, because quota fails open (D5), and the fault is returned so it is seen.
func TestAnUnreadableTableKeepsTheLastCeilingsAndOtherwiseFailsOpen(t *testing.T) {
	t.Parallel()
	fx := newQuotaFx(t, redis.WithLimitsTTL(time.Minute))
	known := w1Email
	unknown := domain.QuotaKey{Realm: "aisat", Tenant: domain.Tenant{Kind: "workspace", ID: "w2"}, Channel: "email"}
	fx.src.set(known, domain.QuotaLimits{PerHour: 1, PerDay: 10})
	fx.take(t, known)

	boom := errors.New("the table is away")
	fx.src.fail(boom)
	fx.clock.Advance(2 * time.Minute) // the cached ceilings are stale

	if allowed, _, err := fx.q.Take(t.Context(), known); allowed || err != nil {
		t.Fatalf("Take of a known, spent budget = %v, %v, want refused on the last ceilings read, with no error to the caller", allowed, err)
	}
	if ops := fx.errs.all(); len(ops) == 0 {
		t.Fatal("the stale ceilings were used without saying so")
	}

	allowed, resetAt, err := fx.q.Take(t.Context(), unknown)
	if !allowed || !resetAt.IsZero() || !errors.Is(err, boom) {
		t.Fatalf("Take of an unknown budget = %v, %v, %v, want allowed, no reset, and the fault", allowed, resetAt, err)
	}
	exhausted, resetAt, err := fx.q.Peek(t.Context(), unknown)
	if exhausted || !resetAt.IsZero() || !errors.Is(err, boom) {
		t.Fatalf("Peek of an unknown budget = %v, %v, %v, want not exhausted and the fault", exhausted, resetAt, err)
	}
}

// ---------------------------------------------------------------- failure --

// A Redis that cannot be reached is a budget with units left, and the fault is returned
// so the caller can report it: the dispatcher sends the delivery and says so (D5).
func TestAnUnreachableRedisFailsOpenAndSaysSo(t *testing.T) {
	t.Parallel()
	dead := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1", DialTimeout: 100 * time.Millisecond, MaxRetries: -1})
	t.Cleanup(func() { _ = dead.Close() })
	src := newLimitSource()
	src.set(w1Email, domain.QuotaLimits{PerHour: 1, PerDay: 1})
	q := redis.NewQuota(dead, src, redis.WithQuotaClock(notifytest.NewClock(notifytest.Epoch)))

	allowed, _, err := q.Take(t.Context(), w1Email)
	if !allowed || err == nil {
		t.Fatalf("Take = %v, %v, want allowed with the fault returned", allowed, err)
	}
	exhausted, _, err := q.Peek(t.Context(), w1Email)
	if exhausted || err == nil {
		t.Fatalf("Peek = %v, %v, want not exhausted with the fault returned", exhausted, err)
	}
}

// Two workers share one budget: the count is in Redis, not in either of them.
func TestTwoCountersShareOneBudget(t *testing.T) {
	t.Parallel()
	fx := newQuotaFx(t)
	fx.src.set(w1Email, domain.QuotaLimits{PerHour: 4, PerDay: 100})
	other := redis.NewQuota(fx.rdb, fx.src, redis.WithQuotaClock(fx.clock))

	for range 2 {
		fx.take(t, w1Email)
	}
	for i := range 2 {
		if allowed, _, err := other.Take(t.Context(), w1Email); !allowed || err != nil {
			t.Fatalf("the other worker's Take %d = %v, %v, want allowed", i+1, allowed, err)
		}
	}
	if allowed, _ := fx.take(t, w1Email); allowed {
		t.Fatal("the first worker took a fifth unit of four: the workers do not share a count")
	}
}
