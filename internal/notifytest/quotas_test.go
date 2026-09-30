package notifytest

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/truongpx396/intel-notification/domain"
)

func quotaKey(realm domain.Realm, tenant string, ch domain.ChannelKind) domain.QuotaKey {
	return domain.QuotaKey{Realm: realm, Tenant: domain.Tenant{Kind: "workspace", ID: tenant}, Channel: ch}
}

// Losing Redis resets the budgets without an error (D5), so a key nobody gave a
// budget is unlimited, and is never reported exhausted.
func TestQuotaCounterIsUnlimitedUntilGivenABudget(t *testing.T) {
	t.Parallel()
	q := NewQuotaCounter()
	k := quotaKey("aisat", "w1", "email")
	for i := range 5 {
		if exhausted, reset, err := q.Peek(t.Context(), k); exhausted || err != nil || !reset.IsZero() {
			t.Fatalf("Peek %d = %v, %v, %v, want not exhausted", i, exhausted, reset, err)
		}
		if allowed, _, err := q.Take(t.Context(), k); !allowed || err != nil {
			t.Fatalf("Take %d = %v, %v, want allowed", i, allowed, err)
		}
	}
	if got := q.Taken(k); got != 5 {
		t.Fatalf("Taken = %d, want 5: an unlimited key still counts what was taken", got)
	}
}

// A peek consumes nothing and a take consumes once (T036). The counts are
// asserted by taking, so a peek that quietly spent a unit shows.
func TestQuotaCounterPeekConsumesNothingAndTakeConsumesOnce(t *testing.T) {
	t.Parallel()
	q := NewQuotaCounter()
	k := quotaKey("aisat", "w1", "email")
	reset := t0.Add(time.Hour)
	q.SetBudget(k, 2, reset)

	for range 10 {
		if exhausted, _, _ := q.Peek(t.Context(), k); exhausted {
			t.Fatal("Peek reported exhausted with units left")
		}
	}
	if allowed, _, _ := q.Take(t.Context(), k); !allowed {
		t.Fatal("the first Take was refused: the peeks spent units")
	}
	if allowed, _, _ := q.Take(t.Context(), k); !allowed {
		t.Fatal("the second Take was refused, with two units given")
	}
	allowed, got, err := q.Take(t.Context(), k)
	if allowed || err != nil {
		t.Fatalf("the third Take = %v, %v, want refused", allowed, err)
	}
	if !got.Equal(reset) {
		t.Fatalf("a refused Take reports reset %v, want %v: the dispatcher defers to it", got, reset)
	}
	exhausted, got, _ := q.Peek(t.Context(), k)
	if !exhausted || !got.Equal(reset) {
		t.Fatalf("Peek = %v at %v, want exhausted, resetting at %v", exhausted, got, reset)
	}
	if n := q.Taken(k); n != 2 {
		t.Fatalf("Taken = %d, want 2: a refused Take consumes nothing", n)
	}
}

func TestQuotaCounterReportsTheResetOnAnAllowedTake(t *testing.T) {
	t.Parallel()
	q := NewQuotaCounter()
	k := quotaKey("aisat", "w1", "email")
	reset := t0.Add(time.Hour)
	q.SetBudget(k, 1, reset)
	if _, got, _ := q.Take(t.Context(), k); !got.Equal(reset) {
		t.Fatalf("Take reports reset %v, want %v", got, reset)
	}
}

func TestQuotaCounterZeroBudgetIsExhaustedAtOnce(t *testing.T) {
	t.Parallel()
	q := NewQuotaCounter()
	k := quotaKey("aisat", "w1", "email")
	q.SetBudget(k, 0, t0)
	if exhausted, _, _ := q.Peek(t.Context(), k); !exhausted {
		t.Fatal("a budget of zero is not exhausted")
	}
	if allowed, _, _ := q.Take(t.Context(), k); allowed {
		t.Fatal("a budget of zero allowed a Take")
	}
}

// Budgets are per realm, tenant and channel (D5): one tenant exhausting its email
// must not touch another tenant's, another channel's, or another realm's.
func TestQuotaCounterKeysAreIndependent(t *testing.T) {
	t.Parallel()
	exhausted := quotaKey("aisat", "w1", "email")
	cases := []struct {
		name string
		key  domain.QuotaKey
	}{
		{"another tenant", quotaKey("aisat", "w2", "email")},
		{"another channel", quotaKey("aisat", "w1", "sms")},
		{"another realm", quotaKey("other", "w1", "email")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			q := NewQuotaCounter()
			q.SetBudget(exhausted, 1, t0)
			q.Take(t.Context(), exhausted)
			if gone, _, _ := q.Peek(t.Context(), exhausted); !gone {
				t.Fatal("the exhausted key is not exhausted")
			}
			if gone, _, _ := q.Peek(t.Context(), tc.key); gone {
				t.Fatalf("exhausting one key exhausted %s", tc.name)
			}
			if allowed, _, _ := q.Take(t.Context(), tc.key); !allowed {
				t.Fatalf("%s was refused a Take", tc.name)
			}
		})
	}
}

func TestQuotaCounterLoseForgetsEveryBudget(t *testing.T) {
	t.Parallel()
	q := NewQuotaCounter()
	a, b := quotaKey("aisat", "w1", "email"), quotaKey("aisat", "w2", "sms")
	q.SetBudget(a, 0, t0)
	q.SetBudget(b, 0, t0)
	q.Lose()
	for _, k := range []domain.QuotaKey{a, b} {
		if exhausted, _, err := q.Peek(t.Context(), k); exhausted || err != nil {
			t.Fatalf("after Lose, Peek(%+v) = %v, %v, want unlimited and no error", k, exhausted, err)
		}
		if allowed, _, _ := q.Take(t.Context(), k); !allowed {
			t.Fatalf("after Lose, Take(%+v) was refused", k)
		}
	}
}

// Workers take concurrently. Under -race this fails on an unsynchronized counter,
// and the total shows exactly the budget was handed out, no more.
func TestQuotaCounterIsSafeForConcurrentUse(t *testing.T) {
	t.Parallel()
	const budget, workers = 10, 50
	q := NewQuotaCounter()
	k := quotaKey("aisat", "w1", "email")
	q.SetBudget(k, budget, t0)
	var allowed atomic.Int32
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if ok, _, _ := q.Take(t.Context(), k); ok {
				allowed.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := allowed.Load(); got != budget {
		t.Fatalf("%d of %d concurrent Takes were allowed, want exactly the budget, %d", got, workers, budget)
	}
}
