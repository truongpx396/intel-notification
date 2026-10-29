package notifytest

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// QuotaWorld is a ports.QuotaCounter a test can give a budget and make forget. The fake
// in this package and the Redis counter differ in how a budget is configured (the fake
// is told a count, Redis reads ceilings from a table), so each suite adapts its
// counter to this, and QuotaContract asserts what the port promises through it.
type QuotaWorld interface {
	ports.QuotaCounter
	// Grant gives k n units, to be taken before the reset it returns. A key nobody
	// granted has no budget: it is unlimited.
	Grant(k domain.QuotaKey, n int) (resetAt time.Time)
	// Lose forgets everything counted, as losing Redis does.
	Lose()
}

// QuotaWorldFactory returns a world with every key unlimited, for t alone.
type QuotaWorldFactory func(t *testing.T) QuotaWorld

// QuotaContract is the table every ports.QuotaCounter runs: a peek consumes nothing, a
// take consumes once, a refused take consumes nothing and says when units return, one
// budget is not another's, the count is atomic under concurrent takers, and losing the
// counter's state resets budgets without an error (D5, D24, T036).
func QuotaContract(t *testing.T, newWorld QuotaWorldFactory) {
	t.Helper()
	key := func(tenant string, ch domain.ChannelKind) domain.QuotaKey {
		return domain.QuotaKey{Realm: "aisat", Tenant: domain.Tenant{Kind: "workspace", ID: tenant}, Channel: ch}
	}

	t.Run("a key nobody gave a budget is unlimited", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		k := key("w1", "email")
		for i := range 5 {
			if exhausted, _, err := w.Peek(t.Context(), k); exhausted || err != nil {
				t.Fatalf("Peek %d = %v, %v, want not exhausted", i, exhausted, err)
			}
			if allowed, _, err := w.Take(t.Context(), k); !allowed || err != nil {
				t.Fatalf("Take %d = %v, %v, want allowed", i, allowed, err)
			}
		}
	})

	t.Run("a peek consumes nothing and a take consumes once", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		k := key("w1", "email")
		reset := w.Grant(k, 2)

		// The peeks come first and are many, so one that quietly spent a unit shows as a
		// refused take below.
		for range 10 {
			if exhausted, _, err := w.Peek(t.Context(), k); exhausted || err != nil {
				t.Fatalf("Peek = %v, %v, want not exhausted with units left", exhausted, err)
			}
		}
		for i := range 2 {
			allowed, _, err := w.Take(t.Context(), k)
			if !allowed || err != nil {
				t.Fatalf("Take %d = %v, %v, want allowed: two units were given", i+1, allowed, err)
			}
		}
		allowed, got, err := w.Take(t.Context(), k)
		if allowed || err != nil {
			t.Fatalf("the third Take = %v, %v, want refused", allowed, err)
		}
		if !got.Equal(reset) {
			t.Fatalf("a refused Take reports reset %v, want %v: the dispatcher defers the delivery to it", got, reset)
		}
		exhausted, got, err := w.Peek(t.Context(), k)
		if !exhausted || err != nil || !got.Equal(reset) {
			t.Fatalf("Peek = %v, %v, %v, want exhausted and resetting at %v", exhausted, got, err, reset)
		}
	})

	t.Run("a refused take consumes nothing", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		k := key("w1", "email")
		w.Grant(k, 1)
		if allowed, _, _ := w.Take(t.Context(), k); !allowed {
			t.Fatal("the only unit was refused")
		}
		for i := range 5 {
			if allowed, _, _ := w.Take(t.Context(), k); allowed {
				t.Fatalf("Take %d after the unit was spent was allowed", i)
			}
		}
		// A refusal that counted would put the count over the ceiling, and a ceiling
		// raised by one would then still be spent. The port can only show it as a budget that
		// is exhausted exactly as before: nothing refused changes what a peek says.
		if exhausted, _, _ := w.Peek(t.Context(), k); !exhausted {
			t.Fatal("Peek after the refusals reports units left")
		}
	})

	t.Run("an allowed take reports when the budget resets", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		k := key("w1", "email")
		reset := w.Grant(k, 3)
		if _, got, err := w.Take(t.Context(), k); err != nil || !got.Equal(reset) {
			t.Fatalf("Take reports reset %v, %v, want %v", got, err, reset)
		}
	})

	t.Run("a budget of none is spent from the start", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		k := key("w1", "email")
		reset := w.Grant(k, 0)
		if exhausted, got, err := w.Peek(t.Context(), k); !exhausted || err != nil || !got.Equal(reset) {
			t.Fatalf("Peek = %v, %v, %v, want exhausted, resetting at %v", exhausted, got, err, reset)
		}
		if allowed, _, err := w.Take(t.Context(), k); allowed || err != nil {
			t.Fatalf("Take = %v, %v, want refused", allowed, err)
		}
	})

	t.Run("one budget is not another's", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		spent := key("w1", "email")
		w.Grant(spent, 1)
		w.Take(t.Context(), spent) //nolint:errcheck // spends the budget; the next assertions are the test

		others := map[string]domain.QuotaKey{
			"another tenant":      key("w2", "email"),
			"another channel":     key("w1", "sms"),
			"another tenant kind": {Realm: "aisat", Tenant: domain.Tenant{Kind: "org", ID: "w1"}, Channel: "email"},
			"another realm":       {Realm: "other", Tenant: spent.Tenant, Channel: "email"},
		}
		for name, k := range others {
			if exhausted, _, err := w.Peek(t.Context(), k); exhausted || err != nil {
				t.Errorf("%s: Peek = %v, %v, want not exhausted: it shares a budget with a key it is not", name, exhausted, err)
			}
			if allowed, _, err := w.Take(t.Context(), k); !allowed || err != nil {
				t.Errorf("%s: Take = %v, %v, want allowed", name, allowed, err)
			}
		}
		if exhausted, _, _ := w.Peek(t.Context(), spent); !exhausted {
			t.Error("the spent budget was given units back by a take on another")
		}
	})

	t.Run("concurrent takers never take more than was given", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		k := key("w1", "email")
		const given, takers = 20, 80
		w.Grant(k, given)
		var (
			wg      sync.WaitGroup
			allowed atomic.Int64
		)
		for range takers {
			wg.Go(func() {
				if ok, _, err := w.Take(t.Context(), k); err != nil {
					t.Errorf("Take: %v", err)
				} else if ok {
					allowed.Add(1)
				}
			})
		}
		wg.Wait()
		if got := allowed.Load(); got != given {
			t.Fatalf("%d of %d concurrent takes were allowed, want exactly the %d given: a budget is not a hint", got, takers, given)
		}
	})

	t.Run("losing the counter's state resets the budget without an error", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		k := key("w1", "email")
		w.Grant(k, 1)
		w.Take(t.Context(), k) //nolint:errcheck // spends the budget
		if exhausted, _, _ := w.Peek(t.Context(), k); !exhausted {
			t.Fatal("the budget was not spent, so losing it proves nothing")
		}

		w.Lose()
		if exhausted, _, err := w.Peek(t.Context(), k); exhausted || err != nil {
			t.Fatalf("Peek after the loss = %v, %v, want units again and no error: quota fails open (D5)", exhausted, err)
		}
		if allowed, _, err := w.Take(t.Context(), k); !allowed || err != nil {
			t.Fatalf("Take after the loss = %v, %v, want allowed and no error", allowed, err)
		}
	})
}
