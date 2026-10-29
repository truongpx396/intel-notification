package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/truongpx396/intel-notification/domain"
)

// T036: the quota gate on its own. The Planner and the Dispatcher each shadow part of what
// it does with a guard of their own (the planner ignores a reset that is not in the future,
// and the dispatcher reads the error before the answer), so a gate that failed the wrong way
// would still pass through them. The gate is where "quota fails open, and a lookup that fails
// must not discard" is held, so it is asserted here, directly (D5).

type scriptedCounter struct {
	exhausted, allowed bool
	resetAt            time.Time
	err                error

	peeks, takes int
}

func (c *scriptedCounter) Peek(context.Context, domain.QuotaKey) (bool, time.Time, error) {
	c.peeks++
	return c.exhausted, c.resetAt, c.err
}

func (c *scriptedCounter) Take(context.Context, domain.QuotaKey) (bool, time.Time, error) {
	c.takes++
	return c.allowed, c.resetAt, c.err
}

type scriptedPolicies struct {
	policy domain.QuotaPolicy
	err    error
}

func (p scriptedPolicies) Policy(context.Context, domain.QuotaKey) (domain.QuotaPolicy, error) {
	return p.policy, p.err
}

var (
	gateKey   = domain.QuotaKey{Realm: "aisat", Tenant: domain.Tenant{Kind: "workspace", ID: "w1"}, Channel: "email"}
	gateReset = time.Date(2031, 3, 1, 13, 0, 0, 0, time.UTC)
	errGate   = errors.New("the counter is away")
)

// A peek asks the counter to peek and nothing else: it consumes nothing, and the answer
// is the counter's. A counter that cannot answer is a budget with units left, whatever
// it said along with its error.
func TestTheGatePeeksAndAnUnansweredPeekIsUnspent(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name          string
		counter       scriptedCounter
		wantExhausted bool
		wantReset     time.Time
	}{
		{"units left", scriptedCounter{exhausted: false, resetAt: gateReset}, false, gateReset},
		{"spent", scriptedCounter{exhausted: true, resetAt: gateReset}, true, gateReset},
		{"a counter that cannot answer, and said spent with its error", scriptedCounter{exhausted: true, resetAt: gateReset, err: errGate}, false, time.Time{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := tc.counter
			exhausted, resetAt := quotaGate{counter: &c}.peek(t.Context(), gateKey)
			if exhausted != tc.wantExhausted || !resetAt.Equal(tc.wantReset) {
				t.Fatalf("peek = %v, %v, want %v, %v", exhausted, resetAt, tc.wantExhausted, tc.wantReset)
			}
			if c.peeks != 1 || c.takes != 0 {
				t.Fatalf("the counter was asked to peek %d times and to take %d, want 1 and 0: a peek consumes nothing", c.peeks, c.takes)
			}
		})
	}
}

// A take asks the counter to take, once. A counter that cannot answer allows the
// delivery and returns why, so the dispatcher can say so; it does not hold deliveries back.
func TestTheGateTakesAndAnUnansweredTakeIsAllowed(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name        string
		counter     scriptedCounter
		wantAllowed bool
		wantReset   time.Time
		wantErr     error
	}{
		{"allowed", scriptedCounter{allowed: true, resetAt: gateReset}, true, gateReset, nil},
		{"refused", scriptedCounter{allowed: false, resetAt: gateReset}, false, gateReset, nil},
		{"a counter that cannot answer, and refused with its error", scriptedCounter{allowed: false, resetAt: gateReset, err: errGate}, true, time.Time{}, errGate},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := tc.counter
			allowed, resetAt, err := quotaGate{counter: &c}.take(t.Context(), gateKey)
			if allowed != tc.wantAllowed || !resetAt.Equal(tc.wantReset) || !errors.Is(err, tc.wantErr) {
				t.Fatalf("take = %v, %v, %v, want %v, %v, %v", allowed, resetAt, err, tc.wantAllowed, tc.wantReset, tc.wantErr)
			}
			if c.takes != 1 || c.peeks != 0 {
				t.Fatalf("the counter was asked to take %d times and to peek %d, want 1 and 0", c.takes, c.peeks)
			}
		})
	}
}

// A spent budget drops only what the policy allows, and discarding is never what a
// missing or broken policy does: no policy store, a lookup that fails and a policy that
// says to defer all defer, even when the failed lookup also named a policy that drops.
func TestTheGateDropsOnlyWhatThePolicyAllowsAndNeverOnAFailedLookup(t *testing.T) {
	t.Parallel()
	ordinary := domain.TopicDef{DefaultPriority: domain.PriorityInfo}
	essential := domain.TopicDef{DefaultPriority: domain.PriorityInfo, Essential: true}
	cases := []struct {
		name     string
		policies func() *scriptedPolicies
		nilStore bool
		def      domain.TopicDef
		pr       domain.Priority
		want     bool
	}{
		{"drop_non_essential on an ordinary notification", func() *scriptedPolicies { return &scriptedPolicies{policy: domain.QuotaDropNonEssential} }, false, ordinary, domain.PriorityInfo, true},
		{"drop_non_essential on an essential topic", func() *scriptedPolicies { return &scriptedPolicies{policy: domain.QuotaDropNonEssential} }, false, essential, domain.PriorityInfo, false},
		{"drop_non_essential on a critical notification", func() *scriptedPolicies { return &scriptedPolicies{policy: domain.QuotaDropNonEssential} }, false, ordinary, domain.PriorityCritical, false},
		{"defer", func() *scriptedPolicies { return &scriptedPolicies{policy: domain.QuotaDefer} }, false, ordinary, domain.PriorityInfo, false},
		{"a lookup that fails, whatever it also said", func() *scriptedPolicies {
			return &scriptedPolicies{policy: domain.QuotaDropNonEssential, err: errGate}
		}, false, ordinary, domain.PriorityInfo, false},
		{"no policy store", nil, true, ordinary, domain.PriorityInfo, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			g := quotaGate{counter: &scriptedCounter{}}
			if !tc.nilStore {
				g.policies = tc.policies()
			}
			if got := g.drops(t.Context(), gateKey, tc.def, tc.pr); got != tc.want {
				t.Fatalf("drops = %v, want %v", got, tc.want)
			}
		})
	}
}
