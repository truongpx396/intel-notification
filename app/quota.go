package app

import (
	"context"
	"time"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// quotaGate is the one place the engine decides what a tenant's budget means (D5, D24,
// NR-023). The Planner peeks at it when a notification is accepted and the Dispatcher takes
// from it when a delivery is sent; both ask the same questions of the same two ports and
// give the same answers when those fail, so the answers are here and not in each caller.
//
//   - Quota fails open and correctness does not. A counter that cannot answer is a budget with
//     units left: a peek says so, and a take allows and returns the fault, for the caller to
//     report. Redis losing its state is the same thing with no fault.
//   - A policy decides whether a spent budget discards, and discarding is never the default:
//     a policy that cannot be read, or that nothing configures, defers.
//   - A peek consumes nothing and a take consumes once. The planner only peeks, so accepting a
//     notification spends nothing, and a delivery that is deferred or retried is not charged
//     for the attempts that sent nothing.
type quotaGate struct {
	counter  ports.QuotaCounter
	policies ports.QuotaPolicies // nil: a spent budget always defers
}

func newQuotaGate(d Deps) quotaGate {
	return quotaGate{counter: d.Quotas, policies: d.QuotaPolicies}
}

// peek reports whether k's budget is spent, and when it returns, without consuming any.
func (g quotaGate) peek(ctx context.Context, k domain.QuotaKey) (exhausted bool, resetAt time.Time) {
	exhausted, resetAt, err := g.counter.Peek(ctx, k)
	if err != nil {
		return false, time.Time{}
	}
	return exhausted, resetAt
}

// take consumes one unit of k's budget if any is left. When the counter cannot answer the
// delivery is allowed, and err says why, so the caller can report it.
func (g quotaGate) take(ctx context.Context, k domain.QuotaKey) (allowed bool, resetAt time.Time, err error) {
	allowed, resetAt, err = g.counter.Take(ctx, k)
	if err != nil {
		return true, time.Time{}, err
	}
	return allowed, resetAt, nil
}

// drops reports whether k's spent budget discards a notification of priority pr on topic
// def, instead of deferring it: only under drop_non_essential, and never for an essential
// topic or a critical notification.
func (g quotaGate) drops(ctx context.Context, k domain.QuotaKey, def domain.TopicDef, pr domain.Priority) bool {
	if g.policies == nil {
		return false
	}
	policy, err := g.policies.Policy(ctx, k)
	return err == nil && policy.Drops(def, pr)
}
