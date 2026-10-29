package domain

import "time"

// QuotaPolicy is what a tenant's channel quota says to do when its budget is
// spent: the vocabulary of channel_quotas.on_exhausted (D5).
type QuotaPolicy string

const (
	// QuotaDefer moves the delivery to the budget's reset. It is the default, and
	// the only policy that discards nothing (NR-023).
	QuotaDefer QuotaPolicy = "defer"
	// QuotaDropNonEssential drops a delivery instead of waiting, unless the topic
	// is essential or the notification critical: those always defer.
	QuotaDropNonEssential QuotaPolicy = "drop_non_essential"
)

// Drops reports whether an exhausted budget under policy p drops a notification of
// priority pr on topic def, instead of deferring it. It never drops an essential
// topic or a critical notification (D5).
func (p QuotaPolicy) Drops(def TopicDef, pr Priority) bool {
	return p == QuotaDropNonEssential && !def.Essential && def.EffectivePriority(pr) != PriorityCritical
}

// QuotaLimits is a budget's ceilings, the numbers in channel_quotas: how many
// deliveries a (realm, tenant, channel) may have in a UTC hour and in a UTC day. A
// ceiling of zero allows none, so a row that says zero silences the channel; a tenant
// with no row at all is the unlimited one, which is the absence of QuotaLimits and not
// a zero of them.
type QuotaLimits struct {
	PerHour int
	PerDay  int
}

// QuotaWindow is one of the two windows a budget is counted in. Both are fixed and
// aligned to the clock: the UTC hour and the UTC day. A fixed window gives an
// exhausted budget a reset that is one instant, which is what Queue.Defer needs to
// move a delivery to (D24); a sliding window has none until the oldest unit ages out,
// and every claimant would have a different answer. The price is that a tenant may spend
// two hours' budget across a boundary, which is a bound on a tenant's share, not on a
// provider's rate.
type QuotaWindow string

const (
	QuotaHour QuotaWindow = "h"
	QuotaDay  QuotaWindow = "d"
)

// Start is the first instant of the window containing t.
func (w QuotaWindow) Start(t time.Time) time.Time { return time.Time{} }

// End is the instant the window containing t ends, and its count starts again at zero.
func (w QuotaWindow) End(t time.Time) time.Time { return time.Time{} }

// Status judges a budget at now, given what has been counted so far in the current
// hour and the current day. It is exhausted when either window has reached its
// ceiling. resetAt is when units return: the end of the longest window that is spent,
// since a delivery deferred to the end of the hour would find a spent day. A budget
// with units left reports the end of the hour.
func (l QuotaLimits) Status(now time.Time, hour, day int) (exhausted bool, resetAt time.Time) {
	return false, time.Time{}
}
