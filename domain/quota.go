package domain

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
