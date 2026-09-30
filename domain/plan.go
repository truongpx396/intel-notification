package domain

import "time"

// PlanMode is what the Notifier decided for one channel at notify time.
type PlanMode int

const (
	PlanImmediate PlanMode = iota + 1 // due now, or at DeliverAfter
	PlanDeferred                      // due at NotBefore: quiet hours or an exhausted quota
	PlanDigest                        // folded into the recipient's open digest window
	PlanDropped                       // quota policy drop_non_essential: recorded, not sent
)

// PlannedDelivery is the plan for one channel.
type PlannedDelivery struct {
	Channel            ChannelKind
	Mode               PlanMode
	NotBefore          time.Time
	Reason             string        // "scheduled" | "quiet_hours" | "quota"
	Fallback           []ChannelKind // the rest of the chain, for a fallback channel
	QuietHoursOverride bool          // critical inside quiet hours, recorded for audit (D9)
}

// DeliveryPlan is everything PersistAndEnqueue writes besides the inbox row.
type DeliveryPlan struct {
	InboxVisible bool // any enabled channel is inbox-backed (D26)
	VisibleFrom  time.Time
	Deliveries   []PlannedDelivery
	DigestWindow time.Duration
	DigestMax    int
	Shard        Shard
}

// Receipt is the outcome of Notify. Applied is false for a replay of a key
// already persisted inside the idempotency window: nothing was written.
type Receipt struct {
	NotificationID string
	IdemKey        string
	Applied        bool
	InboxVisible   bool
	Deliveries     []PlannedDelivery // what was planned on first apply; empty on a replay
}
