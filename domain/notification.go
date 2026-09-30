package domain

import "time"

// The three priorities. A critical notification ignores quiet hours (D9) and is
// never digested (NR-014) or dropped by quota (D5).
const (
	PriorityInfo     Priority = "info"
	PriorityWarning  Priority = "warning"
	PriorityCritical Priority = "critical"
)

// Notification is what a producer builds. It carries no Realm: the realm comes
// from configuration or from the authenticated producer, never from the request
// (NR-009) — see [Notification.Identity].
type Notification struct {
	Tenant    Tenant
	Recipient Recipient
	Topic     Topic
	Priority  Priority // "" means the topic's default

	// Data is the template variables. Copy lives in the TemplateRenderer (D27).
	Data map[string]any
	// Title and Body are optional fallback copy, used only when no template
	// exists for (topic, channel, locale).
	Title string
	Body  string

	Payload    map[string]string // deep-link refs for the UI; never a routing input
	Attributes map[string]string // trace id, source; audit only, never a routing input

	// IdemKey is required: the producer's exactly-once identity for this event
	// and recipient, such as "invoice:42:overdue". 1 to 255 bytes.
	IdemKey string

	OccurredAt    time.Time
	DeliverAfter  time.Time // zero means now: a scheduled send (D29)
	DeliverBefore time.Time // zero means no expiry; past it the delivery is terminal "expired" (D29)
}

// TopicDef is a registered topic's defaults (NR-017).
type TopicDef struct {
	DefaultChannels []ChannelKind // delivered in parallel, each subject to preferences
	Fallback        []ChannelKind // tried in order while each ends undelivered (D29)
	DefaultPriority Priority
	Essential       bool   // cannot be disabled, never digested, no unsubscribe affordance (NR-015)
	TemplateRef     string // "" means the topic name
}

// DeliverySchedule is a recipient's quiet hours and digest cadence. It drives
// deferral and coalescing; a channel never sees it.
type DeliverySchedule struct {
	QuietStart string        // "22:00" local; "" means none
	QuietEnd   string        // "07:00" local
	Timezone   string        // IANA
	Digest     time.Duration // 0 means immediate; above 0, same-topic notifications coalesce into a window
}
