package domain

import (
	"fmt"
	"slices"
	"time"
)

// The three priorities. A critical notification ignores quiet hours (D9) and is
// never digested (NR-014) or dropped by quota (D5).
const (
	PriorityInfo     Priority = "info"
	PriorityWarning  Priority = "warning"
	PriorityCritical Priority = "critical"
)

// Valid reports whether p is one of the three priorities.
func (p Priority) Valid() bool {
	switch p {
	case PriorityInfo, PriorityWarning, PriorityCritical:
		return true
	}
	return false
}

// MaxIdemKeyBytes is the longest idempotency key. It counts bytes, as the
// contract says; the schema's length check counts characters, which is never
// fewer, so a key accepted here is accepted there.
const MaxIdemKeyBytes = 255

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

// Validate reports every rule n breaks, as a *ValidationError, or nil. The
// identity and key rules are the schema's checks on notifications and
// notify_idem: an empty component would equal an unset scope and defeat the
// row-level security predicate (D17).
func (n Notification) Validate() error {
	var v ValidationError
	if n.Tenant.Kind == "" || n.Tenant.ID == "" {
		v.add("Tenant", "kind and id must both be set", ErrEmptyIdentity)
	}
	if n.Recipient.Kind == "" || n.Recipient.ID == "" {
		v.add("Recipient", "kind and id must both be set", ErrEmptyIdentity)
	}
	if n.Topic == "" {
		v.add("Topic", "is required", nil)
	}
	if n.Priority != "" && !n.Priority.Valid() {
		v.add("Priority", fmt.Sprintf("%q is not info, warning or critical", n.Priority), nil)
	}
	switch {
	case n.IdemKey == "":
		v.add("IdemKey", "is required: it is the notification's exactly-once identity", ErrMissingIdem)
	case len(n.IdemKey) > MaxIdemKeyBytes:
		v.add("IdemKey", fmt.Sprintf("is %d bytes, the limit is %d", len(n.IdemKey), MaxIdemKeyBytes), nil)
	}
	// An unset DeliverAfter is the zero time, earlier than any real expiry, so an
	// expiry alone is always after it; the notifier's clock judges it against now.
	if !n.DeliverBefore.IsZero() && !n.DeliverBefore.After(n.DeliverAfter) {
		v.add("DeliverBefore", "must be after DeliverAfter, or the notification could never be delivered", nil)
	}
	return v.err()
}

// Identity is n's full scoping tuple under realm. The realm is an argument, not
// a field of n: it is never taken from the request (NR-009).
func (n Notification) Identity(realm Realm) Identity {
	return Identity{Realm: realm, Tenant: n.Tenant, Recipient: n.Recipient}
}

// Validate reports every rule t breaks, as a *ValidationError, or nil. A channel
// is either fanned out in parallel or tried as a fallback, never both, and never
// twice: the schema's check on notification_topics, written as code so a topic
// registered in code fails the way a row would (NR-017, NR-031).
func (t TopicDef) Validate() error {
	var v ValidationError
	if msg := channelListProblem(t.DefaultChannels); msg != "" {
		v.add("DefaultChannels", msg, nil)
	}
	msg := channelListProblem(t.Fallback)
	if msg == "" {
		for _, ch := range t.Fallback {
			if slices.Contains(t.DefaultChannels, ch) {
				msg = fmt.Sprintf("channel %q is also delivered in parallel; a channel is one or the other", ch)
				break
			}
		}
	}
	if msg != "" {
		v.add("Fallback", msg, nil)
	}
	if !t.DefaultPriority.Valid() {
		v.add("DefaultPriority", fmt.Sprintf("%q is not info, warning or critical", t.DefaultPriority), nil)
	}
	return v.err()
}

// channelListProblem describes the first fault in a list of channels, or returns
// "": an empty kind, or a kind that appears twice.
func channelListProblem(chs []ChannelKind) string {
	for i, ch := range chs {
		if ch == "" {
			return "has an empty channel kind"
		}
		if slices.Contains(chs[:i], ch) {
			return fmt.Sprintf("lists channel %q more than once", ch)
		}
	}
	return ""
}

// EffectivePriority is the priority a notification of priority p is treated at:
// p itself, or the topic's default when p is empty.
func (t TopicDef) EffectivePriority(p Priority) Priority {
	if p == "" {
		return t.DefaultPriority
	}
	return p
}

// Digestible reports whether a notification of priority p on this topic may be
// folded into a digest: never a critical one, and never one on an essential
// topic (NR-014, NR-015). The priority that counts is the effective one, so an
// unset priority on a topic that defaults to critical is critical.
func (t TopicDef) Digestible(p Priority) bool {
	return !t.Essential && t.EffectivePriority(p) != PriorityCritical
}
