package domain

import (
	"fmt"
	"time"
)

// UnreadCount is the bounded badge value (D15): Count stops at the configured
// cap, and Capped says it did.
type UnreadCount struct {
	Count  int
	Capped bool
}

// BroadcastRequest announces to an audience. Exactly one of Audience and
// Recipients is set (NR-020).
type BroadcastRequest struct {
	Tenant     Tenant
	Audience   string      // host selector, resolved in pages by an AudienceResolver
	Recipients []Recipient // or an inline list, at most Config.MaxInlineRecipients (D31)
	Topic      Topic
	Priority   Priority
	Data       map[string]any
	Title      string
	Body       string
	Payload    map[string]string
	// IdemKey is required. Recipient r's key derives from it with
	// [BroadcastMemberKey], so a retried broadcast notifies nobody twice.
	IdemKey string
}

// Validate reports every rule b breaks, as a *ValidationError, or nil. maxInline is
// Config.MaxInlineRecipients: an inline list of exactly that many is allowed, and one
// longer is ErrTooManyRecipients, because an audience that size is a selector the host
// resolves in pages (NR-020, D31).
//
// A broadcast is refused here, where its producer can fix it, and not recorded: a durable
// row for a request that could not be expanded would be retried by a worker for ever.
// An empty list is no source of recipients, as the schema's CHECK says: it would complete
// having notified no one.
func (b BroadcastRequest) Validate(maxInline int) error {
	var v ValidationError
	if b.Tenant.Kind == "" || b.Tenant.ID == "" {
		v.add("Tenant", "kind and id must both be set", ErrEmptyIdentity)
	}
	if b.Topic == "" {
		v.add("Topic", "is required", nil)
	}
	if b.Priority != "" && !b.Priority.Valid() {
		v.add("Priority", fmt.Sprintf("%q is not info, warning or critical", b.Priority), nil)
	}
	v.idemKey("the broadcast's", b.IdemKey)
	switch {
	case b.Audience != "" && len(b.Recipients) > 0:
		v.add("Audience", "and Recipients are both set: a broadcast has exactly one source of recipients", nil)
	case b.Audience == "" && len(b.Recipients) == 0:
		v.add("Audience", "or Recipients is required: a broadcast has exactly one source of recipients", nil)
	}
	if len(b.Recipients) > maxInline {
		v.add("Recipients", fmt.Sprintf("%d recipients, the limit is %d: name an audience selector instead", len(b.Recipients), maxInline), ErrTooManyRecipients)
	}
	for i, r := range b.Recipients {
		if r.Kind == "" || r.ID == "" {
			v.add("Recipients", fmt.Sprintf("recipient %d: kind and id must both be set", i), ErrEmptyIdentity)
			break
		}
	}
	return v.err()
}

// BroadcastReceipt is the outcome of Broadcast.
type BroadcastReceipt struct {
	BroadcastID string
	Applied     bool // false means this broadcast key was already accepted
}

// BroadcastJob is a leased broadcast: the request, and where expansion resumes.
type BroadcastJob struct {
	ID         string
	Realm      Realm
	Request    BroadcastRequest
	Cursor     string // the resolver cursor the next page starts at; "" for the first
	LeaseToken string
}

// PageItem is one recipient's notification in a broadcast page, with the plan
// that recipient's preferences produced.
type PageItem struct {
	Notification Notification
	Plan         DeliveryPlan
}

// CancelRequest names one notification by its producer key.
type CancelRequest struct {
	Tenant    Tenant
	Recipient Recipient
	IdemKey   string
}

// StatusRequest names the notification whose deliveries are asked about.
type StatusRequest = CancelRequest

// DeliveryStatus is one delivery of a notification, by channel and address.
type DeliveryStatus struct {
	Channel           ChannelKind
	AddressKey        string
	State             string // "pending", a TerminalOutcome, or "fanned_out"
	Attempts          int
	ProviderMessageID string
	ProviderStatus    string // latest provider callback
	CompletedAt       time.Time
}

// NotificationStatus answers "was it delivered?" per channel and address
// (NR-032).
type NotificationStatus struct {
	Found          bool
	NotificationID string
	Canceled       bool
	Deliveries     []DeliveryStatus
}
