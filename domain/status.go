package domain

import "time"

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
