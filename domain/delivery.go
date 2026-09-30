package domain

import "time"

// Delivery is one queue entry bound to one address and rendered: the unit a
// channel sends.
type Delivery struct {
	ID string // the queue row
	// IdemKey is what the channel dedupes on. It is not Notification.IdemKey:
	// it is distinct per address and per digest (D25), built by [DeliveryIdemKey].
	IdemKey        string
	Identity       Identity
	NotificationID string // "" for a digest
	DigestID       string // "" for a notification
	Topic          Topic
	Notification   Notification   // zero for a digest
	Digest         []Notification // the members, for a digest delivery
	Channel        ChannelKind
	Address        Address
	Content        RenderedContent
	Attempt        int
}

// Outcome is what a channel reports for one send. It is not a [TerminalOutcome]:
// Retry is not terminal, and the dispatcher maps the rest (D12).
type Outcome int

const (
	Delivered  Outcome = iota + 1
	Retry              // transient: the dispatcher backs off (D10)
	Suppressed         // this address is dead: terminal; the engine records the suppression (D13)
	Rejected           // permanent refusal unrelated to the address: terminal, dead-lettered (D12)
)

// DeliveryResult is a channel's report. A returned Go error means an
// infrastructure fault and is treated as Retry; everything a channel knows about
// the delivery itself is reported here.
type DeliveryResult struct {
	Outcome           Outcome
	ProviderMessageID string            // correlates provider callbacks (D20)
	SuppressReason    SuppressionReason // with Suppressed
	SuppressFor       time.Duration     // with Suppressed: 0 means permanent (D14)
	RetryAfter        time.Duration     // with Retry: a provider hint, a floor on backoff (D10)
	Detail            string
}

// SuppressionReason is why an address stopped being used: the vocabulary of
// channel_suppressions.reason.
type SuppressionReason string

const (
	SuppressHardBounce     SuppressionReason = "hard_bounce"
	SuppressComplaint      SuppressionReason = "complaint"
	SuppressUnsubscribe    SuppressionReason = "unsubscribe"
	SuppressInvalidAddress SuppressionReason = "invalid_address"
	SuppressTokenRevoked   SuppressionReason = "token_revoked"
	SuppressSoftBounce     SuppressionReason = "soft_bounce"
)

// RenderRequest asks the TemplateRenderer for the content of one delivery.
type RenderRequest struct {
	Realm       Realm
	Tenant      Tenant // per-tenant branding varies here
	Topic       Topic
	TemplateRef string
	Channel     ChannelKind
	Locale      string
	Data        map[string]any
	Digest      []Notification // members, for a digest
}

// RenderedContent is what a channel sends.
type RenderedContent struct {
	Subject         string
	Body            string
	Data            map[string]any // structured fields for rich channels
	TemplateVersion int
}

// ChannelCapabilities is what the engine reads from a channel instead of
// branching on its kind (NR-016, invariant 13).
type ChannelCapabilities struct {
	NeedsSubject bool // render a subject (email, push title)
	NeedsAddress bool // false means addressed by the recipient identity alone (in-app)
	RichContent  bool // consumes RenderedContent.Data (Slack blocks, push payload)
	InboxBacked  bool // enabling this channel makes the notification visible in the inbox (D26)

	// Dedup is the strength of this channel's idempotency on Delivery.IdemKey (D32).
	Dedup DedupLevel
	// DedupWindow is, with DedupProvider, how long the provider remembers a key.
	DedupWindow time.Duration
}

// DedupLevel is how strongly a channel collapses a re-driven delivery. It is
// ordered: a composite channel declares the weakest of its members (D29).
type DedupLevel int

const (
	DedupNone     DedupLevel = iota // at-least-once: a crash between send and record may resend
	DedupLocal                      // the channel records sends itself, or is idempotent by construction
	DedupProvider                   // the provider honors an idempotency key for DedupWindow
)
