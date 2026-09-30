package domain

import "time"

// InboxState filters a list by the notification's independent seen, read and
// archived states (NR-033). "" is the default list: unarchived.
type InboxState string

const (
	InboxUnread   InboxState = "unread"
	InboxRead     InboxState = "read"
	InboxArchived InboxState = "archived"
)

// ListQuery is one page request against a recipient's inbox.
type ListQuery struct {
	Limit  int
	Cursor string
	State  InboxState
	Topic  Topic
	Locale string // "" means the viewer's default
}

// InboxItem is one notification as the recipient sees it. A zero time means the
// state has not happened.
type InboxItem struct {
	ID        string
	CreatedAt time.Time
	Topic     Topic
	Priority  Priority
	// Title and Body are rendered from the topic's in-app template in the
	// viewer's locale, or the stored fallback copy when none matches (D27).
	Title      string
	Body       string
	Data       map[string]any
	Payload    map[string]string
	SeenAt     time.Time
	ReadAt     time.Time
	ArchivedAt time.Time
}

// Page is one page of an inbox, newest first.
type Page struct {
	Items      []InboxItem
	NextCursor string // "" on the last page
}

// Preference is one recipient choice for a (topic, channel) pair (D3).
type Preference struct {
	Topic   Topic
	Channel ChannelKind
	Enabled bool
}

// PreferenceSource says which level of the resolution produced a value (D30).
type PreferenceSource string

const (
	SourceRecipient    PreferenceSource = "recipient"
	SourceTenant       PreferenceSource = "tenant"
	SourceTopicDefault PreferenceSource = "topic_default"
)

// ResolvedPreference is the effective state of one (topic, channel) pair, with
// where it came from and whether the recipient may change it.
type ResolvedPreference struct {
	Topic   Topic
	Channel ChannelKind
	Enabled bool
	Source  PreferenceSource
	Locked  bool
}

// TenantPreference is a tenant's default for a (topic, channel) pair. Locked
// makes it win over the recipient's own choice (D30).
type TenantPreference struct {
	Tenant  Tenant
	Topic   Topic
	Channel ChannelKind
	Enabled bool
	Locked  bool
}

// QuotaKey names one tenant's budget on one channel (D5, D24).
type QuotaKey struct {
	Realm   Realm
	Tenant  Tenant
	Channel ChannelKind
}
