package ports

import (
	"context"
	"time"

	"github.com/truongpx396/intel-notification/domain"
)

// Tx is a transaction handle the configured Store understands: a pgx.Tx for the
// PostgreSQL store. It is opaque here so the core imports no driver.
type Tx = any

// Notifier is the single entry point producers depend on. The in-process engine
// and the gRPC client both implement it, so a producer cannot tell them apart.
type Notifier interface {
	// Notify persists and enqueues one notification in one transaction,
	// idempotently on (realm, tenant, recipient, IdemKey) within the idempotency
	// window. A replay writes nothing and returns Applied=false. The inbox row is
	// always written; preferences decide the channels and whether it is inbox
	// visible. It returns after commit (D23).
	Notify(ctx context.Context, n domain.Notification) (domain.Receipt, error)

	// NotifyTx does the same inside a transaction the caller owns and commits
	// (D28). The engine never commits or rolls it back. A remote Notifier returns
	// domain.ErrTxUnsupported.
	NotifyTx(ctx context.Context, tx Tx, n domain.Notification) (domain.Receipt, error)

	// Broadcast records a durable broadcast and returns; expansion runs in the
	// worker, a page per transaction, resumable after a crash (NR-020, D7).
	Broadcast(ctx context.Context, b domain.BroadcastRequest) (domain.BroadcastReceipt, error)

	// Cancel withdraws one notification: pending deliveries stop, and in-flight
	// ones are reported, never falsely recorded as canceled (D29).
	Cancel(ctx context.Context, c domain.CancelRequest) (domain.CancelReceipt, error)

	// Status answers "was it delivered?" per channel and address (NR-032).
	Status(ctx context.Context, s domain.StatusRequest) (domain.NotificationStatus, error)
}

// Inbox is the recipient-facing surface the REST adapter drives. Every method
// takes the caller's Identity from its authenticated session, never from a
// request parameter (NR-008).
type Inbox interface {
	List(ctx context.Context, id domain.Identity, q domain.ListQuery) (domain.Page, error)
	// Get returns one of the caller's notifications, rendered as List renders it, or nil when it
	// is not theirs: a caller cannot tell "absent" from "someone else's" (NR-008). It is what the
	// live relay loads for each nudge, under the session's identity (D16). The item carries what
	// the notification is (its topic, priority, copy, data and payload) and not the states a list
	// reads off the row: a nudge announces a new notification, and its times are the list's. locale
	// is the viewer's, as for List, and "" takes the default, so a live toast reads in the language
	// the list does.
	Get(ctx context.Context, id domain.Identity, notificationID, locale string) (*domain.InboxItem, error)
	Unread(ctx context.Context, id domain.Identity) (domain.UnreadCount, error)
	MarkSeen(ctx context.Context, id domain.Identity, before time.Time) error
	MarkRead(ctx context.Context, id domain.Identity, notificationIDs []string) error
	MarkAllRead(ctx context.Context, id domain.Identity) error
	Archive(ctx context.Context, id domain.Identity, notificationIDs []string, archived bool) error
	Preferences(ctx context.Context, id domain.Identity) ([]domain.ResolvedPreference, error)
	SetPreferences(ctx context.Context, id domain.Identity, p []domain.Preference) error
	Schedule(ctx context.Context, id domain.Identity) (domain.DeliverySchedule, error)
	SetSchedule(ctx context.Context, id domain.Identity, s domain.DeliverySchedule) error
	// Unsubscribe is the RFC 8058 one-click: it disables exactly the one
	// (topic, channel) pair the signed token names (D34).
	Unsubscribe(ctx context.Context, token string) error
}

// Admin is the operator and management surface.
type Admin interface {
	// Erase removes one recipient's personal data everywhere and returns the rows
	// removed per table (D35).
	Erase(ctx context.Context, id domain.Identity) (map[string]int, error)
	// ReplayDeadLetter re-drives one dead letter, once: a second call is a no-op.
	ReplayDeadLetter(ctx context.Context, realm domain.Realm, deadLetterID string) error
	SetTenantPreference(ctx context.Context, realm domain.Realm, p domain.TenantPreference) error
}

// Catalog is the management surface of service mode (D31): what a producer in any language does through
// the Catalog service instead of compiling it into the process. Topics, templates and addresses are
// data a producer registers, so a service started from this repository needs no host code (NR-017,
// NR-019, NR-028, NS-011).
//
// The realm is an argument and never part of what is registered. A transport derives it from the
// authenticated producer principal (D31, NR-009), so a producer cannot write into another product's key
// space. An address set carries its realm in the identity it is for, for the same reason.
type Catalog interface {
	// UpsertTopic registers topic in realm with its defaults, replacing any earlier registration whole. A
	// channel it names that is not registered is domain.ErrUnknownChannel, and a registration that does
	// not validate is a *domain.ValidationError; nothing is stored in either case.
	UpsertTopic(ctx context.Context, realm domain.Realm, topic domain.Topic, def domain.TopicDef) error

	// PutTemplate stores t as a new, inactive version and returns it with its number. A template that
	// reaches outside the restricted function map wraps domain.ErrInvalidTemplate, and a channel that is
	// not registered is domain.ErrUnknownChannel; nothing is stored in either case (D55).
	PutTemplate(ctx context.Context, realm domain.Realm, t domain.Template) (domain.Template, error)

	// ActivateTemplate makes one stored version the one that renders and returns it. A version nobody
	// stored is domain.ErrUnknownTemplate.
	ActivateTemplate(ctx context.Context, realm domain.Realm, v domain.TemplateVersion) (domain.Template, error)

	// PutAddresses replaces id's addresses on ch with addrs and returns what was stored: two spellings
	// of one address are one, and the first is kept. A channel that is not registered is
	// domain.ErrUnknownChannel, and a set the engine cannot use is a *domain.ValidationError; nothing is
	// stored in either case (NR-028).
	PutAddresses(ctx context.Context, id domain.Identity, ch domain.ChannelKind, addrs []domain.Address) ([]domain.Address, error)
}

// Webhook is one provider's callback endpoint, served at /webhooks/{channel}/{provider}: a bounce, a
// complaint or a delivery status the provider reports about a message the engine sent (NR-032, D14). It
// is a driving port implemented beside the provider's channel, so the HTTP surface depends on no provider.
// The header is the request's, as net/http's Header is: this package imports no transport.
type Webhook interface {
	// Verify checks the callback's signature over the raw body and returns an error for one that does
	// not verify. It is called first, on bytes nothing has parsed, and nothing about a body is read
	// until it returns nil: a forged bounce would otherwise suppress a real address, a denial of
	// notification that looks like normal operation.
	Verify(header map[string][]string, body []byte) error
	// Handle parses a body Verify accepted and applies it. It returns domain.ErrUnknownDelivery for a
	// callback that names no delivery yet (it can beat the delivery's own record), so the caller can
	// ask the provider to try again.
	Handle(ctx context.Context, body []byte) error
}

// Jobs runs the single-owner jobs — retention, partition provisioning,
// idempotency and digest expiry, digest flush, broadcast expansion — each under a
// notify_job_leases lease it renews while it runs, so
// any number of workers may call Run and each job still has one runner at a time
// (D22). It drives the driven Maintenance and Digests ports.
type Jobs interface {
	Run(ctx context.Context) error
}

// Dispatcher drains the queue. It runs in the worker role.
type Dispatcher interface {
	// Run claims across every shard until ctx ends, adaptively: again at once
	// after a full batch, backing off to Config.PollInterval when empty (D22).
	Run(ctx context.Context) error

	// Drain processes one batch from one shard: the unit Run repeats, exposed so
	// a test can drive the dispatcher one step at a time.
	Drain(ctx context.Context, shard domain.Shard, max int) (processed int, err error)
}
