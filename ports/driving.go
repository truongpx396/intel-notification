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

// Jobs runs the single-owner jobs — retention, partition provisioning,
// idempotency and digest expiry, digest flush, quota rollover, broadcast
// expansion — each under a notify_job_leases lease it renews while it runs, so
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
