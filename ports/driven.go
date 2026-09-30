// Package ports holds the interfaces at the hexagon's edges. The use-cases in
// app/ depend on these and on domain/, never on an adapter.
//
// driving.go holds what callers use: Notifier, Inbox, Admin, Dispatcher and
// Jobs. This file holds what the engine uses: the delivery queue, digest windows
// and maintenance operations, which adapters/driven/postgres implements as plain
// SQL, and the persist and read paths, preferences, catalog, channels and
// counters whose implementations land with their own tasks. The Go declarations
// are normative; specs/001-notification-core/contracts/notification-ports.md
// explains them.
//
// An interface has no behaviour to test. It is the seam the contract suites are
// written against, and each implementation asserts it at compile time:
//
//	var _ ports.Queue = (*Store)(nil)
package ports

import (
	"context"
	"time"

	"github.com/truongpx396/intel-notification/domain"
)

// Queue is the delivery queue's state machine (D19, D20, D25). Every method that
// takes a Claim is FENCED: it changes state only while the claim's lease token is
// current, and reports ok=false — having written nothing — when the lease was
// lost to another claimer. A caller that gets ok=false must drop the delivery:
// the row's current holder owns it.
type Queue interface {
	// Claim leases up to max due deliveries in one shard, skipping rows another
	// claimer holds. Each claim counts as an attempt, so a delivery that crashes
	// its worker every time still reaches the ceiling.
	Claim(ctx context.Context, shard domain.Shard, max int, lease time.Duration) ([]domain.Claim, error)

	// Retry records a transient failure: the delivery is due again at next, and
	// the lease is released.
	Retry(ctx context.Context, c domain.Claim, next time.Time, lastError string) (ok bool, err error)

	// Defer records a quota or quiet-hours deferral: due again at until, and the
	// claim's attempt is given back, so deferral never walks a delivery into the
	// dead-letter table.
	Defer(ctx context.Context, c domain.Claim, until time.Time, reason string) (ok bool, err error)

	// Finish is the terminal transition: in one transaction it removes the queue
	// row, appends it to the delivery history, and — as f.Disposition says —
	// writes a dead letter and enqueues the next fallback channel. The store
	// decides none of that itself (D37).
	Finish(ctx context.Context, c domain.Claim, f domain.Finish) (ok bool, err error)

	// BindAddresses pins an unbound claim to its resolved addresses. One address
	// binds in place and the claim stays held; several replace the row with one
	// unleased row per address, recorded as fanned_out. keys[i] is addrs[i]'s
	// domain.AddressKey. An empty addrs is an error: finish as no_address instead.
	BindAddresses(ctx context.Context, c domain.Claim, addrs []domain.Address, keys []string) (b domain.Binding, ok bool, err error)

	// DeferTenantChannel moves a tenant's whole due backlog on one channel to
	// until, so a tenant that exhausted its quota stops occupying the head of
	// every shard (D24). Rows under a live lease are left to their claimer.
	DeferTenantChannel(ctx context.Context, realm domain.Realm, t domain.Tenant, ch domain.ChannelKind, until time.Time) (int, error)

	// Cancel withdraws one notification: its inbox row is marked canceled and
	// every pending delivery no claimer holds becomes canceled. Deliveries under
	// a live lease are reported as in flight, never recorded as canceled (D29).
	Cancel(ctx context.Context, id domain.Identity, idemKey string) (domain.CancelReceipt, error)
}

// Digests holds coalescing windows (D4).
type Digests interface {
	// AppendDigest folds a notification into its recipient's open window for
	// (topic, channel), opening one if none is open and sealing it at max
	// members. Returns the window id.
	AppendDigest(ctx context.Context, m domain.DigestMember, window time.Duration, max int) (string, error)

	// DueDigests lists windows in one shard whose flush time has passed.
	DueDigests(ctx context.Context, shard domain.Shard, max int) ([]string, error)

	// FlushDigest seals a window and enqueues its one delivery. A second call is
	// a no-op that returns false, so a duplicate tick never sends two digests.
	FlushDigest(ctx context.Context, windowID string) (queued bool, err error)
}

// Maintenance holds the single-owner jobs' operations (D11, D21, D22, D35).
type Maintenance interface {
	// TryLeaseJob takes or renews the lease for a scheduled job. True when owner
	// now holds it; false while another owner's lease is live.
	TryLeaseJob(ctx context.Context, job, owner string, ttl time.Duration) (bool, error)

	// RehomeShards folds rows in shards >= shards into live ones, after the
	// shard count is lowered. Safe while claimers run.
	RehomeShards(ctx context.Context, shards int) (int, error)

	// ExpireIdem deletes up to batch idempotency keys created before olderThan.
	ExpireIdem(ctx context.Context, olderThan time.Time, batch int) (int, error)

	// ExpireDigests deletes up to batch flushed windows older than olderThan
	// whose delivery has finished.
	ExpireDigests(ctx context.Context, olderThan time.Time, batch int) (int, error)

	// Erase removes one recipient's personal data from every table that holds
	// it, in one transaction, and records the erasure by hash. Suppressions are
	// kept: they hold address hashes, and dropping them would resume mailing
	// someone who complained. Returns rows removed per table.
	Erase(ctx context.Context, id domain.Identity) (map[string]int, error)

	// EnsurePartitions creates each missing monthly partition of the
	// range-partitioned tables, for months months from the one containing from,
	// and scopes every new notifications partition (D17). Returns the partitions
	// created; a second run creates none.
	EnsurePartitions(ctx context.Context, from time.Time, months int) ([]string, error)

	// CheckPartitions reports, from the catalog alone, each range-partitioned
	// table with no partition covering the month containing at or the next one,
	// and notifications or any of its partitions that recipient scoping does not
	// hold on. It changes nothing; the report is what the notify.partition.missing
	// and notify.partition.unscoped alarms fire on.
	CheckPartitions(ctx context.Context, at time.Time) (domain.PartitionHealth, error)
}

// Store is the persist and read path: what Notify writes and what a claim
// delivers from.
type Store interface {
	// PersistAndEnqueue writes, in one transaction and under the identity's
	// transaction-local scope (D17): the notify_idem guard first (a conflict is a
	// replay, and nothing else is written), the inbox row, one queue row per
	// immediate or deferred delivery, one digest append per digested channel, and
	// one dropped_quota record per dropped channel. tx is nil for Notify, which
	// opens and commits its own transaction; for NotifyTx it is the caller's,
	// used as-is and never committed or rolled back here (D28).
	PersistAndEnqueue(ctx context.Context, tx Tx, id domain.Identity, n domain.Notification, plan domain.DeliveryPlan) (domain.Receipt, error)

	// Load reads what a claim delivers — the notification, or a digest's
	// members — scoped to the claim's identity. Canceled members are omitted.
	// canceled is true when the notification is canceled, or when every member
	// of a digest is, so the dispatcher finishes it as canceled instead of
	// sending it.
	Load(ctx context.Context, c domain.Claim) (n *domain.Notification, digest []domain.Notification, canceled bool, err error)

	// Status reads one notification's deliveries by channel and address.
	Status(ctx context.Context, id domain.Identity, idemKey string) (domain.NotificationStatus, error)

	// CreateBroadcast records a broadcast, idempotently on its key (NR-020).
	CreateBroadcast(ctx context.Context, realm domain.Realm, b domain.BroadcastRequest) (domain.BroadcastReceipt, error)

	// LeaseBroadcast takes the next broadcast with pages left, or nil when there
	// is none.
	LeaseBroadcast(ctx context.Context, lease time.Duration) (*domain.BroadcastJob, error)

	// CommitBroadcastPage persists a page's notifications together with the
	// cursor advance, in one transaction, so a crash resumes at the next page and
	// notifies nobody twice (D7).
	CommitBroadcastPage(ctx context.Context, job domain.BroadcastJob, page []domain.PageItem, nextCursor string, done bool) error

	// RecordProviderStatus records a provider's delivery callback against the
	// delivery it concerns, found by provider message id (NR-032).
	RecordProviderStatus(ctx context.Context, ch domain.ChannelKind, providerMessageID, status string) error
}

// InboxStore is the recipient read and write path. Every call runs under the
// identity's scope.
type InboxStore interface {
	List(ctx context.Context, id domain.Identity, q domain.ListQuery) (domain.Page, error)
	// Unread counts unread, visible, unarchived, uncanceled notifications, and
	// stops at limit (D15).
	Unread(ctx context.Context, id domain.Identity, limit int) (domain.UnreadCount, error)
	// Get returns one of the identity's notifications, or nil when it is not
	// theirs: a caller cannot tell "absent" from "someone else's".
	Get(ctx context.Context, id domain.Identity, notificationID string) (*domain.Notification, error)
	MarkSeen(ctx context.Context, id domain.Identity, before time.Time) error
	MarkRead(ctx context.Context, id domain.Identity, ids []string) error
	MarkAllRead(ctx context.Context, id domain.Identity) error
	Archive(ctx context.Context, id domain.Identity, ids []string, archived bool) error
}

// PreferenceStore resolves recipient, then tenant, then topic default (D30).
type PreferenceStore interface {
	Resolve(ctx context.Context, id domain.Identity, topic domain.Topic, def domain.TopicDef) ([]domain.ResolvedPreference, error)
	Schedule(ctx context.Context, id domain.Identity) (domain.DeliverySchedule, error)
	Set(ctx context.Context, id domain.Identity, p []domain.Preference) error
	SetSchedule(ctx context.Context, id domain.Identity, s domain.DeliverySchedule) error
	SetTenant(ctx context.Context, realm domain.Realm, p domain.TenantPreference) error
}

// TopicRegistry holds topic defaults: in code in library mode, in
// notification_topics in service mode. It is realm-aware because one service
// serves several realms (NR-017).
type TopicRegistry interface {
	Lookup(ctx context.Context, realm domain.Realm, topic domain.Topic) (domain.TopicDef, bool, error)
}

// TemplateRenderer is the one home of copy, locale and branding (NR-019, D27).
type TemplateRenderer interface {
	// Render returns domain.ErrNoTemplate when nothing matches after locale
	// fallback (language tag, then language, then the default locale); the
	// dispatcher then uses the notification's fallback title and body, or records
	// "rejected" when there are none.
	Render(ctx context.Context, r domain.RenderRequest) (domain.RenderedContent, error)
}

// AddressBook resolves an identity to its addresses on one channel: zero, one or
// many (D25).
type AddressBook interface {
	Resolve(ctx context.Context, id domain.Identity, ch domain.ChannelKind) ([]domain.Address, error)
}

// AudienceResolver expands a host audience selector, in pages (D7).
type AudienceResolver interface {
	Resolve(ctx context.Context, realm domain.Realm, t domain.Tenant, audience, cursor string) (
		recipients []domain.Recipient, nextCursor string, err error)
}

// SuppressionStore is consulted by the dispatcher before every delivery, for
// every channel (D13). Its keys are address hashes, never addresses (D35).
type SuppressionStore interface {
	Suppressed(ctx context.Context, realm domain.Realm, ch domain.ChannelKind, addressHash []byte) (bool, error)
	// Suppress records an address as dead until `until`; the zero time is
	// permanent (D14).
	Suppress(ctx context.Context, realm domain.Realm, ch domain.ChannelKind, addressHash []byte,
		reason domain.SuppressionReason, until time.Time) error
}

// QuotaCounter is the hot per-(realm, tenant, channel) budget (D5, D24). Losing
// its state resets budgets: it fails open on quota, never on correctness.
type QuotaCounter interface {
	// Peek reports whether the budget is exhausted without consuming any of it.
	Peek(ctx context.Context, k domain.QuotaKey) (exhausted bool, resetAt time.Time, err error)
	// Take consumes one unit of the budget if any is left.
	Take(ctx context.Context, k domain.QuotaKey) (allowed bool, resetAt time.Time, err error)
}

// PreCheck is the optional fast duplicate check (D18). A nil PreCheck means
// every Notify takes the durable path and stays correct. Its keys are the
// durable guard's key space, hashed, and it can only ever miss.
type PreCheck interface {
	Seen(ctx context.Context, key string) (notificationID string, ok bool, err error)
	// Remember is called after commit only, and never for NotifyTx.
	Remember(ctx context.Context, key, notificationID string, ttl time.Duration) error
}

// StreamPublisher carries the live nudge (D16): a notification id on the stream
// keyed by the hash of the identity, and nothing else.
type StreamPublisher interface {
	Publish(ctx context.Context, id domain.Identity, notificationID string) error
}

// Channel is one delivery target. Adding one is implementing this and
// registering it, never an edit to fan-out, persistence, preferences,
// deduplication or the schema (NR-016).
type Channel interface {
	Kind() domain.ChannelKind

	// Deliver sends one delivery to one address. It must be idempotent on
	// d.IdemKey at the level Capabilities().Dedup declares. It reports a dead
	// address as Suppressed and a permanent refusal as Rejected, never as an
	// error; a returned error means an infrastructure fault and is treated as
	// Retry.
	//
	// It does not check suppressions (the dispatcher does, for every channel: D13)
	// and does not retry internally (the dispatcher owns backoff: retrying inside
	// a channel defeats the jitter that prevents a thundering herd: D10).
	Deliver(ctx context.Context, d domain.Delivery) (domain.DeliveryResult, error)

	Capabilities() domain.ChannelCapabilities
}

// AddressNormalizer is optional: a channel that implements it defines address
// equality (lowercase an email domain, E.164 a number), and its output is what
// domain.AddressKey and domain.SuppressionHash hash.
type AddressNormalizer interface {
	NormalizeAddress(value string) string
}

// ChannelRegistry maps a ChannelKind to its Channel. Register is a wiring-time
// call.
type ChannelRegistry interface {
	Register(c Channel)
	Get(kind domain.ChannelKind) (Channel, bool)
	Kinds() []domain.ChannelKind
}

// Clock is the engine's source of time. Tests inject a fake one, so nothing
// sleeps.
type Clock interface {
	Now() time.Time
}

// IDSource issues identifiers: UUIDv7 by default, so they sort by creation time.
type IDSource interface {
	New() string
}
