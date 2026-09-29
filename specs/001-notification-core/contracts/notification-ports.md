# Contract: Notification & Multi-Channel Delivery (reusable ports)

**Plan**: [../plan.md](../plan.md) · **Spec**: [../spec.md](../spec.md) · **Decisions**:
[../design-decisions.md](../design-decisions.md) · **Status**: normative.

This is the core contract: the domain types, every port, the invariants, the contract-test suites,
the module layout, the lint gates that keep it standalone, and the gRPC facade. It began as a
reusability seam *inside* the product this engine was extracted from and was carried here with its
git history ([PROVENANCE.md](../../../PROVENANCE.md)). It was then revised by the architecture review
recorded as D16–D36. That review found the carried version still described a unique constraint D2
proves cannot exist, a live push channel that leaked across tenants, and a worker that could not read
what it delivers. Where this contract and prose elsewhere disagree, this contract wins, so it has to
be right.

**One behaviour changed during the lift.** The originating fan-out did a durable `INSERT` *and* an
in-app publish *and* an email enqueue that were **not transactional**, guarded by a Redis `SET NX`
that short-circuited the entire handler — so a crash between the guard and the email lost that email
permanently. This contract replaces it with a transactional outbox (NR-003, NR-004), and the review
closed the two routes by which the same bug had crept back: a pre-check written before commit, and a
`direct` delivery mode ([D18](../design-decisions.md#d18), [D22](../design-decisions.md#d22)).

---

## Why: the four couplings this removes

| # | The originating coupling | The port that removes it |
|---|---|---|
| 1 | Recipient welded to `(workspace_id, user_id)` — columns, RLS predicate, Redis key and subject all assumed a user in a workspace | `Recipient` + `Tenant` inside a `Realm` — opaque identities the engine never interprets |
| 2 | Channels hard-coded to in-app + email as an inline `if` ladder | `Channel` + `ChannelRegistry` — the fan-out iterates a registry and reads capabilities, never a channel kind |
| 3 | `category` as a 13-value Postgres enum — every event type an `ALTER TYPE` | `Topic` + `TopicRegistry` — a registered string with data-driven defaults |
| 4 | Copy and rendering welded to the email worker — no locale, branding or override seam | `TemplateRenderer` — the one home for copy, locale and branding; producers send data |

The rule: **the notification kernel is generic; only the channels, the topic registry, the identity
binding, the templates and the directory are product-specific** — and in service mode even those are
data the service owns ([D31](../design-decisions.md#d31)).

---

## Ports at a glance

```text
PRODUCERS — thin: build a Notification, call Notify (or NotifyTx inside their own transaction)
   │  Notify / NotifyTx / Broadcast / Cancel / Status
   ▼
┌── Notifier (app) ─────────────────────────────────────────────────────────────┐
│  TopicRegistry.Lookup → PreferenceStore.Resolve → Schedule → QuotaCounter.Peek │
│      → DeliveryPlan {inbox_visible, per-channel: immediate|deferred|digest|drop}│
│  PreCheck.Seen (read-only)                                                     │
│  Store.PersistAndEnqueue(identity, n, plan) — ONE transaction:                 │
│      notify_idem guard · inbox row · queue rows · digest appends · drop records│
│  after commit: PreCheck.Remember · wake local dispatcher                       │
└───────────────────────────────────┬───────────────────────────────────────────┘
                                    │ notification_outbox (pending work only)
                                    ▼
┌── Dispatcher (worker) ────────────────────────────────────────────────────────┐
│  Store.Claim(shard) — SKIP LOCKED + lease + fencing token                      │
│  expired? canceled? → finish                                                   │
│  AddressBook.Resolve → SuppressionStore filter → Store.BindAddresses (1 or N)  │
│  QuotaCounter.Take → exhausted: defer row + move tenant backlog                │
│  TemplateRenderer.Render → Channel.Deliver(Delivery{IdemKey per address})      │
│  Queue.Retry | Defer | Finish(domain.NewFinish(outcome)) — each one fenced     │
│      → notification_deliveries (+ dead_letters, + next fallback channel)       │
└───────────────────────────────────────────────────────────────────────────────┘
   in_app  → StreamPublisher nudge {id} on hash(identity) → relay re-reads under RLS
   email   → provider (Idempotency-Key where supported; List-Unsubscribe for non-essential)
   sms · push · slack · webhook · failover(providers…) · yours
```

---

## Domain types

```go
package notify

import (
	"context"
	"errors"
	"time"
)

// Realm is WHICH PRODUCT — the outermost isolation axis (D1). Never taken from a
// request: library mode reads Config.Realm; service mode derives it from the
// authenticated producer (D31). Lowercase [a-z0-9-], validated.
type Realm string

// Tenant is the host's ISOLATION boundary — workspace, organization, account.
// Opaque: the engine scopes by it and derives keys from it, and never parses it.
type Tenant struct {
	Kind string
	ID   string
}

// Recipient is the host's DELIVERY SUBJECT — user, device, slack_channel, email.
// Opaque. "A recipient within a tenant" is the unit of isolation, so the same user
// in two workspaces is two recipients-within-tenant.
type Recipient struct {
	Kind string
	ID   string
}

// Identity is the full scoping tuple. Every row, key and stream is derived from it.
type Identity struct {
	Realm     Realm
	Tenant    Tenant
	Recipient Recipient
}

// Canonical encodes parts unambiguously: each as <octet length>:<part>, joined by
// ",". It is the only way the engine turns identities into strings (D18). It is
// tested against a frozen vector computed independently of the code:
//   Canonical("aisat","workspace","w1","user","u1") == "5:aisat,9:workspace,2:w1,4:user,2:u1"
func Canonical(parts ...string) string

func (i Identity) Canonical() string {
	return Canonical(string(i.Realm), i.Tenant.Kind, i.Tenant.ID, i.Recipient.Kind, i.Recipient.ID)
}

// Topic is a REGISTERED notification type — a string, never a DB enum (NR-017).
type Topic string

// Priority: "info" | "warning" | "critical". critical ignores quiet hours (D9) and is
// never digested or dropped by quota (D5).
type Priority string

// ChannelKind selects a Channel in the registry. The engine never branches on one;
// it reads the channel's Capabilities (invariant 13).
type ChannelKind string

// Notification is what a producer builds. It carries no Realm (see Realm).
type Notification struct {
	Tenant    Tenant
	Recipient Recipient
	Topic     Topic
	Priority  Priority // "" ⇒ the topic's default

	// Data is the template variables. Copy lives in the TemplateRenderer (D27).
	Data map[string]any
	// Title and Body are OPTIONAL fallback copy, used only when no template exists
	// for (topic, channel, locale).
	Title string
	Body  string

	Payload    map[string]string // deep-link refs for the UI — never a routing input
	Attributes map[string]string // trace_id, source — audit only, never a routing input

	// IdemKey is REQUIRED: the producer's exactly-once identity for this event and
	// recipient, e.g. "invoice:42:overdue". 1–255 bytes.
	IdemKey string

	OccurredAt    time.Time
	DeliverAfter  time.Time // zero ⇒ now. A scheduled send (D29)
	DeliverBefore time.Time // zero ⇒ no expiry. Past it: terminal "expired" (D29)
}

// Address is WHERE one channel delivers, resolved from an Identity — never carried on
// the Notification. A recipient may have several per channel (D25).
type Address struct {
	Channel  ChannelKind
	Value    string            // email, E.164 number, device token, webhook URL
	Locale   string            // BCP-47; "" ⇒ tenant default ⇒ Config.DefaultLocale
	Timezone string            // IANA; used for quiet hours when present
	Meta     map[string]string // provider hints
}

// Key is the address's stable identity: hex(sha256(Canonical(channel, normalized value)))[:32].
// Normalization is the channel's (lowercase an email domain, E.164 a number) when it
// implements AddressNormalizer; otherwise the trimmed value.
func (a Address) Key(n AddressNormalizer) string

// TopicDef is a topic's registered defaults.
type TopicDef struct {
	DefaultChannels []ChannelKind // delivered in parallel, each subject to preferences
	Fallback        []ChannelKind // tried in order while each ends undelivered (D29)
	DefaultPriority Priority
	Essential       bool   // cannot be disabled, never digested, no unsubscribe affordance
	TemplateRef     string // "" ⇒ the topic name
}

// DeliverySchedule drives deferral and coalescing. A channel never sees it.
type DeliverySchedule struct {
	QuietStart string        // "22:00" local; "" ⇒ none
	QuietEnd   string        // "07:00" local
	Timezone   string        // IANA
	Digest     time.Duration // 0 ⇒ immediate; >0 ⇒ coalesce same-topic into a window
}

// PlanMode is what the Notifier decided for one channel at notify time.
type PlanMode int

const (
	PlanImmediate PlanMode = iota + 1 // due now (or at DeliverAfter)
	PlanDeferred                      // due at NotBefore: quiet hours or an exhausted quota
	PlanDigest                        // folded into the recipient's open digest window
	PlanDropped                       // quota policy drop_non_essential: recorded, not sent
)

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
	InboxVisible bool // any enabled channel is InboxBacked (D26)
	VisibleFrom  time.Time
	Deliveries   []PlannedDelivery
	DigestWindow time.Duration
	DigestMax    int
	Shard        Shard
}

// Receipt is the outcome of Notify. Applied=false ⇒ a replay of a key already
// persisted inside the idempotency window: nothing was written.
type Receipt struct {
	NotificationID string
	IdemKey        string
	Applied        bool
	InboxVisible   bool
	Deliveries     []PlannedDelivery // what was planned on first apply; empty on replay
}

// Shard is a contention hint for claimers, NOT a correctness boundary (D11):
// crc32(identity.Canonical()) mod Config.Shards, stored on the row at insert.
type Shard int16

// OutboxEntry is one pending delivery as a claimer sees it.
type OutboxEntry struct {
	ID                    string
	NotificationID        string // "" for a digest
	NotificationCreatedAt time.Time
	DigestID              string // "" for a notification
	Identity              Identity
	Topic                 Topic
	Priority              Priority
	Channel               ChannelKind
	AddressKey            string   // "" until bound (D25)
	Address               *Address // nil until bound
	Fallback              []ChannelKind
	Shard                 Shard
	Attempts              int // counted at claim (D19)
	Deferrals             int
	DeliverBefore         time.Time
	QuietHoursOverride    bool
}

// Claim is an OutboxEntry held under a lease. Every outcome write presents LeaseToken;
// a write with a stale token is discarded (D19).
type Claim struct {
	Entry      OutboxEntry
	LeaseToken string
}

// Delivery is one entry bound to one address, rendered — the unit a Channel sends.
type Delivery struct {
	ID      string // the queue row
	IdemKey string // what the channel dedupes on — NOT Notification.IdemKey (D25):
	//   hex(sha256(Canonical(realm, tenant, recipient, idem_key | "digest:"+id, channel, address_key)))
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

// Outcome is what a channel reports.
type Outcome int

const (
	Delivered  Outcome = iota + 1
	Retry              // transient: the dispatcher backs off (D10)
	Suppressed         // this address is dead: terminal; the engine records the suppression (D13)
	Rejected           // permanent refusal unrelated to the address: terminal, dead-lettered (D12)
)

type DeliveryResult struct {
	Outcome           Outcome
	ProviderMessageID string            // correlates provider callbacks (D20)
	SuppressReason    SuppressionReason // with Suppressed
	SuppressFor       time.Duration     // with Suppressed: 0 ⇒ permanent (D14)
	RetryAfter        time.Duration     // with Retry: a provider hint, a floor on backoff (D10)
	Detail            string
}

type SuppressionReason string // hard_bounce | complaint | unsubscribe | invalid_address | token_revoked | soft_bounce

// TerminalOutcome is the vocabulary of notification_deliveries.outcome (D12).
type TerminalOutcome string // delivered | suppressed | no_address | max_attempts | rejected | expired | canceled | dropped_quota

// UnreadCount is bounded (D15): Count stops at Config.UnreadCap, and Capped says so.
type UnreadCount struct {
	Count  int
	Capped bool
}

type BroadcastRequest struct {
	Tenant     Tenant
	Audience   string      // host selector, resolved in pages by the AudienceResolver
	Recipients []Recipient // OR an inline list, at most Config.MaxInlineRecipients (D31)
	Topic      Topic
	Priority   Priority
	Data       map[string]any
	Title      string
	Body       string
	Payload    map[string]string
	// IdemKey is REQUIRED. Recipient r's key is derived as
	// hex(sha256(Canonical("broadcast", IdemKey, r.Kind, r.ID))), so a retried
	// broadcast notifies nobody twice.
	IdemKey string
}

type BroadcastReceipt struct {
	BroadcastID string
	Applied     bool // false ⇒ this broadcast key was already accepted
}

type CancelRequest struct {
	Tenant    Tenant
	Recipient Recipient
	IdemKey   string
}

type CancelReceipt struct {
	Matched  bool
	Canceled int // pending deliveries stopped
	InFlight int // under a live lease: may already be at the provider (D29)
}

type StatusRequest = CancelRequest

type DeliveryStatus struct {
	Channel           ChannelKind
	AddressKey        string
	State             string // "pending" or a TerminalOutcome, or "fanned_out"
	Attempts          int
	ProviderMessageID string
	ProviderStatus    string // latest provider callback
	CompletedAt       time.Time
}

type NotificationStatus struct {
	Found          bool
	NotificationID string
	Canceled       bool
	Deliveries     []DeliveryStatus
}

// Tx is a transaction handle the configured Store understands (pgx.Tx for the
// PostgreSQL store). Opaque here, so the core imports no driver.
type Tx = any

var (
	ErrTxUnsupported = errors.New("notify: this Notifier cannot join a caller's transaction")
	ErrNoTemplate    = errors.New("notify: no template for topic, channel and locale")
	ErrUnknownTopic  = errors.New("notify: topic is not registered")
	ErrMissingIdem   = errors.New("notify: IdemKey is required")
)
```

---

## Port: `Channel` — the pluggable delivery target

```go
// Channel is ONE delivery target. Adding one is implementing this and registering it —
// never an edit to fan-out, persistence, preferences, deduplication or the schema (NR-016).
type Channel interface {
	Kind() ChannelKind

	// Deliver sends ONE delivery to ONE address. It MUST be idempotent on d.IdemKey at
	// the level Capabilities().Dedup declares. It reports a dead address as Suppressed
	// and a permanent refusal as Rejected — never as an error. A returned error means
	// an infrastructure fault and is treated as Retry.
	//
	// It does NOT check suppressions (the dispatcher does, for every channel — D13) and
	// does NOT retry internally (the dispatcher owns backoff; retrying inside a channel
	// defeats the jitter that prevents a thundering herd — D10).
	Deliver(ctx context.Context, d Delivery) (DeliveryResult, error)

	Capabilities() ChannelCapabilities
}

type ChannelCapabilities struct {
	NeedsSubject bool // render a subject (email, push title)
	NeedsAddress bool // false ⇒ addressed by the recipient identity alone (in_app)
	RichContent  bool // consumes RenderedContent.Data (Slack blocks, push payload)
	InboxBacked  bool // enabling this channel makes the notification visible in the inbox (D26)

	// Dedup is the strength of this channel's idempotency on Delivery.IdemKey (D32).
	Dedup       DedupLevel
	DedupWindow time.Duration // with DedupProvider: how long the provider remembers a key
}

type DedupLevel int

const (
	DedupNone     DedupLevel = iota // at-least-once: a crash between send and record may resend
	DedupLocal                      // the channel records sends itself, or is idempotent by construction
	DedupProvider                   // the provider honors an idempotency key for DedupWindow
)

// AddressNormalizer is optional: a channel that implements it defines address equality.
type AddressNormalizer interface {
	NormalizeAddress(value string) string
}

// ChannelRegistry maps a ChannelKind to its Channel. Register is a wiring-time call.
type ChannelRegistry interface {
	Register(c Channel)
	Get(kind ChannelKind) (Channel, bool)
	Kinds() []ChannelKind
}
```

**Why this is the whole game.** Fan-out is a loop over enabled channels that reads capabilities.
Adding SMS is one `Channel` and one `Register` line, with zero edits to persistence, preferences,
idempotency, the dead-letter path or retention — and in service mode it is one `channel_providers`
row.

---

## Driving ports

```go
// Notifier is the single entry point producers depend on.
type Notifier interface {
	// Notify persists and enqueues ONE notification in one transaction, idempotently on
	// (realm, tenant, recipient, IdemKey) within the idempotency window. A replay writes
	// nothing and returns Applied=false. The inbox row is ALWAYS written; preferences
	// decide channels and inbox visibility. Returns after commit (D23).
	Notify(ctx context.Context, n Notification) (Receipt, error)

	// NotifyTx does the same inside a transaction the CALLER owns and commits (D28). The
	// engine never commits or rolls it back. Returns ErrTxUnsupported from a remote Notifier.
	NotifyTx(ctx context.Context, tx Tx, n Notification) (Receipt, error)

	// Broadcast records a durable broadcast and returns; expansion runs in the worker, a
	// page per transaction, resumable after a crash (NR-020, D7).
	Broadcast(ctx context.Context, b BroadcastRequest) (BroadcastReceipt, error)

	// Cancel withdraws one notification: pending deliveries stop; in-flight ones are
	// reported, never falsely recorded as canceled (D29).
	Cancel(ctx context.Context, c CancelRequest) (CancelReceipt, error)

	// Status answers "was it delivered?" per channel and address.
	Status(ctx context.Context, s StatusRequest) (NotificationStatus, error)
}

// Inbox is the recipient-facing surface the REST adapter drives. Every method takes the
// caller's Identity from its authenticated session — never from a parameter (NR-008).
type Inbox interface {
	List(ctx context.Context, id Identity, q ListQuery) (Page, error)
	Unread(ctx context.Context, id Identity) (UnreadCount, error)
	MarkSeen(ctx context.Context, id Identity, before time.Time) error
	MarkRead(ctx context.Context, id Identity, notificationIDs []string) error
	MarkAllRead(ctx context.Context, id Identity) error
	Archive(ctx context.Context, id Identity, notificationIDs []string, archived bool) error
	Preferences(ctx context.Context, id Identity) ([]ResolvedPreference, error)
	SetPreferences(ctx context.Context, id Identity, p []Preference) error
	Schedule(ctx context.Context, id Identity) (DeliverySchedule, error)
	SetSchedule(ctx context.Context, id Identity, s DeliverySchedule) error
	Unsubscribe(ctx context.Context, token string) error // RFC 8058 one-click (D34)
}

// Admin is the operator and management surface.
type Admin interface {
	Erase(ctx context.Context, id Identity) (map[string]int, error)               // D35
	ReplayDeadLetter(ctx context.Context, realm Realm, deadLetterID string) error // idempotent
	SetTenantPreference(ctx context.Context, realm Realm, p TenantPreference) error
}

// Jobs runs the single-owner jobs — retention, partition provisioning, idempotency
// and digest expiry, digest flush, quota rollover, broadcast expansion — each under a
// notify_job_leases lease that it renews while it runs, so any number of workers may
// call Run and each job still has one runner at a time (D22). It drives the driven
// Maintenance and Digests ports.
type Jobs interface {
	Run(ctx context.Context) error
}

// Dispatcher drains the queue. Runs in the worker role.
type Dispatcher interface {
	// Run claims across every shard until ctx ends, adaptively: again at once after a
	// full batch, backing off to Config.PollInterval when empty (D22).
	Run(ctx context.Context) error
	// Drain processes one batch from one shard — the unit Run repeats, exposed for tests.
	Drain(ctx context.Context, shard Shard, max int) (processed int, err error)
}
```

`ListQuery`, `Page`, `Preference`, `ResolvedPreference` (value plus `Source`:
`recipient|tenant|topic_default` and `Locked`) and `TenantPreference` are plain structs mirroring
[rest-api.md](./rest-api.md).

---

## Driven ports

```go
// Queue, Digests and Maintenance are implemented: ports/driven.go holds them and
// adapters/driven/postgres implements them as plain SQL, tested against PostgreSQL
// by the integration suite (D37). The Go declarations there are normative; they are
// summarized here. Every method taking a Claim is FENCED: it changes state only
// while the claim's lease token is current, and reports ok=false, having written
// nothing, when the lease was lost (D19).
type Queue interface {
	Claim(ctx context.Context, shard Shard, max int, lease time.Duration) ([]Claim, error) // SKIP LOCKED lease; counts the attempt
	Retry(ctx context.Context, c Claim, next time.Time, lastError string) (ok bool, err error)
	Defer(ctx context.Context, c Claim, until time.Time, reason string) (ok bool, err error) // returns the attempt
	// Finish is the terminal transition in ONE statement: queue → history, plus the
	// dead letter and next fallback channel that f.Disposition carries. The store
	// decides neither; f comes from domain.NewFinish, which applies DispositionOf.
	Finish(ctx context.Context, c Claim, f Finish) (ok bool, err error)
	BindAddresses(ctx context.Context, c Claim, addrs []Address, keys []string) (b Binding, ok bool, err error) // D25
	DeferTenantChannel(ctx context.Context, realm Realm, t Tenant, ch ChannelKind, until time.Time) (int, error) // D24
	Cancel(ctx context.Context, id Identity, idemKey string) (CancelReceipt, error)                              // D29
}

type Digests interface {
	AppendDigest(ctx context.Context, m DigestMember, window time.Duration, max int) (windowID string, err error)
	DueDigests(ctx context.Context, shard Shard, max int) ([]string, error)
	FlushDigest(ctx context.Context, windowID string) (queued bool, err error) // idempotent (D4)
}

type Maintenance interface {
	TryLeaseJob(ctx context.Context, job, owner string, ttl time.Duration) (bool, error) // D22
	RehomeShards(ctx context.Context, shards int) (int, error)                           // D11
	ExpireIdem(ctx context.Context, olderThan time.Time, batch int) (int, error)         // D21
	ExpireDigests(ctx context.Context, olderThan time.Time, batch int) (int, error)      // D4
	Erase(ctx context.Context, id Identity) (map[string]int, error)                      // D35
	EnsurePartitions(ctx context.Context, from time.Time, months int) ([]string, error)  // scopes new partitions (D17)
	// Still to come: RetirePartitions (T038), ReplayDeadLetter (T032).
}

// Store is the persist and read path, still to be implemented (T015–T017).
type Store interface {
	// PersistAndEnqueue writes, in ONE transaction and under the identity's
	// transaction-local scope (D17): the notify_idem guard first (a conflict ⇒ replay,
	// nothing else written), the inbox row, one queue row per immediate or deferred
	// delivery, one digest append per digested channel, one dropped_quota record per
	// dropped channel. tx is nil for Notify, which opens and commits its own
	// transaction; for NotifyTx it is the caller's, used as-is and never committed or
	// rolled back here (D28).
	PersistAndEnqueue(ctx context.Context, tx Tx, id Identity, n Notification, plan DeliveryPlan) (Receipt, error)

	// Load reads what a claim delivers — the notification, or a digest's members —
	// scoped to the claim's identity. Canceled members are omitted.
	Load(ctx context.Context, c Claim) (n *Notification, digest []Notification, canceled bool, err error)

	Status(ctx context.Context, id Identity, idemKey string) (NotificationStatus, error)

	// Broadcast jobs: create (idempotent on the broadcast key), lease the next page,
	// and commit a page's notifications together with the cursor advance.
	CreateBroadcast(ctx context.Context, realm Realm, b BroadcastRequest) (BroadcastReceipt, error)
	LeaseBroadcast(ctx context.Context, lease time.Duration) (*BroadcastJob, error)
	CommitBroadcastPage(ctx context.Context, job BroadcastJob, page []PageItem, nextCursor string, done bool) error

	RecordProviderStatus(ctx context.Context, ch ChannelKind, providerMessageID, status string) error
}

// InboxStore is the recipient read/write path. Every call runs under the identity's scope.
type InboxStore interface {
	List(ctx context.Context, id Identity, q ListQuery) (Page, error)
	Unread(ctx context.Context, id Identity, cap int) (UnreadCount, error) // bounded (D15)
	Get(ctx context.Context, id Identity, notificationID string) (*Notification, error)
	MarkSeen(ctx context.Context, id Identity, before time.Time) error
	MarkRead(ctx context.Context, id Identity, ids []string) error
	MarkAllRead(ctx context.Context, id Identity) error
	Archive(ctx context.Context, id Identity, ids []string, archived bool) error
}

// PreferenceStore resolves recipient → tenant → topic default (D30).
type PreferenceStore interface {
	Resolve(ctx context.Context, id Identity, topic Topic, def TopicDef) ([]ResolvedPreference, error)
	Schedule(ctx context.Context, id Identity) (DeliverySchedule, error)
	Set(ctx context.Context, id Identity, p []Preference) error
	SetSchedule(ctx context.Context, id Identity, s DeliverySchedule) error
	SetTenant(ctx context.Context, realm Realm, p TenantPreference) error
}

// TopicRegistry holds topic defaults. In-code in library mode; notification_topics in
// service mode. Realm-aware because one service serves several realms.
type TopicRegistry interface {
	Lookup(ctx context.Context, realm Realm, topic Topic) (TopicDef, bool, error)
}

// TemplateRenderer is the ONE home of copy, locale and branding (NR-019, D27).
type TemplateRenderer interface {
	// Render returns ErrNoTemplate when nothing matches after locale fallback
	// (language-tag → language → Config.DefaultLocale); the dispatcher then uses the
	// notification's fallback Title/Body, or records "rejected" if there is none.
	Render(ctx context.Context, r RenderRequest) (RenderedContent, error)
}

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

type RenderedContent struct {
	Subject         string
	Body            string
	Data            map[string]any // structured fields for rich channels
	TemplateVersion int
}

// AddressBook resolves an identity to its addresses on one channel — zero, one or many (D25).
type AddressBook interface {
	Resolve(ctx context.Context, id Identity, ch ChannelKind) ([]Address, error)
}

// AudienceResolver expands a host audience selector, in pages (D7).
type AudienceResolver interface {
	Resolve(ctx context.Context, realm Realm, t Tenant, audience, cursor string) (
		recipients []Recipient, nextCursor string, err error)
}

// SuppressionStore is consulted by the dispatcher before EVERY delivery (D13). Keys are
// address hashes, never addresses (D35).
type SuppressionStore interface {
	Suppressed(ctx context.Context, realm Realm, ch ChannelKind, addressHash []byte) (bool, error)
	Suppress(ctx context.Context, realm Realm, ch ChannelKind, addressHash []byte,
		reason SuppressionReason, until time.Time) error
}

// QuotaCounter is the hot per-(realm, tenant, channel) budget (D5, D24). Losing its state
// resets budgets — fail-open on quota, never on correctness.
type QuotaCounter interface {
	Peek(ctx context.Context, k QuotaKey) (exhausted bool, resetAt time.Time, err error)
	Take(ctx context.Context, k QuotaKey) (allowed bool, resetAt time.Time, err error)
}

// PreCheck is the OPTIONAL fast duplicate check (D18). nil ⇒ every Notify takes the
// durable path, and stays correct. Keys are the durable guard's key space, hashed.
type PreCheck interface {
	Seen(ctx context.Context, key string) (notificationID string, ok bool, err error)
	Remember(ctx context.Context, key, notificationID string, ttl time.Duration) error // after commit ONLY
}

// StreamPublisher carries the live nudge (D16): an id on hash(identity), nothing else.
type StreamPublisher interface {
	Publish(ctx context.Context, id Identity, notificationID string) error
}

type Clock interface{ Now() time.Time }
type IDSource interface{ New() string } // UUIDv7
```

---

## The dispatcher, step by step

The algorithm every `Dispatcher` implements. Each numbered step names the outcome it can record.

1. **Claim** a batch from a shard (`Queue.Claim`). Attempts are counted here (D19).
2. **Expired?** `DeliverBefore` passed → `expired`.
3. **Load** the notification or the digest's members under the claim's scope (D17). Canceled →
   `canceled`. A digest whose members are all canceled → `canceled`.
4. **Address.** If `NeedsAddress` and unbound: `AddressBook.Resolve`, drop addresses with a live
   suppression. None at all → `no_address`; none left after suppression → `suppressed`. One → bind in
   place and continue. Several → fan out; the claim is spent and each address is claimed on its own
   (D25). If not `NeedsAddress`, the address is the identity (in-app).
5. **Quota.** `QuotaCounter.Take`. Exhausted → under `drop_non_essential` for a non-essential,
   non-critical topic, `dropped_quota`; otherwise defer this row to the reset and move the tenant's
   due backlog on this channel with it (D24). Deferral returns the attempt.
6. **Render** in the address's locale, falling back to the tenant default and `DefaultLocale`.
   `ErrNoTemplate` → the notification's fallback copy; no fallback → `rejected`.
7. **Deliver** with `Delivery.IdemKey` (D25).
8. **Record.** `Delivered` → finish `delivered` with the provider message id. `Retry` or an error →
   if `Attempts >= MaxAttempts`, finish `max_attempts`; else `Queue.Retry` at full-jitter backoff,
   floored by `RetryAfter` (D10). `Suppressed` → write the suppression (with `SuppressFor` as expiry),
   then finish `suppressed`. `Rejected` → finish `rejected`. Every finish is
   `Queue.Finish(domain.NewFinish(outcome, …))`: the domain's `DispositionOf` decides whether it is a
   dead letter and whether the fallback chain continues, and the store writes all of it in one
   statement (D37).
9. **Fenced?** Any write returning `ok=false` means the lease was lost: count
   `notify.lease.lost`, record nothing, move on. The row's current holder owns it.

---

## Invariants every implementation MUST uphold

1. **Recipient-scoping at the data layer (release blocker).** Every recipient-scoped table and every
   partition of one carries the one forced policy; scope is set per transaction with
   `set_config(…, true)`, never with a session `SET` (NR-008, NS-001, D17).
2. **The durable row is always written.** `Notify` always persists the inbox row — it is the inbox
   and the dedup record. Preferences decide which channels are enqueued and whether the row is
   inbox-visible (D26). Identity is authoritative from the trusted producer, never from content
   (NR-009); the realm is never taken from a request at all.
3. **Dual idempotency guard, and the fast one can only miss.** `notify_idem` is the backstop. The
   optional pre-check shares its key space, is read before the transaction and written only after
   commit, and never gates delivery (D18).
4. **Transactional persist + enqueue.** Guard, inbox row, queue rows, digest appends and drop
   records commit together or not at all. A crash after commit leaves durable work.
5. **At-least-once delivery through a fenced lease.** Claims are `SKIP LOCKED` leases; every outcome
   write presents the lease token; a channel dedupes on `Delivery.IdemKey` at the level it declares
   (D19, D25, D32).
6. **Terminal outcomes are explicit, and poison terminates.** Outcomes come from one vocabulary; only
   `max_attempts` and `rejected` are dead letters; attempts are counted at claim so a crashing
   delivery still reaches the ceiling (D8, D12, D19).
7. **The unread badge is authoritative from the store, and bounded** (D15).
8. **The live push path carries nudges only.** Keyed by the hash of the full identity, carrying an id,
   re-read under RLS by a relay that subscribes from the session's identity (D16).
9. **Storm coalescing is a policy.** One open window per `(recipient, topic, channel)`; a window seals
   at `DigestMax`; a flush is idempotent; `critical` and essential notifications are never digested
   (NR-014, D4).
10. **Broadcast is durable and off the request path.** A broadcast row, expanded a page per
    transaction with the cursor, resumable, idempotent per recipient (NR-020, D7).
11. **Suppression is checked by the dispatcher for every channel.** Channel-specific compliance —
    unsubscribe footers, `List-Unsubscribe` headers — lives in the channel (D13, D34).
12. **Bounded growth, everywhere.** Every table that grows has a bound: partitions, windows, or
    pending-only by construction (NR-022, D20, D21, D33).
13. **Opacity.** The kernel never parses, ranks or special-cases a `Realm`, `Tenant`, `Recipient` or
    `Topic`, and never branches on a `ChannelKind` — it reads `Capabilities` (NR-018).
14. **Fairness.** An exhausted tenant's backlog is moved out of the claim range, not deferred row by
    row (NS-007, D24).
15. **Acceptance is the commit.** `Notify` returns after PostgreSQL commits; that is the point NS-003
    measures from (D23).
16. **Every derived key uses the canonical encoding** — pre-check, stream, shard, delivery idempotency
    key, address key, erasure record (D18).

---

## Reference wiring: the originating product as a binding

| Port / type | The originating product's binding |
|---|---|
| `Realm` | `"aisat"` |
| `Tenant` | `{Kind: "workspace", ID: workspace_id}` — later `{Kind: "organization"}`, a binding change |
| `Recipient` | `{Kind: "user", ID: user_id}`; a device or Slack recipient is another `Kind` |
| `Topic` | its 13 topics (`ingestion_complete`, `invite_received`, `credit_exhausted`, …) as registrations |
| `Channel` | `InAppChannel` + `EmailChannel` — two of N |
| `PreferenceStore` | the PostgreSQL store over topic defaults (in-app on; email on for `credit_*`, `invite_received`, `task_halted`) |
| `TemplateRenderer` | per-topic Go templates; inbox copy rendered at read time |
| `AddressBook` | `email` → the member's verified address; `push` → the member's device tokens |
| `Store` | PostgreSQL, as in `migrations/` |

Swapping it for an incident-alerting product: register a `PagerDutyChannel` and a `SlackChannel`,
set `Recipient.Kind = "oncall"`, register the alert topics with `Fallback: ["sms"]`, and write their
templates. Persistence, preferences, idempotency, the queue, the dead-letter path and retention are
untouched.

---

## Reference channels: `InAppChannel` and `EmailChannel`

Both live in `adapters/driven/channel/` and depend only on `domain` + `ports`.

```go
package inapp // adapters/driven/channel/inapp

// Channel publishes a NUDGE — the notification id, nothing else — on the stream keyed by
// the hash of the full identity (D16). The relay re-reads the item under RLS with the
// session's identity, so a misrouted or forged nudge shows nothing. Idempotent by
// construction: the inbox row is the source of truth, a re-publish only re-nudges.
type Channel struct{ stream notify.StreamPublisher }

func (Channel) Kind() notify.ChannelKind { return "in_app" }

func (Channel) Capabilities() notify.ChannelCapabilities {
	return notify.ChannelCapabilities{
		NeedsAddress: false, RichContent: true, InboxBacked: true, Dedup: notify.DedupLocal,
	}
}

func (c Channel) Deliver(ctx context.Context, d notify.Delivery) (notify.DeliveryResult, error) {
	if err := c.stream.Publish(ctx, d.Identity, d.NotificationID); err != nil {
		return notify.DeliveryResult{Outcome: notify.Retry, Detail: err.Error()}, nil
	}
	return notify.DeliveryResult{Outcome: notify.Delivered}, nil
}
```

```go
package email // adapters/driven/channel/email

// Channel sends through a Mailer. It owns email compliance that is specific to email —
// the unsubscribe footer and RFC 8058 headers for non-essential topics (D34) — but not
// suppression, which the dispatcher checks for every channel (D13).
type Channel struct {
	mailer Mailer
	topics notify.TopicRegistry
	links  UnsubscribeLinks // signs one-(realm, tenant, recipient, topic, channel) tokens
	dedup  notify.DedupLevel // DedupProvider for Resend (24h); DedupNone for SMTP/SES
}

func (Channel) Kind() notify.ChannelKind { return "email" }

func (c Channel) Capabilities() notify.ChannelCapabilities {
	return notify.ChannelCapabilities{
		NeedsSubject: true, NeedsAddress: true, Dedup: c.dedup, DedupWindow: 24 * time.Hour,
	}
}

func (c Channel) NormalizeAddress(v string) string { return normalizeEmail(v) } // lowercase domain, trim

func (c Channel) Deliver(ctx context.Context, d notify.Delivery) (notify.DeliveryResult, error) {
	m := Mail{To: d.Address.Value, Subject: d.Content.Subject, HTML: d.Content.Body, IdemKey: d.IdemKey}
	if def, _, _ := c.topics.Lookup(ctx, d.Identity.Realm, d.Topic); !def.Essential {
		link := c.links.For(d) // one pair, signed, >= 60 days
		m.HTML += footer(link)
		m.Headers = map[string]string{
			"List-Unsubscribe":      "<" + link + ">",
			"List-Unsubscribe-Post": "List-Unsubscribe=One-Click",
		}
	}
	id, err := c.mailer.Send(ctx, m)
	switch {
	case err == nil:
		return notify.DeliveryResult{Outcome: notify.Delivered, ProviderMessageID: id}, nil
	case isHardBounce(err):
		return notify.DeliveryResult{Outcome: notify.Suppressed, SuppressReason: "hard_bounce"}, nil
	case isPermanent(err):
		return notify.DeliveryResult{Outcome: notify.Rejected, Detail: err.Error()}, nil
	default:
		return notify.DeliveryResult{Outcome: notify.Retry, RetryAfter: retryAfter(err), Detail: err.Error()}, nil
	}
}
```

A `failover.Channel` wraps an ordered list of provider channels for one kind and, on `Retry` from
one, tries the next inside the same attempt; it declares the weakest `Dedup` of its members (D29).

---

## Contract tests

These validate *any* implementation of the ports against the invariants. Every suite follows
[docs/testing.md](../../../docs/testing.md): table-driven, every test and subtest parallel, unit suites
with no infrastructure, integration suites against real PostgreSQL and Redis via Testcontainers
(`//go:build integration`, one cloned database per test), and the REST and SSE surface end to end with
Playwright. Schema-level guarantees are asserted separately by `make verify-schema`.

The queue, digest and maintenance transitions already have their integration suite in
[`adapters/driven/postgres`](../../../adapters/driven/postgres/) — every transition, real concurrency
for the claim, and a mutation check behind each guarantee ([D37](../design-decisions.md#d37)). The
suites below are the ones still to be written, with their shape fixed now.

```go
package notify_test

// ChannelContract runs against ANY notify.Channel. No assertion is skippable: a channel
// declares its guarantees through Capabilities, and the suite holds it to them (D32).
// With -tags provider, newChannel may return a channel bound to the provider's sandbox,
// which is the only way to prove a real provider dedupes.
func ChannelContract(t *testing.T, newChannel func(t *testing.T) (notify.Channel, Probe)) {
	ctx := context.Background()
	d := func() notify.Delivery {
		return notify.Delivery{ID: "d1", IdemKey: "k1", Channel: "x",
			Address: notify.Address{Value: "a@example.com"},
			Content: notify.RenderedContent{Subject: "Done", Body: "Your upload finished."}}
	}

	t.Run("re-drive collapses at the declared dedup level", func(t *testing.T) {
		t.Parallel()
		c, p := newChannel(t)
		r1, err := c.Deliver(ctx, d()); mustNoErr(t, err); mustBe(t, r1.Outcome, notify.Delivered)
		r2, err := c.Deliver(ctx, d()); mustNoErr(t, err)
		switch c.Capabilities().Dedup {
		case notify.DedupProvider, notify.DedupLocal:
			if r2.Outcome != notify.Delivered || p.Sends("k1") != 1 {
				t.Fatalf("declared dedup, but a re-drive sent %d times", p.Sends("k1"))
			}
		case notify.DedupNone:
			// At-least-once by declaration: two sends are permitted and the engine
			// reports this channel's level. Nothing to assert beyond success.
		}
	})
	t.Run("distinct delivery keys are distinct sends", func(t *testing.T) {
		t.Parallel()
		c, p := newChannel(t)
		a, b := d(), d(); b.IdemKey = "k2"; b.Address.Value = "b@example.com"
		_, _ = c.Deliver(ctx, a); _, _ = c.Deliver(ctx, b)
		if p.Sends("k1") != 1 || p.Sends("k2") != 1 { t.Fatal("per-address keys must not collapse (D25)") }
	})
	t.Run("transient failure is Retry, never an error or a drop", func(t *testing.T) {
		t.Parallel()
		c, p := newChannel(t); p.FailNext(Transient)
		r, err := c.Deliver(ctx, d())
		if err != nil || r.Outcome != notify.Retry { t.Fatalf("want Retry, got %v %v", r.Outcome, err) }
	})
	t.Run("dead address is Suppressed with a reason", func(t *testing.T) {
		t.Parallel()
		c, p := newChannel(t); p.FailNext(DeadAddress)
		r, _ := c.Deliver(ctx, d())
		if r.Outcome != notify.Suppressed || r.SuppressReason == "" { t.Fatal("dead address must be Suppressed") }
	})
	t.Run("permanent refusal is Rejected", func(t *testing.T) {
		t.Parallel()
		c, p := newChannel(t); p.FailNext(Permanent)
		if r, _ := c.Deliver(ctx, d()); r.Outcome != notify.Rejected { t.Fatal("want Rejected") }
	})
}

// NotifierContract runs against a real Notifier.                        //go:build integration
func NotifierContract(t *testing.T, env func(t *testing.T) Env) {
	t.Run("replay is a no-op: one row, one enqueue per channel", ...)
	t.Run("the same user in two tenants gets both (D18)", ...)
	t.Run("a failed transaction leaves no pre-check entry (D18)", func(t *testing.T) {
		e := env(t); e.Store.FailNextCommit()
		_, err := e.Notifier.Notify(ctx, n("u1", "k1")); mustErr(t, err)
		r, err := e.Notifier.Notify(ctx, n("u1", "k1")); mustNoErr(t, err)
		if !r.Applied { t.Fatal("the retry was swallowed by a pre-check written before commit") }
	})
	t.Run("NotifyTx rolled back by the host leaves nothing (D28)", ...)
	t.Run("NotifyTx committed by the host is delivered", ...)
	t.Run("a disabled channel is not enqueued; the row still exists", ...)
	t.Run("no inbox-backed channel enabled ⇒ row exists, not inbox-visible (D26)", ...)
	t.Run("a locked tenant preference overrides the recipient (D30)", ...)
	t.Run("critical inside quiet hours is immediate and marked overridden (D9)", ...)
	t.Run("the realm cannot be set by a producer", ...)
}

// DispatcherContract drives the dispatcher through every crash point.   //go:build integration
func DispatcherContract(t *testing.T, env func(t *testing.T) Env) {
	t.Run("crash after claim, before send: re-driven after the lease lapses", ...)
	t.Run("crash after send, before record: re-driven with the SAME Delivery.IdemKey", ...)
	t.Run("a delivery that panics the worker reaches max_attempts (D19)", ...)
	t.Run("a suppressed address is never handed to the channel (D13)", ...)
	t.Run("three devices: one failing retries alone (D25)", ...)
	t.Run("expired is never sent late (D29)", ...)
	t.Run("no_address on push enqueues the sms fallback (D29)", ...)
	t.Run("exhausted tenant: the quiet tenant's delivery goes next (D24)", ...)
	t.Run("a stale worker's outcome is discarded (D19)", ...)
}

// StreamIsolationContract proves the live path is isolated (D16).       //go:build integration
func StreamIsolationContract(t *testing.T, env func(t *testing.T) Env) {
	t.Run("same recipient id in two tenants: each stream sees only its own", func(t *testing.T) {
		e := env(t)
		a := e.Relay.Connect(t, identity("w1", "u1"))
		b := e.Relay.Connect(t, identity("w2", "u1"))
		notifyAndDrain(t, e, "w1", "u1", "k1")
		a.MustReceive(t, "k1")
		b.MustReceiveNothing(t, time.Second)
	})
	t.Run("a forged nudge naming another recipient's notification shows nothing", func(t *testing.T) {
		e := env(t)
		victim := e.Relay.Connect(t, identity("w1", "u2"))
		other := notifyAndDrain(t, e, "w1", "u1", "k2")
		e.Stream.Publish(ctx, identity("w1", "u2"), other.NotificationID) // wrong recipient's id
		victim.MustReceiveNothing(t, time.Second)                         // RLS returned zero rows
	})
	t.Run("reconnect delivers the bounded authoritative count first", ...)
}

// StoreContract: atomic persist, replay, crash-after-commit leaves drivable work,
// retention with a pending delivery outstanding, erasure. TransportContract runs
// NotifierContract through grpcserver + grpcclient: identical semantics, identical suite.
```

---

## Deployment topology: embedded library **or** standalone service

Two shapes, one set of interfaces.

1. **Embedded library (the default).** A Go host imports this module, wires its own or the provided
   port implementations, and calls `Notify` / `NotifyTx` in-process. The dispatcher runs in the
   host's worker process.
2. **Standalone service.** `notifyd` runs the REST + SSE surface, the gRPC producer surface and the
   dispatcher. It uses the data-backed ports: topics, templates, addresses and providers are rows it
   owns, managed through the `Catalog` API. Producers in any language call gRPC; recipients reach the
   REST surface with a host-signed token (D31).

| Concern | Library | Service |
|---|---|---|
| Realm | `Config.Realm` | derived from the producer principal via `RealmBindings`; never in a request |
| Topics, templates | in code, or the data-backed ports | data, via `Catalog` |
| Addresses | host `AddressBook`, or `recipient_addresses` | `recipient_addresses`, via `Catalog.PutAddresses` |
| Audiences | host `AudienceResolver` | inline list (bounded) or the host's `Directory` callback |
| Transactional enqueue | `NotifyTx` | not available — producers call after commit with a stable `IdemKey`, and retries are safe |
| Recipient auth | the host's session → `Identity` | host-signed recipient token (JWKS per realm) |
| Provider credentials | the host constructs channels | `channel_providers.secret_ref` → env / file / secret manager |

**Recommended posture:** start embedded; move to the service when a second product, or a non-Go
product, needs to send from the same channels and templates. The producer-side change is one line:
`app.New(...)` becomes `grpcclient.New(conn)`, and both return `notify.Notifier`.

---

## Service surface: gRPC (contract-locked)

```proto
syntax = "proto3";
package notify.v1;
option go_package = "github.com/truongpx396/intel-notification/api/notifyv1";

import "google/protobuf/struct.proto";
import "google/protobuf/timestamp.proto";

// No message carries a realm. The service derives it from the authenticated producer
// principal (mTLS SAN or token subject) through RealmBindings (D31).

message Tenant    { string kind = 1; string id = 2; }
message Recipient { string kind = 1; string id = 2; }

message Notification {
  Tenant tenant = 1;
  Recipient recipient = 2;
  string topic = 3;
  string priority = 4;                         // "" ⇒ topic default
  google.protobuf.Struct data = 5;             // template variables (D27)
  string title = 6;                            // fallback copy, optional
  string body = 7;
  map<string, string> payload = 8;             // deep links — never a routing input
  string idem_key = 9;                         // REQUIRED
  google.protobuf.Timestamp occurred_at = 10;
  google.protobuf.Timestamp deliver_after = 11;
  google.protobuf.Timestamp deliver_before = 12;
  map<string, string> attributes = 13;         // audit only
}
message Receipt {
  string notification_id = 1;
  bool applied = 2;
  repeated string channels = 3;
  bool inbox_visible = 4;
}
message NotifyBatchRequest  { repeated Notification notifications = 1; }   // <= 500, each independent
message NotifyBatchResponse { repeated Receipt receipts = 1; repeated string errors = 2; }

message RecipientList { repeated Recipient recipients = 1; }               // <= MaxInlineRecipients
message BroadcastRequest {
  Tenant tenant = 1;
  oneof source { string audience = 2; RecipientList recipients = 3; }
  string topic = 4;
  string priority = 5;
  google.protobuf.Struct data = 6;
  string title = 7;
  string body = 8;
  map<string, string> payload = 9;
  string idem_key = 10;                        // REQUIRED
}
message BroadcastReceipt { string broadcast_id = 1; bool applied = 2; }

message NotificationRef { Tenant tenant = 1; Recipient recipient = 2; string idem_key = 3; }
message CancelReceipt   { bool matched = 1; int32 canceled = 2; int32 in_flight = 3; }
message DeliveryStatus {
  string channel = 1;
  string address_key = 2;
  string state = 3;                            // pending | delivered | suppressed | … | fanned_out
  int32 attempts = 4;
  string provider_message_id = 5;
  string provider_status = 6;
  google.protobuf.Timestamp completed_at = 7;
}
message NotificationStatus {
  bool found = 1;
  string notification_id = 2;
  bool canceled = 3;
  repeated DeliveryStatus deliveries = 4;
}

// The producer surface. Named Notifier: a service may not share a name with a message.
service Notifier {
  rpc Notify      (Notification)       returns (Receipt);
  rpc NotifyBatch (NotifyBatchRequest) returns (NotifyBatchResponse);
  rpc Broadcast   (BroadcastRequest)   returns (BroadcastReceipt);
  rpc Cancel      (NotificationRef)    returns (CancelReceipt);
  rpc Status      (NotificationRef)    returns (NotificationStatus);
}

// ---- management: service mode's replacement for host code (D31) ----

message Topic {
  string topic = 1;
  repeated string default_channels = 2;
  repeated string fallback = 3;
  string default_priority = 4;
  bool essential = 5;
  string template_ref = 6;
}
message Template {
  Tenant tenant = 1;                           // unset ⇒ realm-wide; set ⇒ per-tenant override
  string template_ref = 2;
  string channel = 3;
  string locale = 4;
  int32 version = 5;                           // assigned by the service on Put
  string subject = 6;
  string body = 7;                             // Go template; a restricted function map, no I/O
  google.protobuf.Struct data = 8;
}
message TemplateVersion { Tenant tenant = 1; string template_ref = 2; string channel = 3; string locale = 4; int32 version = 5; }
message Address { string value = 1; string locale = 2; string timezone = 3; map<string, string> meta = 4; }
message RecipientAddresses { Tenant tenant = 1; Recipient recipient = 2; string channel = 3; repeated Address addresses = 4; }
message TenantPreference { Tenant tenant = 1; string topic = 2; string channel = 3; bool enabled = 4; bool locked = 5; }
message EraseRequest { Tenant tenant = 1; Recipient recipient = 2; }
message EraseReceipt { map<string, int32> removed = 1; }

service Catalog {
  rpc UpsertTopic         (Topic)              returns (Topic);
  rpc PutTemplate         (Template)           returns (Template);          // a new version, inactive
  rpc ActivateTemplate    (TemplateVersion)    returns (Template);
  rpc PutAddresses        (RecipientAddresses) returns (RecipientAddresses); // replaces one channel's set
  rpc SetTenantPreference (TenantPreference)   returns (TenantPreference);
  rpc Erase               (EraseRequest)       returns (EraseReceipt);      // D35
}

// Implemented by the HOST when audience selectors should be resolved by callback.
message ResolveAudienceRequest  { Tenant tenant = 1; string audience = 2; string cursor = 3; }
message ResolveAudienceResponse { repeated Recipient recipients = 1; string next_cursor = 2; }
service Directory {
  rpc ResolveAudience (ResolveAudienceRequest) returns (ResolveAudienceResponse);
}
```

Contract rules baked into the surface:

- **`Notify` renders and routes inside the service.** Templates and provider credentials never ship
  to producers; the client stub is just a remote `Notifier`.
- **`idem_key` is required on every mutating producer RPC.** A missing key is `INVALID_ARGUMENT`,
  never a silently unguarded notification.
- **`Broadcast` is coarse.** One call per broadcast; never per-recipient RPCs (invariant 10).
- **No streaming RPC.** Live updates flow over the recipient SSE surface, not a producer stream.
- **Deadlines and retries are the producer's.** A failed call is retried with the same `idem_key`;
  that is what the key is for.

The generated client is wrapped so it satisfies `notify.Notifier`:

```go
// adapters/driving/grpcclient — a remote Notifier. Producers can't tell it from the in-process one.
type Client struct{ c notifyv1.NotifierClient }

func (m *Client) Notify(ctx context.Context, n notify.Notification) (notify.Receipt, error) { /* map → RPC → map */ }
func (m *Client) NotifyTx(context.Context, notify.Tx, notify.Notification) (notify.Receipt, error) {
	return notify.Receipt{}, notify.ErrTxUnsupported
}
// Broadcast, Cancel, Status: map → RPC → map.

var _ notify.Notifier = (*Client)(nil)
```

---

## Module layout

Ports and adapters (hexagonal). This repository **is** the extraction unit. The litmus test, which
runs as a CI gate: **does `go build ./... && go test ./...` pass in a checkout containing only this
module — no host, no replace directives?**

```text
./
  go.mod                   module github.com/truongpx396/intel-notification
  config.go                notify.Config — the entire configuration surface
  domain/                  pure types, canonical encoding, keys, backoff, shard. Imports nothing
    identity.go notification.go delivery.go outcome.go plan.go keys.go backoff.go shard.go
  ports/
    driving.go             Notifier, Inbox, Admin, Dispatcher, Jobs
    driven.go              Queue, Digests, Maintenance (implemented) · Channel, ChannelRegistry,
                           Store, InboxStore, PreferenceStore, TopicRegistry, TemplateRenderer,
                           AddressBook, AudienceResolver, SuppressionStore, QuotaCounter,
                           PreCheck, StreamPublisher, Clock, IDSource
  app/                     use-cases; own the invariants
    notifier.go planner.go broadcast.go dispatcher.go digest.go quota.go inbox.go
    maintenance.go erasure.go
  adapters/
    driven/
      postgres/            store, inbox, prefs, topics, templates, addresses, suppressions, leases
      redis/               precheck, stream, quota
      channel/             inapp · email · sms · push · slack · webhook · failover
    driving/
      inprocess/           returns ports.Notifier directly                  (library mode)
      httpapi/             REST + SSE relay over ports.Inbox                (both modes)
      grpcserver/          notifyv1 over app                                (service mode)
      grpcclient/          notifyv1 client, satisfies ports.Notifier        (service callers)
      natsingest/          optional: JetStream subject → Notifier (D23)
  api/notifyv1/            generated protobuf
  cmd/notifyd/             the service binary: the only place concrete adapters are assembled
  migrations/              owns the schema — travels with the module; migrations.go embeds it
  internal/pgtest/         integration-test support: Testcontainers, one cloned database per test
  e2e/                     Playwright suite against the running service (docs/testing.md)
```

**Dependency rule:** `adapters → app → ports → domain`. `domain` and `ports` import nothing outside
the module; `app` imports only `ports` and `domain`; adapters may import infrastructure SDKs but never
a host product and never `app`. An optional bus is a **driving** adapter — it consumes a subject and
calls the `Notifier` — so the core has no `Bus` port at all.

---

## Machine-enforced boundary

Two linters make the extraction rules a CI gate. The live configurations are
[`.go-arch-lint.yml`](../../../.go-arch-lint.yml) and [`.golangci.yml`](../../../.golangci.yml);
they are the normative copies and are not repeated here. Their load-bearing rules:

- `app` may depend only on `ports` and `domain`, so the use-cases cannot reach infrastructure.
- Driven and driving adapters may depend on `ports` and `domain` but **not** on `app`, so an adapter
  cannot smuggle a use-case decision into itself.
- `domain`, `ports` and `app` may not import `pgx`, `go-redis`, `nats`, `grpc` or `net/http`.
- Nothing in the module may import the product it was extracted from.

---

## `config.go` — the module's only configuration surface

```go
package notify

type Config struct {
	// Realm is required in library mode and must match ^[a-z0-9][a-z0-9-]{0,62}$.
	// Service mode ignores it: realms come from RealmBindings (D31).
	Realm Realm

	StoreDSN string // PostgreSQL
	RedisURL string // pre-check, live stream, quota counters

	Shards       int           // 16. Changeable online (D11)
	ClaimLease   time.Duration // 5m. Must exceed the slowest channel's send timeout
	ClaimBatch   int           // 100
	PollInterval time.Duration // 500ms: the idle ceiling on claim latency (D22)
	ListenNotify bool          // false. Transactional NOTIFY wakeups; see D22 before enabling

	MaxAttempts    int           // 5
	BackoffBase    time.Duration // 10s
	BackoffCeiling time.Duration // 1h

	IdempotencyWindow time.Duration // 7d; >= 24h (D21)
	PreCheckTTL       time.Duration // 24h; <= IdempotencyWindow (D18)

	RetentionWindow      time.Duration // 90d — inbox partitions, read and unread (D33)
	DeliveryLogRetention time.Duration // 30d
	DeadLetterRetention  time.Duration // 30d

	DigestMax           int    // 100
	UnreadCap           int    // 99
	DefaultLocale       string // "en"
	MaxInlineRecipients int    // 1000
}

// Validate checks the EFFECTIVE config (defaults applied to a copy) and returns every
// problem at once. It rejects: an empty or malformed Realm (library mode); empty
// StoreDSN or RedisURL; Shards, ClaimBatch, MaxAttempts, DigestMax, UnreadCap or
// MaxInlineRecipients < 1; ClaimLease or PollInterval <= 0; BackoffCeiling <
// BackoffBase; IdempotencyWindow < 24h; PreCheckTTL > IdempotencyWindow.
func (c Config) Validate() error
```

## `Deps` + `New` — product specifics injected, nothing reached into

```go
package app

type Deps struct {
	Channels     ports.ChannelRegistry
	Topics       ports.TopicRegistry
	Renderer     ports.TemplateRenderer
	Addresses    ports.AddressBook
	Audience     ports.AudienceResolver // nil ⇒ Broadcast accepts inline recipients only
	Store        ports.Store
	Inbox        ports.InboxStore
	Queue        ports.Queue
	Digests      ports.Digests
	Maintenance  ports.Maintenance
	Prefs        ports.PreferenceStore
	Suppressions ports.SuppressionStore
	Quotas       ports.QuotaCounter
	Stream       ports.StreamPublisher
	PreCheck     ports.PreCheck // optional
	Clock        ports.Clock    // default: system clock
	IDs          ports.IDSource // default: UUIDv7
}

// New validates config and deps and returns the engine's driving ports.
func New(cfg notify.Config, d Deps) (*Engine, error)

type Engine struct {
	Notifier   ports.Notifier
	Inbox      ports.Inbox
	Admin      ports.Admin
	Dispatcher ports.Dispatcher
	Jobs       ports.Jobs
}
```

`postgres.NewAll(pool)` returns every PostgreSQL-backed port at once (store, queue, digests,
maintenance, inbox, prefs, suppressions, and the data-backed topics, templates and addresses), so the
common wiring is a handful of lines — see [quickstart.md](../quickstart.md).

---

## Generalization checklist

- [ ] **Realm set** — library mode: `Config.Realm`, not shared with another product. Service mode:
      every producer principal bound to exactly one realm.
- [ ] **Tenant + Recipient defined** — built from the trusted session or producer, never from content.
- [ ] **Channels registered** — each passing `ChannelContract`, each declaring `InboxBacked` and
      `Dedup` truthfully; `DedupNone` channels accepted knowingly.
- [ ] **Topics registered** — default channels, fallback chain, priority, template ref, and
      `Essential` set deliberately.
- [ ] **Templates** — per (template ref, channel, locale); producers send `Data`, fallback copy only
      where no template exists.
- [ ] **Preferences** — recipient UI over `(topic, channel)`; tenant defaults and locks where the
      product has administrators; essential topics shown as non-disableable with the reason.
- [ ] **Addresses** — every address per channel (all devices); a miss is terminal, not a retry.
- [ ] **Audiences** — a paged resolver, a `Directory` callback, or inline lists.
- [ ] **Transactional producers use `NotifyTx`** where the triggering change and the notification
      share a database.
- [ ] **Dispatcher and the single-owner jobs running** — retention, partition provisioning, digest
      flush, idempotency expiry, quota rollover.
- [ ] **Quotas** — a realm-wide default, and per-tenant rows where needed.
- [ ] **Isolation verified** — `make verify-schema` green, and `StreamIsolationContract` green against
      the relay you deploy.
- [ ] **Unsubscribe** — RFC 8058 headers on non-essential email; `POST` endpoint reachable.
- [ ] **Erasure** — the host's data-subject process calls `Admin.Erase`.

---

## Non-goals (stays in the host, by design)

- **What triggers a notification.** Producers build notifications; the kernel instruments nothing.
- **Copy, localization, branding** as kernel code — a `TemplateRenderer` implementation or template
  rows.
- **Provider SDKs in the core** — they live in `Channel` adapters.
- **What a tenant or recipient means** — the kernel treats both as opaque.
- **Authenticating recipients** — the host's session layer, or the host-signed token in service mode.
