# Contract: Notification & Multi-Channel Delivery (reusable ports)

**Plan**: [../plan.md](../plan.md) | **Status**: Design addition — the reusability seam for the notification backbone (US8, FR-032–FR-039; SC-012, SC-013). It factors the existing persist + fan-out + in-app-push + email + DLQ + retention machinery into a small set of ports so the *same* engine drops into other systems without touching their tenancy model, their event vocabulary, or their delivery channels. **One Phase 1 behavior changes** — the unsafe `SET NX`-short-circuit fan-out is replaced by a transactional outbox (a correctness fix for SC-013 / FR-035, see the callout below); everything else is a pure repackaging of existing behavior behind ports.

The Phase 1 notification machinery is already production-grade on its *operational* axes (exactly-once, recipient-scoped, DLQ, retention, suppression), but it is packaged as an app-internal kernel module welded to four of *this* app's assumptions. This contract names the seam that removes that welding — the same treatment [metering-ports.md](./metering-ports.md) gave the credit backbone. Ports are given in Go (the kernel language); ContextEngine's in-app + email delivery is presented at the end as **two implementations** of the `Channel` port, not as the core.

---

## Why: the four couplings this removes

| # | Today's coupling | Evidence it is a coupling | The port that removes it |
|---|---|---|---|
| 1 | Recipient is welded to `(workspace_id, user_id)` | `notifications`/`notification_preferences` columns, RLS `user_id = current_setting('app.user_id')`, Redis `notify:user:<id>`, subject `notify.<ws>` ([data-model.md](../data-model.md) K, [nats-subjects.md](./nats-subjects.md)) — a reusing host whose recipient is an org, a device, a Slack channel, or an external contact (no `user_id`) needs a schema + RLS rewrite | `Recipient` + `Tenant` — two opaque identities the engine never interprets |
| 2 | Delivery channels are hard-coded to in-app + email | The fan-out handler branches `in-app: PUBLISH notify:user:<id>` / `email? → notify.email.<ws>` inline ([README.md](../../../README.md#L657)); the only provider port is `kernel/mailer.go` — there is no push / SMS / Slack / webhook seam | `Channel` + `ChannelRegistry` — a pluggable delivery target; the fan-out iterates a registry, never an `if` ladder |
| 3 | `category` is a fixed Postgres enum | `category` carries 13 baked-in values in the `notifications` table + `notification_preferences` ([data-model.md](../data-model.md) K); every new event type is an `ALTER TYPE` migration | `Topic` + `TopicRegistry` — a registered string with data-driven defaults (channels, priority, template) |
| 4 | Copy + rendering are welded to the email worker | "renders + sends via the `kernel/mailer.go` port" ([nats-subjects.md](./nats-subjects.md)); no localization, per-tenant branding, or template-override seam exists | `TemplateRenderer` — the one home for copy, locale, and branding, per channel |

The rule: **the notification kernel is generic; only the `Channel` set, the `Topic` registry, the `Recipient`/`Tenant` binding, and the templates are product-specific.** Everything that is release-blocking today (recipient-scoping, exactly-once, DLQ discipline, retention) is preserved verbatim — it just stops assuming "workspace/user," "in-app+email," and a fixed category list.

> **One correctness fix travels with this refactor.** Today's fan-out does a durable `INSERT` *and* an in-app publish *and* an email enqueue that are **not transactional**, guarded by a Redis `SET NX notify:applied:{idem_key}` that "short-circuits the *entire* handler" ([README.md](../../../README.md#L648)). That gate is unsafe: if attempt #1 sets the guard, inserts the row, then crashes **before** enqueuing email, the JetStream redelivery hits the guard and short-circuits — the email is **lost**. This contract closes the hole the same way [metering-ports.md](./metering-ports.md) closed billing's: a **transactional outbox** — the row and one outbox entry per channel are written in one DB transaction; a `Dispatcher` drains the outbox at-least-once; the fast Redis guard gates only the *durable write*, never channel delivery (invariant 3).

---

## Ports at a glance

```text
PRODUCERS (ingest · billing · invite · agent · admin) — thin: publish one Notification
   │  Notify(n)     ── persist + fan out, idempotent on n.IdemKey
   │  Broadcast(b)  ── expand an audience OFF the request path
   ▼
┌── Notifier (orchestration) ──────────────────────────────────────────────────┐
│   PreferenceStore.Channels(recipient, topic) → enabled ChannelKinds     PREFS │
│   Store.PersistAndEnqueue(n, channels) → ONE txn: inbox row + N outbox rows   │
│      (idempotent on UNIQUE(recipient, idem_key); Redis SET NX = fast pre-check)│
└───────────────────────────────────────────────┬───────────────────────────────┘
        returns now (no per-channel send wait)   │  notification_outbox (per channel)
                                                 ▼
┌── Dispatcher (drains the outbox · worker role) ───────────────────────────────┐
│   ClaimOutbox(shard) → for each entry:                                        │
│     TemplateRenderer.Render(topic, channel, locale, n) → RenderedContent      │
│     AddressBook.Resolve(recipient, channel)            → Address              │
│     ChannelRegistry.Get(channel).Deliver(delivery)     → DeliveryResult       │
│   at-least-once; retryable → backoff; MaxAttempts → dead_letters (+alarm)     │
└───────────────────────────────────────────────────────────────────────────────┘
        in_app Channel → Redis pub/sub → SSE relay → live badge (recomputed from Store on connect)
        email Channel  → kernel/mailer.go → suppression check → provider (Resend, swappable)
```

Six seams a host can swap independently: the **`Channel`** set (its delivery targets), the **`TopicRegistry`** (its event vocabulary), the **`Recipient`/`Tenant`** binding (its identity + isolation), the **`TemplateRenderer`** (its copy/branding), the **`PreferenceStore`** (its opt-in model), and the **`Store`** backend (its infra). The reference impl uses Postgres+Redis+NATS, but nothing in the port signatures requires them.

---

## Domain types

```go
package notify

import "time"

// Tenant is the ISOLATION boundary a notification is scoped to — the RLS predicate source.
// The HOST decides what it means (workspace, organization, account); the kernel treats it
// only as an identity + a key/subject source. Kept SEPARATE from Recipient so "recipient
// within tenant" (a user within a workspace) is expressible without welding the two together.
type Tenant struct {
	Kind string // host-defined: "workspace" | "organization" | "account" | …
	ID   string // opaque, stable
}

// Recipient is the OPAQUE delivery subject. The HOST decides what it means (user, org,
// device, email, slack_channel, webhook); the kernel treats it only as an identity to
// scope by and to resolve an Address from. Parameterizing this is what makes "notify a
// user" vs "notify an org/device/channel" a binding change, not a schema migration —
// the exact coupling metering-ports removed for the billing subject.
type Recipient struct {
	Kind string // host-defined: "user" | "org" | "device" | "email" | "slack_channel" | …
	ID   string // opaque, stable
}

// Tag is the Redis channel / NATS token for a tenant: subjects derive as `notify.<tag>`,
// keys as `notify:applied:{tag}:{idem}`. The host decides what a tenant IS; the kernel
// only derives a stable string.
func (t Tenant) Tag() string { return t.Kind + ":" + t.ID }

// Topic is a REGISTERED notification type (generalizes the hard-coded `category` enum).
// A string, not a DB enum: a new topic is a TopicRegistry entry (default channels,
// priority, template ref), never an ALTER TYPE migration. The kernel never special-cases one.
type Topic string

// Priority orders + styles a notification; host-extensible via config, not code.
type Priority string // "info" | "warning" | "critical"

// ChannelKind selects a Channel implementation in the registry.
type ChannelKind string // "in_app" | "email" | "sms" | "push" | "slack" | "webhook" | …

// Notification is one durable, recipient-scoped record — the inbox row AND the unit the
// engine dedupes on. IdemKey is REQUIRED: it is the retry/dedup identity that makes the
// whole fan-out exactly-once (one row, at-most-one delivery per channel), per SC-013/FR-032.
type Notification struct {
	Tenant     Tenant
	Recipient  Recipient
	Topic      Topic
	Priority   Priority
	Title      string
	Body       string
	Payload    map[string]string // deep-link refs (doc_id/invite_id/run_id) — for the UI, never a routing input
	IdemKey    string            // REQUIRED — derived from the originating resource + event
	OccurredAt time.Time
	Attributes map[string]string // trace_id, source subject — audit/log ONLY, never a routing/pref input
}

// Address is WHERE a channel delivers — resolved from a Recipient by the AddressBook,
// never carried on the Notification (decouples WHO from WHERE). For in_app the value is
// the recipient id; for email an address; for slack a webhook URL; for push a device token.
type Address struct {
	Channel ChannelKind
	Value   string            // email / device token / webhook URL / recipient id
	Locale  string            // BCP-47, drives TemplateRenderer; "" ⇒ tenant default
	Meta    map[string]string // timezone, provider hints
}

// RenderedContent is the channel-specific, localized payload (TemplateRenderer output).
type RenderedContent struct {
	Subject string            // email subject / push title — omitted when Channel.Capabilities says so
	Body    string            // channel-appropriate body (html / text / markdown)
	Data    map[string]string // structured fields for rich channels (slack blocks, push payload)
}

// Delivery is one Notification bound to one channel + resolved Address + rendered content —
// the unit a Channel actually sends.
type Delivery struct {
	Notification Notification
	Channel      ChannelKind
	Address      Address
	Content      RenderedContent
}

// DeliveryResult is the outcome of a Channel.Deliver call.
type DeliveryResult struct {
	Delivered  bool
	Retryable  bool   // transient failure → Dispatcher re-drives under backoff
	Suppressed bool   // address on the suppression list / opted out — TERMINAL, not an error
	Detail     string // provider message-id or failure reason (audit)
}

// Receipt is the outcome of an idempotent Notify. Applied=false ⇒ this IdemKey was already
// persisted and this call was a no-op (a replay) — the caller relies on that to stay exactly-once.
type Receipt struct {
	NotificationID string
	IdemKey        string
	Applied        bool          // false = idempotent replay, already persisted
	Channels       []ChannelKind // channels enqueued on the first apply (empty on replay)
}

// Preference is one recipient's opt-in for one (topic, channel). Absent ⇒ topic default.
type Preference struct {
	Recipient Recipient
	Tenant    Tenant
	Topic     Topic
	Channel   ChannelKind
	Enabled   bool
}

// DeliverySchedule drives coalescing + deferral (quiet hours, digest cadence). The engine
// uses it to decide immediate vs batched delivery; a channel never sees it.
type DeliverySchedule struct {
	QuietStart string        // "22:00" local; "" ⇒ none
	QuietEnd   string        // "07:00" local
	Timezone   string        // IANA tz for quiet-hours math
	Digest     time.Duration // 0 ⇒ immediate; >0 ⇒ coalesce same-topic into a window
}

// Shard is the tenant/recipient partition key for the outbox. Phase 1 runs N queue-group
// drainers; the shard lets them scale with no key-space redesign and no double-deliver.
type Shard string

// OutboxEntry is one pending per-channel delivery, drained by the Dispatcher.
type OutboxEntry struct {
	ID             string
	NotificationID string
	Channel        ChannelKind
	Attempts       int
	NextAttemptAt  time.Time
}
```

---

## Port: `Channel` — the pluggable delivery target (the load-bearing seam)

```go
// Channel is ONE delivery target. in_app and email are the two Phase 1 implementations;
// sms, push, slack, and webhook are added by REGISTERING another Channel — never by editing
// the fan-out. This is the notification analogue of metering's Pricer: the one place a new
// product plugs in its own behavior.
type Channel interface {
	Kind() ChannelKind

	// Deliver sends ONE delivery. MUST be idempotent on d.Notification.IdemKey for this
	// channel: a Dispatcher re-drive after a crash must not double-send (the terminal
	// provider dedupes on idem_key / message-id). MUST check its own suppression policy and
	// return DeliveryResult{Suppressed:true} — NOT an error — for a bounced/complained/opted-out
	// address, so the Dispatcher marks it terminal instead of re-driving forever.
	Deliver(ctx context.Context, d Delivery) (DeliveryResult, error)

	// Capabilities lets the engine skip rendering fields this channel ignores and skip the
	// AddressBook when a channel addresses by recipient id alone (in_app).
	Capabilities() ChannelCapabilities
}

type ChannelCapabilities struct {
	NeedsSubject bool // email/push: true; sms/in_app: false
	NeedsAddress bool // email/sms/push/slack: true (AddressBook resolves); in_app: false (recipient id)
	RichContent  bool // consumes RenderedContent.Data (slack blocks / push payload) vs plain Body
}

// ChannelRegistry maps a ChannelKind to its Channel. Registering a channel is a wiring-time
// Register call in cmd/ — never an edit to the fan-out loop or a schema change.
type ChannelRegistry interface {
	Register(c Channel)
	Get(kind ChannelKind) (Channel, bool)
	Kinds() []ChannelKind
}
```

**Why this is the whole game.** Fan-out is `for _, ch := range enabledChannels { registry.Get(ch).Deliver(...) }` — adding SMS is writing one `Channel` and one `Register` line, with zero edits to persistence, preferences, idempotency, DLQ, or retention. Contrast today's inline `in-app: … / email?: …` branch, where a third channel means editing the handler.

---

## Port: `Notifier` — orchestration producers actually use

```go
// Notifier is the single entry point producers depend on. It composes PreferenceStore
// (who wants what) + Store (persist + enqueue) so callers never touch channels, keys, or
// the outbox directly. Producers stay THIN: they publish a Notification, nothing more.
type Notifier interface {
	// Notify persists + fans out ONE notification, idempotently on n.IdemKey. A replay
	// (same recipient + IdemKey) is a no-op: no second row, no second delivery. Returns
	// Applied=false on replay. The durable row is ALWAYS written (it is the inbox + the
	// dedup backstop); PreferenceStore decides which delivery channels are enqueued.
	Notify(ctx context.Context, n Notification) (Receipt, error)

	// Broadcast expands an audience into per-recipient Notifications OFF the request path:
	// one enqueue returns immediately so delivery to a large membership never blocks or
	// times out the caller (FR-037). Each expanded notification carries a deterministic
	// IdemKey so a retried broadcast does not double-notify.
	Broadcast(ctx context.Context, b BroadcastRequest) (BroadcastReceipt, error)
}

type BroadcastRequest struct {
	Tenant   Tenant
	Audience string // host-resolved audience selector: "workspace_members" | "admins" | …
	Topic    Topic
	Priority Priority
	Title    string
	Body     string
	Payload  map[string]string
	IdemKey  string // REQUIRED — the broadcast identity; per-recipient keys derive from it
}

type BroadcastReceipt struct {
	IdemKey string
	Applied bool
	Fanned  int // recipients enqueued (0 on replay)
}
```

## Port: `Store` — durable inbox + transactional outbox (driven)

```go
// Store is the durable notification tier: the recipient-scoped inbox rows AND the
// transactional outbox. The critical method is PersistAndEnqueue — it writes the
// notification row and one outbox row per target channel in ONE transaction, so a crash
// can never leave a persisted notification with un-enqueued deliveries (this is the fix
// for today's non-transactional INSERT + publish + email-enqueue).
type Store interface {
	// PersistAndEnqueue is idempotent on (recipient, idem_key) via a UNIQUE constraint: a
	// replay writes nothing and returns Applied=false. On first apply it INSERTs the inbox
	// row + one outbox entry per channel, atomically, in one tx.
	PersistAndEnqueue(ctx context.Context, n Notification, channels []ChannelKind) (Receipt, error)

	// ClaimOutbox pops up to max due entries for a shard (at-least-once: a crash after the
	// claim re-delivers). Entries carry Attempts + NextAttemptAt for backoff.
	ClaimOutbox(ctx context.Context, shard Shard, max int) ([]OutboxEntry, error)

	// MarkDelivered / MarkFailed advance an entry after a Channel result. MarkFailed with
	// retryable=false (or Attempts >= MaxAttempts) parks the entry in dead_letters.
	MarkDelivered(ctx context.Context, entryID string) error
	MarkFailed(ctx context.Context, entryID string, retryable bool, detail string) error

	// Unread recomputes the badge from the durable store — the authoritative count. Redis
	// pub/sub is an at-most-once optimization; the count is always re-derivable from here.
	Unread(ctx context.Context, r Recipient, t Tenant) (int, error)

	// Retention prunes/archives read rows past the window (partition DROP on created_at).
	Retention(ctx context.Context, olderThan time.Duration) (pruned int, err error)
}

// PreferenceStore resolves which channels a recipient wants for a topic + the delivery
// schedule (quiet hours, digest). Absent preference ⇒ the topic's registered default.
type PreferenceStore interface {
	Channels(ctx context.Context, r Recipient, t Tenant, topic Topic) ([]ChannelKind, error)
	Schedule(ctx context.Context, r Recipient, t Tenant, topic Topic) (DeliverySchedule, error)
	SetPreference(ctx context.Context, p Preference) error
}

// TemplateRenderer produces channel-shaped, localized content from a topic + notification.
// The ONE place copy, i18n, and per-tenant branding live — swap it to re-skin every
// notification without touching the engine.
type TemplateRenderer interface {
	Render(ctx context.Context, topic Topic, ch ChannelKind, locale string, n Notification) (RenderedContent, error)
}

// AddressBook resolves a Recipient to a channel Address (email, device token, webhook URL).
// found=false ⇒ the recipient has no address for that channel: the Dispatcher marks the
// entry terminal (not an error, not a retry).
type AddressBook interface {
	Resolve(ctx context.Context, r Recipient, ch ChannelKind) (addr Address, found bool, err error)
}

// TopicRegistry holds per-topic defaults: default channels, default priority, template ref,
// and whether it is broadcast-only. Data-driven — a new topic is a registration, not a migration.
type TopicRegistry interface {
	Register(topic Topic, def TopicDef)
	Lookup(topic Topic) (TopicDef, bool)
}

type TopicDef struct {
	DefaultChannels []ChannelKind
	DefaultPriority Priority
	Essential       bool // essential (security/billing) emails skip the unsubscribe footer + ignore digesting
}
```

## Port: `Dispatcher` — drains the outbox (worker role, analogue of `LedgerWriter`)

```go
// Dispatcher drains the outbox and delivers each entry through its Channel. Runs in the
// worker tier (queue-group consumers on outbox lag). At-least-once: a crash after ClaimOutbox
// re-delivers; the Channel's per-idem_key idempotency collapses a re-drive to one send.
// After MaxAttempts a poison entry parks in dead_letters (+ a dlq.dead.count alarm) — never
// dropped, never retried forever (research §18).
type Dispatcher interface {
	Drain(ctx context.Context, shard Shard, max int) (delivered int, err error)
}
```

---

## Invariants every implementation MUST uphold

1. **Recipient-scoping at the data layer (release blocker).** Every `notifications` read is constrained to `recipient` within `tenant` by an RLS policy (`user_id = current_setting('app.user_id')` within `workspace_id`). A notification is never visible or delivered to another recipient or across tenants, regardless of clearance (FR-036, **SC-012**). Scoping is enforced in the store, not in application code.
2. **The durable row is always written; preferences gate only channels.** `Notify` always persists the inbox row (it is both the inbox and the dedup backstop). `PreferenceStore` decides which *delivery channels* are enqueued; disabling `in_app` for a topic hides it from the inbox/badge but the row still exists for dedup + audit. Producers publish authoritative `Recipient`/`Tenant` — never taken from untrusted content (FR-036).
3. **Dual idempotency guard, gating only the durable write.** Every `Notify` carries an `IdemKey`. A Redis `SET NX notify:applied:{tag}:{idem}` is a fast pre-check to skip the DB round-trip on an obvious duplicate; the durable `UNIQUE(recipient, idem_key)` is the correctness backstop. **The guard MUST gate only `PersistAndEnqueue`, never channel delivery** — channel sends are driven off the durable outbox, so a crash between the guard and a channel send cannot lose a delivery (closes the `SET NX` short-circuit hole). `Receipt.Applied=false` reports a replay.
4. **Transactional persist + enqueue.** `PersistAndEnqueue` writes the inbox row and one outbox entry per target channel in **one** transaction — never as an INSERT followed by best-effort publishes. A crash after commit leaves durable outbox work the `Dispatcher` will drive; a crash before commit leaves nothing (the redelivery re-runs cleanly against the UNIQUE constraint).
5. **At-least-once channel delivery with an idempotent terminal.** The `Dispatcher` re-drives a claimed entry after a crash; each `Channel.Deliver` is idempotent on `IdemKey` (the provider dedupes), so a re-drive collapses to one send. `Suppressed=true` and `AddressBook` miss are **terminal, not retryable**.
6. **Poison messages terminate in `dead_letters`, never loop forever.** After `MaxAttempts` (default 5) an outbox entry parks in `dead_letters` with `last_error` and emits a `dlq.dead.count` alarm; it is admin-inspectable + replayable (research §18, FR-035).
7. **The unread badge is authoritative from the store.** Redis pub/sub (`notify:user:<id>`) is at-most-once — a relay that missed a publish must not diverge permanently. The badge is always recomputable via `Store.Unread`, and the SSE relay reconciles it from the store on (re)connect. Pub/sub is an optimization, never the source of truth (FR-034).
8. **Storm coalescing is a policy, not a per-event send.** High-volume same-`(recipient, topic)` bursts collapse into a digest / rate-limited summary per `DeliverySchedule.Digest`, rather than one push + one email per event (FR-038). Coalescing decisions read the schedule; channels stay dumb.
9. **Broadcast fans out off the request path.** `Broadcast` enqueues one job and returns; per-recipient expansion + delivery happen in the worker, recorded in the audit trail. A large membership never blocks or times out the caller (FR-037).
10. **Compliant email is a channel property, not the core's.** The email `Channel` checks the suppression list (`hard_bounce`/`complaint`/`unsubscribe`), adds rows from provider bounce/complaint webhooks, and appends an unsubscribe link to every non-`Essential` topic. The kernel knows nothing about email compliance — it lives in the email `Channel` impl (FR-035).
11. **Bounded growth.** Read notifications past the retention window (default 90d) are pruned via `Store.Retention`, backed by `PARTITION BY RANGE (created_at)` so expiry is a partition `DROP` and inbox + unread-count queries stay fast (FR-039).
12. **Topic + Recipient opacity.** The kernel never parses, ranks, or special-cases a `Topic`, `Recipient`, or `Tenant`. Re-anchoring the recipient (user→device→slack_channel) or the tenant (workspace→organization) changes only what the host constructs + the RLS predicate — never a kernel signature.

---

## Delivery durability (customizable knob)

One config selects the durability/latency trade for the fan-out, without changing any port signature:

| `DeliveryDurability` | Persist path | On crash mid-fan-out | Use when |
|---|---|---|---|
| `direct` | INSERT row, then deliver channels in-line | Undelivered channels wait for the JetStream redelivery to re-run the handler (safe only because delivery is idempotent) | Low volume, in-app-only, or dev — no outbox table |
| `outbox` (default) | INSERT row **+** N outbox rows in one tx; `Dispatcher` drains | Every enqueued delivery survives a crash and is re-driven; the `SET NX` hole cannot occur | Production multi-channel — the recommended posture |

Both satisfy invariants 1–7; they differ only in *when* a channel delivery becomes crash-safe. `outbox` is the default precisely because notifications have the same two-store shape (`INSERT` + external send) that made the outbox mandatory for billing.

---

## Reference wiring: ContextEngine notifications as implementations of these ports

The Phase 1 system is these ports bound to *this* app — nothing in the kernel knows that:

| Generic port / type | ContextEngine (Phase 1) binding |
|---|---|
| `Tenant` | `{Kind: "workspace", ID: workspace_id}` → Phase 2 `{Kind: "organization", …}` — a binding change, no kernel edit |
| `Recipient` | `{Kind: "user", ID: user_id}` — RLS `app.user_id`; a device/slack recipient is just another `Kind` |
| `Topic` | the 13 registered topics: `ingestion_complete`/`ingestion_failed`/`invite_received`/…/`approval_requested`/…/`admin_broadcast` — a `TopicRegistry`, not a Postgres enum |
| `Channel` (registry) | `InAppChannel` (Redis pub/sub → SSE relay) + `EmailChannel` (`kernel/mailer.go` → Resend) — two of N |
| `PreferenceStore` | `notification_preferences` rows over topic defaults (in-app on; email on for `credit_*`, `invite_received`, `task_halted`) |
| `TemplateRenderer` | per-topic Go/HTML templates (email) + inbox item copy (in-app); i18n/branding seam ready |
| `AddressBook` | `in_app` → user id; `email` → the member's verified email, minus `email_suppressions` |
| `Store` | Postgres `notifications` (RLS, range-partitioned) + `notification_outbox` + Redis `notify:applied` guard |
| `Dispatcher` | the Go `cmd/worker` queue-group consumers on `notify.<ws>` / outbox lag; DLQ sweep + `dead_letters` |

Swapping ContextEngine for, say, an incident-alerting product = register a `PagerDutyChannel` + `SlackChannel`, set `Recipient.Kind="oncall"`, register the alert `Topic`s and their templates. The persistence, preferences, idempotency, outbox, DLQ, and retention code are untouched.

---

## Reference `Channel`s: `InAppChannel` and `EmailChannel`

The two concrete implementations behind the wiring table. Both live in the product/adapter tier and depend only on `kernel/notify`.

```go
package channel // backend-go/internal/notification/channel

import (
	"context"

	"github.com/aisat/backend-go/kernel/notify"
)

// InAppChannel delivers to the browser via Redis pub/sub → SSE relay. Idempotent by nature:
// the inbox row is the source of truth and the badge is recomputed from the Store on connect,
// so a re-published event only re-nudges a live client — it can never double-count.
type InAppChannel struct{ pub RedisPublisher }

func (InAppChannel) Kind() notify.ChannelKind { return "in_app" }

func (InAppChannel) Capabilities() notify.ChannelCapabilities {
	return notify.ChannelCapabilities{NeedsSubject: false, NeedsAddress: false, RichContent: true}
}

func (c InAppChannel) Deliver(ctx context.Context, d notify.Delivery) (notify.DeliveryResult, error) {
	// PUBLISH notify:user:<recipient> {notification_id, unread_delta:+1, item} — fire-and-forget.
	// A disconnected client simply reads the durable row + reconciled badge on reconnect (invariant 7).
	if err := c.pub.Publish(ctx, "notify:user:"+d.Notification.Recipient.ID, d.Content); err != nil {
		return notify.DeliveryResult{Retryable: true, Detail: err.Error()}, nil // transient → re-drive
	}
	return notify.DeliveryResult{Delivered: true}, nil
}

// EmailChannel renders + sends via the kernel/mailer.go port (default Resend, env-swappable).
// It OWNS email compliance — the kernel knows none of this.
type EmailChannel struct {
	mailer      notify.Mailer          // kernel/mailer.go port
	suppression SuppressionStore       // hard_bounce | complaint | unsubscribe
	topics      notify.TopicRegistry   // to read TopicDef.Essential
}

func (EmailChannel) Kind() notify.ChannelKind { return "email" }

func (EmailChannel) Capabilities() notify.ChannelCapabilities {
	return notify.ChannelCapabilities{NeedsSubject: true, NeedsAddress: true, RichContent: false}
}

func (c EmailChannel) Deliver(ctx context.Context, d notify.Delivery) (notify.DeliveryResult, error) {
	if suppressed, _ := c.suppression.IsSuppressed(ctx, d.Address.Value); suppressed {
		return notify.DeliveryResult{Suppressed: true, Detail: "address suppressed"}, nil // TERMINAL, not an error
	}
	def, _ := c.topics.Lookup(d.Notification.Topic)
	body := d.Content.Body
	if !def.Essential {
		body += renderUnsubscribeFooter(d.Notification) // FR-035: non-essential mail carries an unsubscribe link
	}
	// Idempotent on IdemKey: the mailer sets a provider Idempotency-Key so a Dispatcher re-drive is one send.
	id, err := c.mailer.Send(ctx, notify.Mail{
		To: d.Address.Value, Subject: d.Content.Subject, HTML: body, IdemKey: d.Notification.IdemKey,
	})
	if err != nil {
		return notify.DeliveryResult{Retryable: true, Detail: err.Error()}, nil // → notify.email.dlq under backoff
	}
	return notify.DeliveryResult{Delivered: true, Detail: id}, nil
}
```

Notes that make them production-standard: **in-app is idempotent by construction** (durable row + reconciled badge, never a counter the publish increments authoritatively); **email compliance lives in the channel** (suppression check, essential/non-essential footer, provider idempotency key) so the kernel stays channel-agnostic; and both return `Retryable` for transient failures so the `Dispatcher` — not the channel — owns backoff + DLQ.

---

## Contract-test skeleton

These validate *any* implementation of the ports against the invariants, so a new `Channel` or a service-mode `Notifier` is held to the same guarantees. The `Channel` suite is table-driven and mostly pure; the `Store`/`Notifier` suites run against a real impl via Testcontainers (`//go:build integration`, per the repo convention).

```go
package notify_test

// ChannelContract runs against ANY notify.Channel — every channel MUST satisfy these.
func ChannelContract(t *testing.T, newChannel func(t *testing.T) notify.Channel) {
	ctx := context.Background()
	base := func() notify.Delivery {
		return notify.Delivery{
			Notification: notify.Notification{
				Tenant: notify.Tenant{Kind: "workspace", ID: "w1"},
				Recipient: notify.Recipient{Kind: "user", ID: "u1"},
				Topic: "ingestion_complete", IdemKey: "n1",
			},
			Content: notify.RenderedContent{Subject: "Done", Body: "Your upload finished."},
		}
	}

	t.Run("deliver is idempotent on idem_key", func(t *testing.T) {
		c := newChannel(t); d := base()
		r1, err := c.Deliver(ctx, d); mustNoErr(t, err)
		if !r1.Delivered && !r1.Suppressed { t.Fatal("first deliver should complete") }
		r2, err := c.Deliver(ctx, d); mustNoErr(t, err) // re-drive after a simulated crash
		if !r2.Delivered && !r2.Suppressed { t.Fatal("re-drive must be a safe no-op, not a failure") }
		if sends := c.(interface{ Sends() int }).Sends(); sends > 1 {
			t.Fatalf("double-sent on re-drive: %d", sends) // provider deduped on idem_key
		}
	})
	t.Run("suppressed address is terminal, not an error", func(t *testing.T) {
		c := newChannel(t); d := base(); d.Address = notify.Address{Value: suppressedAddr}
		r, err := c.Deliver(ctx, d); mustNoErr(t, err)
		if err == nil && !r.Suppressed { t.Skip("channel has no suppression") }
		if r.Retryable { t.Fatal("suppressed must NOT be retryable — that loops forever") }
	})
	t.Run("transient failure is retryable, never dropped", func(t *testing.T) {
		c := newChannel(t); injectTransientFailure(c)
		r, _ := c.Deliver(ctx, base())
		if r.Delivered || !r.Retryable { t.Fatal("transient failure must report Retryable=true for the Dispatcher") }
	})
}

// NotifierContract runs against a REAL notify.Notifier (Testcontainers PG+Redis).  //go:build integration
func NotifierContract(t *testing.T, newNotifier func(t *testing.T) (notify.Notifier, notify.Store)) {
	ctx := context.Background()
	ws := notify.Tenant{Kind: "workspace", ID: uuidv7()}
	u1 := notify.Recipient{Kind: "user", ID: uuidv7()}
	u2 := notify.Recipient{Kind: "user", ID: uuidv7()}
	n := func(r notify.Recipient, idem string) notify.Notification {
		return notify.Notification{Tenant: ws, Recipient: r, Topic: "invite_received", IdemKey: idem, Title: "Invite"}
	}

	t.Run("notify is idempotent — one row, one enqueue per channel", func(t *testing.T) {
		nf, st := newNotifier(t)
		r1, err := nf.Notify(ctx, n(u1, "e1")); mustNoErr(t, err)
		if !r1.Applied { t.Fatal("first notify should apply") }
		r2, err := nf.Notify(ctx, n(u1, "e1")); mustNoErr(t, err) // replay
		if r2.Applied { t.Fatal("replay must be a no-op (Applied=false)") }
		if got := countRows(t, st, u1, ws); got != 1 { t.Fatalf("duplicated row: %d want 1", got) }
	})
	t.Run("recipient-scoping: u2 never sees u1's notification (SC-012)", func(t *testing.T) {
		nf, st := newNotifier(t)
		_, _ = nf.Notify(ctx, n(u1, "e2"))
		if got, _ := st.Unread(ctx, u2, ws); got != 0 {
			t.Fatalf("cross-recipient leak: u2 sees %d of u1's notifications", got)
		}
	})
	t.Run("crash between persist and channel send loses NO delivery", func(t *testing.T) {
		nf, st := newNotifier(t)
		_, _ = nf.Notify(ctx, n(u1, "e3"))
		crashBeforeDispatch(t)                 // simulate: row + outbox committed, dispatcher never ran
		drained := drainOutbox(t, st, ws)      // recovery re-drives the durable outbox
		if drained < 1 { t.Fatal("outbox delivery was lost — the SET NX short-circuit hole") }
	})
	t.Run("email preference off ⇒ in-app row, no email enqueue", func(t *testing.T) {
		nf, st := newNotifier(t)
		setPref(t, notify.Preference{Recipient: u1, Tenant: ws, Topic: "invite_received", Channel: "email", Enabled: false})
		r, _ := nf.Notify(ctx, n(u1, "e4"))
		if hasChannel(r.Channels, "email") { t.Fatal("email disabled but still enqueued") }
		if !hasChannel(r.Channels, "in_app") { t.Fatal("in_app row must still exist") }
	})
}

// Wiring — concrete impls plug into the shared suites:
//   func TestEmailChannel_Contract(t *testing.T) { ChannelContract(t, func(t *testing.T) notify.Channel { return newFakeEmail(t) }) }
//   func TestPgNotifier_Contract(t *testing.T)   { NotifierContract(t, func(t *testing.T) (notify.Notifier, notify.Store) { return newPgNotifier(t) }) }
```

---

## Deployment topology: embedded library **or** standalone runtime service

**Yes — the notification engine can run as its own runtime service, and the ports are what make it a config+wiring change rather than a rewrite** (the same library↔service move metering and the LLM gateway make). Two shapes, one set of interfaces:

1. **Embedded library (Phase 1 default).** Callers import `kernel/notify`; producers call `Notify`/`Broadcast` in-process (a fast DB tx + Redis pre-check). The delivery half is *already* its own runtime: the `Dispatcher` runs in the `cmd/worker` role, event-driven over NATS/outbox lag.
2. **Standalone notification service.** Wrap the *same* ports behind a thin gRPC facade (`NotificationService`), hand producers a client stub, and the service owns the notifications DB + outbox + channel registry. Only the `Notifier` binding changes (in-process impl → client stub); producer logic does not.

| Concern | Extract as a service? | Note |
|---|---|---|
| Dispatcher (drain → channels → DLQ) | ✅ **already separate** | it is the `cmd/worker` role — nothing to do |
| Producer → engine decoupling | ✅ already | the `notify.<ws>` NATS subject already crosses the boundary |
| `Notify` call path | ✅ but adds one hop | keep the API coarse (one call persists + enqueues); co-locate the DB |
| `Recipient`/`Tenant` context | ✅ | already passed explicitly — no ambient RLS needed at the port |
| Templates / channels | ✅ | the `TemplateRenderer` + `Channel` registry live inside the service; adding a channel is a service deploy |
| Recipient-scoping (SC-012) | ✅ preserved | the service owns the RLS'd store; callers pass opaque ids, never query the inbox directly |

What makes it **not free** (decide before extracting):
- **Fan-in vs latency** — `Notify` is off the request's critical path already (producers fire-and-forget), so a hop is cheap; keep `Broadcast` a single coarse call, never per-recipient RPCs.
- **Multi-tenant channel config** — a shared service holds every host's channel registry + templates; isolate rate limits + provider keys per tenant so one noisy host cannot starve another's email quota.
- **Auth between producer ↔ service** — it can email/SMS on a user's behalf, so producers authenticate (mTLS / signed token); `Recipient`/`Tenant` are authoritative from the trusted producer, never from untrusted content (invariant 2).
- **Suppression + preference ownership moves with the service** — it owns `email_suppressions`, `notification_preferences`, and the unsubscribe endpoint.

**Recommended posture:** start **embedded**; extract to a **co-located service** the moment a *second* product needs to send notifications from the same channels/templates — at which point it is a deploy/config change plus a client stub, exactly the guidance metering and the LLM gateway already follow.

---

## Service surface: `NotificationService` (gRPC, contract-locked)

The service-mode facade is the SAME `Notifier` port over the wire. gRPC (not REST) because this is an internal, strongly-typed producer path; `idem_key` is required on every mutating RPC; `Recipient`/`Tenant` stay opaque `{kind,id}`.

```proto
syntax = "proto3";
package notify.v1;
option go_package = "github.com/acme/notify/api/notifyv1";
import "google/protobuf/timestamp.proto";

message Tenant    { string kind = 1; string id = 2; }   // opaque to the service
message Recipient { string kind = 1; string id = 2; }   // opaque to the service

message Notification {
  Tenant tenant = 1;
  Recipient recipient = 2;
  string topic = 3;                          // registered topic name
  string priority = 4;                       // "info" | "warning" | "critical"
  string title = 5;
  string body = 6;
  map<string,string> payload = 7;            // deep-link refs — never a routing input
  string idem_key = 8;                       // REQUIRED — exactly-once identity
  google.protobuf.Timestamp occurred_at = 9;
  map<string,string> attributes = 10;        // audit only — never a routing/pref input
}
message Receipt { string notification_id = 1; bool applied = 2; repeated string channels = 3; }

message BroadcastRequest {
  Tenant tenant = 1; string audience = 2; string topic = 3; string priority = 4;
  string title = 5; string body = 6; map<string,string> payload = 7; string idem_key = 8;
}
message BroadcastReceipt { bool applied = 1; int32 fanned = 2; }

service Notification {
  rpc Notify    (Notification)     returns (Receipt);           // persist + fan out; idempotent on idem_key
  rpc Broadcast (BroadcastRequest) returns (BroadcastReceipt);  // audience expansion OFF the request path
}
```

Contract rules baked into the surface:

- **`Notify` renders + routes *inside* the service.** The whole engine (PreferenceStore + Store + TemplateRenderer + Channels) moves server-side, so the client stub is just a *remote `Notifier`* — templates and channel keys never ship to producers. (Library mode injects the same ports in-process — identical semantics.)
- **No streaming RPC.** A `Notify` is one durable, idempotent call; live in-app updates flow over the existing SSE tier, not a notify-service stream.
- **`Broadcast` is coarse.** One call fans an audience; the service never exposes per-recipient RPCs, so a large membership is one enqueue, not N round trips (invariant 9).
- **Deadlines + fail policy are the producer's.** `Notify` is off the critical path, so a producer typically fires-and-forgets with a short deadline; a failed enqueue is retried by the producer's own bus, not swallowed.

The generated client is wrapped so it satisfies the `Notifier` interface — producer code depends on `notify.Notifier`, not on gRPC:

```go
// adapters/driving/grpcclient — a remote Notifier. Producers can't tell it from the in-process one.
type Client struct{ c notifyv1.NotificationClient }

func (m *Client) Notify(ctx context.Context, n notify.Notification) (notify.Receipt, error)          { /* map → RPC → map */ }
func (m *Client) Broadcast(ctx context.Context, b notify.BroadcastRequest) (notify.BroadcastReceipt, error) { /* map → RPC → map */ }

var _ notify.Notifier = (*Client)(nil) // ← the swap is invisible to producer code
```

---

## Extraction-ready code organization

Organize the module as **ports & adapters (hexagonal)** now, so lifting it into its own repo/service later is a `git mv` + `go mod init` — never a refactor. In this repo the unit lives at `backend-go/kernel/notify/`; the tree below is module-relative so it reads the same in-repo and after extraction. The litmus test:

> **Could I `git mv notify/ ../notify-service/ && cd ../notify-service && go mod init && go build ./...` and have it compile with zero edits?**
> It compiles iff nothing under `notify/` imports the product, its schema + config travel with it, and the only product-specific things are *injected* (the `Channel`s, the `TopicRegistry` entries, the `TemplateRenderer`, and how a `Recipient`/`Tenant` is built).

```text
notify/                            # THE EXTRACTION UNIT — self-contained, zero inbound product deps
  go.mod                           #   (optional today; the dir is already `go mod init`-ready)
  domain/                          #   pure types + invariants — imports NO infra, NO product
    tenant.go recipient.go topic.go notification.go delivery.go receipt.go
  ports/                           #   the interfaces = the hexagon's edges
    driving.go                     #     Notifier, Dispatcher            (how the world calls notify)
    driven.go                      #     Channel, ChannelRegistry, Store, PreferenceStore,
                                   #     TemplateRenderer, AddressBook, TopicRegistry, Mailer, Bus, Clock, IDs
  app/                             #   use-cases: compose prefs + store + render + channels; own the invariants
    notifier.go broadcast.go       #     implement ports.Notifier
    dispatcher.go retention.go     #     implement ports.Dispatcher + retention
  adapters/
    driven/                        #   swappable INFRA impls of the driven ports
      postgres/  inbox + outbox + reconcile queries (+ owns migrations/)
      redis/     pub/sub publisher + notify:applied guard
      nats/      Bus impl
      channel/   in_app · email(kernel mailer) · (sms · push · slack · webhook — added here)
    driving/                       #   TRANSPORTS that expose app over a boundary
      inprocess/  returns ports.Notifier directly           (library mode)
      grpcserver/ notifyv1 server over app                  (service mode)
      grpcclient/ notifyv1 client, satisfies ports.Notifier (service mode caller)
  migrations/                      #   OWNS its schema — travels with the module
    NNNN_notifications.sql  NNNN_notification_outbox.sql  NNNN_preferences.sql
    NNNN_email_suppressions.sql  NNNN_dead_letters.sql
  config.go                        #   notify.Config struct — no os.Getenv scattered in the core
```

**Dependency rule (one direction only):** `adapters → app → ports → domain`. `domain`/`ports` import nothing outside the module; `app` imports only `ports`+`domain`; `adapters` may import infra SDKs (pgx/redis/nats/grpc) but **never** the product. The product-specific bindings — the `Channel` impls, the `TopicRegistry` entries, the `TemplateRenderer`, and how a `Recipient`/`Tenant` is built from a request — are supplied by the **host at wire time**, in `cmd/`:

```go
// backend-go/cmd/api/main.go — the host wires product specifics INTO the generic module
reg := notifyapp.NewRegistry()
reg.Register(channel.NewInApp(redisPub))                 // product channel (injected driven port)
reg.Register(channel.NewEmail(mailer, suppression, topics))

nf, _ := notifyapp.New(notify.Config{/* … */}, notifyapp.Deps{  // generic core
    Channels:   reg,
    Store:      pgstore.New(db),                          // driven adapter
    Prefs:      pgstore.NewPrefs(db),                     // driven adapter
    Renderer:   templates.New(),                          // product templates
    Addresses:  directory.New(db),                        // AddressBook
    Topics:     topics,                                   // product TopicRegistry
    Bus:        natsbus.New(nc),                          // driven adapter
})
// producer code depends on notify.Notifier — today an in-process value, tomorrow grpcclient.New(conn). No caller change.
```

Enforcement + hygiene that keep the boundary honest:

- **`depguard`/`go-arch-lint`** rule: `notify/**` may not import `backend-go/internal/**` (product). CI fails the moment someone reaches across.
- **Own the schema.** notifications/outbox/preferences/suppressions/dead_letters tables live in `notify/migrations/`; the inbox row is keyed by opaque `recipient_kind`/`recipient_id` + `tenant_tag`, not `workspace_id`/`user_id`.
- **Own the config.** A `notify.Config` struct (PG DSN, Redis DSN, bus, shard count, delivery durability, retention window, max attempts) passed in — the core reads no global app config or env.
- **Abstract the bus.** `app` depends on a `Bus` *port*, not NATS, so the extracted service can keep NATS or swap it.

Net: `notify/` is a cohesive, self-contained unit whose *only* seams to the outside world are the injected `Channel`s, `TopicRegistry`, `TemplateRenderer`, `Recipient`/`Tenant` construction, and the driven infra adapters — exactly the things a different host would provide anyway.

---

## Machine-enforced boundary (lint + config)

Two linters make the extraction rules a CI gate. **go-arch-lint** enforces the internal component graph; **depguard** bans specific imports (product tier + infra SDKs in the core).

### `.go-arch-lint.yml` — the hexagon's dependency graph

```yaml
version: 3
workdir: backend-go
allow:
  depOnAnyVendor: true          # infra SDKs are gated by depguard below, not here

components:
  notify-domain:   { in: kernel/notify/domain }
  notify-ports:    { in: kernel/notify/ports }
  notify-app:      { in: kernel/notify/app }
  notify-driven:   { in: kernel/notify/adapters/driven/** }
  notify-driving:  { in: kernel/notify/adapters/driving/** }
  product:         { in: internal/** }
  cmd:             { in: cmd/** }

deps:
  notify-domain:   { mayDependOn: [] }                                  # pure — nothing
  notify-ports:    { mayDependOn: [ notify-domain ] }
  notify-app:      { mayDependOn: [ notify-domain, notify-ports ] }
  notify-driven:   { mayDependOn: [ notify-domain, notify-ports ] }     # implements ports; NOT app
  notify-driving:  { mayDependOn: [ notify-domain, notify-ports ] }     # drives via the Notifier port
  product:         { mayDependOn: [ notify-ports ] }                    # ← product sees ONLY the interfaces
  cmd:             { mayDependOn: [ notify-domain, notify-ports, notify-app,
                                    notify-driven, notify-driving, product ] }
```

The two load-bearing rows: `product → notify-ports` only (the product wires channels/templates but can't reach into `app`/adapters), and `cmd` is the *only* place allowed to assemble concrete impls.

### `.golangci.yml` — banned imports (depguard v2)

```yaml
linters:
  enable: [ depguard ]
linters-settings:
  depguard:
    rules:
      # 1. The CORE (domain + ports + app) is pure: no infra, no product.
      notify-core-pure:
        list-mode: lax
        files:
          - "**/kernel/notify/domain/**"
          - "**/kernel/notify/ports/**"
          - "**/kernel/notify/app/**"
        deny:
          - { pkg: "github.com/aisat/backend-go/internal", desc: "core must not import the product tier — keep it extractable" }
          - { pkg: "github.com/redis/go-redis",            desc: "no infra in the core; use the Store/Bus driven ports" }
          - { pkg: "github.com/jackc/pgx",                 desc: "no infra in the core; use the Store driven port" }
          - { pkg: "github.com/nats-io",                   desc: "no broker in the core; use the Bus driven port" }
          - { pkg: "google.golang.org/grpc",              desc: "no transport in the core; grpc lives in adapters/driving" }
      # 2. The WHOLE module never depends on the product (the extraction guarantee).
      notify-no-product:
        list-mode: lax
        files: [ "**/kernel/notify/**" ]
        deny:
          - { pkg: "github.com/aisat/backend-go/internal", desc: "notify/** is the extraction unit — it may not import internal/** (product)" }
      # 3. The PRODUCT touches notify only through ports (+ cmd wiring), never its guts.
      product-uses-ports-only:
        list-mode: lax
        files: [ "**/internal/**" ]
        deny:
          - { pkg: "github.com/aisat/backend-go/kernel/notify/app",             desc: "wire via cmd/ + the Notifier port, not app internals" }
          - { pkg: "github.com/aisat/backend-go/kernel/notify/adapters/driven", desc: "product must not reach into notify's infra adapters" }
```

Rule 2 *is* the litmus test as a lint: if nothing under `notify/**` imports `internal/**`, the `git mv … && go mod init` extraction compiles.

### `config.go` — the module's only configuration surface

```go
package notify

import (
	"errors"
	"fmt"
	"time"
)

type DeliveryDurability string

const (
	DeliveryOutbox DeliveryDurability = "outbox" // transactional outbox + dispatcher (default)
	DeliveryDirect DeliveryDurability = "direct" // in-line delivery; relies on redelivery for crash safety
)

// Config is the ENTIRE configuration surface of the notification module.
type Config struct {
	// Stores — driven adapters dial these; the core never does.
	StoreDSN   string // durable Postgres (notifications, outbox, preferences, suppressions, dead_letters)
	RedisURL   string // pub/sub + notify:applied guard

	// Bus — subject convention; the Bus port owns the transport.
	SubjectPrefix string // default "notify" → notify.<tag> / notify.email.<tag>

	// Sharding — N drainers scale under it with no double-deliver.
	Shards int // default 16; MUST be >= 1 and stable for a deployment's life

	// Delivery + lifecycle.
	Delivery        DeliveryDurability // default DeliveryOutbox
	MaxAttempts     int                // per-channel re-drives before dead_letters; default 5
	RetentionWindow time.Duration      // read rows older than this are pruned; default 90 days
	DefaultLocale   string             // TemplateRenderer fallback when Address.Locale == ""; default "en"
}

func (c *Config) withDefaults() {
	if c.SubjectPrefix == "" {
		c.SubjectPrefix = "notify"
	}
	if c.Shards == 0 {
		c.Shards = 16
	}
	if c.Delivery == "" {
		c.Delivery = DeliveryOutbox
	}
	if c.MaxAttempts == 0 {
		c.MaxAttempts = 5
	}
	if c.RetentionWindow == 0 {
		c.RetentionWindow = 90 * 24 * time.Hour
	}
	if c.DefaultLocale == "" {
		c.DefaultLocale = "en"
	}
}

// Validate checks the EFFECTIVE config (defaults applied to a copy; caller not mutated).
func (c Config) Validate() error {
	c.withDefaults()
	var errs []error
	if c.StoreDSN == "" {
		errs = append(errs, errors.New("StoreDSN is required"))
	}
	if c.RedisURL == "" {
		errs = append(errs, errors.New("RedisURL is required"))
	}
	if c.Shards < 1 {
		errs = append(errs, fmt.Errorf("Shards must be >= 1, got %d", c.Shards))
	}
	switch c.Delivery {
	case DeliveryOutbox, DeliveryDirect:
	default:
		errs = append(errs, fmt.Errorf("Delivery must be outbox|direct, got %q", c.Delivery))
	}
	if c.MaxAttempts < 1 {
		errs = append(errs, fmt.Errorf("MaxAttempts must be >= 1, got %d", c.MaxAttempts))
	}
	return errors.Join(errs...)
}
```

### `Deps` + `New` — product specifics injected, nothing reached into

```go
package app // kernel/notify/app

// Deps are the ports the core needs. The host supplies them in cmd/ — the product-specific
// ones are Channels, Renderer, Topics; the rest are generic infra adapters.
type Deps struct {
	Channels  ports.ChannelRegistry  // ← product channels (in_app, email, sms, …)
	Store     ports.Store            // postgres adapter (inbox + outbox)
	Prefs     ports.PreferenceStore  // postgres adapter
	Renderer  ports.TemplateRenderer // ← product templates / i18n / branding
	Addresses ports.AddressBook      // recipient → channel address
	Topics    ports.TopicRegistry    // ← product topic defaults
	Bus       ports.Bus              // nats adapter
	Clock     ports.Clock            // default: system clock
	IDs       ports.IDSource         // uuid v7
}

// New validates config, assembles the core, and returns it as the ports.Notifier interface.
func New(cfg notify.Config, d Deps) (ports.Notifier, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("notify config: %w", err)
	}
	if d.Channels == nil || d.Store == nil || d.Prefs == nil || d.Renderer == nil || d.Topics == nil || d.Bus == nil {
		return nil, errors.New("notify: Channels, Store, Prefs, Renderer, Topics and Bus are required")
	}
	// … construct the Notifier over the driven ports …
	return &notifier{cfg: cfg, deps: d}, nil
}
```

So the day-one wiring in `cmd/api/main.go` is: build a `Config`, build the driven adapters, register the product `Channel`s + `Topic`s + `TemplateRenderer`, call `app.New` → get a `ports.Notifier`. Swapping to service mode later replaces that one `app.New(...)` with `grpcclient.New(conn)` — both return `ports.Notifier`, and the two linters guarantee nothing downstream depended on more than that interface.

---

## Generalization checklist (before reusing this in another system)

Copy-paste and tick per new host system:

- [ ] **Tenant + Recipient defined** — pick the isolation subject (`workspace`/`org`/`account`) and the delivery subject (`user`/`device`/`slack_channel`); construct both from request context; set the RLS predicate to match. No kernel signature changes.
- [ ] **Channels registered** — one `Channel` per delivery target the host needs, each idempotent on `IdemKey` and returning `Suppressed`/`Retryable` correctly; registered in `cmd/`, never in the fan-out.
- [ ] **Topics registered** — the host's event types as `TopicRegistry` entries with default channels, priority, template ref, and the `Essential` flag; no DB enum.
- [ ] **Templates implemented** — a `TemplateRenderer` per (topic, channel, locale); i18n + per-tenant branding live here, nowhere else.
- [ ] **Preferences modeled** — `PreferenceStore` over topic defaults; unsubscribe flips a channel off; quiet-hours/digest via `DeliverySchedule`.
- [ ] **AddressBook wired** — resolve each `Recipient` to a channel `Address`; a miss is terminal, not a retry.
- [ ] **Delivery durability chosen** — `outbox` (default, crash-safe multi-channel) vs `direct` (in-app-only / dev); document the choice in the runbook.
- [ ] **Dispatcher wired** — worker role drains the outbox; DLQ sweep + `dead_letters` + `MaxAttempts` alarm present; retention prune scheduled.
- [ ] **Recipient-scoping enforced** — RLS on the store restricts the inbox to `recipient` within `tenant`; verified by the `NotifierContract` leak test (SC-012).
- [ ] **Idempotency backstop present** — `UNIQUE(recipient, idem_key)` on the store; every producer supplies an `IdemKey`; the Redis guard gates only the durable write.
- [ ] **Badge is store-authoritative** — the unread count is recomputed from the store and reconciled on SSE (re)connect; pub/sub is an optimization only.

Extraction-readiness (so it lifts into its own service later without a refactor):

- [ ] **Import-clean module** — nothing under `notify/` imports the product (`internal/**`); a `depguard`/`go-arch-lint` rule enforces it in CI.
- [ ] **Litmus passes** — `git mv notify/ …/ && go mod init && go build ./...` compiles with zero edits.
- [ ] **Schema travels** — notifications/outbox/preferences/suppressions/dead_letters live in `notify/migrations/`; inbox keyed by opaque `recipient` + `tenant_tag`, not `workspace_id`/`user_id`.
- [ ] **Config self-contained** — a `notify.Config` struct is passed in; the core reads no global app config or env.
- [ ] **Bus abstracted** — `app` depends on a `Bus` port, not a concrete broker.
- [ ] **Both transports present** — `driving/inprocess` (library) and `driving/grpcserver`+`grpcclient` (service), both satisfying `Notifier`.

---

## Non-goals (stays in the host, by design)

- **Copy, localization, branding** — a `TemplateRenderer` implementation, never kernel code. The kernel routes and persists; it does not know what a notification *says*.
- **What triggers a notification** — producers (ingestion, billing, invite, agent, admin) build `Notification`s. The kernel does not instrument callers.
- **Channel provider integrations** — Resend/SES/Twilio/APNs/Slack live in `Channel` adapters + `kernel/mailer.go`. The core sees only the `Channel` interface + `DeliveryResult`.
- **Tenancy + identity semantics** — what a `Tenant`/`Recipient` *means*, and the RLS predicate, are the host's. The kernel treats them as opaque identities.
- **Audience resolution** — how `"workspace_members"`/`"admins"` expands to recipients is the host's directory concern, invoked by `Broadcast`, not modeled in the kernel.
```