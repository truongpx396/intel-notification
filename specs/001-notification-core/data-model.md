# Data model

The authoritative schema is [`migrations/`](../../migrations/), verified against PostgreSQL 16 by
`make verify-schema`. This document explains the shapes and the rules that the DDL cannot express.

Reference implementation is PostgreSQL + Redis, but nothing in the ports requires them — `Store` is a
driven port. What follows describes the reference binding.

## Identity: three nested axes, all opaque

```
realm          "my-product"                 ← WHICH PRODUCT. Outermost. Isolates the key space.
  tenant       {kind:"workspace", id:"w1"}  ← the host's isolation boundary
    recipient  {kind:"user", id:"u1"}       ← the host's delivery subject
```

The engine never parses any of them. It uses them to scope rows, derive keys and derive a shard.
Re-anchoring a recipient from `user` to `device` is a change in what the host constructs — never a
change to a table shape, an engine signature, or the RLS predicate (NR-018).

The `realm` exists because without it, two products sharing one deployment share an idempotency key
space ([D1](design-decisions.md#d1)); the tenant is in the key because without it one user in two
tenants shares one ([D18](design-decisions.md#d18)).

### The canonical encoding

Every string derived from an identity uses one encoding: each part written as
`<octet length>:<part>`, joined by `,`.

```
canonical("aisat", "workspace", "w1", "user", "u1") = "5:aisat,9:workspace,2:w1,4:user,2:u1"
```

Joining opaque ids with a bare separator collides as soon as an id contains it, and a collision in a
dedup key drops a notification while one in a stream key leaks it. `notify_canonical()` implements the
encoding in SQL and the Go domain implements it against the same frozen vector (TEST 19).

| Derived value | Definition |
|---|---|
| Shard | `crc32(canonical(realm, tenant_kind, tenant_id, recipient_kind, recipient_id)) mod Shards` |
| Address key | `hex(sha256(canonical(channel, normalized value)))[:32]` |
| Delivery idempotency key | `hex(sha256(canonical(realm, tenant…, recipient…, idem_key \| "digest:"+id, channel, address_key)))` |
| Broadcast member key | `hex(sha256(canonical("broadcast", broadcast_idem_key, recipient_kind, recipient_id)))` |
| Suppression key | `sha256(canonical(channel, normalized address))` |
| Erasure record | `sha256(canonical(realm, tenant…, recipient…))` |

---

## Row-level security

Four tables are **recipient-scoped**: `notifications`, `notification_preferences`,
`notification_schedules`, `recipient_addresses`. Each, and every partition of `notifications`, carries
the same forced policy, applied by `notify_apply_recipient_scope()`:

```sql
realm = current_setting('notify.realm', true) AND tenant_kind = … AND tenant_id = …
  AND recipient_kind = … AND recipient_id = …
```

Three rules make it hold ([D17](design-decisions.md#d17)):

- **Forced**, because the worker connects as the table owner, and an owner bypasses its own policy
  unless forced.
- **On every partition**, because PostgreSQL applies a partitioned table's policy only to queries
  through the parent. Partition provisioning calls `notify_apply_recipient_scope()` for each new
  partition, and TEST 5 fails if any partition lacks it.
- **Set per transaction** with `set_config(name, value, true)`. A session `SET` on a pooled connection
  outlives the request. Identity columns reject `''`, so an unset scope — which reads back as `''` once
  any transaction in the session has set it — matches nothing.

The remaining tables that hold a recipient — `notify_idem`, `notification_outbox`,
`notification_deliveries`, `dead_letters`, `digest_buffer` — are **worker-only**: never read on a
recipient-facing path. TEST 5 fails if a new table holds a recipient without being classified.

---

## Tables

### `notifications` — the durable inbox

One row per notification per recipient. Written on every `Notify`, whatever the preferences say,
because the row is *both* the inbox entry and the dedup record (NR-001).

| Column | Notes |
|---|---|
| `id`, `created_at` | Composite primary key. `created_at` is the partition key and Postgres requires it in the key |
| `realm`, `tenant_*`, `recipient_*` | Identity. Non-empty, so RLS stays fail-closed |
| `topic` | A **registered string**, not an enum (NR-017) |
| `priority` | `info` / `warning` / `critical` |
| `data` | Template variables. Copy is rendered from these ([D27](design-decisions.md#d27)) |
| `title`, `body` | Optional **fallback** copy, used only when no template matches |
| `payload` | Deep-link refs for the UI. **Never** a routing input |
| `attributes` | Trace id, source. Audit only |
| `idem_key` | Producer-supplied, 1–255 bytes |
| `inbox_visible` | False when no inbox-backed channel was enabled ([D26](design-decisions.md#d26)) |
| `visible_from` | A scheduled notification stays out of the inbox until then ([D29](design-decisions.md#d29)) |
| `seen_at`, `read_at`, `archived_at` | Three independent inbox states |
| `canceled_at` | Set by `Cancel`; the row leaves the inbox |

**Partitioned** by range on `created_at`; retention drops whole partitions — read and unread alike
([D33](design-decisions.md#d33)). There is deliberately **no default partition**: a missing partition
fails an insert loudly rather than silently filling a catch-all that can never be retired.

Two indexes: `notifications_recipient_idx (identity, created_at DESC)` for the list and per-recipient
maintenance, and the partial `notifications_unread_idx` over unread, visible, unarchived, uncanceled
rows for the bounded badge count ([D15](design-decisions.md#d15)).

### `notify_idem` — the exactly-once guard

Primary key `(realm, tenant_kind, tenant_id, recipient_kind, recipient_id, idem_key)`, written in the
same transaction as the inbox row, **before** it: a conflict means a replay and nothing else is
written. Carries the inbox row's `notification_created_at`, so a replay or a cancel finds it in one
partition.

A separate table because a unique index on the inbox **cannot be created** on a table partitioned by
`created_at` ([D2](design-decisions.md#d2)). Bounded by the idempotency window: `notify_expire_idem()`
deletes older keys in batches ([D21](design-decisions.md#d21)). Hash-partitioned 16 ways on the identity
so expiry and vacuum work one partition at a time.

### `notification_outbox` — the queue

One row per **pending** delivery. Written in the same transaction as the inbox row (NR-003). A row
leaves when its delivery finishes ([D20](design-decisions.md#d20)), so the table's size is the backlog.

| Column | Notes |
|---|---|
| `notification_id` + `notification_created_at`, **or** `digest_id` | Exactly one, by check constraint. No foreign key ([D6](design-decisions.md#d6)) |
| identity columns | The full identity, so the worker can scope its read ([D17](design-decisions.md#d17)) |
| `channel`, `address_key`, `address` | `address_key` is `''` until bound; then one row per address ([D25](design-decisions.md#d25)) |
| `fallback` | The rest of the channel chain ([D29](design-decisions.md#d29)) |
| `shard` | A contention hint, stored, never recomputed ([D11](design-decisions.md#d11)) |
| `attempts` | Counted at claim ([D19](design-decisions.md#d19)) |
| `deferrals` | Quota and quiet-hours deferrals — not attempts |
| `next_attempt_at` | Due time; while claimed, the lease expiry |
| `lease_token`, `claimed_at` | The current claim's fence |
| `deliver_before` | Expiry |
| `quiet_hours_override` | A critical delivery inside quiet hours ([D9](design-decisions.md#d9)) |

Unique on `(notification_id, channel, address_key)` and on `(digest_id, address_key)`. The claim rides
`(shard, next_attempt_at)`; bulk tenant deferral rides `(realm, tenant, channel, next_attempt_at)`.
Autovacuum runs at 1% dead tuples, because every claim, retry and deferral is an `UPDATE`.

### `notification_deliveries` — the delivery history

One row per finished delivery, appended by the terminal transition. `outcome` is one of `delivered`,
`suppressed`, `no_address`, `max_attempts`, `rejected`, `expired`, `canceled`, `dropped_quota`,
`fanned_out` ([D12](design-decisions.md#d12)). Carries `provider_message_id` — indexed with the channel,
so a bounce callback finds its delivery — and the latest `provider_status`. Range-partitioned by
`completed_at`, retired after `DeliveryLogRetention`. It is what `Notifier.Status` reads.

### `notification_broadcasts` — broadcast jobs

One row per broadcast, keyed by `(realm, tenant, idem_key)` — its own idempotency guard. Holds either an
`audience` selector or an inline `recipients` list, the request template, the resolver `cursor`, a lease,
and a status. Each page's notifications commit with the cursor advance (NR-020). Completed rows are
deleted after the idempotency window.

### `notification_preferences` — recipient choices

One row per `(realm, tenant, recipient, topic, channel)` with one `enabled` boolean
([D3](design-decisions.md#d3)). Recipient-scoped.

### `notification_tenant_preferences` — tenant defaults

One row per `(realm, tenant, topic, channel)` with `enabled` and `locked`. Resolution: a recipient row
wins unless the tenant row is locked; then the tenant row; then the topic default
([D30](design-decisions.md#d30)). Essential topics cannot be disabled at any level.

### `notification_schedules` — quiet hours and digest cadence

One row per recipient: `quiet_start`, `quiet_end`, `timezone`, `digest_window`. An absent row means
immediate delivery with no quiet hours. Recipient-scoped. An address's own timezone, when present,
takes precedence for quiet-hours math.

### `channel_suppressions` — addresses a channel must stop using

`(realm, channel, address_hash)` with a reason, optional `detail` (never the address) and nullable
`expires_at` ([D13](design-decisions.md#d13), [D14](design-decisions.md#d14)). The **dispatcher** checks
it before every delivery. Keyed by hash so it can outlive the recipient's erasure
([D35](design-decisions.md#d35)).

### `dead_letters` — genuine failures

Only `max_attempts`, `rejected` and bus `poison` ([D8](design-decisions.md#d8)). Carries identity (for
filtering and erasure), the queue row as `payload` — never rendered content — the reason, attempts, last
error, and `replayed_at`. Partitioned, retained for `DeadLetterRetention`.

### `digest_buffer` — coalescing windows

One row per window; `member_ids` bounded by `DigestMax`. At most one window per
`(recipient, topic, channel)` has `sealed_at IS NULL`; a full or due window is sealed, then flushed once
([D4](design-decisions.md#d4)). Flushed windows whose delivery finished are deleted by
`notify_expire_digests()`.

### `channel_quotas` — per-tenant channel budget

`max_per_hour`, `max_per_day` and `on_exhausted` per `(realm, tenant, channel)`; the empty tenant pair
is the realm-wide default ([D5](design-decisions.md#d5)). The hot counter lives in Redis.

### Catalog tables (`0003`) — the data-backed ports

| Table | Port it backs | Notes |
|---|---|---|
| `notification_topics` | `TopicRegistry` | Default channels, fallback chain (disjoint), priority, essential, template ref |
| `notification_templates` | `TemplateRenderer` | Versioned, immutable; one active version per (tenant override, ref, channel, locale) |
| `recipient_addresses` | `AddressBook` | Many per (recipient, channel). Recipient-scoped — personal data |
| `channel_providers` | channel construction | Ordered by priority for failover; `secret_ref` points at a credential, never holds one |
| `erasure_requests` | — | Proof of erasure by identity hash ([D35](design-decisions.md#d35)) |

### `notify_job_leases` — single-owner jobs

One row per job; `notify_try_lease_job()` grants it to one owner at a time
([D22](design-decisions.md#d22)).

---

## State transitions (`0004`)

The transitions whose correctness depends on several atomic writes or a fencing check are SQL
functions, so every adapter and the verification suite share one implementation.

| Function | Transition |
|---|---|
| `notify_claim_outbox(shard, limit, lease)` | Lease due rows `FOR UPDATE SKIP LOCKED`; count the attempt; issue a token |
| `notify_retry_delivery(id, token, next, error)` | Transient failure → due at `next`; releases the lease |
| `notify_defer_delivery(id, token, until, reason)` | Quota / quiet hours → due at `until`; returns the attempt |
| `notify_finish_delivery(id, token, outcome, …)` | Queue → history; dead-letter genuine failures; enqueue the next fallback |
| `notify_bind_addresses(id, token, addresses)` | Bind one address in place, or fan out to one row per address |
| `notify_defer_tenant_channel(…, until)` | Move a tenant's due backlog on one channel out of the claim range |
| `notify_rehome_shards(n)` | After lowering `Shards`, fold retired shards into live ones |
| `notify_digest_append(…)` / `notify_digest_flush(id)` | Join or open a window, sealing at max / flush once |
| `notify_cancel(…, idem_key)` | Cancel pending deliveries; report in-flight ones |
| `notify_expire_idem(before, batch)` / `notify_expire_digests(before, batch)` | Bounded expiry |
| `notify_try_lease_job(job, owner, ttl)` | Single-owner lease |
| `notify_erase_recipient(…)` | Erase one recipient everywhere; record by hash |

Every outcome write takes the claim's lease token and changes nothing without it.

---

## Redis keys

Redis holds only ephemeral or reconstructible state. Losing it costs latency, or loosens quotas — never
correctness. No key contains an identity or an address in plaintext.

| Key | Role | Durability |
|---|---|---|
| `notify:applied:<realm>:<hex sha256(canonical(identity, idem_key))>` | Pre-check. **Read** before the transaction, **written after commit** with the notification id, TTL ≤ the idempotency window ([D18](design-decisions.md#d18)) | ephemeral; `notify_idem` is the backstop |
| `notify:inbox:{<hex sha256(canonical(identity))>}` | Live nudge channel; carries a notification id only ([D16](design-decisions.md#d16)) | at-most-once |
| `notify:quota:{<hex sha256(canonical(realm, tenant, channel))>}:<window>` | Quota counters; the hour and day windows share a hash tag so one script updates both atomically | loss resets the budget (fail-open on quota) |

Braces are Redis Cluster hash tags, used only where keys must share a slot: one recipient's stream
stays on one slot so sharded pub/sub (`SPUBLISH`/`SSUBSCRIBE`) reaches only the relays for it, and one
tenant-channel's quota windows stay together. The pre-check is a single-key operation and carries no
tag — tagging it by realm would put a whole product's traffic on one slot.

## Invariants the DDL cannot express

1. The guard, the inbox row, the queue rows, digest appends and drop records are written in one
   transaction, or none is.
2. The guard is written first; a conflict writes nothing else.
3. The pre-check is written only after commit and never gates delivery.
4. Scope is transaction-local; a session `SET` is never used.
5. An address, once bound to a delivery, is never re-bound.
6. The unread count is recomputed from rows; any cached count is advisory.
7. Retention retires inbox, history and dead-letter partitions independently; partition provisioning
   applies the scope policy to every new `notifications` partition.

These are asserted by the contract tests in
[contracts/notification-ports.md](contracts/notification-ports.md#contract-tests), because a schema
cannot hold them.
