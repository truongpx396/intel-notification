# Data model

The authoritative schema is [`migrations/`](../../migrations/), verified against PostgreSQL 16 by
`make verify-schema`. This document explains the shapes and the rules that the DDL cannot express.

Reference implementation is PostgreSQL + Redis, but nothing in the ports requires them — `Store` is
a driven port. What follows describes the reference binding.

## Identity: three nested axes, all opaque

```
realm          "my-product"                 ← WHICH PRODUCT. Outermost. Isolates the key space.
  tenant       {kind:"workspace", id:"w1"}  ← the host's isolation boundary
    recipient  {kind:"user", id:"u1"}       ← the host's delivery subject
```

The engine never parses any of them. It uses them to scope rows, derive keys and derive a shard.
Re-anchoring a recipient from `user` to `device` is a change in what the host constructs, plus the
RLS predicate — never a change to a table shape or an engine signature (NR-018).

The `realm` exists because without it, two products sharing one deployment share an idempotency key
space; see [D1](design-decisions.md#d1).

---

## Tables

### `notifications` — the durable inbox

One row per notification per recipient. Written on every `Notify`, whatever the preferences say,
because the row is *both* the inbox entry and the dedup backstop (NR-001).

| Column | Notes |
|---|---|
| `id`, `created_at` | Composite primary key. `created_at` is in the key because it is the partition key and Postgres requires it |
| `realm`, `tenant_kind`, `tenant_id` | Isolation |
| `recipient_kind`, `recipient_id` | Delivery subject |
| `topic` | A **registered string**, not an enum — a new topic is a registration (NR-017) |
| `priority` | `info` / `warning` / `critical`; `critical` overrides quiet hours ([D9](design-decisions.md#d9)) |
| `title`, `body` | Channel-agnostic source text; channels render from it via the template seam |
| `payload` | Deep-link refs for the UI. **Never** a routing input |
| `attributes` | Trace id, source subject. Audit only — **never** a routing or preference input |
| `idem_key` | Producer-supplied exactly-once identity (NR-002) |
| `read_at` | `NULL` = unread. Drives the badge |

**Partitioned** by range on `created_at`, so retention is a metadata operation (NR-022). The
bootstrap migration creates three months; provisioning further months is an operator job — see
[migrations/README.md](../../migrations/README.md). There is deliberately **no default partition**:
a missing partition should fail an insert loudly rather than silently accumulate rows in a catch-all
that can never be retired.

**RLS** is enabled *and* forced, with the predicate covering realm, tenant and recipient. Forced
matters: without it the table owner bypasses its own policy, which is exactly the connection a
worker uses.

### `notify_idem` — the exactly-once guard

`(realm, recipient_kind, recipient_id, idem_key)` as the primary key, non-partitioned, written in the
same transaction as the inbox row.

This is a separate table because the obvious design — a unique index on the inbox itself — **cannot
be created** on a partitioned table. That is not a preference; see [D2](design-decisions.md#d2) for
the error and why adding the partition key to the constraint would silently destroy the guarantee.

Being non-partitioned also means the guard outlives inbox partitions, so retiring old inbox data
cannot make an old key replayable.

### `notification_outbox` — pending deliveries

One row per enabled channel, inserted in the **same transaction** as the inbox row (NR-003). This is
the mechanism that makes a crash survivable: after commit, every intended delivery exists durably.

| Column | Notes |
|---|---|
| `notification_id` | Deliberately **not** a foreign key — see [D6](design-decisions.md#d6) |
| `shard` | `crc32(realm\|recipient_kind\|recipient_id) mod Shards`, stable ([D11](design-decisions.md#d11)) |
| `attempts`, `next_attempt_at` | Jittered exponential backoff ([D10](design-decisions.md#d10)) |
| `claimed_at`, `delivered_at` | Claim is visible so a stuck drainer is diagnosable |
| `terminal_reason` | `suppressed` / `no_address` / `max_attempts` — constrained, and distinct from `last_error` ([D12](design-decisions.md#d12)) |

Unique on `(notification_id, channel)`: at most one pending delivery per channel per notification,
enforced rather than assumed. The drain claim rides a partial index over due, undelivered,
non-terminal rows for one shard.

### `notification_preferences` — opt-in state

One row per `(realm, tenant, recipient, topic, channel)` with one `enabled` boolean. An **absent row
means the topic's registered default**, so a new topic needs no backfill and a new channel needs no
migration ([D3](design-decisions.md#d3)).

### `notification_schedules` — quiet hours and digest cadence

One row per recipient: `quiet_start`, `quiet_end`, `timezone`, `digest_window`. An absent row means
immediate delivery with no quiet hours. The timezone is stored per recipient because quiet hours are
meaningless without it, and a tenant-level timezone is wrong for any distributed team.

### `channel_suppressions` — addresses a channel must stop using

`(realm, channel, address)` with a reason and a nullable `expires_at`. Keyed by channel because a
dead device token and a revoked webhook are the same event as a hard bounce
([D13](design-decisions.md#d13)); expiring because a full mailbox is a cooldown, not a life sentence
([D14](design-decisions.md#d14)).

Rows arrive from provider bounce/complaint webhooks and from unsubscribes. A channel checks this
table itself — the engine knows nothing about email compliance (invariant 10).

### `dead_letters` — the terminal park

Partitioned by `created_at` with its own retention window ([D8](design-decisions.md#d8)), because
dead letters accumulate fastest during exactly the incident when the database is least able to
absorb them. Carries the payload, attempt count, last error and a `replayed_at` stamp so a replay is
recorded rather than inferred.

### `digest_buffer` — an open coalescing window

The state that makes storm coalescing implementable at all ([D4](design-decisions.md#d4)). A partial
unique index on `flushed_at IS NULL` enforces **one open window** per
`(recipient, topic, channel)` — the constraint lives in the database, not in the hope that every
code path remembers it.

### `channel_quotas` — per-tenant channel budget

`max_per_hour`, `max_per_day` and an `on_exhausted` policy per `(realm, tenant, channel)`. The hot
counter lives in Redis; this table holds the configured ceiling ([D5](design-decisions.md#d5)).

---

## Redis keys

Redis holds only ephemeral or reconstructible state. Losing it costs latency, never correctness.

| Key | Role | Durability |
|---|---|---|
| `notify:applied:{realm}:{tenant_tag}:{idem}` | Fast pre-check to skip a database round-trip on an obvious duplicate. **Gates the durable write only** | ephemeral; the durable guard is the backstop |
| `notify:inbox:{realm}:{recipient}` | Pub/sub channel for the real-time nudge | transient, at-most-once |
| `notify:quota:{realm}:{tenant}:{channel}:{window}` | Rolling quota counter | `volatile-ttl` |

The pre-check being ephemeral is the point: if Redis is empty, every `Notify` takes one extra
database round-trip and stays correct. If it were the only guard, an eviction would permit a
double-notify.

## Invariants the DDL cannot express

1. The inbox row and its outbox rows are written in one transaction, or neither is written.
2. `notify_idem` is written in that same transaction.
3. The fast pre-check never gates delivery — only the durable write.
4. A notification's shard is derived once and never recomputed with a different `Shards` value.
5. Retention retires inbox partitions and dead-letter partitions independently.
6. The unread count is always recomputable from `notifications`; any cached count is advisory
   ([D15](design-decisions.md#d15)).

These are asserted by the contract tests in
[contracts/notification-ports.md](contracts/notification-ports.md), because a schema cannot hold them.
