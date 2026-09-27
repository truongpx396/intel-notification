# Design decisions

Each entry states the decision, the alternative, and **what goes wrong without it**. A decision with
no failure mode attached is a preference, and preferences do not belong in a specification.

Decisions marked **inherited-gap** resolve something the originating design left open, contradictory,
or unimplementable. Those are the ones worth reading first: D1, D2, D3, D4, D5, D6, D7, D9, D10, D11.

---

## D1 — `Realm` is the outermost isolation axis · *inherited-gap*

**Decision.** Every key, subject and row carries a `Realm` — the product/deployment identity —
outside the tenant. The idempotency guard is `(realm, recipient_kind, recipient_id, idem_key)`, not
`(recipient, idem_key)`.

**Alternative.** Tenant as the outermost axis, as inherited.

**What goes wrong without it.** Two products sharing one notification deployment share an
idempotency key space. Product A sends `invite:42:received`; product B later sends its own
`invite:42:received` for an unrelated invite 42 and it is **silently swallowed as a replay**. The
recipient is never notified and nothing errors — the guard did its job, against the wrong key. This
is the cheapest thing to add now and the most expensive to retrofit, because retrofitting means
rewriting every historical key.

Verified: `verify-schema.sql` TEST 1 shows the same recipient and idem_key coexisting across realms.

## D2 — The idempotency guard is its own non-partitioned table · *inherited-gap*

**Decision.** `notify_idem (realm, recipient_kind, recipient_id, idem_key)` is a separate,
non-partitioned table, written in the same transaction as the inbox row. The inbox keeps
`PARTITION BY RANGE (created_at)` and a `PRIMARY KEY (id, created_at)`.

**Alternative.** The inherited design: a global unique index on `(user_id, idem_key)` placed directly
on a partitioned inbox.

**What goes wrong without it.** It does not exist. PostgreSQL requires every unique constraint on a
partitioned table to include all partition-key columns:

```
ERROR:  unique constraint on partitioned table must include all partitioning columns
DETAIL:  UNIQUE constraint on table "inherited_notifications" lacks column "created_at"
         which is part of the partition key.
```

Adding `created_at` to the constraint would satisfy Postgres and destroy the guarantee — the same
`idem_key` at a different timestamp becomes a different key, so every replay after the clock ticks is
accepted. The two properties are mutually exclusive on one table, and that index is what NS-002 rests
on.

A second benefit makes the split better than either compromise: the guard **outlives inbox
partitions**. Retiring an aged partition under retention cannot resurrect the ability to
double-notify an old key, which it would if the guard lived inside the retired data.

Verified: `make verify-schema` reproduces the failure, then applies the fix. Same defect class as
[intel-payment's D21](https://github.com/truongpx396/intel-payment/blob/main/PROVENANCE.md).

## D3 — Preferences are one row per `(topic, channel)` · *inherited-gap*

**Decision.** `notification_preferences` is keyed by `(realm, tenant, recipient, topic, channel)`
with a single `enabled` boolean. An absent row means "use the topic's registered default".

**Alternative.** The inherited `in_app BOOL, email BOOL` column pair.

**What goes wrong without it.** Every new channel becomes a schema change, plus a backfill, plus an
edit to every query that reads preferences. That directly contradicts the `Channel` registry the same
design introduced to make adding a channel a one-line `Register` call: the fan-out would be pluggable
while preferences stayed hard-coded, so registering an SMS channel would deliver SMS to everyone with
no way to opt out until a migration shipped. The row-per-channel shape makes the schema as extensible
as the registry.

## D4 — `digest_buffer` holds an open coalescing window · *inherited-gap*

**Decision.** A `digest_buffer` table holds one **open** window per
`(realm, tenant, recipient, topic, channel)`, enforced by a partial unique index on
`flushed_at IS NULL`. Notifications arriving inside the window append to `member_ids`; the dispatcher
renders one digest at `flush_at` and enqueues a single outbox entry.

**Alternative.** Coalesce in memory in the dispatcher, or at render time.

**What goes wrong without it.** Invariant 8 requires storm coalescing and `DeliverySchedule.Digest`
names the window, but the inherited design had **nowhere to hold a deferred notification** — the
outbox drains an entry as soon as it is due, so "collapse a burst into one digest" had no state to
collapse into. The invariant was unimplementable as written. In-memory coalescing fails the moment
the dispatcher restarts: the window's contents are lost, and the notifications in it are either lost
with them or re-sent individually, which is the storm the invariant exists to prevent.

The partial unique index is deliberate: "one open window" is a constraint the database holds, not a
convention every code path has to remember.

Verified: `verify-schema.sql` TEST 4 — a second open window is rejected; after a flush a new one opens.

## D5 — `channel_quotas` bounds per-tenant channel spend · *inherited-gap*

**Decision.** A `(realm, tenant, channel)` quota with `max_per_hour`, `max_per_day` and an
`on_exhausted` policy defaulting to `defer`. The hot counter lives in Redis; the table holds the
ceiling.

**Alternative.** Rely on the provider's own rate limits.

**What goes wrong without it.** The design names "isolate rate limits + provider keys per tenant so
one noisy host cannot starve another's email quota" as a prerequisite for shared-service mode, then
defines no state for it. Relying on the provider means the *provider's* limit is the only limit — so
one tenant looping a buggy producer consumes the shared sending budget and **every other tenant's
email stops**, with the failure surfacing as provider 429s in a worker log rather than as a quota
event attributable to the tenant that caused it.

`on_exhausted` defaults to `defer` rather than `drop_non_essential` because deferring is recoverable
and dropping is not.

## D6 — No foreign key from the outbox into the inbox · *inherited-gap*

**Decision.** `notification_outbox.notification_id` carries no foreign-key constraint. The pairing is
an invariant the `Store` upholds and the contract tests assert.

**Alternative.** A composite foreign key from the outbox to `notifications (id, created_at)`.

**What goes wrong without it.** Nothing at runtime — but *with* it, retention breaks. A foreign key
into a partitioned parent makes retiring an aged partition **fail** while any outbox row still
references a row inside it, so one stuck dead-lettered entry blocks retention indefinitely. Retention
is the mechanism that keeps the inbox queryable (NR-022), so the constraint turns routine maintenance
into an outage whose cause sits three layers away from its symptom.

The tradeoff is explicit: referential integrity is given up on a pair written in one transaction and
never updated, in exchange for a retention path that cannot deadlock.

Verified: `verify-schema.sql` TEST 5 asserts zero foreign keys reference the inbox.

## D7 — `AudienceResolver` is a port · *inherited-gap*

**Decision.** Add a driven port:

```go
// AudienceResolver expands a host-defined audience selector into recipients, in pages.
// Paged because a large tenant's membership must not be materialized in one slice.
type AudienceResolver interface {
    Resolve(ctx context.Context, t Tenant, audience string, cursor string) (
        recipients []Recipient, nextCursor string, err error)
}
```

**Alternative.** Leave audience expansion to the host, as inherited.

**What goes wrong without it.** `Broadcast` takes an `Audience string` and invariant 9 requires the
engine to expand it off the request path — but no port existed to expand it *with*. So the one
operation whose entire purpose is fan-out had no seam, and a host could only implement it by reaching
past `Notifier` and calling `Notify` per recipient itself. That moves the deterministic per-recipient
`IdemKey` derivation into host code, where every host derives it slightly differently and a retried
broadcast double-notifies.

Paging belongs in the port, not in an implementation note: a non-paged resolver materializes an
entire tenant's membership in memory, which is exactly the shape that times out on the tenant that
matters most.

## D8 — `dead_letters` is partitioned and retained

**Decision.** `dead_letters` is `PARTITION BY RANGE (created_at)` with its own retention window,
separate from the inbox's.

**Alternative.** An unbounded table, as inherited.

**What goes wrong without it.** Dead letters accumulate fastest precisely during an incident — a
provider outage parks thousands of entries in minutes. An unbounded table means the table recording
the incident contributes to it, and the natural operator reaction (a mass row purge) is the slowest
possible recovery on the busiest possible table. Partitioning makes reclaiming that space a metadata
operation instead.

## D9 — Quiet hours yield to `critical` · *inherited-gap*

**Decision.** `DeliverySchedule` quiet hours defer `info` and `warning`. A `critical` notification
delivers immediately regardless, and the override is recorded on the delivery for audit.

**Alternative.** Quiet hours apply uniformly.

**What goes wrong without it.** Quiet hours and a critical alert are in direct conflict and the
inherited design ranked neither, which means the implementation picks — silently, and probably
uniformly. Deferring `credit_exhausted` or `task_halted` until 07:00 means the recipient discovers a
stopped production workload eight hours late, having been "protected" from knowing. The inverse error
— waking someone for an `ingestion_complete` — is what quiet hours legitimately prevent, so the rule
is a priority rank, not a global switch.

Escalation for an *unacknowledged* critical notification is Phase 2; this decision only settles
whether quiet hours may suppress one.

## D10 — Backoff is exponential with full jitter · *inherited-gap*

**Decision.** Retry at `min(base * 2^attempt, ceiling)` with **full jitter** — the actual delay is
uniformly random in `[0, computed]`. Defaults: `base` 10s, `ceiling` 1h, `MaxAttempts` 5.

**Alternative.** `MaxAttempts: 5`, as inherited, with no interval specified.

**What goes wrong without it.** Five retries with no interval is five *immediate* retries, which
against a transient provider failure is five failures inside a few milliseconds and a dead letter
that should have succeeded. Worse, fixed or purely exponential backoff **synchronizes**: a provider
outage fails every in-flight delivery at once, so all of them retry at the same computed instant and
the recovering provider is hit by the entire parked backlog simultaneously. Full jitter spreads the
herd across the window, which is the whole reason to prefer it over plain exponential backoff.

## D11 — `Shard` is a stable hash of the recipient · *inherited-gap*

**Decision.** `shard = crc32(realm | recipient_kind | recipient_id) mod Config.Shards`. `Shards` is
fixed for a deployment's life; changing it requires draining the outbox first, and the runbook says so.

**Alternative.** The inherited `Shard` type with no derivation rule.

**What goes wrong without it.** Two things. Sharding on anything non-stable (round-robin, arrival
order, notification id) puts two deliveries for one recipient in different shards, so two drainers
process them concurrently and the per-recipient ordering digest coalescing assumes does not hold. And
changing `Shards` while entries are in flight re-maps existing rows to different drainers — the old
drainer's claim and the new one's both match, and the entry is **delivered twice**, defeating
exactly-once at the one layer that cannot dedupe it.

Hashing the recipient rather than the tenant also matters: tenant-hashing puts every recipient in a
large tenant on a single shard, so the biggest tenant gets the least parallelism.

## D12 — Terminal outcomes are a constrained column

**Decision.** `notification_outbox.terminal_reason` is restricted by a check constraint to
`suppressed`, `no_address` or `max_attempts`, and is distinct from `last_error`.

**Alternative.** Infer terminality from `attempts >= max` plus a free-text error.

**What goes wrong without it.** `Suppressed` and an `AddressBook` miss are terminal at attempt 1 —
they are correct outcomes, not failures. Inferring terminality from the attempt count means a
suppressed address is retried five times before parking, generating five provider calls guaranteed to
fail, per notification. Free-text alone means the distinction between "we will not deliver this,
correctly" and "we gave up" is not queryable, so an operator cannot tell a healthy dead-letter table
from an unhealthy one.

## D13 — Suppressions are per channel, not email-only

**Decision.** `channel_suppressions (realm, channel, address)` with reasons spanning `hard_bounce`,
`complaint`, `unsubscribe`, `invalid_address`, `token_revoked`.

**Alternative.** The inherited `email_suppressions`.

**What goes wrong without it.** A revoked Slack webhook, an unregistered APNs device token and a
disconnected number are all the same event — a permanently undeliverable address — and each would
need its own table, its own check inside its own channel, and its own webhook handler. Every channel
after email would re-derive suppression from scratch, and the ones written in a hurry would skip it
and retry a dead token forever.

## D14 — A suppression may expire

**Decision.** `channel_suppressions.expires_at` is nullable; `NULL` means permanent.

**Alternative.** All suppressions permanent.

**What goes wrong without it.** Soft failures get treated as hard ones. A full mailbox or a briefly
unreachable webhook is a *cooldown*, but a permanent-only model either suppresses that address forever
— losing every future notification to a recipient whose mailbox was full for an hour — or does not
suppress it at all and retries into a known failure. The nullable column lets a channel express "not
for an hour" without inventing a second mechanism.

## D15 — The unread badge is always recomputable

**Decision.** `Store.Unread(recipient, tenant)` is the authoritative count. Redis pub/sub carries a
nudge, never an authoritative delta, and the relay recomputes from the store on every (re)connect.

**Alternative.** Maintain the count in Redis, incremented on publish.

**What goes wrong without it.** Redis pub/sub is at-most-once with no delivery receipt. A relay that
misses one publish — a reconnect, a failover, a dropped subscriber — is permanently wrong, and nothing
ever corrects it, because the counter has no source to reconcile against. The user sees a badge
showing 3 over an inbox holding 5, indefinitely. Deriving the count from the durable rows makes every
reconnect self-healing, and makes pub/sub what it should be: an optimization that lowers latency and
cannot affect correctness.

---

## Open decisions

None blocking Phase 1. Two are deferred to Phase 2 by design, recorded here so their absence is
visible rather than implicit:

| Question | Why it waits | Where |
|---|---|---|
| How is delivery **acknowledgement** modelled? | Acknowledgement introduces a per-notification lifecycle; putting that state in the Phase 1 hot path would couple delivery to workflow | [Phase 2](../002-escalation-workflows/) |
| What happens when an **escalation chain** exhausts every step? | Depends on the acknowledgement model above | [Phase 2](../002-escalation-workflows/) |
