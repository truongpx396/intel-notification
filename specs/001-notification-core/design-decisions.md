# Design decisions

Each entry states the decision, the alternative, and **what goes wrong without it**. A decision with
no failure mode attached is a preference, and preferences do not belong in a specification.

Four kinds of entry carry a tag:

- **inherited-gap** resolves something the originating design left open, contradictory, or
  unimplementable: D1, D2, D3, D4, D5, D6, D7, D9, D10, D11.
- **review** fixes a defect found by the architecture review of this repository after the
  extraction, in the design as first written here: D16–D37, plus the amendments marked on D2, D4,
  D5, D7, D8, D11, D12, D13 and D15. Several were real correctness bugs in the normative contract —
  D16 (a cross-tenant leak on the live stream), D17 (a worker that could not read what it delivers),
  D18 (a pre-check that dropped notifications) and D22 (a delivery mode that reintroduced the bug the
  outbox exists to fix). Read those first.
- **measured** comes from running the system rather than reading it: D38, and the amendments marked on
  D19 and D20. Each states what was run and what was not.
- **built** comes from implementing a stage and finding the specification silent where the code had to
  choose: D39–D53. Each names the test that holds the choice, so a later reader can tell what was
  decided from what merely happened.

Every entry that claims a property names the test that asserts it: a schema test (`TEST n` in
[`scripts/verify-schema.sql`](../../scripts/verify-schema.sql), `make verify-schema`), an integration
test (`TestX` in [`adapters/driven/postgres`](../../adapters/driven/postgres/), `make test-integration`,
against a real PostgreSQL), or a unit test (in [`domain`](../../domain/), `make test`).

---

## D1 — `Realm` is the outermost isolation axis · *inherited-gap*

**Decision.** Every key, subject and row carries a `Realm` — the product/deployment identity —
outside the tenant. The idempotency guard is keyed by realm, tenant and recipient ([D18](#d18)), not
by `(recipient, idem_key)`.

**Alternative.** Tenant as the outermost axis, as inherited.

**What goes wrong without it.** Two products sharing one notification deployment share an
idempotency key space. Product A sends `invite:42:received`; product B later sends its own
`invite:42:received` for an unrelated invite 42 and it is **silently swallowed as a replay**. The
recipient is never notified and nothing errors — the guard did its job, against the wrong key. This
is the cheapest thing to add now and the most expensive to retrofit, because retrofitting means
rewriting every historical key.

Verified: TEST 1 — the same recipient and idem_key coexist across realms.

## D2 — The idempotency guard is its own non-partitioned table · *inherited-gap* · *amended by review*

**Decision.** `notify_idem` is a separate table keyed by
`(realm, tenant_kind, tenant_id, recipient_kind, recipient_id, idem_key)`, written in the same
transaction as the inbox row. The inbox keeps `PARTITION BY RANGE (created_at)` and a
`PRIMARY KEY (id, created_at)`. The guard is **hash**-partitioned on the identity and bounded by an
idempotency window ([D21](#d21)); it is not range-partitioned, because a range partition key would
have to join the primary key and would destroy the guarantee exactly as below.

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

*Amendment.* The first version of this decision presented "the guard outlives inbox partitions" as a
benefit and gave the table no retention. That made it the one table in the system that grows forever —
one row per notification ever sent — in direct contradiction of NR-022. [D21](#d21) bounds it; the
split itself stands.

Verified: `make verify-schema` reproduces the failure, then applies the fix. Same defect class as
[intel-payment's D21](https://github.com/truongpx396/intel-payment/blob/main/PROVENANCE.md).

## D3 — Preferences are one row per `(topic, channel)` · *inherited-gap*

**Decision.** `notification_preferences` is keyed by `(realm, tenant, recipient, topic, channel)`
with a single `enabled` boolean. An absent row falls back to the tenant default, then the topic's
registered default ([D30](#d30)).

**Alternative.** The inherited `in_app BOOL, email BOOL` column pair.

**What goes wrong without it.** Every new channel becomes a schema change, plus a backfill, plus an
edit to every query that reads preferences. That directly contradicts the `Channel` registry the same
design introduced to make adding a channel a one-line `Register` call: the fan-out would be pluggable
while preferences stayed hard-coded, so registering an SMS channel would deliver SMS to everyone with
no way to opt out until a migration shipped. The row-per-channel shape makes the schema as extensible
as the registry.

## D4 — `digest_buffer` holds a coalescing window · *inherited-gap* · *amended by review*

**Decision.** A `digest_buffer` row holds one window per `(realm, tenant, recipient, topic, channel)`.
At most one window is **open** (accepting members) at a time, enforced by a partial unique index on
`sealed_at IS NULL`. A window seals when it reaches `DigestMax` members or its `flush_at` passes;
`Digests.FlushDigest` then enqueues exactly one delivery for it, conditionally on
`flushed_at IS NULL`. The digest's queue row names the window (`digest_id`), not a notification.

**Alternative.** Coalesce in memory in the dispatcher, or at render time.

**What goes wrong without it.** Invariant 8 requires storm coalescing and `DeliverySchedule.Digest`
names the window, but the inherited design had **nowhere to hold a deferred notification** — the
outbox drains an entry as soon as it is due, so "collapse a burst into one digest" had no state to
collapse into. In-memory coalescing fails the moment the dispatcher restarts: the window's contents are
lost, and the notifications in it are either lost with them or re-sent individually, which is the
storm the invariant exists to prevent.

*Amendment.* The first version keyed "one open window" on `flushed_at IS NULL`, promised exactly one
digest per burst (NS-005), and separately capped a window at `DigestMax = 100` — three statements that
cannot all hold for a burst of 10,000. It also left the digest's queue row undefined: the outbox was
unique on `(notification_id, channel)`, and a digest has no single notification. Now a full window
**seals** and the next member opens a fresh one, so a burst of N yields `ceil(N / DigestMax)` digests
(NS-005 says so), a flush is idempotent under duplicate ticks, and a queue row is for exactly one
notification or one window (a check constraint). Flushed windows are deleted by
`Maintenance.ExpireDigests` once their delivery finishes.

Verified: TEST 7 — a second open window is rejected. `TestDigestWindows` — a full window seals and
the next member opens another; a duplicate flush is a no-op; one window yields one delivery.
`TestDigestAppendUnderConcurrency` — 32 concurrent appenders produce `ceil(32 / 10)` windows holding
every member once, never more than one open. `TestExpireDigests`.

## D5 — `channel_quotas` bounds per-tenant channel spend · *inherited-gap* · *amended by review*

**Decision.** A `(realm, tenant, channel)` quota with `max_per_hour`, `max_per_day` and an
`on_exhausted` policy defaulting to `defer`. The hot counter lives in Redis; the table holds the
ceiling. A row with an empty tenant pair is the **realm-wide default**. `drop_non_essential` never
drops an essential topic or a `critical` notification — those always defer.

**Alternative.** Rely on the provider's own rate limits.

**What goes wrong without it.** The design names "isolate rate limits + provider keys per tenant so
one noisy host cannot starve another's email quota" as a prerequisite for shared-service mode, then
defines no state for it. Relying on the provider means the *provider's* limit is the only limit — so
one tenant looping a buggy producer consumes the shared sending budget and **every other tenant's
email stops**, with the failure surfacing as provider 429s in a worker log rather than as a quota
event attributable to the tenant that caused it.

*Amendment.* Without a realm default, NS-007 held only for tenants somebody remembered to configure.
And the first version's comment said dropping a *critical* notification was never acceptable while the
policy keyed on *essential* — so a critical, non-essential notification could be dropped. Both are
now explicit. How exhaustion stops a tenant from occupying the drainers is [D24](#d24).

## D6 — No foreign key into a partitioned table · *inherited-gap*

**Decision.** No foreign key references `notifications`, `notification_deliveries` or `dead_letters`.
The queue-to-inbox pairing is an invariant the `Store` upholds and the contract tests assert.

**Alternative.** A composite foreign key from the outbox to `notifications (id, created_at)`.

**What goes wrong without it.** Nothing at runtime — but *with* it, retention breaks. A foreign key
into a partitioned parent makes retiring an aged partition **fail** while any row still references a
row inside it, so one stuck delivery blocks retention indefinitely. Retention is the mechanism that
keeps the inbox queryable (NR-022), so the constraint turns routine maintenance into an outage whose
cause sits three layers away from its symptom.

Verified: TEST 9 — no foreign key references a partitioned table or a partition.

## D7 — `AudienceResolver` is a port · *inherited-gap* · *amended by review*

**Decision.** A driven port expands a host-defined audience selector into recipients, in pages:

```go
type AudienceResolver interface {
    Resolve(ctx context.Context, t Tenant, audience string, cursor string) (
        recipients []Recipient, nextCursor string, err error)
}
```

Expansion runs off the request path from a durable `notification_broadcasts` row, one page per
transaction, with the cursor advanced in the same transaction as that page's notifications.

**Alternative.** Leave audience expansion to the host, as inherited.

**What goes wrong without it.** `Broadcast` takes an `Audience string` and invariant 9 requires the
engine to expand it off the request path — but no port existed to expand it *with*. So the one
operation whose entire purpose is fan-out had no seam, and a host could only implement it by reaching
past `Notifier` and calling `Notify` per recipient itself. That moves the deterministic per-recipient
`IdemKey` derivation into host code, where every host derives it slightly differently and a retried
broadcast double-notifies.

*Amendment.* The first version handed the job to the bus and recorded nothing durable: a crash
mid-expansion restarted a 50,000-member tenant from page one, and "was this broadcast already
applied?" had no state to answer from. A service-mode container has no host code to resolve an
audience with, so it accepts either a bounded inline recipient list or a `Directory` callback the host
serves ([D31](#d31)).

## D8 — `dead_letters` is partitioned, retained, and holds genuine failures only · *amended by review*

**Decision.** `dead_letters` is `PARTITION BY RANGE (created_at)` with its own retention window. Only
`max_attempts`, `rejected` and bus `poison` messages are dead letters; the reason is a constrained
column. Its payload is the queue row, never rendered content.

**Alternative.** An unbounded table holding every terminal delivery, as inherited.

**What goes wrong without it.** Dead letters accumulate fastest precisely during an incident — a
provider outage parks thousands of entries in minutes. An unbounded table means the table recording
the incident contributes to it, and the natural operator reaction (a mass row purge) is the slowest
possible recovery on the busiest possible table.

*Amendment.* The first version also sent suppressed and no-address deliveries here while alarming on
"any increase". Every hard bounce would page someone for the engine working correctly, and the alarm
that means "a user was not told something" would drown. The runbook grouped dead letters by a
`terminal_reason` column the table did not have. See [D12](#d12).

Verified: `TestFinish` — a genuine failure is dead-lettered with its reason. TEST 10 — the schema
refuses a correct outcome as a dead letter.

## D9 — Quiet hours yield to `critical` · *inherited-gap*

**Decision.** `DeliverySchedule` quiet hours defer `info` and `warning`. A `critical` notification
delivers immediately regardless, and the override is recorded on the delivery
(`quiet_hours_override`) for audit.

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
uniformly random in `[0, computed]`. Defaults: `base` 10s, `ceiling` 1h, `MaxAttempts` 5. A provider's
own `Retry-After` hint, when a channel reports one, is a floor on the delay.

**Alternative.** `MaxAttempts: 5`, as inherited, with no interval specified.

**What goes wrong without it.** Five retries with no interval is five *immediate* retries, which
against a transient provider failure is five failures inside a few milliseconds and a dead letter
that should have succeeded. Worse, fixed or purely exponential backoff **synchronizes**: a provider
outage fails every in-flight delivery at once, so all of them retry at the same computed instant and
the recovering provider is hit by the entire parked backlog simultaneously. Full jitter spreads the
herd across the window, which is the whole reason to prefer it over plain exponential backoff.

## D11 — `Shard` is a contention hint, not a correctness boundary · *inherited-gap* · *rewritten by review*

**Decision.** `shard = crc32(canonical(realm, tenant, recipient)) mod Config.Shards`, stored on the
queue row at insert and never recomputed. A shard only tells claimers where to look first so that N
workers do not all contend for the head of one index. Correctness comes from the row claim
([D19](#d19)), so **changing `Shards` is an online operation**: raising it needs nothing; lowering it
needs one `Maintenance.RehomeShards` call to fold rows from retired shards into live ones.

**Alternative.** The first version of this decision: shards fixed for the deployment's life, with
changing the count documented as requiring a full drain because "the old drainer's claim and the new
one's both match, and the entry is delivered twice".

**What goes wrong without it.** That diagnosis was wrong, and the rule it produced was an outage with
a runbook. The shard is stored on the row, so a count change re-maps nothing in flight. The hazard it
described — two claimers matching one row — is real, but it already happens with two replicas of one
shard or a rolling deploy, which the same design encouraged. The fix for it is an atomic row claim,
not a frozen shard count. Freezing the count while leaving the claim unspecified bought an offline
migration and left the actual double-claim unguarded.

Per-recipient ordering was the other argument for recipient-hash sharding. It never held: a retried
delivery is re-queued behind later ones. Ordering is not guaranteed and nothing depends on it. The
shard still hashes the recipient rather than the tenant, so the largest tenant is spread across every
shard instead of confined to one.

Verified: `TestClaimLeasesAndSkipsLockedRows` — a concurrent claimer on the same shard skips the rows
another holds. `TestRehomeShards` — after lowering the count, every row is in a live shard and still
claimable.

## D12 — Terminal outcomes are a constrained vocabulary · *amended by review*

**Decision.** A finished delivery records exactly one outcome in `notification_deliveries.outcome`:
`delivered`, or `suppressed`, `no_address`, `max_attempts`, `rejected`, `expired`, `canceled`,
`dropped_quota`, `fanned_out`. It is distinct from free-text `detail`. Only `max_attempts` and
`rejected` are dead letters. The `Store` port records outcomes through `RecordOutcome`, which carries
the outcome kind — not a `retryable bool`.

**Alternative.** Infer terminality from `attempts >= max` plus a free-text error.

**What goes wrong without it.** `Suppressed` and an `AddressBook` miss are terminal at attempt 1 —
they are correct outcomes, not failures. Inferring terminality from the attempt count means a
suppressed address is retried five times before parking, generating five provider calls guaranteed to
fail, per notification. Free-text alone means the distinction between "we will not deliver this,
correctly" and "we gave up" is not queryable, so an operator cannot tell a healthy dead-letter table
from an unhealthy one.

*Amendment.* The first version's port was `MarkFailed(retryable bool, detail)`, which could not say
*which* terminal reason applied — the reason this decision exists was inexpressible through the port
that was supposed to record it. It also had no place for a provider message id on success, so a bounce
webhook could not be traced back to its delivery. `rejected` (a permanent provider refusal unrelated to
the address, such as a malformed payload) is new: without it such a failure burned all five attempts
before parking.

Verified: `TestDispositionOf` (the policy, in `domain`), `TestFinish` (the store records each outcome
and does exactly what the disposition says), TEST 10 (the vocabulary is a check constraint).

## D13 — Suppressions are per channel, and checked by the dispatcher · *amended by review*

**Decision.** `channel_suppressions (realm, channel, address_hash)` with reasons spanning
`hard_bounce`, `complaint`, `unsubscribe`, `invalid_address`, `token_revoked`, `soft_bounce`. The
**dispatcher** checks it before every delivery on every channel; a channel reports a newly dead address
through `DeliveryResult`, and the engine writes the suppression.

**Alternative.** The inherited `email_suppressions`, checked inside the email channel.

**What goes wrong without it.** A revoked Slack webhook, an unregistered APNs device token and a
disconnected number are all the same event — a permanently undeliverable address — and each would
need its own table, its own check inside its own channel, and its own webhook handler.

*Amendment.* The first version generalized the table but still left the *check* to each channel, which
is the half of the problem that matters: every channel after email would re-derive the check, and the
ones written in a hurry would skip it. The channel contract test even had a `t.Skip` for channels with
no suppression, so skipping it passed. Keying by hash is [D35](#d35).

## D14 — A suppression may expire

**Decision.** `channel_suppressions.expires_at` is nullable; `NULL` means permanent.

**Alternative.** All suppressions permanent.

**What goes wrong without it.** Soft failures get treated as hard ones. A full mailbox or a briefly
unreachable webhook is a *cooldown*, but a permanent-only model either suppresses that address forever
— losing every future notification to a recipient whose mailbox was full for an hour — or does not
suppress it at all and retries into a known failure. The nullable column lets a channel express "not
for an hour" without inventing a second mechanism.

## D15 — The unread badge is always recomputable, and bounded · *amended by review*

**Decision.** `Store.Unread(recipient, tenant)` is the authoritative count, derived from rows. It is a
**bounded** count: it stops at `UnreadCap` (default 99) and reports `capped`, so the UI shows "99+". A
real-time push carries a nudge, never a count or delta, and the relay recomputes from the store on
every (re)connect.

**Alternative.** Maintain the count in Redis, incremented on publish.

**What goes wrong without it.** Redis pub/sub is at-most-once with no delivery receipt. A relay that
misses one publish — a reconnect, a failover, a dropped subscriber — is permanently wrong, and nothing
ever corrects it, because the counter has no source to reconcile against. Deriving the count from the
durable rows makes every reconnect self-healing.

*Amendment.* Recomputing on every reconnect has a cost the first version did not price: redeploying
the relay reconnects every client at once, and an unbounded `count(*)` per client over a large inbox is
a thundering herd on the primary. The count now reads at most `UnreadCap + 1` rows from a partial index
over unread rows, and the reference in-app channel no longer publishes the `unread_delta: +1` its code
comment promised, which contradicted this decision.

---

## D16 — The live push path is isolated as strictly as the data layer · *review*

**Decision.** A real-time nudge is published on
`notify:inbox:{<hex sha256(canonical(realm, tenant, recipient))>}` and carries only
`{notification_id}`. The relay subscribes solely to the key derived from the **authenticated
session's** identity — never from a client-supplied id — and on each nudge loads the item through the
RLS-scoped store under that same identity, then pushes the item and the bounded unread count.

**Alternative.** The first version: the reference in-app channel published the **rendered content** to
`"notify:user:" + Recipient.ID`.

**What goes wrong without it.** That key has no realm, no tenant and no recipient kind. Two tenants —
or two products — each with a user `u1` share one pub/sub channel, and each receives the other's
notification text live on the SSE stream. NS-001 treats any such leak as a release blocker, and
row-level security was no help: the push path never touched the database. Hashing a canonical,
length-prefixed encoding of the full identity makes the key collision-free; publishing only an id and
re-reading under RLS means that even a misrouted or forged nudge yields zero rows, so the push path
inherits the data layer's isolation instead of needing its own. The braces are a Redis Cluster hash
tag, so sharded pub/sub (`SPUBLISH`) keeps one recipient's stream on one slot.

Verified: `StreamIsolationContract` ([notification-ports.md](contracts/notification-ports.md#contract-tests)).

## D17 — The worker runs under row-level security, and can · *review*

**Decision.** Three rules together:

1. Every queue row carries the **full** recipient identity and the inbox row's `created_at`.
2. Scope is set per transaction with `set_config(name, value, true)` — the dispatcher sets it from the
   queue row before reading the notification. Session-level `SET` is forbidden.
3. `notify_apply_recipient_scope()` applies the one policy, forced, to every recipient-scoped table
   **and every partition** of one; partition provisioning calls it for each new partition.

**Alternative.** The first version: a queue row with only realm and tenant, scope "set per
transaction" with no mechanism named, and the policy on the partitioned parent only.

**What goes wrong without it.** Three separate failures:

- Under `FORCE ROW LEVEL SECURITY` the dispatcher could not learn the recipient without reading the
  notification, and could not read the notification without knowing the recipient. Its join silently
  returned `NULL` — verified against PostgreSQL 16 during the review — and the only way out was a
  sequential scan of `notify_idem`, the largest table, which had no index on `notification_id`.
- A session-level `SET` on a pooled connection outlives the request and serves the next request the
  previous recipient's inbox.
- PostgreSQL applies a partitioned table's policy only to queries through the parent. A partition
  queried by name handed the owning role every recipient's rows, so "enforced at the data layer" held
  only for code that remembered to use the parent.

Verified: TEST 4 (as the owner, including a partition by name), TEST 5 (every scoped relation and
partition is forced with the identical predicate), TEST 6 (the worker reads what it delivers; the scope
dies with the transaction). The previous RLS test ran as a *non-owner* role, which is subject to RLS
even without `FORCE`, so it never tested the case its own documentation said mattered.

## D18 — One key space, one encoding, and a pre-check that can only miss · *review*

**Decision.**

- The durable guard's key includes the **tenant** ([D2](#d2)).
- Every derived key — pre-check, pub/sub, shard, delivery idempotency key — is built from one
  **canonical encoding**: each part written as `<octet length>:<part>`, joined by `,`
  (`domain.Canonical`, tested against a frozen vector computed independently of the code).
- The Redis pre-check key is `notify:applied:<realm>:<hex sha256(canonical(realm, tenant, recipient, idem_key))>`
  — exactly the durable guard's key space. It is **read** before the transaction and **written only
  after commit**, with a TTL no longer than the idempotency window. `NotifyTx` reads it and never
  writes it.

**Alternative.** The first version: `SET NX notify:applied:{realm}:{tenant_tag}:{idem}` as the
pre-check, a guard keyed without the tenant, and `Tenant.Tag() = kind + ":" + id`.

**What goes wrong without it.** A pre-check is safe only if its errors are misses. That one made three
kinds of false hit, each a silently dropped notification:

- **Coarser key.** No recipient in the Redis key: the owner and an admin both notified with
  `invoice:42:overdue`, and the second is short-circuited before it is ever persisted.
- **Set before commit.** `SET NX` *is* the check, so it ran before the transaction. If the transaction
  then failed, the key stayed set and the redelivery was skipped — the exact bug the outbox was
  introduced to fix, moved one step earlier.
- **Ambiguous encoding.** Tenant `("a:b","c")` and `("a","b:c")` tag identically.

In the other direction the durable guard lacked the tenant, so a user in two workspaces lost the second
workspace's `weekly_digest:W39` as a "replay" — [D1](#d1)'s failure mode one axis down.

Verified: TEST 2 (tenant in the key); `TestCanonical`, `TestCanonicalIsUnambiguous` and
`TestDerivedKeys` (frozen vectors; no collision across the axes each key must separate); the pre-check's
ordering is `NotifierContract` "a failed transaction leaves no pre-check entry".

## D19 — The claim is a fenced lease · *review*

**Decision.** `Queue.Claim(shard, limit, lease)` selects due rows `FOR UPDATE SKIP LOCKED`,
increments `attempts`, issues a fresh `lease_token`, and sets `lease_expires_at` to `now() + lease`.
The due time, `next_attempt_at`, is not touched. Every state change (`Retry`, `Defer`, `Finish`,
`BindAddresses`) must present the current token and affects nothing otherwise, reporting `ok=false`.

**Alternative.** The first version specified `ClaimOutbox` as "pops up to max due entries" with a
`claimed_at` column and no lease, lock or fence.

**What goes wrong without it.** Without an atomic claim, two workers on one shard — or one worker
during a rolling deploy — both take the same row and both send it. Without a lease, a worker that dies
holding rows either strands them or they are re-driven while it is still sending. Without a fence, the
dead worker's late "delivered" overwrites the live one's outcome. Counting the attempt *at claim* is
what makes a delivery that crashes its worker every time — a poison payload that panics the renderer —
reach the attempt ceiling instead of looping forever; counting on failure never counts a crash.

Verified: `TestClaimLeasesAndSkipsLockedRows` — a second session claims only the row the first does
not hold; leased rows are invisible until the lease lapses; each claim counts. `TestFencedWrites` —
every state change, with a forged token and with the token of a lapsed lease, changes nothing.
`TestClaimLeavesTheDueTimeAlone`, `TestReleasedRowIsClaimableAtOnce`, and the held-row cases of
`TestDeferTenantChannel` and `TestCancel` — the amendment below. `make verify-schema` TEST 12 — no
index covers a column a claim writes, and the table leaves room in each page. Mutation-checked:
removing `SKIP LOCKED`, the fence on any write, or the attempt count each fails a test, and so does
dropping the lease predicate from the claim, tenant deferral or cancel, leaving the lease set on
`Retry` or `Defer`, moving `next_attempt_at` in the claim, indexing `lease_expires_at`, or removing
the fillfactor.

*Amendment (measured with `make bench`).* The first version kept the lease in `next_attempt_at`, an
indexed column, so no claim could be a HOT update: `hot_upd_%` was 0, and every claim wrote a new row
version and an entry in all five of the table's indexes. The lease now lives in `lease_expires_at`,
which is in no index and no index predicate, and the table has `fillfactor = 60` so the new version
fits in the same page. Measured on a laptop (PostgreSQL 16 in Docker, `fsync` on, a 20,000-row backlog),
so read the ratios, not the absolute figures:

| | lease in `next_attempt_at` | lease in `lease_expires_at`, fillfactor 60 |
|---|---|---|
| HOT updates, in-app style (claim, finish) | 0% | 96–100% |
| WAL per delivery, in-app style | ≈ 1.7 KB | ≈ 1.1 KB |
| HOT updates, with address binding | 0% | ≈ 50% |
| WAL per delivery, with address binding | ≈ 2.4 KB | ≈ 1.9 KB |
| Claim p95, 4 workers, batch 100 | ≈ 5 ms | ≈ 2 ms |
| Throughput | | unchanged, within noise |

Fillfactor 80 kept 80% of claims HOT with 16 workers, and 100 kept 30–40%. Three costs:

- A held row stays in the due range of the claim index, and the claim skips it on the heap check.
  There are only as many as workers hold, spread over `Shards`; claim p95 with 1,000,000 pending rows
  was 4.7 ms.
- A delivery that binds an address (email, SMS, push) is still one non-HOT update, because
  `address_key` is in the unique index. That is the ≈ 50%. Removing it means changing when addresses
  are bound, which is a separate decision.
- The lease and the due time are two columns to keep consistent: `Retry`, `Defer` and tenant deferral
  clear the lease along with the token, and "held" means `lease_expires_at > now()` everywhere.

## D20 — The queue holds pending work only; history is a separate log · *review*

**Decision.** `notification_outbox` is a queue: a row exists only while its delivery is pending.
`Queue.Finish` removes it and appends it to `notification_deliveries` — range-partitioned by
completion time, retired by partition — in one statement, together with any dead letter and the next
fallback channel.

**Alternative.** The first version kept every row in the outbox forever, marked `delivered_at`.

**What goes wrong without it.** A queue that keeps its history grows without bound (NR-022 said nothing
about it), and every claim, retry and deferral is an `UPDATE` on that ever-larger table — dead-tuple
churn and vacuum pressure proportional to all traffic ever, not to the backlog. Splitting them keeps the
hot table as small as the work in flight, and puts history where the producer's status query and the
provider's bounce callback need it: `notification_deliveries` is indexed by notification id and by
`(channel, provider_message_id)`.

Verified: `TestFinish` — finished deliveries leave the queue; a provider callback finds its delivery
by provider message id; a refused dead letter leaves the row queued and no history written, because the
transition is one statement.

*Amendment (measured with `make soak`).* "As small as the work in flight" holds for live rows, not for
the table's size on disk, which lags by one vacuum cycle. At 1,000 notifications/s the live set stayed at
a few hundred rows while the table was 50–90 MB in steady state, and it grew four to six times that under
a long-running transaction. See
[operations.md](../../docs/operations.md#protecting-the-queue-from-its-own-database).

## D21 — Idempotency is guaranteed for a window, not forever · *review*

**Decision.** A key guards replays for `Config.IdempotencyWindow` (default 7 days, minimum 24 hours).
`Maintenance.ExpireIdem` deletes older keys in batches, run as a single-owner job. Exactly-once is
stated as holding *within the window*.

**Alternative.** The first version kept every key forever and called it a feature.

**What goes wrong without it.** One row per notification ever sent, in a table with a six-column text
primary key, is the largest table in the system and the only one retention never touches. At a
sustained 200 notifications a second that is 6 billion rows a year. Producers retry within minutes,
buses redeliver within hours; a key's useful life is bounded, and the industry norm reflects it —
Stripe prunes idempotency keys after 24 hours. Hash-partitioning the table on the identity keeps each
expiry batch and each vacuum inside one partition.

Verified: TEST 8 (16 hash partitions, an index by age), `TestExpireIdem` (batches drain aged keys and
keep fresh ones).

## D22 — One delivery path, and no broker required · *review*

**Decision.**

- There is **no `direct` mode.** Every notification goes through the outbox.
- The dispatcher **polls** each shard adaptively: immediately again after a full batch, backing off to
  `PollInterval` (default 500 ms) when a shard is empty. When `Notify` commits in a process that also
  runs a dispatcher, it wakes that dispatcher in memory.
- A transactional `NOTIFY` wakeup is optional (`ListenNotify`, off by default).
- Scheduled work — retention, digest flush, idempotency expiry, quota rollover — is single-owner through
  row leases in `notify_job_leases`, not through a broker tick.

**Alternative.** The first version: a `direct` mode that inserted the row and delivered in-line, plus a
bus nudge subject and broker-driven ticks for everything else.

**What goes wrong without it.** `direct` reintroduced the original bug. A crash after the insert and
before the send left the redelivered handler facing the durable guard, which answered "replay", so the
undelivered channels were never sent. The documentation claimed both modes satisfied the same
invariants; they did not. The latency `direct` promised is available without a second durability model:
in-memory wakeup covers the common case, and polling bounds the rest well inside NS-004. `NOTIFY` is
off by default because PostgreSQL serializes the commit of every transaction that issued one behind a
database-wide lock, which becomes the bottleneck at exactly the write rates this engine targets.
Leases rather than advisory locks, because session advisory locks do not survive transaction-pooling
proxies.

Verified: `TestTryLeaseJob` (a live job lease cannot be taken; a lapsed one passes to another worker).

## D23 — A notification is accepted when PostgreSQL commits · *review*

**Decision.** `Notify` returns after the transaction commits, and that commit is the acceptance point
NS-003 measures from. Producers that publish to a bus instead of calling `Notify` get NS-003 only if the
bus acknowledges after replication; the reference ingest adapter is NATS JetStream with replicated
streams. Redis Streams is not a supported ingest bus.

**Alternative.** The first version made Redis Streams the default bus, with the producer's publish as
the hand-off.

**What goes wrong without it.** A Redis primary acknowledges a write before replicating it. A failover
in that window loses the acknowledged notification, and "no delivery lost to a crash" silently depends
on which node crashed. The research this design rests on makes exactly this argument against `SET NX`
(§10); it applies to `XADD` unchanged. With the commit as the acceptance point, the default deployment
needs no broker at all.

## D24 — Tenant fairness is enforced by moving backlogs, not hoped for from FIFO · *review*

**Decision.** Quota is enforced at two points:

- **At enqueue**, the `Notifier` reads the tenant's counters; if the channel's budget is already spent,
  the delivery is written due at the window's reset (or dropped under `drop_non_essential`, recorded as
  `dropped_quota`).
- **At dispatch**, a delivery that finds the budget spent is deferred (`Queue.Defer`, which returns
  its attempt), and `Queue.DeferTenantChannel` moves that tenant's entire due backlog on that channel to
  the reset in one statement.

**Alternative.** The first version checked quota at dispatch only, deferring one row at a time.

**What goes wrong without it.** Claims take due rows oldest first. A tenant that floods the system
occupies the head of every shard, so drainers spend their throughput claiming and deferring its rows
one by one while other tenants' deliveries wait behind them. The exhausted tenant *did* reduce everyone
else's delivery rate — NS-007, violated by the mechanism meant to uphold it. Moving the backlog out of
the due range costs one indexed statement per exhaustion, after which the claim goes straight to other
tenants.

Verified: `TestDeferTenantChannel` — one statement moves the noisy tenant's 50 due deliveries; the next
claim goes to the quiet tenant; the tenant's other channel and a delivery under a live lease are left
alone.

## D25 — Addresses are plural, and each is its own delivery · *review*

**Decision.** `AddressBook.Resolve` returns `[]Address`. On first claim the dispatcher binds the
delivery (`Queue.BindAddresses`): one address binds in place; several replace the row with one row
per address, each with its own attempts, backoff and outcome. A channel dedupes on
`Delivery.IdemKey = hex(sha256(canonical(realm, tenant, recipient, idem_key | "digest:"+id, channel, address_key)))`,
which is stable across re-drives and distinct per address.

**Alternative.** The first version resolved one `Address` per channel and dedup'd on the notification's
`IdemKey`.

**What goes wrong without it.** A user with a phone and a tablet can be pushed to one of them. Worse, if
the port is widened without per-address state, a failure on one device re-sends to every device on
retry. Deduping on the notification's key made every address and every digest collide at the provider,
and made the key identical across realms. Binding the address at first claim also keeps the key stable
if the recipient changes their address between a send and its re-drive.

Verified: `TestBindAddresses` — three devices bind as three unleased rows, each then claimed on its
own; binding is fenced and happens once; `TestDerivedKeysSeparate` — two devices never share a key.

## D26 — The inbox is a capability, with three states · *review*

**Decision.** A channel whose `Capabilities().InboxBacked` is true makes a notification visible in the
durable inbox; `notifications.inbox_visible` records whether any such channel was enabled at notify
time. The reference in-app channel is inbox-backed. The inbox has three independent states — `seen_at`,
`read_at`, `archived_at` — plus `canceled_at`.

**Alternative.** The first version said disabling `in_app` hides a notification from the inbox and badge
while the row persists, but had no column to record it, and only a `read_at`.

**What goes wrong without it.** The badge counts rows. With no state for "hidden", the only ways to
honor the preference were to read preferences at query time — which rewrites history whenever a
preference changes — or to ignore it. Branching on the channel kind `in_app` would have broken the rule
that the engine never special-cases a channel. Seen versus read versus archived is the minimum every
inbox product ships (a bell that clears when opened, an item marked read, an item put away); inferring
one from another is a bug report waiting to be filed.

## D27 — Templates own copy; producers send data · *review*

**Decision.** A notification carries `Data` — typed template variables — and optional fallback `Title`
and `Body`. The `TemplateRenderer` renders every channel from `(topic, template_ref, channel, locale,
tenant, data)`. The in-app inbox renders each item at **read** time in the viewer's locale, falling
back to the stored title and body. A missing template with no fallback is a terminal `rejected`.

**Alternative.** The first version required producers to pass `Title` and `Body` strings, stored them on
the inbox row, and also declared that copy lived only in the template seam (NR-019).

**What goes wrong without it.** Both could not be true. Producer-supplied strings are in one language,
fixed at the moment of sending, so the inbox could never be localized and a template change could never
reach it — while `Payload`, the only structured field, was reserved for deep links and typed
`map[string]string`, too weak for an amount, a date or a list of digest items.

## D28 — `NotifyTx` enqueues inside the host's transaction · *review*

**Decision.** `Notifier.NotifyTx(ctx, tx, n)` persists and enqueues inside a transaction the host owns
and commits. The PostgreSQL store accepts a `pgx.Tx`; the gRPC client returns `ErrTxUnsupported`. The
engine never commits or rolls back the host's transaction, sets its scope transaction-locally, reads the
pre-check but never writes it, and relies on polling rather than an in-memory wakeup.

**Alternative.** The first version offered only `Notify`, which opens its own transaction.

**What goes wrong without it.** The host has the same dual-write problem the engine fixed internally: it
commits "invoice overdue", crashes before `Notify`, and the dunning notice is lost — or it calls
`Notify` first and rolls back, and notifies about an invoice that is not overdue. Every mature
PostgreSQL-backed queue (River, Oban, Graphile Worker) offers a transactional insert for this reason; a
PostgreSQL-backed notification library without one hands its most important guarantee back to the
caller.

The price is exposure to the host's transaction behaviour: see [D38](#d38).

## D29 — Delivery lifecycle controls · *review*

**Decision.** Five controls, each mapped to schema:

- `Notification.DeliverAfter` schedules a send (the queue row is due then; `visible_from` keeps it out
  of the inbox until then).
- `Notification.DeliverBefore` expires it: past that instant it is terminal `expired`, never sent late.
- `Notifier.Cancel` withdraws a notification: pending deliveries become `canceled`; a delivery under a
  live lease is reported as in-flight, never falsely recorded as canceled.
- `TopicDef.Fallback` is an ordered channel chain tried one at a time while each ends undelivered.
  Children of a multi-address fan-out carry no fallback.
- Provider failover is a composite `Channel` over the ordered `channel_providers`, not an engine
  feature.

**Alternative.** None of these existed.

**What goes wrong without it.** A "meeting starts in 5 minutes" push that spent two hours in retries is
worse than no push. A "your cart is waiting" email after the purchase completed is a support ticket.
"Push, and if that fails, SMS" is how critical notifications are routed in practice, and without a chain
every host builds it by watching outcomes and calling `Notify` again, with its own idempotency bugs.
Keeping provider failover inside a channel keeps the core channel-agnostic.

Verified: `TestFinish` (fallback enqueues on an undelivered outcome, never on expiry or cancel),
`TestCancel` (pending deliveries stop; an in-flight one is left to its worker and reported).

## D30 — Preferences resolve recipient, then tenant, then topic · *review*

**Decision.** A recipient's row wins, unless the tenant's row for that `(topic, channel)` is `locked`;
then the tenant's row; then the topic's registered default. Essential topics cannot be disabled at any
level. `GET /notifications/preferences` reports which level each value came from.

**Alternative.** The first version had recipient rows over topic defaults only.

**What goes wrong without it.** A multi-tenant product's customer administrators need "nobody in our
organization gets marketing SMS" and "everyone gets security email" without editing every member's
preferences — and a member who joins next week must inherit it.

## D31 — Service mode is real: data-backed ports, a directory callback, and derived identity · *review*

**Decision.** The service ships data-backed implementations of the ports a host would otherwise supply:
`TopicRegistry` over `notification_topics`, `TemplateRenderer` over `notification_templates`,
`AddressBook` over `recipient_addresses`, and channels configured from `channel_providers`. Producers
manage them through a management API. Audiences come from a bounded inline recipient list or a
`Directory.ResolveAudience` callback the host serves. The realm is derived from the authenticated
producer principal through `RealmBindings`, never taken from the request. The recipient REST surface
authenticates a short-lived recipient token signed by the host.

**Alternative.** The first version described a standalone container behind the same ports — `Channel`,
`TemplateRenderer`, `AddressBook`, `AudienceResolver`, `TopicRegistry` — every one of which was Go code
compiled into the process.

**What goes wrong without it.** A container built from this repository knows none of a host's topics,
templates, addresses or members, so "run it as a service" and "any language" were not achievable. And
with one `Config.Realm` per deployment, [D1](#d1)'s shared-deployment scenario was impossible in the very
mode meant for sharing. Library mode is unchanged: a Go host may use these implementations or its own.

## D32 — Delivery guarantees are declared per channel, not claimed globally · *review*

**Decision.** Each channel declares `Capabilities().Dedup`: `DedupProvider` (the provider honors an
idempotency key, with its window), `DedupLocal` (the channel records sends itself), or `DedupNone`.
NS-002 is stated per level. The engine logs and exports each registered channel's level;
`ChannelContract` asserts re-drive collapses for `DedupProvider` and `DedupLocal` and, with the
`provider` build tag, runs against the provider's sandbox rather than a fake.

**Alternative.** The first version promised "at most one delivery per channel, in 100% of duplicate
cases" and delegated it to channel authors.

**What goes wrong without it.** Most providers accept no idempotency key — SES, SMTP, Twilio, APNs, FCM,
Slack webhooks — and Resend's keys expire after 24 hours, so a dead-letter replay a day later sends
again. The 100% claim was unachievable, and the test that "verified" it ran against a fake that counted
its own calls. A stated, per-channel guarantee is one an operator can reason about; an unconditional one
is a guarantee that fails silently.

## D33 — Retention retires by age, and says so · *review*

**Decision.** `RetentionWindow` retires inbox partitions whole — read and unread alike. Each other
growing table has its own bound: `notify_idem` (the idempotency window), `notification_deliveries`
(`DeliveryLogRetention`, default 30 days), `dead_letters` (`DeadLetterRetention`), `digest_buffer`
(flushed and finished), `notification_broadcasts` (completed, after the idempotency window). The queue
is bounded by construction ([D20](#d20)).

**Alternative.** The first version said read notifications are retired, while specifying a partition
`DROP` as the mechanism.

**What goes wrong without it.** A partition drop cannot tell read from unread. The specification
promised one behavior and the schema implemented another; the first host to notice would have been one
whose users lost unread notifications they were told would be kept.

## D34 — One-click unsubscribe follows RFC 8058 · *review*

**Decision.** Email carries `List-Unsubscribe` and `List-Unsubscribe-Post: List-Unsubscribe=One-Click`.
`POST /notifications/unsubscribe` performs the change; `GET` renders a confirmation page and changes
nothing. The token is signed with a key id for rotation, scoped to one
`(realm, tenant, recipient, topic, channel)`, and valid for at least 60 days.

**Alternative.** The first version: `GET /notifications/unsubscribe?token=` disabled the preference
directly.

**What goes wrong without it.** Mail security scanners and link previewers fetch every URL in a message.
A state-changing `GET` unsubscribes recipients who never clicked anything. Gmail and Yahoo require RFC
8058 one-click unsubscribe for bulk senders, and a link that stops working after a few days violates
CAN-SPAM's 30-day requirement.

*Built.* The token is `v1.<key id>.<base64url payload>.<base64url HMAC-SHA256>`. The MAC is over
`"notify.unsubscribe.v1\n" + key id + "\n" + payload`, so the id is under the signature and a MAC the same
key makes for another purpose cannot pass; it is compared in constant time and checked before the payload
is parsed. The payload is the five identity components, the topic, the channel and the second the token
lapses, as compact JSON. A signer signs with one key and verifies with every key it holds, so a key rotates
by adding the new one and making it current, and retires when the mail signed with the old one has outlived
its life. A life under 60 days, or a key under 32 bytes, is refused when the signer is made. An invalid or
altered token and an expired one are different errors, and the REST surface answers both with a 410. The
link base must be HTTPS, because RFC 8058 wants one HTTPS URI and a mail scanner follows what it is given;
plain HTTP is an explicit opt-in for development.

Verified: `TestUnsubscribeTokenMatchesTheFrozenVector`, `TestUnsubscribeTokenRefusesAnyAlteredField`,
`TestUnsubscribeTokenKeys`, `TestUnsubscribeTokenExpiry`, `TestUnsubscribeTokenRefusesWhatIsNotAToken`,
`TestSignedLinksRefuseABaseThatCannotWork`. That the comparison is constant-time is held by reading the
source (`TestUnsubscribeVerifyComparesTheMACInConstantTime`): the timing cannot be measured in a unit test,
but the primitive can be pinned.

## D35 — Personal data is erasable, and suppressions survive erasure · *review*

**Decision.** `Maintenance.Erase` removes a recipient's rows from every table that holds them —
inbox, guard, queue, delivery log, dead letters, digests, preferences, schedules, addresses — in one
transaction under the recipient's own scope, and records the erasure in `erasure_requests` by the hash
of the identity. `channel_suppressions` is keyed by `address_hash`, never the address, and is kept. Redis
holds only hashed keys, and the pre-check's entries for the recipient's keys are forgotten with the rest
([D51](#d51)).

**Alternative.** The first version had no erasure path, kept dead-letter payloads with content, and
stored suppressed addresses in plaintext indefinitely.

**What goes wrong without it.** A GDPR erasure request had no answer short of hand-written SQL across ten
tables, some partitioned. Deleting a person who complained would also delete their suppression and
resume mailing them — the one outcome worse than not erasing. A hash is minimization, not anonymity, and
is retained on the legitimate basis of honoring the objection.

Verified: `TestErase` — no table that can hold a recipient, read from the catalog rather than from
the code's own list, keeps a row of u9; u1 is untouched; the suppression survives; the erasure is
recorded by hash. TEST 11 — suppressions and erasure records hold hashes, not identities.

## D36 — The capacity envelope is stated, and so is the next step · *review*

**Decision.** [plan.md § Capacity model](plan.md#capacity-model) states the design envelope for one
PostgreSQL primary as targets, and NS-010 gates release on a load test that meets them. It also names the
scale-out path past one primary: every table is keyed by realm and tenant, so the `Store` can route by
tenant to separate databases, or distribute on `(realm, tenant_id)`, without a schema change.

**Alternative.** The first version called the engine highly scalable without a number or a test.

**What goes wrong without it.** An unfalsifiable scalability claim is adopted by a team whose load is ten
times what the design can carry, and discovered in production. Stated targets make the claim checkable
and the gap to the next step visible.


## D37 — State transitions are SQL in the Go adapter; policy is in the domain · *review*

**Decision.** Every queue state transition — the claim, its fence, retry, defer, the terminal move,
address binding, tenant deferral, digest append and flush, cancel, expiry, erasure, partition
provisioning — is plain SQL in [`adapters/driven/postgres`](../../adapters/driven/postgres/), executed
as one statement (data-modifying CTEs where several writes must be atomic) or inside one transaction.
What a terminal outcome *means* — whether it is a dead letter, whether the fallback chain continues — is
pure Go in `domain.DispositionOf`, and the store executes whatever a `domain.Finish` says. The SQL is
tested against a real PostgreSQL 16 by an integration suite that runs in CI as the non-superuser table
owner, in parallel, one cloned database per test. The only function left in the schema is
`notify_apply_recipient_scope`, a DDL helper that keeps the row-level security predicate defined once.

**Alternative.** The first version of the review put these transitions in the schema as PL/pgSQL
functions (`0004_state_transitions.sql`), including the dead-letter and fallback policy.

**What goes wrong without it.** Three things:

- **Policy buried in the database.** Which failures page an operator and when a fallback channel is
  tried are domain rules. In PL/pgSQL they sat outside `app/` and `domain/`, which the hexagon says own
  the invariants, where Go tooling cannot read, debug, profile or trace them.
- **Deploy coupling.** Changing the claim or the policy was a migration, not a code deploy, and during a
  rolling deploy two binaries had to agree on whichever function version was live.
- **A reference implementation other stores could not read.** A second `Store` adapter still has to
  reimplement the semantics, but now against readable Go and SQL strings and a named test suite, not a
  PL/pgSQL body.

The functions had one real advantage: they were testable before any Go existed. That is why this
decision came with the Go that holds the SQL and the integration suite that proves it — including real
concurrency (two sessions claiming one shard) and 20 mutation checks, each of which removes one
guarantee (`SKIP LOCKED`, a fence, the attempt count, the disposition, a table from erasure, the scope on
a new partition, …) and fails a named test. Without the suite, moving SQL into application code would
only have moved it out of sight.

Verified: the integration suite (`make test-integration`); `TestFinish` "the store executes the
disposition it is given, and decides nothing"; `TestDispositionOf`.

## D38 — The queue shares its database's fate, so where it is deployed decides how exposed it is · *measured*

**Decision.** Service mode is the recommended shape once volume passes a few hundred notifications a
second: the engine owns its database, ideally its own cluster, so no transaction it did not start can pin
its vacuum, and its checkpoint and WAL settings are its own. Library mode stays supported, because
`NotifyTx` ([D28](#d28)) is the guarantee it exists to give, but it shares the host's database and
therefore the host's transactions. In library mode:

- the host's role sets `idle_in_transaction_session_timeout` and `statement_timeout`;
- `NotifyTx` runs in short transactions — no provider or network call, and no waiting on a user, inside one;
- long reads and reports run on a replica, not on the primary the queue lives on;
- the engine reports what it can see (NR-037), as warnings and never as failures: the age of the oldest
  open snapshot, the connected role's timeouts, and checkpoint sizing against the observed WAL rate.

The queue's mechanism does not change. Revisit a vacuum-free queue — rotating tables emptied by
truncation — if a rate ramp shows vacuum cannot keep up at the target rate, or if long transactions
prove uncontrollable in a deployment that cannot move to service mode.

**Alternative.** Three, none adopted:

- **Service only at scale.** Cleaner, but it hands D28's guarantee back to the hosts that would most
  want it.
- **A separate database for the library's queue.** Impossible with `NotifyTx`: one transaction cannot
  span two databases.
- **Say nothing**, as the first version did. The exposure exists in library mode whether or not it is
  written down.

**What goes wrong without it.** Vacuum cannot reclaim a row version that an open snapshot might still
see, and in library mode that snapshot belongs to host code the engine neither sees nor controls: a
report, an admin session, a forgotten transaction. In `make soak`, one held open for two minutes at
1,000 notifications/s took the outbox heap from about 40–50 MB to 180–230 MB, cut HOT updates from 67% to
as low as 18%, and was followed by a latency incident (claim p95 up to 364 ms, end-to-end p95 up to 31 s)
that took about 90 seconds after the release to clear. Nothing was lost. Bloat scales with the rate times
how long the snapshot is held, so the same hold costs less at a lower rate; the soak measured only
1,000/s, so "a few hundred" is a judgment, not a measurement.

Two limits on the mitigation. On PostgreSQL 16 neither timeout ends a transaction that keeps issuing
short statements for minutes; `transaction_timeout`, added in PostgreSQL 17, does. And the soak did not
run with the timeouts: its transaction sat idle in transaction, which is what
`idle_in_transaction_session_timeout` ends, so that they would have contained it is expected, not verified.

Measured: `make soak`, two runs, recorded in
[operations.md](../../docs/operations.md#protecting-the-queue-from-its-own-database). Not yet verified:
the timeouts containing it, and the threshold.

## D39 — An inbox-backed channel is never digested · *built*

**Decision.** The planner folds a channel into a digest window only when the channel does not declare
`InboxBacked`. An inbox-backed channel is planned as an ordinary delivery whatever the recipient's cadence,
and a plan names a window only when some channel uses it. "Digestible channel" in NR-014 and NS-005 means a
channel without that capability, read from the registry and never from the channel's name (NR-016).

**Alternative.** Three, none adopted:

- **Digest every channel, and nudge the newest member at the flush.** Keeps NS-005 literal, but a digest
  delivery names no notification: `Store.Load` reads the members' ids and discards them, and
  `domain.Notification` has none, so the ids would have to be carried through the `Store` port and a type
  the contract fixes.
- **Digest every channel, and let the in-app channel flush without a nudge.** What the planner did first,
  because the contract was silent.
- **A cadence per channel.** The right shape if recipients ask for it, but a change to the preference model
  (D3, D30), not a way out of this one.

**What goes wrong without it.** `DeliverySchedule.Digest` belongs to the recipient, not to a channel. A
recipient who asks for an hourly email digest also has every in-app delivery folded into a window, so the
live nudge is held back for the same hour, though nothing about a nudge on a row the inbox already holds
interrupts anyone. At the flush the in-app channel is handed a digest, which names no notification, so there
is nothing to point a nudge at: the live path is silent for as long as the cadence is set, and the bell
corrects itself only on the next reconnect. The digest exists to cut interruptions; the inbox is the one
channel that is not one.

*Cost.* A burst of N notifications to one recipient is N nudges on the live stream, where a digest would have
made one. A nudge is one `SPUBLISH` and at-most-once ([D15](#d15), [D16](#d16)), and every inbox row is
persisted either way, so what this leaves open is the relay coalescing a burst of nudges for a connected
client, which is the relay's (T044).

Verified: `TestPlan` — an inbox-backed channel is not digested while another is; which channel is
inbox-backed is its capability (a `feed` channel is not digested, a channel named `in_app` that is not
inbox-backed is); a cadence with only an inbox-backed channel enabled opens no window.

## D40 — A port names a fault no retry fixes, and the channel reads it · *built*

**Decision.** A driven port that can tell a fault no retry will fix from one that may pass wraps
`domain.ErrPermanent`; any other error from it is transient. A channel reads it: a permanent fault is
`Rejected`, so the delivery is dead-lettered at once with its reason, and any other is `Retry`. The Redis
stream wraps it for an ACL that refuses the command, a server without it (sharded pub/sub arrived in Redis
7.0), and a nudge with no notification id or an identity with an empty component. It reads only a Redis
*server's* own error text, never a transport error's. `ChannelContract` requires every channel to report a
permanent refusal as `Rejected` with a `Detail`.

**Alternative.** Every error from a port is `Retry`, as the contract's sketch of the in-app channel has it, or
a channel matches error text.

**What goes wrong without it.** A Redis that will never accept the command, because an ACL forbids it or the
version lacks it, is retried to the attempt ceiling with full-jitter backoff up to an hour
([D10](#d10)): hours before an operator learns why, and a dead letter that says `max_attempts` where the
cause is a configuration. And the contract's own case, "a permanent refusal is `Rejected`", cannot be asked
of a channel whose provider has no way to say so. Matching error text in each channel puts a Redis
dependency in every channel that publishes.

Verified: `TestClassifyPublishFaults`, `TestPublishClassifiesWhatARetryCannotFix` (an ACL refusal against a
real Redis), `TestInAppReadsTheStreamsFault`, and the in-app suite's run of `ChannelContract`.

## D41 — `Rejected` is a verdict on the message; a provider's own trouble is `Retry` · *built*

**Decision.** `Rejected` means the message itself will be refused: malformed, too large, content refused.
Anything that is the provider's own, a revoked or suspended key, an unverified sending domain, a refused
sender or credentials, an exhausted plan, is `Retry`, and after `MaxAttempts` it is dead-lettered as
`max_attempts` with the last error. `Suppressed` is a verdict on the address. The failover channel
([D29](#d29)) ends its chain on a delivery, a `Suppressed` or a `Rejected`, and tries the next provider on a
`Retry` or an error. The SMTP and Resend mailers classify this way: 401, 403 and 404 from Resend, and an
SMTP refusal at `AUTH` or `MAIL`, are transient.

**Alternative.** Provider trouble as `Rejected`, which is how the status code reads: a 403 is permanent for
that request. Or a failover chain that also advances on `Rejected`.

**What goes wrong without it.** With provider trouble reported as `Rejected`, a revoked API key on the
primary provider dead-letters every email permanently, one at a time, while a healthy standby sits idle: a
failover chain can only route around what is reported as transient. With a chain that also advanced on
`Rejected`, a malformed message is sent to every provider in turn, and a more lenient one delivering what the
first refused hides the defect in the message.

Verified: `TestSMTPClassifiesTheRelaysAnswers`, `TestResendClassifiesTheAnswers`,
`TestCompositeWalksTheChain`.

## D42 — The dispatcher's failure policy: what it owns, and what it does not · *built*

**Decision.** Six choices the contract's nine steps leave open, each made so that a failure ends in a
record and not in silence:

- **A claim beyond the ceiling is finished unsent.** A delivery claimed more than `MaxAttempts` times is
  finished as `max_attempts` at once, before any other work, without being handed to a channel. The sixth
  claim is not an attempt, so its dead letter reads `MaxAttempts + 1` claims counted and `MaxAttempts` sends
  made at most. The same ceiling applies to a failure the dispatcher records itself: a retry at attempt
  `MaxAttempts` is parked.
- **Anything the engine does not own that faults is a retry.** The address book, the suppression store, the
  renderer, the topic registry, the store's `Load`, a channel that returns an error or an outcome it does not
  have, a channel that is not registered on this worker, and the write of a suppression: each is `Queue.Retry`
  with the error as the last error. A panic in a channel, a renderer or an address book is turned into the same
  retry, with its stack sent to the error handler. The one exception is the quota counter, which fails open
  ([D5](#d5)): a budget that cannot be read is a budget with units left.
- **An outcome is recorded on a context that outlives the worker.** Every write that records an outcome uses
  `context.WithoutCancel` with a timeout of its own.
- **A channel-reported suppression is written before the delivery is finished.**
- **A bound address is checked against the suppressions at every claim**, not only when it is first
  resolved, so a bounce that lands while a delivery waits for its retry is honoured.
- **A claimed batch is handled concurrently**, one goroutine per claim, and a handler's panic is raised again
  in the caller's goroutine once the others have finished.

**Alternative.** The ceiling enforced only where a failure is recorded, which is how the contract's list of
steps reads. Faults in ports terminal (`no_address` for an address book that is down, `rejected` for a
renderer that errors). The outcome recorded on the worker's own context. The suppression written after the
finish, or by the finishing statement. A suppression checked when the address is bound and never again. A
batch handled one claim after another.

**What goes wrong without it.**

- A delivery that kills its worker every time never reaches the place where a failed one is counted. Attempts
  are counted at claim ([D19](#d19)) so the number is right, and a ceiling that is only looked at when a
  failure is recorded never fires: the poison is claimed, kills the worker, and lapses, for ever.
- A transient fault read as a verdict loses deliveries or lies in the record. An address book that is down
  would finish every email as `no_address`, correct to the letter and false to the recipient; a renderer that
  errors would dead-letter every delivery of the hour. A retry costs one attempt and a backoff, and the
  ceiling still parks what never recovers. A channel panic that took the worker down would take the rest of
  its batch with it, each left leased until it lapsed.
- Shutdown arrives after a message was sent and before it is recorded, and the next worker sends it again, on
  a channel that cannot tell ([D32](#d32)). Recording is the one write that must not be cancelled with the
  worker.
- Finished first, a worker that dies before the suppression is written leaves a delivery that is over and a
  dead address nobody knows is dead; the next notification mails it again, after a hard bounce or a
  complaint. Written first, the delivery is still pending, and the next claim finds it suppressed.
- A suppression that lands between a fan-out and a retry is ignored by a retry that sends to the address it was
  bound to.
- The lease of a batch runs from the claim. Handled one by one, 100 sends of a few seconds outlive a
  `ClaimLease` of five minutes although no single send does, and the later ones are re-driven by another
  worker while this one is still going to send them. `ClaimLease` is documented as exceeding the slowest
  send, not the slowest batch.

Verified: `DispatcherContract` — "a delivery that crashes its worker every time terminates unsent at the
ceiling", "a channel that panics is a retry", "a worker that dies right after a Suppressed ending has already
recorded the suppression", "a suppression written after the address was bound is still honoured", "a batch is
sent concurrently"; `TestAFaultingPortIsARetryAndNeverALostDelivery` (nine ports, one row each, and the quota
counter that fails open); `TestRunDrainsEveryShardAndRecordsWhatWasInFlightAtShutdown`. Mutation-checked: each
choice has rows in the [ledger](../../docs/mutations.md).

## D43 — A dead letter is replayed once, as the same delivery · *built*

**Decision.** `Maintenance.ReplayDeadLetter` marks the dead letter replayed and enqueues a fresh delivery from
the queue row it kept, in one statement, with no `ON CONFLICT`. The fresh delivery starts over (no attempts, no
lease, due now) and is the same delivery: it keeps its notification or digest, its bound address (so a channel
dedupes it on the key it had), and its deadline. It carries no fallback chain. A dead letter of another realm,
and one that does not exist, are the same `ErrUnknownDeadLetter`; one parked by the bus has no queue row and
is `ErrNotReplayable`. A second call is a no-op, not an error.

**Alternative.** A replay that enqueues the delivery as new, with the chain and without the deadline. A
replay in two statements, or one that skips a conflict.

**What goes wrong without it.** The fallback chain ran when the delivery was parked ([D29](#d29)); running it
again sends on the fallback channel twice. A message that expired while it sat in the dead letters, a "meeting
in 5 minutes" push, is sent late by the operator who meant to fix a bug. A replay in two statements, or one
that skips a conflict, can mark the dead letter replayed when nothing was enqueued, and the failure is then no
longer in the list of what an operator must look at. Two operators, or one script that retried, enqueue the
same delivery twice. And an id that tells "another realm's" from "does not exist" lets one product probe
another's dead letters.

Verified: `TestReplayDeadLetterEnqueuesOneFreshDelivery`, `TestReplayDeadLetterTwiceEnqueuesOnce`,
`TestReplayDeadLetterConcurrentlyEnqueuesOnce` (eight replayers, one delivery),
`TestReplayDeadLetterThatConflictsLeavesTheDeadLetterUnreplayed`, `TestReplayDeadLetterRefusals`,
`TestAReplayedDeliveryThatHasExpiredIsNotSentLate`.

## D44 — A channel that calls a tenant's URL treats the URL as hostile · *built*

**Decision.** The webhook channel ([T033a](tasks.md)) takes its address from a tenant, so it checks it: https
only; no credentials in it; never a connection to a loopback, private, link-local, carrier-grade-NAT or
reserved address, checked on the address the connection is made to, after the name is resolved, with an IPv4
address written as IPv6 judged as the IPv4 it is; no redirect followed; no proxy from the environment. A URL
it must not call is a dead address (`Suppressed`, `invalid_address`), not a fault. What it reports of a failure
names the host and never the URL. A body it quotes is short and free of control characters. Each request is
signed (`X-Notify-Signature`, an HMAC-SHA256 over the timestamp and the exact body) and carries the delivery
key as `Idempotency-Key`; a channel with no secret is refused. The two opt-ins that relax the checks
(`WithAllowHTTP`, `WithAllowPrivateNetworks`) are named for what they do and are for receivers inside the
same trust boundary.

**Alternative.** Post to whatever URL the address book holds, as the carried design's webhook channel did, and
report the transport's error as it comes.

**What goes wrong without it.** A tenant's webhook address is the engine's own network position, lent to the
tenant: `http://169.254.169.254/latest/meta-data/` returns the host's cloud credentials, and a name that
resolves to an internal address does the same through a check made on the name. A redirect from a public
endpoint leads anywhere; `HTTPS_PROXY` in the environment makes the connection go to the proxy, whose address
the check then judges instead of the destination's. `net/http`'s error carries the URL, and a URL often holds
a token, which then sits in `last_error` of a dead letter every operator reads. Unsigned requests cannot be
told from forged ones.

Verified: `TestWebhookRefusesAddressesItMustNotCall`, `TestBlockedDestinations`, `TestTheTransportUsesNoProxy`,
`TestWebhookDoesNotFollowRedirects`, `TestWebhookDetailNeverCarriesTheURLsSecrets`,
`TestSignAgainstAnIndependentVector`.

## D45 — A budget is two fixed windows, and its ceilings are a table the counter reads · *built*

**Decision.** A `(realm, tenant, channel)` budget is counted in two **fixed windows aligned to UTC**: the
hour and the day. A take spends one unit in both or in neither, in one Redis script, so concurrent takers
cannot spend what is not there and a refused take counts nowhere. A refusal reports when units return: the
**end of the longest window that is spent**, since a delivery deferred to the end of an hour whose day is
spent would find the day still spent and be deferred again. The window's start is in the key
(`domain.QuotaCounterKey`, with a hash tag over the budget so its two counters share a slot), so a window
that has ended is a key nobody reads again, and expires by itself; **there is no quota rollover job**.

The ceilings are not the counter's. `ports.QuotaLimits` reads `channel_quotas.max_per_hour` and
`max_per_day`, with a tenant's own row overriding the realm-wide default, from the same row as
`on_exhausted`, so a tenant is never judged by its own ceiling and the realm's policy. The counter
remembers what it read for 30 seconds, because a take is on the path of every delivery; it goes on with the
last ceilings it read when the table is away; and a budget with no row at all is **unlimited and keeps no
count**. A row of zero allows none: it is a row, not the absence of one.

Quota fails open and correctness does not. Redis losing its state is a budget with all its units back and no
error. A Redis that cannot be reached allows the delivery and returns the fault, so a caller that ignores it
still sends and one that reads it can say so. The one place that is held is `app/quota.go`: the planner
peeks and the dispatcher takes through the same gate, which answers a failure the same way for both.

**Alternative.** A sliding window, or a token bucket. Or a counter that reads the table on every take.

**What goes wrong without it.** `Queue.Defer` and `DeferTenantChannel` move a delivery to an instant
([D24](#d24)). A sliding window has no such instant until the oldest unit ages out, and every claimant would
compute a different one, so the single statement that moves a tenant's backlog out of the claim range has
nothing to move it to. Resetting at the end of the hour while the day is spent defers the same deliveries
every hour until midnight. A refusal that counted would spend the day on deliveries that were never made. A
check and a spend in two steps lets 80 concurrent takers take 27 of 20 units (the mutation that does it is in
the ledger). A counter that read the table per take puts a query in front of every send. And a ceiling of zero
read as "no limit" would send for a tenant an operator meant to silence.

The price of a fixed window is that a tenant may spend two hours' budget across a boundary. The budget is a
bound on a tenant's share of the drainers and of a provider's allowance, not a rate limit on the provider.

Verified: `QuotaContract` (run by the in-memory fake and by the Redis counter), `TestTheHourlyBudgetReturnsAtTheNextHour`,
`TestTheDailyBudgetBindsAndResetsAtMidnight`, `TestARefusedTakeSpendsNeitherWindow`,
`TestCeilingsAreReadOncePerTTL`, `TestAnUnreadableTableKeepsTheLastCeilingsAndOtherwiseFailsOpen`,
`TestAnUnreachableRedisFailsOpenAndSaysSo`, `TestQuotasReadTheRealmDefaultUntilATenantOverridesIt`,
`TestQuotaWindows`, `TestQuotaLimitsStatus`, the frozen vectors of `QuotaCounterKey`, and, against the real counter
and the real table, `TestARealBudgetIsPeekedAtEnqueueAndTakenAtDispatch`. NS-007 itself is
`TestAnExhaustedNeighbourDoesNotSlowAQuietTenant`: a neighbour exhausted with a backlog twelve rows deep on all 16
shards leaves the quiet tenant's p95 at one pass, and with the backlog move removed it is four.

## D46 — The digest flush job keeps no memory of what it meant to flush · *built*

**Decision.** `app.DigestFlusher` finds the windows whose time has passed and flushes them, and nothing
else: it keeps no cursor, no last-run time and no list. It reads the due windows of every shard in batches
under the `digest_flush` lease, **taking or renewing the lease before every batch** and not once per pass.
A window that will not flush, or a shard that cannot be read, is reported and does not hold the others back;
a batch in which nothing could be flushed ends the pass and is not read again. The interval is an option of
the job (5 seconds), not a `Config` field.

**Alternative.** A job that flushes the windows that came due since it last ran, or that holds the lease once
for the whole pass.

**What goes wrong without it.** A window that comes due while no one holds the job, because the worker that did
died, is overdue; a job that asks for "the windows due since my last run" never sees it, and its members are
held and never delivered, which is the storm the window exists to prevent (invariant 8) turned into silence. A
lease taken once per pass lapses under a backlog deeper than its ttl, and a second worker reads the same
windows. A fault that repeats on one window, retried by a loop that treats a full batch as a reason to read
again, spins. The flush itself is idempotent in the store ([D4](#d4)), so a lease lost mid-batch costs a window
flushed twice, which is flushed once; the lease is what makes the job single-owner, not what makes it correct.

Verified: `TestAWindowOverdueAfterACrashIsFlushedByTheNextRun`, `TestAFlusherWithoutTheLeaseFlushesNothing`,
`TestTheFlushJobRenewsItsLeaseBetweenShardsAndStopsWhenItLosesIt`, `TestAPassThatCanFlushNothingDoesNotSpin`,
`TestAWindowThatFailsToFlushDoesNotHoldBackTheOthers`, `TestAPassFlushesEveryShardAndEveryBatch`.

## D47 — Retirement drops whole partitions, waits briefly for the lock, and never retires a range that has not ended · *built*

**Decision.** `Maintenance.RetirePartitions(table, before)` drops each partition of the inbox, the delivery
history or the dead letters whose **whole range ended at or before `before`**, oldest first. A partition's age is
its upper bound, so a notification outlives `RetentionWindow` by up to a month, read or unread: this is
[D33](#d33)'s "says so" made exact. Three guards:

- **`before` is capped at the database's `now()`.** A cutoff in the future cannot drop a partition whose range
  has not ended, whatever the caller computed.
- **Each drop is its own transaction under a `lock_timeout`** (5 seconds, `postgres.WithRetireLockTimeout`). A
  partition it cannot lock in that time is skipped, named in the error, and found again by the next call; the
  names that were retired are returned beside it.
- **Only the three range-partitioned tables**, and only direct partitions with a bounded range. Any other name
  is refused before anything is dropped, and a default partition is never a candidate.

**Alternative.** Three, none adopted:

- **Delete the aged rows, then drop the empty partition.** It is the mass purge NR-022 rules out: the deletes
  are WAL and dead tuples for vacuum to clear, which is the load the partitioning exists to avoid.
- **`DROP TABLE` with no timeout.** What a plain `DROP` does when it cannot get the lock is wait for it.
- **`DETACH PARTITION … CONCURRENTLY`.** It avoids the exclusive lock on the parent, but cannot run inside a
  transaction and, if interrupted, leaves the partition half-detached for `FINALIZE`. Revisit it if drops are
  seen waiting.

**What goes wrong without it.** Dropping a partition asks for `ACCESS EXCLUSIVE` on the table. A request for it
queues behind any transaction that holds a lock on the table, a report or a forgotten session, and every
reader and writer of the inbox that arrives after the request queues behind *it*. A maintenance job that runs
hourly in the background becomes an outage whose cause is three layers from its symptom, which is the same
shape of failure as [D6](#d6). A cutoff computed in the wrong unit, or from a clock that is ahead, drops the
months the inbox is serving, and a dropped partition does not come back.

**Cost.** For up to the timeout the request for the lock is itself in the queue, so a drop that has to wait
holds up the table for that long, bounded and not unbounded. Nothing is retired while a long transaction holds
the partition; the next call, an hour later by default, tries again.

Verified: `TestRetirePartitionsDropsWholePartitionsAndDeletesNoRow`,
`TestRetirePartitionsRetiresOnlyWhatTheCutoffHasWhollyPassed`,
`TestRetirePartitionsNeverRetiresAPartitionWhoseRangeHasNotEnded`,
`TestRetirePartitionsRefusesATableItDoesNotRetire`,
`TestRetirementWaitsOnlyBrieflyForALockAndSkipsThePartitionItCannotLock`,
`TestRetentionSucceedsWhileADeadLetterReferencesAnAgedPartition` and
`TestRetentionLeavesAPendingDeliveryDrivable`, in `adapters/driven/postgres`.

## D48 — The maintenance jobs hold their lease for the gap between their runs · *built*

**Decision.** `app.Maintenance` runs six jobs (`partition_provision`, `retention`, `idem_expiry`,
`digest_expiry`, `broadcast_cleanup`, `preflight`), each under its own `notify_job_leases` row. The lease is
taken before the job works, and **before every batch** of one that works in batches. Its ttl is **twice the
job's interval**, so it outlives the gap between runs: the worker that holds it takes it again at each run and
keeps the job, and every other worker is kept out until it lapses. A job runs no oftener than its interval
counted from *this worker's* last run, and a lease that was refused is not a run. `Run` makes a pass at once, so a
fresh deployment has its months before it takes traffic, and then one every tick. Provisioning is followed by the
check **whether or not it succeeded**. The alarms are counters raised at each check.

**Alternative.** Three, none adopted:

- **A lease held only while the job runs** (ttl a few seconds). Between runs it lapses, so every worker takes
  its turn at every interval: N times the work, and N times the alarm for one gap.
- **A cadence recorded in the database**, so the whole fleet runs a job once per interval. `TryLeaseJob`
  records `last_started_at` at every grant ([D50](#d50)), and a renewal before each batch is one, so it cannot say
  when a *run* began; a cadence would need a second method and a second race for the same single-owner guarantee
  the lease already gives.
- **One lease for a whole pass.** A pass with a deep backlog outlives any ttl worth choosing, and a second worker
  runs the same batch.

**What goes wrong without it.** A lease that lapses between runs runs each job once per worker per interval and
pages the operator once per worker for one missing partition. A check that is skipped when provisioning failed
is silent in exactly the case it exists for: the role cannot create tables, the month is missing, and the alarm
says nothing. A job that keeps a memory of what it meant to do, instead of deriving its cutoffs from the clock
at each run, skips the work a dead worker left ([D46](#d46)'s failure, for retention).

**Cost.** A worker that dies leaves its jobs unrun for up to twice their interval, two hours for the hourly
ones. That is harmless for work measured in months, and the five-minute expiries and the one-minute preflight
are late by ten minutes and two.

Verified: `TestEveryJobTakesItsLeaseBeforeItWorks`, `TestAJobWhoseLeaseIsHeldElsewhereDoesNotRun`,
`TestALeaseOutlivesTheGapBetweenRuns`, `TestTwoWorkersContendingForTheJobsRunEachOnce`,
`TestALapsedLeasePassesTheJobsToAnotherWorker`, `TestAJobRunsNoOftenerThanItsInterval`,
`TestARefusedLeaseIsNotARun`, `TestAnExpiryRenewsItsLeaseBeforeEachBatchAndStopsWhenItLosesIt`,
`TestAProvisioningFailureIsReportedAndTheCheckStillAlarms`, `TestRunPassesAtOnceAndThenEveryTick`, and end to
end `TestTwoWorkersOnOneDatabaseRunEachJobOnce`.

## D49 — The preflight reports what pins vacuum, and says when it cannot see · *built*

**Decision.** `Maintenance.Preflight` reads the database's conditions (`Maintenance.DatabaseConditions`) and
`domain.AssessDatabase` judges them, as warnings and never as errors (NR-037, [D38](#d38)):

- **The role's timeouts**: a warning for each of `idle_in_transaction_session_timeout` and `statement_timeout`
  that is off, read as the connected role has them (`pg_settings`, which shows the session's value after any
  role or database override).
- **A transaction that pins vacuum**, older than 60 seconds: one that holds a snapshot or has been given an id,
  in this database, on a client backend, not the preflight's own session. A read-committed session idle between
  statements holds neither, and is not reported.
- **A role that cannot see other roles' sessions** (neither a superuser nor a member of `pg_read_all_stats`) is
  itself a warning, because `xact_start` is hidden from it and the age it reads covers its own sessions only.
- **`max_wal_size` below fifteen minutes of the peak WAL rate** this process has seen: the greatest of the rates
  over each interval between readings, and for a first reading the average since the counter began. With no
  rate there is no judgement.

A reading that cannot be taken is a fault the loop reports, and never a reason to stop. The two gauges
(`notify.db.oldest_xmin_age` in seconds, `notify.db.checkpoints_requested`) are set through the optional
`ports.GaugeMetrics`, because a counter cannot hold a level.

**Alternative.** Report every open transaction, which cries wolf at every host that keeps a connection open a
moment; judge `max_wal_size` by the average since the server began counting, which hides a burst; or return an
error, which stops a service starting on a database it would run on.

**What goes wrong without it.** The soak that measured [D38](#d38) held one transaction for two minutes and
took the queue's heap from about 45 MB to 200 MB and its latency to 31 seconds. A preflight that reads through a
blind role reports a clean database it has not looked at, and that is worse than no preflight. A `max_wal_size`
sized to a quiet average stalls the queue the first time it is loaded.

**Cost.** The WAL peak is the process's: a restart forgets it, and the first reading after one is the average
since the server began counting, which understates a burst. The checkpoint counters read from
`pg_stat_checkpointer` on PostgreSQL 17 and from `pg_stat_bgwriter` before it, chosen by whether the view exists
and not by a version number. Each branch is held only by a run on its own major, so CI runs the schema and the
integration suites on 16 and on 17 (`make test-pg-next` is the second by hand), and `PGTEST_EXPECT_MAJOR` makes
a leg that did not start the major it names fail and not pass. The filter that leaves out vacuum's own workers
cannot be tested against a live autovacuum worker, which no test can make appear and stay, so the query is run,
unchanged, against rows made for it: a table named `pg_stat_activity` in a schema the test database's owner
searches before `pg_catalog`.

Verified: `TestAssessDatabase`, `TestAssessDatabaseWarningsNameTheirNumbers`, `TestWALRate` and its two siblings in
`domain`; `TestPreflightReportsEachConditionAsAWarningAndNeverAnError`,
`TestPreflightJudgesMaxWALSizeAgainstTheRateItHasSeen`, `TestPreflightSetsItsGauges`,
`TestPreflightThatCannotReadReturnsAFaultAndNoWarnings`, `TestRunReportsAFaultAndGoesOn` in `app`;
`TestDatabaseConditionsReadsTheRolesTimeoutsAndTheServersSizing`,
`TestDatabaseConditionsReportsTheAgeOfATransactionThatPinsVacuum`,
`TestDatabaseConditionsSaysWhetherTheRoleCanSeeOtherSessions`,
`TestDatabaseConditionsCountsOnlyClientBackends` and
`TestDatabaseConditionsCountsTheWALAndTheCheckpointsItsWritesCause` in `adapters/driven/postgres`, the last on both
majors; `TestImageFrom` and `TestCheckMajor` in `internal/pgtest`.

## D50 — A lease grant is the job's last start, the holder's renewal included · *built*

**Decision.** `TryLeaseJob` stamps `notify_job_leases.last_started_at` on **every grant**: the first take, a
take-over after a lapse, and the holder's own renewal. A refused ask changes nothing. Jobs ask for their lease
each time they run, and an expiry before every batch ([D48](#d48)), so the column says when any worker last
started the job (for an expiry, its last batch). The stale-job alarm reads it, and its threshold is the job's
lease plus a margin, so that a worker that dies is replaced before the alarm and not after it: an alarm at the
lease's own length would fire for that gap on a fleet that is working as designed. For the jobs
`app.Maintenance` runs the lease lasts twice the interval and the threshold is **three times the interval**; for
`digest_flush`, whose lease has a floor of 30 s, it is one minute at the defaults. [operations.md](../../docs/operations.md)
lists the seven at their defaults. The three-times rule assumes the tick is shorter than the interval, as the
defaults are (30 s against the shortest, 1 min).

**Alternative.** Two, neither adopted:

- **Alarm on `lease_expires_at`.** It already moves with every renewal, so "lapsed more than one interval ago" is
  the same condition with no change. But it needs each job's ttl to be known to whoever writes the alarm (the
  ttl is `max(30 s, 2 × interval)` and is not stored), and it says when a lease lapses, not when anything ran. The
  column that is named and documented for the second is the better home for it.
- **Record a run's end in `last_finished_at`**, which nothing wrote. It needs a second port method called when a
  job ends, and a decision about a job that ends in a fault. The alarm on a worker that is not taking the job does
  not need it, and it was left out for that reason; the job that takes its lease and fails every run does, and
  [D52](#d52) adds it.

**What goes wrong without it.** The renewal kept the column, so for a worker that keeps its jobs `last_started_at`
stayed at the moment it first took them. The alarm in [operations.md](../../docs/operations.md) reads "older than
its interval" and fired for every healthy job, permanently, until someone switched it off, and then nothing
watched the case it was written for: no worker taking the lease.

**Cost.** None in writes: the renewal already updates the row. A job that takes its lease and fails every run
keeps the column fresh, so *stale* does not catch *failing*; [D52](#d52) records the other half, and the error
handler (`WithMaintenanceErrorHandler`) still reports each fault as it happens.

Verified: `TestTryLeaseJobRecordsEveryGrantAsTheJobsLastRun` in `adapters/driven/postgres` (a grant by the
holder, by a new owner after a lapse and the first one all move the column; a refusal does not).

## D51 — Erasure forgets the pre-check's entries for the recipient's keys · *built*

**Decision.** `Admin.Erase`, when it was given the pre-check (`Deps.PreCheck`, the one the `Notifier` was built
with), first reads the recipient's idempotency keys from the store a page at a time
(`Maintenance.IdemKeys`: ascending bytewise, after the last key of the previous page, 500 to a page), tells the
pre-check to `Forget` the entry under `domain.PreCheckKey(identity, key)` for each, and **then** erases.

- **`ports.PreCheck.Forget(keys)`.** Keys it does not hold are not an error. A fault in the store is the fast
  path's, as for `Seen` and `Remember`: the Redis adapter reports it to its error handler, once for the call,
  and returns nil, because an entry it could not remove lapses with its ttl and the erasure that asked must not
  fail for it. The adapter deletes with a command per key in pipelines of 1,000, because the keys are hashes that
  share no slot and a Redis Cluster refuses one multi-key `DEL` across slots.
- **Forget first.** If the erasure then fails, the pre-check has lost entries for rows that are still there: a
  miss, which is all a fast path may do wrong ([D18](#d18)). The other order fails the other way: a process that
  dies between the erasure and the forget leaves entries for rows that are gone, and a retry finds no key left
  to forget.
- **A store that cannot name the keys, or a pre-check that refuses the call, stops the erasure before it
  begins**, and a page that does not move past the last key is an error and not a loop.
- With no pre-check the keys are not read.

**Alternative.** Three, none adopted:

- **Let the entries lapse**, which is what stood: up to `PreCheckTTL` (24 h by default).
- **Index the entries by recipient** (a Redis set per recipient, written by `Remember`). That is a second write
  on the hot path of every `Notify` to serve an operation that happens rarely.
- **Scan Redis for the recipient's entries.** The key is a hash of the whole identity and the producer's key, with
  no per-recipient prefix, so it would be a scan of the whole keyspace.

**What goes wrong without it.** For up to the ttl after an erasure, a producer that retries a key it had sent is
answered `Applied: false` with the id of a notification that no longer exists, and nothing is sent, while the
durable guard, whose row is gone, would have accepted the key as new. Nor is the entry only a hazard of
correctness: it is a hash of the identity and the key beside a notification id, kept after the erasure that was
meant to take the recipient's data. [D35](#d35)'s "Redis holds only hashed keys" is minimization, which an
erasure does not need to rely on.

**Cost.** An erasure reads the recipient's keys through the primary key and sends one pipeline per thousand. An
`Admin` built without the pre-check the `Notifier` has forgets nothing and does not know it; `Deps` is the one
struct they are built from so that they share it. A notification sent for the recipient *while* the erasure runs
is a new one, and its entry is the pre-check's own, bounded by its ttl. With Redis down the entries stay until
their ttl, the fault is reported, and the erasure succeeds.

Verified: `TestEraseForgetsThePreCheckEntryOfEachOfTheRecipientsKeysBeforeErasing`,
`TestEraseForgetsEveryKeyWhateverThePageSize`, `TestEraseWithoutAPreCheckDoesNotReadTheKeys`,
`TestEraseThatCannotReadTheKeysErasesNothing`, `TestEraseWhoseForgetIsRefusedErasesNothing` and
`TestEraseStopsWhenTheStoreDoesNotAdvanceThroughItsKeys` in `app`; the `Forget` cases of `PreCheckContract`, run
against the fake and against Redis; `TestPreCheckForgetWithRedisDownIsReportedOnceAndIsNotAnError` and
`TestPreCheckForgetDeletesOnlyTheKeysItIsGiven` in `adapters/driven/redis`; `TestIdemKeys` in
`adapters/driven/postgres`; and end to end `TestNS012AnErasedRecipientLeavesNothingAndTheirSuppressionStays`.

## D52 — A run that ends cleanly is recorded, so a job that only ever fails is visible · *built*

**Decision.** `Maintenance.FinishJob(job, owner)` stamps `notify_job_leases.last_finished_at` with the database's
`now()`, and only for the job's **current owner**: a worker whose lease went to another does not claim the other's
run, and the ask is then a no-op and not an error. `app.Maintenance.Pass` and `DigestFlusher.Flush` call it when
a run **ended cleanly**:

- it did not fail (a fault in any batch, table or window is a fault of the run);
- it did not lose its lease on the way (a refusal at any batch means the worker that holds the lease owns the
  rest, and this one did not finish what it began);
- the worker was not told to stop (a context that ended cuts a run short, and the code leaves quietly).

Nothing else writes the column: a grant begins a run ([D50](#d50)) and does not end one, and a take-over leaves
the finish that was there, because the last finish is the last time **any** worker finished the job. A finish that
cannot be recorded is a fault of the job, reported with the others and not stopping them, and the windows a flush
pass queued still count and still wake the dispatcher. The job and the owner are both required.

The two columns then tell three cases apart, and the alarms in [operations.md](../../docs/operations.md) read them:

| `last_started_at` | `last_finished_at` | The job |
|---|---|---|
| recent | recent | works |
| old | (any) | is not being taken: `notify.job.stale` ([D50](#d50)) |
| recent | old, or never | is taken and does not finish, because it fails at every run or runs longer than its threshold: `notify.job.failing` |

**Alternative.** Three, none adopted:

- **Leave it to the error handler** (`WithMaintenanceErrorHandler`, `WithFlushErrorHandler`). It reports each
  fault as it happens, to whatever the host wired, and nothing in the database says a job has not worked for a
  day; a host that wires no handler, or whose alerting drops a repeated message, has no alarm at all.
- **Store the last error** (`last_error text`). A second column and a retention question (how long is an error
  kept, and what does a run that works do to it) to answer what the first two answer without them; the error
  itself is in the handler and the log.
- **Stamp the end of every run, failed ones too.** Then `last_finished_at` is *last ended*, and a job that
  fails at every run looks as healthy as one that works, which is the case this exists for.

**What goes wrong without it.** A job that takes its lease and fails at every run, for instance a role that
cannot create partitions, a table that cannot be dropped for a lock that never clears, or a `DELETE` the database
refuses, keeps `last_started_at` fresh at every attempt, so the stale alarm stays quiet, and what it was for
(retention, expiry, provisioning) is not done and nothing in the database says so until the disk fills.

**Cost.** One more `UPDATE` of a one-row-per-job table per run, by the worker that holds the lease, beside the
renewal it already makes: one per flush pass, five seconds apart at the defaults. A run that is long, a flush
pass draining a backlog for longer than its threshold, shows as `failing` until it ends; that is a thing to look
at, and the alarm says it as *taken and not finishing*. The column does not say *why*, only that a run has not
ended cleanly since; the error handler and the log do. A deployment that does not run the jobs has no rows
and no alarm, as before.

Verified: `TestFinishJobRecordsTheCleanEndOfTheJobsOwnerAndOfNoOneElse` in `adapters/driven/postgres`;
`TestAJobThatRunsCleanlyRecordsItsEndAfterItsWork`, `TestAJobThatFailsIsNotFinished`,
`TestAJobWhoseLeaseIsHeldElsewhereIsNotFinished`, `TestAnExpiryThatLosesItsLeaseMidRunIsNotFinished`,
`TestAJobStoppedMidRunIsNotFinished`, `TestAFinishThatCannotBeRecordedIsReportedAndDoesNotStopTheOtherJobs`,
`TestACleanFlushPassRecordsItsEndOnceAndAfterItsWork`, `TestAFlushPassThatDidNotCompleteIsNotFinished` and
`TestAFlushFinishThatCannotBeRecordedIsReportedAndTheWindowsStillCount` in `app`; and end to end
`TestTheJobsClearWhatIsAgedAndLeaveTheRest` and `TestAJobThatFailsEveryRunIsStartedAndNeverFinished`.

## D53 — A broadcast's lease covers one page, and the commit that advances the cursor releases it · *built*

**Decision.** A worker leases a broadcast **for one page**: the resolver call, a plan for each recipient, and the
commit. `CommitBroadcastPage` is one transaction whose first statement is the fence and the advance together,
an `UPDATE` that matches only a *pending* broadcast under the caller's lease token, moves the cursor, completes the
broadcast on its last page, and **clears the lease**. The page's notifications are written after it, through the
path `Notify` uses, and the broadcast's `fanned` is increased by what was **written**, which a replay by the guard
is not. Any failure rolls all of it back, the cursor with it, and the lease is left to lapse.

Around that, three rules the store and the worker keep:

- **The store holds the derivation of a recipient's key.** A page whose notification is not keyed by
  `BroadcastMemberKey(broadcast key, recipient)` is refused whole, so a second caller of the port cannot get it
  slightly wrong. A page that is not the last and carries no cursor is refused too, because an empty cursor is where
  a broadcast starts and the next worker would begin again.
- **A page that cannot be committed is not retried by the worker that lost it.** `ErrBroadcastLeaseLost` means the
  lease lapsed and another worker holds the page, or the broadcast is done: nothing was written, and it is not a
  fault. Any other fault ends the pass and is left to the page's lease to lapse.
- **What the host's resolver returns is untrusted.** A recipient with an empty kind or id is skipped and reported to
  the error handler, so one bad row does not hold a tenant's broadcast for ever; a resolver that returns the cursor it
  was given with a page is a fault, because the page would be asked for again for ever; and `Broadcast` refuses, before
  recording anything, a topic nobody registered, which every page would fail to plan.

**Alternative.** One lease held across the whole broadcast and renewed at every page; the fence checked at the end of
the page's transaction; the cursor committed in a transaction of its own.

**What goes wrong without it.**

- *A lease across the whole broadcast* needs a renewal the port has no method for. Without one it lapses in the
  middle of a large tenant's broadcast, and the worker that holds it carries on against a fence that is now someone
  else's. With a page-sized lease a lapse costs one page, redone by whoever takes it.
- *A commit that does not release the lease* makes every page wait for the last one's lease to lapse: a broadcast of
  two hundred pages takes two hundred leases.
- *A cursor in a transaction of its own* is advanced while the page rolls back, and the recipients of that page are
  never notified, with nothing to say so. The two are one transaction so that the only states are before and after.
- *The fence at the end of the transaction* lets a lapsed worker write a whole page before it learns it lost. At the
  start it writes nothing, and a stale and a live worker never write the same page.
- *Counting what was handed in* over-counts a recipient a moving audience returns on two pages, and the count is how an
  operator reads a broadcast's progress.
- *The derivation of a key left to the caller* is the failure D7 exists for: a second caller derives it differently and
  a retried broadcast notifies everyone twice.

**Cost.** A page's lease cannot be extended, so a page slower than the lease (2 minutes by default, `WithBroadcastLease`)
is discarded at its commit and done again by another worker, never half written. A skipped recipient is reported
through the error handler and counted nowhere. A broadcast that faults at every page is retried at every lease and
reported at every pass, and nothing in the database marks it failing; D52's alarm covers the jobs, not a broadcast.

Verified: `TestAPageThatFailsInsideItsTransactionRollsBackAsAWhole`, `TestCommitBroadcastPageIsFencedByTheLeaseToken`,
`TestCommitBroadcastPageWritesThePageAndAdvancesTheCursorTogether`, `TestARecipientOnTwoPagesIsNotifiedOnce`,
`TestAPageNotKeyedByTheBroadcastsMemberKeysIsRefused` and `TestAPageThatIsNotTheLastNeedsACursor` in
`adapters/driven/postgres`; `TestALostLeaseIsNotAFaultAndThePassGoesOn`, `TestAResolverThatDoesNotAdvanceIsAFaultAndNotALoop`,
`TestARecipientWithAnEmptyComponentIsSkippedAndReported` and `TestBroadcastRefusesWhatCannotBeExpandedAndRecordsNothing`
in `app`; and end to end `TestACrashBetweenPagesResumesAtTheNextPage` and
`TestACrashInsideAPageRollsTheWholePageBackAndTheNextWorkerRedoesIt`.

---

## Open decisions


None blocking Phase 1. Two are deferred to Phase 2 by design, recorded here so their absence is
visible rather than implicit:

| Question | Why it waits | Where |
|---|---|---|
| How is delivery **acknowledgement** modelled? | Acknowledgement introduces a per-notification lifecycle; putting that state in the Phase 1 hot path would couple delivery to workflow | [Phase 2](../002-escalation-workflows/) |
| What happens when an **escalation chain** exhausts every step? | Depends on the acknowledgement model above | [Phase 2](../002-escalation-workflows/) |
