# Design decisions

Each entry states the decision, the alternative, and **what goes wrong without it**. A decision with
no failure mode attached is a preference, and preferences do not belong in a specification.

Two kinds of entry carry a tag:

- **inherited-gap** resolves something the originating design left open, contradictory, or
  unimplementable: D1, D2, D3, D4, D5, D6, D7, D9, D10, D11.
- **review** fixes a defect found by the architecture review of this repository after the
  extraction, in the design as first written here: D16–D36, plus the amendments marked on D2, D4,
  D5, D7, D8, D11, D12, D13 and D15. Several were real correctness bugs in the normative contract —
  D16 (a cross-tenant leak on the live stream), D17 (a worker that could not read what it delivers),
  D18 (a pre-check that dropped notifications) and D22 (a delivery mode that reintroduced the bug the
  outbox exists to fix). Read those first.

Every entry that claims a schema property names the `make verify-schema` test that asserts it.

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
`notify_digest_flush()` then enqueues exactly one delivery for it, conditionally on
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
`notify_expire_digests()` once their delivery finishes.

Verified: TEST 12 — a second open window is rejected; a full window seals and the next member opens
another; a duplicate flush is a no-op; one window yields one delivery.

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

Verified: TEST 16 — no foreign key references a partitioned table or a partition.

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

Verified: TEST 9, TEST 17 — a genuine failure is dead-lettered with its reason; a correct outcome
cannot be.

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
needs one `notify_rehome_shards()` call to fold rows from retired shards into live ones.

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

Verified: TEST 7 — a concurrent claimer on the same shard skips the rows another holds.

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

Verified: TEST 9, TEST 17.

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
  (`notify_canonical()`; the Go domain implements the same encoding against a frozen vector).
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

Verified: TEST 2 (tenant in the key), TEST 19 (encoding unambiguous; frozen vector); the pre-check's
ordering is `NotifierContract` "a failed transaction leaves no pre-check entry".

## D19 — The claim is a fenced lease · *review*

**Decision.** `notify_claim_outbox(shard, limit, lease)` selects due rows `FOR UPDATE SKIP LOCKED`,
increments `attempts`, issues a fresh `lease_token`, and moves `next_attempt_at` to `now() + lease`.
Every outcome write (`notify_retry_delivery`, `notify_defer_delivery`, `notify_finish_delivery`,
`notify_bind_addresses`) must present the current token and affects nothing otherwise.

**Alternative.** The first version specified `ClaimOutbox` as "pops up to max due entries" with a
`claimed_at` column and no lease, lock or fence.

**What goes wrong without it.** Without an atomic claim, two workers on one shard — or one worker
during a rolling deploy — both take the same row and both send it. Without a lease, a worker that dies
holding rows either strands them or they are re-driven while it is still sending. Without a fence, the
dead worker's late "delivered" overwrites the live one's outcome. Counting the attempt *at claim* is
what makes a delivery that crashes its worker every time — a poison payload that panics the renderer —
reach the attempt ceiling instead of looping forever; counting on failure never counts a crash.

Verified: TEST 7 (a concurrent session skips locked rows; leased rows are invisible until the lease
lapses; each claim counts), TEST 8 (a stale token cannot record an outcome). Mutation-checked: with
`SKIP LOCKED` removed, TEST 7 fails.

## D20 — The queue holds pending work only; history is a separate log · *review*

**Decision.** `notification_outbox` is a queue: a row exists only while its delivery is pending.
`notify_finish_delivery()` removes it and appends it to `notification_deliveries` — range-partitioned
by completion time, retired by partition — in one transaction, together with any dead letter and the
next fallback channel.

**Alternative.** The first version kept every row in the outbox forever, marked `delivered_at`.

**What goes wrong without it.** A queue that keeps its history grows without bound (NR-022 said nothing
about it), and every claim, retry and deferral is an `UPDATE` on that ever-larger table — dead-tuple
churn and vacuum pressure proportional to all traffic ever, not to the backlog. Splitting them keeps the
hot table as small as the work in flight, and puts history where the producer's status query and the
provider's bounce callback need it: `notification_deliveries` is indexed by notification id and by
`(channel, provider_message_id)`.

Verified: TEST 9 — finished deliveries leave the queue; a provider callback finds its delivery by
provider message id.

## D21 — Idempotency is guaranteed for a window, not forever · *review*

**Decision.** A key guards replays for `Config.IdempotencyWindow` (default 7 days, minimum 24 hours).
`notify_expire_idem()` deletes older keys in batches, run as a single-owner job. Exactly-once is stated
as holding *within the window*.

**Alternative.** The first version kept every key forever and called it a feature.

**What goes wrong without it.** One row per notification ever sent, in a table with a six-column text
primary key, is the largest table in the system and the only one retention never touches. At a
sustained 200 notifications a second that is 6 billion rows a year. Producers retry within minutes,
buses redeliver within hours; a key's useful life is bounded, and the industry norm reflects it —
Stripe prunes idempotency keys after 24 hours. Hash-partitioning the table on the identity keeps each
expiry batch and each vacuum inside one partition.

Verified: TEST 14.

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

Verified: TEST 15 (a live job lease cannot be taken; a lapsed one can).

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
- **At dispatch**, a delivery that finds the budget spent is deferred (`notify_defer_delivery`, which
  returns its attempt), and `notify_defer_tenant_channel()` moves that tenant's entire due backlog on
  that channel to the reset in one statement.

**Alternative.** The first version checked quota at dispatch only, deferring one row at a time.

**What goes wrong without it.** Claims take due rows oldest first. A tenant that floods the system
occupies the head of every shard, so drainers spend their throughput claiming and deferring its rows
one by one while other tenants' deliveries wait behind them. The exhausted tenant *did* reduce everyone
else's delivery rate — NS-007, violated by the mechanism meant to uphold it. Moving the backlog out of
the due range costs one indexed statement per exhaustion, after which the claim goes straight to other
tenants.

Verified: TEST 11 — the noisy tenant holds the head of the shard; one statement moves its backlog; the
next claim goes to the quiet tenant.

## D25 — Addresses are plural, and each is its own delivery · *review*

**Decision.** `AddressBook.Resolve` returns `[]Address`. On first claim the dispatcher binds the
delivery (`notify_bind_addresses`): one address binds in place; several replace the row with one row
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

Verified: TEST 10 — three devices bind as three unleased rows; binding is fenced and happens once.

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

Verified: TEST 9 (fallback enqueues on an undelivered outcome), TEST 13 (cancel stops pending
deliveries and leaves an in-flight one to its worker).

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

## D35 — Personal data is erasable, and suppressions survive erasure · *review*

**Decision.** `notify_erase_recipient()` removes a recipient's rows from every table that holds them —
inbox, guard, queue, delivery log, dead letters, digests, preferences, schedules, addresses — in one
transaction under the recipient's own scope, and records the erasure in `erasure_requests` by the hash
of the identity. `channel_suppressions` is keyed by `address_hash`, never the address, and is kept. Redis
holds only hashed keys.

**Alternative.** The first version had no erasure path, kept dead-letter payloads with content, and
stored suppressed addresses in plaintext indefinitely.

**What goes wrong without it.** A GDPR erasure request had no answer short of hand-written SQL across ten
tables, some partitioned. Deleting a person who complained would also delete their suppression and
resume mailing them — the one outcome worse than not erasing. A hash is minimization, not anonymity, and
is retained on the legitimate basis of honoring the objection.

Verified: TEST 18 — erasure removes u9 from all nine tables, leaves u1 untouched, keeps the suppression,
and records the erasure by hash.

## D36 — The capacity envelope is stated, and so is the next step · *review*

**Decision.** [plan.md § Capacity model](plan.md#capacity-model) states the design envelope for one
PostgreSQL primary as targets, and NS-010 gates release on a load test that meets them. It also names the
scale-out path past one primary: every table is keyed by realm and tenant, so the `Store` can route by
tenant to separate databases, or distribute on `(realm, tenant_id)`, without a schema change.

**Alternative.** The first version called the engine highly scalable without a number or a test.

**What goes wrong without it.** An unfalsifiable scalability claim is adopted by a team whose load is ten
times what the design can carry, and discovered in production. Stated targets make the claim checkable
and the gap to the next step visible.

---

## Open decisions

None blocking Phase 1. Two are deferred to Phase 2 by design, recorded here so their absence is
visible rather than implicit:

| Question | Why it waits | Where |
|---|---|---|
| How is delivery **acknowledgement** modelled? | Acknowledgement introduces a per-notification lifecycle; putting that state in the Phase 1 hot path would couple delivery to workflow | [Phase 2](../002-escalation-workflows/) |
| What happens when an **escalation chain** exhausts every step? | Depends on the acknowledgement model above | [Phase 2](../002-escalation-workflows/) |
