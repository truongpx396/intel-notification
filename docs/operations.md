# Operations

Running the engine. Written for the person on call, so each section leads with the symptom.

## The moving parts

| Component | Shape | Scales on |
|---|---|---|
| `Notifier` | in-process in producers, or the gRPC service | producer traffic |
| `Dispatcher` | queue-group workers, one claim loop per shard | outbox depth |
| Digest flush | scheduled, sharded | window count |
| Retention | scheduled, **single owner** | nothing; runs daily |
| Quota reset | scheduled, **single owner** | nothing; rolls the window |
| Dead-letter sweeper | scheduled, **single owner** | dead-letter depth |

Single-owner means exactly one runner. Two workers retiring partitions concurrently is a race with no
upside, so these are not queue groups.

## Alarms worth having

| Alarm | Threshold | Why it matters |
|---|---|---|
| `notify.outbox.depth` | rising for > 5 min | Deliveries are not draining. Either a channel is failing or drainers are down |
| `notify.outbox.oldest_age` | > 10 × `BackoffBase` | Something is stuck rather than merely slow — depth alone can look healthy while one shard starves |
| `notify.dlq.dead.count` | any increase | A delivery gave up permanently. This is the one that means a user did not get told something |
| `notify.digest.overdue` | any window past `flush_at` + interval | A digest window is leaking; its members are held, not delivered |
| `notify.quota.exhausted` | per tenant | A tenant is hitting its ceiling — either abuse or a legitimate limit that needs raising |
| `notify.partition.missing` | next month absent | Provisioning lapsed; inserts will start failing at month roll |

`oldest_age` alongside `depth` is deliberate. Depth can sit flat and healthy-looking while a single
shard's drainer is wedged, and only age exposes that.

## Playbooks

### Outbox depth climbing

1. Is it one shard or all? `GROUP BY shard` on undelivered rows. One shard means a wedged drainer;
   all shards means a channel or a store problem.
2. Check `last_error` on the oldest undelivered rows. A single provider dominating means the outage is
   external and backoff is doing its job — confirm the retry interval is spreading rather than
   hammering.
3. Check drainer replica count and liveness.
4. Do **not** clear the outbox. Every row is an undelivered notification; deleting them is the one
   irreversible mistake available here.

### Dead letters appearing

1. Group by `terminal_reason`. `suppressed` and `no_address` are **correct outcomes**, not incidents —
   they mean the engine refused to retry a hopeless address. Only `max_attempts` indicates a genuine
   failure ([D12](../specs/001-notification-core/design-decisions.md#d12)).
2. For `max_attempts`, read `last_error`. Provider-side means the outage outlived the retry budget;
   consider whether `MaxAttempts` or `BackoffCeiling` should be larger for that channel.
3. Replay via `POST /admin/notifications/dead-letters/{id}/replay` once the cause is fixed. Replay is
   idempotent per channel, and records `replayed_at`.

### Changing the shard count

**This is the operation most likely to cause double-delivery.** `Shards` is in the shard hash, so
changing it re-maps in-flight rows to different drainers, and both the old and new claim can match
([D11](../specs/001-notification-core/design-decisions.md#d11)).

The only safe order:

1. Stop producers, or accept the queueing.
2. Let the outbox drain to zero. Verify: no undelivered, non-terminal rows.
3. Stop all drainers.
4. Change `Shards`; restart.

There is no online path. If you need one, that is a design change, not a runbook step.

### Partition provisioning

Both partitioned tables need next month's partition before the month rolls. There is deliberately no
default partition — a missing one fails inserts loudly rather than silently filling a catch-all that
can never be retired. Run provisioning monthly and alarm on absence.

### Retention

Retirement of an aged partition is a metadata operation, not a row scan. It is safe to run with
pending deliveries outstanding — there is no foreign key from the outbox into the inbox precisely so
that this cannot deadlock ([D6](../specs/001-notification-core/design-decisions.md#d6)).

## Capacity notes

- The pre-check in Redis is an optimization. If Redis is empty or down, every `Notify` costs one extra
  database round-trip and **stays correct**. Do not treat its loss as an incident.
- The unread badge is recomputed from the store. A missed pub/sub message self-heals on the next
  reconnect — also not an incident ([D15](../specs/001-notification-core/design-decisions.md#d15)).
- The inbox's hot read is recipient-scoped and index-covered. The query that degrades first is an
  unfiltered admin listing across tenants; it is not on any user path, and it should not be added to one.
