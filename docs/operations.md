# Operations

Running the engine. Written for the person on call, so each section leads with the symptom.

## The moving parts

| Component | Shape | Scales on |
|---|---|---|
| `Notifier` | in-process in producers, or the gRPC service | producer traffic |
| `Dispatcher` | any number of worker replicas; each claims across every shard with `SKIP LOCKED` leases | queue depth and oldest age |
| Relay | the SSE surface; any replica serves any recipient | connected clients |
| Maintenance | runs in every worker; each job single-owner through a `notify_job_leases` lease | nothing — the lease picks one runner |

Maintenance jobs: retention, partition provisioning, idempotency expiry, digest flush and expiry,
quota rollover, broadcast expansion. `SELECT * FROM notify_job_leases` shows who holds each and when it
last ran.

There is no broker in the default deployment and nothing to scale for one.

## Alarms worth having

| Alarm | Threshold | Why it matters |
|---|---|---|
| `notify.outbox.depth` | rising for > 5 min | Deliveries are not draining. A channel is failing or workers are down |
| `notify.outbox.oldest_age` | > 10 × `BackoffBase` | Something is stuck rather than slow — depth alone can look flat while one tenant or channel starves |
| `notify.dlq.dead.count` | any increase | A genuine failure: someone was not told something. Suppressed and no-address outcomes never land here |
| `notify.lease.lost` | sustained non-zero | Workers are losing claims mid-send: `ClaimLease` is shorter than a channel's send time, or workers are stalling. Each loss may mean a duplicate on a `DedupNone` channel |
| `notify.digest.overdue` | any window past `flush_at` + one interval | The flush job is not running; members are held, not delivered |
| `notify.quota.exhausted` | per tenant | A tenant hit its ceiling — abuse, a bug in its producer, or a limit that needs raising |
| `notify.partition.missing` | next month absent | Provisioning lapsed; inserts start failing at month roll |
| `notify.partition.unscoped` | any `notifications` partition without `recipient_scope` | **Isolation incident.** A partition queried by name would show every recipient's rows. Apply `notify_apply_recipient_scope()` now |
| `notify.job.stale` | a job's `last_started_at` older than twice its interval | No worker is taking the lease |
| `notify.channel.dedup_none` | informational, per registered channel | Which channels are at-least-once by declaration |

## Playbooks

### Queue depth climbing

1. **One tenant or all?** `SELECT tenant_id, channel, count(*) FROM notification_outbox WHERE next_attempt_at <= now() GROUP BY 1, 2 ORDER BY 3 DESC`.
   A single tenant with an exhausted quota should already have been deferred out of the due range; if
   it has not, the quota job is not running.
2. **Read `last_error`** on the oldest due rows. One provider dominating means an external outage and
   backoff doing its job — confirm retries are spreading rather than hammering.
3. **Check worker replicas and `notify.lease.lost`.**
4. **Do not delete from the queue.** Every row is an undelivered notification. To stop something, use
   `Cancel`, which records what it stopped.

### Dead letters appearing

1. **Group by reason:** `SELECT reason, channel, count(*) FROM dead_letters WHERE replayed_at IS NULL GROUP BY 1, 2`.
   - `max_attempts` — the failure outlived the retry budget. Read `last_error`; consider whether
     `MaxAttempts` or `BackoffCeiling` should be larger for that channel.
   - `rejected` — the provider permanently refused the message: a malformed template, an invalid
     sender, a payload over a size limit. Retrying will not help; fix the cause first.
   - `poison` — a bus message that failed validation. Look at the producer.
2. **Correct outcomes are elsewhere.** Suppressed and no-address deliveries are in
   `notification_deliveries`, not here ([D12](../specs/001-notification-core/design-decisions.md#d12)).
3. **Replay** with `POST /admin/notifications/dead-letters/{id}/replay` once the cause is fixed. Replay
   enqueues one fresh delivery and records `replayed_at`; a second replay of the same dead letter does
   nothing. On a `DedupProvider` channel, a replay after the provider's window (24h for Resend) is a new
   send.

### Changing the shard count

An online operation ([D11](../specs/001-notification-core/design-decisions.md#d11)):

- **Raising** `Shards`: change it and roll the workers. Existing rows keep their shard; new rows spread
  wider.
- **Lowering** `Shards`: change it, roll the workers, then run `SELECT notify_rehome_shards(<new count>)`
  so rows in retired shards move into live ones. Safe while workers run.

Nothing is double-delivered either way: the claim locks rows, not shards.

### Partition provisioning

`notifications`, `notification_deliveries` and `dead_letters` need next month's partition before the
month rolls; the provisioning job creates several months ahead. It must call
`notify_apply_recipient_scope()` on every new `notifications` partition — PostgreSQL does not carry a
parent's row-level security to its partitions, and `make verify-schema` TEST 5 asserts every partition
is scoped. There is deliberately no default partition: a missing one fails inserts loudly rather than
filling a catch-all that can never be retired.

### Retention

Retiring an aged partition is a metadata operation, safe with pending deliveries outstanding — nothing
references a partition by foreign key ([D6](../specs/001-notification-core/design-decisions.md#d6)).
Inbox retention drops read and unread notifications alike. `notify_idem`, digest windows and completed
broadcasts expire in batches, so expiry never holds a long lock.

### An erasure request

`POST /admin/erasure {tenant, recipient}` — or `Catalog.Erase` in service mode — removes the recipient
from every table in one transaction and returns the rows removed per table. `erasure_requests` records it
by the hash of the identity. Suppressions are kept by design: they hold address hashes, and removing them
would resume sending to someone who complained. Redis holds nothing to erase — its keys are hashes.

## Capacity notes

- The pre-check in Redis is an optimization. If Redis is empty, every `Notify` takes the durable path and
  **stays correct**. If Redis is down, the live stream falls back to reconnect-time counts and quotas
  fail open. Neither loses a notification.
- The unread badge is a bounded count on a partial index. A relay redeploy reconnecting every client is
  a burst of cheap reads, not a scan of every inbox.
- The queue holds pending work only. A deep queue means a large backlog, never accumulated history.
- Sizing formulas for every growing table are in
  [plan.md § Capacity model](../specs/001-notification-core/plan.md#capacity-model).
