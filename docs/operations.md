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
| `notify.db.oldest_xmin_age` | the oldest `backend_xmin` in `pg_stat_activity` older than 60 s (tune to your rate) | A transaction is pinning vacuum, so the queue table will bloat and latency will follow. Find it and end it ([D38](../specs/001-notification-core/design-decisions.md#d38)) |
| `notify.db.checkpoints_requested` | `checkpoints_req` rising faster than `checkpoints_timed` | Checkpoints are being forced by WAL volume: `max_wal_size` is too small for this write rate |
| `notify.outbox.dead_tuples` | still rising after the last autovacuum, or `last_autovacuum` older than 5 min | Vacuum is blocked or not keeping up. Tens to hundreds of thousands between runs is normal at 1,000 / s; what matters is that it comes back down |
| `notify.channel.dedup_none` | informational, per registered channel | Which channels are at-least-once by declaration |

## Playbooks

### Queue depth climbing

1. **One tenant or all?** `SELECT tenant_id, channel, count(*) FROM notification_outbox WHERE next_attempt_at <= now() AND (lease_expires_at IS NULL OR lease_expires_at <= now()) GROUP BY 1, 2 ORDER BY 3 DESC`.
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

### Protecting the queue from its own database

The queue table is small but churns: every delivery is inserted, updated once or twice and deleted, so
it lives on WAL and on vacuum. Two things stalled it in `make soak` (ten minutes at 1,000
notifications/s, fsync on, on a laptop with PostgreSQL in Docker — read the shape, not the figures):

- **Checkpoints sized for a quiet database.** The queue writes 5–6 KB of WAL per notification, so about
  5 MB/s at 1,000/s. PostgreSQL forces a checkpoint at roughly a third of `max_wal_size`, so with the
  default 1 GB that is one about every 75 seconds at this rate (the server log showed
  `checkpoint starting: wal`), and each checkpoint is followed by full-page images that raise the WAL
  rate further. With the workers waiting on `WALWrite` and `WALSync`, accepts fell to about 200/s,
  the backlog reached 84,000 and end-to-end p95 reached 76 s. The same run with `max_wal_size=16GB` and
  `checkpoint_timeout=15min` had no stall in that stretch. Size `max_wal_size` to at least fifteen
  minutes of your peak WAL rate, and alert when `pg_stat_bgwriter.checkpoints_req` climbs faster than
  `checkpoints_timed`.
- **A long-running transaction.** Vacuum cannot reclaim a row version an open snapshot might still see.
  One held open for two minutes took the outbox heap from about 40–50 MB to 180–230 MB (570,000 to
  750,000 dead tuples) and cut the share of HOT updates from 67% to as low as 18%. Latency
  suffered while it was open and for up to about 90 seconds after (claim p95 up to 360 ms, end-to-end
  p95 up to 31 s) while vacuum caught up. It cleared without intervention and the heap shrank back
  about 100 seconds after the release, but the indexes stayed at their high-water mark (123–177 MB) until
  reindexed. Set `idle_in_transaction_session_timeout` and `statement_timeout` on every role that shares
  the database, and alert on the age of the oldest `backend_xmin` in `pg_stat_activity`. Why the deployment shape
matters here — a library-mode queue shares the host's transactions — is
[D38](../specs/001-notification-core/design-decisions.md#d38).

Autovacuum ran about once a minute (the default `autovacuum_naptime`), so between runs the queue held
tens of thousands to a few hundred thousand dead tuples, and its physical size in steady state was
50–90 MB for a live set of a few hundred rows. That is bounded and stable, and it is what to expect, not
a leak. Neither vacuum settings nor the effect of a slower disk have been varied yet.

### Changing the shard count

An online operation ([D11](../specs/001-notification-core/design-decisions.md#d11)):

- **Raising** `Shards`: change it and roll the workers. Existing rows keep their shard; new rows spread
  wider.
- **Lowering** `Shards`: change it, roll the workers, then run `Maintenance.RehomeShards(<new count>)`
  so rows in retired shards move into live ones — or its SQL by hand, in one transaction:
  `UPDATE notification_outbox SET shard = shard % <n> WHERE shard >= <n>` and the same on
  `digest_buffer`. Safe while workers run.

Nothing is double-delivered either way: the claim locks rows, not shards.

### Partition provisioning

`notifications`, `notification_deliveries` and `dead_letters` need next month's partition before the
month rolls. The provisioning job, `Maintenance.EnsurePartitions`, creates each missing month ahead of
the clock and applies `notify_apply_recipient_scope()` to every new `notifications` partition —
PostgreSQL does not carry a parent's row-level security to its partitions. Run it at startup as well as
on its schedule: a fresh deployment whose bootstrap months have passed cannot accept a notification
until it has run. Its role needs `CREATE` on the schema.

`make verify-schema` TEST 5 asserts every existing partition is scoped, and `TestEnsurePartitions`
asserts a provisioned one is. A partition created by hand without the policy is an isolation incident
(`notify.partition.unscoped`). There is deliberately no default partition: a missing one fails inserts
loudly rather than filling a catch-all that can never be retired.

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
