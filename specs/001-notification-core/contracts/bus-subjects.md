# Contract: Bus subjects (optional ingest)

**The engine needs no message bus.** A notification is accepted when PostgreSQL commits it
([D23](../design-decisions.md#d23)); dispatchers find work by claiming from the queue table and are
woken in memory or by polling; scheduled jobs are single-owner through row leases
([D22](../design-decisions.md#d22)). The default deployment is PostgreSQL + Redis and nothing else.

A bus is supported for one purpose: **producers that prefer to publish rather than call.** An ingest
adapter — a *driving* adapter, `adapters/driving/natsingest` — consumes the subject below and calls
`Notifier.Notify`. The core has no `Bus` port.

## Durability requirement

NS-003 ("no delivery lost to a crash") measures from acceptance. When a producer publishes instead of
calling, acceptance moves to the bus, so the bus must not lose an acknowledged message:

| Bus | Supported for ingest | Why |
|---|---|---|
| **NATS JetStream**, replicated stream (R3), publish acknowledged after quorum | Yes — the reference adapter | An acknowledged message survives the loss of a node |
| Redis Streams | **No** | A primary acknowledges `XADD` before replicating; a failover in that window loses an acknowledged notification. The argument research §10 makes against `SET NX` applies unchanged |
| Core NATS (no JetStream) | No | At-most-once |

## Subjects

Tokens are `<realm>` — restricted to `[a-z0-9-]` by `Config.Validate`, so it is always a valid subject
token — and `<tenant_token>`, the first 16 hex characters of `sha256(canonical(tenant_kind, tenant_id))`.
Opaque tenant ids may contain `.`, `*` or `>`, which would break or wildcard a subject; the hash cannot.
The token only routes; the payload carries the full identity.

| Subject | Publisher | Consumer | Payload and semantics |
|---|---|---|---|
| `notify.ingest.<realm>.<tenant_token>` | A trusted producer | `natsingest`, durable queue group | `{tenant:{kind,id}, recipient:{kind,id}, topic, priority, data, title, body, payload, idem_key, occurred_at, deliver_after, deliver_before, attributes}`. The adapter calls `Notify` and acknowledges only after it returns — after commit. A replay is acknowledged like a success |
| `notify.ingest.dlq.<realm>` | `natsingest` | Operators | A message that failed validation, or exceeded `MaxDeliver`. Also written to `dead_letters` with `source='bus'`, `reason='poison'` |

## Rules

- **The realm comes from the stream, not the payload.** Each realm's subjects are bound to that realm's
  credentials; the adapter takes the realm from the subject it is configured for and ignores any in the
  body (NR-009).
- **Producers are thin.** A producer publishes one notification. It never reads preferences, never picks
  channels, never touches the queue.
- **Identity is authoritative from the publisher.** `recipient` and `tenant` come from the trusted
  producer, never from notification content. A producer that forwards a user-supplied recipient id has
  created a delivery-redirection vulnerability.
- **`idem_key` is required.** A message without one is dead-lettered as `poison`, never delivered
  unguarded.
- **At-least-once.** Redelivery is normal operation; `Notify` is idempotent on the key.

## What moved off the bus, and why

| Previously | Now | Reason |
|---|---|---|
| `notify.dispatch.<realm>.<shard>` nudges | In-memory wakeup after a local commit, adaptive polling otherwise, optional `LISTEN/NOTIFY` | A nudge carried no data; polling bounds latency without a broker ([D22](../design-decisions.md#d22)) |
| `notify.broadcast.…` | A `notification_broadcasts` row | A durable record with a cursor resumes after a crash; a bus message restarts from page one ([D7](../design-decisions.md#d7)) |
| `notify.retention.tick`, `notify.digest.tick`, `notify.quota.reset` | Workers take a `notify_job_leases` lease | Single-owner without a broker, and a record of when each job last ran |
| `notify.dlq.<realm>` | `dead_letters`, written atomically by the terminal transition | A dead letter is a row, queryable and replayable, written in the same transaction as the outcome |

## Observable assertions

- A message redelivered with the same `idem_key` yields one inbox row and one delivery per channel.
- The adapter does not acknowledge a message until `Notify` has returned, so a crash before commit leaves
  the message to be redelivered.
- A message whose body names a different realm is delivered in the subject's realm, or rejected — never
  delivered in the body's realm.
- A message without `idem_key` reaches `dead_letters` as `poison`.
