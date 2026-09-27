# Contract: Bus subjects

The engine depends on a `Bus` **port**, not a broker. The reference default is Redis Streams — so a
deployment needs no extra infrastructure beyond what the store already requires — and NATS JetStream
is a drop-in swap for deployments wanting quorum replication or cross-region mirroring.

Subject tokens use `<realm>` and `<tenant_tag>`, where `tenant_tag` is `kind:id`. The realm is in
every subject so two products sharing a bus cannot cross-deliver ([D1](../design-decisions.md#d1)).

## Subjects

| Subject | Publisher | Consumer | Payload and semantics |
|---|---|---|---|
| `notify.<realm>.<tenant_tag>` | Any producer | Notifier, queue group | `{recipient:{kind,id}, topic, priority, title, body, payload, idem_key, occurred_at, attributes}`. Applies preferences, persists the inbox row and per-channel outbox rows in one transaction. Idempotent on `(realm, recipient, idem_key)` |
| `notify.broadcast.<realm>.<tenant_tag>` | Admin surface | Broadcast expander, queue group | `{audience, topic, priority, title, body, payload, idem_key}`. Expands the audience in **pages** off the request path, deriving each per-recipient key deterministically from `idem_key` (NR-020) |
| `notify.dispatch.<realm>.<shard>` | Notifier (or the drain ticker) | Dispatcher, queue group per shard | `{shard}` — a nudge that work is due. Carries no delivery data: the outbox is the source of truth, so a lost nudge costs latency, not a delivery |
| `notify.dlq.<realm>` | Dispatcher | Dead-letter sweeper, single owner | Deliveries that exhausted their attempt ceiling or failed non-retryably. The sweeper re-drives those still under the cap and parks the rest in `dead_letters` with an alarm — never dropped, never looping (NR-007) |
| `notify.retention.tick` | Scheduler | One worker, single owner | `{trace_id}` → retires aged inbox and dead-letter partitions per the configured windows (NR-022) |
| `notify.digest.tick` | Scheduler | Dispatcher, queue group per shard | `{shard, trace_id}` → flushes `digest_buffer` windows whose `flush_at` has passed, one delivery per window (NR-014) |
| `notify.quota.reset` | Scheduler | One worker, single owner | Rolls the quota counters' window ([D5](../design-decisions.md#d5)) |

## Rules

- **Producers are thin.** A producer publishes one notification and is done. It never reads
  preferences, never picks channels, never touches the outbox. Only the engine knows those.
- **Identity is authoritative from the publisher.** `recipient` and `tenant` come from the trusted
  producer, never from notification content (NR-009). A producer that forwards a user-supplied
  recipient id has created a delivery-redirection vulnerability.
- **`idem_key` is required on every mutating subject.** A message without one is rejected rather
  than delivered, because accepting it silently forfeits NR-002 for that notification.
- **At-least-once everywhere.** Every consumer is idempotent; redelivery is normal operation, not an
  error path.
- **Queue groups for scale-out, single owner for ticks.** Fan-out and dispatch are N-replica queue
  groups scaled on consumer lag. Scheduled ticks are single-owner: two workers retiring partitions
  concurrently is a race with no upside.
- **Dispatch nudges are advisory.** The dispatcher also polls its shard on an interval, so a dropped
  nudge delays a delivery rather than losing it. Nudges exist to make the common case fast, not to be
  reliable.
- **Shards are stable.** `notify.dispatch.<realm>.<shard>` partitions by the recipient hash, fixed
  for the deployment's life ([D11](../design-decisions.md#d11)). Changing the shard count requires
  draining first; doing it live double-delivers.

## Observable assertions

- A message redelivered with the same `idem_key` yields one inbox row and one delivery per channel.
- A message whose recipient disabled a `(topic, channel)` pair produces the inbox row and **no**
  outbox row for that channel.
- A simulated channel-provider failure routes the delivery to `notify.dlq.<realm>`, and delivery on
  every other channel is unaffected.
- A broadcast to a large tenant returns to the caller before expansion completes, and a re-published
  broadcast with the same `idem_key` does not double-notify anyone.
- Two realms publishing the same `idem_key` for the same recipient id both deliver.
