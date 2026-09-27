# Specification: Notification core

**Status**: normative, implementation not started.
**Contracts**: [contracts/](contracts/) · **Data model**: [data-model.md](data-model.md) ·
**Decisions**: [design-decisions.md](design-decisions.md) · **Build order**: [tasks.md](tasks.md)

This specification describes the engine as a product. Requirements carry `NR-0NN` ids and success
criteria `NS-0NN`; the mapping from the originating project's `FR-032`–`FR-039` / `SC-011`–`SC-013`
is recorded in [PROVENANCE.md](../../PROVENANCE.md).

---

## Problem

A product needs to tell specific recipients about specific events, over whichever channels each
recipient chose, exactly once, without ever telling the wrong person. Doing that correctly requires
solving five problems that are easy to get subtly wrong and expensive to discover late:

1. **Fan-out that survives a crash.** Persisting a notification and delivering it are two stores, so
   they cannot share a transaction.
2. **Deduplication that does not eat deliveries.** An at-least-once bus redelivers; a naive guard
   makes the redelivery a no-op *including the parts that had not happened yet*.
3. **Isolation that is provable.** "A notification is only visible to its recipient" must be a
   property of the data layer, not a discipline applied at every call site.
4. **Extensibility without touching fan-out.** Adding a channel or an event type must not mean
   editing the riskiest code in the system.
5. **Bounded growth.** An inbox is append-mostly and grows forever unless retention is designed in.

## Actors

| Actor | Role |
|---|---|
| **Producer** | Any host subsystem that publishes a `Notification`. Thin by design: it builds the value and calls `Notify`. |
| **Recipient** | The opaque delivery subject — a user, an organization, a device, a Slack channel, an external contact. |
| **Tenant** | The host's isolation boundary containing recipients — a workspace, organization, account. |
| **Operator** | Runs the engine: shards, retention, dead-letter inspection, quotas. |
| **Channel** | A delivery target implementation. In-app and email are reference implementations, not the model. |

---

## Functional requirements

### Delivery core

- **NR-001**: The system MUST persist every notification as a durable, recipient-scoped record,
  regardless of which delivery channels are enabled. The record is both the inbox entry and the
  deduplication backstop.
- **NR-002**: Every notification MUST carry an idempotency key supplied by the producer. A replay of
  the same `(realm, recipient, idem_key)` MUST produce no second record, no second delivery on any
  channel, and MUST report itself as a replay rather than as an error.
- **NR-003**: The system MUST write the inbox record and one pending delivery per enabled channel in
  a **single transaction**. It MUST NOT perform a durable write followed by best-effort delivery
  attempts.
- **NR-004**: The system MUST drive channel delivery from the committed pending-delivery set, at
  least once. A fast pre-check MAY be used to avoid a database round-trip on an obvious duplicate,
  but it MUST gate only the durable write and MUST NOT gate delivery.
- **NR-005**: Each channel MUST be idempotent on the notification's idempotency key, so that a
  re-drive after a crash collapses to a single send.
- **NR-006**: The system MUST treat a suppressed address and an unresolvable address as **terminal**
  outcomes, not retryable failures, and MUST record which terminal outcome occurred.
- **NR-007**: The system MUST retry a transient delivery failure under exponential backoff with full
  jitter, and after a configured attempt ceiling MUST park the delivery in a durable dead-letter
  record with its last error, emitting an alarm. A delivery MUST NOT be dropped, and MUST NOT retry
  indefinitely.

### Isolation

- **NR-008**: The system MUST scope every notification to its recipient within its tenant within its
  realm, enforced **at the data layer**. A notification MUST NEVER be readable or deliverable to
  another recipient, another tenant, or another realm, regardless of the caller's privilege.
- **NR-009**: Recipient and tenant identity MUST be authoritative from the trusted producer and MUST
  NEVER be derived from notification content.
- **NR-010**: The realm MUST participate in the idempotency key space, so that two products sharing
  one deployment cannot collide.

### Recipient control

- **NR-011**: The system MUST let a recipient enable or disable delivery per `(topic, channel)`
  independently. An absent preference MUST fall back to the topic's registered default.
- **NR-012**: Disabling every channel for a topic MUST stop all delivery for it while still
  persisting the record, for deduplication and audit.
- **NR-013**: The system MUST support quiet hours and a digest cadence per recipient. Quiet hours
  MUST defer `info` and `warning` notifications and MUST NOT defer `critical` ones.
- **NR-014**: The system MUST coalesce a burst of same-`(recipient, topic)` notifications into a
  single digest per the recipient's cadence, rather than one delivery per event, while still
  persisting every underlying record.
- **NR-015**: Every non-essential notification delivered over a channel that supports it MUST carry
  an unsubscribe affordance that disables that `(topic, channel)` pair. Topics registered as
  essential MUST be exempt and MUST NOT be digestible.

### Extensibility

- **NR-016**: Adding a delivery channel MUST require only implementing the channel interface and
  registering it. It MUST NOT require a change to fan-out, persistence, preferences, deduplication,
  retention, or the schema.
- **NR-017**: Adding a notification topic MUST be a registration carrying its default channels,
  default priority, template reference and essential flag. It MUST NOT require a schema migration.
- **NR-018**: Re-anchoring what a recipient or tenant *is* MUST be a host binding change, not a
  change to any engine signature or table shape.
- **NR-019**: Copy, localization and branding MUST be reachable only through the template seam. The
  engine MUST NOT contain notification copy.

### Scale and operations

- **NR-020**: Audience expansion for a broadcast MUST run off the request path and MUST be paged, so
  that a large tenant neither blocks nor times out the caller. Per-recipient idempotency keys MUST
  derive deterministically from the broadcast key.
- **NR-021**: Pending deliveries MUST be partitioned by a stable shard derived from the recipient, so
  that N drainers scale without double-delivery. The shard count MUST be fixed for a deployment's
  life.
- **NR-022**: The system MUST bound storage growth with a documented retention policy for read
  notifications and for dead letters, implemented so that expiry is a metadata operation rather than
  a mass row purge.
- **NR-023**: The system MUST enforce a per-`(realm, tenant, channel)` delivery quota, defaulting to
  deferring rather than discarding when a budget is exhausted.
- **NR-024**: The unread count MUST be derivable from the durable store at any time. A real-time
  push channel MUST be treated as an at-most-once optimization and MUST NOT be the source of truth.

### Deployment

- **NR-025**: The engine MUST be adoptable both as an in-process library and as a standalone service
  behind a typed RPC facade, with identical semantics and no change to producer code beyond the
  construction site.
- **NR-026**: The engine MUST NOT import host code. Its schema and configuration MUST travel with it,
  and this MUST be enforced by a lint gate rather than by convention.

---

## Success criteria

- **NS-001**: **100% recipient-scoping correctness.** Across all generated notifications, none is
  ever readable by or delivered to any recipient other than its intended one, nor across tenants or
  realms. Any violation is a release blocker.
- **NS-002**: **Exactly-once per recipient.** A triggering event delivered more than once yields at
  most one persisted record and at most one delivery per channel, in 100% of duplicate cases.
- **NS-003**: **No delivery lost to a crash.** For any crash point between accepting a notification
  and completing delivery, every enabled channel is eventually delivered or terminally recorded.
  Nothing is silently absent.
- **NS-004**: An in-app notification reaches a connected recipient in under 5 seconds at p95, and the
  unread count reflects it without a page reload.
- **NS-005**: A burst of N same-`(recipient, topic)` notifications inside a digest window produces
  exactly one delivery on each digestible channel, and N persisted records.
- **NS-006**: Poison deliveries terminate. No delivery exceeds its attempt ceiling, and every
  terminal delivery is inspectable with its cause and replayable.
- **NS-007**: A tenant exhausting its channel quota does not reduce the delivery rate of any other
  tenant.
- **NS-008**: Adding a channel touches only new files plus one registration line. Demonstrated by
  adding a third channel with no diff to fan-out, persistence, or preferences.
- **NS-009**: The engine builds and its tests pass with no host present, enforced in CI.

---

## Edge cases

| Case | Required behaviour |
|---|---|
| Producer retries after a timeout, unsure whether the first call landed | Second call reports a replay; no duplicate record, no duplicate delivery (NR-002) |
| Crash after commit, before any delivery | Dispatcher finds committed pending deliveries and drives them (NR-003, NR-004) |
| Crash after delivering channel A, before channel B | B is re-driven; A's re-drive collapses to one send via channel idempotency (NR-005) |
| Address hard-bounces | Recorded as a suppression; the delivery is terminal at attempt 1, not retried (NR-006, NR-013) |
| Recipient has no address for an enabled channel | Terminal, not an error and not a retry (NR-006) |
| Provider outage across all tenants | Deliveries retry under jittered backoff; the recovering provider is not hit by a synchronized herd (NR-007) |
| 10,000 notifications for one recipient in one minute | One digest per digestible channel; 10,000 records persisted (NR-014, NS-005) |
| Broadcast to a 50,000-member tenant | Caller returns promptly; expansion pages off the request path; a retried broadcast does not double-notify (NR-020) |
| Two products share one deployment and reuse an idempotency key | Both deliver; the realm separates the key spaces (NR-010) |
| Quiet hours active, `critical` notification arrives | Delivered immediately; the override is recorded (NR-013) |
| Real-time push channel misses a message | Recipient's badge is correct on next reconnect, recomputed from the store (NR-024) |
| Retention runs while a dead-lettered delivery references an aged partition | Retention succeeds; it is not blocked by the pending delivery ([D6](design-decisions.md#d6)) |
