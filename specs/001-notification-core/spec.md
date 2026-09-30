# Specification: Notification core

**Status**: normative, implementation started — [tasks.md](tasks.md) records what is built.
**Contracts**: [contracts/](contracts/) · **Data model**: [data-model.md](data-model.md) ·
**Decisions**: [design-decisions.md](design-decisions.md) · **Build order**: [tasks.md](tasks.md)

This specification describes the engine as a product. Requirements carry `NR-0NN` ids and success
criteria `NS-0NN`; the mapping from the originating project's `FR-032`–`FR-039` / `SC-011`–`SC-013`
is recorded in [PROVENANCE.md](../../PROVENANCE.md). NR-027 onward, and the amended wording of
several earlier requirements, come from the architecture review recorded as D16–D37.

---

## Problem

A product needs to tell specific recipients about specific events, over whichever channels each
recipient chose, exactly once, without ever telling the wrong person. Doing that correctly requires
solving six problems that are easy to get subtly wrong and expensive to discover late:

1. **Fan-out that survives a crash.** Persisting a notification and delivering it are two stores, so
   they cannot share a transaction.
2. **Deduplication that does not eat deliveries.** An at-least-once bus redelivers; a naive guard
   makes the redelivery a no-op *including the parts that had not happened yet*.
3. **Isolation that is provable.** "A notification is only visible to its recipient" must be a
   property of the data layer — and of the live push path — not a discipline applied at every call
   site.
4. **Extensibility without touching fan-out.** Adding a channel or an event type must not mean
   editing the riskiest code in the system.
5. **Bounded growth.** An inbox, a delivery history and an idempotency record are append-mostly, and
   grow forever unless retention is designed into each.
6. **Fairness between tenants.** A shared queue drained oldest-first lets one tenant's burst delay
   everyone else's notifications.

## Actors

| Actor | Role |
|---|---|
| **Producer** | Any host subsystem that publishes a `Notification`. Thin by design: it builds the value and calls `Notify`. |
| **Recipient** | The opaque delivery subject — a user, an organization, a device, a Slack channel, an external contact. |
| **Tenant** | The host's isolation boundary containing recipients — a workspace, organization, account. |
| **Tenant administrator** | Sets a tenant's preference defaults and locks. |
| **Operator** | Runs the engine: shards, retention, dead-letter inspection, quotas, erasure. |
| **Channel** | A delivery target implementation. In-app and email are reference implementations, not the model. |

---

## Functional requirements

### Delivery core

- **NR-001**: The system MUST persist every notification as a durable, recipient-scoped record,
  regardless of which delivery channels are enabled. The record is both the inbox entry and the
  deduplication record.
- **NR-002**: Every notification MUST carry an idempotency key supplied by the producer. A replay of
  the same `(realm, tenant, recipient, idem_key)` within the idempotency window MUST produce no second
  record and no second delivery on any channel, and MUST report itself as a replay rather than as an
  error. The window MUST be configurable and MUST NOT be shorter than 24 hours.
- **NR-003**: The system MUST write the inbox record and every planned delivery — one pending delivery
  per enabled channel, plus any digest membership — in a **single transaction**. It MUST NOT perform a
  durable write followed by best-effort delivery attempts, and MUST NOT offer an in-line delivery mode
  that bypasses the pending-delivery record.
- **NR-004**: The system MUST drive channel delivery from the committed pending-delivery set, at least
  once, through a claim that is atomic under concurrent claimers, leased, and fenced so that a claimer
  that lost its lease cannot record an outcome. A fast pre-check MAY skip a database round-trip on an
  obvious duplicate; it MUST use the durable guard's key space, MUST be written only after commit, and
  MUST NOT gate delivery.
- **NR-005**: Each channel MUST be idempotent on the delivery's key — derived from the notification (or
  digest), the channel and the address — at the level the channel declares. The system MUST report each
  registered channel's declared level.
- **NR-006**: The system MUST treat a suppressed address and an unresolvable address as **terminal**
  outcomes, not retryable failures, and MUST record which occurred. The system MUST check suppressions
  before every delivery on every channel.
- **NR-007**: The system MUST retry a transient delivery failure under exponential backoff with full
  jitter, counting an attempt at each claim so that a delivery that crashes its worker still reaches the
  ceiling. After the ceiling, or on a permanent refusal, it MUST park the delivery in a durable
  dead-letter record with its reason and last error, emitting an alarm. Only genuine failures are dead
  letters. A delivery MUST NOT be dropped, and MUST NOT retry indefinitely.

### Isolation

- **NR-008**: The system MUST scope every notification to its recipient within its tenant within its
  realm, enforced **at the data layer** — on every recipient-scoped table and on every partition of one
  — and on the live push path. A notification MUST NEVER be readable or deliverable to another
  recipient, another tenant, or another realm, regardless of the caller's privilege.
- **NR-009**: Recipient and tenant identity MUST be authoritative from the trusted producer and MUST
  NEVER be derived from notification content. The realm MUST NEVER be taken from a request: it comes
  from configuration in library mode and from the authenticated producer in service mode.
- **NR-010**: The realm and the tenant MUST participate in the idempotency key space, so that two
  products sharing one deployment, or one user in two tenants, cannot collide. Every key derived from an
  identity MUST use an unambiguous encoding.

### Recipient control

- **NR-011**: The system MUST let a recipient enable or disable delivery per `(topic, channel)`
  independently. An absent preference MUST fall back to the tenant's default, then to the topic's
  registered default. A tenant default marked locked MUST override the recipient's choice.
- **NR-012**: Disabling every channel for a topic MUST stop all delivery for it while still persisting
  the record, for deduplication and audit. When no inbox-backed channel is enabled, the record MUST NOT
  appear in the inbox or the unread count.
- **NR-013**: The system MUST support quiet hours and a digest cadence per recipient. Quiet hours MUST
  defer `info` and `warning` notifications and MUST NOT defer `critical` ones; a critical delivery inside
  quiet hours MUST be recorded as an override.
- **NR-014**: The system MUST coalesce a burst of same-`(recipient, topic)` notifications into digests
  per the recipient's cadence, rather than one delivery per event, while still persisting every
  underlying record. No digest may exceed the configured maximum member count. `critical` and essential
  notifications MUST NOT be digested.
- **NR-015**: Every non-essential notification delivered over a channel that supports it MUST carry an
  unsubscribe affordance that disables that `(topic, channel)` pair. For email it MUST implement RFC 8058
  one-click unsubscribe, and a `GET` of the link MUST NOT change state. Topics registered as essential
  MUST be exempt and MUST NOT be digestible.

### Extensibility

- **NR-016**: Adding a delivery channel MUST require only implementing the channel interface and
  registering it — or, in service mode, configuring a provider. It MUST NOT require a change to fan-out,
  persistence, preferences, deduplication, retention, or the schema.
- **NR-017**: Adding a notification topic MUST be a registration carrying its default channels, fallback
  chain, default priority, template reference and essential flag. It MUST NOT require a schema migration.
- **NR-018**: Re-anchoring what a recipient or tenant *is* MUST be a host binding change, not a change to
  any engine signature or table shape.
- **NR-019**: Copy, localization and branding MUST be reachable only through the template seam. Producers
  MUST be able to send template data rather than copy; producer-supplied copy MAY serve as a fallback
  when no template exists. The inbox MUST be renderable in the viewer's locale. The engine MUST NOT
  contain notification copy.

### Scale and operations

- **NR-020**: Audience expansion for a broadcast MUST run off the request path from a durable record, in
  pages, resumable at the next page after a crash, so that a large tenant neither blocks nor times out the
  caller. Per-recipient idempotency keys MUST derive deterministically from the broadcast key.
- **NR-021**: Pending deliveries MUST carry a stable shard derived from the identity, used to spread
  claimers. Correctness MUST NOT depend on shard ownership, and the shard count MUST be changeable while
  the system runs.
- **NR-022**: The system MUST bound storage growth for every table that grows: inbox records (retired by
  age, read or unread), idempotency records (the window), delivery history, dead letters, digest windows
  and broadcast records. The pending-delivery set MUST hold pending work only. Retiring inbox, history
  and dead-letter data MUST be a metadata operation rather than a mass row purge.
- **NR-023**: The system MUST enforce a per-`(realm, tenant, channel)` delivery quota with a realm-wide
  default, deferring rather than discarding by default. When a tenant's budget is exhausted, its pending
  backlog on that channel MUST leave the claim range, so that it does not occupy drainers.
- **NR-024**: The unread count MUST be derivable from the durable store at any time, and bounded so that
  recomputing it is cheap. A real-time push channel MUST be treated as an at-most-once optimization,
  MUST carry no notification content or count, and MUST NOT be the source of truth.

### Deployment

- **NR-025**: The engine MUST be adoptable both as an in-process library and as a standalone service
  behind a typed RPC facade, with identical semantics and no change to producer code beyond the
  construction site. The standalone service MUST NOT require host code in its process: topics,
  templates, addresses and channel providers MUST be data it owns and producers manage through its API;
  audiences MUST be resolvable from an inline list or a host callback; recipients MUST authenticate with
  a host-signed token.
- **NR-026**: The engine MUST NOT import host code. Its schema and configuration MUST travel with it, and
  this MUST be enforced by a lint gate rather than by convention.

### Delivery lifecycle

- **NR-027**: In library mode against PostgreSQL, the system MUST let a producer persist and enqueue a
  notification inside the producer's own transaction, so that the notification commits or rolls back with
  the change that caused it.
- **NR-028**: The system MUST support several addresses per `(recipient, channel)` — every device of a
  recipient — with independent delivery state per address.
- **NR-029**: The system MUST support a scheduled send time and an expiry. A notification past its expiry
  MUST NOT be delivered, and MUST be recorded as expired.
- **NR-030**: The system MUST let a producer cancel a notification. Pending deliveries MUST stop; a
  delivery already in progress MUST be reported as such and MUST NOT be recorded as canceled.
- **NR-031**: A topic MUST be able to declare an ordered fallback chain of channels, each tried only when
  the previous one ended undelivered.
- **NR-032**: The system MUST answer, per notification, the state of every delivery by channel and
  address, and MUST correlate a provider's delivery callbacks to the delivery they concern.
- **NR-033**: The inbox MUST record seen, read and archived as independent states.
- **NR-034**: The system MUST erase a recipient's personal data from every table on request, in one
  operation, recording that the erasure happened without retaining the identity. Address suppressions
  MUST survive erasure and MUST NOT store the address in plaintext.
- **NR-035**: A notification MUST count as accepted when the durable store commits it. A message bus used
  for ingest MUST acknowledge only after replication.
- **NR-036**: The design MUST state its capacity envelope for a single primary database and the path
  beyond it.
- **NR-037**: The system MUST report, as warnings and never as failures, the database conditions its
  queue depends on, so that an operator learns of them before their effect: checkpoint sizing against
  the observed WAL rate, the connected role's transaction timeouts, and any open transaction old enough
  to pin vacuum.

---

## Success criteria

- **NS-001**: **100% recipient-scoping correctness.** Across all generated notifications, none is ever
  readable by or delivered to any recipient other than its intended one, nor across tenants or realms —
  including on the live stream and through direct partition access. Any violation is a release blocker.
- **NS-002**: **Exactly-once per recipient, stated per channel.** Within the idempotency window, a
  triggering event delivered more than once yields at most one persisted record in 100% of duplicate
  cases. Per channel, it yields at most one provider submission for channels declaring provider or local
  deduplication; for channels declaring none, a duplicate is possible only when a worker crashes between
  the provider accepting a send and the outcome being recorded.
- **NS-003**: **No delivery lost to a crash.** For any crash point after the accepting commit, every
  enabled channel is eventually delivered or terminally recorded. Nothing is silently absent.
- **NS-004**: An in-app notification reaches a connected recipient in under 5 seconds at p95, and the
  unread count reflects it without a page reload.
- **NS-005**: A burst of N same-`(recipient, topic)` notifications inside a digest window produces
  `ceil(N / DigestMax)` deliveries on each digestible channel — exactly one when N ≤ `DigestMax` — and N
  persisted records.
- **NS-006**: Poison deliveries terminate. No delivery exceeds its attempt ceiling, including one that
  crashes its worker on every attempt, and every dead letter is inspectable with its reason and
  replayable.
- **NS-007**: A tenant exhausting its channel quota does not reduce the delivery rate of any other
  tenant: with one tenant exhausted, another tenant's p95 time from due to claimed stays within 10% of
  its value with no tenant exhausted.
- **NS-008**: Adding a channel touches only new files plus one registration line. Demonstrated by adding
  a third channel with no diff to fan-out, persistence, or preferences.
- **NS-009**: The engine builds and its tests pass with no host present, enforced in CI.
- **NS-010**: The load test in [plan.md § Capacity model](plan.md#capacity-model) meets every stated
  target on the reference configuration. It gates the first release described as production-ready.
- **NS-011**: A producer written in a language other than Go registers a topic and a template, stores a
  recipient's addresses and sends a notification delivered on two channels, using only the service's
  API, with no host code in the service process.
- **NS-012**: After erasing a recipient, no table holds a row carrying that recipient, and a suppression
  recorded for one of its addresses still prevents delivery to that address.

---

## Edge cases

| Case | Required behaviour |
|---|---|
| Producer retries after a timeout, unsure whether the first call landed | Second call reports a replay; no duplicate record, no duplicate delivery (NR-002) |
| The transaction fails after the pre-check was consulted | The retry is applied normally; the pre-check was never written (NR-004, [D18](design-decisions.md#d18)) |
| Host commits its change, then crashes before calling `Notify` | Avoided by `NotifyTx`: the notification commits with the change or not at all (NR-027) |
| Crash after commit, before any delivery | The dispatcher finds committed pending deliveries and drives them (NR-003, NR-004) |
| Worker dies holding claimed deliveries | They become claimable when the lease lapses; the dead worker's late writes are discarded (NR-004, [D19](design-decisions.md#d19)) |
| Crash after delivering channel A, before channel B | B is re-driven; A's re-drive collapses to one send at A's declared level (NR-005) |
| A payload crashes the worker on every attempt | Each claim counts as an attempt; the delivery dead-letters at the ceiling (NR-007) |
| Address hard-bounces | Recorded as a suppression; terminal at attempt 1, not retried, not a dead letter (NR-006) |
| Recipient has no address for an enabled channel | Terminal `no_address`; the next fallback channel, if any, is enqueued (NR-006, NR-031) |
| Recipient has three phones | Three deliveries; one failing retries alone (NR-028) |
| Provider outage across all tenants | Deliveries retry under jittered backoff; the recovering provider is not hit by a synchronized herd (NR-007) |
| 10,000 notifications for one recipient in one minute, `DigestMax` 100 | 100 digests per digestible channel; 10,000 records persisted (NR-014, NS-005) |
| Broadcast to a 50,000-member tenant; the worker crashes at member 30,000 | Caller returned promptly; expansion resumes at the next page; nobody is notified twice (NR-020) |
| Two products share one deployment and reuse an idempotency key | Both deliver; the realm separates the key spaces (NR-010) |
| One user in two workspaces receives the same periodic key | Both deliver; the tenant separates the key spaces (NR-010) |
| Two tenants each have a user `u1` connected to the live stream | Each sees only its own notifications (NR-008, NS-001) |
| Quiet hours active, `critical` notification arrives | Delivered immediately; the override is recorded (NR-013) |
| "Meeting in 5 minutes" push still retrying after the meeting | Terminal `expired`, never sent late (NR-029) |
| Producer cancels a notification while one channel is mid-send | The pending channel is canceled; the in-flight one is reported as in flight (NR-030) |
| One tenant exhausts its email quota with a large backlog | Its backlog leaves the claim range; other tenants' email is unaffected (NR-023, NS-007) |
| Shard count lowered in production | Rows in retired shards are folded into live ones online; nothing is double-delivered (NR-021) |
| Real-time push channel misses a message | The recipient's badge is correct on the next reconnect, recomputed from the store (NR-024) |
| Retention runs while a dead-lettered delivery references an aged partition | Retention succeeds; nothing references a partition by foreign key ([D6](design-decisions.md#d6)) |
| An email security scanner fetches the unsubscribe link | Nothing changes; only the one-click `POST` unsubscribes (NR-015) |
| A recipient who complained is erased | Their data is gone; the suppression survives, so they are not mailed again (NR-034) |
