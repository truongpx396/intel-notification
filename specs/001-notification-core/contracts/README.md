# Contracts

The normative interfaces. Where a contract and prose elsewhere disagree, the contract wins.

| Contract | What it fixes | Consumers |
|---|---|---|
| [notification-ports.md](./notification-ports.md) | **The core.** Domain types, all driving and driven ports, the 12 invariants, the delivery-durability knob, the contract-test suites, the hexagonal layout, the lint gates, and the gRPC facade | Everything |
| [bus-subjects.md](./bus-subjects.md) | Subject names, payloads, delivery semantics and queue-group rules for the message bus | Producers, dispatcher, schedulers |
| [rest-api.md](./rest-api.md) | The recipient-facing HTTP surface: inbox, unread count, read state, preferences, live stream, unsubscribe, provider webhooks | Host BFF, web/mobile clients |

The gRPC producer surface is **not** a separate file — it lives in
[notification-ports.md](./notification-ports.md#service-surface-notificationservice-grpc-contract-locked),
because it is the same `Notifier` port over the wire and splitting it invites the two from drifting.

## Reading order

1. **notification-ports.md** — the domain types and the `Channel` / `Topic` / `Recipient` seams.
   The invariants section is the part to read twice; everything else serves it.
2. **[data-model.md](../data-model.md)** — how the invariants land in tables.
3. **[research.md](../research.md)** — the originating research behind the outbox and DLQ decisions.
4. **[design-decisions.md](../design-decisions.md)** — why the non-obvious choices are what they are.
5. **bus-subjects.md** / **rest-api.md** — only when integrating that boundary.

## Stability

| | |
|---|---|
| **Frozen** | The domain types, `Notifier`, `Channel`, `Store`, and the 12 invariants. Changing these breaks every host. |
| **Additive** | New driven ports, new channel kinds, new topics, new config fields with defaults. |
| **Internal** | Adapter implementations, SQL, Redis key layout, shard arithmetic within a major version. |

A host depends on the ports. A host that depends on an adapter has taken on a private interface and
will be broken without notice — the lint rules in
[notification-ports.md](./notification-ports.md#machine-enforced-boundary-lint--config) exist to make
that mistake fail in CI rather than at upgrade time.
