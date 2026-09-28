# Contracts

The normative interfaces. Where a contract and prose elsewhere disagree, the contract wins.

| Contract | What it fixes | Consumers |
|---|---|---|
| [notification-ports.md](./notification-ports.md) | **The core.** Domain types, every driving and driven port, the dispatcher algorithm, the 16 invariants, the contract-test suites, the module layout, the configuration surface, and the gRPC facade (producer, catalog and directory services) | Everything |
| [rest-api.md](./rest-api.md) | The recipient-facing HTTP surface: authentication, inbox and its three states, bounded unread count, live stream, preferences, RFC 8058 unsubscribe, tenant administration, operator routes, provider callbacks | Host BFF, web/mobile clients, operators |
| [bus-subjects.md](./bus-subjects.md) | The **optional** ingest subject, its durability requirement, and what moved off the bus | Producers that publish rather than call |

The gRPC surface is **not** a separate file — it lives in
[notification-ports.md](./notification-ports.md#service-surface-grpc-contract-locked), because it is
the same `Notifier` port over the wire and splitting it invites the two to drift. The schema's state
transitions are contract too: they live in [`migrations/0004_state_transitions.sql`](../../../migrations/0004_state_transitions.sql)
and are asserted by `make verify-schema`.

## Reading order

1. **notification-ports.md** — the domain types and the seams. The invariants section is the part to
   read twice; everything else serves it.
2. **[data-model.md](../data-model.md)** — how the invariants land in tables, keys and functions.
3. **[design-decisions.md](../design-decisions.md)** — why the non-obvious choices are what they are.
   D16–D24 are the review's correctness fixes and are worth reading before the rest.
4. **[research.md](../research.md)** — the originating research behind the outbox and dead-letter
   decisions.
5. **rest-api.md** / **bus-subjects.md** — only when integrating that boundary.

## Stability

| | |
|---|---|
| **Frozen** | The domain types, `Notifier`, `Channel`, `ChannelCapabilities`, `Store`, the 16 invariants, and the canonical encoding. Changing these breaks every host |
| **Additive** | New driven ports, new channel kinds, new topics, new outcome values, new config fields with defaults |
| **Internal** | Adapter implementations, SQL other than the `0004` functions' contracts, Redis key layout, shard arithmetic within a major version |

A host depends on the ports. A host that depends on an adapter has taken on a private interface and
will be broken without notice — the lint rules in [`.golangci.yml`](../../../.golangci.yml) and
[`.go-arch-lint.yml`](../../../.go-arch-lint.yml) exist to make that mistake fail in CI rather than at
upgrade time.
