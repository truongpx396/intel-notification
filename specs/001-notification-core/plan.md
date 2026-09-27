# Implementation plan

**Spec**: [spec.md](spec.md) · **Contracts**: [contracts/](contracts/) ·
**Build order**: [tasks.md](tasks.md)

## Technical context

| | |
|---|---|
| **Language** | Go 1.24 |
| **Module** | `github.com/truongpx396/intel-notification` |
| **Stores** | PostgreSQL 16 (durable), Redis 7 (pre-check, pub/sub, quota counters) |
| **Bus** | A `Bus` port. Default Redis Streams; NATS JetStream as a swap |
| **Transports** | in-process (library) and gRPC (service) — both satisfy `Notifier` |
| **Testing** | `testing` + table-driven contract suites; Testcontainers for store-backed suites behind `//go:build integration` |
| **Lint** | `go-arch-lint` for the component graph, `golangci-lint` with `depguard` for banned imports |

## Architecture

Ports and adapters, with the dependency rule `adapters → app → ports → domain` enforced in CI. The
layout is specified in
[notification-ports.md § Extraction-ready code organization](contracts/notification-ports.md#extraction-ready-code-organization);
this repository *is* the extracted module, so its root is that tree:

```text
domain/      pure types + invariants. Imports nothing outside the module
ports/       driving.go (Notifier, Dispatcher) · driven.go (Channel, Store, Prefs,
             Renderer, AddressBook, TopicRegistry, AudienceResolver, Bus, Clock, IDs)
app/         use-cases: notifier · broadcast · dispatcher · digest · retention
adapters/
  driven/    postgres/ · redis/ · bus/ · channel/{inapp,email,slack,webhook,sms,push}
  driving/   inprocess/ · grpcserver/ · grpcclient/
migrations/  owns the schema
config.go    the entire configuration surface
```

The litmus test the originating design set — *"could I move this out and have it compile with zero
edits?"* — has been run, which is why this repository exists. It now runs in the other direction: CI
asserts the module builds and tests green with **no host present** (NS-009).

## Phasing

The build order is chosen so that each stage is independently verifiable and nothing depends on a
later stage.

| Stage | Delivers | Verifiable by |
|---|---|---|
| **1. Schema** | migrations + `make verify-schema` assertions | The seven schema assertions pass against PostgreSQL 16 |
| **2. Domain + ports** | types, interfaces, config validation | Compiles; `Config.Validate` table tests |
| **3. Store adapter** | inbox + guard + outbox in one transaction | `StoreContract`, incl. the crash-point tests |
| **4. Notifier** | preferences → persist → enqueue | `NotifierContract`: replay, leak, preference gating |
| **5. Channels** | in-app + email reference implementations | `ChannelContract` against both |
| **6. Dispatcher** | claim → render → resolve → deliver → outcome | Backoff, terminal outcomes, dead-lettering |
| **7. Digest + quotas** | coalescing windows, per-tenant budgets | Burst test yields one delivery, N rows |
| **8. Retention** | partition retirement for inbox and dead letters | Retention runs with a pending delivery outstanding |
| **9. Broadcast** | paged audience expansion off the request path | Large-tenant test; retried broadcast does not double-notify |
| **10. Transports** | gRPC server + client, both as `Notifier` | Same contract suite through the wire |
| **11. Boundary gates** | `go-arch-lint` + `depguard` + no-host build | CI fails on an inbound host import |

Stages 1–6 are the minimum that satisfies NS-001 through NS-003 — the three criteria that are release
blockers. Stages 7–11 are required for the stated scope but do not gate correctness of a single
delivery.

## Risks

| Risk | Mitigation |
|---|---|
| The outbox drain becomes the bottleneck under fan-out | Shard from the start ([D11](design-decisions.md#d11)); the claim rides a partial index; the drain is a queue group, not a singleton |
| RLS is bypassed by the worker's own connection | `FORCE ROW LEVEL SECURITY`, plus a contract test that asserts a leak is impossible *as the owner role* |
| A channel implementation is not idempotent, silently | `ChannelContract` includes a re-drive assertion every channel must pass before registration |
| Digest windows leak, growing unbounded | Windows carry `flush_at`; the flush tick is a scheduled single-owner job, and an unflushed window past its deadline is an alarm |
| `Shards` changed in production | Documented as requiring a drain; `Config.Validate` records the value and the operations runbook makes the ordering explicit |
| Two products adopt one deployment without setting `Realm` | `Config.Validate` rejects an empty `Realm` — it has no safe default |

## Out of scope

Everything in [ROADMAP.md § What this is not](../../ROADMAP.md#what-this-is-not). Most relevant here:
acknowledgement and escalation are [Phase 2](../002-escalation-workflows/), and authoring UI is
Phase 3 and undesigned.
