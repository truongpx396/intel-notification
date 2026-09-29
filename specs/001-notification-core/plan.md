# Implementation plan

**Spec**: [spec.md](spec.md) · **Contracts**: [contracts/](contracts/) ·
**Build order**: [tasks.md](tasks.md)

## Technical context

| | |
|---|---|
| **Language** | Go 1.26 (toolchain `go1.26.8`) |
| **Module** | `github.com/truongpx396/intel-notification` |
| **Stores** | PostgreSQL 16 (durable: inbox, queue, history, catalog, state transitions), Redis 7 (pre-check, live stream, quota counters — all reconstructible) |
| **Bus** | None required. Optional ingest adapter for NATS JetStream ([bus-subjects.md](contracts/bus-subjects.md)) |
| **Transports** | in-process (library), REST + SSE (recipients), gRPC (producers, catalog) |
| **Testing** | `make verify-schema` for schema guarantees; table-driven unit and contract suites; Testcontainers (v0.44) for store-backed suites behind `//go:build integration`, one cloned database per test; provider sandboxes behind `//go:build provider` |
| **Lint** | `go-arch-lint` for the component graph, `golangci-lint` with `depguard` for banned imports |

## Architecture

Ports and adapters, with the dependency rule `adapters → app → ports → domain` enforced in CI. The
layout is specified in [notification-ports.md § Module layout](contracts/notification-ports.md#module-layout);
this repository *is* the extracted module, so its root is that tree:

```text
domain/      pure types, canonical encoding, derived keys, backoff, shard. Imports nothing
ports/       driving.go (Notifier, Inbox, Admin, Dispatcher, Maintenance) · driven.go (Channel, Store,
             InboxStore, MaintenanceStore, PreferenceStore, TopicRegistry, TemplateRenderer,
             AddressBook, AudienceResolver, SuppressionStore, QuotaCounter, PreCheck,
             StreamPublisher, Clock, IDSource)
app/         use-cases: notifier · planner · broadcast · dispatcher · digest · quota · inbox ·
             maintenance · erasure
adapters/
  driven/    postgres/ · redis/ · channel/{inapp,email,sms,push,slack,webhook,failover}
  driving/   inprocess/ · httpapi/ · grpcserver/ · grpcclient/ · natsingest/
api/         notifyv1 (generated)
cmd/notifyd  the service binary
migrations/  owns the schema, including the state-transition functions
config.go    the entire configuration surface
```

The litmus test the originating design set — *"could I move this out and have it compile with zero
edits?"* — has been run, which is why this repository exists. It now runs in the other direction: CI
asserts the module builds and tests green with **no host present** (NS-009).

## Capacity model

The design envelope for **one PostgreSQL primary**. These are targets, not measurements: nothing is
built yet. NS-010 gates the first production-ready release on a load test that meets every row.

**Reference configuration:** PostgreSQL 16 on 16 vCPU / 64 GiB / NVMe (≥ 16k IOPS),
`synchronous_commit = on`; Redis 7, single node; four worker replicas; two channels per notification on
average, one address per channel.

| Dimension | Target | What it exercises |
|---|---|---|
| Sustained `Notify` | 1,000 / s (86 M / day) | One transaction: guard + inbox row + ~2 queue rows |
| Burst `Notify` | 5,000 / s for 10 minutes | Group commit; backlog absorbed by the queue |
| Sustained delivery | 3,000 deliveries / s across workers | Claim (batched) + finish (queue → history) per delivery |
| Backlog | Claim p95 < 20 ms with 1 M pending rows | The claim index and 1% autovacuum threshold |
| Broadcast | 100,000 recipients enqueued in < 2 minutes, alongside sustained load | Paged expansion, a page per transaction |
| Inbox list | p95 < 50 ms for a recipient with 10,000 notifications | `notifications_recipient_idx` |
| Unread count | p95 < 5 ms | Bounded count on the partial unread index |
| Reconnect storm | 50,000 clients reconnecting within 60 s | Bounded count; relay backoff with jitter |
| In-app end to end | p95 < 5 s (NS-004) | Commit → in-memory wakeup or poll → claim → nudge → relay |
| Fairness | NS-007: an unexhausted tenant's p95 due-to-claimed within 10% while another is exhausted | `Queue.DeferTenantChannel` |

**Sizing.** Steady-state row counts follow from rate × retention, and an operator can compute them
before adopting:

| Table | Rows ≈ |
|---|---|
| `notifications` | notify rate × `RetentionWindow` |
| `notify_idem` | notify rate × `IdempotencyWindow` — at 1,000 / s and 7 days, about 600 M across 16 hash partitions. Shorten the window if that is too large; 24 hours is the floor |
| `notification_outbox` | the backlog only |
| `notification_deliveries` | delivery rate × `DeliveryLogRetention` |

**Past one primary.** Every table is keyed by realm and tenant except the catalog
(`notification_topics`, `notification_templates`, `channel_providers` — per realm) and
`channel_suppressions` (per realm and channel). So the path is:

1. **Separate the tiers** — dispatcher on its own nodes, inbox reads on replicas where a slightly stale
   list is acceptable (the badge stays on the primary).
2. **Route by tenant.** A tenant-routing `Store` maps `hash(realm, tenant)` to one of N PostgreSQL
   clusters, each an unmodified copy of this schema. The catalog and suppressions are replicated to
   every cluster (they are small and write-rarely), or distributed as reference tables under Citus with
   `(realm, tenant_id)` as the distribution key.

No table shape changes on either step. That is the benefit of keying everything by tenant from the
start, and the reason it is a requirement rather than a convention.

## Phasing

The build order is chosen so that each stage is independently verifiable and nothing depends on a
later stage.

| Stage | Delivers | Verifiable by |
|---|---|---|
| **1. Schema** | migrations, `make verify-schema` | The schema tests pass against PostgreSQL 16, as the table owner |
| **2. Domain + ports** | types, canonical encoding, derived keys, backoff, config validation | Frozen test vectors shared with the SQL; `Config.Validate` table tests |
| **3. Store adapter** | the queue's transitions (done), persist-and-enqueue, inbox reads | The queue's integration suite (done); `StoreContract`, including every crash point |
| **4. Notifier** | planner, `Notify`, `NotifyTx`, pre-check, `Cancel`, `Status` | `NotifierContract` |
| **5. Channels** | in-app + email reference implementations, failover composite | `ChannelContract` against both; provider sandbox for email |
| **6. Dispatcher** | the nine-step algorithm | `DispatcherContract` |
| **7. Digest, quotas, fairness** | windows, budgets, backlog deferral | Burst yields `ceil(N / DigestMax)`; NS-007 measured |
| **8. Maintenance** | retention, partition provisioning with scope, idempotency and digest expiry, erasure | Retention with a pending delivery; TEST 5 green after provisioning |
| **9. Broadcast** | durable, paged, resumable expansion | Crash mid-broadcast resumes; nobody notified twice |
| **10. Transports** | in-process, REST + SSE relay, gRPC server and client | `NotifierContract` over the wire; `StreamIsolationContract` |
| **11. Service mode** | data-backed ports, `Catalog`, `Directory` client, recipient tokens, realm bindings | NS-011 with a non-Go producer |
| **12. Boundary gates** | `go-arch-lint` + `depguard` + no-host build | CI fails on an inbound host import |
| **13. Load test** | the capacity model above, scripted | NS-010 |

Stages 1–6 are the minimum that satisfies NS-001 through NS-003 — the three criteria that are release
blockers. Stages 7–13 are required for the stated scope.

## Risks

| Risk | Mitigation |
|---|---|
| The queue becomes the bottleneck under fan-out | The queue holds pending work only ([D20](design-decisions.md#d20)); `SKIP LOCKED` claims spread across shards; 1% autovacuum threshold; history on a partitioned log |
| RLS bypassed by the worker's own connection | Forced on every scoped table and partition; TEST 4 runs as the owner; TEST 5 fails on an unscoped partition — mutation-checked |
| A new partition created without the scope policy | Provisioning calls `notify_apply_recipient_scope()`; TEST 5 and the `notify.partition.unscoped` alarm |
| A channel silently not idempotent | Channels declare `Dedup`; `ChannelContract` holds them to it; `DedupNone` is reported, never hidden ([D32](design-decisions.md#d32)) |
| Lease shorter than a slow provider call | `ClaimLease` validated against channel timeouts; a lost lease is fenced and counted (`notify.lease.lost`) |
| `LISTEN/NOTIFY` serializes commits at high write rates | Off by default; polling plus in-memory wakeup meets NS-004 without it ([D22](design-decisions.md#d22)) |
| `notify_idem` larger than expected | Sizing formula published; window configurable down to 24 hours |
| Digest windows leak | `flush_at` plus a leased flush job; `notify.digest.overdue` alarm; finished windows expire |
| Two products adopt one deployment without setting `Realm` | `Config.Validate` rejects an empty or malformed `Realm`; service mode binds realms to producer principals |

## Out of scope

Everything in [ROADMAP.md § What this is not](../../ROADMAP.md#what-this-is-not). Most relevant here:
acknowledgement and escalation are [Phase 2](../002-escalation-workflows/), and a template authoring
UI is Phase 3 and undesigned.
