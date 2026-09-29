# Tasks

Dependency-ordered build order for Phase 1. `[P]` marks tasks that can run in parallel with their
siblings. Each task names the requirement it satisfies, so a task with no requirement is scope creep.

Stages map to [plan.md § Phasing](plan.md#phasing). Stages 1–6 are the release-blocking core.

---

## Stage 1 — Schema

- [x] **T001** `migrations/0001_notification_core.sql`: inbox (partitioned, RLS forced on the parent
      and every partition), `notify_idem` (tenant in the key, hash-partitioned), the queue, the delivery
      history, broadcasts, preferences (recipient and tenant), schedules, job leases (NR-001, NR-003,
      NR-008, NR-010, NR-020, [D2](design-decisions.md#d2), [D17](design-decisions.md#d17),
      [D20](design-decisions.md#d20))
- [x] **T002** `migrations/0002_channels_delivery.sql`: hashed per-channel suppressions, dead letters
      (genuine failures only, partitioned), sealing digest windows, quotas with a realm default (NR-006,
      NR-007, NR-014, NR-023)
- [x] **T003** `migrations/0003_catalog_compliance.sql`: topics, versioned templates, recipient addresses
      (RLS), channel providers (credentials by reference), erasure record (NR-025, NR-028, NR-034)
- [x] **T004** `migrations/migrations.go`: embed the schema (`migrations.FS`) so hosts, the service and
      the integration tests apply the same files. *(Replaces the PL/pgSQL state-transition functions,
      which moved into the Go adapter — [D37](design-decisions.md#d37).)*
- [x] **T005** `scripts/verify-schema.sql` + `make verify-schema`: the schema tests, run as the table
      owner, each raising on failure, including the proof that the inherited constraint cannot be
      constructed and a mutation check on the partition-scope test (NS-001, NS-002)
- [x] **T006** Partition provisioning: `Maintenance.EnsurePartitions` creates each missing month of
      `notifications`, `notification_deliveries` and `dead_letters` and scopes every new `notifications`
      partition; `TestEnsurePartitions` (NR-008, NR-022). *Still to do: the `notify.partition.missing`
      and `notify.partition.unscoped` alarms, with T038.*

## Stage 2 — Domain and ports

- [x] **T007** `domain/identity.go`: `Realm`, `Tenant`, `Recipient`, `Identity`, `Canonical`, tested
      against a frozen vector computed independently of the code ([D18](design-decisions.md#d18))
- [x] **T008** `domain/keys.go`: pre-check key, stream key, delivery idempotency key, address key,
      broadcast member key, suppression and subject hashes — each with a frozen vector
      ([D16](design-decisions.md#d16), [D25](design-decisions.md#d25))
- [ ] **T009** [P] `domain/`: `Notification`, `TopicDef`, `DeliverySchedule`, `DeliveryPlan`, `Receipt`,
      `Delivery`, `DeliveryResult`, `UnreadCount`, broadcast and status types. *(Done: `Address`,
      `OutboxEntry`, `Claim`, `TerminalOutcome`, `Disposition` and the dead-letter/fallback policy,
      `Finish`, `Binding`, `CancelReceipt`, `DigestMember`.)* Imports nothing outside the module
- [x] **T010** [P] `domain/shard.go`: `ShardFor(identity, n)` over the canonical encoding, stable against
      a frozen vector table ([D11](design-decisions.md#d11))
- [x] **T011** [P] `domain/backoff.go`: full jitter with a `RetryAfter` floor, and a test that the
      distribution spreads rather than synchronizes ([D10](design-decisions.md#d10))
- [ ] **T012** [P] `ports/driving.go`: `Notifier`, `Inbox`, `Admin`, `Dispatcher`, `Jobs`
- [ ] **T013** [P] `ports/driven.go`: the remaining driven ports in the contract (NR-016, NR-017,
      [D7](design-decisions.md#d7)). *(Done: `Queue`, `Digests`, `Maintenance`.)*
- [ ] **T014** `config.go`: `Config`, defaults and `Validate` with every rule in the contract
      ([D1](design-decisions.md#d1), [D21](design-decisions.md#d21))

## Stage 3 — Store adapter

- [ ] **T015** `postgres/store.go`: `PersistAndEnqueue` — scope, guard first, inbox row, queue rows,
      digest appends, drop records, one transaction; `tx` for `NotifyTx` (NR-002, NR-003, NR-027)
- [x] **T016** The queue's transitions as SQL in the adapter — `Claim`, `Retry`, `Defer`, `Finish`,
      `BindAddresses`, `DeferTenantChannel`, `Cancel`, the digest windows, and the maintenance
      operations — each fenced where it takes a claim, with an integration suite against PostgreSQL 16
      and a mutation check behind every guarantee (NR-004, [D19](design-decisions.md#d19),
      [D37](design-decisions.md#d37)). *Still to do: `Load`, with T015.*
- [ ] **T017** [P] `postgres/inbox.go`: list, bounded unread, seen / read / archive — all scoped
      (NR-024, NR-033)
- [ ] **T018** [P] `postgres/prefs.go`: recipient → tenant (locked) → topic resolution with source
      reporting (NR-011, [D30](design-decisions.md#d30))
- [ ] **T019** [P] `postgres/suppressions.go`, `postgres/leases.go`
- [ ] **T020** **Contract test** `StoreContract`: replay, atomicity, crash after commit leaves drivable
      work, retention with a pending delivery outstanding, `NotifyTx` rollback leaves nothing (NS-002,
      NS-003)

## Stage 4 — Notifier

- [ ] **T021** `app/planner.go`: topic lookup, preference resolution, inbox visibility from
      `InboxBacked`, quiet hours (critical overrides), digest eligibility (never critical or essential),
      quota peek, fallback chain, `DeliverAfter` (NR-011–NR-014, NR-029, NR-031)
- [ ] **T022** `app/notifier.go`: `Notify`, `NotifyTx`, `Cancel`, `Status`; realm from config, never the
      request (NR-009, NR-027, NR-030, NR-032)
- [ ] **T023** `redis/precheck.go`: read before, write after commit, never for `NotifyTx`; a test that a
      failed transaction leaves no entry ([D18](design-decisions.md#d18))
- [ ] **T024** **Contract test** `NotifierContract` (NS-001, NS-002)

## Stage 5 — Channels

- [ ] **T025** [P] `channel/inapp`: nudge-only publish on the identity stream key; `InboxBacked`
      ([D16](design-decisions.md#d16))
- [ ] **T026** [P] `channel/email`: template content, RFC 8058 headers and footer for non-essential
      topics, provider idempotency key, truthful `Dedup` per provider (NR-005, NR-015,
      [D34](design-decisions.md#d34))
- [ ] **T027** [P] `channel/failover`: ordered providers, weakest `Dedup` of its members
      ([D29](design-decisions.md#d29))
- [ ] **T028** `redis/stream.go`: sharded pub/sub on hash-tagged keys
- [ ] **T029** **Contract test** `ChannelContract` against both reference channels, and against the email
      provider's sandbox under `-tags provider` (NR-005, [D32](design-decisions.md#d32))

## Stage 6 — Dispatcher

- [ ] **T030** `app/dispatcher.go`: the nine steps in the contract, adaptive polling, in-memory wakeup,
      optional `LISTEN/NOTIFY` ([D22](design-decisions.md#d22))
- [ ] **T031** Suppression check before every delivery; channel-reported suppressions recorded with
      expiry (NR-006, [D13](design-decisions.md#d13))
- [ ] **T032** Dead-letter replay (`Admin.ReplayDeadLetter`), once per dead letter
- [ ] **T033** **Contract test** `DispatcherContract`: every crash point, poison terminates including
      worker crashes, fan-out retries per address, expiry, fallback, fencing (NS-003, NS-006)

## Stage 7 — Digest, quotas, fairness

- [ ] **T034** `app/digest.go`: append through `Digests.AppendDigest`, leased flush job, render members
      skipping canceled ones (NR-014)
- [ ] **T035** Burst test: N notifications yield `ceil(N / DigestMax)` deliveries and N rows (NS-005)
- [ ] **T036** `app/quota.go` + `redis/quota.go`: peek at enqueue, take at dispatch, backlog deferral,
      `drop_non_essential` never for essential or critical (NR-023, [D24](design-decisions.md#d24))
- [ ] **T037** Noisy-neighbour test measuring NS-007

## Stage 8 — Maintenance

- [ ] **T038** `app/maintenance.go`: leased jobs for retention (inbox, history, dead letters), partition
      provisioning (T006), idempotency and digest expiry, completed-broadcast cleanup (NR-022,
      [D33](design-decisions.md#d33))
- [ ] **T039** Test: retention succeeds while a dead-lettered delivery references an aged partition
      ([D6](design-decisions.md#d6))
- [ ] **T040** `app/erasure.go` + `Admin.Erase`; test NS-012

## Stage 9 — Broadcast

- [ ] **T041** `app/broadcast.go`: durable record, leased paging, cursor committed with each page,
      inline recipients bounded by `MaxInlineRecipients` (NR-020, [D7](design-decisions.md#d7))
- [ ] **T042** Test: a crash mid-broadcast resumes at the next page; a retried broadcast double-notifies
      nobody

## Stage 10 — Transports

- [ ] **T043** `driving/inprocess`: returns the engine's ports (NR-025)
- [ ] **T044** `driving/httpapi`: the REST surface and the SSE relay — subscription from the session
      identity, per-nudge scoped re-read, bounded count first on connect ([rest-api.md](contracts/rest-api.md))
- [ ] **T045** [P] `api/notifyv1`: the proto from the contract
- [ ] **T046** [P] `driving/grpcserver`, `driving/grpcclient` (satisfies `ports.Notifier`;
      `NotifyTx` returns `ErrTxUnsupported`)
- [ ] **T047** **Contract tests** `NotifierContract` through gRPC, and `StreamIsolationContract` against
      the relay (NS-001)

## Stage 11 — Service mode

- [ ] **T048** `postgres/topics.go`, `postgres/templates.go` (Go templates, restricted function map,
      locale fallback), `postgres/addresses.go` — the data-backed ports ([D31](design-decisions.md#d31))
- [ ] **T049** `Catalog` gRPC service; realm bindings from producer principals; channels constructed
      from `channel_providers` with credentials resolved from `secret_ref`
- [ ] **T050** Recipient token verification (JWKS per realm); `Directory` callback client
- [ ] **T051** NS-011 demonstration with a non-Go producer
- [ ] **T052** [P] `driving/natsingest`: optional JetStream ingest, acknowledged after commit
      ([D23](design-decisions.md#d23))

## Stage 12 — Boundary gates

- [x] **T053** Enable the `lint` CI job: `go-arch-lint` component graph and `depguard` bans (NR-026)
- [x] **T054** CI job asserting the module builds and tests green with **no host present** (NS-009)

## Stage 13 — Load test and release

- [ ] **T055** Load-test harness for every row of [plan.md § Capacity model](plan.md#capacity-model),
      run against the reference configuration (NS-010)
- [ ] **T056** [P] Fill `docs/integration-guide.md` code samples against the built API
- [ ] **T057** Tag `v0.1.0` once Stages 1–6 are green; the README status block moves from "designed" to
      "core implemented". A production-ready release additionally requires T055

---

## Not in this phase

Acknowledgement, escalation chains, multi-step workflows and delivery-outcome webhooks are
[Phase 2](../002-escalation-workflows/). A template authoring UI, per-tenant branding UI and delivery
analytics are Phase 3 and undesigned. See [ROADMAP.md](../../ROADMAP.md).
