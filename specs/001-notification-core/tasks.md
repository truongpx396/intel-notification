# Tasks

Dependency-ordered build order for Phase 1. `[P]` marks tasks that can run in parallel with their
siblings. Each task names the requirement it satisfies, so a task with no requirement is scope creep.

Stages map to [plan.md § Phasing](plan.md#phasing). Stages 1–6 are the release-blocking core.

---

## Stage 1 — Schema

- [x] **T001** `migrations/0001_notification_core.sql`: inbox (partitioned, RLS forced), `notify_idem`,
      outbox, preferences, schedules (NR-001, NR-003, NR-008, [D2](design-decisions.md#d2))
- [x] **T002** `migrations/0002_channels_delivery.sql`: suppressions, dead letters (partitioned),
      digest buffer, quotas (NR-006, NR-007, NR-014, NR-023)
- [x] **T003** `scripts/verify-schema.sql` + `make verify-schema`: the seven assertions, including the
      proof that the inherited constraint cannot be constructed (NS-001, NS-002)
- [ ] **T004** `migrations/0003_partition_provisioning.sql`: a function to provision the next N months
      for both partitioned tables, plus the scheduled call (NR-022)

## Stage 2 — Domain and ports

- [ ] **T005** `domain/`: `Realm`, `Tenant`, `Recipient`, `Topic`, `Priority`, `ChannelKind`,
      `Notification`, `Address`, `RenderedContent`, `Delivery`, `DeliveryResult`, `Receipt`,
      `Preference`, `DeliverySchedule`, `Shard`, `OutboxEntry`. Imports nothing outside the module
- [ ] **T006** `domain/shard.go`: `ShardFor(realm, recipient, n)` as a stable CRC32 hash, with a test
      asserting stability against a frozen vector table ([D11](design-decisions.md#d11))
- [ ] **T007** [P] `ports/driving.go`: `Notifier`, `Dispatcher`
- [ ] **T008** [P] `ports/driven.go`: `Channel`, `ChannelRegistry`, `Store`, `PreferenceStore`,
      `TemplateRenderer`, `AddressBook`, `TopicRegistry`, `AudienceResolver`, `Bus`, `Clock`, `IDSource`
      (NR-016, NR-017, [D7](design-decisions.md#d7))
- [ ] **T009** `config.go`: `Config` + `withDefaults` + `Validate`. `Realm` required with no default;
      `Shards >= 1`; `Delivery` in `outbox|direct`; backoff base/ceiling ([D1](design-decisions.md#d1),
      [D10](design-decisions.md#d10))
- [ ] **T010** [P] `domain/backoff.go`: `min(base * 2^attempt, ceiling)` with full jitter, and a test
      asserting the distribution is spread rather than synchronized ([D10](design-decisions.md#d10))

## Stage 3 — Store adapter

- [ ] **T011** `adapters/driven/postgres/store.go`: `PersistAndEnqueue` — inbox row + `notify_idem` +
      one outbox row per channel, in **one** transaction; replay returns `Applied=false` (NR-002, NR-003)
- [ ] **T012** `ClaimOutbox` for a shard, riding the partial index; sets `claimed_at` (NR-004, NR-021)
- [ ] **T013** `MarkDelivered` / `MarkFailed`, with `terminal_reason` distinct from `last_error`
      ([D12](design-decisions.md#d12))
- [ ] **T014** `Unread` recomputed from rows, never from a counter ([D15](design-decisions.md#d15))
- [ ] **T015** `Retention`: retire aged inbox and dead-letter partitions independently (NR-022,
      [D8](design-decisions.md#d8))
- [ ] **T016** `adapters/driven/postgres/prefs.go`: `PreferenceStore` over topic defaults; absent row
      means default (NR-011, [D3](design-decisions.md#d3))
- [ ] **T017** **Contract test** `StoreContract`: replay, one-transaction atomicity, crash-after-commit
      leaves drivable work, retention succeeds with a pending delivery outstanding (NS-002, NS-003)
- [ ] **T018** **Contract test** RLS leak suite — asserts isolation holds *as the owning role*, because
      that is the role a worker actually uses (NS-001, release blocker)

## Stage 4 — Notifier

- [ ] **T019** `app/notifier.go`: resolve preferences → schedule → persist + enqueue. Always writes the
      row; preferences gate only channels (NR-001, NR-012)
- [ ] **T020** `adapters/driven/redis/precheck.go`: the fast duplicate pre-check. Gates the durable
      write **only** — a test asserts delivery still happens when the pre-check is hot (NR-004)
- [ ] **T021** `app/schedule.go`: quiet-hours and digest decisions; `critical` bypasses quiet hours and
      the override is recorded (NR-013, [D9](design-decisions.md#d9))
- [ ] **T022** **Contract test** `NotifierContract`: replay is a no-op, cross-recipient leak is
      impossible, a disabled channel is not enqueued while the row still exists (NS-001, NS-002)

## Stage 5 — Channels

- [ ] **T023** [P] `adapters/driven/channel/inapp`: Redis publish; idempotent by construction since the
      badge is recomputed (NR-005, [D15](design-decisions.md#d15))
- [ ] **T024** [P] `adapters/driven/channel/email`: renders via the template seam, checks suppressions,
      appends an unsubscribe footer for non-essential topics, sets a provider idempotency key
      (NR-005, NR-006, NR-015)
- [ ] **T025** `adapters/driven/postgres/suppressions.go`: per-channel suppression store with expiry
      ([D13](design-decisions.md#d13), [D14](design-decisions.md#d14))
- [ ] **T026** **Contract test** `ChannelContract`, run against both channels: re-drive is one send,
      suppressed is terminal and not retryable, transient failure reports retryable (NR-005, NR-006)

## Stage 6 — Dispatcher

- [ ] **T027** `app/dispatcher.go`: claim → render → resolve address → deliver → record outcome
- [ ] **T028** Backoff and attempt ceiling; park in `dead_letters` with an alarm at the cap (NR-007)
- [ ] **T029** Terminal handling: suppressed and address-miss end at attempt 1 (NR-006,
      [D12](design-decisions.md#d12))
- [ ] **T030** `app/dlq.go`: sweeper re-drives under the cap, parks above it, records `replayed_at`
- [ ] **T031** **Contract test** dispatcher suite: no delivery lost across every crash point; poison
      terminates; a suppressed address is attempted once (NS-003, NS-006)

## Stage 7 — Digest and quotas

- [ ] **T032** `app/digest.go`: open a window, append a member, flush at `flush_at` into one delivery
      (NR-014, [D4](design-decisions.md#d4))
- [ ] **T033** Burst test: N notifications in one window yield one delivery per digestible channel and
      N persisted rows (NS-005)
- [ ] **T034** `app/quota.go` + Redis counters: per-`(realm, tenant, channel)` budget, `defer` default
      (NR-023, [D5](design-decisions.md#d5))
- [ ] **T035** Noisy-neighbour test: one tenant exhausting its budget does not slow another (NS-007)

## Stage 8 — Retention

- [ ] **T036** `app/retention.go`: scheduled retirement for both partitioned tables, independently
      windowed (NR-022)
- [ ] **T037** Test: retention succeeds while a dead-lettered delivery references an aged partition —
      the case a foreign key would have deadlocked ([D6](design-decisions.md#d6))

## Stage 9 — Broadcast

- [ ] **T038** `app/broadcast.go`: paged audience expansion off the request path, deterministic
      per-recipient keys derived from the broadcast key (NR-020, [D7](design-decisions.md#d7))
- [ ] **T039** Test: a large tenant returns promptly; a retried broadcast double-notifies nobody

## Stage 10 — Transports

- [ ] **T040** `adapters/driving/inprocess`: returns `ports.Notifier` directly (NR-025)
- [ ] **T041** [P] `api/notifyv1`: the proto from
      [notification-ports.md](contracts/notification-ports.md#service-surface-notificationservice-grpc-contract-locked)
- [ ] **T042** [P] `adapters/driving/grpcserver`: serves `app` over `notifyv1`
- [ ] **T043** [P] `adapters/driving/grpcclient`: satisfies `ports.Notifier`, with a compile-time
      assertion that it does (NR-025)
- [ ] **T044** Run `NotifierContract` through the gRPC transport — identical semantics is the claim, so
      it gets the identical suite

## Stage 11 — Boundary gates

- [ ] **T045** `.go-arch-lint.yml`: the component graph; `product` may depend only on `ports`
- [ ] **T046** `.golangci.yml`: `depguard` — no infra in `domain`/`ports`/`app`, no host imports
      anywhere in the module (NR-026)
- [ ] **T047** CI job asserting the module builds and tests green with **no host present** (NS-009)
- [ ] **T048** CI job running `make verify-schema` against PostgreSQL 16 on every migration change

## Stage 12 — Documentation and release

- [ ] **T049** [P] Fill `docs/integration-guide.md` code samples against the built API
- [ ] **T050** [P] `docs/operations.md`: shard-change runbook, dead-letter triage, retention, alarms
- [ ] **T051** Tag `v0.1.0` once Stages 1–6 are green; the README status block moves from "designed" to
      "core implemented"

---

## Not in this phase

Acknowledgement, escalation chains, multi-step workflows and delivery-outcome webhooks are
[Phase 2](../002-escalation-workflows/). Authoring UI, per-tenant branding UI and delivery analytics
are Phase 3 and undesigned. See [ROADMAP.md](../../ROADMAP.md).
