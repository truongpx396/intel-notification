# Tasks

Dependency-ordered build order for Phase 1. `[P]` marks tasks that can run in parallel with their
siblings. Each task names the requirement it satisfies, so a task with no requirement is scope creep.

Stages map to [plan.md § Phasing](plan.md#phasing). Stages 1–6 are the release-blocking core.

---

## How a task is worked

This list is test-driven. The order is the build order, and within a stage the test comes first.

- **Red before green.** A task that changes behaviour starts with a failing test. Tasks are tagged
  *red* (write the failing test), *green* (make it pass), *red → green* (both, for a task small enough
  to carry its own test) or *setup* (the seam or kit the tests stand on). A **Red first** line names the
  test to write before the code, not one to add afterwards.
- **Two loops.** The outer loop is the stage's acceptance test: a contract suite, or for the transports
  the Playwright specs already in `e2e/`. It is written before any code in the stage and fails for the
  right reason. The inner loop is one behaviour at a time: a failing test, the least code that passes it,
  then a refactor with the test green.
- **Red is witnessed, and for the right reason.** Run the test and read the failure. It must fail on its
  assertion, not on a compile error, a fixture panic, a skip or a missing container. Put the failure line
  in the commit that adds the test. A test that has never failed proves nothing
  ([docs/testing.md](../../docs/testing.md#mutation-check-what-matters)). A contract suite is itself
  run against a deliberately non-conforming implementation, to show it can fail.
- **Lowest layer that can hold the guarantee.** A pure rule is a unit test; a database property is the
  schema suite; a state transition is an integration test against PostgreSQL; behaviour through the
  service is Playwright. Do not assert through a higher layer what a lower one can.
- **No sleeps.** Time comes from a fake clock; anything concurrent is waited on with a deadline, so a
  regression fails instead of hanging.
- **Data and interfaces get no test of their own.** A struct with no behaviour, or an interface, is
  exercised by the suites and by a compile-time assertion (`var _ ports.X = (*Y)(nil)`) beside each
  implementation. Behaviour, validation included, gets a table-driven test.
- **Commit order:** `test:` (red), then `feat:` or `fix:` (green), then `refactor:`. CI is judged on the
  branch head, not on the red commit.

**Definition of done.** A box is ticked only when all of these hold:

1. The Red test was seen failing for the right reason, and the failure is recorded.
2. It passes, and `make ci` is green.
3. Every guarantee the task adds was mutation-checked: broken on purpose, the test seen going red,
   the break reverted. The task's **Mutate** line suggests where to start; each mutation tried gets a row
   in the [mutation ledger](../../docs/mutations.md), in the same PR, and a mutation that survived is
   recorded as such and fixed.
4. The refactor pass is done, with the tests green.
5. The requirement the task satisfies has its proving test in [Traceability](#traceability).

**IDs are stable; the list order is the build order.** Other files refer to tasks by number (T026, T032,
T038, T040a, T044, T049), so a test task keeps its number and moves ahead of the tasks it drives. New
tasks take an `a` suffix.

**Built before this rule.** T001–T011 and T016 were written test-after. Their suites exist and were
mutation-checked (T016 against 20 mutations), so they are not redone. Everything still open follows the
rule above.

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
      constructed and a mutation check on the partition-scope test (NS-001, NS-002). *Extended, each
      assertion mutation-checked ([ledger](../../docs/mutations.md)): no default partition and a dated
      insert with no partition (13); an empty identity component on every scoped table (14); one
      pending delivery per address (15); a digest window cannot flush before it seals (7); a whole
      quota tenant pair, one active template version and one broadcast audience source (10)*
- [x] **T006** Partition provisioning: `Maintenance.EnsurePartitions` creates each missing month of
      `notifications`, `notification_deliveries` and `dead_letters` and scopes every new `notifications`
      partition; `TestEnsurePartitions` (NR-008, NR-022). `Maintenance.CheckPartitions(ctx, at)` detects
      the two alarm conditions from the catalog alone and returns `domain.PartitionHealth`: `Missing`,
      the months, `at`'s and the next, that no non-default partition of a table covers, and `Unscoped`,
      the `notifications` partitions recipient scoping does not hold on. `TestCheckPartitions`, every
      guarantee mutation-checked ([ledger](../../docs/mutations.md)). *Emitting* `notify.partition.missing`
      and `notify.partition.unscoped` from that report is the maintenance job's, with T038

## Stage 2 — Domain and ports

- [x] **T007** `domain/identity.go`: `Realm`, `Tenant`, `Recipient`, `Identity`, `Canonical`, tested
      against a frozen vector computed independently of the code ([D18](design-decisions.md#d18))
- [x] **T008** `domain/keys.go`: pre-check key, stream key, delivery idempotency key, address key,
      broadcast member key, suppression and subject hashes — each with a frozen vector
      ([D16](design-decisions.md#d16), [D25](design-decisions.md#d25))
- [ ] **T009** [P] `domain/`: `Notification`, `TopicDef`, `DeliverySchedule`, `DeliveryPlan`, `Receipt`,
      `Delivery`, `DeliveryResult`, `UnreadCount`, broadcast and status types. *(Done: `Address`,
      `OutboxEntry`, `Claim`, `TerminalOutcome`, `Disposition` and the dead-letter/fallback policy,
      `Finish`, `Binding`, `CancelReceipt`, `DigestMember`.)* Imports nothing outside the module.
      **Red first:** plain data gets no test. Any constructor or validator these types grow is written
      as a table test first — for example an essential `TopicDef` that is also digestible, or a fallback
      chain that repeats a channel (NR-015, NR-031)
- [x] **T010** [P] `domain/shard.go`: `ShardFor(identity, n)` over the canonical encoding, stable against
      a frozen vector table ([D11](design-decisions.md#d11))
- [x] **T011** [P] `domain/backoff.go`: full jitter with a `RetryAfter` floor, and a test that the
      distribution spreads rather than synchronizes ([D10](design-decisions.md#d10))
- [ ] **T012** [P] *setup* `ports/driving.go`: `Notifier`, `Inbox`, `Admin`, `Dispatcher`, `Jobs`.
      An interface has no behaviour to test; it is the seam the contract suites are written against, so
      it lands before them, and each implementation adds the compile-time assertion
- [ ] **T013** [P] *setup* `ports/driven.go`: the remaining driven ports in the contract (NR-016,
      NR-017, [D7](design-decisions.md#d7)). *(Done: `Queue`, `Digests`, `Maintenance`.)* Same rule as
      T012
- [ ] **T013a** *setup* `internal/notifytest` (an arch-lint `testing` component, like
      `internal/pgtest`): the kit every later suite is written against, so a Red test can exist before the
      code it drives. It holds `Env` (PostgreSQL through `pgtest`, Redis through Testcontainers from T023,
      one cloned database per test), fault injection (`FailNextCommit`, and a hook at each of the
      dispatcher's nine steps that can panic or stall the worker there), a fake clock, and a channel
      `Probe` that counts sends per idempotency key. In-memory fakes cover only the read-side ports a unit
      test needs (`Topics`, `Preferences`, `QuotaCounter`, `AddressBook`, `Templates`) — never `Queue` or
      `Store`, whose leases, fencing and RLS only PostgreSQL can hold.
      **Red first:** the kit is tested where it has logic — an injector fires once, at the named point,
      and disarms; the clock moves only when told; `Probe` counts per key. Each fake later runs the same
      table as the adapter it stands in for (T018, T048), so it cannot drift
- [x] **T014** [P] *red → green* `config.go`: `Config`, `WithDefaults` and `Validate` with every rule in the
      contract ([D1](design-decisions.md#d1), [D21](design-decisions.md#d21)). `Validate` returns a
      `*ConfigError` naming each field at fault. *Mutation-checked: 22 mutations, two of them equivalent
      ([ledger](../../docs/mutations.md)).*
      **Red first:** `TestConfigValidate`, one row per rule in the contract's list — each rejection row
      changes exactly one field of a valid config; boundary rows (`Realm` at 63 and 64 characters, a
      leading hyphen, uppercase; `IdempotencyWindow` at 24h and one nanosecond under;
      `PreCheckTTL` equal to and above the window); a row that breaks several rules and asserts every
      problem is reported at once; a row that shows defaults are applied to a copy and the caller's value
      is untouched.
      **Mutate:** loosen each comparison by one; validate the raw config instead of the effective one

## Stage 3 — Store adapter

- [x] **T016** The queue's transitions as SQL in the adapter — `Claim`, `Retry`, `Defer`, `Finish`,
      `BindAddresses`, `DeferTenantChannel`, `Cancel`, the digest windows, and the maintenance
      operations — each fenced where it takes a claim, with an integration suite against PostgreSQL 16
      and a mutation check behind every guarantee (NR-004, [D19](design-decisions.md#d19),
      [D37](design-decisions.md#d37)). *Still to do: `Load`, with T015.*
- [ ] **T020** *red* **Contract test** `StoreContract` (integration, against PostgreSQL as the table
      owner). Write it first against a `Store` that returns an error, and watch each case fail on its
      assertion. Cases: a replay reports `Applied == false` and writes nothing else; the guard is written
      first and a conflict writes no inbox, queue or digest row; a failure injected after each statement
      of `PersistAndEnqueue` leaves no inbox row, queue row, digest append or drop record; a crash after
      commit leaves work `Claim` can drive; `NotifyTx` rolled back by the host leaves nothing, and
      committed leaves claimable work; the same recipient id in two tenants and in two realms stays
      separate even to the table owner; `Status` reports every delivery by channel and address;
      `Load` omits canceled digest members (NR-002, NR-003, NR-027, NR-032, NS-001, NS-002, NS-003).
      *The retention case that was here moved to T039, because it needs `RetirePartitions` (T038)*
- [ ] **T015** *green* `postgres/store.go`: `PersistAndEnqueue` — scope, guard first, inbox row, queue
      rows, digest appends, drop records, one transaction; `tx` for `NotifyTx`; `Load` and `Status`
      (NR-002, NR-003, NR-027, NR-032). Make T020 pass one case at a time, in the order listed there.
      **Mutate:** write the inbox row before the guard; run the enqueue outside the transaction; commit
      or roll back the caller's `tx`; drop the scope from `Load`
- [ ] **T017** [P] *red → green* `postgres/inbox.go`: list, bounded unread, seen / read / archive — all
      scoped (NR-024, NR-033).
      **Red first:** an integration table — `list` returns only the caller's rows when two tenants share
      a recipient id; seen, read and archive are independent, each transition leaving the other two flags
      alone; marking another recipient's notification returns not-found and changes nothing; with
      `UnreadCap + k` unread the count stops at the cap and reports `Capped`; a row with no inbox-backed
      channel enabled is neither listed nor counted (NR-012).
      **Mutate:** drop the scope from `list`; count without the cap; let `read` clear `seen`
- [ ] **T018** [P] *red → green* `postgres/prefs.go`: recipient → tenant (locked) → topic resolution with
      source reporting (NR-011, [D30](design-decisions.md#d30)).
      **Red first:** a resolution matrix over {recipient set or unset} × {tenant set, unset or locked} ×
      {topic default}, asserting both the winner and the reported source; a locked tenant default beats
      the recipient's opposite choice; a pair is resolved independently per channel; the quiet-hours and
      cadence schedule round-trips; one recipient's preferences never appear for another.
      **Mutate:** swap the precedence; ignore `locked`; report the wrong source
- [ ] **T019** [P] *red → green* `postgres/suppressions.go`. The job lease is already
      `Maintenance.TryLeaseJob` with `TestTryLeaseJob`, so no separate `leases.go` is planned; add one only
      if the leased flush job (T034) finds a gap (NR-006, NS-012).
      **Red first:** a catalog scan finds no plaintext address in any column of the suppression table; a
      live suppression is found by address hash and an expired one is not; suppressions are per channel;
      recording the same suppression twice is one row; one survives `Erase`.
      **Mutate:** store the address; ignore the expiry; delete suppressions in `Erase`

## Stage 4 — Notifier

- [ ] **T024** *red* **Contract test** `NotifierContract` (integration, on the T013a `Env`). Write it
      before the notifier, against the ports with nothing behind them. Cases from the contract: a replay
      is one row and one enqueue per channel; the same user in two tenants gets both; a failed
      transaction leaves no pre-check entry; `NotifyTx` rolled back leaves nothing and committed leaves
      claimable work; a disabled channel is not enqueued while the row still exists; no inbox-backed
      channel enabled means the row exists but is not inbox-visible; a locked tenant preference overrides
      the recipient; `critical` inside quiet hours is due now and marked overridden; the realm cannot be
      set by a producer. Added here: `DeliverAfter` defers the queue row and `DeliverBefore` is carried to
      it (NR-029); `Cancel` stops pending deliveries and reports an in-flight one without recording it as
      canceled (NR-030); `Status` answers per channel and address (NR-032) (NS-001, NS-002)
- [ ] **T021** *red → green* `app/planner.go`: topic lookup, preference resolution, inbox visibility from
      `InboxBacked`, quiet hours (critical overrides), digest eligibility (never critical or essential),
      quota peek, fallback chain, `DeliverAfter` (NR-011–NR-014, NR-029, NR-031).
      **Red first:** a table-driven unit test with no infrastructure, a fixed clock and the T013a fakes,
      one row per rule, input to `DeliveryPlan`. Rows include: a disabled channel is absent; every
      channel disabled gives an empty plan; inbox visibility follows `InboxBacked`; quiet hours defer
      `info` and `warning`; quiet hours that wrap midnight and one that spans a DST change; `critical` in
      quiet hours is immediate and flagged; a digest only for a non-critical, non-essential topic with a
      cadence set; an exhausted quota defers, and drops only under `drop_non_essential` for a
      non-essential, non-critical topic; the fallback order is kept.
      **Mutate:** defer `critical`; digest an essential topic; drop an essential notification
- [ ] **T023** *red → green* `redis/precheck.go`: read before, write after commit, never for `NotifyTx`;
      a test that a failed transaction leaves no entry ([D18](design-decisions.md#d18)).
      **Red first:** against Redis under Testcontainers — a commit failure leaves no entry; `NotifyTx`
      writes none; the key is the `notify_idem` key, checked against the frozen vector; the TTL never
      exceeds `IdempotencyWindow`; a Redis outage is a miss and never an error that blocks `Notify`
      (NR-004).
      **Mutate:** write before commit; write for `NotifyTx`; return the Redis error
- [ ] **T022** *green* `app/notifier.go`: `Notify`, `NotifyTx`, `Cancel`, `Status`; realm from config,
      never the request (NR-009, NR-027, NR-030, NR-032). Make T024 pass one case at a time, with T021 and
      T023 in place; add `var _ ports.Notifier = (*Notifier)(nil)`.
      **Mutate:** take the realm from the request; return `Applied == true` on a replay; treat a pre-check
      hit as authoritative and skip the guard

## Stage 5 — Channels

- [ ] **T029** *red* **Contract test** `ChannelContract`: the five cases in the contract, plus each
      channel reporting its declared `Dedup` level (NR-005, [D32](design-decisions.md#d32)). Run it first
      against two deliberately broken channels — one that declares `DedupProvider` and sends twice, one
      that returns an error for a transient failure — and confirm it fails for each; only then is it a
      test. It then runs against both reference channels, and against the email provider's sandbox under
      `-tags provider`
- [ ] **T028** *red → green* `redis/stream.go`: sharded pub/sub on hash-tagged keys.
      **Red first:** against Redis under Testcontainers — an identity's keys share one cluster slot (CRC16
      of the hash tag); the same recipient id in two tenants gets two keys, checked against the frozen
      stream-key vector; a subscriber on one identity receives nothing published to another.
      **Mutate:** build the key without the tenant; drop the hash tag
- [ ] **T025** [P] *red → green* `channel/inapp`: nudge-only publish on the identity stream key;
      `InboxBacked` ([D16](design-decisions.md#d16)).
      **Red first:** `ChannelContract`, plus: the published payload is an id and nothing else, no content
      and no count (NR-024); it is published on the identity's stream key and no other; `InboxBacked` is
      true.
      **Mutate:** put the title or the count in the payload
- [ ] **T026** [P] *red → green* `channel/email`: template content, RFC 8058 headers and footer for
      non-essential topics, provider idempotency key, truthful `Dedup` per provider (NR-005, NR-015,
      [D34](design-decisions.md#d34)).
      **Red first:** `ChannelContract`, plus golden-file tests of the rendered message: a non-essential
      topic carries `List-Unsubscribe`, `List-Unsubscribe-Post: List-Unsubscribe=One-Click` and the footer;
      an essential topic carries none of them; the provider is given `Delivery.IdemKey`; a provider's
      declared `Dedup` is no stronger than its sandbox shows. This is also the first step toward the
      `unsubscribe` and `webhooks` Playwright specs.
      **Mutate:** omit the header on a non-essential topic; add the footer to an essential one; declare
      `DedupProvider` for a provider that does not dedupe
- [ ] **T027** [P] *red → green* `channel/failover`: ordered providers, weakest `Dedup` of its members
      ([D29](design-decisions.md#d29)).
      **Red first:** a table over every pairing of member `Dedup` levels showing the composite declares
      the weakest; on `Retry` from one provider the next is tried inside the same attempt; the first
      provider's `Delivered` stops the chain; `ChannelContract` over the composite.
      **Mutate:** report the strongest level; try the next provider after `Delivered`

## Stage 6 — Dispatcher

- [ ] **T033** *red* **Contract test** `DispatcherContract`: every crash point, poison terminates
      including worker crashes, fan-out retries per address, expiry, fallback, fencing (NS-003, NS-006).
      Write each named case from the contract, driving each crash with the T013a hook at the step where
      it happens (after claim before send, after send before record, and the rest of the nine), before any
      dispatcher code exists. Also: a fenced outcome records nothing and increments `notify.lease.lost`
- [ ] **T030** *green* `app/dispatcher.go`: the nine steps in the contract, adaptive polling, in-memory
      wakeup, optional `LISTEN/NOTIFY` ([D22](design-decisions.md#d22)). Make T033 pass one case at a
      time, from the plain delivery outwards.
      **Red first** for what T033 does not reach, as unit tests on the fake clock with no sleeps: polling
      backs off to `PollInterval` when idle and resets on work; an in-process `Notify` wakes an idle
      dispatcher at once; `LISTEN/NOTIFY` is off by default.
      **Mutate:** count the attempt at finish instead of claim; skip the lease token on a write; retry
      without the `RetryAfter` floor
- [ ] **T031** *red → green* Suppression check before every delivery; channel-reported suppressions
      recorded with expiry (NR-006, [D13](design-decisions.md#d13)).
      **Red first:** a live suppression means the channel is never called, on every channel kind, and the
      outcome is `suppressed`; a `Suppressed` result writes the suppression with `SuppressFor` as its
      expiry and then finishes; an unresolvable address ends `no_address`, distinct from `suppressed`.
      **Mutate:** check suppressions only for email; write the suppression after the finish
- [ ] **T032** *red → green* Dead-letter replay (`Admin.ReplayDeadLetter`), once per dead letter.
      **Red first:** a dead letter is inspectable with its reason and last error (NS-006); replaying it
      enqueues one delivery; a second replay, and two concurrent replays, enqueue nothing more.
      **Mutate:** drop the replay guard
- [ ] **T033a** *red → green* NS-008 demonstration: add a third channel (`channel/webhook`, as laid out
      in the plan) with no diff to fan-out, persistence or preferences.
      **Red first:** register the channel in the `Env` of `NotifierContract` and `DispatcherContract` with
      one line, and run both suites unchanged; they fail until the channel exists and pass without a change
      to `app/`, `adapters/driven/postgres/` or preferences. The evidence is the PR's `git diff --stat`:
      new files plus one registration line (NR-016, NS-008)

## Stage 7 — Digest, quotas, fairness

- [ ] **T035** *red* Burst test: N notifications yield `ceil(N / DigestMax)` deliveries and N rows
      (NS-005). A table over N = `DigestMax` − 1, `DigestMax`, `DigestMax` + 1 and 2 × `DigestMax` + 1,
      plus a `critical` burst and an essential burst that must never be digested (NR-014)
- [ ] **T034** *green* `app/digest.go`: append through `Digests.AppendDigest`, leased flush job, render
      members skipping canceled ones (NR-014). Make T035 pass.
      **Red first** for the rest: two flushers of one window produce one delivery; a window overdue after a
      crash is flushed by the next run; a digest whose members are all canceled ends `canceled` and not
      delivered.
      **Mutate:** flush without the lease; render a canceled member
- [ ] **T037** *red* Noisy-neighbour test measuring NS-007, written before the quota code. One tenant
      exhausts its budget while another sends at a steady rate; the quiet tenant's p95 from due to claimed
      must stay within 10% of its value with no tenant exhausted. It fails against a build that only
      defers row by row, which is its red
- [ ] **T036** *red → green* `app/quota.go` + `redis/quota.go`: peek at enqueue, take at dispatch,
      backlog deferral, `drop_non_essential` never for essential or critical (NR-023,
      [D24](design-decisions.md#d24)). Make T037 pass.
      **Red first:** a peek consumes nothing; a take consumes once, and a deferral returns the attempt;
      the realm default applies until a tenant overrides it; losing Redis resets the budget without an
      error; a table for `drop_non_essential` over {info, warning, critical} × {essential, not}.
      **Mutate:** peek consumes; defer row by row instead of moving the backlog; drop an essential
      notification

## Stage 8 — Maintenance

- [ ] **T039** *red* Test: retention succeeds while a dead-lettered delivery references an aged
      partition ([D6](design-decisions.md#d6)). Also the case moved from T020: retention with a pending
      delivery outstanding leaves that delivery drivable. Written against `RetirePartitions` before it
      exists
- [ ] **T038** *red → green* `app/maintenance.go`: leased jobs for retention (inbox, history, dead
      letters), partition provisioning and its check (T006), idempotency and digest expiry,
      completed-broadcast cleanup (NR-022, [D33](design-decisions.md#d33)). Make T039 pass.
      **Red first:** two workers contending for a job run it once; a re-run of each job changes nothing;
      retiring a partition is a metadata operation, so `n_tup_del` on the table does not move; the
      partition check runs at startup and on its schedule, and against a fake `Maintenance` whose
      `CheckPartitions` returns a `PartitionHealth`, each `Missing` gap raises `notify.partition.missing`
      naming its table and month, each `Unscoped` partition raises `notify.partition.unscoped` naming
      it, and a healthy report raises neither. Detecting the conditions is T006's, tested there against
      PostgreSQL; this task tests only that the job turns a report into the two alarms.
      **Mutate:** retire with `DELETE`; skip the lease; drop the `Unscoped` half of the report
- [ ] **T040** *red → green* `app/erasure.go` + `Admin.Erase`; test NS-012.
      **Red first:** erase a recipient, then scan every table listed by the database catalog, not by the
      adapter, and find no row carrying the recipient; the erasure record keeps no identity; a suppression
      recorded for one of its addresses still blocks delivery to that address (NR-034).
      **Mutate:** skip a table; delete the suppression
- [ ] **T040a** *red → green* `app/maintenance.go`: `Maintenance.Preflight`, at startup and on a
      schedule, reports as warnings and never failures the database conditions the queue depends on —
      `max_wal_size` against the observed WAL rate, the connected role's
      `idle_in_transaction_session_timeout` and `statement_timeout`, the age of the oldest `backend_xmin`
      — and exports `notify.db.oldest_xmin_age` and `notify.db.checkpoints_requested` (NR-037,
      [D38](design-decisions.md#d38)).
      **Red first:** a role with no timeouts produces its warning; an open old transaction produces its
      warning; an undersized `max_wal_size` produces its warning; a healthy configuration produces none;
      and none of them stops the service starting.
      **Mutate:** return an error instead of a warning; read a stale `backend_xmin`

## Stage 9 — Broadcast

- [ ] **T042** *red* Test: a crash mid-broadcast resumes at the next page; a retried broadcast
      double-notifies nobody. Cases: a kill between pages, and a kill inside a page's transaction (the page
      rolls back as a whole); a per-recipient key derives deterministically from the broadcast key
      (frozen vector); an inline list over `MaxInlineRecipients` is rejected (NR-020)
- [ ] **T041** *green* `app/broadcast.go`: durable record, leased paging, cursor committed with each
      page, inline recipients bounded by `MaxInlineRecipients` (NR-020, [D7](design-decisions.md#d7)).
      Make T042 pass.
      **Mutate:** commit the cursor in a separate transaction from the page; derive a key from the page
      number

## Stage 10 — Transports

- [ ] **T047a** *red* Enable the Playwright suite: remove `test.describe.fixme` from the `e2e/` specs as
      the routes they drive land, implement `/healthz` and `/readyz`, and switch on the `e2e` CI job
      ([docs/testing.md](../../docs/testing.md)). This is the standing outer loop and stays open until
      T049 lands. Take one spec at a time: remove its `fixme`, see it fail because the route or the
      service is missing, then land the route
- [ ] **T047** *red* **Contract tests** `NotifierContract` through gRPC, and `StreamIsolationContract`
      against the relay (NS-001). The gRPC run is the same suite as T024 with a different `Env`
- [ ] **T043** *green* `driving/inprocess`: returns the engine's ports (NR-025). `NotifierContract`
      runs through it unchanged
- [ ] **T044** *red → green* `driving/httpapi`: the REST surface and the SSE relay — subscription from
      the session identity, per-nudge scoped re-read, bounded count first on connect
      ([rest-api.md](contracts/rest-api.md)).
      **Red first:** handler tests over `httptest` with a fake `Notifier` and `Inbox`, no infrastructure,
      one row per line of the contract's error table and observable assertions: `401` for a missing or
      expired token; `404`, never `403`, for another recipient's notification; the `422` cases; `410` for an
      expired unsubscribe token; `GET` on an unsubscribe link makes no call to the preference writer, and
      `POST` disables exactly one pair; a webhook with a bad signature returns `401` and its body is never
      parsed; the stream subscribes from the session's identity and not from a query parameter; a forged
      nudge shows nothing; the bounded count is sent first on connect.
      **Mutate:** read the recipient from a header; parse the webhook body before verifying it
- [ ] **T045** [P] *setup* `api/notifyv1`: the proto from the contract. Generated code is data with no
      test of its own; a lint and breaking-change check on the proto runs in CI
- [ ] **T046** [P] *green* `driving/grpcserver`, `driving/grpcclient` (satisfies `ports.Notifier`;
      `NotifyTx` returns `ErrTxUnsupported`). Make the T047 gRPC run pass.
      **Red first:** a unit test that `NotifyTx` returns `ErrTxUnsupported` and never opens a transaction

## Stage 11 — Service mode

- [ ] **T051** *red* NS-011 demonstration with a non-Go producer. Write the scenario first, using only
      the service's API and the generated client: register a topic and a template, store a recipient's
      addresses, send one notification, and see it delivered on two channels. It fails until T048–T050 land
- [ ] **T048** *red → green* `postgres/topics.go`, `postgres/templates.go` (Go templates, restricted
      function map, locale fallback), `postgres/addresses.go` — the data-backed ports
      ([D31](design-decisions.md#d31)).
      **Red first:** an integration table — a topic registered at runtime is usable without a migration
      (NR-017); a template calling a function outside the restricted map is rejected when it is saved, not
      when it is sent; locale falls back from the address to the tenant default to `DefaultLocale`, and to
      `ErrNoTemplate` when none exists; several addresses per `(recipient, channel)` with independent state
      (NR-028); the address table is scoped; the T013a fakes run the same table.
      **Mutate:** allow `env` or a file function in a template; skip the locale fallback
- [ ] **T049** *red → green* `Catalog` gRPC service; realm bindings from producer principals; channels
      constructed from `channel_providers` with credentials resolved from `secret_ref`.
      **Red first:** a producer authenticated for realm A cannot register or send in realm B (NR-009); no
      call returns a credential, only the `secret_ref`; an unresolvable `secret_ref` marks the provider
      unavailable and does not crash the service.
      **Mutate:** take the realm from the request body; echo the resolved secret
- [ ] **T050** *red → green* Recipient token verification (JWKS per realm); `Directory` callback client.
      **Red first:** a table over tokens — expired, wrong audience, signed by another realm's key,
      `alg: none`, tampered, an unknown `kid` (refetched once, bounded) — and the recipient identity comes
      only from verified claims; a `Directory` timeout fails the page and leaves the broadcast to resume at
      it (T042).
      **Mutate:** accept `alg: none`; trust the unverified `sub`
- [ ] **T052** [P] *red → green* `driving/natsingest`: optional JetStream ingest, acknowledged after
      commit ([D23](design-decisions.md#d23)).
      **Red first:** a kill after the message is handled and before the commit means redelivery, with no
      second notification (NR-035); the acknowledgement is sent only after the commit; a poison message
      does not block the stream.
      **Mutate:** acknowledge before the commit

## Stage 12 — Boundary gates

- [x] **T053** Enable the `lint` CI job: `go-arch-lint` component graph and `depguard` bans (NR-026)
- [x] **T054** CI job asserting the module builds and tests green with **no host present** (NS-009)

## Stage 13 — Load test and release

- [ ] **T055** *red → green* Load-test harness for every row of
      [plan.md § Capacity model](plan.md#capacity-model), run against the reference configuration
      (NS-010). *Partly started: `make bench` measures the queue's claim and finish, claim latency under
      backlog and a proxy for the accept path, and `make soak` runs it under sustained load with a
      long-running transaction, on a laptop. Broadcast, reconnect, fairness and the reference
      configuration remain.*
      **Red first:** encode every row's target as a threshold before the first run, so a row that misses
      fails the run. Unlike `make bench`, this is a gate: it decides whether a release may be called
      production-ready
- [ ] **T056** [P] *red → green* Fill `docs/integration-guide.md` code samples against the built API.
      **Red first:** each sample is an `Example` test, or is extracted and built in CI, so a sample that
      stops compiling fails the build
- [ ] **T057** Tag `v0.1.0` once Stages 1–6 are green; the README status block moves from "designed" to
      "core implemented". A production-ready release additionally requires T055. Before tagging, every
      NS-001 to NS-003 row in the traceability table below names a suite that is green in CI

---

## Traceability

Every success criterion is proven by a test that is written before the code it proves. A criterion
that lists no red task has no test scheduled.

| Criterion | Proven by | Written (red) in |
|---|---|---|
| NS-001 recipient scoping | schema tests 4, 5 and 14 (`make verify-schema`); `StoreContract` (same id in two tenants, read as owner); `NotifierContract`; `StreamIsolationContract`; the `isolation` and `stream` Playwright specs | T005 (done), T020, T024, T047, T047a |
| NS-002 exactly once, per channel | schema tests 3 and 15 (`make verify-schema`); `StoreContract` and `NotifierContract` replay cases; `ChannelContract` at each declared `Dedup` | T005 (done), T020, T024, T029 |
| NS-003 no delivery lost to a crash | `StoreContract` crash after commit; `DispatcherContract` at every crash point | T020, T033 |
| NS-004 in-app under 5 s at p95 | the `stream` Playwright spec (arrives without a reload); the load test | T047a, T055 |
| NS-005 digest burst | burst test | T035 |
| NS-006 poison terminates | `DispatcherContract` (worker panic, attempt ceiling); dead-letter inspect and replay | T033, T032 |
| NS-007 tenant fairness | noisy-neighbour test | T037 |
| NS-008 a channel is one file and one line | third-channel demonstration | T033a |
| NS-009 builds with no host | no-host CI job | T054 (done) |
| NS-010 capacity | load-test thresholds | T055 |
| NS-011 non-Go producer | producer scenario against the service | T051 |
| NS-012 erasure | catalog-wide scan after `Erase` | T040 |

---

## Not in this phase

Acknowledgement, escalation chains, multi-step workflows and delivery-outcome webhooks are
[Phase 2](../002-escalation-workflows/). A template authoring UI, per-tenant branding UI and delivery
analytics are Phase 3 and undesigned. See [ROADMAP.md](../../ROADMAP.md).
