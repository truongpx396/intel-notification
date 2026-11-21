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
  test to write before the code, not one to add afterwards. A *green* task carries no **Red first** of
  its own: whatever its contract suite does not reach is a *red* task of its own, listed before it with
  an `a` suffix (T030a, T034a), so every test has a place in the build order. A *red* task carries no
  **Mutate** line; its *green* partner does.
- **A red never waits on a missing seam.** Whatever a suite needs in order to compile and start (a
  generated client, an `Env` field, a fake) is a *setup* task listed before it: T012, T013, T013a, T023a,
  T045. What the code under test will provide is stood in for by the port returning
  `errors.New("not implemented")`, the device T020 uses for the `Store`, so the first run fails on the
  suite's assertions and not on a missing package.
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
- **A statistical criterion is asserted deterministically.** A p95 or a rate (NS-007) is measured in a
  unit that does not move with the machine, such as dispatcher passes or the fake clock, at the lowest
  layer that can hold it. Its wall-clock value belongs to the load test (T055), which is a gate, not a
  unit test.
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
   recorded as such and fixed. A task that adds no guarantee of its own says **Mutate: none** and why.
4. The refactor pass is done, with the tests green.
5. Every requirement the task cites names this task's test in [Requirements](#requirements), and every
   success criterion it serves has its row in [Traceability](#traceability).

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
- [x] **T009** [P] `domain/`: `Notification`, `TopicDef`, `DeliverySchedule`, `DeliveryPlan`, `Receipt`,
      `Delivery`, `DeliveryResult`, `UnreadCount`, broadcast and status types, and the inbox and
      preference types the ports speak in. Imports nothing outside the module. Plain data got no test.
      The behaviour these types grew was written test-first: `TopicDef.Validate` (a fallback chain that
      repeats a channel, a channel both fanned out and a fallback), `TopicDef.Digestible` and
      `EffectivePriority` (an essential or critical notification is never digestible, including one that
      is critical only by its topic's default; NR-014, NR-015), `Notification.Validate` (the `IdemKey`
      bounds and the identity components the schema checks) and `Notification.Identity`, which takes the
      realm as an argument (NR-009). *Mutation-checked: 42 mutations, one of which survived until the
      test helper was strengthened ([ledger](../../docs/mutations.md)). `DeliverySchedule` has no
      validator yet: its quiet-hours format and time zone rules belong with the planner (T021).*
- [x] **T010** [P] `domain/shard.go`: `ShardFor(identity, n)` over the canonical encoding, stable against
      a frozen vector table ([D11](design-decisions.md#d11))
- [x] **T011** [P] `domain/backoff.go`: full jitter with a `RetryAfter` floor, and a test that the
      distribution spreads rather than synchronizes ([D10](design-decisions.md#d10))
- [x] **T012** [P] *setup* `ports/driving.go`: `Notifier`, `Inbox`, `Admin`, `Dispatcher`, `Jobs`, and the
      opaque `Tx`. An interface has no behaviour to test; it is the seam the contract suites are written
      against, so it lands before them, and each implementation adds the compile-time assertion
- [x] **T013** [P] *setup* `ports/driven.go`: the remaining driven ports in the contract (NR-016,
      NR-017, [D7](design-decisions.md#d7)): `Store`, `InboxStore`, `PreferenceStore`, `TopicRegistry`,
      `TemplateRenderer`, `AddressBook`, `AudienceResolver`, `SuppressionStore`, `QuotaCounter`,
      `PreCheck`, `StreamPublisher`, `Channel`, `AddressNormalizer`, `ChannelRegistry`, `Clock`,
      `IDSource`, beside the `Queue`, `Digests` and `Maintenance` already there. Same rule as T012
- [x] **T013a** *setup* `internal/notifytest` (an arch-lint `testing` component, like
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
      *Built: `Clock`, `Injector` (a hold or a crash at any of the nine `domain.DispatchStep`s, once),
      `FaultStore` (`FailNextCommit` through the port's own transaction seam), `Probe` with a `Channel`
      that honours its declared `Dedup`, `Registry`, the five fakes, and `Env` with `Main`. Each has its
      own table test and its mutations are in the [ledger](../../docs/mutations.md). Not yet in `Env`,
      each arriving with its task: Redis (T023), and the `Notifier`, `Dispatcher` and `Store`. The fakes
      implement the semantics the contract states; where it is silent (the fallback chain being subject
      to preferences, what an essential topic resolves to, locale fallback being tag, language, default)
      the choice is in the fake's doc comment, and T018's and T048's shared tables decide it for both.*

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
      [D37](design-decisions.md#d37)). *`Load` came with T015.*
- [x] **T020** *red* **Contract test** `StoreContract` (integration, against PostgreSQL as the table
      owner). Write it first against a `Store` that returns an error, and watch each case fail on its
      assertion. Cases: a replay reports `Applied == false` and writes nothing else; the guard is written
      first and a conflict writes no inbox, queue or digest row; a failure injected after each statement
      of `PersistAndEnqueue` leaves no inbox row, queue row, digest append or drop record; a crash after
      commit leaves work `Claim` can drive; `NotifyTx` rolled back by the host leaves nothing, and
      committed leaves claimable work; the same recipient id in two tenants and in two realms stays
      separate even to the table owner; `Status` reports every delivery by channel and address;
      `Load` omits canceled digest members; `RecordProviderStatus` records a callback's status on the
      delivery named by its channel and provider message id and on no other (NR-002, NR-003, NR-027,
      NR-032, NS-001, NS-002, NS-003).
      *The retention case that was here moved to T039, because it needs `RetirePartitions` (T038)*
      *Built: `store_integration_test.go`, ten tests. Written against the `Store` as stubs returning
      `errors.New("not implemented")`; 10 tests and 47 subtests failed on their first assertion, none on a
      fixture. A failure "after each statement" is a database trigger that refuses inserts into one of the
      five tables, so it needs no seam in the store and fires the same in the store's own transaction and a
      caller's. "The guard is written first" is shown by refusing every table but the guard and replaying: a
      store that wrote the inbox row first fails instead of reporting a replay. Refusals carry a control write
      first, so a store that accepts nothing cannot pass them. The three broadcast methods have no case;
      they are T042's.*
- [x] **T015** *green* `postgres/persist.go`, `load.go`, `status.go`: `PersistAndEnqueue` — scope, guard first, inbox row, queue
      rows, digest appends, drop records, one transaction; `tx` for `NotifyTx`; `Load`, `Status` and
      `RecordProviderStatus` (NR-002, NR-003, NR-027, NR-032). Make T020 pass one case at a time, in the
      order listed there.
      **Mutate:** write the inbox row before the guard; run the enqueue outside the transaction; commit
      or roll back the caller's `tx`; drop the scope from `Load`; match a provider message id without
      its channel
      *Built, with 62 rows in the [ledger](../../docs/mutations.md). Four survived and the tests were
      strengthened until they died; three more are equivalent and left, each said why. Where the contract
      was silent the choice is in the code's doc comment: the store takes the notification's id and
      `created_at` from a `Clock` and an `IDSource` it is built with (`postgres.WithClock`,
      `WithIDSource`; UUIDv7 by default) and leaves a queue row's due time to the database's `now()`, which
      is what a claim compares against; it refuses an unresolved priority, because it cannot know the
      topic's default and storing `info` for a critical topic would let quiet hours defer it, so T022 must
      resolve it first; a replay names the first notification and its visibility; `Load` reports a gone
      row (an erased recipient) as canceled, not as an error a retry would dead-letter; `Status` reads one
      snapshot and includes a digested channel's window; and `RecordProviderStatus` returns
      `domain.ErrUnknownDelivery` for a callback that names no delivery, so the webhook handler (T044) can
      ask the provider to try again when a callback beats the delivery's own record. `notifytest.Env` gains
      `Store`, dated from the Env's clock.*
- [x] **T017** [P] *red → green* `postgres/inbox.go`: list, bounded unread, seen / read / archive — all
      scoped (NR-024, NR-033).
      **Red first:** an integration table — `list` returns only the caller's rows when two tenants share
      a recipient id; seen, read and archive are independent, each transition leaving the other two flags
      alone; marking another recipient's notification returns not-found and changes nothing; with
      `UnreadCap + k` unread the count stops at the cap and reports `Capped`; a row with no inbox-backed
      channel enabled is neither listed nor counted (NR-012).
      **Mutate:** drop the scope from `list`; count without the cap; let `read` clear `seen`
      *Built as `postgres.Inbox`, a `ports.InboxStore`, with 39 rows in the [ledger](../../docs/mutations.md);
      two survivors are equivalent and left, three were repaired. The contract left several things open, and the
      choices are in `ports.InboxStore` and the tests: what the inbox shows (inbox-visible, uncanceled, send
      time passed) is, to every method including `Get` and the marks, all there is, so a hidden row reads as
      absent; `MarkRead` and `Archive` are all-or-nothing and answer `domain.ErrNotFound` for a foreign,
      unknown or malformed id with one text, which names no id; `MarkAllRead` leaves what is archived; the
      list's cursor is a keyset on `(created_at, id)`, refused as `domain.ErrInvalidCursor` when malformed; a
      missing limit is `DefaultPageSize` and a large one `MaxPageSize`. The badge's "reads at most cap+1 rows"
      is asserted on the executed plan, captured from the statement `Unread` runs, not on its answer.*
- [x] **T018** [P] *red → green* `postgres/prefs.go`: recipient → tenant (locked) → topic resolution with
      source reporting (NR-011, [D30](design-decisions.md#d30)).
      **Red first:** a resolution matrix over {recipient set or unset} × {tenant set, unset or locked} ×
      {topic default}, asserting both the winner and the reported source; a locked tenant default beats
      the recipient's opposite choice; a pair is resolved independently per channel; the quiet-hours and
      cadence schedule round-trips; one recipient's preferences never appear for another.
      **Mutate:** swap the precedence; ignore `locked`; report the wrong source
      *Built as `postgres.Preferences`, with 66 rows in the [ledger](../../docs/mutations.md); three survivors
      are equivalent and left, three were gaps in the shared table (a tenant row for another topic, a
      tenant of another kind, and a refusal the fake and the store would both have failed to make) and
      the table was strengthened until they died. D30's precedence is written once, in
      `domain.ResolvePreferences`, which the fake and the store both call, with `Preference.Validate` and
      `TenantPreference.Validate` beside it. The fake's own table became `notifytest.PreferenceStoreContract`,
      which the fake runs as a unit test and the store runs against PostgreSQL, so the two cannot drift
      (the T013a rule). The store adds what only a database holds: a choice stored under the recipient's
      scope and no other, sixteen writers on one pair leaving one row, three hundred pairs in one call, and a
      schedule that keeps its clock times, seconds and all.*
- [x] **T019** [P] *red → green* `postgres/suppressions.go`. The job lease is already
      `Maintenance.TryLeaseJob` with `TestTryLeaseJob`, so no separate `leases.go` is planned; add one only
      if the leased flush job (T034) finds a gap (NR-006, NS-012).
      **Red first:** a catalog scan finds no plaintext address in any column of the suppression table; a
      live suppression is found by address hash and an expired one is not; suppressions are per channel;
      recording the same suppression twice is one row; one survives `Erase`.
      **Mutate:** store the address; ignore the expiry; delete suppressions in `Erase`
      *Built as `postgres.Suppressions`, with 27 rows in the [ledger](../../docs/mutations.md) (eight of them for
      `domain.SuppressionReason.Valid`, which the store uses to refuse a reason before the schema's check does).
      "Store the address" cannot be done through the port, which receives only the hash, so the mutation is the
      one that could put an address in the table: a new `address` column, caught by reading the table's columns
      from the catalog and dumping every stored row as text. One mutation survived (a reason not checked,
      which the schema's own `CHECK` also refuses) and the test was made to ask for the store's own wording.
      Where the contract was silent: a later record can only strengthen a suppression. A permanent one stays
      permanent and keeps its reason when a soft bounce follows it, an expiring one becomes permanent when a
      permanent record arrives, and of two expiries the later wins, so a stale callback cannot quietly
      resume mail to someone who complained. A suppression ends at its expiry, not after it.*

## Stage 4 — Notifier

- [x] **T023a** *setup* `internal/notifytest`: an in-memory `PreCheck` and the `Env.PreCheck` field the
      `Notifier` is built with. It stands where Redis will, so `NotifierContract` can be written, and fail
      on its assertions, before any Redis adapter exists (T013a puts Redis in `Env` with T023, after
      T024). It follows the port: `Seen` only reads, `Remember` writes, an entry lapses after its `ttl` on
      the fake clock, and it keeps each `Remember` with the instant it happened, so a test can see
      whether the write came before or after the commit. T023 later runs its table against this fake and
      against Redis, so the two cannot drift (the T013a rule).
      **Red first:** the kit has logic, so it has a table test: `Remember` then `Seen` returns the id; an
      entry is present one nanosecond before its `ttl` and gone at it; `Seen` creates nothing; a key is
      stored exactly as given.
      **Mutate:** ignore the `ttl`; let `Seen` create the entry
      *Built as `notifytest.PreCheck`, with 8 rows in the [ledger](../../docs/mutations.md), none surviving.
      Its table is `notifytest.PreCheckContract`, written once and run by the fake as a unit test; T023 runs
      it against Redis (the T018 device). Where the port is silent: a `ttl` that is not positive is refused
      by `Remember` and stores nothing, because Redis reads a zero TTL as "never expires" and an entry that
      outlives the idempotency window could drop a notification the guard would accept (D18). The lapse at
      the `ttl` needs a clock the test can move, so it is the fake's own table, not the shared one. `Writes`
      lists each stored `Remember` with its instant on the fake clock; a read and a refused write are not
      in it.*
- [x] **T024** *red* **Contract test** `NotifierContract` (integration, on the T013a `Env` and T023a's
      `PreCheck`). Write it before the notifier, against the ports with nothing behind them. Cases from
      the contract: a replay is one row and one enqueue per channel; the same user in two tenants gets
      both; a failed transaction leaves no pre-check entry (run again on Redis once T023 lands);
      `NotifyTx` rolled back leaves nothing and committed leaves
      claimable work; a disabled channel is not enqueued while the row still exists; no inbox-backed
      channel enabled means the row exists but is not inbox-visible; a locked tenant preference overrides
      the recipient; `critical` inside quiet hours is due now and marked overridden; the realm cannot be
      set by a producer. Added here: `DeliverAfter` defers the queue row and `DeliverBefore` is carried to
      it (NR-029); `Cancel` stops pending deliveries and reports an in-flight one without recording it as
      canceled (NR-030); `Status` answers per channel and address (NR-032) (NS-001, NS-002)
      *Built as `notifytest.NotifierContract`, in the kit so the transport suites reuse it: 17 cases, each
      refusal after a control write, written against a `Notifier` that returned `errors.New("not
      implemented")` and seen to fail on its first assertion in every case. The failure "after commit" is
      `FaultStore.FailNextCommit` through the port's own seam, and the quiet-hours, schedule and
      preference cases drive the Env's fakes. `notifytest.Env` gained `Config`, `Notifier`, `PreCheck` (and
      `Memory`, the same object as its own type), and its `Store` is now a `FaultStore`. It runs in memory
      and on Redis (`notifytest.WithRedis()`), which T022a asserts the same way. The cases that join a
      caller's transaction hold for the in-process Notifier only: a remote one returns
      `domain.ErrTxUnsupported`, and the transport suite says which it leaves out.*
- [x] **T021** *red → green* `app/planner.go`: topic lookup, preference resolution, inbox visibility from
      `InboxBacked`, quiet hours (critical overrides), digest eligibility (never critical or essential),
      quota peek, fallback chain, `DeliverAfter` (NR-011–NR-014, NR-029, NR-031).
      **Red first:** a table-driven unit test with no infrastructure, a fixed clock and the T013a fakes,
      one row per rule, input to `DeliveryPlan`. Rows include: a disabled channel is absent; every
      channel disabled gives an empty plan; inbox visibility follows `InboxBacked`; quiet hours defer
      `info` and `warning`; quiet hours that wrap midnight and one that spans a DST change; `critical` in
      quiet hours is immediate and flagged; `DeliverySchedule.Validate`, which T009 left for here, rejects
      a malformed quiet-hours time and an unknown time zone, one changed field per row; a digest only for
      a non-critical, non-essential topic with a cadence set; an exhausted quota defers, and drops only
      under `drop_non_essential` for a non-essential, non-critical topic; the fallback order is kept.
      **Mutate:** defer `critical`; digest an essential topic; drop an essential notification; accept a
      time zone the database does not know
      *Built as `app.Planner` with `domain.DeliverySchedule.Validate` and `QuietUntil` and
      `domain.QuotaPolicy.Drops`, and 60 rows in the [ledger](../../docs/mutations.md): two survivors, both
      gaps in the table (the configuration's defaults, and a failing counter that returned a zero reset),
      and one equivalent mutant, a redundant guard that was removed. The contract was silent on several
      points and the choices are in `Planner.Plan`'s doc comment: the planner reads a quota's `on_exhausted`
      through a new `ports.QuotaPolicies` (nil, or a lookup that fails, defers; quota fails open and a
      lookup that fails must not discard); a fallback channel is never fanned out and the chain rides each
      fanned-out delivery, skipping a channel the recipient disabled; a channel only a tenant row enables
      does fan out (D30's "everyone gets security email"); quiet hours are judged at the instant a
      delivery would otherwise be due, so a send scheduled for the morning is not held by tonight's, and of
      quota and quiet hours the later wins; a scheduled send and one with an expiry are never digested,
      since a window could deliver them early or late; a schedule that does not validate is not applied,
      cadence and all, rather than lose the notification; and the inbox shows a scheduled notification
      from its send time whatever quiet hours do to the deliveries. The zone database is embedded
      (`time/tzdata`) so two hosts end quiet hours at the same instant.*
- [x] **T023** *red → green* `redis/precheck.go`: read before, write after commit, never for `NotifyTx`;
      a test that a failed transaction leaves no entry ([D18](design-decisions.md#d18)).
      **Red first:** against Redis under Testcontainers — a commit failure leaves no entry; `NotifyTx`
      writes none; the key is the `notify_idem` key, checked against the frozen vector; the TTL never
      exceeds `IdempotencyWindow`; a Redis outage is a miss and never an error that blocks `Notify`
      (NR-004).
      **Mutate:** write before commit; write for `NotifyTx`; return the Redis error
      *Built as `redis.PreCheck` over go-redis, with 16 rows in the [ledger](../../docs/mutations.md), one
      survivor repaired (a ttl truncated to whole seconds, which a row that asked only for a PTTL above zero
      let through). The first two mutations name the Notifier's side of the pre-check, "write before commit"
      and "write for `NotifyTx`", and are T022's rows; the adapter's are the Redis error, the clamp to the
      idempotency window, an entry with no expiry, and the key as given. A Redis fault is a miss: `Seen`
      answers "not seen" and `Remember` answers nil, each telling `WithErrorHandler`, so an operator can see
      what Notify never will; a `ttl` that is not positive is the one error returned, because Redis would
      read it as an entry that never lapses. `WithMaxTTL(IdempotencyWindow)` clamps every entry to the
      guard's window. `internal/redistest` starts one Redis per test binary with a logical database per test,
      beside `pgtest`; `notifytest.WithRedis()` starts it from `Main` and builds the `Env`'s Notifier on it,
      so `NotifierContract` runs on both pre-checks.*
- [x] **T022a** *red* `app/notifier_test.go`, and the Redis cases of `app/notifier_integration_test.go`:
      what `NotifierContract` does not reach. `NotifierContract` runs through the real store, which cannot
      say what was *not* called or in what order, so these use recording stand-ins for the ports in the
      test file (not the fake `Store` the kit refuses: they assert call order and arguments and claim
      nothing about leases or row-level security). Cases: `NewNotifier` names every missing port and
      refuses a configuration that does not validate; a notification, cancel or status request that cannot
      be right touches no port, and `NotifyTx` refuses a nil transaction; an unknown topic persists and
      remembers nothing; the pre-check is read, then the store runs, then the entry is written, under the
      frozen `notify:applied` key, for `PreCheckTTL`; a hit answers the replay without planning or
      persisting, for `NotifyTx` too; a pre-check that errors, on read or on write, is a miss and never
      fails `Notify`; nothing is remembered for a replay, a failed commit or a `NotifyTx`, and a first
      apply by `Notify` is (the control); the store gets the identity built from the engine's realm and
      the request's tenant and recipient, the resolved priority, the plan, and the caller's own
      transaction for `NotifyTx`; `Cancel` and `Status` ask for the engine's realm; and, on Redis:
      `NotifierContract` again, the entry under the frozen key with the configured TTL, a replay still found
      by the guard after Redis is flushed, and a replay the pre-check knows opening no transaction (NR-002,
      NR-004, NR-009, NR-027, NR-030, NR-032, [D18](design-decisions.md#d18),
      [D28](design-decisions.md#d28)). Written against a `Notifier` that returns `errors.New("not
      implemented")`
- [x] **T022** *green* `app/notifier.go`: `Notify`, `NotifyTx`, `Cancel`, `Status`; realm from config,
      never the request (NR-009, NR-027, NR-030, NR-032). Make T024 pass one case at a time, with T021,
      T023a and T023 in place; add `var _ ports.Notifier = (*Notifier)(nil)`.
      **Mutate:** take the realm from the request; return `Applied == true` on a replay; treat a pre-check
      hit as authoritative and skip the guard; write the pre-check before the commit
      *Built as `app.Notifier`, with 24 rows in the [ledger](../../docs/mutations.md), none surviving;
      `NotifierContract` passes on the in-memory pre-check and on Redis, and `Broadcast` says it is T042's.
      The contract was silent on what a pre-check hit does, and the choice is in `Notifier.notify`'s doc
      comment: a hit answers the replay on its own, with no plan and no transaction, because it was written
      only after a commit, under the durable guard's key, for no longer than the window, and it is read to
      be used (D18 says `NotifyTx` reads it too, which is only worth doing to skip work). The receipt it
      gives names the notification and not whether the inbox shows it, because the entry holds an id and
      nothing else. A miss, and a fault in reading, is only a miss: the guard inside the store's transaction
      is the one thing that decides a key is new, so a lost Redis costs a transaction and never a
      notification (`TestNotifierStillFindsAReplayAfterRedisIsLost`). So the line above, "treat a pre-check
      hit as authoritative and skip the guard", is the design and not a mutation of it; what can go wrong
      is a miss believed, which the structure makes unwritable, and the ledger records the nearest, a lost
      Redis answered as new. Only a first apply by `Notify` is remembered, never a replay (the receipt does
      not say how old the guard row is, and an entry that outlived it would answer "replay" past the
      window), a failed commit or a `NotifyTx`. `NewNotifier` validates the whole configuration, as the
      contract's `New` does, and names every port it lacks.*
- [x] **T017a** *red → green* `app/inbox.go`: the driving `Inbox` over `InboxStore` and
      `TemplateRenderer`, rendering each item at read time in the viewer's locale and falling back to the
      stored title and body (NR-019, [D27](design-decisions.md#d27)). [plan.md](plan.md#architecture)
      lists this file and no task built it; the REST surface (T044) and the in-process run (T043) both
      stand on it.
      **Red first:** an integration table on the T013a `Env` (the real `InboxStore`, the `Templates`
      fake): an item is rendered from its topic's in-app template in the requested locale; a locale with
      no template falls back along the chain T048 fixes; a topic with no template shows the stored title
      and body; a template edited after the notification was stored changes what the next read shows (the
      failure D27 names).
      **Mutate:** render at send time and store the result; ignore the requested locale; fall back to an
      empty string
      *Built as `app.Inbox`, with 22 rows in the [ledger](../../docs/mutations.md), none surviving. It renders
      the store's page: each item from its topic's template (the topic's `TemplateRef`, else its name) for
      the registry's inbox-backed channel, found by the capability and not by a kind, in the asked locale or
      the configured default, with the viewer's realm and tenant and the notification's own data. The
      renderer owns the locale ladder (tag, language, default); "no template" keeps the stored title and
      body, and any other renderer error is returned, because a template store that is down is for an
      operator and a stored copy shown in its place would hide it. A page looks each topic up once, and
      with no inbox-backed channel registered the renderer is not asked. `Unread` is bounded at
      `UnreadCap`; the marks are the store's, passed through with its errors. The preference, schedule and
      unsubscribe methods of `ports.Inbox` are T044's and T026's: they return an error that says so rather
      than nil, since a nil would tell a recipient their choice was saved, and `SetSchedule` must call
      `DeliverySchedule.Validate` when it is built. `notifytest.Env` gains `Inbox`, the real
      `postgres.Inbox` on the Env's clock.*

## Stage 5 — Channels

- [x] **T029** *red* **Contract test** `ChannelContract`: the five cases in the contract, plus each
      channel reporting its declared `Dedup` level (NR-005, [D32](design-decisions.md#d32)). Run it first
      against two deliberately broken channels — one that declares `DedupProvider` and sends twice, one
      that returns an error for a transient failure — and confirm it fails for each; only then is it a
      test. It then runs against both reference channels, and against the email provider's sandbox under
      `-tags provider`
      *Built as `notifytest.ChannelContract`, with `ChannelDedupContract`, the part that injects no failure
      and so the part a provider's sandbox can answer. The suite is data (`channelCases`) written against a
      small `contractT`, so `TestChannelContractFailsABrokenChannel` runs it in process against 21 channels,
      each broken in one way (the two the task names among them), and requires the exact set of cases that
      should fail to fail: the evidence that it can fail is a standing test and not a one-off run. It
      departs from the sketch in the contract in two ways. `newChannel` returns a `ChannelUnderTest`, the
      channel and its probe plus, when it needs them, the key the provider side counts a send under (the
      in-app effect is keyed by the notification, not the delivery), how to arm a failure on every member of
      a composite, and how many provider calls one failed delivery may make. And it is stricter and wider:
      a channel with `NeedsAddress` false must never report `Suppressed`, since the engine would suppress the
      empty address hash for the whole realm ([D13](design-decisions.md#d13)); a `Rejected` must carry a
      `Detail`; a fault the provider did not classify is `Retry` or an error and never a verdict; a window is
      declared only with `DedupProvider`; and a channel does not retry inside
      ([D10](design-decisions.md#d10)). 16 mutations of the suite itself are in the
      [ledger](../../docs/mutations.md); five survived their first run, because the fixtures did not isolate
      an assertion another one also caught, and now die. `make test-provider` runs the dedup cases against
      Resend's sandbox and fails loudly with no key. *Run against the real provider on 2026-10-13 it
      passed, and with a fresh key per call it failed on the re-drive case. Direct calls agreed (one key
      twice, one id; a changed body, `409 invalid_idempotent_request`). The 24-hour window itself was not
      measured.*
- [x] **T028** *red → green* `redis/stream.go`: sharded pub/sub on hash-tagged keys.
      **Red first:** against Redis under Testcontainers — an identity's keys share one cluster slot (CRC16
      of the hash tag); the same recipient id in two tenants gets two keys, checked against the frozen
      stream-key vector; a subscriber on one identity receives nothing published to another.
      **Mutate:** build the key without the tenant; drop the hash tag
      *Built as `redis.Stream`. `Publish` is `SPUBLISH` on `domain.StreamKey` and the payload is the id and
      nothing else. `Subscribe(ctx, identity)` returns a `Subscription` once the server has confirmed it, and
      `Close` returns when the server has dropped it. *The relay (T044) is a driving adapter and cannot import
      this package, so the subscribing side is now `ports.StreamSubscriber`, held by
      `notifytest.StreamContract`, which the Redis stream and the kit's in-memory `Stream` both run.* A fault no retry fixes, an ACL that refuses the
      command, a server without it, an identity or id with nothing to nudge, wraps the new
      `domain.ErrPermanent`, which the in-app channel reads as `Rejected`; every other fault is the transient
      kind, and only a Redis *server's* own error text is read for it. The suites found something about the
      kit: **Redis pub/sub is global to the server, not to a logical database**, so `redistest`'s private
      database isolates a test's keys and not its channels. The suites publish as identities of their own (a
      realm derived from the database), one sequential test owns the three frozen literal keys (computed with
      Python's hashlib and a CRC16 written from the cluster specification), and `redistest`'s documentation
      says so. 16 mutations in the ledger; two survived their first run (a refused subscription and the order
      of `Close`) and now die.*
- [x] **T025** [P] *red → green* `channel/inapp`: nudge-only publish on the identity stream key;
      `InboxBacked` ([D16](design-decisions.md#d16)).
      **Red first:** `ChannelContract`, plus: the published payload is an id and nothing else, no content
      and no count (NR-024); it is published on the identity's stream key and no other; `InboxBacked` is
      true.
      **Mutate:** put the title or the count in the payload
      *Built as `inapp.Channel`, over `ports.StreamPublisher`: the nudge is the notification id, published to
      the delivery's own identity. A stream refusal that wraps `domain.ErrPermanent` is `Rejected`, any other
      fault `Retry`. **One decision the contract does not make:** `domain.Notification` has no id, so a digest
      delivery's members carry none, and the sketch's `Publish(ctx, id, d.NotificationID)` would send `""`,
      which the stream refuses as permanent: every digested in-app delivery would be dead-lettered. The
      channel succeeds without a nudge for a digest (their inbox rows are visible already). *That was the
      planner's open question, and [D39](design-decisions.md#d39) closed it: an inbox-backed channel is
      never digested, so that branch is now defensive.* `notifytest.Stream` is the fake, with its own table test and 7 mutations. The channel is also
      run over the real Redis stream, for the bytes on the wire and the tenant boundary
      (`inapp_integration_test.go`). 17 mutations in the ledger, none surviving.*
- [x] **T026** [P] *red → green* `channel/email`: template content, RFC 8058 headers and footer for
      non-essential topics, provider idempotency key, truthful `Dedup` per provider (NR-005, NR-015,
      [D34](design-decisions.md#d34)).
      **Red first:** `ChannelContract`, plus golden-file tests of the rendered message: a non-essential
      topic carries `List-Unsubscribe`, `List-Unsubscribe-Post: List-Unsubscribe=One-Click` and the footer;
      an essential topic carries none of them; the provider is given `Delivery.IdemKey`; a provider's
      declared `Dedup` is no stronger than its sandbox shows. This is also the first step toward the
      `unsubscribe` and `webhooks` Playwright specs.
      **Mutate:** omit the header on a non-essential topic; add the footer to an essential one; declare
      `DedupProvider` for a provider that does not dedupe
      *Built as `email.Channel` over a `Mailer` and an `UnsubscribeLinks`, with two mailers: `email.SMTP`
      (Mailpit in the e2e stack, any relay: `DedupNone`; STARTTLS when offered with the certificate checked
      and never a fall back to clear text; header injection refused; failures classified by RFC 3463 status
      wherever in the conversation they arrive) and `email.Resend` (`DedupProvider` for 24 hours, from
      Resend's own documentation; its error reference mapped to outcomes). The channel's `Dedup` is its
      mailer's and no stronger. A topic the registry does not know is treated as non-essential, so doubt falls
      on the side of the unsubscribe link; a registry or link fault waits as `Retry` rather than sending
      without the headers. Provider-wide faults (a revoked key, an unverified domain, a refused sender) are
      reported as transient, not as a refusal of the message, so a failover chain can route around them. The
      footer is plain English with `WithFooter` to replace it. **Not built here:** the signer behind
      `UnsubscribeLinks`, which is T026a, and the webhook that turns a Resend bounce into a suppression,
      since `POST /emails` cannot report one, which is T044's and needed before the `webhooks` spec can run.
      No `text/plain` alternative part is sent.
      73 mutations in the ledger: 55 died at once, 11 did not build and were rewritten, and nine survivors,
      over two passes, each exposed a missing test, now written; one real bug was found on the way (the connection deadline was
      taken from the injectable clock). `-tags provider` is as described under T029.*
- [x] **T026a** *red → green* `domain/unsubscribe.go`, `channel/email/links.go`: the unsubscribe token and
      the link that carries it (NR-015, [D34](design-decisions.md#d34)). T026 wrote `UnsubscribeLinks` as an
      interface and left it without an implementation, so the email channel could not be wired; this is the
      signer, the verifier T044 calls, and the links the channel asks for.
      **Red first:** a frozen token computed with Python's `hmac`, `hashlib`, `base64` and `json`; a table that
      alters each field of a signed token and keeps its signature; key rotation (an old key still verifies
      the mail already sent, a retired one does not, one id relabelled to another key does not); the expiry
      to the second, and a 60-day floor on the configured life; a table of what is not a token, none of which
      may panic or verify; a signed payload that is not a whole scope; and for the links, the URL's shape
      under a prefix and a trailing slash, an HTTPS-only base, and the pair and the date being the
      delivery's own and the moment it is made.
      **Mutate:** leave the key id out of the MAC; accept a token one second after it lapses; accept plain
      HTTP by default; build the token for another pair than the delivery's
      *Built as `domain.UnsubscribeSigner` (`Sign`, `Verify`) and `email.SignedLinks`. The token is
      `v1.<key id>.<base64url payload>.<base64url HMAC-SHA256>`, the MAC over the key id and the payload
      under a domain separator and checked in constant time before the payload is parsed; the payload is the
      pair and the second it lapses. A token the signer did not make is `ErrInvalidUnsubscribeToken` and a
      genuine one that has lapsed is `ErrExpiredUnsubscribeToken`, and the REST handler (T044) answers both
      with a 410. Found by the tests: Go's base64 decoder skips CR and LF, so a token with a trailing
      newline verified; the alphabet is now checked first. The length cap is observable only by a token with
      a valid signature, so its test builds one. 27 mutations of the signer and 17 of the links are in the
      [ledger](../../docs/mutations.md). The MAC comparison's *timing* is not measurable in a unit
      test; its primitive is, and `TestUnsubscribeVerifyComparesTheMACInConstantTime` reads the source and
      fails `bytes.Equal`, `bytes.Compare` and `==` on a MAC (four mutations survived every behaviour test
      before it, and the checker is run against sources that get it right and wrong). The signing keys come
      from the host's wiring; where the service reads them from is T045's.*
- [x] **T027** [P] *red → green* `channel/failover`: ordered providers, weakest `Dedup` of its members
      ([D29](design-decisions.md#d29)).
      **Red first:** a table over every pairing of member `Dedup` levels showing the composite declares
      the weakest; on `Retry` from one provider the next is tried inside the same attempt; the first
      provider's `Delivered` stops the chain; `ChannelContract` over the composite.
      **Mutate:** report the strongest level; try the next provider after `Delivered`
      *Built as `failover.Channel`. Each attempt starts at the first provider. A delivery, a `Suppressed` or a
      `Rejected` ends the chain; a `Retry` or an error tries the next. When none can send the result is
      `Retry`, floored at the earliest `RetryAfter` any provider offered (none if any offered none), with
      what each said in `Detail`. It declares the weakest `Dedup`, and the shortest window only for
      `DedupProvider`. **A limit that is the design's, not a bug:** a delivery that the first provider failed
      and the second sent can, if its record is lost to a crash, be re-driven from the first, which has not
      seen the key; the composite keeps no record of which provider sent, so no level above the weakest is
      honest, and none stronger than the members' is claimed. **A reading the spec leaves open:** only a
      transient result tries the next provider, so a provider that reports its own account trouble as
      `Rejected` would stop the chain; the mailers here report that as transient. 28 mutations in the
      ledger; three survived their first run and now die.*

## Stage 6 — Dispatcher

- [x] **T033** *red* **Contract test** `DispatcherContract`: every crash point, poison terminates
      including worker crashes, fan-out retries per address, expiry, fallback, fencing (NS-003, NS-006,
      NR-007). Write each named case from the contract, driving each crash with the T013a hook at the
      step where it happens (after claim before send, after send before record, and the rest of the
      nine), against a `Dispatcher` that returns `errors.New("not implemented")`, before any dispatcher
      code exists. Also: a fenced outcome records nothing and increments `notify.lease.lost`; a delivery
      that reaches the attempt ceiling, and one refused permanently, is parked as a dead letter with its
      reason and last error and raises `notify.dlq.dead.count`, while a `suppressed` or `no_address`
      ending raises nothing (NR-007)
      *Built as `notifytest.DispatcherContract`, in the kit so the transport suites reuse it: 70 cases on the
      Env's real queue and store. Written against `app.Dispatcher` returning `errors.New("not implemented")`,
      with the seams it needs already in (`ports.Metrics`, `StepHook`, `Waker`, `WakeSource`; `Env`'s
      `Suppressions`, `Metrics` and `Dispatcher`): the first 55 cases all failed on their first assertion
      (`Drain: not implemented`, and for the crash table "the worker did not die at <step>"), none on a
      fixture. The nine crash points are a table over a channel with an address and one without, because
      in-app has nothing to do at the address step and its only write is the last. "Poison terminates including
      worker crashes" is two cases and a decision ([D42](design-decisions.md#d42)): a delivery that kills its
      worker on every attempt is finished unsent at the ceiling, which needs the ceiling enforced where a
      crash is seen and not where a failure is recorded, and a channel that panics is a retry. A stale worker
      is shown twice, with a delivery and with a refusal, because the second is the one that would raise the
      alarm for a dead letter that was never written. The kit gained `Metrics`, `Scripted` (a channel that
      answers from a script and remembers what it was asked), `Normalizing`, and
      `ChannelUnderTest.Address`, each with its own table test. The contract passed on its first run
      against the implementation, so its worth is shown by mutation and not by that: 10 rows are its own (each
      of the nine steps unreached, and the record step reached before the send) and most of T030's and T031's
      are killed by it. The first pass left three survivors, none of them a dispatcher bug: nothing
      observed what the renderer was asked for a digest, and no channel in the suite had a normalizer of its
      own (`TestADigestIsRenderedFromItsMembers`, "address equality is the channel's"). A real `Run` over 24
      recipients in several shards, stopped in the middle of a send, is
      `TestRunDrainsEveryShardAndRecordsWhatWasInFlightAtShutdown`.*
- [x] **T030a** *red* `app/dispatcher.go`'s loop, for what T033 does not reach, as unit tests on the
      fake clock with no sleeps ([D22](design-decisions.md#d22)): polling backs off to `PollInterval`
      when idle and resets on work; an in-process `Notify` wakes an idle dispatcher at once;
      `LISTEN/NOTIFY` is off by default. Written against a `Dispatcher` with no loop, so each fails on
      its assertion
      *Built as `app/dispatcher_test.go` on `testing/synctest`: waits are virtual, so a test says when each
      pass began to the millisecond and nothing sleeps. The queue behind it is a recording stand-in in the test
      file, which claims nothing about leases. The wait is the floor (10ms) after work, none after a full batch,
      and after an idle pass a wait that doubles to `PollInterval`; a wake or a signal from the listener ends it
      and starts the doubling again, and a wake sent while a pass runs is kept. The in-process wakeup is
      `Deps.Waker`, which the `Notifier` calls after a commit by `Notify`: not for `NotifyTx` (the host's
      commit may not have happened, and a dispatcher woken then finds nothing and sleeps), not for a replay or a
      failed commit, not for a pre-check hit (`TestNotifyWakesTheDispatcherOnlyAfterAFirstApply`).
      Four of the tests pass against a stub by construction (`LISTEN/NOTIFY` off by default, `Wake` never
      blocking, `Drain` refusing an impossible batch, a `Notifier` with no waker); the mutations that turn
      `LISTEN` on, make `Wake` block, accept the batch and wake for `NotifyTx` are what show them able to fail.
      23 rows in the [ledger](../../docs/mutations.md), none surviving.*
- [x] **T030** *green* `app/dispatcher.go`: the nine steps in the contract, adaptive polling, in-memory
      wakeup, optional `LISTEN/NOTIFY` ([D22](design-decisions.md#d22)). Make T033 and then T030a pass
      one case at a time, from the plain delivery outwards.
      **Mutate:** count the attempt at finish instead of claim; skip the lease token on a write; retry
      without the `RetryAfter` floor; skip the alarm on a permanent refusal; raise it for a `suppressed`
      ending; never reset the poll interval on work; enable `LISTEN/NOTIFY` by default
      *Built as `app.Dispatcher` (`Run`, `Drain`, `Wake`), `app.WithRand` and `app.WithErrorHandler`, with
      `Deps` gaining `Metrics`, `Steps`, `Waker` and `Listener`, and `ports.Metrics`, `StepHook`, `Waker` and
      `WakeSource`. The contract was silent on six things and the choices are [D42](design-decisions.md#d42):
      the ceiling is enforced at claim, so a claim beyond `MaxAttempts` is finished unsent; every fault in a
      port the engine does not own is a retry, a panic in a channel, renderer or address book included, and a
      quota counter that cannot answer fails open; an outcome is recorded on a context that outlives the
      worker; a channel-reported suppression is written before the delivery is finished; a bound address is
      checked against the suppressions at every claim; and a claimed batch is handled concurrently, because its
      lease runs from the claim and 100 sends in turn outlive `ClaimLease` although no one of them does.
      `LISTEN/NOTIFY` is `postgres.WithNotify` on the store (a `pg_notify` in the transaction that writes the
      queue rows, so a rollback announces nothing) and `postgres.Listener` on a connection of its own, taken out
      of the pool and closed when it ends; it is a latency optimisation, and a dispatcher whose listener fails
      polls on (`TestListenNotifyGetsAnIdleDispatcherClaimingAtOnce` puts the next poll 2.5 s away and sees the
      delivery inside 1.5 s). Of T030's "Mutate" list, "count the attempt at finish instead of claim" has no
      counterpart in the dispatcher, since the queue counts at claim and that is T016's row; its counterpart
      here is the ceiling check removed. The others each have a row. The tests for what a port's fault becomes
      (`TestAFaultingPortIsARetryAndNeverALostDelivery`, nine ports) and for `Run` were written after the
      dispatcher, because they pin choices the contract does not make; they stand on their mutations. 75 rows,
      three of which survived their first run (the gaps named under T033), none now.*
- [x] **T031** *red → green* Suppression check before every delivery; channel-reported suppressions
      recorded with expiry (NR-006, [D13](design-decisions.md#d13)).
      **Red first:** a live suppression means the channel is never called, on every channel kind, and the
      outcome is `suppressed`; a `Suppressed` result writes the suppression with `SuppressFor` as its
      expiry and then finishes; an unresolvable address ends `no_address`, distinct from `suppressed`.
      **Mutate:** check suppressions only for email; write the suppression after the finish
      *Built in the dispatcher's steps 4 and 8 (`Dispatcher.address`, `Dispatcher.record`). Red was
      witnessed by taking the feature out, not by a stub, because the dispatcher was written whole: with the
      check turned into `return false`, cases of the contract failed on their assertions ("the slack
      channel was asked to send to a suppressed address"), and with the write moved after the finish the case
      "a worker that dies right after a Suppressed ending has already recorded the suppression" failed
      ("a delivery that ended as suppressed left no suppression behind"). That case dies at the fence step,
      which is reached after the last write, so the order of the two writes is observable without a hook between
      them. Beyond the task's list: the check applies to a bound address again at every claim, so a bounce that
      lands while a delivery waits for its retry is honoured; a suppression is per realm and per channel and
      ends at its expiry; the channel defines address equality, so the key, the check and the record all use
      its normalizer; a channel with no address that reports `Suppressed` writes nothing, since the empty
      address hash would silence it for every recipient of the realm; and a reason that is not one of the six is
      recorded as `invalid_address`, the least a channel reporting a dead address can have meant. 13 rows, two of
      them survivors that were gaps in the suite (no channel had a normalizer), killed after.*
- [x] **T032** *red → green* Dead-letter replay (`Admin.ReplayDeadLetter`), once per dead letter.
      **Red first:** a dead letter is inspectable with its reason and last error (NS-006); replaying it
      enqueues one delivery; a second replay, and two concurrent replays, enqueue nothing more.
      **Mutate:** drop the replay guard
      *Built as `Maintenance.ReplayDeadLetter` (`postgres/deadletters.go`, one statement with no `ON CONFLICT`)
      and `app.Admin`, which also holds `Erase` and `SetTenantPreference` as methods that say which task builds
      them (T040, T044), because a nil would tell an operator that a recipient was erased. The port gained the
      method and two sentinels, `ErrUnknownDeadLetter` (none, another realm's, or not an id: one answer, so
      an id cannot be probed across products) and `ErrNotReplayable` (a dead letter the bus parked). The
      choices are [D43](design-decisions.md#d43): the fresh delivery starts over but is the same delivery, with
      its bound address and its deadline, and no fallback chain. Eight replayers on one dead letter enqueue
      one delivery. "Inspectable" is asserted on the dead letter's row, which is what an operator's query reads
      (`reason`, `last_error`, `channel`, `attempts`); the list endpoint that reads it is T044's. Written against
      a `ReplayDeadLetter` that returned "not implemented": 10 of the 11 tests failed on their first assertion
      and the eleventh is the parent of the refusals table. 20 rows.*
- [x] **T033a** *red → green* NS-008 demonstration: add a third channel (`channel/webhook`, as laid out
      in the plan) with no diff to fan-out, persistence or preferences.
      **Red first:** register the channel in the `Env` of `NotifierContract` and `DispatcherContract` with
      one line, and run both suites unchanged; they fail until the channel exists and pass without a change
      to `app/`, `adapters/driven/postgres/` or preferences. The evidence is the PR's `git diff --stat`:
      new files plus one registration line (NR-016, NS-008).
      **Mutate:** import `channel/webhook` from `app/`; `make arch-lint` must go red. Branching on the
      `webhook` kind string inside `app/` compiles and passes every suite, which is why the diff stat is
      the evidence and not only the suites
      *Built as `channel/webhook`: a signed HTTP POST to the address's URL, at-least-once by declaration
      (a receiver can dedupe on the `Idempotency-Key` it is sent), RichContent, addressed by URL. The address is
      a tenant's, so it is hostile input ([D44](design-decisions.md#d44)): https only, never a connection to a
      private, loopback, link-local, carrier-grade-NAT or reserved address (checked at the connection, after
      the name resolves), no redirect, no proxy, no credentials in the URL, and nothing it reports carries the
      URL. A 410 is a dead address for good and a 404 or a redirect a day's cooldown; a 401 or 403 is the
      customer's credential trouble and a retry ([D41](design-decisions.md#d41)); a 429 or 5xx is a retry
      floored by `Retry-After`; another 4xx is a refusal of the message. The registration is one line,
      `e.Channels.Register(wh)`, in `webhook_integration_test.go`, which runs `NotifierContract` and
      `DispatcherContract` unchanged with the channel registered and a topic that fans out on it to the end.
      **The evidence for NS-008:** the channel is the new files of `adapters/driven/channel/webhook/`, and
      this task changed nothing in `app/`, `adapters/driven/postgres/` or preferences. The one existing file it
      touched is the kit's `ChannelContract`, which made every delivery to `a<n>@example.com`; a channel whose
      address is a URL needed to say what its addresses look like (`ChannelUnderTest.Address`). `make arch-lint`
      goes red when `app/` imports the channel ("Component app shouldn't depend on ...webhook"), and a branch
      on the `webhook` string inside `app/` survives every suite, as the task said it would: it is the one
      mutation here a test cannot kill, and the diff stat is what holds it. **The red of "they fail until the
      channel exists" would have been a missing package, which is a compile error and not a red;** the witnessed
      red is `ChannelContract` and the channel's tests against a stub whose `Deliver` returned "not
      implemented" (85 failures on assertions). The end-to-end tests were written after the channel and stand
      on their mutations. 50 rows, three survivors, all closed: two test gaps (an IPv4-mapped address's
      prefixes, `Scripted`'s copies) and one equivalent mutant.*

## Stage 7 — Digest, quotas, fairness

- [x] **T035** *red* Burst test: N notifications yield `ceil(N / DigestMax)` deliveries and N rows
      (NS-005). A table over N = `DigestMax` − 1, `DigestMax`, `DigestMax` + 1 and 2 × `DigestMax` + 1,
      plus a `critical` burst and an essential burst that must never be digested (NR-014)
      *Built as `TestABurstYieldsOneDigestPerDigestMaxMembers` and `TestACriticalOrEssentialBurstIsNeverDigested`
      (`app/digest_integration_test.go`), on the Env's real store, queue and dispatcher: 99, 100, 101 and 201
      notifications give 1, 1, 2 and 3 deliveries, every member is in exactly one of them, none carries more than
      `DigestMax`, and every notification is a record. The critical burst is shown three ways (by the notification,
      by its topic's default, and an essential topic), each of `DigestMax` + 1. Written against a
      `DigestFlusher` whose `Flush` returned "not implemented": every case failed on `Flush = 0, not implemented,
      want 1 windows flushed`, none on a fixture. The critical and essential cases went further before they failed,
      since the planner already holds that rule (T021): no window opens and each notification is a delivery of its
      own. A window's flush time is the database's `now()`, as every due time in the queue is, so the tests age a
      window with the admin connection, which a fixture may use. 5 rows in the [ledger](../../docs/mutations.md),
      none surviving.*
- [x] **T034a** *red* The digest flush job, for what T035 does not reach (NR-014): two flushers of one
      window produce one delivery; a window overdue after a crash is flushed by the next run; a digest
      whose members are all canceled ends `canceled` and not delivered. Integration, on the T013a `Env`,
      written before `app/digest.go` exists
      *Built as `app/digest_integration_test.go` (the cases above, plus a lease held by another worker, a window that
      fails to flush, every shard and every batch, and a member canceled after the flush) and `app/digest_test.go`
      (the lease, the loop, the wake and the wiring, with no database and on virtual time). Two flushers of one
      window is four at once, and it is the store's idempotent flush that makes it one delivery; the lease is shown
      by its own test, since a flusher with no lease passes the first. The crash is a flusher whose `FlushDigest`
      panics: the window stays due, and the next run flushes it once the dead worker's lease has lapsed, which the
      test brings about by ending the lease with the admin connection. Written against the stub: all 21 tests failed on
      their first assertion (`Flush = 0, not implemented, …`, `NewDigestFlusher succeeded, want an error`). 1 row in
      the ledger of its own, the overdue window; the lease and the loop are T034's.*
- [x] **T034** *green* `app/digest.go`: append through `Digests.AppendDigest`, leased flush job, render
      members skipping canceled ones (NR-014). Make T035 and then T034a pass.
      **Mutate:** flush without the lease; render a canceled member; skip the overdue window after a
      crash
      *Built as `app.DigestFlusher` (`Flush`, `Run`), with `WithFlushInterval`, `WithFlushBatch`, `WithFlushOwner` and
      `WithFlushErrorHandler`. Two of the task's three parts were built already: the append is the persist path's
      (a notification is folded into its window in the transaction that accepts it), and the render is
      `Store.Load`'s, which the dispatcher reads at send, minus the canceled members (T015, T033). What was left is
      the part between, the window's flush time. The choices are [D46](design-decisions.md#d46): the job keeps no
      memory of what it meant to flush, so a window that came due while no one ran is late and not skipped; the
      lease is taken or renewed before every batch and not once per pass; a fault in one window or one shard is
      reported and does not hold the others back; a batch in which nothing could be flushed ends the pass and
      is not read again; a pass that queued work wakes the dispatcher of the process. The lease's owner name is
      the host, the process and a random suffix, so two flushers never share one. Of the task's **Mutate** list,
      "flush without the lease", "render a canceled member" and "skip the overdue window after a crash" each
      have a row (the last as a `DueDigests` that looks back ten minutes and no further). 13 rows, none
      surviving, three of which did not build on their first form and were rewritten.*
- [x] **T037** *red* Noisy-neighbour test for NS-007, written before the quota code. Integration on the
      T013a `Env`, with no sleeps and no wall-clock reads, so it cannot flake. One tenant is exhausted
      (`Env.Quotas`) and holds a due backlog many claim batches deep on every shard; a quiet tenant sends
      at a steady rate. The dispatcher is driven one pass at a time, and the quiet tenant's delay is
      counted in passes from due to claimed, a unit that does not move with the machine. The test runs
      the scenario twice, once with the neighbour exhausted and once with no tenant exhausted, and
      asserts NS-007's inequality: the first p95 is within 10% of the second. T033's exhausted-tenant
      case proves the mechanism with two rows; this proves the inequality at a backlog that spans many
      batches.
      **Red:** run it against the dispatcher as T030 leaves it, before T036 gives it backlog deferral.
      The quiet tenant waits behind the neighbour's rows, the ratio is far above 1.10, and both p95s go
      in the commit message as the failure line. The row-by-row deferral that D24 rejects is not a
      hypothetical build to red against; it is T036's first mutation, and it must send this test red too.
      The wall-clock p95 under real load is the fairness row of T055
      *Built as `TestAnExhaustedNeighbourDoesNotSlowAQuietTenant` (`app/fairness_integration_test.go`). A neighbour
      with an exhausted budget holds twelve due rows on each of the 16 shards, four claim batches of three; the quiet
      tenant sends one notification to a recipient on every shard in each of 30 passes, 480 in all. A pass is one
      `Drain` of a batch from every shard, which is what `Dispatcher.Run` repeats. **The comparison holds the quiet
      tenant's traffic fixed and varies one thing**: whether a neighbour with an exhausted budget and a deep backlog
      exists, the baseline being the quiet tenant alone. A neighbour that is not exhausted is a tenant with a
      backlog to send, and the quiet tenant waits behind it however the queue is built, so that comparison would
      not tell the designs apart. The measure is the passes it took to be claimed, so the baseline is exactly 1, and
      the test says so, since the measure means nothing if it is not. The budget runs out *after* the neighbour's
      rows were queued, or the planner, which peeks at enqueue, would have written them due at the reset and there
      would be no backlog. **The red the task describes cannot be witnessed against the dispatcher as T030 left
      it**, because T030 built the backlog deferral with the rest of the dispatcher and the test passes there. It
      was witnessed by taking the feature out, as T031's was: with the `DeferTenantChannel` call removed from
      `Dispatcher.exhausted`, which leaves exactly the row-by-row deferral D24 rejects, `p95 due-to-claimed: 1
      passes alone, 4 passes beside an exhausted neighbour`. Both p95s are in the commit message. With the backlog
      moved, only the quiet rows of the first pass on the first shard wait an extra pass, which is under 5% of 480.
      1 row in the ledger, the removal itself.*
- [x] **T036** *red → green* `app/quota.go` + `redis/quota.go`: peek at enqueue, take at dispatch,
      backlog deferral, `drop_non_essential` never for essential or critical (NR-023,
      [D24](design-decisions.md#d24)). Make T037 pass.
      **Red first:** a peek consumes nothing; a take consumes once, and a deferral returns the attempt;
      the realm default applies until a tenant overrides it; losing Redis resets the budget without an
      error; a table for `drop_non_essential` over {info, warning, critical} × {essential, not}.
      **Mutate:** peek consumes; defer row by row instead of moving the backlog (T037 must go red); drop
      an essential notification
      *Built as `redis.Quota`, `postgres.Quotas` (which reads `channel_quotas`), `app/quota.go` (the gate the
      planner's peek and the dispatcher's take both go through), `domain.QuotaLimits`, `QuotaWindow` and
      `QuotaCounterKey`, the new port `ports.QuotaLimits`, and `notifytest.QuotaContract`. The planner already
      peeked and the dispatcher already took, deferred and moved the backlog, against the kit's fake (T022,
      T030); what did not exist was a counter, and anything that supplies its ceilings. The contract was silent
      on what a budget is made of and the choices are [D45](design-decisions.md#d45): two fixed windows
      aligned to UTC, a take that spends both or neither in one script, a refusal that resets at the end of the
      longest spent window, ceilings read from `channel_quotas` with the tenant's row over the realm's and
      remembered for 30 seconds, a row of zero that allows none, and no rollover job, since the window is in the key.
      `QuotaContract` is the table the in-memory fake already passed (its own tests moved into it, as the
      preferences' did in T018) and the Redis counter must pass: a peek consumes nothing, a take consumes
      once, one budget is not another's, 80 concurrent takers take exactly the 20 given, and losing the state
      resets the budget without an error. Written against stubs that returned "not implemented", every new
      test failed on its assertion (`Limits(…) = {PerHour:0 PerDay:0}, false, not implemented, want {PerHour:10
      PerDay:100}, true`); `TestADeliveryTakesOneUnitAndADeferralTakesNone` passed already, on the fake, and
      is there for the system as a whole. "A deferral returns the attempt" is T016's (`TestFencedWrites`) and
      is asserted again here against the real queue. `app/quota.go` is a refactor with a test of its own,
      since the planner and the dispatcher each shadow its fail-open answers with a guard (the planner ignores
      a reset that is not in the future, the dispatcher reads the error before the answer), so a gate that
      failed the wrong way passed through both: the two mutations that showed it survived their first run. Of
      the task's **Mutate** list, "peek consumes" and "drop an essential notification" each have rows, and
      "defer row by row" is T037's. 46 rows, four of which survived their first run: two equivalent
      mutants (a redundant `.UTC()`, removed, and a tenant-kind test the table's `CHECK` makes redundant,
      kept) and the two gaps above, closed.*

## Stage 8 — Maintenance

- [x] **T039** *red* Test: retention succeeds while a dead-lettered delivery references an aged
      partition ([D6](design-decisions.md#d6)). Also the case moved from T020: retention with a pending
      delivery outstanding leaves that delivery drivable. Written against `RetirePartitions` before it
      exists
      *Eight tests in `adapters/driven/postgres/retention_integration_test.go` and a control for the witness. Retirement
      deletes no row: a `DELETE` trigger on the aged partitions writes to a witness table, which must stay empty
      (`n_tup_del` is not used, because a partitioned parent keeps no statistics of its own, a dropped partition takes its
      with it, and statistics reach the collector after the statement). Only a partition whose whole range the cutoff has
      passed goes, with the boundary cases; one whose range has not ended is never retired whatever the cutoff; only the
      three tables can be retired; the two T039 cases proper; and the bounded lock wait of
      [D47](design-decisions.md#d47), which also requires the error to say the partition was *skipped*.
      **Red, witnessed after the green:** Docker did not answer when these were written, so no red was seen first. With
      `RetirePartitions` restored to return nothing, they fail on their assertions
      (`RetirePartitions = [], <nil>, want the aged partition retired`; ledger row `retire-stub`), and with the code back they pass. **The witness:** the store's role has no recipient scope, so
      forced row-level security shows it no row of the inbox, and a `DELETE` by it deletes nothing and leaves the witness
      empty, so *delete, then drop* survives as a mutant and is equivalent for that role.
      `TestTheRetirementWitnessRecordsADeletion` shows the witness records a deletion by a role that can reach the rows,
      and fails when the witness is blinded, so an empty witness after a retirement means something.*
- [x] **T038** *red → green* `app/maintenance.go`: leased jobs for retention (inbox, history, dead
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
      *Built as `app.Maintenance` (`Pass`, `Run`, `Preflight`) over six jobs, `postgres.RetirePartitions` and
      `ExpireBroadcasts`, and the choices of [D47](design-decisions.md#d47) and [D48](design-decisions.md#d48); the three
      expiries are one function over three store methods. The use-case half is held by 19 tests over a recording
      `Maintenance` that keeps real lease semantics over a clock (each job takes its lease first, two workers contending
      run each job once, a lapsed lease passes the jobs on, a job runs no oftener than its interval, the partition job
      turns a `PartitionHealth` into the two alarms, and the check still runs when provisioning failed), red witnessed
      against stubs (with T040 and T040a's, all 28 new unit tests failed on an assertion). The PostgreSQL half and the end-to-end
      suite (`app/maintenance_integration_test.go`, which runs two workers on one database and checks who holds each
      lease) ran when Docker came back and are green; `make ci` is green. Mutations: 24 of the use-case code, and for the PostgreSQL side
      *retire with `DELETE`* (the task's own, caught by `TestRetirePartitionsDropsWholePartitionsAndDeletesNoRow`), the
      cutoff cap, the whole-range test at both its edges, the table check, the lock wait, and `ExpireBroadcasts`'s status,
      boundary and batch (the boundary survived until a fixture at exactly the cutoff was added). **One difference from the
      task text:** the metadata-operation witness is a `DELETE` trigger and not `n_tup_del`, for the reason given under T039.*
- [x] **T038a** *red → green* `TryLeaseJob` records every grant, the holder's renewal included, as the job's last start
      (`notify_job_leases.last_started_at`), so the `notify.job.stale` alarm of [operations.md](../../docs/operations.md)
      has something to read ([D50](design-decisions.md#d50)). Found when Stage 8 was reviewed: a renewal kept the column,
      so for a worker that keeps its jobs it never moved, and the alarm fired for every healthy job.
      **Red first:** with `last_started_at` backdated, a renewal by the holder moves it, a refused ask does not, and a
      take-over after a lapse does (`TestTryLeaseJobRecordsEveryGrantAsTheJobsLastRun`, no sleep).
      **Mutate:** keep the column on a renewal; move it on a refused ask
      *The test was committed before the fix, but Docker did not answer, so its red was witnessed afterwards: with the
      old `CASE` restored it fails with `the holder's renewal kept last_started_at, so the column shows the job last ran
      when worker-1 took it, and notify.job.stale fires for a healthy job (D50)`. Both mutations are killed (ledger).
      The alarm thresholds are per job, in a table in [operations.md](../../docs/operations.md#maintenance-jobs-and-their-leases).
      A job that takes its lease and fails every run is not stale, which is what T038b is for.
      No refactor was needed: it is one expression.*
- [x] **T038b** *red → green* `Maintenance.FinishJob(job, owner)` records a run that ended cleanly in
      `notify_job_leases.last_finished_at`, for the job's current owner only, and `app.Maintenance.Pass` and
      `DigestFlusher.Flush` call it, so a job that takes its lease and fails at every run can be told from one that works
      ([D52](design-decisions.md#d52)). Found when T038a was closed: `last_started_at` says a worker is taking a job and cannot
      say it is getting anywhere, and the error handler that does say so is the host's to wire.
      **Red first:** a run that ended cleanly records its end once, after its work; one that failed, was refused, lost its
      lease on the way or was stopped does not; a finish that cannot be recorded is reported and stops no other job; the same
      for the flush pass; in PostgreSQL only the owner records an end, a grant and a take-over leave the column alone, and a
      job and an owner are both required.
      **Mutate:** record the end of a failed job; of a job that lost its lease; of one cut short by a stop; for a worker that is
      not the owner; under the wrong name; ignore a finish that failed; let one stop the other jobs
      *Red witnessed first, for the use-cases and for PostgreSQL (Docker answered this time, so there is no departure from
      the rule as there was for T039): with the port method stubbed, the failures are
      `recorded the end of [], want [partition_provision retention idem_expiry ...]` and `the owner's finish was not
      recorded, so a job that works is indistinguishable from one that fails every run (D52)`, in the `test:` commit. The tests
      that say *not finished* pass against a stub, as they must, and are held by their mutants. 24 mutants, all killed (ledger),
      over the SQL, `Pass`, `Flush` and the end-to-end wiring; the end-to-end test
      (`TestAJobThatFailsEveryRunIsStartedAndNeverFinished`) runs a real database whose expiry always fails and finds that job
      started and never finished while the other five finished. Two guards were redundant and were removed, not kept
      unexercised. The alarm `notify.job.failing` is in [operations.md](../../docs/operations.md). **Not done:** the column does not
      say *why* a job fails (the error handler and the log do), and a run longer than its threshold also shows as failing,
      which is a thing to look at.*
- [x] **T040** *red → green* `app/erasure.go` + `Admin.Erase`; test NS-012.
      **Red first:** erase a recipient, then scan every table listed by the database catalog, not by the
      adapter, and find no row carrying the recipient; the erasure record keeps no identity; a suppression
      recorded for one of its addresses still blocks delivery to that address (NR-034).
      **Mutate:** skip a table; delete the suppression
      *`Admin.Erase` is built (`app/erasure.go`): it refuses an identity with an empty component before the store is
      asked, hands the identity to `Maintenance.Erase` once, and returns what was removed, and no counts on a failure.
      Held by three unit tests, red witnessed (all three failed on an assertion) and four ledger rows. NS-012 itself,
      `TestNS012AnErasedRecipientLeavesNothingAndTheirSuppressionStays`, ran green: it lists the tables from the catalog,
      insists on a fixture row in every one (so a table never filled cannot pass for one emptied), checks that Erase's
      counts equal the rows that were there and that a second recipient is untouched, that the record names no `u1`, `w1`
      or address, and that a later notification to the suppressed address is recorded as suppressed with the channel
      never asked. The task's mutations are killed by it and by `TestErase`: *skip a table* (`notify_idem` removed from
      the list) and *delete the suppression* (`Erase` also deletes the realm's suppressions).*
- [x] **T040a** *red → green* `app/maintenance.go`: `Maintenance.Preflight`, at startup and on a
      schedule, reports as warnings and never failures the database conditions the queue depends on —
      `max_wal_size` against the observed WAL rate, the connected role's
      `idle_in_transaction_session_timeout` and `statement_timeout`, the age of the oldest `backend_xmin`
      — and exports `notify.db.oldest_xmin_age` and `notify.db.checkpoints_requested` (NR-037,
      [D38](design-decisions.md#d38)).
      **Red first:** a role with no timeouts produces its warning; an open old transaction produces its
      warning; an undersized `max_wal_size` produces its warning; a healthy configuration produces none;
      and none of them stops the service starting.
      **Mutate:** return an error instead of a warning; read a stale `backend_xmin`
      *Built in three parts: the judgement in `domain` (`AssessDatabase`, `WALRate`), the job in
      `app.Maintenance.Preflight`, and the reading in `postgres.DatabaseConditions`, with the choices of
      [D49](design-decisions.md#d49). The first two are done and held, red witnessed, with 12 ledger rows (two
      survivors, both equivalent, so the redundant guards were removed) for the domain and kit and 7 for the job: *return
      an error instead of a warning* is killed. The PostgreSQL reading ran when Docker came back and is green, including
      the assumption the design rested on, that a read-committed session idle between statements holds no snapshot and no
      id, so it pins nothing. Its mutations are in the ledger: *every open transaction counts*, *only a snapshot counts*,
      *the preflight's own session counts*, *another database's transactions count*, *the youngest is read*, *a
      `pg_read_all_stats` member is not seen as seeing all*, and the three readings. *The youngest is read* and *a member is not
      seen* survived until the tests were given a second transaction, begun 250 ms after the first, and a role granted
      `pg_read_all_stats`. The task asks for the oldest `backend_xmin`
      and exports two names; the reading counts a transaction that holds a snapshot **or an assigned id**, in seconds,
      because an id pins vacuum as a snapshot does, and it adds a warning the task does not list, for a role that cannot
      see other roles' sessions, because a clean reading through one is a clean bill it has not earned. The gauges need a
      `Metrics` that can hold a level, which `ports.Metrics` (`Inc` only) cannot, so there is an optional
      `ports.GaugeMetrics`.
      **Closed by a follow-up, which is why the box is ticked.** Two gaps had kept it open. *Count backends that are not clients*
      survived because a test cannot make an autovacuum worker appear and stay; the query now runs, unchanged, against a table
      named `pg_stat_activity` in a schema the test database's owner searches before `pg_catalog`
      (`TestDatabaseConditionsCountsOnlyClientBackends`: an autovacuum worker, a walsender and a background worker an hour
      old are not counted, a client backend in the same table is, which is the control, and a worker older than a client
      does not hide it), and the mutant dies three ways. The PostgreSQL 17 branch had no test because the suite started 16:
      `PGTEST_IMAGE` now chooses the image, `make test-pg-next` runs the schema and integration suites on `postgres:17-alpine`,
      CI runs both majors, and `PGTEST_EXPECT_MAJOR` fails a leg that did not start the major it names. The whole suite and
      the schema assertions passed on 17.12 unchanged, and the 17 branch's mutants (the counters swapped, the wrong view
      read, on 17 and on 16) are killed. The tests that closed the gaps were written against code that already held, so their red
      is the mutant, not a failing run: the rows are in the ledger (36 for this follow-up). As for the red of this task's
      PostgreSQL tests, it was witnessed after the green, and T039's note says why.*
- [x] **T040b** *red → green* `Admin.Erase` forgets the pre-check's entries for the recipient's idempotency keys, so a
      producer retrying a key after an erasure is not told it is a replay of a notification that is gone (NR-034,
      [D51](design-decisions.md#d51)). Found when Stage 8 was reviewed: `Erase` removed the `notify_idem` rows and left
      the Redis entries, which live up to `PreCheckTTL`.
      **Red first:** `PreCheck.Forget(keys)` and `Maintenance.IdemKeys(identity, after, limit)` join the ports. `Erase`
      forgets the entry under `PreCheckKey(identity, key)` for each of the recipient's keys, in pages, **before** it
      erases; with no pre-check it reads no keys; an unreadable key list or a refused `Forget` stops the erasure before it
      begins; a page that does not advance is an error and not a loop. `PreCheckContract` gains `Forget`'s cases and runs
      against the fake and Redis (a list longer than a chunk, an unknown key, a forgotten key remembered again); the
      Redis adapter reports a fault once and returns nil; `IdemKeys` lists one recipient's keys, bytewise, a page at a
      time; and NS-012 shows the entries of the erased recipient gone and another's kept.
      **Mutate:** forget after the erasure; forget under the wrong key or identity; stop after the first page; skip the
      guard against a page that does not advance; ignore a refused `Forget`; do not wire the pre-check into `NewAdmin`
      *Unit red witnessed first (the failures are in the `test:` commit); the PostgreSQL and Redis tests were witnessed
      afterwards, by their mutants, because Docker did not answer. 24 rows in the ledger, all killed; one of them
      (`IdemKeys` ordering) survived first and died once the test gave the column a locale-aware collation. The first mutant to
      drop `IdemKeys`'s identity predicate died of a raw SQL error and was rewritten, and the rewrite showed that
      `notify_idem` has **no recipient policy**, so the predicate is the only bound on the read (a comment of mine that
      claimed otherwise was wrong, and is fixed). `IdemKeys` orders `COLLATE "C"`, and its
      test gives the column an ICU collation so the order is not one the database would give anyway. The refactor pass
      made the three expiries of `app.Maintenance` one function and the page-size probe of the tests a helper.*

## Stage 9 — Broadcast

- [x] **T042a** *setup* `internal/notifytest`: an in-memory `AudienceResolver` (`Audiences`) and the `Env`
      fields the broadcast suites stand on (`Env.Audiences`, `Env.BroadcastDeps`). A host selector names an
      ordered list of recipients in one realm and tenant; the resolver serves it in pages of a size the test
      picks (two, on the `Env`, so a small audience still spans pages), ends with the empty cursor, remembers every
      question it was asked, and fails on request. It is a fake of a port, so it is held to its behaviour by
      its own tests (NR-020, [D7](design-decisions.md#d7)).
      **Mutate:** the last page's cursor, an armed failure that is not spent, the call log, a page or a set that
      shares its slice with the caller, a cursor it did not issue, and the audience's key without its tenant or its
      realm
      *Seven tests of its own, eight mutants in the ledger, all killed. One of them (a cursor that is not a number)
      first died of a slice-bounds panic inside the fake, which shows nothing, and was rewritten to fail on the
      assertion.*
- [x] **T042** *red* Test: a crash mid-broadcast resumes at the next page; a retried broadcast
      double-notifies nobody. Cases: a kill between pages, and a kill inside a page's transaction (the page
      rolls back as a whole); a per-recipient key derives deterministically from the broadcast key
      (frozen vector); an inline list over `MaxInlineRecipients` is rejected (NR-020)
      *Two tests in `domain` (`BroadcastRequest.Validate`, the rules a request must meet to be recorded), 23 in `app`
      (the front door and the expander against a recording stand-in for the three broadcast methods), 23 in
      `adapters/driven/postgres` (create, lease, the fence, the page's one transaction) and 10 end to end on the
      `Env`. **Between pages:** the worker's context ends once the first page has committed, and the next worker's
      resolver is asked for the cursors `2` and `4` and never for the start again. **Inside a page:** a trigger on
      `notifications` raises at the fourth recipient, so the page's first write, its guards and its cursor advance
      roll back together, the dead worker's lease is still held, and once it lapses the page is redone from its
      own cursor (the resolver is asked for `""`, `2`, `2`, `4`). **A retry:** the same key twice before expansion
      and again after it records and notifies once; the member key's frozen vector already stood
      (`TestDerivedKeys`), and the page-size test shows a recipient's key does not depend on the page that carries
      them. **Red, witnessed where the code under test could reach an assertion:** the domain cases and the
      app's unit cases fail on their assertions (the lines are in the `test:` commit). The PostgreSQL and end-to-end
      cases stop at the stubbed port's first call (`CreateBroadcast: not implemented`), which is the device this
      stage's setup rule describes, so their guarantees are witnessed by their mutants, as T040a's were. Two cases
      were added before T041 (a page keyed any way but by the broadcast's member keys is refused; a non-final page
      needs a cursor), and five after the first round of mutants (see T041).*
- [x] **T041** *green* `app/broadcast.go`: durable record, leased paging, cursor committed with each
      page, inline recipients bounded by `MaxInlineRecipients` (NR-020, [D7](design-decisions.md#d7)).
      Make T042 pass.
      **Mutate:** commit the cursor in a separate transaction from the page; derive a key from the page
      number
      *`Notifier.Broadcast` validates and records and returns; `BroadcastExpander` leases a page, resolves it, plans
      each recipient as `Notify` does under `BroadcastMemberKey`, and commits it; the store's three methods do the
      rest in SQL ([D53](design-decisions.md#d53) records the choices the spec left open: a lease per page that the
      commit releases, the fence and the advance in one statement, `fanned` counting what was written, the store
      refusing a page whose keys are not the member keys). **Mutants:** 60 for this task and 8 for T042a, 73 rows in
      the ledger with the survivors' first rows, among them the two this task names: the cursor on the pool and not in the page's transaction
      dies on `TestAPageThatFailsInsideItsTransactionRollsBackAsAWhole`, and a key derived from the page's cursor
      dies on `TestAMemberKeyDoesNotDependOnThePageThatCarriesIt` and on the store's own member-key check. **Five
      survived the first round and were gaps in the tests, not the code:** a planning fault (a page committed short),
      the fence's `status = 'pending'` (a canceled broadcast still holding its lease), the replay's read by tenant
      and by realm (the other tenant's row was recorded after, not before), and `SKIP LOCKED` (nothing held a row
      while another worker leased). Each has a test now and a second row showing it dies. Four mutants did not
      build and were rewritten (a fifth, T042a's, is above). **Refactor pass:** the idempotency-key rule is one function for a notification and a
      broadcast; `resolve` returns whether the page is the last; the store's check of a page's tenant, which
      duplicated `validatePersist`'s, went (its mutant was equivalent). The expander's page size, lease and
      interval are options, not `Config`, because the contract fixes `Config`. A broadcast is not under
      `app.Maintenance`'s `notify_job_leases`: its lease is on its own row, so it is its own loop until the one-call
      `New` composes it. **The contract follows the code** (`contracts/notification-ports.md`, confirmed by the
      maintainer): two sentences that said the broadcast methods "arrive with T041" are gone, the three sentinels
      (`ErrTooManyRecipients`, `ErrAudienceUnsupported`, `ErrBroadcastLeaseLost`) are listed, and the `Store`
      comment carries the lease, fence and refusals that `ports/driven.go` already said. Held by
      `TestCommitBroadcastPageIsFencedByTheLeaseToken` and `TestALostLeaseIsNotAFaultAndThePassGoesOn`.*

## Stage 10 — Transports

- [x] **T045** [P] *setup* `api/notifyv1`: the proto from the contract. It is listed first because the
      gRPC run of T047 and T046 are both written against its generated code. Generated code is data
      with no test of its own; a lint and breaking-change check on the proto runs in CI
      *The proto is the contract's, verbatim, in a buf module at `api/proto` (so a file's directory matches its
      package); the Go it generates is committed at `api/notifyv1`, so a host that imports the module needs no
      protoc. Three gates, each shown to fail ([ledger](../../docs/mutations.md)): `make proto-lint` (buf, `BASIC`
      rules: `STANDARD` would rename the locked RPCs), `make proto-check` (regenerates, and fails when the
      committed code was stale) and `make proto-breaking` (against the base branch; it says so, and passes, where the
      base has no proto yet). CI has a `proto` job that installs the Makefile's pinned versions; the `api` and
      `driving` components are switched on in `.go-arch-lint.yml`.*
- [ ] **T047a** *red* Enable the Playwright suite: remove `test.describe.fixme` from the `e2e/` specs as
      the routes they drive land, implement `/healthz` and `/readyz`, and switch on the `e2e` CI job
      ([docs/testing.md](../../docs/testing.md)). This is the standing outer loop and stays open until
      T049 lands. Take one spec at a time: remove its `fixme`, see it fail because the route or the
      service is missing, then land the route
      *Progress, and why the box stays open. **Done:** `/healthz` (asks nothing, so a database that blinks does not
      restart a healthy process) and `/readyz` (503 naming the check that failed and nothing of its error), with
      `httpapi.PartitionsCheck`, which fails only for this month's missing partition; 7 mutants in the ledger.
      **Not done:** removing any `fixme`, and switching on the `e2e` job. Every spec drives a running service,
      and nothing assembles the packages into one: there is no `cmd/notifyd`, and service-mode authentication
      (recipient-token verification, realm bindings, operator tokens, T049 and T050) is what `global-setup.ts`
      mints tokens for. A spec enabled now could not be seen failing for the right reason, only for a missing
      binary, so none was. The REST routes the specs drive are served by `httpapi` (T044) and are held by its handler
      tests; the `webhooks` spec also needs the Resend decoder (T044a) and the `unsubscribe` spec needs
      `PUT /admin/recipients/addresses` (T048).*
- [x] **T047** *red* **Contract tests** `NotifierContract` through gRPC, and `StreamIsolationContract`
      against the relay (NS-001). The gRPC run is the same suite as T024 with a different `Env`, built on
      T045's generated code; until T046 lands its `Notifier` is a stub returning
      `errors.New("not implemented")`, so the run fails on each assertion and not on a missing package.
      The kit's `Relay`, an SSE client for a base URL, is built here as part of the red's setup; until
      T044 lands nothing serves the route, so `StreamIsolationContract` fails on its first assertion
      *`TestNotifierContractOverGRPC` (in memory and on Redis) is `NotifierContract` unchanged on an Env whose
      `Notifier` is the gRPC client of a server over the Env's own, with `notifytest.Remote()`: the three `NotifyTx`
      cases become the one a remote Notifier owes (`ErrTxUnsupported`, nothing written), and the failed-commit case
      asks for an error and not the injected one, since a producer is never shown the engine's internals. **Red,
      witnessed on the stub client:** `notifiercontract.go:175: Notify(k1): grpcclient: not implemented`, 21 cases (the
      lines are in the `test:` commit). `StreamIsolationContract` (`RelayWorld`: an Env with the real in-app
      channel, a client of the relay, and the stream's publisher) has four cases: the same recipient id in two tenants,
      a forged nudge (and that the stream is not poisoned by it), another realm's identity, and a reconnect that gets
      the bounded count first after a missed push. It runs against the real relay over PostgreSQL with the in-memory
      stream and with Redis's sharded pub/sub. **It was written alongside the relay and not run against a stub**, so
      its red is its mutants: a relay that shows a nudge that loads nothing, one that loads as another recipient, and
      one that sends no count first each fail it, and one that listens on another tenant's stream **survives, by
      design** (the re-read under the session's scope is the second layer, [D54](design-decisions.md#d54)); with the
      relay acting as another tenant altogether it fails.*
- [x] **T043** *green* `driving/inprocess`: returns the engine's ports (NR-025). `NotifierContract`
      runs through it unchanged.
      **Mutate:** none. It adds no guarantee of its own; the T024 suite passing through it is the proof,
      and a mutation here would be one of T022's
      *`inprocess.Engine` is the five driving ports as an embedded host holds them, with nothing in it: a transport
      that did anything here would make the library and the service differ. `NotifierContract` runs through its
      `Notifier` (`TestNotifierContractThroughTheInProcessEngine`). The one-call `app.New` that the contract's
      `Engine` describes is still unbuilt: `app` cannot import a driving adapter, so it would return its own type
      (the README and the integration guide say so).*
- [x] **T044** *red → green* `driving/httpapi`: the REST surface and the SSE relay — subscription from
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
      *Built in three commits ([D54](design-decisions.md#d54)). **The use-cases the routes stand on**, in `app`:
      `Inbox.Preferences`, `SetPreferences` (all or none, every refused pair named with its reason: an unregistered
      topic or channel, an essential topic's channel, a pair the tenant locked, refused even to the value in force),
      `Schedule`, `SetSchedule` (validating first), `Unsubscribe` (verifying the token, disabling its one pair) and
      `Get`, and `Admin.SetTenantPreference`. They needed two port additions, `TopicRegistry.List` and `Inbox.Get`, a
      `Deps.UnsubscribeSigner`, and `NewInbox` and `NewAdmin` now name the ports they need. 23 mutants, two of which
      survived once and have tests now. **The surface:** the inbox, preferences, schedule, the stream, one-click
      unsubscribe, the operator's broadcast, replay, erasure and tenant defaults, provider webhooks, and health; 40
      handler tests and 10 stream tests, **red witnessed against a stub that answered 501** (the `test:` commit
      lists the lines), then 59 mutants (the health and relay-contract ones included), among them the two this task names. A route whose dependency is absent is not
      registered. **Split out, so the box can be ticked honestly:** [T044a](#t044a) and [T044b](#t044b), because their
      ports do not exist. `NotifierContract` and the relay run over a fake IdentityFunc: how a recipient token is
      verified is T050.*
- [ ] <a id="t044a"></a>**T044a** *red → green* The Resend webhook: the `ports.Webhook` for
      `/webhooks/email/resend` and the policy it applies (NR-032, [D14](design-decisions.md#d14)). The route, the
      verify-before-parse order and the port exist (T044); what does not is the provider's end.
      **Red first:** a svix-style signature over the raw body (a frozen vector, a stale timestamp, a body altered by
      one byte, a missing header); a hard bounce or a complaint suppresses the address by hash, in the right realm
      (the callback names none: it comes from the provider's own configuration); a soft bounce sets `expires_at`; a
      status updates `notification_deliveries.provider_status`; a callback for a message not yet recorded is
      `ErrUnknownDelivery`. The `webhooks` spec turns on with it.
      **Mutate:** apply a bounce without verifying; suppress for the wrong realm
- [ ] <a id="t044b"></a>**T044b** *red → green* The operator's reads: `GET /admin/notifications/dead-letters`
      (the list T032 left to the REST surface), `GET /admin/notifications/deliveries`, and `GET` of a tenant's
      defaults. Each needs a read port that does not exist, so each starts as a *setup* of that port and its
      PostgreSQL adapter, scoped as the tables are (a worker-only read is declared in `verify-schema.sql` TEST 5).
      **Red first:** a list filtered by tenant, channel and reason, paged, and never showing another realm's rows.
      **Mutate:** drop the realm predicate
- [x] **T046** [P] *red → green* `driving/grpcserver`, `driving/grpcclient` (satisfies `ports.Notifier`;
      `NotifyTx` returns `ErrTxUnsupported`). Make the T047 gRPC run pass.
      **Red first:** a unit test that `NotifyTx` returns `ErrTxUnsupported` and never opens a transaction.
      **Mutate:** open a transaction in `NotifyTx`; return `nil` instead of `ErrTxUnsupported`
      *Both stand on `adapters/driving/wire`, which maps every message and every error. **Red, witnessed:**
      `client_test.go:46: a caller's transaction: NotifyTx = grpcclient: not implemented, want domain.ErrTxUnsupported`
      (the lines are in the `test:` commit). The server adds only the two rules that belong to the wire, a key on
      every mutating call and a batch of at most 500 (refused whole, each item independent within it); a refusal
      crosses as a code and an `ErrorInfo` reason a producer in any language branches on, and the client turns it back
      into the library's sentinel; a fault is reported and shown as "internal error". A `Struct` holds numbers as
      doubles, so an integer beyond 2^53 in `Data` is refused by the client and not rounded. The two mutants this task
      names die on `TestNotifyTxIsUnsupportedAndNeverOpensATransaction`; the ledger holds 25 rows for this task and 10 for
      T047's gRPC run, all killed. The contract's text now carries the error table and the batch's shape.*

## Stage 11 — Service mode

- [ ] **T051** *red* NS-011 demonstration with a non-Go producer. Write the scenario first, using only
      the service's API and the generated client: register a topic and a template, store a recipient's
      addresses, send one notification, and see it delivered on two channels. It fails until T048–T050 land.
      Before then, show it can fail: run it against a service started with one channel only and against
      one where the template registration is skipped; each must fail the scenario at the step it removes
- [x] **T048** *red → green* `postgres/topics.go`, `postgres/templates.go` (Go templates, restricted
      function map, locale fallback), `postgres/addresses.go` — the data-backed ports
      ([D31](design-decisions.md#d31)).
      **Red first:** an integration table — a topic registered at runtime is usable without a migration
      (NR-017); a template calling a function outside the restricted map is rejected when it is saved, not
      when it is sent; locale falls back from the address to the tenant default to `DefaultLocale`, and to
      `ErrNoTemplate` when none exists; several addresses per `(recipient, channel)` with independent state
      (NR-028); the address table is scoped; the T013a fakes run the same table.
      **Mutate:** allow `env` or a file function in a template; skip the locale fallback
      *Three contract suites (`TopicRegistryContract`, `TemplateRendererContract`, `AddressBookContract`), each run by
      the in-memory fake and by the PostgreSQL adapter, plus what only a database holds (the address table's scope,
      a refused template leaving no row, concurrent saves numbering each version once, activation never leaving a
      window with no active version and two activations taken in turn), and the Notifier and Dispatcher run on these
      ports (`TestATopicRegisteredAtRuntimeIsUsableWithoutAMigration`, `TestADeliveryIsRenderedAndAddressedFromTheDataBackedPorts`).
      **Red, witnessed on stub adapters** against a real PostgreSQL, each on its assertion
      (`catalogcontract.go:76: Lookup = {...}, false, not implemented, want found=false and no error`; `catalog_integration_test.go:244:
      versions = [0 0 0 0 0 0 0 0], want [1 2 3 4 5 6 7 8]`); the pure domain half (the restricted engine) was red against a
      stub in its own commit (89 failing cases). The tenant default of the task's wording is read as the tenant's own
      template, and the ladder is walked before the tenant ([D55](design-decisions.md#d55)). A version is immutable and
      activated explicitly. The write side is three new driven ports (`TopicWriter`, `TemplateWriter`, `AddressWriter`) and `postgres.NewAll`. 37 rows in the ledger for T048, three of which survived once and were closed.*
- [x] **T049** *red → green* `Catalog` gRPC service; realm bindings from producer principals; channels
      constructed from `channel_providers` with credentials resolved from `secret_ref`.
      **Red first:** a producer authenticated for realm A cannot register or send in realm B (NR-009); no
      call returns a credential, only the `secret_ref`; an unresolvable `secret_ref` marks the provider
      unavailable and does not crash the service.
      **Mutate:** take the realm from the request body; echo the resolved secret
      *`app.Catalog` (the checks a registration must pass before it is written: a channel nobody registered, a template
      outside the map, an address set the engine cannot use), as `ports.Catalog`. The `Catalog` service (`grpcserver.CatalogServer` and the Go client `grpcclient.Catalog`, over the wire mappings
      and four new refusal reasons), `domain.RealmBindings`, realm interceptors for unary and stream calls
      (`auth.RealmInterceptors`: no credentials is Unauthenticated, a credential that is not good is Unauthenticated
      and repeats nothing of why, a principal with no binding is PermissionDenied, none reaching a handler), mTLS
      principals (a verified chain only), `auth.Route` and a producer server that picks an engine per call, so one
      process serves several realms ([D56](design-decisions.md#d56)). Channels from rows: `postgres.Providers`,
      `secrets.Resolver` (`env:`, `file:`, others registered), and `providers.Manager`
      ([D57](design-decisions.md#d57)). **Red, witnessed on stubs:** `realm_test.go:67: producer-a: reached=true realm="",
      err=<nil>, want realm "alpha"`; `catalog_test.go:72: ... Unimplemented desc = method UpsertTopic not implemented`;
      `providers_test.go:204: Reload = not implemented, want nil: a provider that cannot be built is reported, and does
      not stop the service` (the first run of that file hung, and every send in it is now bound by a deadline). **The
      two named mutants are killed:** the realm taken from a metadata key the caller sets dies on
      `TestARealmTheCallerNamesIsNeverRead` (`realm="beta" ... want alpha`), and the resolved secret echoed (in the report,
      and in a factory's own error) dies on `TestACredentialIsHandedToTheFactoryAndAppearsNowhereElse`. **A reading of the
      task:** there is no gRPC call that returns a provider, since the contract's `Catalog` has none, so "no call returns
      a credential" is held where a credential could escape: the manager's report, its errors and a delivery's detail. 58 ledger rows for T049, seven of which survived once (a realm hard-coded to the one value a test used, an empty file the reader reported as an error, a relative path refused for another reason, and others) and were closed.*
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
      production-ready.
      **Mutate:** make one path slower than its row (drop the claim index; add a fixed delay to the accept
      path); the run must fail on that row and name it
- [ ] **T056** [P] *red → green* Fill `docs/integration-guide.md` code samples against the built API.
      **Red first:** each sample is an `Example` test, or is extracted and built in CI, so a sample that
      stops compiling fails the build.
      **Mutate:** rename an exported symbol a sample calls; the build must fail on that sample
- [ ] **T057** Tag `v0.1.0` once Stages 1–6 are green; the README status block moves from "designed" to
      "core implemented". A production-ready release additionally requires T055. Before tagging, every
      NS-001 to NS-003 row in the traceability table below names a suite that is green in CI

---

## Traceability

Every success criterion is proven by a test that is written before the code it proves. A criterion
that lists no red task has no test scheduled.

| Criterion | Proven by | Written (red) in |
|---|---|---|
| NS-001 recipient scoping | schema tests 4, 5 and 14 (`make verify-schema`); `StoreContract` (same id in two tenants, read as owner); `NotifierContract`; `StreamIsolationContract`; the `isolation` and `stream` Playwright specs | T005 (done), T020 (done), T024 (done), T028 (done), T047 (done), T047a |
| NS-002 exactly once, per channel | schema tests 3 and 15 (`make verify-schema`); `StoreContract` and `NotifierContract` replay cases; `ChannelContract` at each declared `Dedup` | T005 (done), T020 (done), T024 (done), T029 (done) |
| NS-003 no delivery lost to a crash | `StoreContract` crash after commit; `DispatcherContract` at every crash point | T020 (done), T033 (done) |
| NS-004 in-app under 5 s at p95 | the `stream` Playwright spec (arrives without a reload); the load test | T047a, T055 |
| NS-005 digest burst | burst test | T035 (done) |
| NS-006 poison terminates | `DispatcherContract` (worker panic, attempt ceiling); dead-letter inspect and replay | T033 (done), T032 (done) |
| NS-007 tenant fairness | noisy-neighbour test in dispatcher passes; the load test's fairness row in wall time | T037 (done), T055 |
| NS-008 a channel is one file and one line | third-channel demonstration | T033a (done) |
| NS-009 builds with no host | no-host CI job | T054 (done) |
| NS-010 capacity | load-test thresholds | T055 |
| NS-011 non-Go producer | producer scenario against the service | T051 |
| NS-012 erasure | catalog-wide scan after `Erase`: `TestNS012AnErasedRecipientLeavesNothingAndTheirSuppressionStays`, which also shows the erased recipient's pre-check entries gone and another's kept ([D51](design-decisions.md#d51)), with the adapter's `TestErase` | T040, T040b |

## Requirements

The same rule one level down, and the table that definition of done 5 checks a task against. A
requirement that lists no red task has no test scheduled. *Structural* means the property is held by
construction or by a gate and no task adds a behaviour test for it.

| Requirement | Proven by | Written (red) in |
|---|---|---|
| NR-001 durable record | schema tests; the record exists with every channel disabled | T005 (done), T021 (done), T024 (done) |
| NR-002 idempotency key and window | `StoreContract` and `NotifierContract` replay cases; `Config.Validate` window floor; `IdemKey` bounds | T009 (done), T014 (done), T020 (done), T022a (done), T024 (done) |
| NR-003 one transaction | `StoreContract` failure after each statement; `NotifyTx` rollback | T020 (done), T024 (done) |
| NR-004 claim, lease, fence; pre-check after commit | the queue's integration suite; `DispatcherContract` crash points; pre-check tests | T016 (done), T023 (done), T022a (done), T024 (done), T033 (done) |
| NR-005 channel idempotency at its declared level | frozen delivery-key vector; `ChannelContract`; per-channel tests | T008 (done), T025 (done), T026 (done), T027 (done), T029 (done) |
| NR-006 suppressed and unresolvable are terminal | suppression table; `DispatcherContract` | T019 (done), T031 (done), T033 (done) |
| NR-007 backoff, ceiling, dead letters, alarm | backoff distribution; queue suite; `DispatcherContract` (poison, `notify.dlq.dead.count`); replay | T011 (done), T016 (done), T032 (done), T033 (done) |
| NR-008 scoped at the data layer and on the live path | schema tests 4, 5 and 14; `StoreContract`; `NotifierContract`; `StreamIsolationContract`; the REST handlers (the recipient is the session, a 404 and never a 403); the `isolation` and `stream` specs | T005 (done), T020 (done), T024 (done), T028 (done), T044 (done), T047 (done), T047a |
| NR-009 identity from the producer, realm never from a request | `Notification.Identity`; `NotifierContract` realm case (also over gRPC: no message carries a realm); an operator body that names a realm is refused; realm bindings | T009 (done), T022a (done), T024 (done), T044 (done), T047 (done), T049 (done) |
| NR-010 realm and tenant in the key space | frozen key vectors; schema tests; two tenants and two realms in `StoreContract` | T005 (done), T007 (done), T008 (done), T020 (done) |
| NR-011 preference precedence | resolution matrix; planner rows; locked tenant case | T018 (done), T021 (done), T024 (done) |
| NR-012 all channels off still persists; inbox visibility | planner rows; inbox table; `NotifierContract` | T017 (done), T021 (done), T024 (done) |
| NR-013 quiet hours and cadence | planner rows including `DeliverySchedule.Validate`; critical override; schedule round-trip | T018 (done), T021 (done), T024 (done) |
| NR-014 digests | `TopicDef.Digestible`; planner rows; burst test; flush job | T009 (done), T021 (done), T034a (done), T035 (done) |
| NR-015 unsubscribe | email golden files; the token's vector and tamper table; `Inbox.Unsubscribe`; REST unsubscribe handler rows; the `unsubscribe` spec | T026 (done), T026a (done), T044 (done), T047a |
| NR-016 a channel is one file and one line | third-channel demonstration; `app` cannot import a channel (arch-lint) | T033a (done), T053 (done) |
| NR-017 a topic is a registration | topics integration table, no migration | T048 (done) |
| NR-018 re-anchoring identity is a host binding | structural: `Identity` is opaque strings with frozen vectors; `domain` imports nothing | T007 (done), T053 (done) |
| NR-019 copy only through the template seam | template golden files; restricted function map; locale fallback; inbox render at read time | T017a (done), T026 (done), T048 (done) |
| NR-020 resumable paged broadcast | crash between pages and inside a page (`TestACrashBetweenPagesResumesAtTheNextPage`, `TestACrashInsideAPageRollsTheWholePageBackAndTheNextWorkerRedoesIt`); a retried broadcast (`TestARetriedBroadcastDoubleNotifiesNobody`); deterministic member keys, frozen and checked by the store on every page; the inline limit; the lease-token fence | T042a (done), T042 (done) |
| NR-021 stable shard, changeable count | shard vectors; `RehomeShards` integration test | T010 (done), T016 (done) |
| NR-022 bounded growth, metadata retirement | `TestEnsurePartitions`; retention with a dead letter outstanding and with a pending delivery; no row deleted on the way to a retirement (a `DELETE` witness, in place of `n_tup_del`); the jobs' cutoffs (`TestRetentionRetiresEachTableAtItsOwnWindow`, `TestTheExpiriesCutAtTheIdempotencyWindowInBatches`); that the `DELETE` witness can fire (`TestTheRetirementWitnessRecordsADeletion`) | T006 (done), T038, T039 |
| NR-023 quota defers and moves the backlog | `TestDeferTenantChannel`; `QuotaContract` and the `channel_quotas` table; noisy-neighbour test | T016 (done), T036 (done), T037 (done) |
| NR-024 bounded unread count; nudge only | inbox cap table; in-app payload; stream keys | T017 (done), T025 (done), T028 (done) |
| NR-025 library and service, identical semantics | `NotifierContract` in-process and over gRPC; producer scenario | T043 (done), T047 (done), T051 |
| NR-026 no host import | `go-arch-lint` and `depguard`; the no-host CI job | T053 (done), T054 (done) |
| NR-027 `NotifyTx` | `StoreContract` and `NotifierContract` commit and rollback cases | T020 (done), T022a (done), T024 (done) |
| NR-028 several addresses per channel | `TestBindAddresses`; addresses table; per-address retry | T016 (done), T033 (done), T048 (done) |
| NR-029 scheduled send and expiry | planner `DeliverAfter`; `NotifierContract`; `DispatcherContract` expiry | T021 (done), T024 (done), T033 (done) |
| NR-030 cancel | `NotifierContract` cancel cases; the queue's `Cancel` | T016 (done), T022a (done), T024 (done) |
| NR-031 fallback chain | `TopicDef.Validate`; planner order; `DispatcherContract` fallback | T009 (done), T021 (done), T033 (done) |
| NR-032 delivery status and callback correlation | `Status` and `RecordProviderStatus` in `StoreContract`; `NotifierContract`; the webhook route (verified before parsed); the Resend decoder; the `webhooks` spec | T020 (done), T022a (done), T024 (done), T044 (done), T044a, T047a |
| NR-033 seen, read, archived | inbox table, independent flags | T017 (done) |
| NR-034 erasure | catalog-wide scan; suppression survives and holds no plaintext; `Admin.Erase`'s own table (`TestErase…` in `app`); the pre-check's entries for the recipient's keys forgotten before the erasure (`TestEraseForgets…`, the `Forget` cases of `PreCheckContract`, `TestIdemKeys`) | T019 (done), T040, T040b |
| NR-035 accepted when committed; ack after commit | `StoreContract` crash after commit; ingest kill-before-ack | T020 (done), T052 |
| NR-036 capacity envelope stated | stated in [plan.md § Capacity model](plan.md#capacity-model); the load-test thresholds | T055 |
| NR-037 preflight warnings | `TestAssessDatabase` and `TestWALRate`; `TestPreflight…` in `app`; `TestDatabaseConditions…` against PostgreSQL 16 and 17 (`make test-pg-next`; CI runs both), `TestDatabaseConditionsCountsOnlyClientBackends` for the filter | T040a |

---

## Not in this phase

Acknowledgement, escalation chains, multi-step workflows and delivery-outcome webhooks are
[Phase 2](../002-escalation-workflows/). A template authoring UI, per-tenant branding UI and delivery
analytics are Phase 3 and undesigned. See [ROADMAP.md](../../ROADMAP.md).
