---
paths:
  - "**/*_test.go"
  - "internal/notifytest/**"
  - "internal/pgtest/**"
---

# Go test conventions

The full version, with examples, is [docs/testing.md](../../docs/testing.md). The ones a linter checks
(`paralleltest`, `tparallel`, `thelper`, `usetesting`) fail `make lint`; the rest are held in review.

- **Red first, and red for the right reason.** A new behaviour starts with a failing test. Run it and
  read the failure: it must fail on its assertion, not on a compile error, a fixture panic, a skip or a
  missing container. Put the failure line in the commit that adds the test.
- **Table-driven, named subtests.** Give a case a `why` when the expected value is a decision rather than
  arithmetic. A test whose steps depend on each other stays a sequence, with a comment saying so.
- **`t.Parallel()` first, in every test and every subtest.** Copy any loop-external variable inside the
  loop; never depend on a sibling subtest. Runs are `-race -shuffle=on`.
- **Use `t.Context()` and `t.Helper()`**, and `t.TempDir()` / `t.Setenv()` over hand-rolled versions.
- **No sleeps.** Time comes from `notifytest.Clock` (it moves only when told). Wait on concurrency with
  `testing/synctest` or a deadline, so a regression fails instead of hanging.
- **Frozen vectors for anything stored or sent.** A key, hash or encoding is tested against a literal
  computed independently of the code, from `data-model.md`, with a different tool.
- **Lowest layer that can hold the guarantee.** Pure rule → unit; database property → `verify-schema.sql`;
  state transition → integration; behaviour through the service → Playwright.

## Integration tests (`//go:build integration`)

- Start the suite with `func TestMain(m *testing.M) { os.Exit(notifytest.Main(m)) }`, then
  `e := notifytest.NewEnv(t)`, or `db := pgtest.New(t)` for a bare database.
- **The code under test gets `db.Owner`, never `db.Admin`.** The owner is neither superuser nor
  BYPASSRLS, so it is the only role that proves forced row-level security holds. `db.Admin` is for
  fixtures that must bypass RLS.
- **Fixed timestamps inside the bootstrap partitions**, never `now()` for partitioned data, so the suite
  does not break the month the bootstrap partitions run out.
- **Prove concurrency with concurrency**: two sessions or many goroutines, with a deadline.
- **Check against something independent of the code**: list the tables that can hold a recipient from the
  catalog, not from the adapter's own list.
- A suite in `adapters/driven/postgres` that uses `notifytest.Env` must be an external test package
  (`package postgres_test`); an internal one is an import cycle.
- There is deliberately **no fake `Queue` or `Store`**: leases, fencing and RLS only PostgreSQL can hold.

## Before a guarantee counts as tested

Break it on purpose, watch the named test go red on its assertion, revert, and add a row to the
[mutation ledger](../../docs/mutations.md) in the same PR. A mutation that survives is recorded as
`SURVIVED`, the test is strengthened, and a second row shows it now dies.
