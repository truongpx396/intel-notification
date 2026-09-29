# Testing

How this repository is tested, and the conventions every new test follows. Written for anyone adding
code: each layer below says what it is for, how to run it, and the rules a test in it must keep.

The principle the layers serve is the constitution's second: **a guarantee without an assertion is a
comment.** Each guarantee in the specification is asserted at the lowest layer that can hold it — a
pure rule in a unit test, a database property in the schema suite, a state transition against a real
PostgreSQL, a user-visible behaviour through the running service.

## The layers

| Layer | Tool | What it proves | Run | In CI |
|---|---|---|---|---|
| **Unit** | `go test`, table-driven, parallel | Pure rules in `domain/`: the canonical encoding and every derived key (frozen vectors), shard, backoff, the terminal-outcome policy | `make test` | `unit` |
| **Schema** | `psql` against a throwaway PostgreSQL 16 | What the schema alone holds: constraints, forced row-level security on every scoped table and partition, key spaces, shapes | `make verify-schema` | `schema` |
| **Integration** | `go test -tags integration` + **Testcontainers** | Every state transition in `adapters/driven/postgres`, against a real PostgreSQL, as the non-superuser table owner — including real concurrency | `make test-integration` | `integration` |
| **End to end** | **Playwright** | The running service as a host's front end sees it: the REST inbox, the SSE stream, unsubscribe links, provider callbacks | `make e2e` | `e2e-suite` (typecheck + list) now; `e2e` once the service exists |
| **Boundaries** | `golangci-lint` (depguard, paralleltest…), `go-arch-lint` | The hexagon's dependency graph, the extraction guarantee, and the test conventions below | `make lint arch-lint` | `lint` |

`make ci` runs everything that runs today.

## Conventions

These hold for every Go test. The ones a linter can check are enforced in CI (`.golangci.yml`); the
rest are enforced in review.

### Table-driven

A test that checks a rule against several inputs is a table of cases, each run as a named subtest:

```go
func TestDispositionOf(t *testing.T) {
	t.Parallel()
	cases := []struct {
		outcome TerminalOutcome
		want    Disposition
		why     string
	}{
		{OutcomeMaxAttempts, Disposition{DeadLetter: true, Fallback: true}, "a genuine failure"},
		{OutcomeExpired, Disposition{}, "never sent late, on this channel or another"},
		// …
	}
	for _, tc := range cases {
		t.Run(string(tc.outcome), func(t *testing.T) {
			t.Parallel()
			if got := DispositionOf(tc.outcome); got != tc.want {
				t.Fatalf("DispositionOf(%s) = %+v, want %+v: %s", tc.outcome, got, tc.want, tc.why)
			}
		})
	}
}
```

Give a case a `why` when the expected value is a decision rather than arithmetic, so a failure explains
itself. A test whose steps depend on one another — a lease taken, then contested, then lapsed — stays a
sequence; say so in a comment rather than forcing it into a table.

### Parallel, always

Every test and every subtest calls `t.Parallel()` first (`paralleltest`, `tparallel`). Tests run with
`-race -shuffle=on`, so a hidden dependency on order or a data race fails the build rather than hiding
behind a serial run.

- **Never capture a loop-external variable** in a parallel subtest. A counter declared outside the loop
  and incremented inside is shared by every subtest; copy it inside the loop (`shard := next`).
- **Never depend on a sibling subtest.** If a check needs another subtest's result, it belongs in that
  subtest.
- Use `t.Context()` for anything that should stop when the test ends (`usetesting`), and `t.Helper()`
  in helpers (`thelper`).

### Frozen vectors for anything stored or sent

A key, hash or encoding that is persisted or reaches a provider is tested against a literal value
**computed independently of the code** — from the definition in `data-model.md`, with a different tool.
A vector produced by running the code only proves the code agrees with itself.

### Integration tests: Testcontainers, one database per test

Integration tests carry `//go:build integration`, so `make test` needs no Docker. `internal/pgtest`
starts **one** PostgreSQL 16 container per test binary and builds a template database once: every
migration, the non-superuser owner role, and any state the package prepares (partitions for the current
month). Then:

```go
func TestSomething(t *testing.T) {
	t.Parallel()
	db := pgtest.New(t)            // a private database cloned from the template, dropped afterwards
	store := postgres.New(db.Owner) // the code under test runs as the table owner
	// db.Admin is a superuser on the same database, for fixtures that must bypass RLS
}
```

Rules:

- **The code under test gets `db.Owner`, never `db.Admin`.** The owner is neither superuser nor
  BYPASSRLS — the role the engine runs as, and the only one that proves forced row-level security holds.
- **Fixtures use fixed timestamps** inside a bootstrap partition, never `now()` for partitioned data, so
  the suite does not start failing the month the bootstrap partitions run out.
- **Prove concurrency with concurrency.** A property that only matters under contention — the claim
  skipping locked rows, digest windows under simultaneous appenders — is tested with two sessions or many
  goroutines, with a deadline so that a regression fails instead of hanging.
- **Check a guarantee against something independent of the code.** The erasure test lists the tables
  that can hold a recipient from the database catalog, not from the adapter's own list, so a table the
  code forgot — or one added later — fails the test.

### Mutation-check what matters

A test that cannot fail proves nothing. For each guarantee worth a test, break it on purpose once and
confirm the test goes red: remove `SKIP LOCKED`, drop a lease fence, skip a table in erasure. The queue's
integration suite was checked this way against 13 such mutations, every one caught (two of them only
after the tests were strengthened — which is the point). Do this whenever you add or change a guarantee.

### End to end: Playwright

`e2e/` is a Playwright project (`@playwright/test`, TypeScript, Node 24) that drives the service the way
a host's front end does:

- **`*.api.spec.ts`** run in the `api` project over HTTP only; **`*.browser.spec.ts`** run in Chromium,
  for what needs a real browser — the SSE stream through `EventSource`.
- **Isolation by data.** Every test builds its own tenants, recipients and idempotency keys
  (`support/fixtures.ts`), so the suite runs `fullyParallel` against one stack. The `same recipient id in
  two tenants` shape appears on purpose: it is the one that exposes an isolation bug.
- **The run is the host.** Global setup generates a signing key, serves its JWKS, and mints recipient
  tokens with it, exactly as a host would; no key is committed. Producers are driven through the operator
  broadcast route, so no test needs a gRPC client. Outgoing mail lands in Mailpit (compose profile `e2e`)
  and is read back through its API.
- **Retries show, not hide.** CI retries a failed test once and records a trace of the retry, so a flaky
  test is visible in the report.

The service does not exist yet, so every spec is written against the REST contract and marked
`test.describe.fixme` with the tasks that will enable it (T044, T049); T047a switches the suite on. CI typechecks and lists the suite
on every change, so it cannot rot before the service arrives, and the full `e2e` job switches on with
those tasks.

## Editor setup

Integration tests are behind a build tag, so editors hide them by default. For gopls (VS Code, Neovim,
GoLand's gopls mode), add the tag:

```json
{ "gopls": { "buildFlags": ["-tags=integration"] } }
```

## Versions

Go 1.26 (`go.mod`, toolchain `go1.26.8`), PostgreSQL 16 (`postgres:16-alpine`), Testcontainers-go
v0.44, pgx v5.11, golangci-lint v2.14, go-arch-lint v1.19, Playwright 1.63 on Node 24, TypeScript 7.
Bump them together, and run `make ci` and `make e2e-list` after.
