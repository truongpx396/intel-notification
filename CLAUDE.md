# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`intel-notification` is a reusable multi-channel notification engine (Go 1.26, PostgreSQL 16, Redis),
adopted either as a library or as a gRPC/REST service. **The specification is the primary artifact and the
implementation is partway through it.** [ROADMAP.md](ROADMAP.md) says what is built, designed or absent, and
[specs/001-notification-core/tasks.md](specs/001-notification-core/tasks.md) is the build order. Do not
trust README snippets about the notifier, dispatcher, channels or transports: those layers do not exist yet.

Where code and spec disagree, one of them is a bug and the spec says which. Order of authority:
[constitution](.specify/memory/constitution.md) → `specs/*/contracts/` → `spec.md` →
`design-decisions.md` → code. Code comments cite decisions (`D17`), requirements (`NR-003`) and success
criteria (`NS-001`); look them up in `specs/001-notification-core/` rather than guessing.

## Commands

```bash
make help              # list targets
make build             # go build ./...
make test              # unit tests: -race -shuffle=on, no Docker
make test-integration  # -tags integration, PostgreSQL 16 via Testcontainers (needs Docker)
make verify-schema     # migrations + schema assertions on a throwaway PG16 (needs Docker)
make lint              # golangci-lint v2: depguard boundaries, paralleltest, goimports
make arch-lint         # go-arch-lint: the hexagon's dependency graph
make lint-sql          # migration hygiene (BEGIN/COMMIT, header comment, numbering)
make docs-links        # dangling relative markdown links
make e2e-list          # typecheck + list the Playwright suite, no stack needed (Node 24, e2e/.nvmrc)
make ci                # everything CI runs today
make bench / make soak # queue benchmarks / sustained-load soak; Docker, minutes to hours, not in ci
```

A single test (tests are shuffled and parallel, so always pass `-race -shuffle=on -count=1`):

```bash
go test -race -shuffle=on -count=1 -run 'TestDispositionOf' ./domain
go test -race -shuffle=on -count=1 -tags integration -run 'TestFencedWrites' ./adapters/driven/postgres
```

Integration tests sit behind the `integration` build tag, so editors hide them by default; gopls needs
`"buildFlags": ["-tags=integration"]`.

## Architecture

A hexagon, enforced by `.go-arch-lint.yml` and depguard in `.golangci.yml` (`.claude/rules/architecture.md`
has the rules):

- `domain/`: pure rules, imports nothing from the module. Canonical encoding and every derived key
  (`keys.go`), shard, backoff, the terminal-outcome policy, dispatch steps.
- `ports/`: `driven.go` and `driving.go`, the interfaces. Every other layer depends on these.
- `adapters/driven/postgres/`: the delivery queue, digests and maintenance jobs. The store's persist and
  read paths are not built yet.
- `migrations/`: the schema, embedded as `migrations.FS` so hosts, the service binary and the integration
  tests apply the same files. `config.go` is the one configuration surface.
- `internal/notifytest/` is the kit every suite is written against (fake clock, fault injection, probe,
  in-memory fakes, `Env`); `internal/pgtest/` starts Testcontainers. Both are test support only.
- Not created yet: `app/`, `adapters/driving/`, `api/`, `cmd/`. Their lines in `.go-arch-lint.yml` are
  commented out and are uncommented in the change that creates the directory.

How a notification moves, which spans several files: accepting one is a single transaction (the `notify_idem`
guard, the inbox row, one queue row per enabled channel, digest windows). Dispatcher replicas then claim queue
rows with `SKIP LOCKED` leases fenced by a token, so a stale worker's write is discarded. A finished delivery
leaves the queue for the delivery history; only genuine failures become dead letters. The queue holds
pending work only.

The two properties the design exists for are held by the schema, not by call-site discipline: **tenant
isolation** (forced row-level security on every scoped table and every partition, scope set per
transaction) and **exactly-once** (primary keys and the idempotency guard, with the realm, tenant and
recipient in every key). Queue state transitions are SQL in the Go adapter, proven against a real
PostgreSQL ([D37](specs/001-notification-core/design-decisions.md#d37)), not PL/pgSQL.

## Workflow

Test-driven, as defined in [tasks.md](specs/001-notification-core/tasks.md#how-a-task-is-worked):

- Write the failing test first, **run it, and read the failure**: it must fail on its assertion, not on a
  compile error, a skip or a missing container. Commit order is `test:` (red), then `feat:` / `fix:`
  (green), then `refactor:`. `feat:` subjects name the task (`feat: T014 — Config, WithDefaults and Validate`).
- A box in `tasks.md` is ticked only when the red was witnessed, `make ci` is green, each new guarantee
  was mutation-checked with a row in [docs/mutations.md](docs/mutations.md), and the refactor is done.
- Assert each guarantee at the lowest layer that can hold it ([docs/testing.md](docs/testing.md)).
- Task IDs are stable; never renumber, new tasks take an `a` suffix.
- Work on a branch (`feat/`, `fix/`, `test/`, `docs/`, `perf/`) and open a PR into `main`.

## Things that are easy to get wrong

- **Integration tests run the code under test as `db.Owner`, never `db.Admin`.** Only the non-superuser
  owner proves forced RLS holds. Running the schema suite as a superuser proves nothing.
- **Migrations are rewritten in place until the first `v*` tag, then immutable.** The existing
  `extraction-baseline-intel-agent` tag is not a release. A migration that adds a table holding
  `recipient_id` must be scoped or declared worker-only in `verify-schema.sql` TEST 5.
- **The module must never import `github.com/aisat/...`**, and `testcontainers` stays in tests and
  `internal/pgtest`. Both are depguard rules, so CI fails on them.
- **No fake `Queue` or `Store`** in the kit, on purpose: leases, fencing and RLS only PostgreSQL can hold.
- **Provider credentials never go in config or env.** `channel_providers` rows reference them
  (`env:NAME`, `file:/run/secrets/x`). `.env.example` lists the service's inputs only.

## Claude Code setup in this repo

`.claude/settings.json` is shared; put personal overrides in `.claude/settings.local.json` (gitignored).

- After editing a `.go` file, a hook runs goimports with the repo's local-import prefix. After editing
  `migrations/*.sql` it runs `make lint-sql` and reminds you of `make verify-schema`.
- Before `git commit`, a hook runs `lint-sql docs-links build lint arch-lint`, plus `verify-schema` when SQL
  changed and Docker is up. It deliberately skips `make test`, so a red `test:` commit is allowed.
- Edits to the constitution and to `specs/*/contracts/**` ask for confirmation. Edits to a merged migration
  are denied once a `v*` tag exists. `.env` files, `go.sum` and `e2e/package-lock.json` cannot be edited.
- Path-scoped guidance lives in `.claude/rules/` and loads when you touch matching files.
