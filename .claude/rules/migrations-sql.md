---
paths:
  - "migrations/**"
  - "scripts/*.sql"
---

# Migrations and schema assertions

Background and the reasons for each choice: [migrations/README.md](../../migrations/README.md). A hook
runs `make lint-sql` after every edit under `migrations/`.

## Adding a migration

1. Sequential filename, `NNNN_description.sql`, in `migrations/` (it is embedded by `migrations.go`).
2. First line is a `--` comment; the body is wrapped in `BEGIN;` / `COMMIT;`. Comment *why*, not what.
3. A new table holding `recipient_id` must either call `notify_apply_recipient_scope()` or be added to the
   worker-only list in `verify-schema.sql` TEST 5. The test fails until you choose.
4. A new `notifications` partition **must** be passed to `notify_apply_recipient_scope()`: PostgreSQL does
   not carry a parent's RLS to its partitions.
5. Add an assertion to `scripts/verify-schema.sql` for any new guarantee. It must `RAISE` on failure, and
   it must be mutation-checked (a guarantee with no assertion is a comment).
6. Run `make verify-schema` (needs Docker) before committing, and update the table in
   `migrations/README.md`.

## Do not

- **Put a foreign key on a partitioned table.** It would block retiring an aged partition (D6).
- **Add a default partition.** A missing partition must fail loudly; a default one silently absorbs rows
  and can never be retired (`verify-schema` TEST 13).
- **Put a unique index on a table partitioned by `created_at`** to get idempotency. It cannot be created;
  that is why `notify_idem` is a separate table (D2), and `make verify-schema` asserts the failure.
- **Put behaviour in the schema.** Constraints, RLS and indexes live here. Queue state transitions are SQL
  in the Go adapter (D37).
- **Edit a merged migration once a release is tagged.** Before the first `v*` tag, migrations are rewritten
  in place; after it they are immutable and every change is a new file. A hook denies the edit once that
  holds.
- **Run the schema suite as a superuser or a non-owner.** It proves nothing about `FORCE ROW LEVEL
  SECURITY`; `make verify-schema` runs it as the table owner for that reason.
