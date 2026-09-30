# Migrations

Applied in filename order. Verified against **PostgreSQL 16** by `make verify-schema`, which runs them
against a throwaway container and asserts the guarantees they are supposed to provide — as the table
owner, the role a worker actually uses.

| File | Contents |
|---|---|
| `0001_notification_core.sql` | The scope policy helper; inbox (partitioned, RLS on parent and partitions); `notify_idem` guard (tenant in the key, hash-partitioned); the delivery queue; the delivery history (partitioned); broadcasts; recipient and tenant preferences; schedules; job leases |
| `0002_channels_delivery.sql` | Hashed per-channel suppressions; dead letters (genuine failures only, partitioned); sealing digest windows; quotas with a realm-wide default |
| `0003_catalog_compliance.sql` | The data-backed ports: topics, versioned templates, recipient addresses (RLS), channel providers (credentials by reference); the erasure record |
| `migrations.go` | Embeds the `.sql` files (`migrations.FS`), so a host, the service binary and the integration tests apply the same schema |

These files were rewritten in place during the architecture review rather than amended by new
migrations, because nothing had been deployed from them. From the first tagged release on, a merged
migration is immutable and every change is a new file.

## Why the schema looks the way it does

Several choices are counterintuitive and each is load-bearing. Read
[design-decisions.md](../specs/001-notification-core/design-decisions.md) for the full reasoning:

- **The idempotency guard is a separate table** (`notify_idem`). A unique index on
  `(recipient, idem_key)` **cannot be created** on a table partitioned by `created_at`. `verify-schema`
  demonstrates the error before applying the fix ([D2](../specs/001-notification-core/design-decisions.md#d2)).
- **No foreign key references a partitioned table.** A foreign key would block retiring an aged
  partition while any row still referenced it, turning routine retention into an outage
  ([D6](../specs/001-notification-core/design-decisions.md#d6)).
- **The queue holds pending work only.** A finished delivery moves to `notification_deliveries` in the
  same transaction, so the hot table is as small as the backlog
  ([D20](../specs/001-notification-core/design-decisions.md#d20)).
- **The queue row repeats the recipient identity.** The worker needs it to scope its read of the
  notification under forced RLS ([D17](../specs/001-notification-core/design-decisions.md#d17)).
- **The schema holds shape, not behaviour.** Constraints, row-level security and indexes live here;
  the queue's state transitions are SQL in the Go adapter, tested against a real PostgreSQL by
  `make test-integration` ([D37](../specs/001-notification-core/design-decisions.md#d37)). The one
  function here, `notify_apply_recipient_scope`, is a DDL helper that defines the RLS predicate once.
- **Preferences are rows, not columns.** `(topic, channel, enabled)` rather than `in_app BOOL,
  email BOOL`, so adding a channel is not a migration
  ([D3](../specs/001-notification-core/design-decisions.md#d3)).

## Partitions

`notifications`, `notification_deliveries` and `dead_letters` are `PARTITION BY RANGE`. The bootstrap
migration creates three months of each. **Provisioning further months is the maintenance job's
responsibility** — `Maintenance.EnsurePartitions`, which creates each missing month ahead of the clock —
see [docs/operations.md](../docs/operations.md#partition-provisioning).

Every new `notifications` partition **must** be passed to `notify_apply_recipient_scope()`. PostgreSQL
does not carry a parent's row-level security to its partitions, so an unscoped partition queried by name
shows every recipient's rows. `verify-schema` TEST 5 fails on one, and at run time
`Maintenance.CheckPartitions` reports one.

There is deliberately **no default partition** (`verify-schema` TEST 13 fails on one). A missing partition makes an insert fail loudly, which
is recoverable; a default partition silently absorbs those rows and can never be retired, which is not.

## Adding a migration

1. Sequential filename, `NNNN_description.sql`.
2. Wrap in `BEGIN` / `COMMIT`.
3. Comment *why*, not what — the DDL already says what.
4. A new table holding `recipient_id` must either get `notify_apply_recipient_scope()` or be added to the
   worker-only list in `verify-schema` TEST 5 — the test fails until you choose.
5. Add an assertion to `scripts/verify-schema.sql` for any new guarantee, and make it `RAISE` on failure.
   A guarantee with no assertion is a comment.
6. Run `make verify-schema` before committing. CI runs it too, but finding it locally is cheaper.
