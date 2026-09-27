# Migrations

Applied in filename order. Verified against **PostgreSQL 16** by `make verify-schema`, which runs them
against a throwaway container and asserts the guarantees they are supposed to provide.

| File | Contents |
|---|---|
| `0001_notification_core.sql` | Inbox (partitioned, RLS forced), `notify_idem` guard, outbox, preferences, schedules |
| `0002_channels_delivery.sql` | Per-channel suppressions, dead letters (partitioned), digest buffer, quotas |

## Why the schema looks the way it does

Three choices are counterintuitive and each is load-bearing. Read
[design-decisions.md](../specs/001-notification-core/design-decisions.md) for the full reasoning:

- **The idempotency guard is a separate table** (`notify_idem`). A unique index on
  `(recipient, idem_key)` **cannot be created** on a table partitioned by `created_at`. `verify-schema`
  demonstrates the error before applying the fix ([D2](../specs/001-notification-core/design-decisions.md#d2)).
- **The outbox has no foreign key into the inbox.** A foreign key would block retiring an aged
  partition while any delivery still referenced it, turning routine retention into an outage
  ([D6](../specs/001-notification-core/design-decisions.md#d6)).
- **Preferences are rows, not columns.** `(topic, channel, enabled)` rather than `in_app BOOL,
  email BOOL`, so adding a channel is not a migration
  ([D3](../specs/001-notification-core/design-decisions.md#d3)).

## Partitions

`notifications` and `dead_letters` are both `PARTITION BY RANGE (created_at)`. The bootstrap migration
creates three months. **Provisioning further months is an operator responsibility** — see
[docs/operations.md](../docs/operations.md#partition-provisioning).

There is deliberately **no default partition**. A missing partition makes an insert fail loudly, which
is recoverable; a default partition silently absorbs those rows and can never be retired, which is not.

## Adding a migration

1. Sequential filename, `NNNN_description.sql`.
2. Wrap in `BEGIN` / `COMMIT`.
3. Comment *why*, not what — the DDL already says what.
4. Add an assertion to `scripts/verify-schema.sql` for any new guarantee. A guarantee with no assertion
   is a comment.
5. Run `make verify-schema` before committing. CI runs it too, but finding it locally is cheaper.
