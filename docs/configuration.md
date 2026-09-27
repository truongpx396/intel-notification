# Configuration

`notify.Config` is the **entire** configuration surface. The core reads no global app config and no
environment variables; adapters dial what they are given. That is what makes the module extractable
and what makes its behaviour reproducible in a test.

## Fields

| Field | Default | Notes |
|---|---|---|
| `Realm` | **none — required** | The product identity. `Validate` rejects empty, because a wrong-but-plausible default silently merges two products' idempotency key spaces ([D1](../specs/001-notification-core/design-decisions.md#d1)) |
| `StoreDSN` | **none — required** | PostgreSQL connection string |
| `RedisURL` | **none — required** | Pre-check, pub/sub, quota counters |
| `SubjectPrefix` | `notify` | Bus subject root |
| `Shards` | `16` | Outbox partitions. **Fixed for the deployment's life** — changing it live double-delivers ([D11](../specs/001-notification-core/design-decisions.md#d11)) |
| `Delivery` | `outbox` | `outbox` (transactional, crash-safe) or `direct` (in-line; dev or in-app only) |
| `MaxAttempts` | `5` | Per-channel re-drives before dead-lettering |
| `BackoffBase` | `10s` | First retry interval |
| `BackoffCeiling` | `1h` | Cap on the computed interval. Actual delay is uniform in `[0, computed]` — full jitter ([D10](../specs/001-notification-core/design-decisions.md#d10)) |
| `RetentionWindow` | `90d` | Read notifications older than this are retired |
| `DeadLetterRetention` | `30d` | Independent of the inbox window ([D8](../specs/001-notification-core/design-decisions.md#d8)) |
| `DefaultLocale` | `en` | Template fallback when an address carries no locale |
| `DigestMax` | `100` | Members folded into one digest before it flushes early, so a pathological burst cannot render an unbounded email |

## Environment mapping (service mode)

The container reads these and builds a `Config`. In library mode your host does the mapping.

```bash
NOTIFY_REALM=my-product              # required
NOTIFY_PG_DSN=postgres://...         # required
NOTIFY_REDIS_URL=redis://...         # required
NOTIFY_SHARDS=16
NOTIFY_DELIVERY=outbox
NOTIFY_MAX_ATTEMPTS=5
NOTIFY_BACKOFF_BASE=10s
NOTIFY_BACKOFF_CEILING=1h
NOTIFY_RETENTION_WINDOW=2160h
NOTIFY_DEAD_LETTER_RETENTION=720h
NOTIFY_DEFAULT_LOCALE=en
```

Provider credentials are **not** engine config — they belong to the channel implementations, which
your host constructs. The engine never holds a provider key, which is also why a shared deployment
can hold per-tenant keys without the core knowing.

## Validation

`Config.Validate()` applies defaults to a copy and returns every problem at once rather than the first
one, so a misconfigured deployment fails at startup with a complete list. It rejects:

- empty `Realm`, `StoreDSN`, `RedisURL`
- `Shards < 1`
- `Delivery` outside `outbox|direct`
- `MaxAttempts < 1`
- `BackoffCeiling < BackoffBase`

Validation runs before any connection is dialled, so a bad config never produces a half-initialized
engine.

## Choosing `Delivery`

| | `outbox` (default) | `direct` |
|---|---|---|
| Persist path | inbox row + per-channel outbox rows in one transaction | inbox row, then deliver in-line |
| Crash mid-fan-out | every enqueued delivery survives and is re-driven | undelivered channels rely on the bus redelivering the whole handler |
| Extra table | yes | no |
| Use when | production, multi-channel | dev, or genuinely in-app-only |

Both satisfy invariants 1–7; they differ only in *when* a delivery becomes crash-safe. `outbox` is the
default because the two-store shape (durable write plus external send) is exactly the shape that
loses messages without one.
