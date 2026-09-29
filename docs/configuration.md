# Configuration

`notify.Config` is the **entire** configuration surface of the engine. The core reads no global app
config and no environment variables; adapters dial what they are given. That is what makes the module
extractable and its behaviour reproducible in a test. Service mode adds a small set of container-only
settings, listed separately below.

## Fields

| Field | Default | Notes |
|---|---|---|
| `Realm` | **none — required in library mode** | The product identity, `^[a-z0-9][a-z0-9-]{0,62}$`. `Validate` rejects empty, because a wrong-but-plausible default silently merges two products' idempotency key spaces ([D1](../specs/001-notification-core/design-decisions.md#d1)). Service mode ignores it — see `NOTIFY_REALM_BINDINGS` |
| `StoreDSN` | **none — required** | PostgreSQL connection string |
| `RedisURL` | **none — required** | Pre-check, live stream, quota counters |
| `Shards` | `16` | Claim contention hint. **Changeable online** — raising needs nothing; after lowering, run the rehome step (`Maintenance.RehomeShards`) ([D11](../specs/001-notification-core/design-decisions.md#d11)) |
| `ClaimLease` | `5m` | How long a claim hides a delivery. Must exceed the slowest channel's send timeout, or a slow send is re-driven while in progress |
| `ClaimBatch` | `100` | Deliveries per claim |
| `PollInterval` | `500ms` | The idle ceiling on claim latency. A full batch re-polls at once ([D22](../specs/001-notification-core/design-decisions.md#d22)) |
| `ListenNotify` | `false` | Transactional `NOTIFY` wakeups. Lowers idle latency; serializes committing transactions behind a database-wide lock. Leave off above a few hundred notifications a second |
| `MaxAttempts` | `5` | Claims per delivery before `max_attempts`. Counted at claim, so crashes count ([D19](../specs/001-notification-core/design-decisions.md#d19)) |
| `BackoffBase` | `10s` | First retry interval |
| `BackoffCeiling` | `1h` | Cap on the computed interval. Actual delay is uniform in `[0, computed]`, floored by a provider's `Retry-After` ([D10](../specs/001-notification-core/design-decisions.md#d10)) |
| `IdempotencyWindow` | `168h` (7d) | How long a key guards replays. Minimum 24h. Drives the size of `notify_idem` ([D21](../specs/001-notification-core/design-decisions.md#d21)) |
| `PreCheckTTL` | `24h` | Redis pre-check entry lifetime; must not exceed `IdempotencyWindow` ([D18](../specs/001-notification-core/design-decisions.md#d18)) |
| `RetentionWindow` | `2160h` (90d) | Inbox partitions older than this are retired — **read and unread** ([D33](../specs/001-notification-core/design-decisions.md#d33)) |
| `DeliveryLogRetention` | `720h` (30d) | Delivery history. Bounce callbacks after this cannot be correlated |
| `DeadLetterRetention` | `720h` (30d) | Independent of the inbox window ([D8](../specs/001-notification-core/design-decisions.md#d8)) |
| `DigestMax` | `100` | Members per digest; a larger burst yields more digests, never an unbounded one ([D4](../specs/001-notification-core/design-decisions.md#d4)) |
| `UnreadCap` | `99` | The bounded unread count; the UI shows "99+" ([D15](../specs/001-notification-core/design-decisions.md#d15)) |
| `DefaultLocale` | `en` | Template fallback when neither the address nor the tenant has a locale |
| `MaxInlineRecipients` | `1000` | Largest inline broadcast list; larger audiences use a selector |

There is no delivery-mode switch. Every notification goes through the queue; the old `direct` mode
reintroduced the crash-loses-a-channel bug the queue exists to prevent
([D22](../specs/001-notification-core/design-decisions.md#d22)).

## Validation

`Config.Validate()` applies defaults to a copy and returns **every** problem at once, so a misconfigured
deployment fails at startup with a complete list, before any connection is dialled. It rejects:

- an empty or malformed `Realm` (library mode); an empty `StoreDSN` or `RedisURL`
- `Shards`, `ClaimBatch`, `MaxAttempts`, `DigestMax`, `UnreadCap` or `MaxInlineRecipients` below 1
- `ClaimLease` or `PollInterval` not positive
- `BackoffCeiling < BackoffBase`
- `IdempotencyWindow < 24h`, or `PreCheckTTL > IdempotencyWindow`

## Environment mapping (service mode)

The container reads these and builds a `Config`. In library mode your host does the mapping.

```bash
NOTIFY_PG_DSN=postgres://...          # required
NOTIFY_REDIS_URL=redis://...          # required
NOTIFY_SHARDS=16
NOTIFY_CLAIM_LEASE=5m
NOTIFY_POLL_INTERVAL=500ms
NOTIFY_MAX_ATTEMPTS=5
NOTIFY_BACKOFF_BASE=10s
NOTIFY_BACKOFF_CEILING=1h
NOTIFY_IDEMPOTENCY_WINDOW=168h
NOTIFY_RETENTION_WINDOW=2160h
NOTIFY_DELIVERY_LOG_RETENTION=720h
NOTIFY_DEAD_LETTER_RETENTION=720h
NOTIFY_DIGEST_MAX=100
NOTIFY_DEFAULT_LOCALE=en
```

### Service-mode settings

| Variable | Purpose |
|---|---|
| `NOTIFY_REALM_BINDINGS` | `principal=realm` pairs, comma-separated. A producer principal is its mTLS certificate SAN or its token subject. A request from an unbound principal is rejected; a request never names its own realm ([D31](../specs/001-notification-core/design-decisions.md#d31)) |
| `NOTIFY_RECIPIENT_JWKS` | `realm=https://…/jwks.json` pairs: the keys that verify each realm's recipient tokens |
| `NOTIFY_UNSUBSCRIBE_KEYS` | `kid=secret-ref` pairs for signing unsubscribe tokens; the first signs, all verify, so keys rotate without breaking old mail ([D34](../specs/001-notification-core/design-decisions.md#d34)) |
| `NOTIFY_OPERATOR_TOKENS` | Bearer tokens accepted on `/admin` routes, as secret references (`env:`, `file:`). Prefer mTLS in production; a token is for local stacks and the e2e suite |
| `NOTIFY_DIRECTORY_ADDRS` | `realm=host:port` pairs: the host's `Directory` service, if audiences resolve by callback |
| `NOTIFY_INGEST_NATS_URL` | Optional: enables the JetStream ingest adapter ([bus-subjects.md](../specs/001-notification-core/contracts/bus-subjects.md)) |

Provider credentials are **not** configuration. In library mode they belong to the channel
implementations your host constructs. In service mode `channel_providers.secret_ref` points at them —
`env:NAME`, `file:/run/secrets/name`, or a secret-manager URI — and the schema rejects a bare value, so a
key pasted into the table fails to insert rather than sitting in a database dump.
