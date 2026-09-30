# intel-notification

**A reusable multi-channel notification engine.** Persist it once, fan it out everywhere — in-app,
email, SMS, push, Slack, webhook — with idempotent delivery, per-recipient and per-tenant preferences,
a transactional outbox, a dead-letter path and bounded retention. Adopt it as a Go library, or run it
as a self-contained service that any language can call.

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)

> ## ⚠️ Status: design complete, **implementation started** (Go 1.26)
> This repository contains the **specification** — ports, data model, migrations — and the first of
> the implementation: the domain types and rules, every port, the configuration, and the **PostgreSQL
> adapter for the delivery queue**, proven against a real PostgreSQL 16 by an integration suite. The
> notifier, dispatcher, channels and transports are not built yet, so most snippets below still
> describe the intended interface.
> [ROADMAP.md](ROADMAP.md) says what exists, what is designed and what is deliberately absent;
> [tasks.md](specs/001-notification-core/tasks.md) is the build order.
>
> **Scope, stated honestly:** this is a *delivery* engine — it decides who gets told what, on which
> channel, once, and proves it never told the wrong person. It is **not** a marketing campaign tool
> and **not** an on-call escalation system (that is [Phase 2](specs/002-escalation-workflows/),
> designed and not built). If you need neither multi-channel fan-out nor recipient-scoped durability,
> [ROADMAP.md](ROADMAP.md) names the tools you probably want instead.

```text
                 ┌──────────────────────────────────────────────────────────────┐
  HOST PRODUCT   │  Notify(n) / NotifyTx(tx, n) → persisted and enqueued, once   │  returns at commit,
  Go library, or │  Broadcast(b)                → durable, paged, resumable      │  never waits on a
  any language   │  Cancel · Status                                              │  provider
  over gRPC      └───────────────────────────────┬──────────────────────────────┘
                                                 ▼
              ┌── ONE TRANSACTION ───────────────────────────────────────────────┐
              │  notify_idem guard (realm, tenant, recipient, idem_key)          │
              │  inbox row · one queue row per enabled channel · digest windows  │
              └───────────────────────────┬──────────────────────────────────────┘
                                          ▼  SKIP LOCKED leases, fenced by token
              ┌── Dispatcher (any number of replicas) ────────────────────────────┐
              │  resolve addresses (all devices) → suppression check → quota     │
              │  → render (templates, locale) → Channel.Deliver → record outcome │
              │  retry → jittered backoff · dead address → suppressed            │
              │  exhausted → dead letter · undelivered → next fallback channel   │
              └───────────────────────────┬──────────────────────────────────────┘
                     in_app · email · sms · push · slack · webhook · yours
```

---

## Why this exists

Every product rebuilds this, and every rebuild makes the same mistakes. This engine is the same
machinery with those mistakes designed out:

| The usual mistake | What it costs | What this does instead |
|---|---|---|
| Recipient hard-coded as `(workspace_id, user_id)` | Notifying an org, a device, or a Slack channel becomes a schema + RLS rewrite | `Recipient` and `Tenant` are **opaque** `{kind, id}` pairs the engine never parses |
| Channels hard-coded as an `if` ladder | Adding SMS means editing the fan-out, and the fan-out is the riskiest code you own | A **`Channel` registry** read through capabilities — adding a channel is one `Register` call |
| Event type as a Postgres `enum` | Every new notification kind is an `ALTER TYPE` migration | A **`Topic` registry** — a registered string with data-driven defaults and a fallback chain |
| `INSERT` + publish + email, not transactional | A crash between them **silently loses the email**, and the fast dedup guard makes it permanent | A **transactional outbox**: row + per-channel entries in one transaction, claimed under fenced leases |
| Producer notifies after its own commit | A crash in between loses the notification | **`NotifyTx`** enqueues inside the producer's transaction |
| A shared queue drained oldest-first | One tenant's burst delays everyone else | An exhausted tenant's backlog is **moved out of the claim range** in one statement |

The fourth one is not hypothetical. It is the bug this design was extracted to fix — see
[the outbox note](#the-bug-this-fixes) below.

## Seams, swapped independently

A host adopts this by supplying its own specifics and touching none of the delivery core. In service
mode each of these is data the service owns, managed over the API:

| Seam | You provide | The engine never |
|---|---|---|
| **`Channel`** | your delivery targets and their dedup guarantees | branches on a channel kind |
| **`TopicRegistry`** | your event vocabulary + defaults | special-cases a topic |
| **`Recipient`/`Tenant`** | your identity + isolation model | parses either one |
| **`TemplateRenderer`** | your copy, i18n, branding | knows what a notification says |
| **`AddressBook`** | where each recipient receives — every device | assumes one address per channel |
| **`PreferenceStore`** | your opt-in model, per recipient and per tenant | assumes a channel is on |
| **`Store`** | your infra (PostgreSQL + Redis reference) | requires a specific database |

## Integrating it

```go
reg := notify.NewChannelRegistry()
reg.Register(inapp.New(stream))                        // your channels
reg.Register(email.New(mailer, topics, links))
reg.Register(slack.New(webhookClient))                 // adding one is a line, not a refactor

pg := postgres.NewAll(pool)
eng, err := app.New(notify.Config{
    Realm:    "my-product",                            // isolates this product's key space
    StoreDSN: os.Getenv("NOTIFY_PG_DSN"),
    RedisURL: os.Getenv("NOTIFY_REDIS_URL"),
}, app.Deps{
    Channels: reg, Topics: topics, Renderer: pg.Templates, Addresses: pg.Addresses,
    Store: pg.Store, Queue: pg.Queue, Digests: pg.Digests, Maintenance: pg.Maintenance,
    Inbox: pg.Inbox, Prefs: pg.Prefs,
    Suppressions: pg.Suppressions, Quotas: redis.NewQuota(rdb), Stream: redis.NewStream(rdb),
})

// Producers depend on the interface only — in-process today, a gRPC stub tomorrow.
eng.Notifier.Notify(ctx, notify.Notification{
    Tenant:    notify.Tenant{Kind: "workspace", ID: wsID},
    Recipient: notify.Recipient{Kind: "user", ID: userID},
    Topic:     "ingestion_complete",
    Data:      map[string]any{"doc_name": docName}, // templates own the copy
    IdemKey:   "ingest:" + docID + ":complete",      // REQUIRED — the exactly-once identity
})
```

Swapping to service mode replaces `app.New(...)` with `grpcclient.New(conn)`. Both satisfy the same
interface, so no producer changes. Producers in other languages use the generated `notify.v1` client.
Full walkthrough: [docs/integration-guide.md](docs/integration-guide.md).

## What it guarantees

| Guarantee | How it is enforced | Verified |
|---|---|---|
| **A notification reaches only its recipient** | One forced RLS policy on every scoped table **and every partition**; scope set per transaction; the live stream keyed by the identity's hash and re-read under RLS | [TEST 4–6](scripts/verify-schema.sql), stream contract test |
| **Once per recipient** | `notify_idem` keyed by realm, tenant and recipient, written first in the same transaction, within a bounded window | [TEST 1–3](scripts/verify-schema.sql) |
| **Once per channel, stated honestly** | Each channel declares provider, local or no deduplication on a per-address delivery key; the engine reports it | channel contract test |
| **No delivery lost to a crash** | Inbox row + queue rows in one transaction; `SKIP LOCKED` leases; stale workers fenced by token | [`TestClaimLeasesAndSkipsLockedRows`, `TestFencedWrites`](adapters/driven/postgres/queue_integration_test.go) |
| **Poison terminates** | Attempts counted at claim, so even a delivery that crashes its worker reaches the ceiling; genuine failures dead-lettered with a reason | [`TestClaimLeasesAndSkipsLockedRows`, `TestFinish`](adapters/driven/postgres/queue_integration_test.go) |
| **A burst becomes bounded digests** | One open window per `(recipient, topic, channel)`, sealed at `DigestMax`, flushed once | [TEST 7](scripts/verify-schema.sql), [`TestDigestAppendUnderConcurrency`](adapters/driven/postgres/digest_integration_test.go) |
| **One noisy tenant cannot starve another** | Quotas with a realm default; an exhausted tenant's backlog leaves the claim range | [`TestDeferTenantChannel`](adapters/driven/postgres/queue_integration_test.go) |
| **Storage stays bounded** | Partitioned inbox, history and dead letters; windowed idempotency; a queue that holds pending work only | [TEST 8–9](scripts/verify-schema.sql), [`TestFinish`, `TestExpireIdem`](adapters/driven/postgres/) |
| **Erasable** | One call removes a recipient everywhere; suppressions survive as hashes | [TEST 11](scripts/verify-schema.sql), [`TestErase`](adapters/driven/postgres/maintenance_integration_test.go) |

Two suites prove these, both **as the table owner** — the role a worker actually uses, neither
superuser nor BYPASSRLS:

- `make verify-schema` applies the migrations to a throwaway PostgreSQL 16 and asserts what the schema
  alone holds: constraints, forced row-level security, key spaces.
- `make test-integration` runs every queue state transition — SQL in the Go adapter — against a real
  PostgreSQL 16 started by Testcontainers, in parallel, one cloned database per test. Each guarantee was
  mutation-checked: break it on purpose and a named test fails.

`make test` runs the domain's unit tests with no infrastructure; `make ci` runs everything. A
Playwright suite for the REST and SSE surface is written against the contract and switches on with the
HTTP layer. How the layers fit, and the conventions every test follows — table-driven, parallel,
Testcontainers, Playwright — are in [docs/testing.md](docs/testing.md).

### The bug this fixes

The originating design guarded its fan-out with a Redis `SET NX notify:applied:{idem_key}` that
short-circuited **the entire handler**. Consider attempt #1: it sets the guard, inserts the row, then
crashes *before* enqueuing the email. The redelivery hits the guard and short-circuits — so the email is
**never sent, and never can be**. The guard made the loss permanent.

Here channel delivery is driven off the committed queue, so a crash anywhere leaves durable work. The
fast Redis check stays as what it should always have been: a read-only optimization, written only after
commit, that can skip a database round-trip and can never skip a notification.

## Documentation

| | |
|---|---|
| [ROADMAP.md](ROADMAP.md) | scope, phases, and when to use something else |
| [specs/001-notification-core/](specs/001-notification-core/) | the normative specification |
| [contracts/notification-ports.md](specs/001-notification-core/contracts/notification-ports.md) | every port, the 16 invariants, the dispatcher algorithm, contract tests, the gRPC surface |
| [design-decisions.md](specs/001-notification-core/design-decisions.md) | 38 decisions, each with what it prevents — D16–D37 from the architecture review, D38 from measurement |
| [data-model.md](specs/001-notification-core/data-model.md) | tables, keys, RLS, state transitions, retention |
| [plan.md](specs/001-notification-core/plan.md) | build order and the capacity model |
| [research.md](specs/001-notification-core/research.md) | the originating research the design rests on, carried with its history |
| [docs/integration-guide.md](docs/integration-guide.md) | adopting it in a host product, in either mode |
| [docs/operations.md](docs/operations.md) | running it: shards, leases, dead letters, retention, erasure, alarms |
| [docs/security.md](docs/security.md) | isolation, the live stream, producer and recipient auth, personal data |
| [docs/testing.md](docs/testing.md) | the test layers and the conventions every new test follows |
| [PROVENANCE.md](PROVENANCE.md) | where this came from and what changed in the lift and the review |

## License

MIT — see [LICENSE](LICENSE).
