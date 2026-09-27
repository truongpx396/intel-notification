# intel-notification

**A reusable multi-channel notification engine.** Persist it once, fan it out everywhere — in-app,
email, SMS, push, Slack, webhook — with exactly-once delivery, per-recipient preferences, a
transactional outbox, a dead-letter path and bounded retention. Adopt it as a Go library or run it
as a self-contained container.

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)

> ## ⚠️ Status: design complete, **implementation not started**
> This repository contains the **specification** — ports, data model, migrations verified against
> PostgreSQL 16, and deployment scaffolding. There is **no Go code yet**, so the snippets below
> describe the intended interface rather than something you can `go get` today.
> [ROADMAP.md](ROADMAP.md) says what exists, what is designed and what is deliberately absent;
> [tasks.md](specs/001-notification-core/tasks.md) is the build order.
>
> **Scope, stated honestly:** this is a *delivery* engine — it decides who gets told what, on which
> channel, exactly once, and proves it never told the wrong person. It is **not** a marketing
> campaign tool and **not** an on-call escalation system (that is
> [Phase 2](specs/002-escalation-workflows/), designed and not built). If you need neither
> multi-channel fan-out nor recipient-scoped durability, [ROADMAP.md](ROADMAP.md) names the tools
> you probably want instead.

```text
                 ┌──────────────────────────────────────────────────────────┐
  HOST PRODUCT   │  Notify(n)     → persisted, fanned out, exactly once     │  fire and forget,
  (any language) │  Broadcast(b)  → audience expanded OFF the request path  │  never blocking
                 └───────────────────────────┬──────────────────────────────┘
                                             ▼
              ┌── ONE TRANSACTION ───────────────────────────────────────────┐
              │  inbox row  +  one outbox row per enabled channel            │
              │  guarded by notify_idem (realm, recipient, idem_key)         │
              └───────────────────────────┬──────────────────────────────────┘
                                          ▼  drained at-least-once, per shard
              ┌── Dispatcher ────────────────────────────────────────────────┐
              │  render → resolve address → Channel.Deliver → result        │
              │  retryable → backoff · suppressed → terminal · max → DLQ    │
              └───────────────────────────┬──────────────────────────────────┘
                     in_app · email · sms · push · slack · webhook · yours
```

---

## Why this exists

Every product rebuilds this, and every rebuild makes the same four mistakes. This engine is the
same machinery with those mistakes designed out:

| The usual mistake | What it costs | What this does instead |
|---|---|---|
| Recipient hard-coded as `(workspace_id, user_id)` | Notifying an org, a device, or a Slack channel becomes a schema + RLS rewrite | `Recipient` and `Tenant` are **opaque** `{kind, id}` pairs the engine never parses |
| Channels hard-coded as an `if` ladder | Adding SMS means editing the fan-out, and the fan-out is the riskiest code you own | A **`Channel` registry** — adding a channel is one `Register` call, zero edits to fan-out |
| Event type as a Postgres `enum` | Every new notification kind is an `ALTER TYPE` migration | A **`Topic` registry** — a registered string with data-driven defaults |
| `INSERT` + publish + email, not transactional | A crash between them **silently loses the email**, and the fast dedup guard makes it permanent | A **transactional outbox**: row + per-channel entries in one txn, drained at-least-once |

The last one is not hypothetical. It is the bug this design was extracted to fix — see
[the outbox note](#the-bug-this-fixes) below.

## Six seams, swapped independently

A host adopts this by supplying its own specifics and touching none of the delivery core:

| Seam | You provide | The engine never |
|---|---|---|
| **`Channel`** | your delivery targets | branches on a channel kind |
| **`TopicRegistry`** | your event vocabulary + defaults | special-cases a topic |
| **`Recipient`/`Tenant`** | your identity + isolation model | parses either one |
| **`TemplateRenderer`** | your copy, i18n, branding | knows what a notification says |
| **`PreferenceStore`** | your opt-in model | assumes a channel is on |
| **`Store`** | your infra (Postgres+Redis reference) | requires a specific database |

## Integrating it

```go
reg := notify.NewChannelRegistry()
reg.Register(channel.NewInApp(redisPub))               // your channels
reg.Register(channel.NewEmail(mailer, suppression))
reg.Register(channel.NewSlack(webhookClient))          // adding one is a line, not a refactor

n, err := app.New(notify.Config{
    Realm:      "my-product",                          // isolates this product's idem key space
    StoreDSN:   os.Getenv("NOTIFY_PG_DSN"),
    RedisURL:   os.Getenv("NOTIFY_REDIS_URL"),
    Delivery:   notify.DeliveryOutbox,                 // crash-safe multi-channel (default)
}, app.Deps{
    Channels: reg, Store: pg.New(db), Prefs: pg.NewPrefs(db),
    Renderer: templates.New(), Topics: topics, Addresses: directory.New(db),
})

// Producers depend on the interface only — in-process today, a gRPC stub tomorrow.
n.Notify(ctx, notify.Notification{
    Tenant:    notify.Tenant{Kind: "workspace", ID: wsID},
    Recipient: notify.Recipient{Kind: "user", ID: userID},
    Topic:     "ingestion_complete",
    Title:     "Your upload finished",
    IdemKey:   "ingest:" + docID + ":complete",         // REQUIRED — the exactly-once identity
})
```

Swapping to service mode replaces `app.New(...)` with `grpcclient.New(conn)`. Both return the same
interface, so no producer changes. Full walkthrough: [docs/integration-guide.md](docs/integration-guide.md).

## What it guarantees

| Guarantee | How it is enforced | Verified |
|---|---|---|
| **A notification reaches only its recipient** | RLS predicate on `(realm, tenant, recipient)`; enforced in the store, not application code | [TEST 3](scripts/verify-schema.sql) |
| **Exactly-once per recipient** | `notify_idem (realm, recipient_kind, recipient_id, idem_key)` primary key, written in the same txn as the row | [TEST 2](scripts/verify-schema.sql) |
| **No delivery lost to a crash** | inbox row + per-channel outbox rows in one transaction; dispatcher drains at-least-once | contract test |
| **Poison messages terminate** | `MaxAttempts` then `dead_letters` + alarm — never dropped, never looping | contract test |
| **A burst becomes one digest** | one open `digest_buffer` window per `(recipient, topic, channel)`, as a partial unique index | [TEST 4](scripts/verify-schema.sql) |
| **Storage stays bounded** | inbox and dead letters range-partitioned; retention is a partition `DROP` | [migrations](migrations/) |
| **One noisy tenant cannot starve another** | per-`(realm, tenant, channel)` quota with a `defer` default | [TEST 7](scripts/verify-schema.sql) |

`make verify-schema` applies the migrations to a throwaway PostgreSQL 16 and runs those assertions.

### The bug this fixes

The originating design guarded its fan-out with a Redis `SET NX notify:applied:{idem_key}` that
short-circuited **the entire handler**. Consider attempt #1: it sets the guard, inserts the row, then
crashes *before* enqueuing the email. The redelivery hits the guard and short-circuits — so the email
is **never sent, and never can be**. The guard made the loss permanent.

Here the guard gates only the **durable write**. Channel delivery is driven off the committed outbox,
so a crash anywhere leaves durable work the dispatcher will finish. The fast Redis check stays, as
what it should always have been: an optimization to skip a database round-trip, never a correctness
mechanism.

## Documentation

| | |
|---|---|
| [ROADMAP.md](ROADMAP.md) | scope, phases, and when to use something else |
| [specs/001-notification-core/](specs/001-notification-core/) | the normative specification |
| [research.md](specs/001-notification-core/research.md) | the originating research the design rests on, carried with its history |
| [contracts/notification-ports.md](specs/001-notification-core/contracts/notification-ports.md) | every port, the 12 invariants, contract tests |
| [design-decisions.md](specs/001-notification-core/design-decisions.md) | 15 decisions, with what each one prevents |
| [data-model.md](specs/001-notification-core/data-model.md) | tables, keys, RLS, retention |
| [docs/integration-guide.md](docs/integration-guide.md) | adopting it in a host product |
| [docs/operations.md](docs/operations.md) | running it: shards, DLQ, retention, alarms |
| [docs/security.md](docs/security.md) | isolation, authn between producer and service, compliance |
| [PROVENANCE.md](PROVENANCE.md) | where this came from and what changed in the lift |

## License

MIT — see [LICENSE](LICENSE).
