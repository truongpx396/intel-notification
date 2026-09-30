# Integration guide

How to adopt this engine in a host product. The short version: you supply your channels, topics,
templates, addresses and an identity binding — as Go code in library mode, or as data through the API in
service mode — and you touch nothing in the delivery core.

> The notifier, dispatcher, channels and transports are not built yet, so the code here describes the
> intended API. The **checklist** and the **reasoning** are usable today — they are what the design is for.

## 0. Pick a mode

| | Library | Service |
|---|---|---|
| Your language | Go | any |
| Topics, templates | Go registrations, or the provided tables | rows, via the `Catalog` API |
| Addresses | your `AddressBook`, or the provided table | the provided table, via `Catalog.PutAddresses` |
| Notify inside your own transaction | yes — `NotifyTx` | no — call after commit; retries are safe on the same key |
| Where the queue lives | your database, so your transactions and settings apply to it ([D38](../specs/001-notification-core/design-decisions.md#d38)) | its own database |
| Recipient auth for the inbox | your session → `Identity` | a recipient token you sign |

Start embedded if you are a Go shop with one product. Move to the service when a second product, or a
non-Go one, needs to send from the same channels and templates — or when volume reaches a few hundred
notifications a second.

The reason for that last one: in library mode the queue lives in your database, and a long transaction of
yours stops vacuum reclaiming its rows. In a soak, one held open for two minutes at 1,000 notifications a
second grew the queue four to six times and cost about ninety seconds of degraded latency
([D38](../specs/001-notification-core/design-decisions.md#d38)). If you stay embedded:

- set `idle_in_transaction_session_timeout` and `statement_timeout` on your application's database role;
- keep `NotifyTx` transactions short — no provider or network call, and no waiting on a user, inside one;
- run long reads and reports on a replica, not on the primary the queue lives on.

## 1. Decide your identity binding

Three questions, answered once:

| Question | You pick | Example |
|---|---|---|
| Which product is this? | `Realm` | `"my-product"` — required, no default ([D1](../specs/001-notification-core/design-decisions.md#d1)) |
| What isolates recipients from each other? | `Tenant` | `{Kind: "workspace", ID: wsID}` |
| Who receives a notification? | `Recipient` | `{Kind: "user", ID: userID}` |

The engine never interprets these. That is what lets you notify an organization, a device, a Slack channel
or an external contact later without a migration — you change what you construct, and nothing else
(NR-018).

**Set `Realm` even if you only have one product.** It costs nothing now and cannot be retrofitted
cheaply. In service mode you do not set it per request at all: your producer credential is bound to a
realm.

**The same user in two tenants is two recipients-within-tenant.** Their idempotency keys, preferences and
inboxes are separate, which is what you want — a workspace-scoped weekly digest from each workspace.

## 2. Register your topics

A topic is a registered string with defaults, not an enum value:

```go
topics.Register("invoice_overdue", notify.TopicDef{
    DefaultChannels: []notify.ChannelKind{"in_app", "email"},
    Fallback:        []notify.ChannelKind{"sms"}, // tried only if the channels above end undelivered
    DefaultPriority: "warning",
    Essential:       true,                        // cannot be disabled; never digested; no unsubscribe
})
```

`Essential` is the flag people get wrong. It means *this recipient cannot opt out and this must not be
folded into a digest*. Reserve it for notifications whose absence causes harm — a dunning notice, a
security alert, a paused workload. Marking everything essential trains recipients to ignore the channel,
which is the failure mode that makes a notification system worthless.

`DefaultChannels` fan out in parallel; `Fallback` is a chain. A channel cannot be in both.

## 3. Implement or configure your channels

One `Channel` per delivery target. The interface is three methods, and the contract is stricter than it
looks:

- **Declare your deduplication truthfully** in `Capabilities().Dedup`. If the provider honors an
  idempotency key, pass `Delivery.IdemKey` through and declare `DedupProvider` with its window. If it does
  not — SES, SMTP, Twilio, APNs, FCM, Slack webhooks — declare `DedupNone`. The engine reports it; it does
  not pretend ([D32](../specs/001-notification-core/design-decisions.md#d32)).
- **Dedupe on `Delivery.IdemKey`, not on the notification's key.** It is distinct per address and per
  digest ([D25](../specs/001-notification-core/design-decisions.md#d25)).
- **Report outcomes, do not raise them.** `Suppressed` for a dead address (with a reason), `Rejected` for a
  permanent refusal, `Retry` for anything transient. An error means "infrastructure fault" and is treated
  as `Retry`.
- **Do not check suppressions and do not retry.** The dispatcher checks suppressions for every channel and
  owns backoff. A channel that retries internally fights the jitter that prevents a thundering herd.
- **Set `InboxBacked`** on the channel whose enablement means "show it in the inbox" — normally in-app.

Run every channel through `ChannelContract` before registering it, and against the provider's sandbox with
`-tags provider` if you declare provider deduplication.

In service mode, a channel is a `channel_providers` row: provider, priority, non-secret config, and a
`secret_ref`. Two rows for one channel give you provider failover.

## 4. Implement the template seam — and send data, not copy

Producers send `Data`; templates own the words:

```go
eng.Notifier.Notify(ctx, notify.Notification{
    Tenant: tenant, Recipient: user, Topic: "invoice_overdue",
    Data:    map[string]any{"invoice_number": "INV-042", "amount": 1250, "currency": "EUR"},
    IdemKey: "invoice:42:overdue",
})
```

One `TemplateRenderer` handles every topic, channel and locale — your own, or the provided one over
`notification_templates`. Per-tenant branding is a template override for that tenant. The in-app inbox is
rendered at read time in the viewer's locale, so changing a template changes how existing items read.
`Title` and `Body` still exist, as fallback copy for topics you have not templated yet.

## 5. Wire the driven ports (library mode)

```go
pg := postgres.NewAll(pool)
eng, err := app.New(cfg, app.Deps{
    Channels:  reg,                        // yours
    Topics:    topics,                     // yours, or pg.Topics
    Renderer:  pg.Templates,               // or yours
    Addresses: directory.New(db),          // yours: Identity → []Address, or pg.Addresses
    Audience:  directory.NewAudience(db),  // yours: audience selector → recipients, paged
    Store: pg.Store, Queue: pg.Queue, Digests: pg.Digests, Maintenance: pg.Maintenance,
    Inbox: pg.Inbox, Prefs: pg.Prefs,
    Suppressions: pg.Suppressions,
    Quotas: redis.NewQuota(rdb), Stream: redis.NewStream(rdb), PreCheck: redis.NewPreCheck(rdb),
})
```

`Addresses` returns **every** address for a channel — every phone, every email — and each becomes its own
delivery. `Audience` is paged on purpose: a non-paged resolver times out on your largest tenant
([D7](../specs/001-notification-core/design-decisions.md#d7)).

## 6. Enqueue inside your transaction where you can

If the change that causes a notification is in the same PostgreSQL database, use `NotifyTx`:

```go
tx, _ := pool.Begin(ctx)
markOverdue(ctx, tx, invoiceID)
_, err := eng.Notifier.NotifyTx(ctx, tx, n)
tx.Commit(ctx)
```

Otherwise you have a dual write: commit then crash before `Notify`, and the notification is lost
([D28](../specs/001-notification-core/design-decisions.md#d28)). If you cannot share a transaction — service
mode, or a different database — call `Notify` after commit with a deterministic `IdemKey`, and retry it
from your own outbox or job queue until it succeeds. Retries are safe; that is what the key is for.

## 7. Mount the inbox and run the worker

```go
http.Handle("/notifications/", httpapi.New(eng.Inbox, eng.Admin, identityFromSession)) // library mode
go eng.Dispatcher.Run(ctx)
go eng.Jobs.Run(ctx)
```

`identityFromSession` builds the `Identity` from **your** authenticated session — never from a request
parameter. In service mode, mint a recipient token for the signed-in user instead and let the client call
the service directly.

Any number of worker replicas may run the dispatcher and maintenance: claims are leased row by row and
jobs are single-owner through leases. See [operations.md](operations.md).

## 8. Service mode, end to end, from any language

1. Get a producer credential bound to your realm.
2. `Catalog.UpsertTopic` for each topic; `Catalog.PutTemplate` + `ActivateTemplate` for each
   (topic, channel, locale).
3. `Catalog.PutAddresses` whenever a user's email, phone or devices change.
4. Optionally implement the `Directory.ResolveAudience` service, if you broadcast to selectors rather
   than lists.
5. `Notifier.Notify` from your producers.
6. Mint recipient tokens for your front end, pointed at the service's REST surface.

## Generalization checklist

Tick per host. This is the originating design's checklist, extended by the review:

- [ ] **Realm set** — library: `Config.Realm`, not shared with another product. Service: your producer
      credential bound to one realm.
- [ ] **Tenant + Recipient chosen** — constructed from the trusted session or producer.
- [ ] **Channels registered** — each passing `ChannelContract`, each declaring `Dedup` and `InboxBacked`
      truthfully.
- [ ] **Topics registered** — default channels, fallback chain, priority, template ref, and `Essential`
      set deliberately rather than by habit.
- [ ] **Templates implemented** — producers send `Data`; branding varies by tenant in the templates.
- [ ] **Preferences exposed** — a UI over `(topic, channel)` showing each value's source; essential and
      tenant-locked pairs shown as unchangeable **with the reason stated**.
- [ ] **Tenant defaults** — if your product has customer administrators, a UI over tenant defaults and
      locks.
- [ ] **Addresses** — every address per channel; a miss is terminal, not a retry.
- [ ] **Audiences** — paged resolver, `Directory` callback, or inline lists.
- [ ] **Transactional producers use `NotifyTx`**; the rest retry `Notify` with a stable key.
- [ ] **Dispatcher and maintenance running.**
- [ ] **Quotas configured** — a realm-wide default at minimum, if you are multi-tenant.
- [ ] **Isolation verified** — `make verify-schema` green; `StreamIsolationContract` green against your
      relay.
- [ ] **Unsubscribe reachable** — the `POST` endpoint behind your `List-Unsubscribe` URL.
- [ ] **Erasure wired** — your data-subject process calls `Admin.Erase`.
- [ ] **Badge recomputed** — the unread count comes from the store and is bounded
      ([D15](../specs/001-notification-core/design-decisions.md#d15)).

## Common mistakes

| Mistake | Consequence |
|---|---|
| Taking `Recipient` from request content instead of the trusted session | Delivery redirection — an attacker notifies someone else (NR-009) |
| Omitting `IdemKey`, or deriving it non-deterministically | Duplicates on every retry; NR-002 is forfeited for that notification |
| Calling `Notify` after your commit with no retry | A crash in between loses the notification. Use `NotifyTx`, or retry from your own outbox |
| Passing copy in `Title`/`Body` instead of `Data` | No localization, no branding, and template changes never reach the inbox |
| Returning one address per channel | Only one of a user's devices is ever notified |
| Declaring `DedupProvider` for a provider without idempotency keys | The engine reports exactly-once where there is none |
| Marking every topic `Essential` | Recipients cannot opt out of anything, so they ignore the channel entirely |
| Channel retries internally | Fights the dispatcher's jittered backoff; turns a provider outage into a herd |
| Reading the unread count from a cached counter | Permanently wrong badge after one missed push |
| Creating a `notifications` partition by hand without `notify_apply_recipient_scope()` | That partition, queried by name, shows every recipient's rows |
