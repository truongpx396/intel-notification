# Integration guide

How to adopt this engine in a host product. The short version: you supply your channels, topics,
templates and an identity binding; you touch nothing in the delivery core.

> Implementation has not started, so the code here describes the intended API. The **checklist** and
> the **reasoning** are usable today — they are what the design is for.

## 1. Decide your identity binding

Three questions, answered once:

| Question | You pick | Example |
|---|---|---|
| Which product is this? | `Realm` | `"my-product"` — required, no default ([D1](../specs/001-notification-core/design-decisions.md#d1)) |
| What isolates recipients from each other? | `Tenant` | `{Kind: "workspace", ID: wsID}` |
| Who receives a notification? | `Recipient` | `{Kind: "user", ID: userID}` |

The engine never interprets these. That is what lets you notify an organization, a device, a Slack
channel or an external contact later without a migration — you change what you construct, plus the
RLS predicate, and nothing else (NR-018).

**Set `Realm` even if you only have one product.** It costs nothing now and cannot be retrofitted
cheaply: retrofitting means rewriting every historical idempotency key.

## 2. Register your topics

A topic is a registered string with defaults, not an enum value:

```go
topics.Register("invoice_overdue", notify.TopicDef{
    DefaultChannels: []notify.ChannelKind{"in_app", "email"},
    DefaultPriority: "warning",
    Essential:       true,   // no unsubscribe footer; never digested
})
```

`Essential` is the flag people get wrong. It means *this recipient cannot opt out and this must not be
folded into a digest*. Reserve it for notifications whose absence causes harm — a dunning notice, a
security alert, a paused workload. Marking everything essential trains recipients to ignore the
channel, which is the failure mode that makes a notification system worthless.

## 3. Implement your channels

One `Channel` per delivery target. The interface is three methods, and the contract is stricter than
it looks:

- **`Deliver` must be idempotent on `IdemKey`.** The dispatcher re-drives after a crash. If your
  channel is not idempotent, exactly-once is false regardless of what the engine does. For providers
  with an idempotency-key header, pass it through; for those without, dedupe on your side.
- **Return `Suppressed: true`, not an error, for a permanently undeliverable address.** An error means
  "retry me"; suppression means "never again". Getting this wrong retries a dead address forever.
- **Return `Retryable: true` for transient failures** and let the dispatcher own backoff. A channel
  that retries internally fights the dispatcher's backoff and defeats the jitter that prevents a
  thundering herd ([D10](../specs/001-notification-core/design-decisions.md#d10)).

Run every channel through `ChannelContract` before registering it. The re-drive assertion is the one
that catches the mistake that matters.

## 4. Implement the template seam

`TemplateRenderer` is the only place copy lives. One implementation handles every topic, channel and
locale:

```go
func (r Renderer) Render(ctx context.Context, topic notify.Topic, ch notify.ChannelKind,
    locale string, n notify.Notification) (notify.RenderedContent, error)
```

Per-tenant branding belongs here too — take the tenant from `n.Tenant` and vary the template. The
engine deliberately does not know what a notification says, which is why re-skinning every
notification is one implementation swap and no engine change.

## 5. Wire the driven ports

```go
n, err := app.New(cfg, app.Deps{
    Channels:  reg,                        // yours
    Topics:    topics,                      // yours
    Renderer:  templates.New(),             // yours
    Addresses: directory.New(db),           // yours: Recipient → Address
    Audience:  directory.NewAudience(db),   // yours: audience selector → recipients, paged
    Store:     pg.New(db),                  // provided
    Prefs:     pg.NewPrefs(db),             // provided
    Bus:       bus.NewRedisStreams(rdb),    // provided
})
```

The four "yours" entries are the entire product-specific surface. `Addresses` and `Audience` are yours
because only your directory knows how to resolve them — and `Audience` is paged on purpose, because a
non-paged resolver times out on your largest tenant
([D7](../specs/001-notification-core/design-decisions.md#d7)).

## 6. Run the dispatcher

Delivery happens in a worker, not in the request. Run one drainer per shard, or N replicas in a queue
group:

```go
d := app.NewDispatcher(cfg, deps)
for _, shard := range cfg.Shards() {
    go d.Run(ctx, shard)
}
```

Also schedule the three periodic jobs: digest flush, retention, quota reset. They are single-owner —
two workers retiring partitions concurrently is a race with no upside. See
[operations.md](operations.md).

## Generalization checklist

Tick per host. This is the originating design's checklist, kept because it is genuinely the right one:

- [ ] **Realm set** — not empty, and not shared with another product.
- [ ] **Tenant + Recipient chosen** — constructed from request context; RLS predicate matches.
- [ ] **Channels registered** — each idempotent on `IdemKey`, each returning `Suppressed` / `Retryable`
      correctly, each passing `ChannelContract`.
- [ ] **Topics registered** — default channels, priority, template ref, and `Essential` set
      deliberately rather than by habit.
- [ ] **Templates implemented** — per (topic, channel, locale); branding varies by tenant here.
- [ ] **Preferences exposed** — a UI that writes `(topic, channel, enabled)`; essential topics shown as
      non-disableable **with the reason stated**, never silently ignored.
- [ ] **AddressBook wired** — a miss is terminal, not a retry.
- [ ] **AudienceResolver wired** — paged.
- [ ] **Delivery durability chosen** — `outbox` (default, crash-safe) vs `direct` (dev / in-app only).
- [ ] **Dispatcher + the three scheduled jobs running.**
- [ ] **Quotas configured** per tenant per channel, if you are multi-tenant.
- [ ] **Isolation verified** — run the RLS leak suite against *your* binding, as the role your worker
      actually uses.
- [ ] **Badge recomputed** — the unread count comes from the store, and your relay reconciles on
      reconnect ([D15](../specs/001-notification-core/design-decisions.md#d15)).

## Common mistakes

| Mistake | Consequence |
|---|---|
| Taking `Recipient` from request content instead of the trusted session | Delivery redirection — an attacker notifies someone else (NR-009) |
| Omitting `IdemKey`, or deriving it non-deterministically | Duplicates on every retry; NR-002 is forfeited for that notification |
| Marking every topic `Essential` | Recipients cannot opt out of anything, so they ignore the channel entirely |
| Channel retries internally | Fights the dispatcher's jittered backoff; turns a provider outage into a herd |
| Reading the unread count from a cached counter | Permanently wrong badge after one missed push |
| Changing `Shards` in place | Double-delivery — drain first ([D11](../specs/001-notification-core/design-decisions.md#d11)) |
