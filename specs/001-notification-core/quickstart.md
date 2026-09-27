# Quickstart

> **Implementation not started.** These commands describe the intended interface. What runs today is
> `make verify-schema`, which is real and worth running — it applies the migrations to a throwaway
> PostgreSQL 16 and asserts the schema's guarantees.

## What works today

```bash
make verify-schema     # apply migrations to a throwaway PG16, assert the 7 invariants
make lint-sql          # migration hygiene
```

`verify-schema` also reproduces the inherited constraint failure before applying the fix, so the
reason the guard is a separate table is demonstrated rather than asserted
([D2](design-decisions.md#d2)).

## Intended: as a library

```bash
go get github.com/truongpx396/intel-notification
```

```go
// 1. Register your channels. Adding one later is this line plus a file.
reg := notify.NewChannelRegistry()
reg.Register(inapp.New(redisPub))
reg.Register(email.New(mailer, suppressions, topics))

// 2. Register your topics with their defaults.
topics := notify.NewTopicRegistry()
topics.Register("ingestion_complete", notify.TopicDef{
    DefaultChannels: []notify.ChannelKind{"in_app"},
    DefaultPriority: "info",
})
topics.Register("credit_exhausted", notify.TopicDef{
    DefaultChannels: []notify.ChannelKind{"in_app", "email"},
    DefaultPriority: "critical",
    Essential:       true,   // no unsubscribe footer, never digested
})

// 3. Construct. Realm is required and has no default.
n, err := app.New(notify.Config{
    Realm:    "my-product",
    StoreDSN: os.Getenv("NOTIFY_PG_DSN"),
    RedisURL: os.Getenv("NOTIFY_REDIS_URL"),
}, app.Deps{
    Channels: reg, Topics: topics,
    Store: pg.New(db), Prefs: pg.NewPrefs(db),
    Renderer: templates.New(), Addresses: directory.New(db),
    Audience: directory.NewAudience(db),
    Bus: bus.NewRedisStreams(rdb),
})

// 4. Notify. Producers depend on the interface, nothing else.
receipt, err := n.Notify(ctx, notify.Notification{
    Tenant:    notify.Tenant{Kind: "workspace", ID: wsID},
    Recipient: notify.Recipient{Kind: "user", ID: userID},
    Topic:     "ingestion_complete",
    Title:     "Your upload finished",
    Body:      "document.pdf is ready to search.",
    Payload:   map[string]string{"doc_id": docID},
    IdemKey:   "ingest:" + docID + ":complete",
})
if !receipt.Applied {
    // A replay. Not an error — the notification already exists.
}
```

Run the dispatcher in your worker process:

```go
d := app.NewDispatcher(cfg, deps)
for shard := range cfg.ShardRange() {
    go d.Run(ctx, shard)   // claim → render → deliver → record outcome
}
```

## Intended: as a service

```bash
docker compose -f deploy/docker-compose.yml up
```

Producers swap one line and nothing else:

```go
n := grpcclient.New(conn)   // satisfies the same notify.Notifier
```

## Adding a channel

The whole point of the design. One file, one registration:

```go
type SlackChannel struct{ hooks WebhookClient }

func (SlackChannel) Kind() notify.ChannelKind { return "slack" }

func (SlackChannel) Capabilities() notify.ChannelCapabilities {
    return notify.ChannelCapabilities{NeedsSubject: false, NeedsAddress: true, RichContent: true}
}

func (c SlackChannel) Deliver(ctx context.Context, d notify.Delivery) (notify.DeliveryResult, error) {
    if err := c.hooks.Post(ctx, d.Address.Value, d.Content.Data); err != nil {
        if isPermanent(err) {
            return notify.DeliveryResult{Suppressed: true, Detail: err.Error()}, nil  // terminal
        }
        return notify.DeliveryResult{Retryable: true, Detail: err.Error()}, nil       // re-driven
    }
    return notify.DeliveryResult{Delivered: true}, nil
}
```

Then `reg.Register(slack.New(hooks))`. No change to fan-out, persistence, preferences, deduplication,
retention or the schema (NR-016). Before registering it, run it through `ChannelContract` — the
re-drive assertion is what keeps exactly-once true.

## Next

- [docs/integration-guide.md](../../docs/integration-guide.md) — adopting it properly, including the
  generalization checklist.
- [docs/operations.md](../../docs/operations.md) — shards, dead letters, retention, alarms.
- [contracts/notification-ports.md](contracts/notification-ports.md) — the full port surface.
