# Quickstart

> **Implementation started.** The domain rules and the PostgreSQL queue adapter exist and are tested;
> the notifier, dispatcher, channels and transports do not, so most snippets below describe the
> intended interface. What runs today is below.

## What works today

```bash
make test              # domain unit tests: no Docker, race detector, shuffled
make test-integration  # the queue adapter against PostgreSQL 16 via Testcontainers (needs Docker)
make verify-schema     # apply migrations to a throwaway PG16, run the schema tests
make lint arch-lint    # import boundaries and the hexagon's dependency graph
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
reg.Register(inapp.New(stream))
reg.Register(email.New(mailer, topics, links))

// 2. Register your topics with their defaults.
topics := notify.NewTopicRegistry()
topics.Register("ingestion_complete", notify.TopicDef{
    DefaultChannels: []notify.ChannelKind{"in_app"},
    Fallback:        []notify.ChannelKind{"email"}, // if in-app cannot be delivered
    DefaultPriority: "info",
})
topics.Register("credit_exhausted", notify.TopicDef{
    DefaultChannels: []notify.ChannelKind{"in_app", "email"},
    DefaultPriority: "critical",
    Essential:       true,   // cannot be disabled, never digested, no unsubscribe footer
})

// 3. Construct. Realm is required and has no default.
pg := postgres.NewAll(pool) // store, inbox, maintenance, prefs, suppressions, addresses, templates
eng, err := app.New(notify.Config{
    Realm:    "my-product",
    StoreDSN: os.Getenv("NOTIFY_PG_DSN"),
    RedisURL: os.Getenv("NOTIFY_REDIS_URL"),
}, app.Deps{
    Channels: reg, Topics: topics, Renderer: pg.Templates, Addresses: pg.Addresses,
    Audience: directory.NewAudience(db),
    Store: pg.Store, Queue: pg.Queue, Digests: pg.Digests, Maintenance: pg.Maintenance,
    Inbox: pg.Inbox, Prefs: pg.Prefs,
    Suppressions: pg.Suppressions, Quotas: redis.NewQuota(rdb), Stream: redis.NewStream(rdb),
    PreCheck: redis.NewPreCheck(rdb),
})

// 4. Notify — send data, not copy. Producers depend on the interface, nothing else.
receipt, err := eng.Notifier.Notify(ctx, notify.Notification{
    Tenant:    notify.Tenant{Kind: "workspace", ID: wsID},
    Recipient: notify.Recipient{Kind: "user", ID: userID},
    Topic:     "ingestion_complete",
    Data:      map[string]any{"doc_name": "document.pdf"},
    Payload:   map[string]string{"doc_id": docID},
    IdemKey:   "ingest:" + docID + ":complete",
})
if !receipt.Applied {
    // A replay. Not an error — the notification already exists.
}
```

When the notification belongs to a change in your own database, enqueue it in the same transaction:

```go
tx, _ := pool.Begin(ctx)
_, _ = tx.Exec(ctx, `UPDATE invoices SET status = 'overdue' WHERE id = $1`, invoiceID)
_, err = eng.Notifier.NotifyTx(ctx, tx, notify.Notification{
    Tenant: tenant, Recipient: owner, Topic: "invoice_overdue",
    Data: map[string]any{"invoice": invoiceID}, IdemKey: "invoice:" + invoiceID + ":overdue",
})
_ = tx.Commit(ctx) // the notification commits with the change, or not at all
```

Run the dispatcher and the maintenance jobs in your worker process:

```go
go eng.Dispatcher.Run(ctx)   // claims across all shards; safe with any number of replicas
go eng.Jobs.Run(ctx)  // retention, partitions, expiry — single-owner through leases
```

## Intended: as a service

```bash
docker compose -f deploy/docker-compose.yml up
```

Go producers swap one line and nothing else:

```go
n := grpcclient.New(conn)   // satisfies the same notify.Notifier
```

Producers in other languages use the generated `notify.v1` client: register topics and templates
through `Catalog`, store recipients' addresses with `Catalog.PutAddresses`, then call `Notifier.Notify`.

## Adding a channel

The whole point of the design. One file, one registration:

```go
type SlackChannel struct{ hooks WebhookClient }

func (SlackChannel) Kind() notify.ChannelKind { return "slack" }

func (SlackChannel) Capabilities() notify.ChannelCapabilities {
    return notify.ChannelCapabilities{NeedsAddress: true, RichContent: true, Dedup: notify.DedupNone}
}

func (c SlackChannel) Deliver(ctx context.Context, d notify.Delivery) (notify.DeliveryResult, error) {
    err := c.hooks.Post(ctx, d.Address.Value, d.Content.Data)
    switch {
    case err == nil:
        return notify.DeliveryResult{Outcome: notify.Delivered}, nil
    case isRevoked(err):
        return notify.DeliveryResult{Outcome: notify.Suppressed, SuppressReason: "token_revoked"}, nil
    case isPermanent(err):
        return notify.DeliveryResult{Outcome: notify.Rejected, Detail: err.Error()}, nil
    default:
        return notify.DeliveryResult{Outcome: notify.Retry, Detail: err.Error()}, nil
    }
}
```

Then `reg.Register(slack.New(hooks))`. No change to fan-out, persistence, preferences, deduplication,
retention or the schema (NR-016). Slack webhooks accept no idempotency key, so the channel declares
`DedupNone` and the engine reports it rather than pretending otherwise
([D32](design-decisions.md#d32)). Run it through `ChannelContract` before registering it.

## Next

- [docs/integration-guide.md](../../docs/integration-guide.md) — adopting it properly, including the
  generalization checklist.
- [docs/operations.md](../../docs/operations.md) — shards, dead letters, retention, alarms.
- [contracts/notification-ports.md](contracts/notification-ports.md) — the full port surface.
