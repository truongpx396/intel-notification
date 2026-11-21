# Engineering review — 2026-11-21

A whole-codebase review of `intel-notification` at `23a1be3` (Stages 1–12 built), covering Go idiom, structure,
reliability, scalability, security and operability. It is a **backlog of findings to fix**, written so that an
engineer or a coding agent can pick one finding up and fix it without re-reading the whole repository.

Status of this document: findings are **open** until a PR closes them. When you fix one, tick its box in the
[summary table](#summary) and link the PR. Do not renumber findings; add new ones with the next free number.

## How to use this document

Read these rules before fixing anything. They are the repository's rules ([CLAUDE.md](../../CLAUDE.md)),
restated here because a review finding is not a licence to skip them.

1. **The spec outranks this review.** Order of authority: constitution → `specs/*/contracts/` → `spec.md` →
   `design-decisions.md` → code. Several findings below say **"needs a decision"**: the fix changes a
   behaviour the spec states, so the decision (or contract) is amended first, in the same PR, with the failure
   mode it prevents. Never "fix" code into disagreement with the spec.
2. **Test first, and watch it fail on its assertion.** Commit order `test:` (red) → `fix:` (green) →
   `refactor:`. Assert each guarantee at the lowest layer that can hold it ([testing.md](../testing.md)). Add a
   row to [mutations.md](../mutations.md) for every new guarantee.
3. **One finding per PR** unless the table groups them. Branch `fix/…`, `perf/…`, `docs/…`.
4. **Keep the hexagon.** `domain` imports nothing from the module; `app` imports only `config`, `domain`,
   `ports`; adapters never import `app`; only `cmd` wires concrete types ([architecture rules](../../.claude/rules/architecture.md)).
   No `if channel == "email"`: anything channel-specific is a declared capability.
5. **Integration tests run as `db.Owner`, never `db.Admin`.** Single tests always run with
   `-race -shuffle=on -count=1`.
6. Line numbers below are as of `23a1be3`; search for the quoted identifier if they have drifted.

Severity: **High** = a correctness, security or availability defect a production deployment would hit.
**Medium** = a real weakness under load, at scale or in operations. **Low** = hygiene, idiom, small risk.

## What is already good (do not "fix" these)

The codebase is well above average. These choices look unusual and are deliberate; a change to any of them
needs a design decision, not a refactor:

- Queue state transitions are single SQL statements with data-modifying CTEs in the Go adapter, fenced by a
  lease token, not PL/pgSQL ([D37](../../specs/001-notification-core/design-decisions.md#d37)). The fence is in every
  worker write. Keep it in any new one.
- `lease_expires_at` is in **no index** so a claim stays a HOT update; `fillfactor = 60` and the aggressive
  autovacuum settings on `notification_outbox` are measured, not guessed (D19).
- No foreign key into a partitioned table (D6); no default partition (D33); `notify_idem` is a separate,
  hash-partitioned table because a unique index on a `created_at`-partitioned table cannot exist (D2).
- Recipient scope is set **per transaction** (`set_config(..., true)`), never per session (D17); the RLS
  policy compares against `current_setting(..., true)`, so an unset scope matches nothing.
- No fake `Queue` or `Store` in the test kit: leases, fencing and RLS are proven against PostgreSQL.
- A fast path (Redis pre-check, quota counter, live stream) never holds correctness and fails open/miss.
- Hand-written JWT/JWKS verification pins the algorithm to the key type (no `alg` confusion, no `none`, no
  HS256), bounds token and key-set sizes, and verifies issuer/audience/exp/nbf/iat with leeway. Keep it, but
  see [R-23](#r-23) (fuzz it).
- The webhook channel blocks private/link-local/CGNAT destinations **at dial time** (DNS-rebinding safe),
  disables proxies and redirects, and signs `timestamp.body` with HMAC.
- Constant-time comparisons for operator tokens (digest + `subtle`), unsubscribe tokens and Svix signatures.
- `LoadConfig` reports every problem at once; credentials are references (`env:`, `file:`), redacted from
  provider reports.

## Summary

| ID | Sev | Area | Finding | Needs a decision? | Status |
|----|-----|------|---------|-------------------|--------|
| [R-01](#r-01) | High | Security | HTML injection: producer data is rendered into HTML email bodies with `text/template`, unescaped | Yes (capability in contract) | ☐ |
| [R-02](#r-02) | High | Operability | Service mode wires no `Metrics`: every alarm in operations.md is dead | No | ☐ |
| [R-03](#r-03) | High | Reliability | Shutdown: open SSE streams consume the whole grace period, so gRPC is hard-stopped; in-flight sends are cancelled mid-request | No | ☐ |
| [R-04](#r-04) | High | Reliability | Nothing bounds a send to less than `ClaimLease`; failover chains of SMTP (2 min each) outlive the 5 min lease → duplicates | No | ☐ |
| [R-05](#r-05) | High | Reliability | `NOTIFY_PROVIDER_REFRESH=0` (or negative) panics the process | No | ☐ |
| [R-06](#r-06) | High | Privacy | Erasure misses recipient identifiers in `notification_broadcasts.recipients` and in bus poison dead letters | Maybe (D35) | ☐ |
| [R-07](#r-07) | Medium | Reliability | gRPC server: no panic recovery, keepalive policy, health service or stream limits | No | ☐ |
| [R-08](#r-08) | Medium | Operability | HTTP surface: no access log, request id, structured panic logging; `DeadlineExceeded` → 500 | No | ☐ |
| [R-09](#r-09) | Medium | Scalability/DoS | One Redis connection per SSE stream and no cap per identity or per process | No | ☐ |
| [R-10](#r-10) | Medium | Availability | `/readyz` fails when Redis is down, although Redis is a fast path that never holds correctness | Yes | ☐ |
| [R-11](#r-11) | Medium | Availability | JWKS refresh runs inline, under the realm's mutex, on the caller's context | No | ☐ |
| [R-12](#r-12) | Medium | Performance | Provider manager rebuilds every channel (and every HTTP transport) on each refresh tick | No | ☐ |
| [R-13](#r-13) | Medium | Throughput | Dispatcher batch is a barrier: one slow send holds every other shard; goroutines unbounded per batch | No | ☐ |
| [R-14](#r-14) | Medium | Correctness | Quota is taken before render and send, so retries and render failures spend budget | Yes (D5/D24 wording) | ☐ |
| [R-15](#r-15) | Medium | Throughput | Broadcast expansion is ~8 round trips per recipient; pages are FIFO across tenants | No (fairness: yes) | ☐ |
| [R-16](#r-16) | Medium | Throughput/Correctness | NATS ingest handles one message at a time per realm; DLQ subject lives inside the ingest stream; realm `dlq` collides | No | ☐ |
| [R-17](#r-17) | Medium | Operations | No migration runner or schema version table; required before the first `v*` tag (T057) | Yes | ☐ |
| [R-18](#r-18) | Medium | Security | The service connects as the table **owner**, which can `ALTER TABLE … NO FORCE ROW LEVEL SECURITY` | Yes (D17) | ☐ |
| [R-19](#r-19) | Medium | Performance | Topic and template lookups are uncached on hot paths; templates are re-parsed on every render | No | ☐ |
| [R-20](#r-20) | Medium | Correctness | `PUT /admin/tenants/…/preferences` applies rows one by one: a failure at row *i* leaves rows 0..i-1 applied | No | ☐ |
| [R-21](#r-21) | Low | Latency | The LISTEN wake source never reconnects after its connection drops | No | ☐ |
| [R-22](#r-22) | Medium | Config | Negative or tiny retention windows are accepted and make the retention job drop every past partition | No | ☐ |
| [R-23](#r-23) | Medium | Security/Quality | CI has no `govulncheck`, no `gosec`, no fuzz tests for the hand-written parsers | No | ☐ |
| [R-24](#r-24) | Low | Feature gap | The `smtp` provider factory ignores the row's credential: no SMTP AUTH in service mode | No | ☐ |
| [R-25](#r-25) | Low | Performance | Webhook transport keeps 4 idle conns per host while a batch sends up to 100 concurrently | No | ☐ |
| [R-26](#r-26) | Low | Correctness | `RecordPoison` is check-then-insert, not atomic | No | ☐ |
| [R-27](#r-27) | Low | Performance | Loading digest members cannot prune `notifications` partitions | No | ☐ |
| [R-28](#r-28) | Low | Correctness | `NotifyTx` inside a caller's REPEATABLE READ transaction cannot read a concurrent replay | Doc only | ☐ |
| [R-29](#r-29) | Low | Idiom | Small duplications and idiom nits | No | ☐ |
| [R-30](#r-30) | Low | API | An idempotency key reused with a different payload is silently a replay | Yes | ☐ |
| [R-31](#r-31) | Low | Design | Claims are ordered by due time only; a critical notification waits behind an info backlog | Yes | ☐ |
| [R-32](#r-32) | Low | Operability | Log level is not configurable; the binary carries no version | No | ☐ |

Suggested order: R-05 (minutes), R-02, R-03, R-04, R-01, R-06, R-07, R-17 (before `v0.1.0`), then the rest.

---

## High

### R-01

**HTML injection in email bodies.** · Security · High · needs a decision

- **Where:** [email.go:211](../../adapters/driven/channel/email/email.go#L211) sends `d.Content.Body` as
  `Mail.HTML`. The body comes from [domain/template.go:304](../../domain/template.go#L304) `ExecuteTemplate`,
  which is `text/template`; with no template, the dispatcher uses the notification's raw `Title`/`Body`
  ([dispatcher.go:364](../../app/dispatcher.go#L364)).
- **Problem:** `Notification.Data` is producer-supplied and routinely carries end-user content (a comment, a
  display name, a project title). `{{.comment}}` in an email template is inserted verbatim into HTML. A user who
  names themselves `<a href="https://evil.example">Reset your password</a>` gets that link mailed, from the
  product's own domain, to everyone notified about them. [security.md](../security.md) says "Channels escape
  content for their medium"; the email channel does not.
- **Fix:**
  1. Add a declared capability, e.g. `ChannelCapabilities.BodyFormat` (`text` | `html`), in `domain` and in
     [contracts/notification-ports.md](../../specs/001-notification-core/contracts/notification-ports.md) (contract edit: needs
     confirmation and a decision entry naming this failure mode). The email channel declares `html`.
  2. The renderer executes a template for an `html` channel with **`html/template`** (contextual escaping),
     and with `text/template` otherwise. `RenderRequest` needs the format; the dispatcher reads it from the
     channel's capabilities (no channel-name branch). Keep the closed function map and the `checkNode`
     restrictions for both.
  3. The no-template fallback must be escaped too: when the channel is `html`, the dispatcher (or the email
     channel) wraps `n.Title`/`n.Body` with `html.EscapeString` and converts newlines to `<br>`.
  4. `CheckTemplate` / `PutTemplate` validate an `html` template with `html/template` so a template that does
     not parse as HTML is refused at save time.
- **Tests:** domain unit test: a template `<p>{{.name}}</p>` rendered for an html channel with
  `name = "<script>x</script>"` yields `&lt;script&gt;`; text channels are unchanged. Email channel test: the
  fallback copy is escaped. Mutation: switch back to `text/template` → the test fails.

### R-02

**Service mode counts nothing.** · Operability · High

- **Where:** [service.go:186-215](../../cmd/notifyd/service.go#L186) builds the dispatcher, maintenance and
  flusher with no `Deps.Metrics`. There is no production implementation of `ports.Metrics` in the module
  (only `notifytest.NewMetrics`).
- **Problem:** [operations.md](../operations.md) tells operators to alarm on `notify.dlq.dead.count`,
  `notify.lease.lost`, `notify.partition.missing`, `notify.partition.unscoped` (an **isolation incident**
  alarm), `notify.db.oldest_xmin_age` and `notify.db.checkpoints_requested`. In `notifyd` none of them is ever
  emitted, so a tenant-isolation incident or a dead-letter storm is silent.
- **Fix:**
  1. Add `adapters/driven/metrics/prometheus` (or OpenTelemetry metrics) implementing `ports.GaugeMetrics`.
     Map dotted names to Prometheus names (`notify_dlq_dead_count_total`, …) in one table; labels are the
     closed set in `domain/metrics.go`.
  2. Serve `/metrics` on a **separate** listener (`NOTIFY_METRICS_ADDR`, default off or `:9100`), never on the
     recipient-facing 8080.
  3. Wire it into every `app.Deps` in `cmd/notifyd` (dispatcher, maintenance, flusher, expander).
  4. While there, add the standard RED metrics the surfaces lack: HTTP/gRPC request count and latency by
     route/method and code, dispatcher claim latency, deliveries by channel and outcome, queue depth (a cheap
     `count(*)` per shard on the maintenance tick, or `pg_class.reltuples`).
- **Tests:** a unit test that the adapter's `Inc`/`Gauge` produce the expected series; a `cmd/notifyd`
  integration test that a dead-lettered delivery increments the counter scraped from `/metrics`.
- **Docs:** add the variable to [configuration.md](../configuration.md) and `.env.example`.

### R-03

**Shutdown is not graceful when any SSE stream is open, and in-flight sends are aborted.** · Reliability · High

- **Where:** [service.go:549-563](../../cmd/notifyd/service.go#L549) and
  [dispatcher.go:379](../../app/dispatcher.go#L379).
- **Problem 1:** `http.Server.Shutdown` waits for connections to go idle, but an SSE handler
  ([httpapi/stream.go:96](../../adapters/driving/httpapi/stream.go#L96)) only returns when its request
  context ends, and `Shutdown` does not cancel request contexts. With one stream open, `Shutdown` burns the
  whole 15 s `stopCtx`, then the gRPC branch finds `stopCtx` already expired and calls `grpcSrv.Stop()`
  immediately, cutting in-flight producer `Notify` calls. The HTTP connections themselves are never closed
  (`Close` is not called after the timed-out `Shutdown`).
- **Problem 2:** the dispatcher passes the worker context to `ch.Deliver`. Cancelling it on SIGTERM aborts an
  HTTP/SMTP request that may already have reached the provider; the outcome is recorded as a retry and the
  next worker sends again — a duplicate on any `DedupNone` channel (webhook). `recordCtx` already detaches the
  *write*; the *send* needs the same treatment.
- **Fix:**
  1. Give the HTTP server a base context that is cancelled when shutdown starts
     (`BaseContext: func(net.Listener) context.Context { return streamsCtx }`, cancel it via
     `httpSrv.RegisterOnShutdown(cancelStreams)`), so streams end promptly and clients reconnect elsewhere.
  2. Shut HTTP and gRPC down **in parallel**, each with its own deadline; after a timed-out `Shutdown`, call
     `httpSrv.Close()`.
  3. In the dispatcher, call `Deliver` on `context.WithTimeout(context.WithoutCancel(ctx), sendBudget)` (see
     R-04 for `sendBudget`), and stop claiming new batches when `ctx` ends. `Run` then returns once in-flight
     handlers finish. Make the overall shutdown grace (`NOTIFY_SHUTDOWN_GRACE`, default ≥ the send budget)
     configurable and document that the orchestrator's termination grace must exceed it.
- **Tests:** `cmd/notifyd` integration: open a stream, start a slow gRPC call, cancel the run context, assert
  the gRPC call completes and `Run` returns well under 15 s. App unit test with a channel that blocks until
  released: cancel the dispatcher's context, release the channel, assert the outcome is recorded as delivered
  (not retried).

### R-04

**No send is bounded by the lease.** · Reliability · High

- **Where:** [dispatcher.go:379](../../app/dispatcher.go#L379) (no deadline);
  [smtp.go:105](../../adapters/driven/channel/email/smtp.go#L105) (`defaultTimeout = 2 * time.Minute`);
  [failover.go:107](../../adapters/driven/channel/failover/failover.go#L107) (members are tried in sequence);
  [config.go:30](../../config.go#L30) (`ClaimLease` default 5 m, "must exceed the slowest channel's send
  timeout" — stated, not enforced).
- **Problem:** a failover chain of two SMTP relays and Resend can take 2 m + 2 m + 30 s plus render and
  address lookups, approaching the 5 m lease; with three SMTP members it exceeds it. When the lease lapses,
  another worker re-claims and sends again while the first is still sending; the first worker's write is fenced
  out and counted as `notify.lease.lost` — after the duplicate has happened.
- **Fix:**
  1. Dispatcher: `sendBudget = ClaimLease - safetyMargin` measured from the claim (the claim time is known),
     applied as a deadline to the `Deliver` call (and, ideally, to render/address work). A send that hits it is
     a `Retry` recorded while the lease is still held.
  2. Failover: give each member a share of the remaining deadline rather than its own full timeout, or stop
     trying further members when the remaining budget is below a floor.
  3. Optional: an optional `ports` interface `SendTimeout() time.Duration` that channels implement, and a
     startup check in `cmd/notifyd` that refuses a configuration whose worst-case chain exceeds `ClaimLease`.
- **Tests:** app unit test with a fake clock and a channel that blocks past the budget: the delivery is retried
  under the held lease, never left to lapse. Failover unit test: the second member receives a context whose
  deadline is the remaining budget.

### R-05

**A zero or negative provider refresh interval crashes the service.** · Reliability · High (trivial fix)

- **Where:** [cmd/notifyd/config.go:158-159](../../cmd/notifyd/config.go#L158) accepts any parsed duration;
  [service.go:158](../../cmd/notifyd/service.go#L158) calls `time.NewTicker(cfg.ProviderRefresh)`, which panics
  for `d <= 0`. The panic happens in a loop goroutine after the listeners are bound.
- **Fix:** in `LoadConfig`, `if c.ProviderRefresh <= 0 { bad("NOTIFY_PROVIDER_REFRESH", "must be positive") }`.
- **Test:** `TestLoadConfig` table row: `NOTIFY_PROVIDER_REFRESH=0s` and `-1m` are reported problems.

### R-06

**Erasure leaves recipient identifiers behind.** · Privacy · High · maybe needs a decision (D35)

- **Where:** [postgres/maintenance.go:144](../../adapters/driven/postgres/maintenance.go#L144) `erasedTables`.
- **Problem:** `Erase` deletes by the `recipient_*` columns. Two places hold a recipient identity without those
  columns, so `verify-schema` TEST 5 does not see them and `Erase` does not touch them:
  1. `notification_broadcasts.recipients jsonb` — an inline broadcast list keeps every recipient's
     `{kind, id}` until `JobBroadcastCleanup` expires the completed row (and indefinitely if the broadcast
     never completes).
  2. Bus poison dead letters ([poison.go:24](../../adapters/driven/postgres/poison.go#L24)): `payload.body`
     is the producer's message, which names the tenant and recipient, but the row has NULL recipient columns.
     The same body is republished to `notify.ingest.dlq.<realm>` in the `NOTIFY_INGEST` stream (7-day max age).
- **Fix (pick per D35; record the choice):**
  - Broadcasts: after a page commits, the recipients it covered are no longer needed; either null out
    `recipients` when the broadcast completes, or have `Erase` rewrite the array without the recipient
    (`jsonb_path` filter) for pending broadcasts in the realm/tenant. Expire completed broadcasts on a short
    retention.
  - Poison letters: store `tenant_*`/`recipient_*` when the body decodes far enough to read them (even if it
    fails validation later), so `Erase` finds them; otherwise keep only a hash of the body and its size.
    Document that the NATS DLQ subject is retained by stream policy and is outside erasure, or shorten its
    `MaxAge`.
- **Tests:** integration test in `maintenance_integration_test.go`: create a pending inline broadcast and a
  poison letter naming recipient R, erase R, assert neither still contains R's id.

---

## Medium

### R-07

**gRPC server hardening.** · Reliability · Medium

- **Where:** [service.go:450-461](../../cmd/notifyd/service.go#L450).
- **Problem:** grpc-go does **not** recover handler panics: a nil dereference in any RPC kills the whole
  process (and with it the dispatcher in role `all`). There is no keepalive enforcement policy, no
  `MaxConcurrentStreams`, no `grpc.health.v1` service for load balancers, and no reflection toggle.
- **Fix:** add a recovery unary + stream interceptor (outermost, before the realm interceptor) that logs the
  stack via the error handler and returns `codes.Internal`; `grpc.KeepaliveEnforcementPolicy` and
  `grpc.KeepaliveParams`; `grpc.MaxConcurrentStreams`; register `health.NewServer()` and flip it to
  `NOT_SERVING` at shutdown start. Do not expose reflection by default.
- **Tests:** grpcserver unit test with a handler that panics: the client gets `Internal`, the server keeps
  serving.

### R-08

**HTTP surface has no request-level observability.** · Operability · Medium

- **Where:** [httpapi.go:123-156](../../adapters/driving/httpapi/httpapi.go#L123),
  [service.go:439](../../cmd/notifyd/service.go#L439).
- **Problem:** no access log, no request id/trace propagation, panics are logged by `net/http` in plain text
  to stderr (not JSON, no request context). `fail` maps `context.Canceled` to 503 but
  `context.DeadlineExceeded` falls through to the 500 path and is reported as an engine fault.
- **Fix:** a small middleware chain in `httpapi` (or in `cmd`): request id (accept `X-Request-Id`, generate
  otherwise, echo it), structured access log via `slog` (method, route pattern `r.Pattern`, status, duration,
  realm — **never** the query string, because the stream route carries a token there), JSON panic recovery.
  Set `http.Server.ErrorLog` to an `slog` bridge. Treat `DeadlineExceeded` like `Canceled` (or 504). Consider
  OpenTelemetry tracing (`otelhttp`/`otelgrpc` are already indirect deps) with trace ids in logs.
- **Tests:** httpapi tests: a request with no id gets one in the response; a panicking handler yields a JSON
  500 and the handler's error callback sees the stack; the access log does not contain `token=`.

### R-09

**SSE does not scale and is not bounded.** · Scalability/DoS · Medium

- **Where:** [redis/stream.go:115-134](../../adapters/driven/redis/stream.go#L115),
  [httpapi/stream.go:55](../../adapters/driving/httpapi/stream.go#L55).
- **Problem:** every stream calls `SSubscribe`, and a go-redis `PubSub` holds its **own connection**. 20k open
  tabs on one API pod are 20k Redis connections (Redis' default `maxclients` is 10k) plus 20k goroutine pairs.
  Nothing limits streams per identity or per process, so one authenticated user can open thousands.
- **Fix:**
  1. Short term: a per-process semaphore (`NOTIFY_MAX_STREAMS`) and a per-identity cap (e.g. 5) enforced in
     `httpapi.stream`; refuse with 503 + `Retry-After` / 429.
  2. Structural: a shared subscriber in the Redis adapter — one `PubSub` per process (per cluster node for
     sharded pub/sub) that `SSubscribe`s keys on demand with reference counting and fans messages out to local
     subscribers. `ports.StreamSubscriber` does not change.
- **Tests:** httpapi test: the (N+1)th stream for one identity is refused. Redis integration: two local
  subscribers to one identity share one server-side subscription (`PUBSUB SHARDNUMSUB` = 1).

### R-10

**Readiness depends on Redis.** · Availability · Medium · needs a decision

- **Where:** [service.go:418](../../cmd/notifyd/service.go#L418).
- **Problem:** the constitution says a fast path never holds correctness and losing Redis "costs speed, not
  correctness". But `/readyz` fails when Redis does not answer, so a Redis blip makes every API pod unready and
  the load balancer drops **all** traffic, including `Notify` and inbox reads that would have worked.
- **Fix:** keep PostgreSQL and partitions in readiness; report Redis as degraded (log + metric, optionally a
  `"degraded": ["redis"]` field with HTTP 200). The SSE route already answers 503 on its own when it cannot
  subscribe. Record the choice in a decision.
- **Test:** httpapi/service test: with the Redis check failing, `/readyz` is 200 and names Redis as degraded.

### R-11

**JWKS refresh blocks authentication.** · Availability · Medium

- **Where:** [auth/keys.go:172-192](../../adapters/driving/auth/keys.go#L172) and `refetchGen`
  ([keys.go:203](../../adapters/driving/auth/keys.go#L203)).
- **Problem:** when the TTL lapses, the first request performs the HTTP fetch (timeout 5 s) **while holding
  the realm's mutex**, so every concurrent request of that realm waits up to 5 s. The fetch uses the caller's
  request context: a client that disconnects cancels the fetch, which is recorded as a failed try and
  suppresses refetching for `minRefetch`.
- **Fix:** stale-while-revalidate. Serve the current set without blocking whenever one exists; refresh in a
  background goroutine (single-flight per realm) on `context.WithoutCancel` with its own timeout. Only the very
  first fetch (no set yet) blocks, and it should also be detached from the request context. Optionally
  pre-fetch every realm at startup so the first request never waits.
- **Tests:** keys unit test with a slow fake JWKS server: with a cached set, concurrent `Identify` calls return
  immediately while the refresh runs; a cancelled request context does not mark the refresh failed.

### R-12

**Provider reload rebuilds everything every minute.** · Performance · Medium

- **Where:** [providers.go:166-214](../../adapters/driven/channel/providers/providers.go#L166),
  [webhook.go:144-170](../../adapters/driven/channel/webhook/webhook.go#L144).
- **Problem:** each tick re-resolves every secret and calls every factory; `webhook.New` builds a new
  `http.Transport`. The old transports are dropped with their idle connections open (until
  `IdleConnTimeout`), and every send after a reload starts with a fresh TCP + TLS handshake. Secrets backed by a
  remote manager (future) would be fetched 1440×/day per row per pod.
- **Fix:** key each built channel by a fingerprint of `(row fields, config JSON, secret)` (hash the secret,
  never store it in clear beyond the channel); on reload, reuse the existing channel when the fingerprint is
  unchanged; for a replaced channel, call `CloseIdleConnections()` if it implements an optional
  `interface{ Close() error }`/`io.Closer`.
- **Tests:** providers unit test with a counting factory: two reloads over unchanged rows build once; a changed
  row rebuilds only that row.

### R-13

**Dispatcher batches are a barrier.** · Throughput · Medium

- **Where:** [dispatcher.go:207-222](../../app/dispatcher.go#L207) (`pass`) and
  [dispatcher.go:254-276](../../app/dispatcher.go#L254) (`Drain`, `wg.Wait()`).
- **Problem:** `pass` drains shards one after another, and `Drain` waits for **every** claim in the batch. One
  webhook that takes its full 10 s timeout holds the worker's whole pass — all 16 shards — for 10 s, while
  the other 99 goroutines are long finished. Concurrency per batch is unbounded (= `ClaimBatch`), all competing
  for a pgx pool whose default size is `max(4, NumCPU)`; pool waits count against the lease.
- **Fix:** keep `Drain` (it is a port method and tests rely on it) but change `Run` to a bounded worker pool:
  `NOTIFY_DISPATCH_CONCURRENCY` workers fed by a claimer that claims `min(ClaimBatch, free slots)` per shard,
  round-robin, as slots free up. Document the relation between concurrency and `pool_max_conns` in the DSN.
  The lease reasoning in the `Dispatcher` comment still holds (each claim's lease runs from its own claim).
- **Tests:** app unit test with one channel that blocks and many that return at once: deliveries from other
  shards are handled while the slow one is in flight. Keep `DispatcherContract` green.

### R-14

**Quota is spent by attempts that send nothing.** · Correctness · Medium · needs a decision

- **Where:** [dispatcher.go:330](../../app/dispatcher.go#L330) takes the unit before render
  ([:356](../../app/dispatcher.go#L356)) and `Deliver` ([:379](../../app/dispatcher.go#L379)).
- **Problem:** [quota.go:21-23](../../app/quota.go#L21) says "a delivery that is deferred or retried is not
  charged for the attempts that sent nothing". Deferral holds, but a render failure, a channel `Retry`, a
  transport error or a panic each consume a unit and then retry, so a delivery that fails 5 times spends 5 units
  of the tenant's budget while sending zero messages. A provider outage drains every tenant's daily budget.
- **Fix options (choose in D5/D24, then test):** (a) take the unit just before `Deliver` (after render), which
  removes render failures; and (b) add `QuotaCounter.Refund(ctx, k)` (a `DECR` of both windows, floored at 0,
  via a script) called when the outcome is `Retry` or an error. Or (c) amend the comment and the decision to
  say retries are charged, if that is the intended behaviour.
- **Tests:** app unit test: a channel returning `Retry` three times leaves the budget unchanged (with the
  in-memory quota fake); `QuotaContract` gains the refund case.

### R-15

**Broadcast expansion is slow and not fair across tenants.** · Throughput · Medium

- **Where:** [app/broadcast.go:183-202](../../app/broadcast.go#L183),
  [postgres/broadcast.go:225-247](../../adapters/driven/postgres/broadcast.go#L225),
  [postgres/broadcast.go:39-52](../../adapters/driven/postgres/broadcast.go#L39).
- **Problem:** per recipient, `Planner.Plan` does a topic lookup (same topic for the whole broadcast), a
  preference resolve and a schedule read (2 queries), and a quota peek per channel (Redis); then
  `CommitBroadcastPage` runs `persist` per item (~5 statements). A 500-recipient page is ~4,000 sequential round
  trips; a 1M-recipient audience takes hours per worker. `LeaseBroadcast` takes the **oldest** pending
  broadcast, so with one worker a huge broadcast from tenant A starves every later broadcast from B.
- **Fix:**
  1. Look the topic up once per page (pass the `TopicDef` into planning, or use a page-scoped cache like the
     dispatcher's `topicCache`).
  2. Add batch variants on `PreferenceStore` (`ResolveMany`, `ScheduleMany`) — one query per page with
     `= ANY($n)` — and peek quota once per `(tenant, channel)` per page: the tenant is the same for every item.
  3. Pipeline `persist` with `pgx.Batch` (scope, guard, inbox, queue per item in one round trip each batch), or
     a set-based insert for the page.
  4. Fairness: order the lease by `last_page_at NULLS FIRST, created_at` (a new column, set on commit) so pages
     of concurrent broadcasts interleave. Fairness changes behaviour → note it in a decision.
- **Tests:** broadcast integration benchmark (`-tags integration`, like `queue_bench_integration_test.go`)
  before/after; unit test that two pending broadcasts alternate pages.

### R-16

**NATS ingest is serial, and its DLQ shares the ingest stream.** · Throughput/Correctness · Medium

- **Where:** [natsingest.go:259-260](../../adapters/driving/natsingest/natsingest.go#L259),
  [service.go:594-605](../../cmd/notifyd/service.go#L594).
- **Problem:** `Consume` invokes the handler for one message at a time, and each `Notify` is several DB round
  trips, so one realm ingests roughly 100–200 msg/s per process regardless of load. The DLQ publishes to
  `notify.ingest.dlq.<realm>`, which matches the stream's `notify.ingest.>` subjects, so poison copies are
  stored in the ingest stream itself; a realm literally named `dlq` would have its consumer filter
  `notify.ingest.dlq.*` match the DLQ subjects, and `parseSubject` would then `Term` them.
- **Fix:** process with a bounded pool (`Consume` with `PullMaxMessages(n)` and hand messages to N workers, or
  `Messages()`/`Fetch` + worker pool; ack per message — order is not a contract because each message is
  idempotent). Move the DLQ to its own subject root and stream (`notify.dlq.<realm>`, separate retention), or
  exclude it via the stream's subjects. Refuse the realm name `dlq` (and any other reserved token) in
  `domain.Realm.Validate` or in `ParseRealmBindings`.
- **Tests:** natsingest integration: N messages are handled concurrently (a notifier that blocks shows >1 in
  flight); a DLQ message never reaches a realm consumer.

### R-17

**There is no way to apply a migration to a live database.** · Operations · Medium · needs a decision

- **Where:** [deploy/Dockerfile](../../deploy/Dockerfile) ("applied by the database's first boot"),
  [deploy/initdb/10-schema.sh](../../deploy/initdb/10-schema.sh), [migrations/](../../migrations/).
- **Problem:** migrations are applied once by `initdb` on an empty volume, with no version table. After the
  first `v*` tag migrations become immutable and every change is a new file (CLAUDE.md), but no tool would
  apply `0004_*.sql` to an existing database, and nothing tells a binary that the schema is behind it.
- **Fix (before T057 tags `v0.1.0`):** a small embedded runner over `migrations.FS`: a
  `notify_schema_migrations(version, name, checksum, applied_at)` table, `pg_advisory_lock` around the run (the
  runner connects directly, not through a transaction pooler), each file in its own transaction (the files
  already `BEGIN/COMMIT`), checksum verification of applied files. Expose it as `notifyd migrate` (a
  subcommand or `NOTIFY_ROLE=migrate`) run as the owner/admin role, and make `Build` refuse to start when the
  database's version is not the binary's. A library host calls the same runner.
- **Tests:** integration: apply all, re-apply is a no-op, a tampered checksum is refused, two concurrent
  runners apply once. `verify-schema` keeps working.

### R-18

**The runtime role can switch tenant isolation off.** · Security · Medium · needs a decision (D17)

- **Where:** [service.go:228-243](../../cmd/notifyd/service.go#L228) (`checkRole` refuses superuser and
  `BYPASSRLS` only); [security.md](../security.md) ("the owner is the role a worker connects as").
- **Problem:** `FORCE ROW LEVEL SECURITY` protects against application bugs, but the table owner may
  `ALTER TABLE notifications NO FORCE ROW LEVEL SECURITY`, `DROP POLICY`, or `DROP TABLE`. Any SQL-injection or
  compromised-process scenario therefore defeats isolation outright. The owner is needed only for DDL
  (partition provisioning/retirement, migrations).
- **Fix:** two roles. `notify_owner` owns the schema and is used by migrations and the maintenance jobs that run
  DDL (`EnsurePartitions`, `RetirePartitions`); `notify_app` has `SELECT/INSERT/UPDATE/DELETE` (granted via
  default privileges, including on future partitions) and is used by every request and dispatcher path. With a
  non-owner role, plain `ENABLE` RLS already applies; keep `FORCE` for the owner. In `cmd/notifyd`, accept an
  optional `NOTIFY_PG_OWNER_DSN` for the maintenance pool; `checkRole` additionally warns when the app role owns
  the tables. Update `verify-schema` to prove the app role cannot `ALTER` the tables.
- **Tests:** verify-schema assertion; `service_integration_test` builds with two roles.

### R-19

**Hot-path lookups are uncached.** · Performance · Medium

- **Where:** topic lookup on every `Notify` ([planner.go:92](../../app/planner.go#L92)), every `Broadcast`
  ([notifier.go:151](../../app/notifier.go#L151)), once per dispatcher batch, and **again** inside the email
  channel per delivery ([email.go:207](../../adapters/driven/channel/email/email.go#L207)); template rows are
  queried on every render ([postgres/templates.go:49](../../adapters/driven/postgres/templates.go#L49)) and
  re-parsed on every execution ([domain/template.go:304](../../domain/template.go#L304)).
- **Fix:**
  1. A decorator `ports.TopicRegistry` with a short TTL (e.g. 10–30 s, bounded size) built in `cmd`;
     `Catalog.UpsertTopic` invalidates locally (other pods converge within the TTL, which the catalog docs
     must state). Keep the cache out of `domain`/`app` invariants: a stale topic must never break correctness
     (it only affects which channels a new notification plans).
  2. Pass the dispatcher's already-resolved `TopicDef` (or `Essential`) on `domain.Delivery` so the email
     channel does not look the topic up again.
  3. Cache parsed `*template.Template` by `(realm, tenant, ref, channel, locale, version)` — versions are
     immutable once stored, so the cache needs no invalidation, only a size bound. Cache the active-version
     lookup with a short TTL, invalidated by `ActivateTemplate`.
- **Tests:** decorator unit tests (hit, expiry, invalidation); a benchmark for `Render` before/after.

### R-20

**Tenant preference upload is not atomic.** · Correctness · Medium

- **Where:** [httpapi/operator.go:101-130](../../adapters/driving/httpapi/operator.go#L101).
- **Problem:** rows are applied one by one; a refusal at row *i* returns 422 naming the row, but rows `0..i-1`
  are already stored. The operator's retry after fixing row *i* re-applies them (harmless), but a request that
  is abandoned leaves a half-applied policy (e.g. half of the "lock email for everyone" rows).
- **Fix:** validate every row first (topic known, channel known, essential rule) and only then write; better, add
  `Admin.SetTenantPreferences(ctx, realm, []TenantPreference)` with a store method that writes all rows in one
  transaction. Mirror the change on the gRPC catalog if it has the same shape.
- **Tests:** httpapi test: a body whose third row is refused stores nothing.

### R-22

**Config validation gaps that can destroy data.** · Medium. [config.go:119-180](../../config.go#L119): negative
`RetentionWindow`, `DeliveryLogRetention`, `DeadLetterRetention`, `BackoffBase` and `PreCheckTTL` pass
`orDefault` (only zero takes the default) and `Validate` (no sign check). The retention job
([app/maintenance.go:305](../../app/maintenance.go#L305)) retires partitions before `now.Add(-window)`; with
`NOTIFY_RETENTION_WINDOW=-90d` that cutoff is in the future, the SQL caps it at `now()`, and **every past month
of the inbox, the delivery history and the dead letters is dropped** on the next pass. A positive but tiny
window (`90h` typed for 90 days) does nearly the same. Fix: "must be positive" for every duration, and a
floor for the three retention windows (at least `IdempotencyWindow`, so `Status` and replays never point at a
retired partition, and at least a month is a sane minimum for the inbox). A `BackoffCeiling` above
`IdempotencyWindow` is also worth refusing. Tests: `config_test.go` rows for each. See R-05 and R-16 for the
service-level checks.

### R-23

**No vulnerability scanning, security lint or fuzzing.** · Security/Quality · Medium

- **Where:** [.github/workflows/ci.yml](../../.github/workflows/ci.yml), [.golangci.yml](../../.golangci.yml).
- **Problem:** the module hand-writes security-critical parsers — JWT and JWKS (`domain/token.go`), unsubscribe
  tokens (`domain/unsubscribe.go`), the template checker, inbox cursors (`postgres/inbox.go` `decodeCursor`),
  Svix verification (`email/resendhook.go`), bus payload decoding (`natsingest.decode`) — and none has a fuzz
  test. CI does not run `govulncheck`, and `gosec`/`noctx`/`sqlclosecheck`/`rowserrcheck` are not enabled.
- **Fix:** add `govulncheck ./...` as a CI job (and `make vuln`); enable `gosec` (with justified excludes),
  `noctx`, `rowserrcheck`, `sqlclosecheck`, `gocritic` (diagnostic tags) in `.golangci.yml`; add `FuzzXxx`
  targets with seed corpora for each parser above (properties: never panics; a token that verifies re-encodes to
  the same claims; a cursor that decodes was issued by `encodeCursor`), run briefly in CI
  (`go test -fuzz=. -fuzztime=30s` per target, or a nightly job). Consider coverage reporting for unit tests.

---

## Low

### R-21

**LISTEN never reconnects.** [dispatcher.go:189-193](../../app/dispatcher.go#L189) sets `signals = nil` when
the listener closes and polls alone for the life of the process. Polling stays correct, but latency silently
regresses to `PollInterval` after any connection blip. Fix: re-`Listen` with capped exponential backoff from
`Run`, reporting each loss once. Test with a `WakeSource` fake that closes and then works again.

### R-24

**SMTP without credentials in service mode.** [service.go:300-313](../../cmd/notifyd/service.go#L300): the
`smtp` factory never reads `sp.Secret`, so a relay that requires AUTH (most hosted relays) cannot be used. Add
`username` in `config` and the password as the row's secret, wired to `email.WithSMTPAuth(smtp.PlainAuth(...))`
— and only with TLS (`require_tls`), since `PlainAuth` refuses plaintext to non-localhost anyway.

### R-25

**Webhook connection reuse.** [webhook.go:165](../../adapters/driven/channel/webhook/webhook.go#L165):
`MaxIdleConnsPerHost: 4` while a dispatcher batch may send up to `ClaimBatch` deliveries to one endpoint
concurrently, so most requests open a new TLS connection. Raise it (e.g. 32–64) and set `MaxConnsPerHost` to
protect receivers. Shared with R-12 (do not rebuild the transport on every reload).

### R-26

**Poison parking is not atomic.** [poison.go:24-27](../../adapters/driven/postgres/poison.go#L24):
`INSERT … WHERE NOT EXISTS` is a check-then-insert under READ COMMITTED; two concurrent parkings of one message
can both insert (no unique index across `created_at` partitions). Rare (one consumer per realm), but the port
promises idempotency. Use `pg_advisory_xact_lock(hashtext(id))` in the transaction before the insert, or a small
non-partitioned `poison_keys` guard table with a primary key.

### R-27

**Digest member loads probe every partition.** [load.go:36-41](../../adapters/driven/postgres/load.go#L36)
joins `notifications` on `id` only; the window stores ids without `created_at`, so the read cannot prune. Store
`(id, created_at)` pairs (or a parallel `member_created_at timestamptz[]`) in `digest_buffer` and join on both.
Schema change: still allowed in place before the first tag.

### R-28

**`NotifyTx` and REPEATABLE READ.** [persist.go:184-203](../../adapters/driven/postgres/persist.go#L184): in a
caller's REPEATABLE READ/SERIALIZABLE transaction, a guard insert that conflicts with a concurrently committed
row cannot read that row (snapshot), so the loop ends in "the idempotency guard kept changing" instead of a
replay. Document that `NotifyTx` requires READ COMMITTED (or map the case to a clear sentinel error) in
[integration-guide.md](../integration-guide.md).

### R-29

**Duplication and idiom nits** (one `refactor:` PR):

- `systemClock` is declared five times (`cmd/notifyd`, `app`, `postgres`, `redis`, `webhook`). Adapters cannot
  import `app`, so the copies exist for a reason. Either accept them (they are one line each) or export one
  `ports.SystemClock` that every layer may use, since all of them already import `ports`. Pick one and apply
  it everywhere.
- The UUID regex exists twice: [inbox.go:183](../../adapters/driven/postgres/inbox.go#L183) `uuidPattern` and
  [deadletters.go:66](../../adapters/driven/postgres/deadletters.go#L66) `uuidText`. Keep one.
- `containsNUL` in [poison.go:60](../../adapters/driven/postgres/poison.go#L60) is `bytes.IndexByte(b, 0) >= 0`.
- [service.go](../../cmd/notifyd/service.go) builds `secrets.New()` twice and declares the `fault` closure
  twice; build each once in `Build`.
- `ids.go` hand-rolls UUIDv7; `github.com/google/uuid` (already in the module graph) has `NewV7`. Keep the
  hand-rolled one only if the injectable clock matters (it does for tests) — then say so in its comment.
- `RoutedNotifier`, `Recipients`, `Operators` live in `auth` but `RoutedNotifier` is routing, not
  authentication; consider `adapters/driving/route`.

### R-30

**Idempotency key reuse with different content.** [persist.go:192-201](../../adapters/driven/postgres/persist.go#L192)
returns the first notification's receipt for any later call with the same key, whatever its content. That is
correct for retries, but a producer bug that reuses keys (e.g. a constant key) silently drops every later
notification. Common practice (Stripe et al.) stores a fingerprint of the request and refuses a mismatch
(409/`ErrIdempotencyConflict`). Decide; if adopted, add `request_hash bytea` to `notify_idem` and compare on
conflict.

### R-31

**Priority does not affect claim order.** [queue.go:38-56](../../adapters/driven/postgres/queue.go#L38) orders
by `next_attempt_at` only, so a `critical` notification queues behind a tenant's info backlog in its shard.
Options: a priority column in the claim index (`(shard, priority_rank, next_attempt_at)`), or claiming
critical first in a separate statement. This changes D19's index and HOT reasoning — measure with
`queue_bench_integration_test.go` before deciding.

### R-32

**Log level and version.** [main.go:17](../../cmd/notifyd/main.go#L17) always logs at Info with no
`NOTIFY_LOG_LEVEL`; the binary has no version (`-ldflags -X main.version=…` in the Dockerfile, or
`debug.ReadBuildInfo`), which operators need in the startup log and in a metric (`notify_build_info`).

---

## Also noted (no action required now)

- `NotifyBatch` handles up to 500 items serially, one transaction each
  ([grpcserver/server.go:91](../../adapters/driving/grpcserver/server.go#L91)). Acceptable; if producers
  batch heavily, a bounded parallel loop is a cheap win.
- Resend webhooks are verified within a 5-minute tolerance but not de-duplicated by `svix-id`; both effects
  (status record, suppression upsert) are idempotent, so a replay is harmless.
- `RehomeShards` updates every out-of-range row in one transaction; for a large backlog, batch it.
- The SSE route accepts the token in the query string because `EventSource` cannot set headers; make sure the
  ingress and any access log redact `token=` (see R-08).
