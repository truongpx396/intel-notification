# Roadmap

Written to be honest about what this is and is not, because a notification system that overstates
its scope gets adopted for jobs it cannot do — and notification bugs are the ones users notice.

## What this is

**A multi-channel delivery engine** for one shape: a product that must tell specific recipients
about specific events, **exactly once**, over **whichever channels each recipient chose**, and be
able to prove it never told the wrong person. That is the shape of every multi-tenant SaaS inbox,
and the correctness bar is higher than it looks — the failure modes are a lost dunning email, a
duplicated alert at 3am, and a notification leaking across a tenant boundary.

Concretely, in scope and designed:

- Durable recipient-scoped inbox with a bounded unread count and seen, read and archived states.
- Transactional outbox fan-out — a crash cannot lose a channel delivery — with `NotifyTx` to enqueue
  inside the producer's own transaction.
- A pluggable `Channel` registry: in-app and email as reference implementations; SMS, push, Slack and
  webhook by registering one more. Every device of a recipient is its own delivery.
- Preferences per `(recipient, topic, channel)`, with tenant defaults and locks over registered topic
  defaults.
- Quiet hours and bounded digest coalescing, including what happens to a `critical` notification at 2am.
- Scheduled sends, expiry, cancellation, fallback channel chains and provider failover.
- A delivery history that answers "was it delivered?" and correlates provider bounces.
- Dead-letter path for genuine failures, with attempt caps, alarms, inspection and replay.
- Per-tenant per-channel quotas that move an exhausted tenant's backlog aside, so one noisy tenant cannot
  starve another.
- Bounded growth for every table, and erasure of a recipient's personal data in one call.
- Both deployment shapes: embedded Go library, or a service any language can call — with topics,
  templates and addresses managed as data.

## What this is not

It is **not** a campaign or marketing automation tool — no audience segmentation over behavioural
data, no A/B testing of copy, no send-time optimization. It is **not** an on-call escalation
system today: ack-tracking and escalation chains are [Phase 2](#phase-2--escalation--delivery-workflows),
designed and not built. It is **not** a chat or messaging product — it delivers notifications *to*
channels, it does not model conversations.

**If you do not need multi-channel fan-out with recipient-scoped durability, you probably want
something else**, and saying so is more useful than pretending otherwise:

| Instead | When |
|---|---|
| **Your framework's mailer** | You send transactional email only, one channel, no inbox, no preferences. Most products start and stay here, correctly |
| **Knock / Courier / Novu** | You want multi-channel notifications as a managed product, with a template editor non-engineers can use. The right default for most teams |
| **Novu** (self-hosted) | Same, open source, running code today rather than a specification |
| **PagerDuty / Opsgenie** | Your problem is on-call escalation and incident response, not product notifications |
| **Customer.io / Braze** | Your problem is marketing lifecycle campaigns, not transactional delivery |
| **this** | You need an inbox you own, opaque recipients, a hard tenant-isolation proof, exactly-once delivery, and channels you can add without touching fan-out |

The honest summary: pick a managed product unless recipient-scoping is a compliance requirement or
owning the delivery core matters to you. This engine's advantage is that isolation and exactly-once
are properties of the schema, checkable by a test, rather than a vendor's assurance.

---

## Phase 1 — Notification core

**Status: designed and normative; implementation started.** Built so far: the schema, the domain
rules, and the PostgreSQL adapter for the delivery queue, digests and maintenance jobs, with the CI
boundary gates. Not built: the notifier, dispatcher, channels and transports. Task-level status is in
[tasks.md](specs/001-notification-core/tasks.md).
→ [specs/001-notification-core](specs/001-notification-core/)

The durable inbox, preferences, the transactional outbox, the dispatcher, the channel registry, the
template seam, digest coalescing, quotas and fairness, the dead-letter path, retention, erasure, and
both transports (in-process library and a gRPC service with data-backed topics, templates and
addresses).

**Required infrastructure: PostgreSQL + Redis.** No message broker: a notification is accepted when
PostgreSQL commits, workers claim from the queue table, and scheduled jobs are single-owner through row
leases. NATS JetStream is supported as an optional ingest path for producers that prefer to publish.
The schema's guarantees are verified against PostgreSQL 16 by `make verify-schema`.

The design went through an architecture review after the extraction, recorded as decisions D16–D37.
It fixed a cross-tenant leak on the live stream, a worker that could not read what it delivered under
row-level security, a pre-check that could drop notifications, and a delivery mode that reintroduced
the bug the outbox exists to fix — and added what a general-purpose engine is expected to have. The
capacity envelope for one PostgreSQL primary is stated in
[plan.md](specs/001-notification-core/plan.md#capacity-model) and gates a production-ready release.

## Phase 2 — Escalation & delivery workflows

**Status: designed, not started. Depends on Phase 1.**
→ [specs/002-escalation-workflows](specs/002-escalation-workflows/)

The gap between "we delivered it" and "someone actually dealt with it". Adds delivery
**acknowledgement** as first-class state, **escalation chains** (unacknowledged after N minutes →
next channel, then next recipient), **multi-step workflows** with waits and conditions, and
**delivery-outcome webhooks** so a host can react to a notification going unread.

This is the phase that turns a delivery engine into a notification *platform*, and it is the largest
gap between what a buyer assumes and what Phase 1 provides. It is separated deliberately: everything
in Phase 1 is stateless per notification, while acknowledgement introduces a lifecycle, and mixing
the two would put workflow state in the hot delivery path.

## Phase 3 — Authoring & self-service

**Status: not designed.** Listed so its absence is a decision rather than an oversight.

A template editor non-engineers can use, per-tenant branding UI, localization workflow, preview and
test-send, and delivery analytics. This is most of what a managed vendor actually sells, and it is
the strongest argument for buying rather than building. If you need it, revisit the table above.

---

## Non-goals, permanently

- **Deciding what triggers a notification.** Producers build notifications; this engine does not
  instrument callers or watch a database for changes.
- **Copy, localization and branding as engine concerns.** They live behind `TemplateRenderer`.
- **Channel provider integrations in the core.** Resend, SES, Twilio, APNs and Slack live in
  `Channel` adapters; the core sees one interface and a result.
- **Interpreting tenancy.** What a `Tenant` or `Recipient` *means* is the host's; the engine treats
  both as opaque identities and will not grow a notion of "user".
