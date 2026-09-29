# Provenance

`intel-notification` was extracted from
[**truongpx396/aisat-intel**](https://github.com/truongpx396/aisat-intel), where this engine was
designed as that product's notification backbone and deliberately factored for reuse. This file
records exactly what moved, what was generalized, and what was fixed — so any claim here can be
checked against the upstream repository.

Extracted at upstream `main` = `2f983a03f03b5232c66930aa8cb64b3e75020fb7` on 2026-09-28.

It is the third extraction from that project, after
[intel-agent](https://github.com/truongpx396/intel-agent) and
[intel-payment](https://github.com/truongpx396/intel-payment), and it follows the same method:
carry the dedicated artifacts with their history, lift the interleaved content and record it here,
then resolve what the originating design left open.

---

## Files carried across **with their git history**

`git filter-repo` preserved the real commit history for every notification artifact. `git log
--follow <path>` in this repository shows each one's evolution in the originating project.

**Three files were dedicated to notifications and came across whole:**

| This repository | Upstream path | Commits |
|---|---|---|
| `specs/001-notification-core/contracts/notification-ports.md` | `specs/001-contextengine-mvp/contracts/notification-ports.md` | 2 |
| `specs/001-notification-core/diagrams/notification-flow.excalidraw` | `specs/001-contextengine-mvp/diagrams/addition/notification-flow.excalidraw` | 2 |
| `design-system/pages/notifications.md` | `design-system/aisat-studio/` → `aisat-intel/pages/notifications.md` | 3 |

**Six were shared spec files covering the whole product**, where notifications are one user story
among eight. Carrying them whole would drag every commit about retrieval, ingestion, agents and
billing into a notification repository. Instead each historical revision was **sliced to its
notification content** during the rewrite, so the commit lineage is genuinely the original file's
while the content stays scoped:

| This repository | Upstream path | Commits | Slice |
|---|---|---|---|
| `specs/001-notification-core/research.md` | `specs/001-contextengine-mvp/research.md` | 12 | §23 outbox · §18 DLQ · §15 scheduled work · §14 seams · §10 Redis roles · §4 fallback |
| `specs/001-notification-core/plan.md` | `specs/001-contextengine-mvp/plan.md` | 23 | technical context, worker/relay tiers, source layout |
| `specs/001-notification-core/tasks.md` | `specs/001-contextengine-mvp/tasks.md` | 22 | Stage 10 (US8) plus the notification clauses of T011 / T019 / T020 / T024 |
| `specs/001-notification-core/data-model.md` | `specs/001-contextengine-mvp/data-model.md` | 7 | section K — the notification tables, RLS and retention rules |
| `specs/001-notification-core/spec.md` | `specs/001-contextengine-mvp/spec.md` | 4 | User Story 8, FR-032–FR-039, SC-011–SC-013, notification edge cases |
| `specs/001-notification-core/quickstart.md` | `specs/001-contextengine-mvp/quickstart.md` | 2 | the notification verification steps |

Selection was by **keyword match, not fixed section numbers**, because section numbering moves across
193 upstream commits — matching on content means the slice tracks each file as it evolved rather than
going blank the moment a heading was renumbered. Commits that touched none of the notification content
became empty and were pruned, which is why **59 commits reach this repository's root** out of
193 upstream. The slicer is kept at `scripts/notifslice.py` so the rewrite is reproducible.

The design page was carried through **both** of its upstream paths, because it was renamed when the
product was renamed (`aisat-studio` → `aisat-intel`). In this repository both map to the same file, so
that rename commit correctly collapses to a no-op and is pruned — the content history either side of
it is intact.

The tip of each shared file is **not** its slice: a generalization commit replaces
`spec.md`, `plan.md`, `tasks.md`, `data-model.md` and `quickstart.md` with versions rewritten for this
repository, so the slice is what the *history* shows and the rewrite is what you read. `research.md`
keeps its sliced content, generalized in place, because this repository has no independently authored
equivalent — the research is the originating project's and is credited as such.

### Upstream commits, oldest first

| Upstream SHA | Subject |
|---|---|
| `c95e340` | update notification feature |
| `906bafe` | update notification design |
| `cc02bc4` | feat(design): stage Phase 2 surfaces in mockups; resolve all open design decisions |
| `803f997` | docs(spec): add reusable notification ports + transactional-outbox fix |
| `22e98c7` | docs(spec): add human-in-the-loop approval port + Phase-1 agent action tools |

SHAs are rewritten by the filter; `git log` here shows the new ones with the same messages.

A note on one absence: `git log --follow` on the upstream diagram also lists `3e7bc36`, `0c5abe3`
and `23b63bc`. Those are **not** notification commits — `--follow` misattributed them from
`ingestion-pipeline.excalidraw`, because Excalidraw's JSON is similar enough between unrelated
diagrams to trip git's rename-detection heuristic. They are correctly excluded.

---

## Content derived without history

Two upstream sources contributed content whose history was **not** carried, because the notification
material in them is a handful of lines inside a much larger document:

| This repository | Upstream source | What was taken |
|---|---|---|
| `specs/001-notification-core/contracts/bus-subjects.md` | `specs/001-contextengine-mvp/contracts/nats-subjects.md` | `notify.<tenant>`, `notify.email.<tenant>`, the DLQ subject and `notify.retention.tick`, and their rules |
| `specs/001-notification-core/contracts/rest-api.md` | `specs/001-contextengine-mvp/contracts/bff-rest.md` | The endpoint set, scoping rules and error semantics |

`README.md`'s architecture prose also restates the outbox explanation and the `SET NX` hazard analysis
from the originating `README.md`.

Everything else in this repository — `ROADMAP.md`, `design-decisions.md`, the migrations, the
`docs/` set, `specs/002-escalation-workflows/`, and the tooling — is new work with no upstream
ancestor, and is a single commit for that reason.

## Generalization applied to the carried files

The carried contract was written as *a seam inside one product*. It now describes *the product*, so
the framing changed while the substance did not:

- **Host-specific vocabulary removed** — `(workspace_id, user_id)` became the opaque
  `(realm, tenant_kind/tenant_id, recipient_kind/recipient_id)` tuple; the 13-value `category` enum
  became a registered `Topic`; "ContextEngine" became "one implementation"; in-app and email were
  demoted from *the* channels to *two reference* channels alongside SMS, push, Slack and webhook.
- **`email_suppressions` became `channel_suppressions`** — a dead device token and a revoked Slack
  webhook are the same event shape as a hard bounce, so suppression is keyed by channel.
- **Cross-references repointed** — links into the originating spec (its research sections, its
  `FR-0NN`/`SC-0NN` ids, sibling contracts that did not come across) were replaced with this
  repository's own ids, or with the reasoning they stood in for. No dangling reference remains.
- **Import paths rewritten** — the upstream kernel path became
  `github.com/truongpx396/intel-notification/notify`, and the in-repo tree became this
  repository's root layout.
- **"Extraction-ready" became "extracted"** — the *"move it and it still compiles"* litmus test now
  runs in the other direction, as a CI gate asserting the engine stands alone.
- **Phase language removed** — "Phase 1 ships in-app + email, Phase 2 adds …" was the originating
  product's roadmap. This repository has its own.

## Design work added during the lift

15 decisions were recorded during the lift in
[design-decisions.md](specs/001-notification-core/design-decisions.md) (D16–D36 came later — see
[the review](#architecture-review-after-the-extraction)). The ones that change behaviour rather than
presentation:

| # | Change | Why it mattered |
|---|---|---|
| **D1** | `Realm` as the outermost isolation axis | Without it, two products sharing a deployment share an **idempotency key space**, so one's `invite:42:received` is silently swallowed as a replay of the other's |
| **D2** | Idempotency guard moved to its own non-partitioned table | **The inherited schema was not constructible.** See below |
| **D3** | Preferences as one row per `(topic, channel)` | The inherited `in_app BOOL, email BOOL` pair makes every new channel a migration — which defeats the pluggable `Channel` registry the same design introduced |
| **D4** | `digest_buffer` — state for a coalescing window | Invariant 8 requires storm coalescing and `DeliverySchedule.Digest` names the window, but there was nowhere to **hold** a deferred notification. The invariant was unimplementable as specified |
| **D5** | `channel_quotas` — per-tenant channel budget | The design named noisy-neighbour provider starvation as a prerequisite for shared-service mode, then defined no state for it |
| **D6** | No foreign key from the outbox into the partitioned inbox | A FK would make a retention partition `DROP` fail while any outbox row still referenced it — turning routine retention into an outage |
| **D7** | The `AudienceResolver` port | `Broadcast` takes an `Audience string` and is required to expand it, but no port existed to expand it *with*, so the one operation that fans out had no seam |
| **D9** | Quiet hours yield to `critical` | Quiet hours and a critical alert are in direct conflict and the inherited design ranked neither. Silently deferring a `credit_exhausted` notice until 07:00 is a worse outcome than waking someone |
| **D10** | Backoff schedule with jitter, specified | `MaxAttempts: 5` was given without an interval. Five immediate retries is not a retry policy, and synchronized retries across shards are how a provider outage becomes a thundering herd |
| **D11** | `Shard` derived from a stable hash of the recipient | The type existed with no derivation rule. Sharding by anything non-stable double-delivers the moment the shard count changes |

### D2 — a constraint that could not exist

The inherited design specified the inbox as **both** `PARTITION BY RANGE (created_at)` *and*
carrying a global **`UNIQUE (user_id, idem_key)`**. PostgreSQL forbids that combination, and that
one index is the backstop for every exactly-once guarantee in the system (SC-013):

```
ERROR:  unique constraint on partitioned table must include all partitioning columns
DETAIL:  UNIQUE constraint on table "inherited_notifications" lacks column "created_at"
         which is part of the partition key.
```

Found by applying the migrations rather than by reading them — `make verify-schema` reproduces both
the failure and the fix. Resolved with a dedicated non-partitioned
`notify_idem (realm, recipient_kind, recipient_id, idem_key)` guard written in the same transaction
as the inbox row. That keeps both properties and is better than either compromise, because the guard
is narrow and hot, and it **outlives inbox partitions** — so dropping an aged partition cannot
resurrect the ability to double-notify an old key.

*Since amended by the review:* the guard's key now includes the tenant, and it is bounded by an
idempotency window rather than kept forever ([D18](specs/001-notification-core/design-decisions.md#d18),
[D21](specs/001-notification-core/design-decisions.md#d21)).

This is the same class of defect, in the same place, that
[intel-payment](https://github.com/truongpx396/intel-payment) found in `credit_ledger`. Two
independent subsystems of the originating design inherited one unconstructible pattern, which is
itself the finding: the pattern was copied between specifications without either being applied.

---

## Architecture review after the extraction

The design as first written here was reviewed against the bar of a production-grade, general-purpose
engine. The review found defects in the carried contract and in this repository's own additions, and
the fixes are recorded as decisions **D16–D36**, plus amendments to D2, D4, D5, D7, D8, D11, D12, D13
and D15 in [design-decisions.md](specs/001-notification-core/design-decisions.md). The ones that were
correctness bugs:

| # | Defect | Consequence |
|---|---|---|
| **D16** | The reference in-app channel published rendered content to `notify:user:<id>` — no realm, no tenant | Two tenants with a user `u1` received each other's notifications on the live stream |
| **D17** | Queue rows carried no recipient; RLS was on the partitioned parent only | Under `FORCE` RLS the dispatcher's read returned nothing (verified against PostgreSQL 16); a partition queried by name bypassed the policy |
| **D18** | The pre-check was `SET NX` before the transaction, keyed without the recipient; the guard omitted the tenant | A failed transaction or a second recipient silently dropped a notification; one user in two tenants lost one tenant's notification |
| **D22** | A `direct` delivery mode inserted then delivered in-line | A crash between the two lost the undelivered channels — the originating bug, reintroduced |
| **D11** | Shards were fixed for life on a misdiagnosed double-delivery risk, while the claim itself was unspecified | An offline migration for a safe operation, and the real double-claim unguarded |
| **D2, D20** | The guard and the outbox were never pruned | Two tables grew forever, against NR-022 |

The review also added the claim protocol (D19), delivery history (D20), tenant fairness (D24),
multi-address delivery (D25), templates that own copy (D27), `NotifyTx` (D28), lifecycle controls
(D29), tenant preferences (D30), a service mode that needs no host code (D31), per-channel dedup
guarantees (D32), RFC 8058 unsubscribe (D34), erasure (D35) and a stated capacity envelope (D36). The
migrations were rewritten in place rather than amended, because nothing had been deployed; the schema
suite grew from seven printed checks to tests that raise on failure and run as the table owner.

The queue's state transitions were first written as PL/pgSQL functions, then moved into the Go adapter
as plain SQL with the domain owning the dead-letter and fallback policy (D37), together with the first
Go code — the domain rules and the PostgreSQL queue adapter on Go 1.26 — and an integration suite that
runs every transition against a real PostgreSQL through Testcontainers.

---

## What this repository does **not** take

Deliberately left in the originating project, because they are that product's concerns:

- Retrieval, ingestion, the LLM gateway, the agent runtime, sandboxing.
- Metering, credits and payments — extracted separately to
  [intel-payment](https://github.com/truongpx396/intel-payment).
- Authorization, audit and approval ports — sibling reuse seams for other subsystems.
- The design system beyond the notifications screen.
- The originating spec's own requirements, success criteria and roadmap.
- The generated HTML mockup of the notifications screen (`.stitch/designs/notifications.html`): it
  is an artifact of that product's design-system tooling, while
  `design-system/pages/notifications.md` is the normative UI description and did come across.

## Relationship going forward

The two repositories are independent. `aisat-intel` points here as the canonical home for the
notification design and keeps a short binding note describing how it adopts these ports; this
repository does not depend on it.

Where the two disagree, **this repository is authoritative** for the notification, channel,
preference and delivery contracts.
