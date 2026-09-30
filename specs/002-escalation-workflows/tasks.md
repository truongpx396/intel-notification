# Tasks (Phase 2)

**Not started.** Depends on [Phase 1](../001-notification-core/tasks.md) Stages 1–6 being green.
Sketched at the granularity needed to judge scope, not yet to the granularity needed to execute.

Worked under the same rules as Phase 1: see
[How a task is worked](../001-notification-core/tasks.md#how-a-task-is-worked) — red before green, red
seen failing for the right reason, the lowest layer that can hold the guarantee, and the definition of
done including the [mutation ledger](../../docs/mutations.md). Task IDs are stable and the list order is
the build order, so within a stage the test comes first. The **Red first** lines are sketches: they name
what must be proven, and the exact cases are settled when the stage starts.

**Blocked** marks a task whose behaviour is still an open question in
[spec.md](spec.md#open-questions). A red test cannot be written for behaviour nobody has decided, so
the question is answered first; the parts that do not depend on it are tested now.

## Stage 0 — Baseline

- [ ] **T100** *setup* Capture the Phase 1 latency baseline for topics with no acknowledgement:
      `make bench` on the reference configuration, numbers committed to the docs, before any Phase 2
      code lands. This cannot be done after the fact, and T122 compares against it (NS-106)

## Stage 1 — Acknowledgement

- [ ] **T106** *red* **Contract test**, written first (NR-101, NR-103, NR-104). Delivered, read and
      acknowledged are independent — acknowledging does not mark read, reading does not acknowledge; a
      double-ack is a no-op and reports itself as one; the acknowledging channel is recorded; a token is
      refused after first use and after expiry; a token for notification A acknowledges nothing on
      notification B, and nothing for another recipient; another recipient's `POST` returns not-found and
      changes nothing; a topic without `RequiresAck` writes no acknowledgement rows (NR-102)
- [ ] **T101** *red → green* Migration: `notification_acks` keyed `(realm, tenant, recipient,
      notification_id)`, with the acknowledging channel recorded, recipient-scoped through
      `notify_apply_recipient_scope()` (NR-101, NR-103,
      [D17](../001-notification-core/design-decisions.md#d17)).
      **Red first:** extend `scripts/verify-schema.sql` — RLS is forced on the table and every partition,
      the key rejects a second row, the same recipient id in two tenants does not collide — and watch it
      fail before the migration exists. **Mutate:** create a partition without the scope
- [ ] **T102** *red → green* Migration: `ack_tokens` — single-use, per-notification, per-recipient,
      expiring (NR-104). **Red first:** schema assertions that a token row cannot omit its notification,
      recipient or expiry, and that a used token cannot be marked unused again
- [ ] **T103** *red → green* `TopicDef.RequiresAck` and its propagation through registration (NR-102).
      **Red first:** a table test — the flag defaults to false, survives registration, and a topic without
      it produces a `DeliveryPlan` identical to Phase 1's
- [ ] **T104** *green* `Acknowledge(ctx, notificationID, recipient, viaChannel)` on the driving port,
      idempotent. Make T106 pass one case at a time.
      **Mutate:** infer acknowledgement from read; write a second row on a repeat
- [ ] **T105** *red → green* REST: `POST /notifications/{id}/ack`, and for email a tokenised link whose
      `GET` renders a confirmation and whose `POST` acknowledges — a link scanner must not acknowledge on
      the recipient's behalf ([D34](../001-notification-core/design-decisions.md#d34)).
      **Red first:** a Playwright spec, added with `fixme` like the Phase 1 specs and enabled as the route
      lands, plus handler tests: `GET` on the link makes no call to the acknowledgement writer; `POST`
      acknowledges exactly once; an expired token returns `410`.
      **Mutate:** acknowledge on `GET`

## Stage 2 — Escalation

- [ ] **T113** *red* **Contract test**, written first, driving the scheduler through crash points with
      the Phase 1 test kit. Restart and duplicate ticks never double-escalate (NS-102); every chain
      terminates (NS-105); an acknowledgement stops the chain within one scheduler interval, in every
      case (NS-101); an escalation to a second recipient is not visible to the first, and the reverse
      (NS-103); a step fires inside quiet hours (NR-110); an exhausted chain emits once and does not loop
      (NR-109)
- [ ] **T107** *red → green* Migration: `escalation_state` carrying the full identity and the
      notification's `created_at` (so the scheduler can scope its reads under forced RLS), the current
      step and next-fire time, unique per `(notification, step)` (NR-107,
      [D17](../001-notification-core/design-decisions.md#d17)).
      **Red first:** schema assertions — RLS forced, a second row for the same `(notification, step)` is
      rejected, the scheduler's read as the table owner returns only its own scope.
      **Mutate:** drop the uniqueness; drop `created_at` from the scoped read
- [ ] **T108** *red → green* `EscalationChain` in `TopicDef`: ordered steps of
      `{delay, channels, recipient?}` (NR-105). **Red first:** a validation table — an empty chain, a step
      with no channel, a non-positive delay and a chain past a maximum length are rejected, so a chain
      terminates by construction (NS-105)
- [ ] **T109** *red → green* `app/escalation.go`: the advance decision, stopping on acknowledgement
      (NR-106, NR-110). **Red first:** a table-driven unit test on a fake clock — advance only if still
      unacknowledged when the delay elapses; an acknowledgement one tick earlier stops it; a step fired
      inside quiet hours is not deferred; the last step yields exhaustion, not another step.
      **Mutate:** advance without re-reading acknowledgement; apply quiet hours to a step
- [ ] **T110** *red → green* Scheduler: claims due steps with the Phase 1 discipline — `SKIP LOCKED`
      leases fenced by a token — idempotent per `(notification, step)`; no broker tick (NR-107,
      [D19](../001-notification-core/design-decisions.md#d19),
      [D22](../001-notification-core/design-decisions.md#d22)). Make T113 pass.
      **Red first:** two schedulers on one due step fire it once; a crash between claiming and firing is
      re-driven after the lease lapses, without a second fire; a stale scheduler's write is discarded.
      **Mutate:** drop the fence; fire before recording the step
- [ ] **T111** *red → green* Cross-recipient escalation creating a properly scoped notification
      (NR-108). **Blocked** on the first open question: whether a step derives `<idem>:esc:<step>` or
      mints its own key. Testable now, and written first: the created notification belongs to the target
      recipient under forced RLS, and the original recipient cannot read it (NS-103)
- [ ] **T112** *red → green* Terminal `escalation_exhausted` emission (NR-109). **Blocked** on the third
      open question: an event, or a notification that preferences could disable. Testable now, and
      written first: exhaustion is emitted exactly once per chain, and never loops (NS-105)

## Stage 3 — Workflows

- [ ] **T118** *red* **Contract test**, written first: a table over every step boundary and each crash
      point around it, with the instance resuming at the correct position (NS-104, NR-112); changing the
      definition while an instance is in flight leaves that instance on the version it started (NR-111);
      cancelling stops pending steps and leaves every sent delivery as it was (NR-113)
- [ ] **T114** *red → green* Workflow definition type, versioned, with in-flight instances pinned to
      their version (NR-111). **Blocked** on the second open question (data or Go) for how a definition is
      represented; the pinning behaviour does not depend on it and is tested first against a Go
      definition
- [ ] **T115** *red → green* Migration: `workflow_instances` with a durable position (NR-112).
      **Red first:** schema assertions — RLS forced, the position is written in the same transaction as the
      step it records
- [ ] **T116** *red → green* `app/workflow.go`: send / wait / branch execution. **Red first:** a unit
      table on a fake clock — send enqueues, wait defers to its delay, branch follows acknowledgement or
      its absence.
      **Mutate:** record the position before the send commits
- [ ] **T117** *red → green* Cancellation semantics, explicitly not retracting sent deliveries (NR-113).
      **Red first:** cancel mid-wait stops the next step; a delivery already made stays `delivered`

## Stage 4 — Outcome webhooks

- [ ] **T121** *red* **Contract test**, written first: a host that dedupes on the key sees each outcome
      once, however many times it is delivered (NR-115); a failing receiver gets the same key on every
      retry; a body altered in transit fails signature verification; losing the webhook loses no state,
      because the outcome is still readable from `notification_deliveries` (NR-115)
- [ ] **T119** *red → green* Migration: `outcome_webhook_outbox` — reuses the Phase 1 queue discipline
      (pending rows only, fenced claims, history on a partitioned log) rather than inventing a second
      delivery mechanism; outcomes are read from `notification_deliveries`
      ([D20](../001-notification-core/design-decisions.md#d20)).
      **Red first:** schema assertions — RLS forced, the outbox holds pending rows only, and finished
      rows leave it for the partitioned history
- [ ] **T120** *green* Signed, retried, at-least-once delivery with an idempotency key (NR-114, NR-115).
      Make T121 pass.
      **Mutate:** mint a new key per retry; sign before the body is final

## Stage 5 — Non-regression

- [ ] **T122** *red* Benchmark asserting NS-106: topics without acknowledgement see no latency change.
      This is the task that keeps Phase 2 from taxing Phase 1 adopters, so it gates the release. It
      compares against the T100 baseline with the tolerance fixed before the first run, and it is a gate
      and not a report: a run outside the tolerance fails

---

## Traceability

| Criterion | Proven by | Written (red) in |
|---|---|---|
| NS-101 acknowledgement stops the chain | escalation contract test | T113 |
| NS-102 no double escalation | escalation contract test; scheduler crash and duplicate-tick cases | T113, T110 |
| NS-103 second recipient isolated | escalation contract test; cross-recipient scope test | T113, T111 |
| NS-104 workflow resumes | workflow contract test at every step boundary | T118 |
| NS-105 every chain terminates | chain validation table; escalation contract test | T108, T113 |
| NS-106 no Phase 1 regression | benchmark against the baseline | T100, T122 |
