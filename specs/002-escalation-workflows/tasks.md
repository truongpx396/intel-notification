# Tasks (Phase 2)

**Not started.** Depends on [Phase 1](../001-notification-core/tasks.md) Stages 1–6 being green.
Sketched at the granularity needed to judge scope, not yet to the granularity needed to execute.

## Stage 1 — Acknowledgement

- [ ] **T101** Migration: `notification_acks` keyed `(realm, notification_id, recipient)`, with the
      acknowledging channel recorded (NR-101, NR-103)
- [ ] **T102** Migration: `ack_tokens` — single-use, per-notification, per-recipient, expiring (NR-104)
- [ ] **T103** `TopicDef.RequiresAck` and its propagation through registration (NR-102)
- [ ] **T104** `Acknowledge(ctx, notificationID, recipient, viaChannel)` on the driving port, idempotent
- [ ] **T105** REST: `POST /notifications/{id}/ack` and a tokenised `GET /notifications/ack` for email
- [ ] **T106** Contract test: double-ack is a no-op; a leaked token acknowledges nothing else (NR-104)

## Stage 2 — Escalation

- [ ] **T107** Migration: `escalation_state` keyed `(realm, notification_id)`, holding the current step
      and next-fire time, unique per `(notification, step)` (NR-107)
- [ ] **T108** `EscalationChain` in `TopicDef`: ordered steps of `{delay, channels, recipient?}` (NR-105)
- [ ] **T109** `app/escalation.go`: the advance decision, stopping on acknowledgement (NR-106)
- [ ] **T110** Scheduler: sharded tick, idempotent per `(notification, step)` (NR-107)
- [ ] **T111** Cross-recipient escalation creating a properly scoped notification (NR-108)
- [ ] **T112** Terminal `escalation_exhausted` emission (NR-109)
- [ ] **T113** Contract test: restart and duplicate ticks never double-escalate (NS-102); every chain
      terminates (NS-105)

## Stage 3 — Workflows

- [ ] **T114** Workflow definition type, versioned, with in-flight instances pinned to their version
      (NR-111)
- [ ] **T115** Migration: `workflow_instances` with a durable position (NR-112)
- [ ] **T116** `app/workflow.go`: send / wait / branch execution
- [ ] **T117** Cancellation semantics, explicitly not retracting sent deliveries (NR-113)
- [ ] **T118** Contract test: resume at every step boundary after a restart (NS-104)

## Stage 4 — Outcome webhooks

- [ ] **T119** Migration: `outcome_webhook_outbox` — reuses the Phase 1 outbox discipline rather than
      inventing a second delivery mechanism
- [ ] **T120** Signed, retried, at-least-once delivery with an idempotency key (NR-114, NR-115)
- [ ] **T121** Contract test: a host that dedupes on the key sees each outcome once

## Stage 5 — Non-regression

- [ ] **T122** Benchmark asserting NS-106: topics without acknowledgement see no latency change. This
      is the task that keeps Phase 2 from taxing Phase 1 adopters, so it gates the release
