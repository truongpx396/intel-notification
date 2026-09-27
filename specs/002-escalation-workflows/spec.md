# Specification: Escalation & delivery workflows (Phase 2)

**Status: designed, not started. Depends on [Phase 1](../001-notification-core/).**

Phase 1 answers "was it delivered?". This phase answers **"did anyone deal with it?"** — which is a
different question with different state, and conflating them is why notification systems grow
unmaintainable.

## Why this is a separate phase

Everything in Phase 1 is stateless per notification: a notification is persisted, fanned out, and
each delivery reaches a terminal outcome. Acknowledgement introduces a **lifecycle** — a notification
can be delivered but unacknowledged, and it can stay that way for a while, and something must happen
when it does.

That state does not belong in the Phase 1 hot path. If acknowledgement lived on the inbox row, every
delivery would contend with acknowledgement writes, and the escalation scheduler would be polling the
same partitions the inbox reads from. Keeping it separate means Phase 1 stays a delivery engine you
can adopt without buying into a workflow model.

## Requirements

### Acknowledgement

- **NR-101**: The system MUST record acknowledgement as explicit state per `(notification, recipient)`,
  distinct from delivery and from read state. Delivered, read and acknowledged are three different
  facts and MUST NOT be inferred from one another.
- **NR-102**: A topic MUST be able to declare itself **acknowledgement-requiring**. Topics that do not
  are unaffected and carry no lifecycle cost.
- **NR-103**: Acknowledgement MUST be idempotent and MUST record which channel it arrived through, so
  that an email link and an in-app button are distinguishable in audit.
- **NR-104**: An acknowledgement token delivered over a channel MUST be scoped to one notification and
  one recipient, single-use, and expiring. A leaked link MUST NOT acknowledge anything else.

### Escalation

- **NR-105**: A topic MUST be able to declare an **escalation chain**: an ordered list of steps, each
  naming a delay, a channel set, and optionally a different recipient.
- **NR-106**: The system MUST advance to the next step only if the notification is still
  unacknowledged when that step's delay elapses, and MUST stop the chain immediately on acknowledgement.
- **NR-107**: Escalation MUST be idempotent per `(notification, step)`. A scheduler crash or a
  duplicate tick MUST NOT double-escalate.
- **NR-108**: Escalating to a different recipient MUST create a notification scoped to *that*
  recipient, subject to the Phase 1 isolation rules. It MUST NOT make one recipient's notification
  visible to another.
- **NR-109**: When a chain exhausts every step with no acknowledgement, the system MUST emit a
  terminal `escalation_exhausted` event and MUST NOT loop.
- **NR-110**: Escalation steps MUST ignore quiet hours, because a step only fires when an earlier
  delivery went unacknowledged — which is precisely the case quiet hours must not suppress
  ([D9](../001-notification-core/design-decisions.md#d9) sets the precedent).

### Workflows

- **NR-111**: The system MUST support a declarative multi-step workflow: a sequence of send, wait, and
  branch-on-acknowledgement steps, versioned so that a workflow change does not retroactively alter
  in-flight instances.
- **NR-112**: A workflow instance MUST be resumable after a process restart, with its position durable.
- **NR-113**: A workflow MUST be cancellable, and cancellation MUST stop pending steps without
  retracting deliveries already made — a sent notification cannot be unsent, and pretending otherwise
  is worse than admitting it.

### Outcome webhooks

- **NR-114**: The system MUST be able to notify the host of delivery outcomes — delivered, failed,
  acknowledged, escalated, exhausted — through a signed, retried, at-least-once webhook.
- **NR-115**: Outcome webhooks MUST carry an idempotency key so a host can dedupe, and MUST NOT be the
  only record of an outcome; the durable state remains authoritative.

## Success criteria

- **NS-101**: An acknowledged notification stops its chain within one scheduler interval, in 100% of
  cases.
- **NS-102**: No notification escalates twice for the same step, across scheduler restarts and
  duplicate ticks.
- **NS-103**: An escalation to a second recipient is never visible to the first, and vice versa.
- **NS-104**: A workflow instance survives a restart at any step boundary and resumes at the correct
  position.
- **NS-105**: Every chain terminates. No notification escalates indefinitely.
- **NS-106**: Phase 1 topics that declare no acknowledgement requirement show no measurable change in
  delivery latency after this phase ships.

## Open questions

| Question | Options |
|---|---|
| Does an escalation step re-use the original notification's `idem_key` namespace or mint its own? | Deriving `<idem>:esc:<step>` keeps it traceable and idempotent; a fresh key loses the link |
| Should a workflow be expressible as data (JSON) or only in Go? | Data enables Phase 3 authoring, but needs its own validation and versioning story |
| Is `escalation_exhausted` a notification or only an event? | As a notification it is subject to preferences, which a recipient could disable — defeating the point |

These are genuinely open. They are recorded rather than guessed because the answers depend on the
acknowledgement model settling first.
