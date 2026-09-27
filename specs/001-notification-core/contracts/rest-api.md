# Contract: REST surface

The recipient-facing HTTP surface. Producers do **not** use this — they use the `Notifier` port
(in-process) or the gRPC facade (service mode). This surface exists for inboxes, preference screens
and provider callbacks.

All recipient-scoped routes resolve the caller's `(realm, tenant, recipient)` from its authenticated
session and set the corresponding data-layer scope for the transaction. No route accepts a recipient
id as a parameter — that is what makes NR-008 enforceable rather than aspirational.

## Inbox

| Method | Path | Purpose | Notes |
|---|---|---|---|
| GET | `/notifications` | The caller's notifications, newest first | Paginated `?limit=&cursor=`; `?unread=true` filters; `?topic=` filters. Recipient-scoped at the data layer |
| GET | `/notifications/unread-count` | The badge value | `{unread: number}`, recomputed from the store — never from a cached counter ([D15](../design-decisions.md#d15)) |
| POST | `/notifications/{id}/read` | Mark one read | Idempotent. Returns **404**, not 403, for a notification the caller does not own — a 403 confirms the id exists |
| POST | `/notifications/read-all` | Mark all read | Idempotent |
| GET | `/notifications/stream` | Live push of new notifications and the unread count | Server-sent events. On every (re)connect the server sends the authoritative count **before** streaming, so a missed push self-heals |

## Preferences

| Method | Path | Purpose | Notes |
|---|---|---|---|
| GET | `/notifications/preferences` | Per-`(topic, channel)` state | Absent rows are returned as the topic's registered default, explicitly flagged as a default rather than a stored choice |
| PUT | `/notifications/preferences` | Upsert preferences | Body: `[{topic, channel, enabled}]`. A pair whose topic is registered **essential** is rejected with a stated reason, never silently ignored |
| GET | `/notifications/schedule` | Quiet hours and digest cadence | |
| PUT | `/notifications/schedule` | Update them | `critical` notifications ignore quiet hours; the response restates that rule so a client cannot present a false promise ([D9](../design-decisions.md#d9)) |
| GET | `/notifications/unsubscribe` | One-click unsubscribe from an email | Signed token in `?token=`; no session required. Disables exactly one `(topic, channel)` pair. Token is single-topic scoped, so a leaked link cannot disable everything |

## Administrative

| Method | Path | Purpose | Notes |
|---|---|---|---|
| POST | `/admin/notifications/broadcast` | Announce to an audience | Body `{audience, topic, title, body, priority?, idem_key}`. Enqueues expansion and returns promptly; audited (NR-020) |
| GET | `/admin/notifications/dead-letters` | Inspect terminal deliveries | Filter by realm, tenant, channel, reason |
| POST | `/admin/notifications/dead-letters/{id}/replay` | Re-drive one | Records `replayed_at`; idempotent per channel |

## Provider callbacks

| Method | Path | Purpose | Notes |
|---|---|---|---|
| POST | `/webhooks/{channel}/{provider}` | Bounce, complaint and delivery-status callbacks | **Signature-verified before parsing.** An unverified body is never read as data. A hard bounce or complaint upserts a `channel_suppressions` row; a soft failure sets `expires_at` ([D14](../design-decisions.md#d14)) |

## Error semantics

| Condition | Status | Body |
|---|---|---|
| Not the recipient of `{id}` | `404` | Generic not-found. Never distinguishes "exists but not yours" |
| Unknown topic or channel in a preference write | `422` | Names the unregistered value |
| Preference write against an essential topic | `422` | Names the topic and why it cannot be disabled |
| Missing `idem_key` on a broadcast | `422` | States that the key is required for exactly-once |
| Unverified webhook signature | `401` | Empty body; the payload is not parsed |
| Quota exhausted on a broadcast | `202` | Accepted and deferred, not rejected — deferring is recoverable ([D5](../design-decisions.md#d5)) |

## Observable assertions

- A member's inbox and stream never contain another recipient's or another tenant's notifications
  (NS-001, hard).
- Marking another recipient's notification read returns 404 and changes nothing.
- With a `(topic, channel)` pair disabled, an event in that topic yields an inbox row and no delivery
  on that channel.
- Reconnecting the stream yields the correct unread count even after a missed push.
- A broadcast returns before per-recipient delivery completes and appears in the audit trail.
- A signature-invalid webhook mutates no suppression state.
