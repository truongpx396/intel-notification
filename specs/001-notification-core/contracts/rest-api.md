# Contract: REST surface

The recipient-facing HTTP surface, served by `adapters/driving/httpapi` over the `Inbox` and `Admin`
ports. Producers do **not** use this — they use the `Notifier` port (in-process) or the gRPC facade
(service mode). This surface exists for inboxes, preference screens, the live stream, unsubscribe
links, provider callbacks and operators.

## Authentication and scope

Every recipient-scoped route resolves the caller's `Identity` — realm, tenant, recipient — from its
authentication and sets that scope, transaction-locally, for every query it runs. **No route accepts a
recipient, tenant or realm as a parameter** — that is what makes NR-008 enforceable rather than
aspirational.

| Mode | How the identity is established |
|---|---|
| Library | The host mounts the handler with an `IdentityFunc(*http.Request) (notify.Identity, error)` over its own session |
| Service | A **recipient token**: a JWT the host signs (ES256, EdDSA or RS256; `HS256` only in development) and verifies against the realm's JWKS. Claims: `iss` (bound to one realm), `tenant` `{kind,id}`, `recipient` `{kind,id}`, `exp` ≤ 1 hour. The host mints it for the signed-in user; the service never sees the host's session |

Administrative routes require an operator credential — mTLS or an operator token — never a recipient
token.

## Inbox

| Method | Path | Purpose | Notes |
|---|---|---|---|
| GET | `/notifications` | The caller's notifications, newest first | `?limit=&cursor=`; `?state=unread\|read\|archived`; `?topic=`; `?locale=` (default from `Accept-Language`). Only inbox-visible, uncanceled rows whose send time has passed. Each item is rendered at read time from its topic's in-app template in the caller's locale, falling back to stored copy ([D27](../design-decisions.md#d27)) |
| GET | `/notifications/unread-count` | The badge value | `{count, capped}` — a **bounded** count recomputed from the store; `capped: true` means display "99+" ([D15](../design-decisions.md#d15)) |
| POST | `/notifications/seen` | Clear the bell | Body `{before}`: marks everything up to that instant seen. Seen is not read ([D26](../design-decisions.md#d26)) |
| POST | `/notifications/{id}/read` | Mark one read | Idempotent. **404**, not 403, for a notification the caller does not own — a 403 confirms the id exists |
| POST | `/notifications/read-all` | Mark all read | Idempotent |
| POST | `/notifications/{id}/archive` | Archive one | Body `{archived: bool}`; idempotent; archived items leave the default list and the badge |
| GET | `/notifications/stream` | Live push | Server-sent events. On every (re)connect the server sends the authoritative bounded count **first**. Then, per nudge, it loads the item **under the caller's scope** and sends it with the new count; a nudge that loads nothing sends nothing ([D16](../design-decisions.md#d16)) |

## Preferences and schedule

| Method | Path | Purpose | Notes |
|---|---|---|---|
| GET | `/notifications/preferences` | Per-`(topic, channel)` state | Each value carries `source: recipient \| tenant \| topic_default` and `locked` ([D30](../design-decisions.md#d30)) |
| PUT | `/notifications/preferences` | Upsert preferences | Body `[{topic, channel, enabled}]`. A pair that is essential or tenant-locked is rejected with a stated reason, never silently ignored |
| GET | `/notifications/schedule` | Quiet hours and digest cadence | |
| PUT | `/notifications/schedule` | Update them | The response restates that `critical` notifications ignore quiet hours, so a client cannot present a false promise ([D9](../design-decisions.md#d9)) |

## Unsubscribe (RFC 8058)

| Method | Path | Purpose | Notes |
|---|---|---|---|
| GET | `/notifications/unsubscribe?token=` | Confirmation page | **Changes nothing.** Mail scanners and link previewers fetch every URL in a message ([D34](../design-decisions.md#d34)) |
| POST | `/notifications/unsubscribe?token=` | One-click unsubscribe | The `List-Unsubscribe-Post: List-Unsubscribe=One-Click` target, and the confirmation page's button. Disables exactly one `(realm, tenant, recipient, topic, channel)` pair. No session required |

The token is signed with a key id (so keys rotate without breaking old mail), scoped to one pair (so a
leaked link disables one preference, not everything), and valid for at least 60 days.

## Tenant administration

| Method | Path | Purpose | Notes |
|---|---|---|---|
| GET | `/admin/tenants/{tenant_kind}/{tenant_id}/preferences` | A tenant's defaults and locks | Operator, or a host-authorized tenant administrator |
| PUT | `/admin/tenants/{tenant_kind}/{tenant_id}/preferences` | Set them | Body `[{topic, channel, enabled, locked}]`. Essential topics cannot be disabled |

## Operator

| Method | Path | Purpose | Notes |
|---|---|---|---|
| POST | `/admin/notifications/broadcast` | Announce to an audience | Body `{tenant, audience \| recipients, topic, data, title?, body?, priority?, idem_key}`. Records a durable broadcast and returns `202` with its id; audited (NR-020) |
| GET | `/admin/notifications/dead-letters` | Inspect dead letters | Filter by tenant, channel, `reason` (`max_attempts \| rejected \| poison`). Correct outcomes (suppressed, no address) are not here — see `/admin/notifications/deliveries` |
| POST | `/admin/notifications/dead-letters/{id}/replay` | Re-drive one | Enqueues a fresh delivery and records `replayed_at`, once per dead letter |
| GET | `/admin/notifications/deliveries` | Delivery history | Filter by tenant, channel, outcome, time |
| POST | `/admin/erasure` | Erase a recipient | Body `{tenant, recipient}`. Returns rows removed per table; the erasure is recorded by hash ([D35](../design-decisions.md#d35)) |

## Provider callbacks

| Method | Path | Purpose | Notes |
|---|---|---|---|
| POST | `/webhooks/{channel}/{provider}` | Bounce, complaint and delivery-status callbacks | **Signature-verified before parsing**; an unverified body is never read as data. The provider message id finds the delivery in `notification_deliveries` and records its `provider_status`. A hard bounce or complaint upserts a suppression keyed by address hash; a soft bounce sets `expires_at` ([D14](../design-decisions.md#d14)) |

## Error semantics

| Condition | Status | Body |
|---|---|---|
| Missing, expired or unverifiable recipient token | `401` | Empty |
| Not the recipient of `{id}` | `404` | Generic not-found. Never distinguishes "exists but not yours" |
| Unknown topic or channel in a preference write | `422` | Names the unregistered value |
| Preference write against an essential or locked pair | `422` | Names the pair and why it cannot be changed |
| Missing `idem_key` on a broadcast | `422` | States that the key is required for exactly-once |
| Inline broadcast list over `MaxInlineRecipients` | `422` | States the limit and points to audience selectors |
| Invalid or expired unsubscribe token | `410` | A page explaining the link expired, with a route to preferences |
| Unverified webhook signature | `401` | Empty body; the payload is not parsed |

## Observable assertions

- A recipient's inbox and stream never contain another recipient's, tenant's or realm's notifications
  (NS-001, hard) — including when two tenants each have a recipient with the same id.
- A forged nudge naming another recipient's notification produces no event on the victim's stream.
- Marking another recipient's notification read returns 404 and changes nothing.
- With a `(topic, channel)` pair disabled, an event in that topic yields an inbox row and no delivery on
  that channel; with every inbox-backed channel disabled, no inbox row is listed and the badge is unchanged.
- Reconnecting the stream yields the correct bounded unread count even after a missed push.
- `GET` on an unsubscribe link changes no preference.
- A broadcast returns before per-recipient delivery completes and appears in the audit trail.
- A signature-invalid webhook mutates no suppression or delivery state.
