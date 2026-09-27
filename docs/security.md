# Security

The threat model is narrow and specific: this engine can email, text and push on a person's behalf,
and it holds a record of what every recipient was told. Two things must be true — a notification
reaches only its recipient, and only a trusted producer can cause one.

## Isolation

Recipient scoping is enforced **at the data layer**, by an RLS policy covering realm, tenant and
recipient. This is a release blocker (NS-001), not a hardening measure.

Three details carry the weight:

1. **`FORCE ROW LEVEL SECURITY`**, not just `ENABLE`. Without forcing, the table owner bypasses its own
   policy — and the owner is the role a worker connects as, so the policy would protect nobody.
2. **The realm is in the predicate.** Two products sharing a deployment are isolated by the same
   mechanism that isolates two tenants, rather than by a convention.
3. **No route accepts a recipient id.** The recipient-facing API resolves identity from the
   authenticated session. An endpoint that took a recipient id as a parameter would move isolation from
   the database into every handler's argument validation.

Verify isolation against *your* binding, as the role your worker actually uses. The contract suite
includes that test; running it as a superuser proves nothing.

## Producer trust

`Recipient` and `Tenant` are **authoritative from the producer** and must never come from notification
content (NR-009). A producer that forwards a user-supplied recipient id has built a delivery
redirection: an attacker chooses who gets told what, in your product's voice.

In service mode, producers authenticate — mTLS or a signed token. The service is a capability to send
messages as your product, so an unauthenticated producer path is equivalent to an open relay.

## Tokens in delivered content

Two kinds of link get embedded in notifications, and both are scoped narrowly on purpose:

| Token | Scope | Why |
|---|---|---|
| Unsubscribe | one `(topic, channel)` pair, signed | A leaked link disables one preference, not every notification the recipient receives |
| Acknowledgement (Phase 2) | one notification, one recipient, single-use, expiring | A leaked link acknowledges nothing else, and cannot be replayed |

Neither requires a session, because both arrive by email where a session does not exist. That is
exactly why their scope has to be minimal.

## Provider webhooks

Bounce and complaint callbacks are **signature-verified before the body is parsed**. Parsing first and
verifying after means an attacker's payload has already been through your parser, and a forged bounce
suppresses a real address — a denial of notification that looks like normal operation.

An unverified request returns 401 with an empty body and mutates nothing.

## Content handling

- `payload` is deep-link references for the UI and is **never** a routing input. Treating it as
  routing data would let content decide delivery.
- `attributes` is audit metadata only — never a routing or preference input.
- Notification bodies may contain user-supplied text. Channels are responsible for escaping it for
  their medium; the engine stores it verbatim and makes no claim about its safety.

## Multi-tenant deployments

- Provider credentials live in channel implementations, per tenant where needed. The engine never holds
  a provider key.
- Quotas are per `(realm, tenant, channel)`, so one tenant cannot consume another's sending budget
  ([D5](../specs/001-notification-core/design-decisions.md#d5)).
- Suppression lists are realm-scoped. A hard bounce in one realm does not suppress the same address in
  another, because they are different relationships with that address.

## What this engine does not do

- It does not encrypt notification bodies at rest beyond whatever the database provides. If a title or
  body is sensitive enough to require field-level encryption, encrypt before calling `Notify`.
- It does not audit reads of the inbox. It records what was sent and delivered, not who looked.
- It does not authenticate recipients. That is the host's session layer.
