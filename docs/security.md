# Security

The threat model is narrow and specific: this engine can email, text and push on a person's behalf,
and it holds a record of what every recipient was told. Two things must be true — a notification
reaches only its recipient, and only a trusted producer can cause one.

## Isolation

Recipient scoping is enforced **at the data layer**, by one row-level security policy covering realm,
tenant and recipient. This is a release blocker (NS-001), not a hardening measure. Five details carry
the weight ([D17](../specs/001-notification-core/design-decisions.md#d17)):

1. **`FORCE ROW LEVEL SECURITY`**, not just `ENABLE`. Without forcing, the table owner bypasses its own
   policy — and the owner is the role a worker connects as.
2. **Every partition is scoped.** PostgreSQL applies a partitioned table's policy only to queries through
   the parent; a partition queried by name has its own settings. `notify_apply_recipient_scope()` applies
   the policy to each partition, and `make verify-schema` fails if any lacks it.
3. **Scope is transaction-local.** The engine sets it with `set_config(…, true)` for each transaction and
   never with a session `SET`, which on a pooled connection would hand the next request the previous
   recipient's rows. Identity columns reject empty strings, so an unset scope matches nothing.
4. **The realm and the tenant are in the predicate and in every key.** Two products, or one user in two
   workspaces, are isolated by the same mechanism that isolates two users.
5. **No route accepts a recipient, tenant or realm.** Identity comes from authentication. An endpoint that
   took one as a parameter would move isolation from the database into every handler.

Verify isolation against *your* binding, as the role your worker actually uses. `make verify-schema`
runs its suite as a non-superuser table owner for exactly this reason; running it as a superuser — or as
a non-owner, which is subject to RLS even without `FORCE` — proves nothing about `FORCE`.

## The live stream

The push path never touched the database in the first design, so row-level security did not protect it,
and its channel key — `notify:user:<id>` — leaked across tenants
([D16](../specs/001-notification-core/design-decisions.md#d16)). Now:

- The stream key is the hash of the canonical encoding of the full identity, so no two identities share
  one.
- A nudge carries a notification id and nothing else — no title, no body, no count.
- The relay subscribes only to the key derived from the **authenticated** identity, and loads each item
  through the RLS-scoped store under that identity. A misrouted or forged nudge loads zero rows and emits
  nothing.

## Producer trust

`Recipient` and `Tenant` are **authoritative from the producer** and must never come from notification
content (NR-009). A producer that forwards a user-supplied recipient id has built a delivery redirection:
an attacker chooses who gets told what, in your product's voice.

The **realm** is never taken from a request at all. In library mode it is configuration. In service mode
it is derived from the authenticated producer principal through `NOTIFY_REALM_BINDINGS`; a producer
cannot name a realm, so it cannot write into another product's key space or read another product's
status.

In service mode, producers authenticate with mTLS or a signed token. The service is a capability to send
messages as your product, so an unauthenticated producer path is an open relay.

## Recipients in service mode

The recipient REST surface accepts a **recipient token**: a JWT the host signs for its signed-in user,
verified against the realm's JWKS, carrying the tenant and recipient, expiring within an hour. The
service never sees the host's session, and a token for one realm cannot be used in another. `HS256` is
accepted only in development: a shared secret held by both sides means either can mint tokens for any
recipient.

## Tokens in delivered content

| Token | Scope | Why |
|---|---|---|
| Unsubscribe | one `(realm, tenant, recipient, topic, channel)`, signed with a key id, valid ≥ 60 days | A leaked link disables one preference. Key ids let keys rotate without breaking old mail; the lifetime meets CAN-SPAM's 30-day rule |
| Acknowledgement (Phase 2) | one notification, one recipient, single-use, expiring | A leaked link acknowledges nothing else, and cannot be replayed |

Neither requires a session, because both arrive by email where a session does not exist. That is why
their scope is minimal. The unsubscribe link changes state only on `POST` (RFC 8058): a `GET` shows a
confirmation page, because mail scanners fetch every URL in a message
([D34](../specs/001-notification-core/design-decisions.md#d34)).

## Provider webhooks

Bounce and complaint callbacks are **signature-verified before the body is parsed**. Parsing first means
an attacker's payload has already been through your parser, and a forged bounce suppresses a real address
— a denial of notification that looks like normal operation. An unverified request returns 401 with an
empty body and mutates nothing.

## Personal data

- **Erasure.** `Maintenance.Erase` removes a recipient from every table in one transaction and
  records the erasure by hash ([D35](../specs/001-notification-core/design-decisions.md#d35)).
- **Suppressions** store the hash of the normalized address, never the address, and survive erasure — so
  honoring an erasure request never resumes mailing someone who complained. A hash is minimization, not
  anonymity: it is retained on the legitimate basis of honoring the objection.
- **Dead letters** hold the queue row, never rendered content.
- **Redis** holds hashed keys and notification ids only.
- **Bodies at rest** are protected only by whatever the database provides. If a notification's data is
  sensitive enough to need field-level encryption, encrypt it before calling `Notify`, and render it in a
  channel that can decrypt.

## Provider credentials

The engine core never holds a provider key. In library mode they belong to the channel implementations
your host constructs. In service mode `channel_providers.secret_ref` references them — `env:`, `file:`, or
a secret-manager URI — and the schema rejects a value with no scheme, which catches a key pasted into the
table before it reaches a backup.

## Multi-tenant deployments

- Quotas are per `(realm, tenant, channel)` with a realm-wide default, and an exhausted tenant's backlog
  leaves the claim range, so one tenant cannot consume another's sending budget or worker time
  ([D24](../specs/001-notification-core/design-decisions.md#d24)).
- Suppression lists are realm-scoped. A hard bounce in one realm does not suppress the same address in
  another, because they are different relationships with that address.

## What this engine does not do

- It does not audit reads of the inbox. It records what was sent and delivered, not who looked.
- It does not authenticate recipients itself. That is the host's session layer, or the host-signed token
  in service mode.
- It does not sanitize bodies. Channels escape content for their medium; the engine stores data verbatim
  and templates render it.
