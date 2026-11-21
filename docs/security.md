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

In service mode, producers authenticate with mTLS or a signed token ([D56](../specs/001-notification-core/design-decisions.md#d56)).
With mTLS the principal is the first URI SAN (else the first DNS SAN) of a client certificate the TLS layer
verified: a certificate that was presented and not verified is no principal. With a token the principal is its
**subject**, and the token is verified with the keys of the realm that subject is bound to, so a subject bound to
one realm cannot be claimed with another realm's keys, and its `iss` must be that realm. A principal with no
binding is refused before any handler runs; a caller is told only that it is not bound, and a credential that
fails is told only that it was not accepted, never why. The service is a capability to send messages as your
product, so an unauthenticated producer path is an open relay. An operator's token (for `/admin` routes)
administers the one realm it was issued for, and the realm of an administrative call is that token's and never a
request's.

## The database role

The schema's tenant isolation is row-level security, and PostgreSQL does not apply it to a superuser or a role with
`BYPASSRLS`, whatever `FORCE` says. A service connected as one would pass every test of the policies and hold none
of the isolation, so `notifyd` refuses to start as one (`NOTIFY_ALLOW_PRIVILEGED_DB_ROLE` is for a throwaway
development database). The compose stack builds the schema as the superuser and hands it to an application role,
as the test kit does.

## Credentials of providers

`channel_providers.secret_ref` is a reference (`env:`, `file:`, or a secret manager's URI) and the schema refuses a
value with no scheme. The service resolves it when a channel is built and hands the result to the provider's
constructor and to nothing else: it appears in no report, error, log or delivery result, and an error a provider's
own constructor writes with the key in it is redacted. A reference that cannot be resolved makes that provider
unavailable, with the reference and the reason, and does not stop the others.

## Recipients in service mode

The recipient REST surface accepts a **recipient token**: a JWT the host signs for its signed-in user,
verified against the realm's JWKS, carrying the tenant and recipient, expiring within an hour. The
service never sees the host's session, and a token for one realm cannot be used in another. ES256, EdDSA and
RS256 are accepted, with the algorithm the key allows and not the one the header asks for; `alg: none` and
`HS256` are not accepted at all, not even in development: a shared secret held by both sides means either can mint tokens for any
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

## Outbound webhooks

The `webhook` channel calls a URL a tenant supplied, which makes the engine's network position the
tenant's to point. It is hostile input ([D44](../specs/001-notification-core/design-decisions.md#d44)):
https only, never a connection to a loopback, private, link-local or carrier-grade-NAT address (checked
on the address the connection is made to, after the name is resolved, so a name that points inward is no
way round, and an IPv4 address written as IPv6 is judged as the IPv4 it is), no redirect followed, no
proxy taken from the environment, and no credentials in the URL. A URL it must not call is reported as a
dead address. Every request is signed with the channel's secret
(`X-Notify-Signature: v1=<hex HMAC-SHA256 of "<timestamp>.<body>">`), so a receiver that verifies it
and rejects a timestamp far from its own clock cannot be sent a forged or a replayed notification. What
the channel reports of a failure never carries the URL, because a URL often holds a token and the report
is stored in the dead letter.

`WithAllowHTTP` and `WithAllowPrivateNetworks` relax those checks for a receiver inside your own trust
boundary. Turn them on for a channel whose addresses tenants supply and a tenant can make the engine call
your internal network, including a cloud's metadata service.

## Provider webhooks

Bounce and complaint callbacks are **signature-verified before the body is parsed**. Parsing first means
an attacker's payload has already been through your parser, and a forged bounce suppresses a real address
— a denial of notification that looks like normal operation. An unverified request returns 401 with an
empty body and mutates nothing.

Resend signs with Svix: HMAC-SHA256 over the message id, the timestamp and the raw body, with the endpoint's secret.
The decoder checks every configured secret and every `v1` signature the request carries (a secret is rotated by
overlap), inside a five-minute window either way, so a captured callback is not replayable for ever. The secret is a
reference like every credential (`NOTIFY_RESEND_WEBHOOK_SECRETS`, `env:` or `file:`). A callback names no realm, so each
realm has a route of its own and the suppressions it writes are under that realm and no other (NR-009): a bounce that
says another realm in its body is not read as saying it. Only a bounce the provider calls `Permanent`, or a
complaint, suppresses an address for ever; a bounce that says less is a cooldown (D14).

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
