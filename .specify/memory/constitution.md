# Constitution

Principles this engine is held to. Each one exists because violating it produced a real defect —
either in the originating design or in the class of systems this replaces.

## I. Correctness is a property of the schema, not of discipline

Isolation and exactly-once are enforced by constraints and policies, not by every call site
remembering to be careful. If a guarantee can only be upheld by convention, it will eventually be
violated by someone reading different code.

**In practice**: recipient scoping is an RLS policy on every scoped table and every partition, not a
`WHERE` clause. Exactly-once is a primary key, not a check-then-insert. One open digest window is a
partial unique index. A claim is a fenced lease in a SQL function, not a convention every worker has to
remember.

## II. A guarantee without an assertion is a comment

Every claim in a specification maps to something executable — a contract test or an assertion in
`verify-schema.sql`. The originating design specified a unique index that **could not be created**,
and nobody noticed because nothing ran it.

**In practice**: `make verify-schema` applies the migrations to a real PostgreSQL and asserts the
guarantees, including asserting that the known-bad shape still fails.

## III. The core knows nothing about the host

No product import, no provider SDK, no global config, no notion of "user". The module's only seams are
injected. This is checked by a lint gate, because "we'll be careful" does not survive a deadline.

**In practice**: `domain` imports nothing. `depguard` bans infra in the core and bans host imports
everywhere. CI builds the module with no host present.

## IV. Adding a channel does not touch fan-out

The fan-out loop is the riskiest code here, because every notification passes through it. Extending
delivery must not mean editing it. A registry makes a new channel additive; an `if` ladder makes it a
change to the code path everything depends on.

**In practice**: `for _, ch := range enabled { registry.Get(ch).Deliver(...) }`, forever.

## V. Never lose a notification; never send it twice

Between those two, **losing is worse**. A duplicate is an annoyance; a missing dunning notice or
security alert is a material harm. So delivery is at-least-once with idempotent terminals, not
at-most-once.

**In practice**: durable outbox, at-least-once drain, per-channel idempotency on `IdemKey`. A crash
anywhere leaves durable work, never a silent gap.

## VI. Terminal and retryable are different, and both are explicit

A suppressed address is a correct outcome. A provider timeout is a transient failure. Conflating them
produces either infinite retries against a dead address, or a permanent give-up on a recoverable one.

**In practice**: `DeliveryResult` distinguishes `Delivered`, `Retry`, `Suppressed` and `Rejected`;
a finished delivery's `outcome` is a constrained column distinct from its free-text detail, and only
genuine failures become dead letters.

## VII. Fast paths may not hold correctness

A cache, a pre-check or a pub/sub message may make the common case faster. None of them may be the
only thing standing between a notification and a duplicate, or between a recipient and an accurate
badge. Every one must be reconstructible from durable state.

**In practice**: the Redis pre-check is read before the transaction and written only after commit, so
it can miss but never falsely hit; the unread count is recomputed from rows; a lost pub/sub nudge
self-heals on reconnect.

## VIII. Opaque identity, permanently

The engine does not know what a recipient is. Not a user, not an account — an opaque `{kind, id}`. The
moment it knows, re-anchoring becomes a migration and the engine stops being reusable, which was the
entire reason it was extracted.

**In practice**: `Recipient`, `Tenant` and `Realm` are never parsed, ranked or special-cased.

## IX. State the scope honestly

A notification system adopted for a job it cannot do fails in production, at the point someone was
relying on it. So the README names what this is not, and the roadmap names the tools that are better
for adjacent problems.

**In practice**: [ROADMAP.md](../../ROADMAP.md) recommends managed alternatives for most teams, and
says which phases are designed rather than built.

## X. Record the failure mode, not the preference

A decision is documented with what goes wrong without it. If no failure mode can be named, the choice
is a preference and belongs in a style guide, not a specification.

**In practice**: every entry in
[design-decisions.md](../../specs/001-notification-core/design-decisions.md) has a
"what goes wrong without it" paragraph.
