# Reference UI: Notifications (Inbox & Preferences)

> The reference inbox and preference surface for this engine — what a host builds on
> top of the [REST contract](../../specs/001-notification-core/contracts/rest-api.md).
> The top-bar **bell dropdown** is the quick view; this screen is the full history and
> delivery-preference control surface. Covers NR-001, NR-002, NR-008, NR-011, NR-024.
>
> Carried from the product this engine was extracted from, where it was a page override
> in that product's design system; see [PROVENANCE.md](../../PROVENANCE.md).
>
> **The topic list below is that product's**, kept deliberately as a worked example
> rather than genericized into nothing. Your topics will differ; the structure — a bell
> quick-view, a full inbox, and a per-`(topic, channel)` preference table — transfers.

## Purpose
The recipient's home for everything that concerns them in this workspace: a real-time
in-app inbox (ingestion done/failed, invites, credit warnings/exhaustion, a long-horizon
task halting at its cost cap, **a human-in-the-loop approval awaiting the viewer** — an
an agent action or a paused long-horizon step awaiting them — a document shared
or clearance changed, a new member joining for admins, and admin broadcasts), plus
per-category control over how each is delivered
(in-app and/or email). Reinforces that every notification is strictly scoped to the
viewer within this workspace and never leaks across members or workspaces.

## Layout
- App shell; reached from the top-bar **bell** and the **Notifications** sidebar
  nav item (both badged with the unread count). Active nav = **Notifications**.
- Two tabbed sections: **Inbox** and **Preferences**.
- Header strip: title + **unread count** (Fira Code) + **Mark all read** button + a
  scope note ("Only you can see these — never shared across members or workspaces").

## Bell dropdown (top-bar quick view)
- Opens a compact panel: the latest ~10 notifications, newest first, with the unread
  count and **Mark all read**.
- Live: new items prepend and the badge increments over the existing stream (SSE) with
  no page reload (NR-024); marking read decrements it immediately and persists.
- Footer link **View all** → this screen's Inbox tab.

## Inbox tab (NR-001, NR-002, NR-024)
- **Filters:** unread-only toggle, category multi-select, and a search box.
- **Notification list:** uses the `Notification item` pattern — category icon + accent,
  title + one-line body, relative timestamp, unread dot. Unread rows sit on
  `--color-surface-2`; read rows on the base surface.
- **Priority:** `urgent` items (e.g., credits exhausted, task halted) carry a red accent
  and may have surfaced as a toast when live.
- **Deep-link:** clicking a row marks it read and navigates to the originating resource
  via its payload (e.g., the document, the credits page, the invite, the task run). An
  `approval_requested` row deep-links to the **pending approval card** — the paused
  long-horizon run on Agents, or the inline gate in Chat — so the viewer can act on it.
- **Per-item actions:** mark read/unread; the list supports **Mark all read**.
- **Grouping:** day separators ("Today", "Yesterday", date) for scannability.

## Preferences tab (NR-011, NR-015)
- A table: one row per **category**, two channel toggles — **In-app** and **Email** —
  each independent.
- Categories: Ingestion, Invites & membership, Credits, **Billing & payments** *(Phase 2)*,
  Agent / long-horizon tasks, **Approvals** (human-in-the-loop gates awaiting you —
  in-app **and** email on by default, since a paused action is actionable and
  time-sensitive), Shares & clearance, Admin broadcasts.
- **Billing & payments** *(Phase 2)* covers `payment_succeeded`, `payment_failed`,
  `subscription_renewed`, `subscription_canceled`. It is delivered to **owners and admins
  only** — members neither cause nor can act on a billing event, so the row states its
  restricted audience rather than appearing as a preference everyone can toggle.
- Disabling a channel for a category stops delivery on that channel only; disabling both
  stops all delivery for that category (still recorded server-side, just not surfaced).
- **One exception:** the **email** channel for `payment_failed` is rendered checked and
  disabled with an explanatory tooltip. A dunning notice that nobody sees ends in lost
  service for the whole workspace, which is a worse outcome than an unwanted email. Any
  other non-disableable channel needs the same standard of justification — surface the
  reason in the UI, never silently ignore the toggle.
- Note under the table: "Email is best-effort and provider-agnostic; transient failures
  are retried and parked, never silently dropped (admins can inspect the dead-letter
  path)." In-app delivery is real-time and independent of email.
- Email channel rows show the verified delivery address (read-only here).

## States to show
- Inbox with a mix of read/unread across several categories, including one `urgent`.
- Empty inbox ("You're all caught up") with the badge hidden.
- Preferences with at least one category's email channel disabled.
- Live arrival: a new unread item prepends and the bell badge ticks up.

## Don'ts
- Don't show notifications belonging to another member or another workspace — ever,
  regardless of clearance (NR-008, NS-001). Scope is enforced at the data layer; the UI must
  never assume otherwise.
- Don't block or hide an in-app notification because its email failed; the two channels
  are independent.
- Don't use emojis as category icons — use the shared SVG icon set.
- Don't reset unread state on reload; read state is durable (NR-001).
- Don't deep-link to a resource above the viewer's clearance.
- Don't show billing events to members. Payment amounts and card failures are
  owner/admin information, and the Credits screen already gates its billing section the
  same way.

---

## Phase 2+ affordances on this screen

| Affordance | Phase | Backing contract |
|---|---|---|
| Billing category (inbox filter, preference row, sample item) | 2 | a `TopicRegistry` entry — no schema change ([D3](../../specs/001-notification-core/design-decisions.md#d3)) |

Two further categories are anticipated but **not yet mocked**, because their parent
features are unscheduled: mind-map generation completion (Workspace Mind Map) and
mirrored-artifact staleness (`knowledge.staleness.tick`, Enterprise Knowledge Layer).
Both slot into this table as ordinary rows when those features are planned — the
preference model needs no change to absorb them. See
[ROADMAP.md](../../ROADMAP.md).
