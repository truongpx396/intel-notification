---
paths:
  - "specs/**"
  - "docs/**"
  - ".specify/**"
  - "README.md"
  - "ROADMAP.md"
  - "PROVENANCE.md"
  - "migrations/README.md"
---

# Specs and docs

The specification is the primary artifact, and where code and spec disagree one of them is a bug.
Order of authority (`.specify/README.md`): constitution → `specs/*/contracts/` → `spec.md` →
`design-decisions.md` → code.

- **Constitution and contracts need deliberate edits.** Edits to `.specify/memory/constitution.md` and
  `specs/*/contracts/**` prompt for confirmation. A contract change needs the requirement it serves, the
  failure mode it prevents, and either a new `verify-schema.sql` assertion or a test at the right layer.
  A change with none of those is a preference, and preferences do not go in contracts.
- **Record the failure mode, not the preference.** A design decision states what goes wrong without it
  (constitution X). If you cannot name one, it belongs in a style guide.
- **Task IDs are stable; list order is the build order.** Other files cite tasks by number (T026, T032,
  T044...). Never renumber. A new task takes an `a` suffix.
- **A task box is ticked only when the definition of done holds** (`tasks.md`, "How a task is worked"):
  the red test was seen failing and the failure recorded, `make ci` is green, each guarantee was
  mutation-checked with a row in `docs/mutations.md`, the refactor pass is done, and the requirement has
  its proving test in Traceability.
- **State status honestly.** README and ROADMAP say what is built, what is designed and what is absent
  (constitution IX). When a stage lands, update those sentences and the counts that quote the specs
  (decisions, invariants, tasks) in the same change.
- **Relative links must resolve.** `make docs-links` fails on a dangling one, in CI and in the commit
  gate. Run it after moving or renaming a file.
