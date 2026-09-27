# Spec-driven workflow

This repository carries its specification as the primary artifact. The design was written before any
code, extracted with its history from
[aisat-intel](https://github.com/truongpx396/aisat-intel), and is normative: where code and spec
disagree, one of them is a bug and the spec says which.

| Directory | Role |
|---|---|
| [`specs/001-notification-core/`](../specs/001-notification-core/) | Phase 1 — normative, implementation not started |
| [`specs/002-escalation-workflows/`](../specs/002-escalation-workflows/) | Phase 2 — designed, not started |
| [`memory/constitution.md`](memory/constitution.md) | The principles every change is checked against |

## Order of authority

1. `memory/constitution.md` — a change that violates a principle is wrong even if the spec permits it.
2. `specs/*/contracts/` — the interfaces. Breaking one breaks every host.
3. `specs/*/spec.md` — requirements and success criteria.
4. `specs/*/design-decisions.md` — why, with the failure mode each choice prevents.
5. Code.

## Changing the design

A change to a contract needs: the requirement it serves, the failure mode it prevents, and either a
new assertion in `scripts/verify-schema.sql` or a new contract test. A change with none of those is a
preference, and preferences do not go in contracts.
