#!/usr/bin/env bash
# PreToolUse on Bash: before Claude runs `git commit`, run the fast, Docker-free half of CI.
#
# docs/testing.md and tasks.md judge CI on the branch head, and the task lists commit in the
# order test: (red), feat:/fix: (green), refactor:. A red commit has a failing test by design,
# so this gate deliberately does NOT run `make test`: it checks that the tree is well-formed,
# not that it is green. `make ci` is the full bar.
set -uo pipefail
command -v jq >/dev/null || exit 0

cmd=$(jq -r '.tool_input.command // empty')
printf '%s' "$cmd" | grep -Eq '(^|[;&|[:space:]])git[[:space:]]+commit([[:space:]]|$)' || exit 0

root=${CLAUDE_PROJECT_DIR:-$(git rev-parse --show-toplevel 2>/dev/null)} || exit 0
cd "$root" || exit 0

notes=()
targets=(lint-sql docs-links build)
if command -v golangci-lint >/dev/null; then targets+=(lint); else notes+=("golangci-lint is not installed, so make lint was skipped"); fi
if command -v go-arch-lint >/dev/null; then targets+=(arch-lint); else notes+=("go-arch-lint is not installed, so make arch-lint was skipped"); fi

# Migrations are the one place CI's schema job catches what the gate above cannot. Run it
# when SQL changed and Docker is there; say so when it cannot.
if git status --porcelain | grep -Eq '(migrations|scripts)/.*\.sql$'; then
  if docker info >/dev/null 2>&1; then
    targets+=(verify-schema)
  else
    notes+=("migrations or schema SQL changed but Docker is not running, so make verify-schema was skipped; CI will run it")
  fi
fi

unformatted=$(gofmt -l . 2>/dev/null)
if [ -n "$unformatted" ]; then
  printf 'Commit blocked: these files are not gofmt-formatted (gofmt -w them):\n%s\n' "$unformatted" >&2
  exit 2
fi

if ! out=$(make --no-print-directory "${targets[@]}" 2>&1); then
  printf 'Commit blocked: the fast CI gate failed (make %s).\n\n' "${targets[*]}" >&2
  printf '%s\n' "$out" | tail -n 60 >&2
  exit 2
fi

if [ ${#notes[@]} -gt 0 ]; then
  jq -n --arg m "commit gate: $(IFS=';'; printf '%s' "${notes[*]}")" '{systemMessage: $m}'
fi
exit 0
