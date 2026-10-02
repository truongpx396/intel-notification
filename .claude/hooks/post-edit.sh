#!/usr/bin/env bash
# PostToolUse on Edit|Write: keep what Claude writes in the shape CI expects.
#
# A formatter must never block work, so Go formatting fails open. The migration lint is a
# gate: exit 2 feeds its output back to Claude.
set -uo pipefail
command -v jq >/dev/null || exit 0

file=$(jq -r '.tool_response.filePath // .tool_input.file_path // empty')
[ -n "$file" ] && [ -f "$file" ] || exit 0
root=${CLAUDE_PROJECT_DIR:-$(git rev-parse --show-toplevel 2>/dev/null)} || exit 0

case "$file" in
  *.go)
    # The same formatters and local-import prefix as .golangci.yml, so `make lint` never
    # has a formatting finding to report. A file that does not parse yet (mid-edit, or a
    # red test under construction) is left alone.
    if command -v goimports >/dev/null; then
      goimports -local github.com/truongpx396/intel-notification -w "$file" 2>/dev/null
    else
      gofmt -w "$file" 2>/dev/null
    fi
    ;;
  "$root"/migrations/*.sql)
    if ! out=$(cd "$root" && make --no-print-directory lint-sql 2>&1); then
      printf '%s\n' "$out" >&2
      exit 2
    fi
    jq -n '{hookSpecificOutput: {hookEventName: "PostToolUse", additionalContext:
      "Migration edited and `make lint-sql` passes. Not done yet: run `make verify-schema` (needs Docker); a new guarantee needs an assertion in scripts/verify-schema.sql that RAISEs on failure; a new table holding recipient_id needs notify_apply_recipient_scope() or an entry in the worker-only list in verify-schema TEST 5; keep the table in migrations/README.md current."}}'
    ;;
esac
exit 0
