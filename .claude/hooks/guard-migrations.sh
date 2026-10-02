#!/usr/bin/env bash
# PreToolUse on Edit|Write: a merged migration is immutable once the first release is tagged
# (migrations/README.md). Static path rules live in settings.json; this is a hook because
# "has a release been tagged yet" is a question about git, not about a path.
#
# Before the first v* tag, migrations are rewritten in place, so this allows everything.
# The extraction-baseline-* tag is not a release and does not count.
set -uo pipefail
command -v jq >/dev/null || exit 0

file=$(jq -r '.tool_input.file_path // empty')
root=${CLAUDE_PROJECT_DIR:-$(git rev-parse --show-toplevel 2>/dev/null)} || exit 0
case "$file" in "$root"/migrations/*.sql) ;; *) exit 0 ;; esac

git -C "$root" tag -l 'v[0-9]*' | grep -q . || exit 0

rel=${file#"$root"/}
for ref in main origin/main; do
  if git -C "$root" cat-file -e "$ref:$rel" 2>/dev/null; then
    jq -n --arg rel "$rel" --arg ref "$ref" '{hookSpecificOutput: {
      hookEventName: "PreToolUse",
      permissionDecision: "deny",
      permissionDecisionReason: "\($rel) is already merged (it exists on \($ref)) and a release is tagged, so it is immutable. Add a new migration with the next sequential number instead (migrations/README.md)."}}'
    exit 0
  fi
done
exit 0
