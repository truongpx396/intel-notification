#!/usr/bin/env bash
# SessionStart: say once, up front, what this machine cannot run, instead of letting Claude
# find out from a Testcontainers stack trace. Silent when everything is available.
notes=()

if ! command -v docker >/dev/null || ! docker info >/dev/null 2>&1; then
  notes+=("Docker is not running: make verify-schema, test-integration, bench, soak and e2e will fail. make test, lint, arch-lint, lint-sql and docs-links do not need it.")
fi

missing=()
for tool in jq goimports golangci-lint go-arch-lint; do
  command -v "$tool" >/dev/null || missing+=("$tool")
done
if [ ${#missing[@]} -gt 0 ]; then
  notes+=("Not installed: ${missing[*]}. The hooks that use them skip themselves, and make lint / arch-lint fail until they are installed (install lines are in the Makefile).")
fi

[ ${#notes[@]} -gt 0 ] && printf '%s\n' "${notes[@]}"
exit 0
