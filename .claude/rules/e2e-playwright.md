---
paths:
  - "e2e/**"
---

# Playwright end-to-end suite

Written against the REST contract (`specs/001-notification-core/contracts/rest-api.md`) before the
service exists; see [docs/testing.md](../../docs/testing.md#end-to-end-playwright).

- **Node 24** (`e2e/.nvmrc`, `engines`). `nvm use` inside `e2e/` first; a different Node fails in
  confusing ways. Check your changes with `make e2e-list` (typecheck + list), which needs no stack.
- **`*.api.spec.ts`** run in the `api` project over HTTP only. **`*.browser.spec.ts`** run in Chromium and
  are for what needs a real browser (the SSE stream through `EventSource`).
- **Isolation by data.** Each test builds its own tenants, recipients and idempotency keys through
  `support/fixtures.ts`, so the suite runs `fullyParallel` against one stack. Keep the "same recipient id
  in two tenants" shape: it is the one that exposes an isolation bug.
- **The run is the host.** Global setup generates a signing key, serves its JWKS and mints recipient
  tokens. Never commit a key or token. Drive producers through the operator broadcast route so no test
  needs a gRPC client; read outgoing mail back from Mailpit.
- **Specs stay `test.describe.fixme` until their service task lands.** Each one opens with a requirement
  comment (NS-001...) and a `Pending:` line naming the tasks that enable it (T044, T049; T047a switches
  the suite on). Keep both when adding a spec, and do not delete a spec to make CI quiet.
- **Edit `package.json`, not `package-lock.json`.** The lockfile is regenerated with `npm`, never by hand.
