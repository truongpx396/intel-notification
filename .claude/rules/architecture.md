---
paths:
  - "domain/**"
  - "ports/**"
  - "app/**"
  - "adapters/**"
  - "config.go"
  - ".go-arch-lint.yml"
  - ".golangci.yml"
---

# Hexagon rules

Enforced by `make arch-lint` (the graph) and `make lint` (depguard). Both run in CI and in the
commit gate, so a violation is a build failure, not a review comment.

- **Dependency direction.** `domain` imports nothing from this module. `ports` → `domain`. Driven
  adapters → `domain`, `ports`, `migrations`. `app` → `config`, `domain`, `ports` only. An adapter must
  never import `app`: it implements a port, it does not call a use-case. `cmd` is the only place that
  wires concrete implementations together.
- **No infrastructure in the core.** `domain`, `ports` and `app` may not import pgx, go-redis, nats,
  grpc, `net/http` or testcontainers. Transport lives in `adapters/driving`, storage in `adapters/driven`.
- **Never import a host product.** Nothing may import `github.com/aisat/...`. The module must build with
  no host present (constitution III).
- **Layers not built yet are commented out** in `.go-arch-lint.yml` (`app`, `adapters/driving`, `api`,
  `cmd`). When you create one of those directories, uncomment its `components` and `deps` lines in the
  same change. go-arch-lint refuses a component whose directory does not exist, which is why they wait.

## Behaviour the code must keep

- **Identity is opaque.** `Recipient`, `Tenant` and `Realm` are `{kind, id}` pairs. Never parse, rank,
  compare by prefix or special-case one (constitution VIII).
- **No `if channel == ...`.** Fan-out reads a channel's declared capabilities through the registry. Adding
  a channel must not edit the fan-out loop (constitution IV).
- **Every derived key goes through `domain/keys.go`**, over `Canonical`, so two call sites cannot derive
  the same key differently. A new key gets a frozen test vector.
- **Terminal and retryable are different results.** `Delivered`, `Retry`, `Suppressed`, `Rejected`; a
  dead letter is for genuine failures only (constitution VI).
- **A fast path never holds correctness.** A cache or pre-check may be read before a transaction and
  written only after commit; it must be reconstructible from durable state (constitution VII).
- **Queue state transitions are SQL in the Go adapter**, not PL/pgSQL functions (D37), proven against a
  real PostgreSQL. Writes by a worker are fenced by the lease token; keep the fence in any new write.
