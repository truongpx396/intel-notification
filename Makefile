# intel-notification — see ROADMAP.md for what is built vs designed, and
# docs/testing.md for what each test layer covers.

SHELL       := /usr/bin/env bash
PG_IMAGE    ?= postgres:16-alpine
PG_CONTAINER?= intel-notification-verify
PG_DB       ?= notify
PG_USER     ?= postgres
PG_PASS     ?= verify

.PHONY: help
help: ## Show available targets
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) \
	  | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-26s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------- schema ------

.PHONY: verify-schema
verify-schema: ## Apply migrations to a throwaway PG16 and assert the guarantees
	@set -e; \
	trap '$(MAKE) --no-print-directory _pg-down' EXIT; \
	$(MAKE) --no-print-directory _pg-up; \
	echo "==> asserting the INHERITED constraint cannot be built (D2)"; \
	if d2out=$$(docker exec -i $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB) -v ON_ERROR_STOP=1 -q \
	              < scripts/verify-inherited-constraint.sql 2>&1); then \
	  echo "FAIL: the inherited partitioned+unique shape was ACCEPTED."; \
	  echo "      PostgreSQL behaviour changed -- revisit D2."; exit 1; \
	elif ! printf '%s' "$$d2out" | grep -q 'must include all partitioning columns'; then \
	  echo "FAIL: it failed, but NOT for the reason D2 claims. Actual error:"; \
	  printf '%s\n' "$$d2out" | sed 's/^/      /'; exit 1; \
	else \
	  echo "    OK: rejected with the partitioning-columns error D2 documents"; \
	fi; \
	echo "==> applying migrations"; \
	for f in migrations/*.sql; do \
	  printf '    %s' "$$f"; \
	  docker exec -i $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB) -v ON_ERROR_STOP=1 -q < "$$f"; \
	  echo "  OK"; \
	done; \
	echo "==> asserting the guarantees"; \
	docker exec -i $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB) -v ON_ERROR_STOP=1 \
	  < scripts/verify-schema.sql

.PHONY: _pg-up
_pg-up:
	@docker rm -f $(PG_CONTAINER) >/dev/null 2>&1 || true
	@docker run -d --name $(PG_CONTAINER) -e POSTGRES_PASSWORD=$(PG_PASS) \
	   -e POSTGRES_DB=$(PG_DB) $(PG_IMAGE) >/dev/null
	@printf "==> waiting for %s" "$(PG_IMAGE)"; \
	ready=0; \
	for i in $$(seq 1 90); do \
	  if docker exec $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB) -tAc 'select 1' >/dev/null 2>&1; then \
	    ready=1; break; \
	  fi; \
	  printf "."; sleep 1; \
	done; \
	if [ $$ready -ne 1 ]; then echo " FAILED: $(PG_DB) never became reachable"; exit 1; fi; \
	echo " ready"

.PHONY: _pg-down
_pg-down:
	@docker rm -f $(PG_CONTAINER) >/dev/null 2>&1 || true

.PHONY: lint-sql
lint-sql: ## Migration hygiene: transactional, commented, sequentially named
	@set -e; fail=0; \
	for f in migrations/*.sql; do \
	  grep -q '^BEGIN;' "$$f" || { echo "$$f: missing BEGIN;"; fail=1; }; \
	  grep -q '^COMMIT;' "$$f" || { echo "$$f: missing COMMIT;"; fail=1; }; \
	  head -1 "$$f" | grep -q '^--' || { echo "$$f: missing header comment"; fail=1; }; \
	done; \
	[ $$fail -eq 0 ] && echo "migrations OK" || exit 1

# ------------------------------------------------------------------- go -------
# Tests run shuffled and under the race detector: parallel tests (docs/testing.md)
# are only worth having if order-dependence and data races fail the build.
GOTEST := go test -race -shuffle=on -count=1

.PHONY: build test test-integration test-provider lint arch-lint
build: ## Build every package
	go build ./...

test: ## Unit tests: no Docker, no network
	$(GOTEST) ./...

test-integration: ## Integration tests against PostgreSQL via Testcontainers (needs Docker; PG_IMAGE= picks the major)
	PGTEST_IMAGE=$(PG_IMAGE) $(GOTEST) -tags integration ./...

# The checkpoint counters moved views in PostgreSQL 17, so the code has a branch for each major
# and only a run on each holds both. CI runs both; this is the second one, by hand. The expected
# major is stated apart from the image, so a run that did not start 17 fails instead of passing.
PG_NEXT_IMAGE ?= postgres:17-alpine
PG_NEXT_MAJOR ?= 17
.PHONY: test-pg-next
test-pg-next: ## verify-schema and the integration tests on PostgreSQL 17 (needs Docker)
	$(MAKE) verify-schema test-integration PG_IMAGE=$(PG_NEXT_IMAGE) PGTEST_EXPECT_MAJOR=$(PG_NEXT_MAJOR)

# Not in `make ci`: it spends a real provider's sending quota and needs credentials
# (NOTIFY_PROVIDER_RESEND_API_KEY). It is the only way to show a provider dedupes.
test-provider: ## Channel suites against the email provider's sandbox (needs NOTIFY_PROVIDER_RESEND_API_KEY)
	go test -race -count=1 -tags provider -run 'Sandbox' ./adapters/driven/channel/email

lint: ## golangci-lint: depguard boundaries, parallel-test rules, formatting
	@command -v golangci-lint >/dev/null || { echo "golangci-lint not installed: https://golangci-lint.run/docs/welcome/install/"; exit 1; }
	golangci-lint run ./...

arch-lint: ## go-arch-lint: the hexagonal dependency graph
	@command -v go-arch-lint >/dev/null || { echo "go-arch-lint not installed: go install github.com/fe3dback/go-arch-lint@v1.19.0"; exit 1; }
	go-arch-lint check

# Not in `make ci`: timings are for reading, not for gating, and a run takes minutes.
# No -race: the detector slows the code being measured. PGTEST_DURABLE=1 leaves fsync on,
# because a commit that costs nothing hides what batching and group commit would save.
BENCH_N      ?= 20000
BENCH_CLAIMS ?= 1000
SOAK_DURATION ?= 10m

.PHONY: bench
bench: ## Queue throughput benchmarks against PostgreSQL (needs Docker; see docs/testing.md)
	PGTEST_DURABLE=1 go test -tags integration -run '^$$' -timeout 60m \
	  -bench 'QueueCycle|AcceptProxy' -benchtime=$(BENCH_N)x ./adapters/driven/postgres
	PGTEST_DURABLE=1 go test -tags integration -run '^$$' -timeout 60m \
	  -bench 'ClaimLatencyWithBacklog' -benchtime=$(BENCH_CLAIMS)x ./adapters/driven/postgres

# SOAK_RATE, SOAK_WORKERS, SOAK_HOLD and the rest are read from the environment (see
# queue_soak_integration_test.go). -timeout 0: a soak may run for hours.
.PHONY: soak
soak: ## Sustained-load soak of the queue for SOAK_DURATION (default 10m); needs Docker
	SOAK_DURATION=$(SOAK_DURATION) PGTEST_DURABLE=1 go test -tags integration -run '^TestSoak$$' \
	  -v -count=1 -timeout 0 ./adapters/driven/postgres

# ---------------------------------------------------------------- proto -------
# api/proto is the contract's gRPC surface (T045); api/notifyv1 is what it generates, committed, so a
# host that only imports the module needs no protoc. The tools are on PATH: CI installs the versions
# below, and a different local version shows up as a change in `proto-check`.
BUF_VERSION                ?= v1.60.0
PROTOC_GEN_GO_VERSION      ?= v1.36.12
PROTOC_GEN_GO_GRPC_VERSION ?= v1.6.2
PROTO_BASE                 ?= origin/main

.PHONY: proto proto-lint proto-check proto-breaking
proto: ## Regenerate api/notifyv1 from api/proto (needs buf, protoc-gen-go, protoc-gen-go-grpc)
	@command -v buf >/dev/null || { echo "buf not installed: go install github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION)"; exit 1; }
	buf generate

proto-lint: ## buf lint on the proto
	@command -v buf >/dev/null || { echo "buf not installed: go install github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION)"; exit 1; }
	buf lint

# Regenerates in place and compares a checksum of the generated files before and after, so it needs
# no scratch directory and says the same thing in a clean checkout (CI) and in a working tree.
proto-check: ## Fail when api/notifyv1 is not what api/proto generates (regenerates it)
	@command -v buf >/dev/null || { echo "buf not installed: go install github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION)"; exit 1; }
	@before=$$(cat api/notifyv1/*.go | cksum); buf generate; after=$$(cat api/notifyv1/*.go | cksum); \
	if [ "$$before" = "$$after" ]; then echo "api/notifyv1 is current"; \
	else echo "api/notifyv1 was stale and has been regenerated: commit the result"; exit 1; fi

# A breaking change to a wire contract breaks every producer in every language, so the proto is
# compared with the base branch's. Where the base has no proto yet (the change that adds it) there is
# nothing to break, and the target says so instead of failing.
proto-breaking: ## buf breaking against PROTO_BASE (default origin/main)
	@command -v buf >/dev/null || { echo "buf not installed: go install github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION)"; exit 1; }
	@if git cat-file -e "$(PROTO_BASE):api/proto/notify/v1/notify.proto" 2>/dev/null; then \
	  buf breaking --against ".git#ref=$(PROTO_BASE)"; \
	else echo "$(PROTO_BASE) has no proto yet: nothing to break"; fi

# ------------------------------------------------------------------ e2e -------
.PHONY: e2e e2e-list
e2e: ## Playwright end-to-end tests against the compose stack (needs Docker, Node 24)
	cd e2e && npm ci && npx playwright install --with-deps chromium && npx playwright test

e2e-list: ## Typecheck the e2e suite and list its tests, without running the stack
	cd e2e && npm ci && npx tsc --noEmit && npx playwright test --list

.PHONY: docs-links
docs-links: ## Fail on a relative markdown link with no target on disk
	@set -e; fail=0; \
	while IFS='|' read -r f link; do \
	  case "$$link" in http*|mailto*|\#*) continue;; esac; \
	  base="$${link%%\#*}"; \
	  [ -z "$$base" ] && continue; \
	  target="$$(dirname "$$f")/$$base"; \
	  [ -e "$$target" ] || { echo "  $$f -> $$link"; fail=1; }; \
	done < <(grep -roE --exclude-dir=node_modules '\]\([^)]+\)' --include='*.md' . | sed -E 's/:\]\(/|/; s/\)$$//'); \
	if [ $$fail -eq 0 ]; then echo "links OK"; else echo "dangling links above"; exit 1; fi

.PHONY: ci
ci: lint-sql verify-schema docs-links proto-lint proto-check proto-breaking build test test-integration lint arch-lint e2e-list ## Everything CI runs today
