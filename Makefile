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

.PHONY: build test test-integration lint arch-lint
build: ## Build every package
	go build ./...

test: ## Unit tests: no Docker, no network
	$(GOTEST) ./...

test-integration: ## Integration tests against PostgreSQL via Testcontainers (needs Docker)
	$(GOTEST) -tags integration ./...

lint: ## golangci-lint: depguard boundaries, parallel-test rules, formatting
	@command -v golangci-lint >/dev/null || { echo "golangci-lint not installed: https://golangci-lint.run/docs/welcome/install/"; exit 1; }
	golangci-lint run ./...

arch-lint: ## go-arch-lint: the hexagonal dependency graph
	@command -v go-arch-lint >/dev/null || { echo "go-arch-lint not installed: go install github.com/fe3dback/go-arch-lint@v1.19.0"; exit 1; }
	go-arch-lint check

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
ci: lint-sql verify-schema docs-links build test test-integration lint arch-lint e2e-list ## Everything CI runs today
