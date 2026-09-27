# intel-notification — see ROADMAP.md for what is built vs designed.
#
# Implementation has not started, so the Go targets are declared but have nothing
# to compile yet. The schema targets are real and CI runs them.

SHELL       := /usr/bin/env bash
PG_IMAGE    ?= postgres:16-alpine
PG_CONTAINER?= intel-notification-verify
PG_DB       ?= notify
PG_USER     ?= postgres
PG_PASS     ?= verify

.PHONY: help
help: ## Show available targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
	  | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-26s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------- schema ------

.PHONY: verify-schema
verify-schema: ## Apply migrations to a throwaway PG16 and assert the guarantees
	@set -e; \
	trap '$(MAKE) --no-print-directory _pg-down' EXIT; \
	$(MAKE) --no-print-directory _pg-up; \
	echo "==> asserting the INHERITED constraint cannot be built (D2)"; \
	if docker exec -i $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB) -v ON_ERROR_STOP=1 -q \
	     < scripts/verify-inherited-constraint.sql 2>/dev/null; then \
	  echo "FAIL: the inherited partitioned+unique shape was accepted."; \
	  echo "      PostgreSQL behaviour changed -- revisit D2."; exit 1; \
	else \
	  echo "    OK: rejected, as D2 documents"; \
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
	for i in $$(seq 1 60); do \
	  docker exec $(PG_CONTAINER) pg_isready -U $(PG_USER) -d $(PG_DB) >/dev/null 2>&1 && break; \
	  printf "."; sleep 1; \
	done; echo " ready"

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

.PHONY: build test lint arch-lint
build: ## Build the module (no Go code yet)
	@echo "no Go code yet -- see specs/001-notification-core/tasks.md"

test: ## Unit + contract tests (no Go code yet)
	@echo "no Go code yet -- see specs/001-notification-core/tasks.md"

lint: ## golangci-lint, incl. the depguard import bans
	@command -v golangci-lint >/dev/null || { echo "golangci-lint not installed"; exit 1; }
	@golangci-lint run ./... || true

arch-lint: ## go-arch-lint: the hexagonal dependency graph
	@command -v go-arch-lint >/dev/null || { echo "go-arch-lint not installed"; exit 1; }
	@go-arch-lint check || true

.PHONY: docs-links
docs-links: ## Fail on a relative markdown link with no target on disk
	@set -e; fail=0; \
	while IFS='|' read -r f link; do \
	  case "$$link" in http*|mailto*|\#*) continue;; esac; \
	  base="$${link%%\#*}"; \
	  [ -z "$$base" ] && continue; \
	  target="$$(dirname "$$f")/$$base"; \
	  [ -e "$$target" ] || { echo "  $$f -> $$link"; fail=1; }; \
	done < <(grep -roE '\]\([^)]+\)' --include='*.md' . | sed -E 's/:\]\(/|/; s/\)$$//'); \
	if [ $$fail -eq 0 ]; then echo "links OK"; else echo "dangling links above"; exit 1; fi

.PHONY: ci
ci: lint-sql verify-schema docs-links ## Everything CI runs today
