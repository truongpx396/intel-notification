#!/bin/sh
# Local development and the e2e suite: when NOTIFY_SEED_SMTP_ADDR is set, a provider row for the local
# realm's email channel that sends through that SMTP relay (Mailpit, in the e2e profile). A real deployment
# manages channel_providers itself, and a credential is only ever a reference there (D31).
set -eu
[ -n "${NOTIFY_SEED_SMTP_ADDR:-}" ] || exit 0
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<SQL
INSERT INTO channel_providers (realm, channel, provider, priority, config, secret_ref)
VALUES ('${NOTIFY_SEED_REALM:-local}', 'email', 'smtp', 0,
        '{"addr": "${NOTIFY_SEED_SMTP_ADDR}", "from": "Notify <notify@example.test>"}'::jsonb, '');
SQL
