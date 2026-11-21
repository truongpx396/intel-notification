#!/bin/sh
# Local development and the e2e suite: the topics the REST specs send on, and the copy of the ones that email,
# registered for the local realm as a producer would register them through the Catalog (NR-017, NR-019). A real
# deployment registers its own.
set -eu
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<SQL
INSERT INTO notification_topics (realm, topic, default_channels, fallback, default_priority, essential)
VALUES ('${NOTIFY_SEED_REALM:-local}', 'ingestion_complete', ARRAY['in_app'], ARRAY['email'], 'info', false),
       ('${NOTIFY_SEED_REALM:-local}', 'invite_received',    ARRAY['in_app','email'], '{}', 'info', false),
       ('${NOTIFY_SEED_REALM:-local}', 'credit_exhausted',   ARRAY['in_app','email'], ARRAY['sms'], 'critical', true)
ON CONFLICT DO NOTHING;

INSERT INTO notification_templates (realm, template_ref, channel, locale, version, subject, body, active)
VALUES ('${NOTIFY_SEED_REALM:-local}', 'invite_received',    'email',  'en', 1, 'You have been invited', 'Invitation {{ .label }}', true),
       ('${NOTIFY_SEED_REALM:-local}', 'invite_received',    'in_app', 'en', 1, 'You have been invited', 'Invitation {{ .label }}', true),
       ('${NOTIFY_SEED_REALM:-local}', 'ingestion_complete', 'in_app', 'en', 1, 'Your upload finished',   '{{ .label }}', true),
       ('${NOTIFY_SEED_REALM:-local}', 'credit_exhausted',   'in_app', 'en', 1, 'Credits exhausted',      '{{ .label }}', true),
       ('${NOTIFY_SEED_REALM:-local}', 'credit_exhausted',   'email',  'en', 1, 'Credits exhausted',      '{{ .label }}', true)
ON CONFLICT DO NOTHING;
SQL
