-- seed.sql — a minimal, readable dataset for local development.
--
-- Two recipients in one tenant, the same user in a second tenant, and one in a
-- second realm: the shape that makes isolation bugs visible immediately. If a
-- query ever returns u2's row for u1, crosses a tenant, or crosses the realm
-- boundary, the seed shows it without a test harness.
--
-- Apply AFTER the migrations, as a role that can bypass RLS (seeding writes
-- many recipients' rows in one transaction):
--   psql "$NOTIFY_PG_DSN" -f scripts/seed.sql

BEGIN;

INSERT INTO notifications (id, realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
                           topic, priority, data, title, body, payload, idem_key, created_at)
VALUES
  ('018f1000-0000-7000-8000-000000000001','local','workspace','w1','user','u1',
   'ingestion_complete','info','{"doc_name":"handbook.pdf"}','Your upload finished',
   'handbook.pdf is ready to search.', '{"doc_id":"d1"}', 'ingest:d1:complete', now()),

  ('018f1000-0000-7000-8000-000000000002','local','workspace','w1','user','u1',
   'credit_exhausted','critical','{"workspace":"w1"}','Credits exhausted',
   'Workspace w1 has no remaining credits.', '{}', 'credit:w1:exhausted', now()),

  ('018f1000-0000-7000-8000-000000000003','local','workspace','w1','user','u2',
   'invite_received','info','{"workspace":"w1"}','You were invited',
   'You have been invited to workspace w1.', '{"invite_id":"i9"}', 'invite:i9:received', now()),

  -- The same user in a second tenant, reusing an idem_key on purpose: it must
  -- coexist, which is what putting the tenant in the key buys (D18).
  ('018f1000-0000-7000-8000-000000000004','local','workspace','w2','user','u1',
   'credit_exhausted','critical','{"workspace":"w2"}','Credits exhausted',
   'Workspace w2 has no remaining credits.', '{}', 'credit:w1:exhausted', now()),

  -- A second realm reusing an identical idem_key and recipient id on purpose:
  -- it must coexist, which is what D1 buys.
  ('018f1000-0000-7000-8000-000000000005','other-product','workspace','w1','user','u1',
   'ingestion_complete','info','{}','A different product',
   'Same recipient id, same idem_key, different realm.', '{}', 'ingest:d1:complete', now());

INSERT INTO notify_idem (realm, tenant_kind, tenant_id, recipient_kind, recipient_id, idem_key,
                         notification_id, notification_created_at)
SELECT realm, tenant_kind, tenant_id, recipient_kind, recipient_id, idem_key, id, created_at
  FROM notifications;

-- u1 has turned email off for ingestion but left it on everywhere else.
INSERT INTO notification_preferences (realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
                                      topic, channel, enabled)
VALUES ('local','workspace','w1','user','u1','ingestion_complete','email', false);

-- u1 keeps quiet hours. credit_exhausted is critical, so it delivers anyway (D9).
INSERT INTO notification_schedules (realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
                                    quiet_start, quiet_end, timezone, digest_window)
VALUES ('local','workspace','w1','user','u1','22:00','07:00','Europe/London', interval '15 minutes');

-- u1 has one email and two phones; a push goes to both phones as two deliveries (D25).
INSERT INTO recipient_addresses (realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
                                 channel, address_key, value, locale)
VALUES ('local','workspace','w1','user','u1','email','seed-email-1','u1@example.com','en-GB'),
       ('local','workspace','w1','user','u1','push','seed-push-1','fcm-token-phone','en-GB'),
       ('local','workspace','w1','user','u1','push','seed-push-2','apns-token-tablet','en-GB');

-- The topic catalog service mode reads (D31).
INSERT INTO notification_topics (realm, topic, default_channels, fallback, default_priority, essential)
VALUES ('local','ingestion_complete', ARRAY['in_app'], ARRAY['email'], 'info', false),
       ('local','invite_received', ARRAY['in_app','email'], '{}', 'info', false),
       ('local','credit_exhausted', ARRAY['in_app','email'], ARRAY['sms'], 'critical', true);

-- A realm-wide default quota, and a tighter one for w1 (D24).
INSERT INTO channel_quotas (realm, tenant_kind, tenant_id, channel, max_per_hour, max_per_day)
VALUES ('local','','','email', 1000, 10000),
       ('local','workspace','w1','email', 100, 1000);

COMMIT;
