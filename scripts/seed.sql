-- seed.sql — a minimal, readable dataset for local development.
--
-- Two recipients in one tenant plus one in a second realm, which is the shape that
-- makes isolation bugs visible immediately: if a query ever returns u2's row for
-- u1, or crosses the realm boundary, the seed shows it without a test harness.
--
-- Apply AFTER the migrations:
--   psql "$NOTIFY_PG_DSN" -f scripts/seed.sql

BEGIN;

INSERT INTO notifications (id, realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
                           topic, priority, title, body, payload, idem_key, created_at)
VALUES
  ('018f1000-0000-7000-8000-000000000001','local','workspace','w1','user','u1',
   'ingestion_complete','info','Your upload finished',
   'handbook.pdf is ready to search.', '{"doc_id":"d1"}', 'ingest:d1:complete', now()),

  ('018f1000-0000-7000-8000-000000000002','local','workspace','w1','user','u1',
   'credit_exhausted','critical','Credits exhausted',
   'Workspace w1 has no remaining credits.', '{}', 'credit:w1:exhausted', now()),

  ('018f1000-0000-7000-8000-000000000003','local','workspace','w1','user','u2',
   'invite_received','info','You were invited',
   'You have been invited to workspace w1.', '{"invite_id":"i9"}', 'invite:i9:received', now()),

  -- A second realm reusing an identical idem_key and recipient id on purpose:
  -- it must coexist, which is what D1 buys.
  ('018f1000-0000-7000-8000-000000000004','other-product','workspace','w1','user','u1',
   'ingestion_complete','info','A different product',
   'Same recipient id, same idem_key, different realm.', '{}', 'ingest:d1:complete', now());

INSERT INTO notify_idem (realm, recipient_kind, recipient_id, idem_key, notification_id)
VALUES
  ('local','user','u1','ingest:d1:complete','018f1000-0000-7000-8000-000000000001'),
  ('local','user','u1','credit:w1:exhausted','018f1000-0000-7000-8000-000000000002'),
  ('local','user','u2','invite:i9:received','018f1000-0000-7000-8000-000000000003'),
  ('other-product','user','u1','ingest:d1:complete','018f1000-0000-7000-8000-000000000004');

-- u1 has turned email off for ingestion but left it on everywhere else.
INSERT INTO notification_preferences (realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
                                      topic, channel, enabled)
VALUES ('local','workspace','w1','user','u1','ingestion_complete','email', false);

-- u1 keeps quiet hours. credit_exhausted is critical, so it delivers anyway (D9).
INSERT INTO notification_schedules (realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
                                    quiet_start, quiet_end, timezone, digest_window)
VALUES ('local','workspace','w1','user','u1','22:00','07:00','Europe/London', interval '15 minutes');

INSERT INTO channel_quotas (realm, tenant_kind, tenant_id, channel, max_per_hour, max_per_day)
VALUES ('local','workspace','w1','email', 100, 1000);

COMMIT;
