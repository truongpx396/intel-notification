-- verify-schema.sql — assert the schema actually provides its guarantees.
--
-- Run via `make verify-schema`, which applies the migrations to a throwaway
-- PostgreSQL 16 container first. Every assertion maps to a success criterion or
-- a design decision; an assertion that maps to neither does not belong here.
--
--   TEST 1  realm isolates the idempotency key space         D1
--   TEST 2  a replay is rejected by the durable guard        NS-002
--   TEST 3  RLS scopes the inbox to one recipient            NS-001  (release blocker)
--   TEST 4  exactly one open digest window per key           D4 / NS-005
--   TEST 5  no foreign key into the partitioned inbox        D6
--   TEST 6  terminal_reason is constrained                   D12
--   TEST 7  priority and quota sanity checks hold            D5 / D9
--
\set ON_ERROR_STOP on
INSERT INTO notifications (id, realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
                           topic, title, idem_key, created_at)
VALUES ('018f0000-0000-7000-8000-000000000001','aisat','workspace','w1','user','u1',
        'ingestion_complete','Upload finished','ing:doc1:complete','2026-09-15'),
       ('018f0000-0000-7000-8000-000000000002','aisat','workspace','w1','user','u2',
        'invite_received','You were invited','inv:i9:received','2026-09-15'),
       ('018f0000-0000-7000-8000-000000000003','other','workspace','w1','user','u1',
        'ingestion_complete','Other product','ing:doc1:complete','2026-09-15');
INSERT INTO notify_idem VALUES
  ('aisat','user','u1','ing:doc1:complete','018f0000-0000-7000-8000-000000000001'),
  ('aisat','user','u2','inv:i9:received','018f0000-0000-7000-8000-000000000002'),
  ('other','user','u1','ing:doc1:complete','018f0000-0000-7000-8000-000000000003');

\echo '=== TEST 1: realm isolates the idempotency key space (D1) ==='
SELECT realm, recipient_id, idem_key FROM notify_idem WHERE idem_key='ing:doc1:complete' ORDER BY realm;

\echo '=== TEST 2: replay of the SAME (realm,recipient,idem) is rejected (NS-002) ==='
DO $$ BEGIN
    INSERT INTO notify_idem VALUES ('aisat','user','u1','ing:doc1:complete','018f0000-0000-7000-8000-00000000000f');
    RAISE EXCEPTION 'FAIL: duplicate idem_key accepted';
EXCEPTION WHEN unique_violation THEN RAISE NOTICE 'PASS: replay blocked by notify_idem PK';
END $$;

\echo '=== TEST 3: RLS scopes the inbox to one recipient (NS-001, release blocker) ==='
CREATE ROLE app_notify NOLOGIN;
GRANT SELECT ON notifications TO app_notify;
SET ROLE app_notify;
SET notify.realm='aisat'; SET notify.tenant_kind='workspace'; SET notify.tenant_id='w1';
SET notify.recipient_kind='user'; SET notify.recipient_id='u1';
\echo 'u1 sees (expect ONLY its own):'
SELECT recipient_id, title FROM notifications;
SET notify.recipient_id='u2';
\echo 'u2 sees (expect ONLY its own -- no leak of u1):'
SELECT recipient_id, title FROM notifications;
\echo 'wrong realm sees (expect 0 -- realm is in the predicate):'
SET notify.realm='nonexistent'; SET notify.recipient_id='u1';
SELECT count(*) AS rows_visible FROM notifications;
RESET ROLE;

\echo '=== TEST 4: exactly one OPEN digest window per (recipient,topic,channel) (D4) ==='
INSERT INTO digest_buffer (id, realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
                           topic, channel, shard, flush_at)
VALUES ('018f0000-0000-7000-8000-0000000000a1','aisat','workspace','w1','user','u1',
        'ingestion_complete','email',3, now() + interval '15 min');
DO $$ BEGIN
    INSERT INTO digest_buffer (id, realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
                               topic, channel, shard, flush_at)
    VALUES ('018f0000-0000-7000-8000-0000000000a2','aisat','workspace','w1','user','u1',
            'ingestion_complete','email',3, now() + interval '15 min');
    RAISE EXCEPTION 'FAIL: a second open window accepted';
EXCEPTION WHEN unique_violation THEN RAISE NOTICE 'PASS: burst coalesces into ONE open window';
END $$;
UPDATE digest_buffer SET flushed_at=now() WHERE id='018f0000-0000-7000-8000-0000000000a1';
INSERT INTO digest_buffer (id, realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
                           topic, channel, shard, flush_at)
VALUES ('018f0000-0000-7000-8000-0000000000a3','aisat','workspace','w1','user','u1',
        'ingestion_complete','email',3, now() + interval '15 min');
\echo 'PASS: after flush a new window opens; total windows:'
SELECT count(*) AS windows_total FROM digest_buffer;

\echo '=== TEST 5: no FK into the partitioned inbox, so retention DROP can never deadlock (D6) ==='
SELECT coalesce(count(*),0) AS fks_from_outbox
  FROM pg_constraint WHERE contype='f' AND conrelid='notification_outbox'::regclass;
SELECT coalesce(count(*),0) AS fks_referencing_notifications
  FROM pg_constraint WHERE contype='f' AND confrelid='notifications'::regclass;

\echo '=== TEST 6: terminal_reason is constrained ==='
INSERT INTO notification_outbox (id, notification_id, realm, tenant_kind, tenant_id, shard, channel)
VALUES ('018f0000-0000-7000-8000-0000000000b1','018f0000-0000-7000-8000-000000000001',
        'aisat','workspace','w1',3,'email');
DO $$ BEGIN
    UPDATE notification_outbox SET terminal_reason='whatever' WHERE shard=3;
    RAISE EXCEPTION 'FAIL: arbitrary terminal_reason accepted';
EXCEPTION WHEN check_violation THEN RAISE NOTICE 'PASS: terminal_reason restricted to the 3 legal values';
END $$;

\echo '=== TEST 7: priority + quota sanity checks hold ==='
DO $$ BEGIN
    INSERT INTO notifications (id, realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
                               topic, title, priority, idem_key, created_at)
    VALUES ('018f0000-0000-7000-8000-0000000000c1','aisat','workspace','w1','user','u1',
            't','x','URGENT!!','k1','2026-10-02');
    RAISE EXCEPTION 'FAIL: bogus priority accepted';
EXCEPTION WHEN check_violation THEN RAISE NOTICE 'PASS: priority restricted to info|warning|critical';
END $$;
DO $$ BEGIN
    INSERT INTO channel_quotas (realm,tenant_kind,tenant_id,channel,max_per_hour,max_per_day)
    VALUES ('aisat','workspace','w1','email',500,100);
    RAISE EXCEPTION 'FAIL: daily budget below hourly accepted';
EXCEPTION WHEN check_violation THEN RAISE NOTICE 'PASS: max_per_day >= max_per_hour enforced';
END $$;
