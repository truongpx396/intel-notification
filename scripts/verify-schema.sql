-- verify-schema.sql — assert the schema actually provides its guarantees.
--
-- Run via `make verify-schema`, which applies the migrations to a throwaway
-- PostgreSQL 16 container first. Every assertion maps to a success criterion or
-- a design decision; an assertion that maps to neither does not belong here.
-- Every test RAISEs on failure, so a broken guarantee fails the build instead of
-- printing a row somebody has to eyeball.
--
-- The suite runs AS THE TABLE OWNER: a role that is neither superuser nor
-- BYPASSRLS, which is exactly the role a worker connects as. A superuser skips
-- row-level security altogether, and a non-owner is subject to it even without
-- FORCE -- so neither proves that FORCE ROW LEVEL SECURITY holds (D17).
--
--   TEST 1   realm isolates the idempotency key space                      D1
--   TEST 2   tenant isolates the idempotency key space                     D18
--   TEST 3   a replay is rejected by the durable guard                     NS-002
--   TEST 4   RLS scopes the inbox as the OWNER; unscoped reads see nothing NS-001
--   TEST 5   every recipient-scoped relation, partitions included, is forced NS-001
--   TEST 6   the worker reads what it delivers; scope dies with the txn    D17
--   TEST 7   the claim leases rows; concurrent claimers skip locked rows   D19
--   TEST 8   a stale lease cannot record an outcome                        D19
--   TEST 9   terminal transition: history, dead letters, fallback          D12 / D20 / D29
--   TEST 10  per-address fan-out, fenced and one-shot                      D25
--   TEST 11  a noisy tenant's backlog leaves the due range in one step     D24
--   TEST 12  digest: one open window, sealed at max, flush is idempotent   D4 / NS-005
--   TEST 13  cancel stops pending deliveries and never an in-flight one    D29
--   TEST 14  the idempotency window is bounded                             D21
--   TEST 15  single-owner jobs: a live lease cannot be taken               D22
--   TEST 16  no foreign key references a partitioned table                 D6
--   TEST 17  constrained vocabularies                                      D5 / D9 / D12
--   TEST 18  erasure removes a recipient everywhere, keeps suppressions    D35
--   TEST 19  the canonical identity encoding is unambiguous                D16 / D18
--
\set ON_ERROR_STOP on
\set QUIET on
-- Query results are noise here; assertions report through NOTICE and \echo.
\o /dev/null

-- ---------------------------------------------------------------- setup ----
-- The owner: not a superuser, no BYPASSRLS. Every table, partition and
-- function is handed to it, and the suite runs as it.
CREATE ROLE notify_owner NOLOGIN NOSUPERUSER NOBYPASSRLS;
GRANT USAGE ON SCHEMA public TO notify_owner;
DO $$
DECLARE r record;
BEGIN
    FOR r IN SELECT c.oid::regclass AS t FROM pg_class c
              WHERE c.relnamespace = 'public'::regnamespace AND c.relkind IN ('r', 'p')
    LOOP
        EXECUTE format('ALTER TABLE %s OWNER TO notify_owner', r.t);
    END LOOP;
    FOR r IN SELECT p.oid::regprocedure AS f FROM pg_proc p
              WHERE p.pronamespace = 'public'::regnamespace
    LOOP
        EXECUTE format('ALTER FUNCTION %s OWNER TO notify_owner', r.f);
    END LOOP;
END $$;
-- A second session, for the concurrent-claim test. Created after the ownership
-- loop so its functions stay the superuser's.
CREATE EXTENSION dblink;

SET ROLE notify_owner;

CREATE FUNCTION pg_temp.expect(ok boolean, what text) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    IF ok IS NOT TRUE THEN
        RAISE EXCEPTION 'FAIL: %', what;
    END IF;
    RAISE NOTICE 'PASS: %', what;
END $$;

-- Transaction-local scope, the only form the engine uses.
CREATE FUNCTION pg_temp.scope(r text, tk text, tid text, rk text, rid text) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM set_config('notify.realm', r, true),
            set_config('notify.tenant_kind', tk, true),
            set_config('notify.tenant_id', tid, true),
            set_config('notify.recipient_kind', rk, true),
            set_config('notify.recipient_id', rid, true);
END $$;

-- Persist one notification the way PersistAndEnqueue does: scoped, with its guard.
CREATE FUNCTION pg_temp.put(p_id uuid, r text, tk text, tid text, rk text, rid text,
                            p_title text, p_idem text) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_temp.scope(r, tk, tid, rk, rid);
    INSERT INTO notifications (id, realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
                               topic, title, idem_key, created_at)
    VALUES (p_id, r, tk, tid, rk, rid, 'ingestion_complete', p_title, p_idem, '2026-09-15');
    INSERT INTO notify_idem (realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
                             idem_key, notification_id, notification_created_at)
    VALUES (r, tk, tid, rk, rid, p_idem, p_id, '2026-09-15');
END $$;

-- Enqueue one delivery for aisat/workspace/<tenant>/user/<recipient>.
CREATE FUNCTION pg_temp.q(p_id uuid, p_notification uuid, p_shard smallint, p_channel text,
                          p_tenant text DEFAULT 'w1', p_recipient text DEFAULT 'u1',
                          p_fallback text[] DEFAULT '{}',
                          p_due timestamptz DEFAULT now()) RETURNS void
LANGUAGE sql AS $$
    INSERT INTO notification_outbox (id, notification_id, notification_created_at, realm,
                                     tenant_kind, tenant_id, recipient_kind, recipient_id,
                                     topic, channel, shard, fallback, next_attempt_at)
    VALUES (p_id, p_notification, '2026-09-15', 'aisat', 'workspace', p_tenant, 'user',
            p_recipient, 'ingestion_complete', p_channel, p_shard, p_fallback, p_due);
$$;

-- Fixtures. n1 and n2 are two recipients in one tenant. n3 reuses n1's
-- recipient id and idem_key in ANOTHER REALM; n4 does so in ANOTHER TENANT.
DO $$
BEGIN
    PERFORM pg_temp.put('018f0000-0000-7000-8000-000000000001', 'aisat', 'workspace', 'w1',
                        'user', 'u1', 'Upload finished', 'ing:doc1:complete');
    PERFORM pg_temp.put('018f0000-0000-7000-8000-000000000002', 'aisat', 'workspace', 'w1',
                        'user', 'u2', 'You were invited', 'inv:i9:received');
    PERFORM pg_temp.put('018f0000-0000-7000-8000-000000000003', 'other', 'workspace', 'w1',
                        'user', 'u1', 'Other product', 'ing:doc1:complete');
    PERFORM pg_temp.put('018f0000-0000-7000-8000-000000000004', 'aisat', 'workspace', 'w2',
                        'user', 'u1', 'Other tenant', 'ing:doc1:complete');
END $$;

\echo '=== TEST 1: realm isolates the idempotency key space (D1) ==='
DO $$ BEGIN
    PERFORM pg_temp.expect(
        (SELECT count(DISTINCT realm) FROM notify_idem
          WHERE recipient_id = 'u1' AND idem_key = 'ing:doc1:complete') = 2,
        'one recipient id and idem_key coexist across two realms');
END $$;

\echo '=== TEST 2: tenant isolates the idempotency key space (D18) ==='
DO $$ BEGIN
    PERFORM pg_temp.expect(
        (SELECT count(*) FROM notify_idem
          WHERE realm = 'aisat' AND recipient_id = 'u1' AND idem_key = 'ing:doc1:complete') = 2,
        'the same user in two tenants gets both notifications, not one swallowed as a replay');
END $$;

\echo '=== TEST 3: a replay of the SAME identity and idem_key is rejected (NS-002) ==='
DO $$ BEGIN
    INSERT INTO notify_idem (realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
                             idem_key, notification_id, notification_created_at)
    VALUES ('aisat', 'workspace', 'w1', 'user', 'u1', 'ing:doc1:complete',
            '018f0000-0000-7000-8000-00000000000f', now());
    RAISE EXCEPTION 'FAIL: a duplicate idem_key was accepted';
EXCEPTION WHEN unique_violation THEN
    RAISE NOTICE 'PASS: the replay is blocked by the notify_idem primary key';
END $$;

\echo '=== TEST 4: RLS scopes the inbox AS THE OWNER (NS-001, release blocker) ==='
DO $$
DECLARE n int; t text;
BEGIN
    PERFORM pg_temp.scope('aisat', 'workspace', 'w1', 'user', 'u1');
    SELECT count(*), max(title) INTO n, t FROM notifications;
    PERFORM pg_temp.expect(n = 1 AND t = 'Upload finished',
        'as the owner, u1 sees exactly its own notification');

    SELECT count(*) INTO n FROM notifications_2026m09;
    PERFORM pg_temp.expect(n = 1, 'querying a partition by name is scoped too');

    PERFORM pg_temp.scope('aisat', 'workspace', 'w1', 'user', 'u2');
    SELECT count(*) INTO n FROM notifications WHERE recipient_id = 'u1';
    PERFORM pg_temp.expect(n = 0, 'u2 cannot see u1''s notification, even by asking for it');

    PERFORM pg_temp.scope('aisat', 'workspace', 'w2', 'user', 'u1');
    SELECT count(*), max(title) INTO n, t FROM notifications;
    PERFORM pg_temp.expect(n = 1 AND t = 'Other tenant',
        'the same user in another tenant sees only that tenant''s notification');

    PERFORM pg_temp.scope('nonexistent', 'workspace', 'w1', 'user', 'u1');
    SELECT count(*) INTO n FROM notifications;
    PERFORM pg_temp.expect(n = 0, 'a wrong realm sees nothing: the realm is in the predicate');
END $$;

-- A new transaction, which sets no scope.
DO $$
DECLARE n int;
BEGIN
    SELECT count(*) INTO n FROM notifications;
    PERFORM pg_temp.expect(n = 0, 'an unscoped transaction sees nothing (fail-closed)');
    SELECT count(*) INTO n FROM notifications_2026m09;
    PERFORM pg_temp.expect(n = 0, 'an unscoped read of a partition by name sees nothing');
END $$;

DO $$ BEGIN
    PERFORM pg_temp.scope('aisat', 'workspace', 'w1', 'user', 'u1');
    INSERT INTO notifications (id, realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
                               topic, title, idem_key, created_at)
    VALUES ('018f0000-0000-7000-8000-0000000000f1', 'aisat', 'workspace', 'w1', 'user', 'u2',
            'ingestion_complete', 'forged', 'k-forged', '2026-09-15');
    RAISE EXCEPTION 'FAIL: wrote a notification outside the current scope';
EXCEPTION WHEN insufficient_privilege THEN
    RAISE NOTICE 'PASS: a write outside the current scope is rejected';
END $$;

\echo '=== TEST 5: every recipient-scoped relation is forced, partitions included (NS-001) ==='
DO $$
DECLARE missing text; unclassified text; quals int;
BEGIN
    -- Scoped relations: these tables and every partition of them.
    WITH scoped AS (
        SELECT c.oid, c.relname FROM pg_class c
         WHERE c.relname IN ('notifications', 'notification_preferences',
                             'notification_schedules', 'recipient_addresses')
        UNION ALL
        SELECT i.inhrelid, i.inhrelid::regclass::text FROM pg_inherits i
         WHERE i.inhparent = 'notifications'::regclass
    )
    SELECT string_agg(s.relname, ', ') INTO missing
      FROM scoped s JOIN pg_class c ON c.oid = s.oid
     WHERE NOT (c.relrowsecurity AND c.relforcerowsecurity)
        OR NOT EXISTS (SELECT 1 FROM pg_policy p
                        WHERE p.polrelid = s.oid AND p.polname = 'recipient_scope');
    PERFORM pg_temp.expect(missing IS NULL,
        'forced RLS + recipient_scope on every scoped relation'
        || coalesce(' -- MISSING on: ' || missing, ''));

    SELECT count(DISTINCT pg_get_expr(p.polqual, p.polrelid)) INTO quals
      FROM pg_policy p WHERE p.polname = 'recipient_scope';
    PERFORM pg_temp.expect(quals = 1, 'every recipient_scope policy has the identical predicate');

    -- Any other top-level table carrying a recipient must be a known worker-only
    -- table. A new table with recipient_id and no decision fails here.
    SELECT string_agg(c.relname, ', ') INTO unclassified
      FROM pg_class c
     WHERE c.relnamespace = 'public'::regnamespace AND c.relkind IN ('r', 'p')
       AND NOT c.relispartition
       AND EXISTS (SELECT 1 FROM pg_attribute a
                    WHERE a.attrelid = c.oid AND a.attname = 'recipient_id' AND NOT a.attisdropped)
       AND c.relname NOT IN ('notifications', 'notification_preferences', 'notification_schedules',
                             'recipient_addresses',
                             -- worker-only: never read on a recipient-facing path
                             'notify_idem', 'notification_outbox', 'notification_deliveries',
                             'dead_letters', 'digest_buffer');
    PERFORM pg_temp.expect(unclassified IS NULL,
        'every table holding a recipient is either scoped or declared worker-only'
        || coalesce(' -- UNCLASSIFIED: ' || unclassified, ''));
END $$;

\echo '=== TEST 6: the worker reads what it delivers, and its scope dies with the txn (D17) ==='
SELECT pg_temp.q('018f0000-0000-7000-8000-0000000000b1', '018f0000-0000-7000-8000-000000000001',
                 3::smallint, 'email');
DO $$
DECLARE o notification_outbox; t text;
BEGIN
    SELECT * INTO o FROM notification_outbox WHERE id = '018f0000-0000-7000-8000-0000000000b1';
    PERFORM pg_temp.scope(o.realm, o.tenant_kind, o.tenant_id, o.recipient_kind, o.recipient_id);
    SELECT title INTO t FROM notifications
     WHERE id = o.notification_id AND created_at = o.notification_created_at;
    PERFORM pg_temp.expect(t = 'Upload finished',
        'the dispatcher scopes itself from the queue row and reads the notification');
END $$;
DO $$ BEGIN
    PERFORM pg_temp.expect((SELECT count(*) FROM notifications) = 0,
        'the scope was transaction-local: the next transaction on the connection sees nothing');
END $$;
DELETE FROM notification_outbox WHERE id = '018f0000-0000-7000-8000-0000000000b1';

\echo '=== TEST 7: the claim leases rows; concurrent claimers skip locked rows (D19) ==='
SELECT pg_temp.q('018f0000-0000-7000-8000-0000000000c1', '018f0000-0000-7000-8000-000000000001', 5::smallint, 'email'),
       pg_temp.q('018f0000-0000-7000-8000-0000000000c2', '018f0000-0000-7000-8000-000000000001', 5::smallint, 'sms'),
       pg_temp.q('018f0000-0000-7000-8000-0000000000c3', '018f0000-0000-7000-8000-000000000001', 5::smallint, 'push');
BEGIN;
DO $$ BEGIN
    PERFORM pg_temp.expect(
        (SELECT count(*) FROM notify_claim_outbox(5::smallint, 2, '5 minutes')) = 2,
        'session A claims two of three due rows and holds them in an open transaction');
END $$;
RESET ROLE;
DO $$ BEGIN
    PERFORM pg_temp.expect(
        -- A timeout, so a claim that blocks instead of skipping FAILS rather than hangs.
        (SELECT count(*) FROM dblink('dbname=' || current_database()
                                     || ' user=postgres options=''-c statement_timeout=5000''',
             'SELECT id FROM notify_claim_outbox(5::smallint, 10, ''5 minutes'')') AS t(id uuid)) = 1,
        'a concurrent session B skips the rows A holds and claims only the free one');
END $$;
SET ROLE notify_owner;
COMMIT;
DO $$ BEGIN
    PERFORM pg_temp.expect(
        (SELECT count(*) FROM notify_claim_outbox(5::smallint, 10, '5 minutes')) = 0,
        'leased rows are invisible to every claimer until the lease lapses');
END $$;
-- The workers die holding their leases; the leases lapse.
UPDATE notification_outbox SET next_attempt_at = now() - interval '1 second' WHERE shard = 5;
DO $$
DECLARE n int; a int;
BEGIN
    SELECT count(*), min(attempts) INTO n, a FROM notify_claim_outbox(5::smallint, 10, '5 minutes');
    PERFORM pg_temp.expect(n = 3 AND a = 2,
        'lapsed leases are re-claimed, and each claim counted as an attempt');
END $$;

\echo '=== TEST 8: a stale lease cannot record an outcome (D19) ==='
DO $$
DECLARE r notification_outbox;
BEGIN
    SELECT * INTO r FROM notification_outbox WHERE id = '018f0000-0000-7000-8000-0000000000c1';
    PERFORM pg_temp.expect(NOT notify_retry_delivery(r.id, gen_random_uuid(), now(), 'late'),
        'a write carrying someone else''s lease token is discarded');
    PERFORM pg_temp.expect(notify_retry_delivery(r.id, r.lease_token, now() + interval '1 minute', 'timeout'),
        'the lease holder records a retry');
    PERFORM pg_temp.expect(NOT notify_finish_delivery(r.id, r.lease_token, 'delivered'),
        'recording the retry released the lease: the old token cannot finish the row');
END $$;
DELETE FROM notification_outbox WHERE shard = 5;

\echo '=== TEST 9: terminal transition -- history, dead letters, fallback (D12, D20, D29) ==='
SELECT pg_temp.q('018f0000-0000-7000-8000-0000000000d1', '018f0000-0000-7000-8000-000000000002', 6::smallint, 'email', 'w1', 'u2'),
       pg_temp.q('018f0000-0000-7000-8000-0000000000d2', '018f0000-0000-7000-8000-000000000002', 6::smallint, 'sms', 'w1', 'u2'),
       pg_temp.q('018f0000-0000-7000-8000-0000000000d3', '018f0000-0000-7000-8000-000000000002', 6::smallint, 'push', 'w1', 'u2',
                 ARRAY['sms_fallback', 'email_fallback']),
       pg_temp.q('018f0000-0000-7000-8000-0000000000d4', '018f0000-0000-7000-8000-000000000002', 6::smallint, 'slack', 'w1', 'u2');
DO $$
DECLARE r notification_outbox;
BEGIN
    FOR r IN SELECT * FROM notify_claim_outbox(6::smallint, 10, '5 minutes') LOOP
        PERFORM notify_finish_delivery(r.id, r.lease_token,
            CASE r.channel WHEN 'email' THEN 'delivered' WHEN 'sms' THEN 'max_attempts'
                           WHEN 'push' THEN 'no_address' ELSE 'suppressed' END,
            CASE r.channel WHEN 'email' THEN 'prov-123' END);
    END LOOP;

    PERFORM pg_temp.expect(
        NOT EXISTS (SELECT 1 FROM notification_outbox WHERE id IN (
            '018f0000-0000-7000-8000-0000000000d1', '018f0000-0000-7000-8000-0000000000d2',
            '018f0000-0000-7000-8000-0000000000d3', '018f0000-0000-7000-8000-0000000000d4')),
        'finished deliveries leave the queue: it holds pending work only');
    PERFORM pg_temp.expect(
        (SELECT outcome FROM notification_deliveries
          WHERE channel = 'email' AND provider_message_id = 'prov-123') = 'delivered',
        'a provider callback finds its delivery by provider message id');
    PERFORM pg_temp.expect(
        (SELECT count(*) FROM dead_letters
          WHERE outbox_id = '018f0000-0000-7000-8000-0000000000d2' AND reason = 'max_attempts') = 1,
        'a genuine failure is dead-lettered with its reason');
    PERFORM pg_temp.expect(
        (SELECT count(*) FROM dead_letters WHERE outbox_id IN (
            '018f0000-0000-7000-8000-0000000000d3', '018f0000-0000-7000-8000-0000000000d4')) = 0,
        'suppressed and no_address are correct outcomes, not dead letters');
    PERFORM pg_temp.expect(
        (SELECT fallback FROM notification_outbox
          WHERE notification_id = '018f0000-0000-7000-8000-000000000002'
            AND channel = 'sms_fallback') = ARRAY['email_fallback'],
        'an undelivered channel enqueues the next one in its fallback chain');
END $$;
DO $$ BEGIN
    PERFORM notify_finish_delivery(gen_random_uuid(), gen_random_uuid(), 'fanned_out');
    RAISE EXCEPTION 'FAIL: a finish recorded fanned_out';
EXCEPTION WHEN raise_exception THEN
    IF SQLERRM LIKE 'FAIL:%' THEN RAISE; END IF;
    RAISE NOTICE 'PASS: fanned_out can only come from address binding';
END $$;

\echo '=== TEST 10: per-address fan-out, fenced and one-shot (D25) ==='
SELECT pg_temp.q('018f0000-0000-7000-8000-0000000000e1', '018f0000-0000-7000-8000-000000000001', 8::smallint, 'push'),
       pg_temp.q('018f0000-0000-7000-8000-0000000000e2', '018f0000-0000-7000-8000-000000000001', 8::smallint, 'email');
DO $$
DECLARE r notification_outbox; push_token uuid; email_token uuid;
BEGIN
    FOR r IN SELECT * FROM notify_claim_outbox(8::smallint, 10, '5 minutes') LOOP
        IF r.channel = 'push' THEN push_token := r.lease_token; ELSE email_token := r.lease_token; END IF;
    END LOOP;

    PERFORM pg_temp.expect(
        notify_bind_addresses('018f0000-0000-7000-8000-0000000000e1', push_token,
            '[{"key":"dev-a","address":{"value":"tok-a"}},
              {"key":"dev-b","address":{"value":"tok-b"}},
              {"key":"dev-c","address":{"value":"tok-c"}}]') = 3,
        'three devices bind as three deliveries');
    PERFORM pg_temp.expect(
        (SELECT count(*) FROM notification_outbox
          WHERE notification_id = '018f0000-0000-7000-8000-000000000001' AND channel = 'push'
            AND address_key IN ('dev-a', 'dev-b', 'dev-c') AND lease_token IS NULL) = 3,
        'each device is its own unleased row, with its own retry state');
    PERFORM pg_temp.expect(
        (SELECT outcome FROM notification_deliveries
          WHERE id = '018f0000-0000-7000-8000-0000000000e1') = 'fanned_out',
        'the channel-level row is recorded as fanned out');
    PERFORM pg_temp.expect(
        notify_bind_addresses('018f0000-0000-7000-8000-0000000000e1', push_token,
            '[{"key":"dev-a","address":{}},{"key":"dev-z","address":{}}]') IS NULL,
        'binding again with the spent lease is fenced out');

    PERFORM pg_temp.expect(
        notify_bind_addresses('018f0000-0000-7000-8000-0000000000e2', email_token,
            '[{"key":"addr-x","address":{"value":"u1@example.com"}}]') = 1,
        'a single address binds in place');
    PERFORM pg_temp.expect(
        (SELECT address_key = 'addr-x' AND lease_token = email_token FROM notification_outbox
          WHERE id = '018f0000-0000-7000-8000-0000000000e2'),
        'the in-place binding keeps the lease, so the claimer goes on to deliver');
    PERFORM pg_temp.expect(
        notify_bind_addresses('018f0000-0000-7000-8000-0000000000e2', email_token,
            '[{"key":"addr-y","address":{}}]') IS NULL,
        'an address, once bound, is never re-bound, so the delivery key stays stable');
END $$;
DELETE FROM notification_outbox WHERE shard = 8;

\echo '=== TEST 11: a noisy tenant''s backlog leaves the due range in one step (D24) ==='
INSERT INTO notification_outbox (notification_id, notification_created_at, realm, tenant_kind,
                                 tenant_id, recipient_kind, recipient_id, topic, channel, shard,
                                 next_attempt_at)
SELECT gen_random_uuid(), now(), 'aisat', 'workspace', 'w-noisy', 'user', 'u' || g,
       'ingestion_complete', 'email', 9, now() - interval '10 minutes'
  FROM generate_series(1, 50) g;
SELECT pg_temp.q(gen_random_uuid(), gen_random_uuid(), 9::smallint, 'email', 'w-quiet', 'q1',
                 '{}', now() - interval '1 minute');
DO $$ BEGIN
    PERFORM pg_temp.expect(
        (SELECT tenant_id FROM notification_outbox WHERE shard = 9 AND next_attempt_at <= now()
          ORDER BY next_attempt_at LIMIT 1) = 'w-noisy',
        'before deferral the noisy tenant holds the head of the shard');
    PERFORM pg_temp.expect(
        notify_defer_tenant_channel('aisat', 'workspace', 'w-noisy', 'email',
                                    now() + interval '1 hour') = 50,
        'its exhausted quota moves its whole due backlog in one statement');
    PERFORM pg_temp.expect(
        (SELECT array_agg(DISTINCT tenant_id) FROM notify_claim_outbox(9::smallint, 5, '5 minutes'))
            = ARRAY['w-quiet'],
        'the next claim goes straight to the quiet tenant');
END $$;
DELETE FROM notification_outbox WHERE shard = 9;

\echo '=== TEST 12: digest -- one open window, sealed at max, flush idempotent (D4, NS-005) ==='
DO $$
DECLARE a uuid; b uuid; c uuid;
BEGIN
    a := notify_digest_append('aisat', 'workspace', 'w1', 'user', 'u1', 'ingestion_complete',
                              'email', 3::smallint, gen_random_uuid(), '15 minutes', 2);
    b := notify_digest_append('aisat', 'workspace', 'w1', 'user', 'u1', 'ingestion_complete',
                              'email', 3::smallint, gen_random_uuid(), '15 minutes', 2);
    c := notify_digest_append('aisat', 'workspace', 'w1', 'user', 'u1', 'ingestion_complete',
                              'email', 3::smallint, gen_random_uuid(), '15 minutes', 2);
    PERFORM pg_temp.expect(a = b, 'members inside one window share it');
    PERFORM pg_temp.expect(c <> a, 'at DigestMax the window seals and the next member opens another');
    PERFORM pg_temp.expect(
        (SELECT sealed_at IS NOT NULL AND flush_at <= now() AND member_count = 2
           FROM digest_buffer WHERE id = a),
        'the full window is sealed with DigestMax members and due at once');

    BEGIN
        INSERT INTO digest_buffer (realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
                                   topic, channel, shard, flush_at)
        VALUES ('aisat', 'workspace', 'w1', 'user', 'u1', 'ingestion_complete', 'email', 3,
                now() + interval '15 minutes');
        RAISE EXCEPTION 'FAIL: a second open window was accepted';
    EXCEPTION WHEN unique_violation THEN
        RAISE NOTICE 'PASS: at most one open window per (recipient, topic, channel)';
    END;

    PERFORM pg_temp.expect(notify_digest_flush(a) IS NOT NULL, 'flushing a due window enqueues it');
    PERFORM pg_temp.expect(notify_digest_flush(a) IS NULL, 'a duplicate flush tick is a no-op');
    PERFORM pg_temp.expect(
        (SELECT count(*) FROM notification_outbox WHERE digest_id = a) = 1,
        'one window, exactly one delivery');
END $$;

\echo '=== TEST 13: cancel stops pending deliveries, never an in-flight one (D29) ==='
DO $$ BEGIN
    PERFORM pg_temp.put('018f0000-0000-7000-8000-000000000005', 'aisat', 'workspace', 'w1',
                        'user', 'u3', 'Meeting starts soon', 'remind:m1:start');
END $$;
SELECT pg_temp.q('018f0000-0000-7000-8000-0000000000f2', '018f0000-0000-7000-8000-000000000005',
                 10::smallint, 'email', 'w1', 'u3'),
       pg_temp.q('018f0000-0000-7000-8000-0000000000f3', '018f0000-0000-7000-8000-000000000005',
                 11::smallint, 'sms', 'w1', 'u3');
-- A worker is mid-send on the SMS.
SELECT count(*) AS claimed FROM notify_claim_outbox(11::smallint, 1, '5 minutes') \gset
DO $$
DECLARE r record;
BEGIN
    SELECT * INTO r FROM notify_cancel('aisat', 'workspace', 'w1', 'user', 'u3', 'remind:m1:start');
    PERFORM pg_temp.expect(r.matched AND r.canceled = 1 AND r.in_flight = 1,
        'cancel stops the pending email and reports the SMS already in flight');
    PERFORM pg_temp.expect(
        (SELECT outcome FROM notification_deliveries
          WHERE id = '018f0000-0000-7000-8000-0000000000f2') = 'canceled',
        'the canceled delivery is recorded as canceled');
    PERFORM pg_temp.expect(
        EXISTS (SELECT 1 FROM notification_outbox WHERE id = '018f0000-0000-7000-8000-0000000000f3'),
        'the in-flight delivery is left to its worker, not falsely recorded as canceled');
    PERFORM pg_temp.scope('aisat', 'workspace', 'w1', 'user', 'u3');
    PERFORM pg_temp.expect(
        (SELECT canceled_at IS NOT NULL FROM notifications
          WHERE id = '018f0000-0000-7000-8000-000000000005'),
        'the inbox row is marked canceled');
    SELECT * INTO r FROM notify_cancel('aisat', 'workspace', 'w1', 'user', 'u3', 'no-such-key');
    PERFORM pg_temp.expect(NOT r.matched, 'canceling an unknown key reports no match');
END $$;
DELETE FROM notification_outbox WHERE shard IN (10, 11);

\echo '=== TEST 14: the idempotency window is bounded (D21) ==='
INSERT INTO notify_idem (realm, tenant_kind, tenant_id, recipient_kind, recipient_id, idem_key,
                         notification_id, notification_created_at, created_at)
VALUES ('aisat', 'workspace', 'w1', 'user', 'u1', 'aged-key', gen_random_uuid(),
        '2026-09-01', now() - interval '8 days');
DO $$ BEGIN
    PERFORM pg_temp.expect(notify_expire_idem(now() - interval '7 days', 1000) = 1,
        'keys older than the window expire');
    PERFORM pg_temp.expect(
        EXISTS (SELECT 1 FROM notify_idem WHERE idem_key = 'ing:doc1:complete' AND tenant_id = 'w1'
                  AND realm = 'aisat'),
        'keys inside the window are kept');
    PERFORM pg_temp.expect(
        (SELECT count(*) FROM pg_inherits WHERE inhparent = 'notify_idem'::regclass) = 16,
        'the guard is hash-partitioned, so expiry and vacuum work a partition at a time');
END $$;

\echo '=== TEST 15: single-owner jobs -- a live lease cannot be taken (D22) ==='
DO $$ BEGIN
    PERFORM pg_temp.expect(notify_try_lease_job('retention', 'worker-1', '1 minute'),
        'the first worker takes the retention lease');
    PERFORM pg_temp.expect(NOT notify_try_lease_job('retention', 'worker-2', '1 minute'),
        'a second worker cannot take a live lease');
    PERFORM pg_temp.expect(notify_try_lease_job('retention', 'worker-1', '1 minute'),
        'the holder renews its own lease');
    UPDATE notify_job_leases SET lease_expires_at = now() - interval '1 second' WHERE job = 'retention';
    PERFORM pg_temp.expect(notify_try_lease_job('retention', 'worker-2', '1 minute'),
        'once the lease lapses another worker takes over');
END $$;

\echo '=== TEST 16: no foreign key references a partitioned table (D6) ==='
DO $$ BEGIN
    PERFORM pg_temp.expect(
        (SELECT count(*) FROM pg_constraint k JOIN pg_class c ON c.oid = k.confrelid
          WHERE k.contype = 'f' AND (c.relkind = 'p' OR c.relispartition)) = 0,
        'retiring a partition can never be blocked by a referencing row');
END $$;

\echo '=== TEST 17: constrained vocabularies (D5, D9, D12) ==='
DO $$ BEGIN
    BEGIN
        UPDATE notification_outbox SET priority = 'URGENT!!' WHERE digest_id IS NOT NULL;
        RAISE EXCEPTION 'FAIL: a bogus priority was accepted';
    EXCEPTION WHEN check_violation THEN
        RAISE NOTICE 'PASS: priority is info | warning | critical';
    END;
    BEGIN
        INSERT INTO notification_deliveries (id, realm, tenant_kind, tenant_id, recipient_kind,
                                             recipient_id, topic, channel, address_key, outcome, attempts)
        VALUES (gen_random_uuid(), 'aisat', 'workspace', 'w1', 'user', 'u1', 't', 'email', '',
                'whatever', 1);
        RAISE EXCEPTION 'FAIL: an arbitrary outcome was accepted';
    EXCEPTION WHEN check_violation THEN
        RAISE NOTICE 'PASS: a delivery outcome is one of the named terminal states';
    END;
    BEGIN
        INSERT INTO dead_letters (id, realm, source, reason, payload, attempts, last_error)
        VALUES (gen_random_uuid(), 'aisat', 'outbox', 'suppressed', '{}', 1, 'x');
        RAISE EXCEPTION 'FAIL: a correct outcome was accepted as a dead letter';
    EXCEPTION WHEN check_violation THEN
        RAISE NOTICE 'PASS: only genuine failures can be dead letters';
    END;
    BEGIN
        INSERT INTO notification_outbox (notification_id, notification_created_at, digest_id, realm,
                                         tenant_kind, tenant_id, recipient_kind, recipient_id,
                                         topic, channel, shard)
        VALUES (gen_random_uuid(), now(), gen_random_uuid(), 'aisat', 'workspace', 'w1', 'user',
                'u1', 't', 'email', 0);
        RAISE EXCEPTION 'FAIL: a delivery for both a notification and a digest was accepted';
    EXCEPTION WHEN check_violation THEN
        RAISE NOTICE 'PASS: a delivery is for exactly one notification or one digest';
    END;
    BEGIN
        INSERT INTO channel_quotas (realm, tenant_kind, tenant_id, channel, max_per_hour, max_per_day)
        VALUES ('aisat', 'workspace', 'w1', 'email', 500, 100);
        RAISE EXCEPTION 'FAIL: a daily budget below the hourly one was accepted';
    EXCEPTION WHEN check_violation THEN
        RAISE NOTICE 'PASS: max_per_day >= max_per_hour';
    END;
    INSERT INTO channel_quotas (realm, channel, max_per_hour, max_per_day)
    VALUES ('aisat', 'email', 1000, 10000);
    RAISE NOTICE 'PASS: an empty tenant pair is the realm-wide default quota';
    BEGIN
        INSERT INTO channel_providers (realm, channel, provider, secret_ref)
        VALUES ('aisat', 'email', 'resend', 're_live_abc123');
        RAISE EXCEPTION 'FAIL: a bare credential was accepted as a secret reference';
    EXCEPTION WHEN check_violation THEN
        RAISE NOTICE 'PASS: provider credentials are referenced, never stored';
    END;
    BEGIN
        INSERT INTO notification_topics (realm, topic, default_channels, fallback)
        VALUES ('aisat', 't', ARRAY['email'], ARRAY['email']);
        RAISE EXCEPTION 'FAIL: a channel was both fanned out and a fallback';
    EXCEPTION WHEN check_violation THEN
        RAISE NOTICE 'PASS: a channel is either parallel or a fallback, not both';
    END;
    BEGIN
        -- The case the CHECK exists for: a scope whose realm reads back as ''
        -- would otherwise match a row with an empty realm.
        PERFORM pg_temp.scope('', 'workspace', 'w1', 'user', 'u1');
        INSERT INTO notifications (id, realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
                                   topic, idem_key, created_at)
        VALUES (gen_random_uuid(), '', 'workspace', 'w1', 'user', 'u1', 't', 'k', '2026-09-15');
        RAISE EXCEPTION 'FAIL: an empty realm was accepted';
    EXCEPTION WHEN check_violation THEN
        RAISE NOTICE 'PASS: an empty identity component is rejected, keeping RLS fail-closed';
    END;
END $$;

\echo '=== TEST 18: erasure removes a recipient everywhere, keeps suppressions (D35) ==='
DO $$ BEGIN
    PERFORM pg_temp.put('018f0000-0000-7000-8000-000000000009', 'aisat', 'workspace', 'w1',
                        'user', 'u9', 'To be erased', 'erase:me');
    INSERT INTO notification_preferences (realm, tenant_kind, tenant_id, recipient_kind,
                                          recipient_id, topic, channel, enabled)
    VALUES ('aisat', 'workspace', 'w1', 'user', 'u9', 'ingestion_complete', 'email', false);
    INSERT INTO notification_schedules (realm, tenant_kind, tenant_id, recipient_kind, recipient_id)
    VALUES ('aisat', 'workspace', 'w1', 'user', 'u9');
    INSERT INTO recipient_addresses (realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
                                     channel, address_key, value)
    VALUES ('aisat', 'workspace', 'w1', 'user', 'u9', 'email', 'k9', 'u9@example.com');
END $$;
SELECT pg_temp.q('018f0000-0000-7000-8000-0000000000a9', '018f0000-0000-7000-8000-000000000009',
                 12::smallint, 'email', 'w1', 'u9');
INSERT INTO notification_deliveries (id, notification_id, realm, tenant_kind, tenant_id,
                                     recipient_kind, recipient_id, topic, channel, address_key,
                                     outcome, attempts)
VALUES (gen_random_uuid(), '018f0000-0000-7000-8000-000000000009', 'aisat', 'workspace', 'w1',
        'user', 'u9', 'ingestion_complete', 'sms', 'k9s', 'delivered', 1);
INSERT INTO dead_letters (id, realm, tenant_kind, tenant_id, recipient_kind, recipient_id, source,
                          reason, payload, attempts, last_error)
VALUES (gen_random_uuid(), 'aisat', 'workspace', 'w1', 'user', 'u9', 'outbox', 'max_attempts',
        '{}', 5, 'timeout');
SELECT notify_digest_append('aisat', 'workspace', 'w1', 'user', 'u9', 'ingestion_complete',
                            'email', 12::smallint, gen_random_uuid(), '15 minutes', 100) IS NOT NULL
    AS digested \gset
INSERT INTO channel_suppressions (realm, channel, address_hash, reason)
VALUES ('aisat', 'email',
        sha256(convert_to(notify_canonical('email', 'u9@example.com'), 'UTF8')), 'complaint');

DO $$
DECLARE c jsonb;
BEGIN
    c := notify_erase_recipient('aisat', 'workspace', 'w1', 'user', 'u9');
    PERFORM pg_temp.expect(
        (c ->> 'notifications')::int = 1 AND (c ->> 'notify_idem')::int = 1
        AND (c ->> 'notification_outbox')::int = 1 AND (c ->> 'notification_deliveries')::int = 1
        AND (c ->> 'dead_letters')::int = 1 AND (c ->> 'digest_buffer')::int = 1
        AND (c ->> 'notification_preferences')::int = 1
        AND (c ->> 'notification_schedules')::int = 1
        AND (c ->> 'recipient_addresses')::int = 1,
        'erasure reports one removed row from each of the nine tables that held u9: ' || c::text);
END $$;
DO $$ BEGIN
    PERFORM pg_temp.scope('aisat', 'workspace', 'w1', 'user', 'u9');
    PERFORM pg_temp.expect(
        (SELECT count(*) FROM notifications) + (SELECT count(*) FROM notification_preferences)
        + (SELECT count(*) FROM notification_schedules) + (SELECT count(*) FROM recipient_addresses)
        + (SELECT count(*) FROM notify_idem WHERE recipient_id = 'u9')
        + (SELECT count(*) FROM notification_outbox WHERE recipient_id = 'u9')
        + (SELECT count(*) FROM notification_deliveries WHERE recipient_id = 'u9')
        + (SELECT count(*) FROM dead_letters WHERE recipient_id = 'u9')
        + (SELECT count(*) FROM digest_buffer WHERE recipient_id = 'u9') = 0,
        'nothing of u9 remains, including behind its own scope');
    PERFORM pg_temp.scope('aisat', 'workspace', 'w1', 'user', 'u1');
    PERFORM pg_temp.expect((SELECT count(*) FROM notifications) = 1,
        'erasing u9 left u1''s notification untouched');
    PERFORM pg_temp.expect(
        (SELECT count(*) FROM channel_suppressions WHERE reason = 'complaint') = 1,
        'the complaint suppression survives erasure, so u9 is not mailed again');
    PERFORM pg_temp.expect(
        (SELECT subject_hash FROM erasure_requests) =
            sha256(convert_to(notify_canonical('aisat', 'workspace', 'w1', 'user', 'u9'), 'UTF8')),
        'the erasure is recorded by hash, not by identity');
    PERFORM pg_temp.expect(
        NOT EXISTS (SELECT 1 FROM information_schema.columns
                     WHERE table_name = 'channel_suppressions' AND column_name = 'address'),
        'suppressions hold an address hash, never the address');
END $$;

\echo '=== TEST 19: the canonical identity encoding is unambiguous (D16, D18) ==='
DO $$ BEGIN
    PERFORM pg_temp.expect(notify_canonical('a:b', 'c') <> notify_canonical('a', 'b:c'),
        'ids containing the separator cannot collide');
    PERFORM pg_temp.expect(notify_canonical('ab', '') <> notify_canonical('a', 'b'),
        'empty components cannot collide either');
    -- The frozen vector the Go implementation is tested against.
    PERFORM pg_temp.expect(
        notify_canonical('aisat', 'workspace', 'w1', 'user', 'u1') = '5:aisat,9:workspace,2:w1,4:user,2:u1',
        'the encoding matches its frozen test vector');
END $$;

RESET ROLE;
\echo '=== all schema guarantees hold ==='
