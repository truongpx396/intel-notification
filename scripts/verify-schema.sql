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
--   TEST 7   at most one open digest window per (recipient, topic, channel);
--            a window cannot be flushed before it is sealed                   D4
--   TEST 8   the idempotency guard is hash-partitioned and indexed by age  D21
--   TEST 9   no foreign key references a partitioned table                 D6
--   TEST 10  constrained vocabularies and shapes: a tenant pair is whole or
--            empty, one template version is active, a broadcast has one
--            audience source                                     D5 / D9 / D12 / D27 / D31
--   TEST 11  suppressions and erasure records hold hashes, not identities  D35
--   TEST 12  a claim can be a HOT update: no index on the columns it writes D19
--   TEST 13  no default partition, so a date with no partition is refused  D33 / NR-022
--   TEST 14  every recipient-scoped table rejects an empty identity component NS-001
--   TEST 15  at most one pending delivery per address                      NS-002 / D25
--
-- The queue's state transitions — the claim, its fence, the terminal move,
-- fan-out, fairness, digests, cancel, expiry, erasure — are SQL in the Go
-- adapter and are tested there, against a real PostgreSQL, by
-- `make test-integration` (D37). This file asserts what the schema alone holds.
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
SET ROLE notify_owner;

CREATE FUNCTION pg_temp.expect(ok boolean, what text) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    IF ok IS NOT TRUE THEN
        RAISE EXCEPTION 'FAIL: %', what;
    END IF;
    RAISE NOTICE 'PASS: %', what;
END $$;

-- Run one statement a guarantee must ALLOW. It is the control half of a rejection test:
-- if over-constraining broke it, the failure names the guarantee instead of surfacing as
-- a raw error from a setup step.
CREATE FUNCTION pg_temp.accepts(p_sql text, what text) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    BEGIN
        EXECUTE p_sql;
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION 'FAIL: % -- but it was rejected: %', what, SQLERRM;
    END;
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

\echo '=== TEST 7: one open digest window per (recipient, topic, channel); flush only after seal (D4) ==='
DO $$
DECLARE first_window uuid; open_window uuid;
BEGIN
    INSERT INTO digest_buffer (realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
                               topic, channel, shard, flush_at)
    VALUES ('aisat', 'workspace', 'w1', 'user', 'u1', 'ingestion_complete', 'email', 3,
            now() + interval '15 minutes')
    RETURNING id INTO first_window;
    BEGIN
        INSERT INTO digest_buffer (realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
                                   topic, channel, shard, flush_at)
        VALUES ('aisat', 'workspace', 'w1', 'user', 'u1', 'ingestion_complete', 'email', 3,
                now() + interval '15 minutes');
        RAISE EXCEPTION 'FAIL: a second open window was accepted';
    EXCEPTION WHEN unique_violation THEN
        RAISE NOTICE 'PASS: a second open window is rejected by the partial unique index';
    END;
    UPDATE digest_buffer SET sealed_at = now() WHERE id = first_window;
    INSERT INTO digest_buffer (realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
                               topic, channel, shard, flush_at)
    VALUES ('aisat', 'workspace', 'w1', 'user', 'u1', 'ingestion_complete', 'email', 3,
            now() + interval '15 minutes')
    RETURNING id INTO open_window;
    RAISE NOTICE 'PASS: once a window is sealed, the next one can open';
    BEGIN
        UPDATE digest_buffer SET member_count = 5 WHERE id = first_window;
        RAISE EXCEPTION 'FAIL: member_count drifted from member_ids';
    EXCEPTION WHEN check_violation THEN
        RAISE NOTICE 'PASS: member_count always equals the members held';
    END;
    -- A window is flushed once, and only after it stops taking members: flushing an
    -- open one would enqueue a digest that misses the notifications still to arrive.
    BEGIN
        UPDATE digest_buffer SET flushed_at = now() WHERE id = open_window;
        RAISE EXCEPTION 'FAIL: a window still taking members was flushed';
    EXCEPTION WHEN check_violation THEN
        RAISE NOTICE 'PASS: a window cannot be flushed before it is sealed';
    END;
    BEGIN
        INSERT INTO digest_buffer (realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
                                   topic, channel, shard, flush_at, flushed_at)
        VALUES ('aisat', 'workspace', 'w1', 'user', 'u1', 'other_topic', 'email', 3, now(), now());
        RAISE EXCEPTION 'FAIL: a window was created already flushed and never sealed';
    EXCEPTION WHEN check_violation THEN
        RAISE NOTICE 'PASS: nor can one be created flushed and unsealed';
    END;
    PERFORM pg_temp.accepts(format('UPDATE digest_buffer SET flushed_at = now() WHERE id = %L', first_window),
                            'a sealed window can be flushed');
END $$;

\echo '=== TEST 8: the idempotency guard is hash-partitioned and indexed by age (D21) ==='
DO $$ BEGIN
    PERFORM pg_temp.expect(
        (SELECT count(*) FROM pg_inherits WHERE inhparent = 'notify_idem'::regclass) = 16,
        'notify_idem has 16 hash partitions, so expiry and vacuum work one at a time');
    PERFORM pg_temp.expect(
        EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = 'notify_idem'
                  AND indexdef LIKE '%(created_at)%'),
        'expiry by age rides an index');
END $$;

\echo '=== TEST 9: no foreign key references a partitioned table (D6) ==='
DO $$ BEGIN
    PERFORM pg_temp.expect(
        (SELECT count(*) FROM pg_constraint k JOIN pg_class c ON c.oid = k.confrelid
          WHERE k.contype = 'f' AND (c.relkind = 'p' OR c.relispartition)) = 0,
        'retiring a partition can never be blocked by a referencing row');
END $$;

\echo '=== TEST 10: constrained vocabularies and shapes (D5, D9, D12) ==='
DO $$ BEGIN
    BEGIN
        INSERT INTO notification_outbox (notification_id, notification_created_at, realm,
                                         tenant_kind, tenant_id, recipient_kind, recipient_id,
                                         topic, priority, channel, shard)
        VALUES (gen_random_uuid(), '2026-09-15', 'aisat', 'workspace', 'w1', 'user', 'u1',
                't', 'URGENT!!', 'email', 0);
        RAISE EXCEPTION 'FAIL: a bogus priority was accepted';
    EXCEPTION WHEN check_violation THEN
        RAISE NOTICE 'PASS: priority is info | warning | critical';
    END;
    BEGIN
        INSERT INTO notification_deliveries (id, realm, tenant_kind, tenant_id, recipient_kind,
                                             recipient_id, topic, channel, address_key, outcome,
                                             attempts, completed_at)
        VALUES (gen_random_uuid(), 'aisat', 'workspace', 'w1', 'user', 'u1', 't', 'email', '',
                'whatever', 1, '2026-09-15');
        RAISE EXCEPTION 'FAIL: an arbitrary outcome was accepted';
    EXCEPTION WHEN check_violation THEN
        RAISE NOTICE 'PASS: a delivery outcome is one of the named terminal states';
    END;
    BEGIN
        INSERT INTO dead_letters (id, realm, source, reason, payload, attempts, last_error, created_at)
        VALUES (gen_random_uuid(), 'aisat', 'outbox', 'suppressed', '{}', 1, 'x', '2026-09-15');
        RAISE EXCEPTION 'FAIL: a correct outcome was accepted as a dead letter';
    EXCEPTION WHEN check_violation THEN
        RAISE NOTICE 'PASS: only genuine failures can be dead letters';
    END;
    BEGIN
        INSERT INTO notification_outbox (notification_id, notification_created_at, digest_id, realm,
                                         tenant_kind, tenant_id, recipient_kind, recipient_id,
                                         topic, channel, shard)
        VALUES (gen_random_uuid(), '2026-09-15', gen_random_uuid(), 'aisat', 'workspace', 'w1',
                'user', 'u1', 't', 'email', 0);
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
    -- The pair is whole or empty, in both directions (D5): a half-empty one would be
    -- neither a tenant's own quota nor the realm default, and no lookup would find it.
    BEGIN
        INSERT INTO channel_quotas (realm, tenant_kind, tenant_id, channel, max_per_hour, max_per_day)
        VALUES ('aisat', 'workspace', '', 'sms', 10, 100);
        RAISE EXCEPTION 'FAIL: a tenant kind with no tenant id was accepted as a quota';
    EXCEPTION WHEN check_violation THEN
        RAISE NOTICE 'PASS: a quota with a tenant kind and no id is rejected';
    END;
    BEGIN
        INSERT INTO channel_quotas (realm, tenant_kind, tenant_id, channel, max_per_hour, max_per_day)
        VALUES ('aisat', '', 'w1', 'sms', 10, 100);
        RAISE EXCEPTION 'FAIL: a tenant id with no tenant kind was accepted as a quota';
    EXCEPTION WHEN check_violation THEN
        RAISE NOTICE 'PASS: a quota with a tenant id and no kind is rejected';
    END;
    PERFORM pg_temp.accepts($q$INSERT INTO channel_quotas (realm, tenant_kind, tenant_id, channel, max_per_hour, max_per_day)
                                VALUES ('aisat', 'workspace', 'w1', 'sms', 10, 100)$q$,
                            'a whole tenant pair is that tenant''s own quota');
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
    -- One version of a template is active (D27): versions are immutable and activated by
    -- flipping `active`, so two active ones would make the rendering depend on row order.
    INSERT INTO notification_templates (realm, template_ref, channel, locale, version, body, active)
    VALUES ('aisat', 'welcome', 'email', 'en', 1, 'v1', true);
    BEGIN
        INSERT INTO notification_templates (realm, template_ref, channel, locale, version, body, active)
        VALUES ('aisat', 'welcome', 'email', 'en', 2, 'v2', true);
        RAISE EXCEPTION 'FAIL: two active versions of one template were accepted';
    EXCEPTION WHEN unique_violation THEN
        RAISE NOTICE 'PASS: only one version of a template is active';
    END;
    PERFORM pg_temp.accepts($q$INSERT INTO notification_templates (realm, template_ref, channel, locale, version, body, active)
                                VALUES ('aisat', 'welcome', 'email', 'en', 2, 'v2', false)$q$,
                            'an inactive version can sit beside the active one');
    PERFORM pg_temp.accepts($q$UPDATE notification_templates SET active = false
                                WHERE realm = 'aisat' AND template_ref = 'welcome' AND channel = 'email'
                                  AND locale = 'en' AND version = 1$q$,
                            'the active version can be retired');
    PERFORM pg_temp.accepts($q$UPDATE notification_templates SET active = true
                                WHERE realm = 'aisat' AND template_ref = 'welcome' AND channel = 'email'
                                  AND locale = 'en' AND version = 2$q$,
                            'a new version is activated once the old one is retired');
    -- The rule is per (tenant, template, channel, locale): each of these has its own.
    PERFORM pg_temp.accepts($q$INSERT INTO notification_templates (realm, template_ref, channel, locale, version, body, active)
                                VALUES ('aisat', 'welcome', 'email', 'fr', 1, 'fr', true)$q$,
                            'another locale has its own active version');
    PERFORM pg_temp.accepts($q$INSERT INTO notification_templates (realm, template_ref, channel, locale, version, body, active)
                                VALUES ('aisat', 'welcome', 'sms', 'en', 1, 'sms', true)$q$,
                            'another channel has its own active version');
    PERFORM pg_temp.accepts($q$INSERT INTO notification_templates (realm, template_ref, channel, locale, version, body, active)
                                VALUES ('aisat', 'other', 'email', 'en', 1, 'other', true)$q$,
                            'another template has its own active version');
    PERFORM pg_temp.accepts($q$INSERT INTO notification_templates (realm, tenant_kind, tenant_id, template_ref,
                                                                   channel, locale, version, body, active)
                                VALUES ('aisat', 'workspace', 'w1', 'welcome', 'email', 'en', 1, 'branded', true)$q$,
                            'a tenant''s override has its own active version');
    -- A broadcast has exactly one source of recipients (D31).
    BEGIN
        INSERT INTO notification_broadcasts (realm, tenant_kind, tenant_id, idem_key, request)
        VALUES ('aisat', 'workspace', 'w1', 'b-neither', '{}');
        RAISE EXCEPTION 'FAIL: a broadcast with no audience source was accepted';
    EXCEPTION WHEN check_violation THEN
        RAISE NOTICE 'PASS: a broadcast with no audience is rejected';
    END;
    BEGIN
        INSERT INTO notification_broadcasts (realm, tenant_kind, tenant_id, idem_key, audience, recipients, request)
        VALUES ('aisat', 'workspace', 'w1', 'b-both', 'all-members', '[{"kind":"user","id":"u1"}]', '{}');
        RAISE EXCEPTION 'FAIL: a broadcast with two audience sources was accepted';
    EXCEPTION WHEN check_violation THEN
        RAISE NOTICE 'PASS: a broadcast with two audiences is rejected';
    END;
    PERFORM pg_temp.accepts($q$INSERT INTO notification_broadcasts (realm, tenant_kind, tenant_id, idem_key, audience, request)
                                VALUES ('aisat', 'workspace', 'w1', 'b-selector', 'all-members', '{}')$q$,
                            'a broadcast can take a host selector as its audience');
    PERFORM pg_temp.accepts($q$INSERT INTO notification_broadcasts (realm, tenant_kind, tenant_id, idem_key, recipients, request)
                                VALUES ('aisat', 'workspace', 'w1', 'b-inline', '[{"kind":"user","id":"u1"}]', '{}')$q$,
                            'a broadcast can take an inline recipient list');
    -- An empty identity component is rejected on every recipient-scoped table: TEST 14.
END $$;

\echo '=== TEST 11: suppressions and erasure records hold hashes, not identities (D35) ==='
DO $$ BEGIN
    PERFORM pg_temp.expect(
        NOT EXISTS (SELECT 1 FROM information_schema.columns
                     WHERE table_name = 'channel_suppressions' AND column_name = 'address'),
        'suppressions hold an address hash, never the address, so they can survive erasure');
    PERFORM pg_temp.expect(
        NOT EXISTS (SELECT 1 FROM information_schema.columns
                     WHERE table_name = 'erasure_requests'
                       AND column_name IN ('tenant_id', 'recipient_id')),
        'the erasure record keeps a hash of the subject, not the subject');
    BEGIN
        INSERT INTO channel_suppressions (realm, channel, address_hash, reason)
        VALUES ('aisat', 'email', '\x0102'::bytea, 'complaint');
        RAISE EXCEPTION 'FAIL: a suppression key that is not a sha256 was accepted';
    EXCEPTION WHEN check_violation THEN
        RAISE NOTICE 'PASS: a suppression key is a 32-byte hash';
    END;
END $$;

\echo '=== TEST 12: a claim can be a HOT update (D19) ==='
DO $$ BEGIN
    -- An update is HOT only if it changes no indexed column, and a column named in an
    -- index's predicate or expressions counts. The columns a claim writes must be in none.
    PERFORM pg_temp.expect(
        NOT EXISTS (
            SELECT 1
              FROM pg_index i
              JOIN pg_attribute a ON a.attrelid = i.indrelid
             WHERE i.indrelid = 'notification_outbox'::regclass
               AND a.attname IN ('attempts', 'lease_token', 'claimed_at', 'lease_expires_at')
               AND (a.attnum = ANY (i.indkey::int2[])
                    OR coalesce(pg_get_expr(i.indpred,  i.indrelid), '') ~ ('\m' || a.attname || '\M')
                    OR coalesce(pg_get_expr(i.indexprs, i.indrelid), '') ~ ('\m' || a.attname || '\M'))),
        'no index covers a column a claim writes, so a claim can be a HOT update');
    -- ...and the page needs room for the new row version, or the update is not HOT.
    PERFORM pg_temp.expect(
        coalesce((SELECT split_part(o, '=', 2)::int
                    FROM pg_class c, unnest(c.reloptions) AS o
                   WHERE c.oid = 'notification_outbox'::regclass AND o LIKE 'fillfactor=%'), 100) < 100,
        'the queue table leaves free space in each page for a HOT update');
END $$;

\echo '=== TEST 13: no default partition; a date with no partition is refused (D33, NR-022) ==='
-- Insert one row of each range-partitioned table, dated d, holding nothing else invalid.
CREATE FUNCTION pg_temp.put_dated(p_table text, d timestamptz) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_temp.scope('aisat', 'workspace', 'w13', 'user', 'u13');
    IF p_table = 'notifications' THEN
        INSERT INTO notifications (id, realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
                                   topic, idem_key, created_at)
        VALUES (gen_random_uuid(), 'aisat', 'workspace', 'w13', 'user', 'u13', 't13', 'k13', d);
    ELSIF p_table = 'notification_deliveries' THEN
        INSERT INTO notification_deliveries (id, realm, tenant_kind, tenant_id, recipient_kind,
                                             recipient_id, topic, channel, address_key, outcome,
                                             attempts, completed_at)
        VALUES (gen_random_uuid(), 'aisat', 'workspace', 'w13', 'user', 'u13', 't13', 'email', '',
                'delivered', 1, d);
    ELSIF p_table = 'dead_letters' THEN
        INSERT INTO dead_letters (id, realm, source, reason, payload, attempts, last_error, created_at)
        VALUES (gen_random_uuid(), 'aisat', 'outbox', 'max_attempts', '{"t":"t13"}', 1, 'x', d);
    ELSE
        RAISE EXCEPTION 'FAIL: no insert for range-partitioned table % -- add one to put_dated', p_table;
    END IF;
END $$;
DO $$
DECLARE tbl text; d timestamptz;
BEGIN
    -- A default partition would catch a missing month silently, fill up, and never be
    -- retirable by a metadata operation. So none exists, and a row with nowhere to go fails.
    PERFORM pg_temp.expect(
        NOT EXISTS (SELECT 1 FROM pg_partitioned_table WHERE partdefid <> 0),
        'no partitioned table has a default partition'
        || coalesce(' -- DEFAULT on: ' || (SELECT string_agg(partrelid::regclass::text, ', ')
                                             FROM pg_partitioned_table WHERE partdefid <> 0), ''));

    -- The range-partitioned tables come from the catalog, so a new one is decided here.
    PERFORM pg_temp.expect(
        (SELECT array_agg(c.relname::text ORDER BY c.relname) FROM pg_partitioned_table p
           JOIN pg_class c ON c.oid = p.partrelid WHERE p.partstrat = 'r')
        = ARRAY['dead_letters', 'notification_deliveries', 'notifications'],
        'the range-partitioned tables are the three this test inserts into');

    FOREACH tbl IN ARRAY ARRAY['notifications', 'notification_deliveries', 'dead_letters'] LOOP
        PERFORM pg_temp.accepts(format('SELECT pg_temp.put_dated(%L, %L)', tbl, '2026-09-15'),
                                tbl || ' accepts a row dated inside a partition');   -- the control
        FOREACH d IN ARRAY ARRAY['2001-01-15', '2099-01-15']::timestamptz[] LOOP
            BEGIN
                PERFORM pg_temp.put_dated(tbl, d);
                RAISE EXCEPTION 'FAIL: % accepted a row dated % with no partition to hold it', tbl, d;
            EXCEPTION WHEN check_violation THEN
                PERFORM pg_temp.expect(SQLERRM LIKE 'no partition of relation "' || tbl || '" found for row%',
                    format('%s refuses a row dated %s: %s', tbl, d::date, SQLERRM));
            END;
        END LOOP;
    END LOOP;
    DELETE FROM notifications WHERE idem_key = 'k13';
    DELETE FROM notification_deliveries WHERE topic = 't13';
    DELETE FROM dead_letters WHERE payload = '{"t":"t13"}';
END $$;

\echo '=== TEST 14: every recipient-scoped table rejects an empty identity component (NS-001) ==='
-- An empty component would equal an unset scope setting, which reads back as '' once any
-- transaction on the connection has set it, so the policy could match the row. The CHECK
-- keeps RLS fail-closed. Every table carrying the recipient_scope policy is found from
-- the catalog, and each is given a valid row and then, once per component, the same row
-- with that component empty.
--
-- A literal for each type a required column can have. An unknown type fails, so a new
-- scoped table with an exotic NOT NULL column has to be decided here, not skipped.
CREATE FUNCTION pg_temp.sample(p_type text, p_what text) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE v text;
BEGIN
    v := CASE p_type
        WHEN 'text'                     THEN quote_literal('x')
        WHEN 'uuid'                     THEN 'gen_random_uuid()'
        WHEN 'boolean'                  THEN 'true'
        WHEN 'integer'                  THEN '1'
        WHEN 'smallint'                 THEN '1'
        WHEN 'jsonb'                    THEN quote_literal('{}') || '::jsonb'
        WHEN 'timestamp with time zone' THEN quote_literal('2026-09-15') || '::timestamptz'
    END;
    IF v IS NULL THEN
        RAISE EXCEPTION 'FAIL: no sample value for type % of % -- extend pg_temp.sample', p_type, p_what;
    END IF;
    RETURN v;
END $$;
DO $$
DECLARE
    t     record;
    comps text[] := ARRAY['realm', 'tenant_kind', 'tenant_id', 'recipient_kind', 'recipient_id'];
    good  text[] := ARRAY['aisat', 'workspace', 'w14', 'user', 'u14'];
    ident text[];
    cols  text;
    vals  text;
    found text[] := '{}';
    i     int;
BEGIN
    FOR t IN
        SELECT c.oid, c.relname::text AS name FROM pg_class c
         WHERE c.relnamespace = 'public'::regnamespace AND c.relkind IN ('r', 'p') AND NOT c.relispartition
           AND EXISTS (SELECT 1 FROM pg_policy p WHERE p.polrelid = c.oid AND p.polname = 'recipient_scope')
         ORDER BY c.relname
    LOOP
        found := found || t.name;
        -- What a row needs beyond its identity: every NOT NULL column with no default, and
        -- the partition key, so the row never lands where no partition is.
        SELECT string_agg(', ' || quote_ident(a.attname), '' ORDER BY a.attnum),
               string_agg(', ' || pg_temp.sample(format_type(a.atttypid, a.atttypmod), t.name || '.' || a.attname),
                          '' ORDER BY a.attnum)
          INTO cols, vals
          FROM pg_attribute a
         WHERE a.attrelid = t.oid AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated = ''
           AND a.attname <> ALL (comps)
           AND ((a.attnotnull AND NOT a.atthasdef)
                OR a.attnum IN (SELECT unnest(p.partattrs::int2[]) FROM pg_partitioned_table p
                                 WHERE p.partrelid = t.oid));

        FOR i IN 0..5 LOOP    -- 0 is the control: every component valid
            ident := good;
            IF i > 0 THEN ident[i] := ''; END IF;
            PERFORM pg_temp.scope(ident[1], ident[2], ident[3], ident[4], ident[5]);
            BEGIN
                EXECUTE format('INSERT INTO %I (realm, tenant_kind, tenant_id, recipient_kind, recipient_id%s) '
                               'VALUES (%L, %L, %L, %L, %L%s)',
                               t.name, coalesce(cols, ''), ident[1], ident[2], ident[3], ident[4], ident[5],
                               coalesce(vals, ''));
                IF i > 0 THEN
                    RAISE EXCEPTION 'FAIL: % accepted an empty %', t.name, comps[i];
                END IF;
                RAISE NOTICE 'PASS: % accepts a row with every component set', t.name;
                EXECUTE format('DELETE FROM %I WHERE recipient_id = %L', t.name, 'u14');
            EXCEPTION WHEN check_violation THEN
                IF i = 0 THEN
                    RAISE EXCEPTION 'FAIL: % rejected a valid row: %', t.name, SQLERRM;
                END IF;
                RAISE NOTICE 'PASS: % rejects an empty %', t.name, comps[i];
            END;
        END LOOP;
    END LOOP;
    -- Not vacuous: the inbox is among the tables the catalog turned up.
    PERFORM pg_temp.expect('notifications' = ANY (found),
        'the recipient-scoped tables found in the catalog: ' || array_to_string(found, ', '));
END $$;

\echo '=== TEST 15: at most one pending delivery per address (NS-002, D25) ==='
CREATE FUNCTION pg_temp.ob(p_notification uuid, p_digest uuid, p_channel text, p_address text)
RETURNS void LANGUAGE sql AS $$
    INSERT INTO notification_outbox (notification_id, notification_created_at, digest_id, realm,
                                     tenant_kind, tenant_id, recipient_kind, recipient_id,
                                     topic, channel, address_key, shard)
    VALUES (p_notification, CASE WHEN p_notification IS NULL THEN NULL ELSE '2026-09-15'::timestamptz END,
            p_digest, 'aisat', 'workspace', 'w15', 'user', 'u15', 't15', p_channel, p_address, 0);
$$;
DO $$
DECLARE n uuid := gen_random_uuid(); d uuid := gen_random_uuid();
BEGIN
    -- A notification: one row per (notification, channel, address).
    PERFORM pg_temp.ob(n, NULL, 'push', 'phone');
    BEGIN
        PERFORM pg_temp.ob(n, NULL, 'push', 'phone');
        RAISE EXCEPTION 'FAIL: a second pending delivery to one address was accepted';
    EXCEPTION WHEN unique_violation THEN
        RAISE NOTICE 'PASS: one pending delivery per (notification, channel, address)';
    END;
    PERFORM pg_temp.accepts(format('SELECT pg_temp.ob(%L, NULL, %L, %L)', n, 'push', 'tablet'),
        'another address on the channel is its own delivery (multi-device push)');
    PERFORM pg_temp.accepts(format('SELECT pg_temp.ob(%L, NULL, %L, %L)', n, 'sms', 'phone'),
        'the same address key on another channel is its own delivery');
    PERFORM pg_temp.accepts(format('SELECT pg_temp.ob(%L, NULL, %L, %L)', gen_random_uuid(), 'push', 'phone'),
        'another notification to the same address is its own delivery');
    -- A digest window: one row per (digest, address).
    PERFORM pg_temp.ob(NULL, d, 'email', 'a1');
    BEGIN
        PERFORM pg_temp.ob(NULL, d, 'email', 'a1');
        RAISE EXCEPTION 'FAIL: a second pending delivery of one digest to one address was accepted';
    EXCEPTION WHEN unique_violation THEN
        RAISE NOTICE 'PASS: one pending delivery per (digest, address)';
    END;
    PERFORM pg_temp.accepts(format('SELECT pg_temp.ob(NULL, %L, %L, %L)', d, 'email', 'a2'),
        'another address of the digest is its own delivery');
    PERFORM pg_temp.accepts(format('SELECT pg_temp.ob(NULL, %L, %L, %L)', gen_random_uuid(), 'email', 'a1'),
        'another digest to the same address is its own delivery');
    PERFORM pg_temp.accepts(format('SELECT pg_temp.ob(%L, NULL, %L, %L)', gen_random_uuid(), 'email', 'a1'),
        'a notification and a digest to one address do not collide');
    DELETE FROM notification_outbox WHERE topic = 't15';
END $$;

RESET ROLE;
\echo '=== all schema guarantees hold ==='
