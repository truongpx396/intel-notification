-- 0004_state_transitions.sql — the Store's state machine, as functions.
--
-- Verified against PostgreSQL 16.
--
-- Every transition whose correctness depends on doing several writes atomically,
-- or on a fencing check, lives here rather than in adapter code: the claim, the
-- outcome writes, the terminal move from queue to history, per-address fan-out,
-- bulk tenant deferral, digest append and flush, cancel, expiry and erasure.
-- One implementation, shared by every Store adapter and exercised directly by
-- `make verify-schema`, so the guarantee and its assertion cannot drift apart.
--
-- All functions are SECURITY INVOKER: they run with the caller's privileges and
-- under the caller's row-level security, never around it.

BEGIN;

-- ---------------------------------------------------------------------------
-- notify_canonical — the one unambiguous encoding of an identity tuple.
--
-- Each part is written as <octet length>:<part>, so ('a:b','c') and ('a','b:c')
-- can never encode alike. Joining opaque ids with a separator -- the inherited
-- Tenant.Tag() = kind + ":" + id -- collides as soon as an id contains the
-- separator, and a collision in a dedup or pub/sub key is a dropped or leaked
-- notification (D16, D18). The Go domain implements the identical encoding and
-- tests it against this function.
-- ---------------------------------------------------------------------------
CREATE FUNCTION notify_canonical(VARIADIC parts text[]) RETURNS text
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
    SELECT coalesce(string_agg(octet_length(p)::text || ':' || p, ',' ORDER BY ord), '')
      FROM unnest(parts) WITH ORDINALITY AS u(p, ord);
$$;

-- ---------------------------------------------------------------------------
-- notify_claim_outbox — take up to p_limit due deliveries from one shard (D19).
--
-- FOR UPDATE SKIP LOCKED makes the claim safe under any number of concurrent
-- claimers: two workers on one shard, a rolling deploy, a shard-count change.
-- The claim is a LEASE: next_attempt_at moves to now() + p_lease, so a worker
-- that dies holding rows releases them when the lease lapses, and a fresh
-- lease_token fences its late writes out. attempts is incremented here, not on
-- failure, so a delivery that crashes its worker still reaches the ceiling.
-- ---------------------------------------------------------------------------
CREATE FUNCTION notify_claim_outbox(p_shard smallint, p_limit int, p_lease interval)
RETURNS SETOF notification_outbox
LANGUAGE sql AS $$
    WITH due AS (
        SELECT id
          FROM notification_outbox
         WHERE shard = p_shard
           AND next_attempt_at <= now()
         ORDER BY next_attempt_at
         LIMIT p_limit
           FOR UPDATE SKIP LOCKED
    )
    UPDATE notification_outbox o
       SET attempts        = o.attempts + 1,
           lease_token     = gen_random_uuid(),
           claimed_at      = now(),
           next_attempt_at = now() + p_lease
      FROM due
     WHERE o.id = due.id
    RETURNING o.*;
$$;

-- ---------------------------------------------------------------------------
-- notify_retry_delivery — a transient failure: due again at p_next.
-- Returns false when the lease was lost (the caller's write is discarded).
-- ---------------------------------------------------------------------------
CREATE FUNCTION notify_retry_delivery(p_id uuid, p_lease uuid, p_next timestamptz, p_error text)
RETURNS boolean
LANGUAGE sql AS $$
    WITH x AS (
        UPDATE notification_outbox
           SET next_attempt_at = p_next, last_error = p_error, lease_token = NULL
         WHERE id = p_id AND lease_token = p_lease
        RETURNING 1
    )
    SELECT EXISTS (SELECT 1 FROM x);
$$;

-- ---------------------------------------------------------------------------
-- notify_defer_delivery — quota exhausted or quiet hours: due again at p_until,
-- and the claim's attempt is given back. Deferral is not failure, so it can
-- never walk a delivery into the dead-letter table (D24).
-- ---------------------------------------------------------------------------
CREATE FUNCTION notify_defer_delivery(p_id uuid, p_lease uuid, p_until timestamptz, p_reason text)
RETURNS boolean
LANGUAGE sql AS $$
    WITH x AS (
        UPDATE notification_outbox
           SET next_attempt_at = p_until,
               attempts        = greatest(attempts - 1, 0),
               deferrals       = deferrals + 1,
               last_error      = p_reason,
               lease_token     = NULL
         WHERE id = p_id AND lease_token = p_lease
        RETURNING 1
    )
    SELECT EXISTS (SELECT 1 FROM x);
$$;

-- ---------------------------------------------------------------------------
-- notify_finish_delivery — the terminal transition (D12, D20, D29).
--
-- In ONE transaction: remove the queue row (fenced by the lease), append it to
-- notification_deliveries, dead-letter it if it is a genuine failure, and
-- enqueue the next channel of its fallback chain if it ended undelivered.
-- Returns false when fenced out: another claim owns the row, or it already
-- finished.
-- ---------------------------------------------------------------------------
CREATE FUNCTION notify_finish_delivery(p_id uuid, p_lease uuid, p_outcome text,
                                       p_provider_message_id text DEFAULT NULL,
                                       p_detail text DEFAULT NULL)
RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE
    o notification_outbox;
BEGIN
    IF p_outcome = 'fanned_out' THEN
        RAISE EXCEPTION 'fanned_out is recorded by notify_bind_addresses, not by a finish';
    END IF;

    DELETE FROM notification_outbox
     WHERE id = p_id AND lease_token = p_lease
    RETURNING * INTO o;
    IF NOT FOUND THEN
        RETURN false;
    END IF;

    INSERT INTO notification_deliveries (
        id, notification_id, notification_created_at, digest_id,
        realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
        topic, channel, address_key, outcome, provider_message_id,
        attempts, deferrals, quiet_hours_override, detail)
    VALUES (
        o.id, o.notification_id, o.notification_created_at, o.digest_id,
        o.realm, o.tenant_kind, o.tenant_id, o.recipient_kind, o.recipient_id,
        o.topic, o.channel, o.address_key, p_outcome, p_provider_message_id,
        o.attempts, o.deferrals, o.quiet_hours_override, coalesce(p_detail, o.last_error));

    -- Only genuine failures are dead letters. Suppressed, no_address, expired,
    -- canceled and dropped_quota are correct outcomes, and an alarm on them
    -- would page someone for the engine doing its job.
    IF p_outcome IN ('max_attempts', 'rejected') THEN
        INSERT INTO dead_letters (
            id, realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
            source, outbox_id, notification_id, digest_id, channel, reason,
            payload, attempts, last_error)
        VALUES (
            gen_random_uuid(), o.realm, o.tenant_kind, o.tenant_id, o.recipient_kind,
            o.recipient_id, 'outbox', o.id, o.notification_id, o.digest_id, o.channel,
            p_outcome, to_jsonb(o), o.attempts,
            coalesce(p_detail, o.last_error, p_outcome));
    END IF;

    -- Undelivered on this channel: try the next one in the chain.
    IF p_outcome IN ('suppressed', 'no_address', 'max_attempts', 'rejected')
       AND cardinality(o.fallback) > 0 THEN
        INSERT INTO notification_outbox (
            notification_id, notification_created_at, digest_id,
            realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
            topic, priority, channel, fallback, shard, deliver_before,
            quiet_hours_override)
        VALUES (
            o.notification_id, o.notification_created_at, o.digest_id,
            o.realm, o.tenant_kind, o.tenant_id, o.recipient_kind, o.recipient_id,
            o.topic, o.priority, o.fallback[1], o.fallback[2:], o.shard, o.deliver_before,
            o.quiet_hours_override)
        ON CONFLICT (notification_id, channel, address_key)
            WHERE notification_id IS NOT NULL DO NOTHING;
    END IF;

    RETURN true;
END $$;

-- ---------------------------------------------------------------------------
-- notify_bind_addresses — pin a delivery to its resolved address(es) (D25).
--
-- p_addresses is a JSON array of {"key": <address_key>, "address": {...}}.
-- One address: the row is bound in place and stays leased, so the caller goes
-- on to deliver it. Several: the row is replaced by one unleased row per
-- address, each with its own attempts and backoff, and recorded as
-- 'fanned_out' -- so a failure on one device never re-sends to the others.
-- Returns the number of addresses bound, or NULL when fenced out.
-- ---------------------------------------------------------------------------
CREATE FUNCTION notify_bind_addresses(p_id uuid, p_lease uuid, p_addresses jsonb)
RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    o notification_outbox;
    n int := jsonb_array_length(p_addresses);
BEGIN
    IF n = 0 THEN
        RAISE EXCEPTION 'no addresses: finish the delivery as no_address instead';
    END IF;

    IF n = 1 THEN
        UPDATE notification_outbox
           SET address_key = p_addresses -> 0 ->> 'key',
               address     = p_addresses -> 0 -> 'address'
         WHERE id = p_id AND lease_token = p_lease AND address_key = '';
        RETURN CASE WHEN FOUND THEN 1 END;
    END IF;

    DELETE FROM notification_outbox
     WHERE id = p_id AND lease_token = p_lease AND address_key = ''
    RETURNING * INTO o;
    IF NOT FOUND THEN
        RETURN NULL;
    END IF;

    -- Children carry no fallback: one device failing is not the channel failing.
    INSERT INTO notification_outbox (
        notification_id, notification_created_at, digest_id,
        realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
        topic, priority, channel, address_key, address, shard,
        deliver_before, quiet_hours_override)
    SELECT o.notification_id, o.notification_created_at, o.digest_id,
           o.realm, o.tenant_kind, o.tenant_id, o.recipient_kind, o.recipient_id,
           o.topic, o.priority, o.channel, a ->> 'key', a -> 'address', o.shard,
           o.deliver_before, o.quiet_hours_override
      FROM jsonb_array_elements(p_addresses) AS a;

    INSERT INTO notification_deliveries (
        id, notification_id, notification_created_at, digest_id,
        realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
        topic, channel, address_key, outcome, attempts, deferrals,
        quiet_hours_override, detail)
    VALUES (
        o.id, o.notification_id, o.notification_created_at, o.digest_id,
        o.realm, o.tenant_kind, o.tenant_id, o.recipient_kind, o.recipient_id,
        o.topic, o.channel, o.address_key, 'fanned_out', o.attempts, o.deferrals,
        o.quiet_hours_override, n || ' addresses');

    RETURN n;
END $$;

-- ---------------------------------------------------------------------------
-- notify_defer_tenant_channel — move a tenant's whole due backlog on one channel
-- out of the claim range until p_until (D24).
--
-- Without this a tenant that exhausted its quota still sits at the head of every
-- shard, and drainers spend their throughput claiming and deferring its rows one
-- by one while other tenants wait. One statement takes the backlog out of the
-- way. Rows under a live lease are left to their claimer.
-- ---------------------------------------------------------------------------
CREATE FUNCTION notify_defer_tenant_channel(p_realm text, p_tenant_kind text, p_tenant_id text,
                                            p_channel text, p_until timestamptz)
RETURNS int
LANGUAGE sql AS $$
    WITH x AS (
        UPDATE notification_outbox
           SET next_attempt_at = p_until,
               deferrals       = deferrals + 1,
               lease_token     = NULL
         WHERE realm = p_realm AND tenant_kind = p_tenant_kind AND tenant_id = p_tenant_id
           AND channel = p_channel
           AND next_attempt_at <= now()
        RETURNING 1
    )
    SELECT count(*)::int FROM x;
$$;

-- ---------------------------------------------------------------------------
-- notify_rehome_shards — after LOWERING Config.Shards, fold rows in retired
-- shards into live ones (D11). Raising the count needs nothing. Safe to run
-- while drainers work: it does not touch leases, and the claim never depended
-- on shard ownership.
-- ---------------------------------------------------------------------------
CREATE FUNCTION notify_rehome_shards(p_shards int)
RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    n int;
    m int;
BEGIN
    IF p_shards < 1 THEN
        RAISE EXCEPTION 'shard count must be >= 1';
    END IF;
    UPDATE notification_outbox SET shard = shard % p_shards WHERE shard >= p_shards;
    GET DIAGNOSTICS n = ROW_COUNT;
    UPDATE digest_buffer SET shard = shard % p_shards WHERE shard >= p_shards;
    GET DIAGNOSTICS m = ROW_COUNT;
    RETURN n + m;
END $$;

-- ---------------------------------------------------------------------------
-- notify_digest_append — fold one notification into its open window (D4).
--
-- Opens a window if none is open. Seals the window when it reaches p_max
-- members, and pulls its flush_at forward so the next tick sends it; the next
-- member then opens a fresh window. So a burst of N produces ceil(N / p_max)
-- digests, never an unbounded one. Returns the window id.
-- ---------------------------------------------------------------------------
CREATE FUNCTION notify_digest_append(p_realm text, p_tenant_kind text, p_tenant_id text,
                                     p_recipient_kind text, p_recipient_id text,
                                     p_topic text, p_channel text, p_shard smallint,
                                     p_member uuid, p_window interval, p_max int)
RETURNS uuid
LANGUAGE plpgsql AS $$
DECLARE
    v_id uuid;
BEGIN
    IF p_max < 1 THEN
        RAISE EXCEPTION 'digest max must be >= 1';
    END IF;
    LOOP
        UPDATE digest_buffer
           SET member_ids   = member_ids || p_member,
               member_count = member_count + 1,
               sealed_at    = CASE WHEN member_count + 1 >= p_max THEN now() END,
               flush_at     = CASE WHEN member_count + 1 >= p_max THEN now() ELSE flush_at END
         WHERE realm = p_realm AND tenant_kind = p_tenant_kind AND tenant_id = p_tenant_id
           AND recipient_kind = p_recipient_kind AND recipient_id = p_recipient_id
           AND topic = p_topic AND channel = p_channel
           AND sealed_at IS NULL
        RETURNING id INTO v_id;
        IF FOUND THEN
            RETURN v_id;
        END IF;

        BEGIN
            INSERT INTO digest_buffer (
                realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
                topic, channel, shard, member_ids, member_count, flush_at, sealed_at)
            VALUES (
                p_realm, p_tenant_kind, p_tenant_id, p_recipient_kind, p_recipient_id,
                p_topic, p_channel, p_shard, ARRAY[p_member], 1,
                CASE WHEN p_max = 1 THEN now() ELSE now() + p_window END,
                CASE WHEN p_max = 1 THEN now() END)
            RETURNING id INTO v_id;
            RETURN v_id;
        EXCEPTION WHEN unique_violation THEN
            -- A concurrent append opened the window first. Loop and join it.
        END;
    END LOOP;
END $$;

-- ---------------------------------------------------------------------------
-- notify_digest_flush — seal a due window and enqueue its ONE delivery.
-- Conditional on flushed_at IS NULL, so a duplicate tick is a no-op rather than
-- a second digest (NS-005). Returns the queue row id, or NULL if already flushed.
-- ---------------------------------------------------------------------------
CREATE FUNCTION notify_digest_flush(p_id uuid)
RETURNS uuid
LANGUAGE plpgsql AS $$
DECLARE
    w digest_buffer;
    v uuid := gen_random_uuid();
BEGIN
    UPDATE digest_buffer
       SET sealed_at = coalesce(sealed_at, now()), flushed_at = now()
     WHERE id = p_id AND flushed_at IS NULL
    RETURNING * INTO w;
    IF NOT FOUND THEN
        RETURN NULL;
    END IF;

    INSERT INTO notification_outbox (
        id, digest_id, realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
        topic, channel, shard)
    VALUES (
        v, w.id, w.realm, w.tenant_kind, w.tenant_id, w.recipient_kind, w.recipient_id,
        w.topic, w.channel, w.shard);
    RETURN v;
END $$;

-- ---------------------------------------------------------------------------
-- notify_cancel — withdraw one notification (D29).
--
-- Marks the inbox row canceled and finishes every pending delivery that no
-- worker is holding as 'canceled'. A delivery under a live lease may already be
-- at the provider; it is reported as in-flight rather than falsely recorded as
-- canceled -- a sent notification cannot be unsent, and the record must not
-- pretend otherwise. Runs under the recipient's scope, which it sets itself.
-- ---------------------------------------------------------------------------
CREATE FUNCTION notify_cancel(p_realm text, p_tenant_kind text, p_tenant_id text,
                              p_recipient_kind text, p_recipient_id text, p_idem_key text)
RETURNS TABLE (matched boolean, canceled int, in_flight int)
LANGUAGE plpgsql AS $$
DECLARE
    g notify_idem;
    v_canceled int := 0;
    v_in_flight int := 0;
BEGIN
    SELECT * INTO g FROM notify_idem
     WHERE realm = p_realm AND tenant_kind = p_tenant_kind AND tenant_id = p_tenant_id
       AND recipient_kind = p_recipient_kind AND recipient_id = p_recipient_id
       AND idem_key = p_idem_key;
    IF NOT FOUND THEN
        RETURN QUERY SELECT false, 0, 0;
        RETURN;
    END IF;

    PERFORM set_config('notify.realm', p_realm, true),
            set_config('notify.tenant_kind', p_tenant_kind, true),
            set_config('notify.tenant_id', p_tenant_id, true),
            set_config('notify.recipient_kind', p_recipient_kind, true),
            set_config('notify.recipient_id', p_recipient_id, true);

    UPDATE notifications SET canceled_at = now()
     WHERE id = g.notification_id AND created_at = g.notification_created_at
       AND canceled_at IS NULL;

    WITH gone AS (
        DELETE FROM notification_outbox
         WHERE notification_id = g.notification_id
           AND (lease_token IS NULL OR next_attempt_at <= now())
        RETURNING *
    ), logged AS (
        INSERT INTO notification_deliveries (
            id, notification_id, notification_created_at, digest_id,
            realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
            topic, channel, address_key, outcome, attempts, deferrals,
            quiet_hours_override)
        SELECT id, notification_id, notification_created_at, digest_id,
               realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
               topic, channel, address_key, 'canceled', attempts, deferrals,
               quiet_hours_override
          FROM gone
        RETURNING 1
    )
    SELECT count(*) INTO v_canceled FROM logged;

    SELECT count(*) INTO v_in_flight FROM notification_outbox
     WHERE notification_id = g.notification_id;

    RETURN QUERY SELECT true, v_canceled, v_in_flight;
END $$;

-- ---------------------------------------------------------------------------
-- notify_expire_idem — delete idempotency keys older than the window (D21), in
-- batches, so expiry never holds a long lock or produces one huge transaction.
-- Joins on the primary key rather than ctid: ctid is not unique across the
-- table's partitions.
-- ---------------------------------------------------------------------------
CREATE FUNCTION notify_expire_idem(p_older_than timestamptz, p_limit int)
RETURNS int
LANGUAGE sql AS $$
    WITH victims AS (
        SELECT realm, tenant_kind, tenant_id, recipient_kind, recipient_id, idem_key
          FROM notify_idem
         WHERE created_at < p_older_than
         LIMIT p_limit
    ), gone AS (
        DELETE FROM notify_idem d
         USING victims v
         WHERE d.realm = v.realm AND d.tenant_kind = v.tenant_kind
           AND d.tenant_id = v.tenant_id AND d.recipient_kind = v.recipient_kind
           AND d.recipient_id = v.recipient_id AND d.idem_key = v.idem_key
        RETURNING 1
    )
    SELECT count(*)::int FROM gone;
$$;

-- ---------------------------------------------------------------------------
-- notify_expire_digests — delete flushed windows older than p_older_than whose
-- delivery has finished. digest_buffer is otherwise one more table that only
-- ever grows.
-- ---------------------------------------------------------------------------
CREATE FUNCTION notify_expire_digests(p_older_than timestamptz, p_limit int)
RETURNS int
LANGUAGE sql AS $$
    WITH victims AS (
        SELECT d.id
          FROM digest_buffer d
         WHERE d.flushed_at < p_older_than
           AND NOT EXISTS (SELECT 1 FROM notification_outbox o WHERE o.digest_id = d.id)
         LIMIT p_limit
    ), gone AS (
        DELETE FROM digest_buffer d USING victims v WHERE d.id = v.id RETURNING 1
    )
    SELECT count(*)::int FROM gone;
$$;

-- ---------------------------------------------------------------------------
-- notify_try_lease_job — take (or renew) the single-owner lease for a job (D22).
-- True when p_owner now holds it; false while another owner's lease is live.
-- ---------------------------------------------------------------------------
CREATE FUNCTION notify_try_lease_job(p_job text, p_owner text, p_ttl interval)
RETURNS boolean
LANGUAGE sql AS $$
    WITH x AS (
        INSERT INTO notify_job_leases AS l (job, owner, lease_expires_at, last_started_at)
        VALUES (p_job, p_owner, now() + p_ttl, now())
        ON CONFLICT (job) DO UPDATE
           SET owner            = EXCLUDED.owner,
               lease_expires_at = EXCLUDED.lease_expires_at,
               last_started_at  = CASE WHEN l.owner = EXCLUDED.owner
                                       THEN l.last_started_at ELSE EXCLUDED.last_started_at END
         WHERE l.lease_expires_at < now() OR l.owner = EXCLUDED.owner
        RETURNING 1
    )
    SELECT EXISTS (SELECT 1 FROM x);
$$;

-- ---------------------------------------------------------------------------
-- notify_erase_recipient — remove one recipient's personal data (D35).
--
-- Deletes the recipient's rows from every table that holds them, in one
-- transaction and under the recipient's own scope (so RLS applies to the
-- erasure too), and records the erasure by hash. Suppressions are kept: they
-- are keyed by address hash, not identity, and dropping them would resume
-- mailing someone who complained.
-- ---------------------------------------------------------------------------
CREATE FUNCTION notify_erase_recipient(p_realm text, p_tenant_kind text, p_tenant_id text,
                                       p_recipient_kind text, p_recipient_id text)
RETURNS jsonb
LANGUAGE plpgsql AS $$
DECLARE
    c jsonb := '{}'::jsonb;
    n int;
BEGIN
    PERFORM set_config('notify.realm', p_realm, true),
            set_config('notify.tenant_kind', p_tenant_kind, true),
            set_config('notify.tenant_id', p_tenant_id, true),
            set_config('notify.recipient_kind', p_recipient_kind, true),
            set_config('notify.recipient_id', p_recipient_id, true);

    DELETE FROM notifications
     WHERE realm = p_realm AND tenant_kind = p_tenant_kind AND tenant_id = p_tenant_id
       AND recipient_kind = p_recipient_kind AND recipient_id = p_recipient_id;
    GET DIAGNOSTICS n = ROW_COUNT; c := c || jsonb_build_object('notifications', n);

    DELETE FROM notify_idem
     WHERE realm = p_realm AND tenant_kind = p_tenant_kind AND tenant_id = p_tenant_id
       AND recipient_kind = p_recipient_kind AND recipient_id = p_recipient_id;
    GET DIAGNOSTICS n = ROW_COUNT; c := c || jsonb_build_object('notify_idem', n);

    DELETE FROM notification_outbox
     WHERE realm = p_realm AND tenant_kind = p_tenant_kind AND tenant_id = p_tenant_id
       AND recipient_kind = p_recipient_kind AND recipient_id = p_recipient_id;
    GET DIAGNOSTICS n = ROW_COUNT; c := c || jsonb_build_object('notification_outbox', n);

    DELETE FROM notification_deliveries
     WHERE realm = p_realm AND tenant_kind = p_tenant_kind AND tenant_id = p_tenant_id
       AND recipient_kind = p_recipient_kind AND recipient_id = p_recipient_id;
    GET DIAGNOSTICS n = ROW_COUNT; c := c || jsonb_build_object('notification_deliveries', n);

    DELETE FROM dead_letters
     WHERE realm = p_realm AND tenant_kind = p_tenant_kind AND tenant_id = p_tenant_id
       AND recipient_kind = p_recipient_kind AND recipient_id = p_recipient_id;
    GET DIAGNOSTICS n = ROW_COUNT; c := c || jsonb_build_object('dead_letters', n);

    DELETE FROM digest_buffer
     WHERE realm = p_realm AND tenant_kind = p_tenant_kind AND tenant_id = p_tenant_id
       AND recipient_kind = p_recipient_kind AND recipient_id = p_recipient_id;
    GET DIAGNOSTICS n = ROW_COUNT; c := c || jsonb_build_object('digest_buffer', n);

    DELETE FROM notification_preferences
     WHERE realm = p_realm AND tenant_kind = p_tenant_kind AND tenant_id = p_tenant_id
       AND recipient_kind = p_recipient_kind AND recipient_id = p_recipient_id;
    GET DIAGNOSTICS n = ROW_COUNT; c := c || jsonb_build_object('notification_preferences', n);

    DELETE FROM notification_schedules
     WHERE realm = p_realm AND tenant_kind = p_tenant_kind AND tenant_id = p_tenant_id
       AND recipient_kind = p_recipient_kind AND recipient_id = p_recipient_id;
    GET DIAGNOSTICS n = ROW_COUNT; c := c || jsonb_build_object('notification_schedules', n);

    DELETE FROM recipient_addresses
     WHERE realm = p_realm AND tenant_kind = p_tenant_kind AND tenant_id = p_tenant_id
       AND recipient_kind = p_recipient_kind AND recipient_id = p_recipient_id;
    GET DIAGNOSTICS n = ROW_COUNT; c := c || jsonb_build_object('recipient_addresses', n);

    INSERT INTO erasure_requests (realm, subject_hash, counts, completed_at)
    VALUES (p_realm,
            sha256(convert_to(notify_canonical(p_realm, p_tenant_kind, p_tenant_id,
                                               p_recipient_kind, p_recipient_id), 'UTF8')),
            c, now());
    RETURN c;
END $$;

COMMIT;
