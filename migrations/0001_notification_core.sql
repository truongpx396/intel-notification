-- 0001_notification_core.sql — the durable notification tier: the inbox, the
-- idempotency guard, the delivery queue and its history, broadcasts,
-- preferences, schedules and single-owner job leases.
--
-- Verified against PostgreSQL 16 by `make verify-schema`. Every design choice
-- that looks unusual here is load-bearing; the ones worth arguing about are
-- recorded in specs/001-notification-core/design-decisions.md.
--
-- Three nested isolation axes, all opaque to the engine:
--   realm     — WHICH PRODUCT. The outermost axis, so two products sharing one
--               deployment cannot collide in the idempotency key space (D1).
--   tenant    — the host's isolation boundary (workspace | organization | account).
--   recipient — the host's delivery subject (user | device | slack_channel | email).
--
-- The engine never parses any of them. Re-anchoring a recipient from a user to a
-- device is a binding change in the host, not a migration here.
--
-- Scope for the recipient-scoped tables is set PER TRANSACTION with
-- set_config(name, value, true). Session-level SET is forbidden: on a pooled
-- connection it outlives the request and hands the next one the previous
-- recipient's rows (D17).

BEGIN;

-- ---------------------------------------------------------------------------
-- notify_apply_recipient_scope — the ONE recipient-scoping policy, applied to a
-- table and forced.
--
-- Every recipient-scoped table, and every PARTITION of one, gets exactly this
-- predicate. Partitions matter: PostgreSQL applies a partitioned table's policy
-- only to queries through the parent, so a partition queried by name would
-- otherwise hand its owner every recipient's rows (D17). Partition provisioning
-- calls this for each new partition; `make verify-schema` fails if any
-- recipient-scoped relation lacks it.
--
-- FORCE, because the worker connects as the table owner, and an owner bypasses
-- its own policy unless forced. No WITH CHECK clause: the USING expression
-- doubles as one, so an INSERT or UPDATE can only write inside the scope.
-- ---------------------------------------------------------------------------
CREATE FUNCTION notify_apply_recipient_scope(p_table regclass) RETURNS void
LANGUAGE plpgsql AS $fn$
BEGIN
    EXECUTE format('ALTER TABLE %s ENABLE ROW LEVEL SECURITY', p_table);
    EXECUTE format('ALTER TABLE %s FORCE ROW LEVEL SECURITY', p_table);
    EXECUTE format($p$
        CREATE POLICY recipient_scope ON %s USING (
            realm          = current_setting('notify.realm', true)
        AND tenant_kind    = current_setting('notify.tenant_kind', true)
        AND tenant_id      = current_setting('notify.tenant_id', true)
        AND recipient_kind = current_setting('notify.recipient_kind', true)
        AND recipient_id   = current_setting('notify.recipient_id', true)
        )$p$, p_table);
END $fn$;

-- ---------------------------------------------------------------------------
-- notifications — the recipient-scoped durable inbox.
--
-- PARTITION BY RANGE (created_at) so retention (NR-022) is a partition DROP
-- rather than a mass DELETE. Note the PK *must* carry created_at: PostgreSQL
-- requires every unique constraint on a partitioned table to include all
-- partition-key columns. That requirement is exactly why the idempotency guard
-- cannot live on this table -- see notify_idem below.
-- ---------------------------------------------------------------------------
CREATE TABLE notifications (
    id              uuid        NOT NULL,
    realm           text        NOT NULL,
    tenant_kind     text        NOT NULL,
    tenant_id       text        NOT NULL,
    recipient_kind  text        NOT NULL,
    recipient_id    text        NOT NULL,
    topic           text        NOT NULL,
    priority        text        NOT NULL DEFAULT 'info'
                                CHECK (priority IN ('info', 'warning', 'critical')),
    -- Template variables. Copy, locale and branding live in the template seam,
    -- which renders from these (D27). Never a routing input.
    data            jsonb       NOT NULL DEFAULT '{}'::jsonb,
    -- Producer-supplied FALLBACK copy, used only when no template exists for
    -- (topic, channel, locale). A templated topic leaves both empty.
    title           text        NOT NULL DEFAULT '',
    body            text        NOT NULL DEFAULT '',
    -- Deep-link refs for the UI (doc_id / invite_id / run_id). NEVER a routing input.
    payload         jsonb       NOT NULL DEFAULT '{}'::jsonb,
    -- trace_id, source subject. Audit/log ONLY -- never a routing or preference input.
    attributes      jsonb       NOT NULL DEFAULT '{}'::jsonb,
    idem_key        text        NOT NULL CHECK (length(idem_key) BETWEEN 1 AND 255),
    -- False when no inbox-backed channel was enabled for this recipient and
    -- topic: the row still exists for dedup and audit, but the inbox list and the
    -- badge skip it (D26).
    inbox_visible   boolean     NOT NULL DEFAULT true,
    -- A scheduled notification is not in the inbox before its send time (D29).
    visible_from    timestamptz NOT NULL DEFAULT now(),
    occurred_at     timestamptz NOT NULL DEFAULT now(),
    -- Three inbox states, never inferred from one another (D26).
    seen_at         timestamptz,
    read_at         timestamptz,
    archived_at     timestamptz,
    -- Set by Notifier.Cancel; a canceled notification leaves the inbox (D29).
    canceled_at     timestamptz,
    created_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (id, created_at),
    -- An empty identity component would equal an unset scope setting (which
    -- reads back as '' once any transaction in the session has set it), so the
    -- RLS predicate below could match it. Forbid it and the policy stays
    -- fail-closed.
    CHECK (realm <> '' AND tenant_kind <> '' AND tenant_id <> ''
           AND recipient_kind <> '' AND recipient_id <> '')
) PARTITION BY RANGE (created_at);

-- The inbox list, newest first, and every per-recipient maintenance path
-- (erasure, cancel). Covers read and unread alike; the list filters
-- inbox_visible / archived_at, which exclude a minority of rows. Not
-- (…, read_at, created_at): read_at in the middle breaks the created_at order
-- the list needs, and forces a sort of the whole inbox.
CREATE INDEX notifications_recipient_idx
    ON notifications (realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
                      created_at DESC);

-- The badge. Partial, so the bounded unread count (D15) touches unread rows only.
CREATE INDEX notifications_unread_idx
    ON notifications (realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
                      created_at DESC)
    WHERE read_at IS NULL AND archived_at IS NULL AND canceled_at IS NULL AND inbox_visible;

-- Bootstrap partitions. Provisioning beyond these is the operator's monthly job
-- (see migrations/README.md); a missing partition makes INSERT fail loudly rather
-- than silently routing to a default partition that can never be dropped.
CREATE TABLE notifications_2026m09 PARTITION OF notifications
    FOR VALUES FROM ('2026-09-01 00:00:00+00') TO ('2026-10-01 00:00:00+00');
CREATE TABLE notifications_2026m10 PARTITION OF notifications
    FOR VALUES FROM ('2026-10-01 00:00:00+00') TO ('2026-11-01 00:00:00+00');
CREATE TABLE notifications_2026m11 PARTITION OF notifications
    FOR VALUES FROM ('2026-11-01 00:00:00+00') TO ('2026-12-01 00:00:00+00');

-- Recipient-scoping is enforced at the DATA layer, not in application code
-- (NR-008 / NS-001 -- a release blocker): on the parent AND on each partition.
DO $$
BEGIN
    PERFORM notify_apply_recipient_scope('notifications');
    PERFORM notify_apply_recipient_scope('notifications_2026m09');
    PERFORM notify_apply_recipient_scope('notifications_2026m10');
    PERFORM notify_apply_recipient_scope('notifications_2026m11');
END $$;

-- ---------------------------------------------------------------------------
-- notify_idem — the exactly-once guard, deliberately NOT part of notifications.
--
-- The inherited design specified the inbox as BOTH partitioned by created_at AND
-- carrying a global UNIQUE (recipient, idem_key). PostgreSQL forbids that pair,
-- and that one index is the backstop for NS-002 (D2). The guard is its own
-- table, written in the SAME transaction as the inbox row.
--
-- The key carries the TENANT as well as the realm (D18). A user in two
-- workspaces is two recipients-within-tenant; without the tenant, the second
-- workspace's `weekly_digest:W39` is swallowed as a replay of the first.
--
-- Bounded (D21): a key guards replays for Config.IdempotencyWindow, then
-- the adapter's Maintenance.ExpireIdem removes it. Hash-partitioned on the identity -- every
-- identity column is in the PK, which is what makes that legal -- so expiry
-- and vacuum work one partition at a time rather than across one table
-- holding every key in the window.
-- ---------------------------------------------------------------------------
CREATE TABLE notify_idem (
    realm                   text        NOT NULL,
    tenant_kind             text        NOT NULL,
    tenant_id               text        NOT NULL,
    recipient_kind          text        NOT NULL,
    recipient_id            text        NOT NULL,
    idem_key                text        NOT NULL,
    notification_id         uuid        NOT NULL,
    -- The inbox row's partition key: a replay or a Cancel finds the row in one
    -- partition instead of probing all of them.
    notification_created_at timestamptz NOT NULL,
    created_at              timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (realm, tenant_kind, tenant_id, recipient_kind, recipient_id, idem_key)
) PARTITION BY HASH (realm, tenant_kind, tenant_id, recipient_kind, recipient_id);

DO $$
BEGIN
    FOR i IN 0..15 LOOP
        EXECUTE format(
            'CREATE TABLE notify_idem_p%s PARTITION OF notify_idem
                 FOR VALUES WITH (MODULUS 16, REMAINDER %s)', lpad(i::text, 2, '0'), i);
    END LOOP;
END $$;

-- Expiry scans by age (Maintenance.ExpireIdem).
CREATE INDEX notify_idem_expiry_idx ON notify_idem (created_at);

-- ---------------------------------------------------------------------------
-- notification_outbox — the delivery QUEUE: one row per pending delivery.
--
-- Written in the SAME transaction as the inbox row (invariant 4), so a crash
-- after commit always leaves durable work.
--
-- A row lives only while its delivery is pending (D20). The terminal
-- transition (the adapter's Queue.Finish) moves it to notification_deliveries in
-- one transaction, so this table holds in-flight work only: its size tracks
-- the backlog, not history, and its indexes stay small and hot.
--
-- The row carries the full recipient identity (D17). The dispatcher needs it to
-- scope its read of the notification under FORCE RLS; without it the join to
-- notifications silently returns nothing.
--
-- Deliberately NO foreign key to notifications (D6): a FK into a partitioned
-- parent would make a retention partition DROP fail while any row referenced it.
-- ---------------------------------------------------------------------------
CREATE TABLE notification_outbox (
    id                      uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    -- A delivery is for exactly one notification OR one digest window (D4).
    notification_id         uuid,
    -- With notification_id: the inbox row's partition key, so loading the row
    -- prunes to one partition instead of probing every one.
    notification_created_at timestamptz,
    digest_id               uuid,
    realm                   text        NOT NULL,
    tenant_kind             text        NOT NULL,
    tenant_id               text        NOT NULL,
    recipient_kind          text        NOT NULL,
    recipient_id            text        NOT NULL,
    topic                   text        NOT NULL,
    priority                text        NOT NULL DEFAULT 'info'
                                        CHECK (priority IN ('info', 'warning', 'critical')),
    channel                 text        NOT NULL,
    -- '' until the dispatcher resolves addresses; then the stable key of the ONE
    -- address this row delivers to (D25). Multi-device push is N rows.
    address_key             text        NOT NULL DEFAULT '',
    address                 jsonb,
    -- The rest of the topic's fallback chain, tried in order if this delivery
    -- ends undelivered (D29).
    fallback                text[]      NOT NULL DEFAULT '{}',
    -- A contention hint for claimers, NOT a correctness boundary (D11, D19).
    shard                   smallint    NOT NULL CHECK (shard >= 0),
    -- Incremented AT CLAIM, so a delivery that crashes its worker every time
    -- still reaches the attempt ceiling instead of looping forever (D19).
    attempts                int         NOT NULL DEFAULT 0,
    -- Quota and quiet-hours deferrals. Not attempts, so they never dead-letter.
    deferrals               int         NOT NULL DEFAULT 0,
    -- Due time. While a claim holds the row, it is the lease expiry.
    next_attempt_at         timestamptz NOT NULL DEFAULT now(),
    -- Past this instant the delivery is terminal 'expired', never sent late (D29).
    deliver_before          timestamptz,
    -- The fencing token of the current claim; outcome writes must present it (D19).
    lease_token             uuid,
    claimed_at              timestamptz,
    -- A critical notification delivered inside quiet hours, recorded for audit (D9).
    quiet_hours_override    boolean     NOT NULL DEFAULT false,
    last_error              text,
    created_at              timestamptz NOT NULL DEFAULT now(),
    CHECK ((notification_id IS NULL) <> (digest_id IS NULL)),
    CHECK ((notification_id IS NULL) = (notification_created_at IS NULL))
) WITH (
    -- A queue churns: every claim, retry and deferral is an UPDATE. Vacuum it
    -- at 1% dead tuples rather than the default 20%, or the claim index bloats
    -- faster than autovacuum reclaims it.
    autovacuum_vacuum_scale_factor        = 0.01,
    autovacuum_vacuum_insert_scale_factor = 0.01,
    autovacuum_analyze_scale_factor       = 0.02
);

-- At most one pending delivery per (notification, channel, address) and per
-- (digest, address) -- enforced, not assumed. Also serves the status lookup.
CREATE UNIQUE INDEX notification_outbox_one_per_address
    ON notification_outbox (notification_id, channel, address_key)
    WHERE notification_id IS NOT NULL;
CREATE UNIQUE INDEX notification_outbox_one_per_digest_address
    ON notification_outbox (digest_id, address_key)
    WHERE digest_id IS NOT NULL;

-- The claim: due work in one shard, oldest first (Queue.Claim). Every
-- row in this table is pending, so the index needs no partial predicate.
CREATE INDEX notification_outbox_claim_idx
    ON notification_outbox (shard, next_attempt_at);

-- Bulk deferral of one tenant's channel backlog when its quota is spent (D24).
CREATE INDEX notification_outbox_tenant_idx
    ON notification_outbox (realm, tenant_kind, tenant_id, channel, next_attempt_at);

-- Cancel, status and erasure: every pending delivery of one recipient.
CREATE INDEX notification_outbox_recipient_idx
    ON notification_outbox (realm, tenant_kind, tenant_id, recipient_kind, recipient_id);

-- ---------------------------------------------------------------------------
-- notification_deliveries — the delivery HISTORY: one row per finished delivery.
--
-- The other half of D20. Append-only, range-partitioned by completion time and
-- retired by partition like the inbox (NR-022). It answers "was this
-- delivered?" (Notifier.Status), correlates provider callbacks back to a
-- delivery through provider_message_id, and keeps the audit trail -- without
-- making the queue carry history.
-- ---------------------------------------------------------------------------
CREATE TABLE notification_deliveries (
    id                      uuid        NOT NULL,          -- the queue row's id
    notification_id         uuid,
    notification_created_at timestamptz,
    digest_id               uuid,
    realm                   text        NOT NULL,
    tenant_kind             text        NOT NULL,
    tenant_id               text        NOT NULL,
    recipient_kind          text        NOT NULL,
    recipient_id            text        NOT NULL,
    topic                   text        NOT NULL,
    channel                 text        NOT NULL,
    address_key             text        NOT NULL,
    -- 'delivered', or why it never will be (D12). 'fanned_out' records a
    -- delivery that split into one queue row per address (D25).
    outcome                 text        NOT NULL CHECK (outcome IN (
                                'delivered', 'suppressed', 'no_address', 'max_attempts',
                                'rejected', 'expired', 'canceled', 'dropped_quota',
                                'fanned_out')),
    provider_message_id     text,
    -- The latest provider callback for this message (delivered, bounced, ...).
    provider_status         text,
    provider_status_at      timestamptz,
    attempts                int         NOT NULL,
    deferrals               int         NOT NULL DEFAULT 0,
    quiet_hours_override    boolean     NOT NULL DEFAULT false,
    detail                  text,
    completed_at            timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (id, completed_at)
) PARTITION BY RANGE (completed_at);

-- A bounce or complaint callback names the provider's message id, not ours.
CREATE INDEX notification_deliveries_provider_idx
    ON notification_deliveries (channel, provider_message_id)
    WHERE provider_message_id IS NOT NULL;
CREATE INDEX notification_deliveries_notification_idx
    ON notification_deliveries (notification_id)
    WHERE notification_id IS NOT NULL;
CREATE INDEX notification_deliveries_recipient_idx
    ON notification_deliveries (realm, tenant_kind, tenant_id, recipient_kind, recipient_id);

CREATE TABLE notification_deliveries_2026m09 PARTITION OF notification_deliveries
    FOR VALUES FROM ('2026-09-01 00:00:00+00') TO ('2026-10-01 00:00:00+00');
CREATE TABLE notification_deliveries_2026m10 PARTITION OF notification_deliveries
    FOR VALUES FROM ('2026-10-01 00:00:00+00') TO ('2026-11-01 00:00:00+00');
CREATE TABLE notification_deliveries_2026m11 PARTITION OF notification_deliveries
    FOR VALUES FROM ('2026-11-01 00:00:00+00') TO ('2026-12-01 00:00:00+00');

-- ---------------------------------------------------------------------------
-- notification_broadcasts — a durable broadcast job and its progress.
--
-- Broadcast returns once this row commits; expansion happens in the worker, a
-- page at a time. Each page's notifications and the cursor advance commit in
-- ONE transaction, so a crash resumes at the next page rather than restarting
-- a 50,000-member tenant from the top (NR-020). The PK is the broadcast's own
-- idempotency guard: a retried Broadcast finds its row and reports a replay.
-- ---------------------------------------------------------------------------
CREATE TABLE notification_broadcasts (
    realm           text        NOT NULL,
    tenant_kind     text        NOT NULL,
    tenant_id       text        NOT NULL,
    idem_key        text        NOT NULL CHECK (length(idem_key) BETWEEN 1 AND 255),
    id              uuid        NOT NULL UNIQUE DEFAULT gen_random_uuid(),
    -- Exactly one source of recipients: a host selector resolved in pages by the
    -- AudienceResolver, or a bounded inline list (service mode, D31).
    audience        text,
    recipients      jsonb,
    -- topic, priority, data, title, body, payload: the template for every
    -- expanded notification.
    request         jsonb       NOT NULL,
    -- The AudienceResolver cursor of the next page to expand.
    cursor          text        NOT NULL DEFAULT '',
    fanned          int         NOT NULL DEFAULT 0,
    status          text        NOT NULL DEFAULT 'pending'
                                CHECK (status IN ('pending', 'completed', 'canceled')),
    lease_token     uuid,
    lease_expires_at timestamptz,
    created_at      timestamptz NOT NULL DEFAULT now(),
    completed_at    timestamptz,
    PRIMARY KEY (realm, tenant_kind, tenant_id, idem_key),
    CHECK ((audience IS NULL) <> (recipients IS NULL))
);

CREATE INDEX notification_broadcasts_pending_idx
    ON notification_broadcasts (created_at) WHERE status = 'pending';

-- ---------------------------------------------------------------------------
-- notification_preferences — one row per (recipient, topic, channel).
--
-- NOT the inherited `in_app BOOL, email BOOL` column pair: that shape makes
-- every new channel a migration, which defeats the whole point of a pluggable
-- Channel registry. A row per channel is the extensible shape (D3).
-- An ABSENT row falls back to the tenant default, then the topic default (D30).
-- ---------------------------------------------------------------------------
CREATE TABLE notification_preferences (
    realm           text        NOT NULL,
    tenant_kind     text        NOT NULL,
    tenant_id       text        NOT NULL,
    recipient_kind  text        NOT NULL,
    recipient_id    text        NOT NULL,
    topic           text        NOT NULL,
    channel         text        NOT NULL,
    enabled         boolean     NOT NULL,
    updated_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (realm, tenant_kind, tenant_id, recipient_kind, recipient_id, topic, channel),
    CHECK (realm <> '' AND tenant_kind <> '' AND tenant_id <> ''
           AND recipient_kind <> '' AND recipient_id <> '')
);

DO $$ BEGIN PERFORM notify_apply_recipient_scope('notification_preferences'); END $$;

-- ---------------------------------------------------------------------------
-- notification_tenant_preferences — a tenant's defaults, between the recipient's
-- choice and the topic's registered default (D30).
--
-- `locked` lets a tenant administrator force a (topic, channel) on or off for
-- every recipient in the tenant; a recipient row cannot override a locked one.
-- Essential topics are locked-on by the registry and cannot be turned off here.
-- ---------------------------------------------------------------------------
CREATE TABLE notification_tenant_preferences (
    realm           text        NOT NULL,
    tenant_kind     text        NOT NULL,
    tenant_id       text        NOT NULL,
    topic           text        NOT NULL,
    channel         text        NOT NULL,
    enabled         boolean     NOT NULL,
    locked          boolean     NOT NULL DEFAULT false,
    updated_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (realm, tenant_kind, tenant_id, topic, channel)
);

-- ---------------------------------------------------------------------------
-- notification_schedules — quiet hours + digest cadence per recipient.
-- Drives DeliverySchedule. Absent row means immediate delivery, no quiet hours.
-- ---------------------------------------------------------------------------
CREATE TABLE notification_schedules (
    realm           text        NOT NULL,
    tenant_kind     text        NOT NULL,
    tenant_id       text        NOT NULL,
    recipient_kind  text        NOT NULL,
    recipient_id    text        NOT NULL,
    quiet_start     time,
    quiet_end       time,
    timezone        text        NOT NULL DEFAULT 'UTC',
    digest_window   interval    NOT NULL DEFAULT '0'::interval,
    updated_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (realm, tenant_kind, tenant_id, recipient_kind, recipient_id),
    CHECK (realm <> '' AND tenant_kind <> '' AND tenant_id <> ''
           AND recipient_kind <> '' AND recipient_id <> '')
);

DO $$ BEGIN PERFORM notify_apply_recipient_scope('notification_schedules'); END $$;

-- ---------------------------------------------------------------------------
-- notify_job_leases — single-owner scheduled work without a broker (D22).
--
-- Retention, digest flush, idempotency expiry and quota rollover each take a
-- lease here (Maintenance.TryLeaseJob) before running. A row lease, not a session
-- advisory lock, because advisory locks do not survive transaction-pooling
-- proxies, and a lease also records when each job last ran.
-- ---------------------------------------------------------------------------
CREATE TABLE notify_job_leases (
    job              text        PRIMARY KEY,
    owner            text,
    lease_expires_at timestamptz NOT NULL DEFAULT '-infinity',
    last_started_at  timestamptz,
    last_finished_at timestamptz
);

COMMIT;
