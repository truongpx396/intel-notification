-- 0001_notification_core.sql — the durable notification tier.
--
-- Verified against PostgreSQL 16. Every design choice that looks unusual here is
-- load-bearing; the ones worth arguing about are recorded in
-- specs/001-notification-core/design-decisions.md.
--
-- Three nested isolation axes, all opaque to the engine:
--   realm     — WHICH PRODUCT. The outermost axis, so two products sharing one
--               deployment cannot collide in the idempotency key space (D1).
--   tenant    — the host's isolation boundary (workspace | organization | account).
--   recipient — the host's delivery subject (user | device | slack_channel | email).
--
-- The engine never parses any of them. Re-anchoring a recipient from a user to a
-- device is a binding change in the host, not a migration here.

BEGIN;

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
    title           text        NOT NULL,
    body            text        NOT NULL DEFAULT '',
    -- deep-link refs for the UI (doc_id / invite_id / run_id). NEVER a routing input.
    payload         jsonb       NOT NULL DEFAULT '{}'::jsonb,
    -- trace_id, source subject. Audit/log ONLY -- never a routing or preference input.
    attributes      jsonb       NOT NULL DEFAULT '{}'::jsonb,
    idem_key        text        NOT NULL,
    occurred_at     timestamptz NOT NULL DEFAULT now(),
    read_at         timestamptz,
    created_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

-- Inbox list + unread count, the two hot reads. Recipient-leading because every
-- query is recipient-scoped by RLS anyway.
CREATE INDEX notifications_inbox_idx
    ON notifications (realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
                      read_at, created_at DESC);

-- Bootstrap partitions. Provisioning beyond these is the operator's monthly job
-- (see migrations/README.md); a missing partition makes INSERT fail loudly rather
-- than silently routing to a default partition that can never be dropped.
CREATE TABLE notifications_2026m09 PARTITION OF notifications
    FOR VALUES FROM ('2026-09-01') TO ('2026-10-01');
CREATE TABLE notifications_2026m10 PARTITION OF notifications
    FOR VALUES FROM ('2026-10-01') TO ('2026-11-01');
CREATE TABLE notifications_2026m11 PARTITION OF notifications
    FOR VALUES FROM ('2026-11-01') TO ('2026-12-01');

-- Recipient-scoping is enforced at the DATA layer, not in application code
-- (NR-008 / NS-001 -- a release blocker). The host sets these GUCs per transaction.
ALTER TABLE notifications ENABLE ROW LEVEL SECURITY;
ALTER TABLE notifications FORCE ROW LEVEL SECURITY;

CREATE POLICY notifications_recipient_scope ON notifications
    USING (
        realm          = current_setting('notify.realm', true)
    AND tenant_kind    = current_setting('notify.tenant_kind', true)
    AND tenant_id      = current_setting('notify.tenant_id', true)
    AND recipient_kind = current_setting('notify.recipient_kind', true)
    AND recipient_id   = current_setting('notify.recipient_id', true)
    );

-- ---------------------------------------------------------------------------
-- notify_idem — the exactly-once guard, deliberately NOT part of notifications.
--
-- The inherited design specified the inbox as BOTH partitioned by created_at AND
-- carrying a global UNIQUE (recipient, idem_key). PostgreSQL forbids that pair,
-- and that one index is the backstop for NS-002. Splitting it keeps both
-- properties, and the guard OUTLIVES ledger partitions -- so dropping an aged
-- partition cannot resurrect the ability to double-notify an old key.
-- Written in the SAME transaction as the inbox row.
-- ---------------------------------------------------------------------------
CREATE TABLE notify_idem (
    realm           text        NOT NULL,
    recipient_kind  text        NOT NULL,
    recipient_id    text        NOT NULL,
    idem_key        text        NOT NULL,
    notification_id uuid        NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (realm, recipient_kind, recipient_id, idem_key)
);

-- ---------------------------------------------------------------------------
-- notification_outbox — one pending delivery per channel, written in the SAME
-- transaction as the inbox row (invariant 4). This is what closes the
-- SET NX short-circuit hole: a crash after commit leaves durable work.
--
-- Deliberately NO foreign key to notifications. A FK into a partitioned parent
-- would make a retention partition DROP fail while any outbox row still
-- referenced it -- turning routine retention into an outage. The pairing is an
-- invariant the Store upholds, asserted by the contract tests instead (D6).
-- ---------------------------------------------------------------------------
CREATE TABLE notification_outbox (
    id              uuid        PRIMARY KEY,
    notification_id uuid        NOT NULL,
    realm           text        NOT NULL,
    tenant_kind     text        NOT NULL,
    tenant_id       text        NOT NULL,
    shard           smallint    NOT NULL,
    channel         text        NOT NULL,
    attempts        int         NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    claimed_at      timestamptz,
    delivered_at    timestamptz,
    -- Suppressed and address-miss are TERMINAL, not failures (invariant 5).
    terminal_reason text        CHECK (terminal_reason IN ('suppressed', 'no_address', 'max_attempts')),
    last_error      text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (notification_id, channel)
);

-- The drain claim: due, undelivered, non-terminal work for one shard.
CREATE INDEX notification_outbox_claim_idx
    ON notification_outbox (shard, next_attempt_at)
    WHERE delivered_at IS NULL AND terminal_reason IS NULL;

-- ---------------------------------------------------------------------------
-- notification_preferences — one row per (recipient, topic, channel).
--
-- NOT the inherited `in_app BOOL, email BOOL` column pair: that shape makes
-- every new channel a migration, which defeats the whole point of a pluggable
-- Channel registry. A row per channel is the extensible shape (D3).
-- An ABSENT row means "use the topic's registered default".
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
    PRIMARY KEY (realm, tenant_kind, tenant_id, recipient_kind, recipient_id, topic, channel)
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
    PRIMARY KEY (realm, tenant_kind, tenant_id, recipient_kind, recipient_id)
);

COMMIT;
