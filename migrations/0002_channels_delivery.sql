-- 0002_channels_delivery.sql — channel-owned state: suppression, dead letters,
-- digest coalescing, and per-tenant channel rate limits.
--
-- Verified against PostgreSQL 16.
--
-- Everything here belongs to a CHANNEL or to the DISPATCHER, never to the core.
-- The engine knows nothing about email compliance or provider quotas; it only
-- knows a Channel returned Suppressed or Retryable (invariant 10).

BEGIN;

-- ---------------------------------------------------------------------------
-- channel_suppressions — addresses a channel must stop delivering to.
--
-- Generalized from the inherited `email_suppressions`: a hard bounce is not an
-- email-only concept (a dead device token and a revoked Slack webhook are the
-- same event shape), so the table is keyed by channel and carries any address.
-- Rows are added by provider bounce/complaint webhooks and by unsubscribes.
-- ---------------------------------------------------------------------------
CREATE TABLE channel_suppressions (
    realm       text        NOT NULL,
    channel     text        NOT NULL,
    address     text        NOT NULL,
    reason      text        NOT NULL
                            CHECK (reason IN ('hard_bounce', 'complaint', 'unsubscribe',
                                              'invalid_address', 'token_revoked')),
    detail      text,
    created_at  timestamptz NOT NULL DEFAULT now(),
    -- A suppression may expire (a soft-bounce cooldown); NULL means permanent.
    expires_at  timestamptz,
    PRIMARY KEY (realm, channel, address)
);

CREATE INDEX channel_suppressions_expiry_idx
    ON channel_suppressions (expires_at) WHERE expires_at IS NOT NULL;

-- ---------------------------------------------------------------------------
-- dead_letters — the terminal park for poison deliveries.
--
-- Never dropped, never retried forever (invariant 6). Admin-inspectable and
-- replayable. Partitioned so this table cannot become the thing that fills the
-- disk during an incident -- the inherited design left it unbounded (D8).
-- ---------------------------------------------------------------------------
CREATE TABLE dead_letters (
    id              uuid        NOT NULL,
    realm           text        NOT NULL,
    source          text        NOT NULL,      -- 'notification_outbox' | bus subject
    notification_id uuid,
    channel         text,
    payload         jsonb       NOT NULL,
    attempts        int         NOT NULL,
    last_error      text        NOT NULL,
    replayed_at     timestamptz,
    created_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

CREATE TABLE dead_letters_2026m09 PARTITION OF dead_letters
    FOR VALUES FROM ('2026-09-01') TO ('2026-10-01');
CREATE TABLE dead_letters_2026m10 PARTITION OF dead_letters
    FOR VALUES FROM ('2026-10-01') TO ('2026-11-01');
CREATE TABLE dead_letters_2026m11 PARTITION OF dead_letters
    FOR VALUES FROM ('2026-11-01') TO ('2026-12-01');

CREATE INDEX dead_letters_unreplayed_idx
    ON dead_letters (realm, created_at DESC) WHERE replayed_at IS NULL;

-- ---------------------------------------------------------------------------
-- digest_buffer — where a coalesced notification waits for its window to close.
--
-- Invariant 8 requires storm coalescing, and DeliverySchedule.Digest names the
-- window, but the inherited design had nowhere to HOLD a deferred notification:
-- the outbox drains as soon as an entry is due, so "collapse a burst into one
-- digest" had no state to collapse into. Without this table the invariant is
-- unimplementable (D4).
--
-- One row per (recipient, topic, channel, window). flush_at closes the window;
-- the Dispatcher renders one digest per row and enqueues a single outbox entry.
-- ---------------------------------------------------------------------------
CREATE TABLE digest_buffer (
    id              uuid        PRIMARY KEY,
    realm           text        NOT NULL,
    tenant_kind     text        NOT NULL,
    tenant_id       text        NOT NULL,
    recipient_kind  text        NOT NULL,
    recipient_id    text        NOT NULL,
    topic           text        NOT NULL,
    channel         text        NOT NULL,
    shard           smallint    NOT NULL,
    -- The notifications folded into this digest, oldest first.
    member_ids      uuid[]      NOT NULL DEFAULT '{}',
    member_count    int         NOT NULL DEFAULT 0,
    opened_at       timestamptz NOT NULL DEFAULT now(),
    flush_at        timestamptz NOT NULL,
    flushed_at      timestamptz,
    -- One OPEN window per (recipient, topic, channel); a closed one is history.
    -- A partial unique index is how "one open window" becomes a constraint
    -- rather than a convention the application has to remember.
    UNIQUE (realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
            topic, channel, opened_at)
);

CREATE UNIQUE INDEX digest_buffer_one_open_window_idx
    ON digest_buffer (realm, tenant_kind, tenant_id, recipient_kind, recipient_id, topic, channel)
    WHERE flushed_at IS NULL;

CREATE INDEX digest_buffer_flush_idx
    ON digest_buffer (shard, flush_at) WHERE flushed_at IS NULL;

-- ---------------------------------------------------------------------------
-- channel_quotas — per (realm, tenant, channel) delivery budget.
--
-- The design names "isolate rate limits + provider keys per tenant so one noisy
-- host cannot starve another's email quota" as a prerequisite for running this
-- as a shared service, but defined no state for it. This is that state (D5).
-- The hot counter lives in Redis; this table holds the configured ceiling.
-- ---------------------------------------------------------------------------
CREATE TABLE channel_quotas (
    realm            text        NOT NULL,
    tenant_kind      text        NOT NULL,
    tenant_id        text        NOT NULL,
    channel          text        NOT NULL,
    max_per_hour     int         NOT NULL CHECK (max_per_hour >= 0),
    max_per_day      int         NOT NULL CHECK (max_per_day >= 0),
    -- What to do when the budget is exhausted. Dropping a critical notification
    -- is never acceptable, so the default defers instead.
    on_exhausted     text        NOT NULL DEFAULT 'defer'
                                 CHECK (on_exhausted IN ('defer', 'drop_non_essential')),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (realm, tenant_kind, tenant_id, channel),
    CHECK (max_per_day >= max_per_hour)
);

COMMIT;
