-- 0002_channels_delivery.sql — channel-owned state: suppression, dead letters,
-- digest coalescing, and per-tenant channel rate limits.
--
-- Verified against PostgreSQL 16.
--
-- Everything here belongs to a CHANNEL or to the DISPATCHER, never to the core.
-- The engine knows nothing about email compliance or provider quotas; it only
-- knows a delivery was delivered, retryable, suppressed or rejected.

BEGIN;

-- ---------------------------------------------------------------------------
-- channel_suppressions — addresses a channel must stop delivering to.
--
-- Generalized from the inherited `email_suppressions`: a hard bounce is not an
-- email-only concept (a dead device token and a revoked Slack webhook are the
-- same event shape), so the table is keyed by channel (D13). The dispatcher
-- checks it before EVERY delivery, so no channel can forget to.
--
-- Keyed by a HASH of the normalized address, never the address itself (D35). A
-- suppression must outlive the recipient's erasure -- otherwise erasing someone
-- who complained re-enables mailing them -- and a hash is what can be kept.
-- ---------------------------------------------------------------------------
CREATE TABLE channel_suppressions (
    realm        text        NOT NULL,
    channel      text        NOT NULL,
    -- sha256(canonical(channel, normalized address)); see data-model.md.
    address_hash bytea       NOT NULL CHECK (octet_length(address_hash) = 32),
    reason       text        NOT NULL
                             CHECK (reason IN ('hard_bounce', 'complaint', 'unsubscribe',
                                               'invalid_address', 'token_revoked', 'soft_bounce')),
    -- Provider diagnostics. MUST NOT contain the address.
    detail       text,
    created_at   timestamptz NOT NULL DEFAULT now(),
    -- A suppression may expire (a soft-bounce cooldown); NULL means permanent (D14).
    expires_at   timestamptz,
    PRIMARY KEY (realm, channel, address_hash)
);

CREATE INDEX channel_suppressions_expiry_idx
    ON channel_suppressions (expires_at) WHERE expires_at IS NOT NULL;

-- ---------------------------------------------------------------------------
-- dead_letters — the terminal park for GENUINE failures, and the operator's
-- replay queue.
--
-- Only 'max_attempts', 'rejected' and bus 'poison' messages land here. A
-- suppressed address or a missing one is a correct outcome recorded in
-- notification_deliveries, not a dead letter -- so a dead letter always means
-- someone was not told something, and the alarm on this table stays meaningful
-- (D12). Partitioned so it cannot become the thing that fills the disk during
-- an incident (D8).
--
-- `payload` is the queue row as it stood, never rendered content: replay
-- re-renders, and the dead-letter table holds no copy of what was said.
-- ---------------------------------------------------------------------------
CREATE TABLE dead_letters (
    id              uuid        NOT NULL,
    realm           text        NOT NULL,
    tenant_kind     text,
    tenant_id       text,
    recipient_kind  text,
    recipient_id    text,
    source          text        NOT NULL CHECK (source IN ('outbox', 'bus')),
    outbox_id       uuid,
    notification_id uuid,
    digest_id       uuid,
    channel         text,
    reason          text        NOT NULL CHECK (reason IN ('max_attempts', 'rejected', 'poison')),
    payload         jsonb       NOT NULL,
    attempts        int         NOT NULL,
    last_error      text        NOT NULL,
    replayed_at     timestamptz,
    created_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

CREATE TABLE dead_letters_2026m09 PARTITION OF dead_letters
    FOR VALUES FROM ('2026-09-01 00:00:00+00') TO ('2026-10-01 00:00:00+00');
CREATE TABLE dead_letters_2026m10 PARTITION OF dead_letters
    FOR VALUES FROM ('2026-10-01 00:00:00+00') TO ('2026-11-01 00:00:00+00');
CREATE TABLE dead_letters_2026m11 PARTITION OF dead_letters
    FOR VALUES FROM ('2026-11-01 00:00:00+00') TO ('2026-12-01 00:00:00+00');

CREATE INDEX dead_letters_unreplayed_idx
    ON dead_letters (realm, tenant_kind, tenant_id, channel, reason, created_at DESC)
    WHERE replayed_at IS NULL;
CREATE INDEX dead_letters_recipient_idx
    ON dead_letters (realm, tenant_kind, tenant_id, recipient_kind, recipient_id);

-- ---------------------------------------------------------------------------
-- digest_buffer — where a coalesced notification waits for its window to close.
--
-- Invariant 8 requires storm coalescing, and DeliverySchedule.Digest names the
-- window, but the inherited design had nowhere to HOLD a deferred notification
-- (D4). One row per window. A window accepts members while sealed_at IS NULL;
-- it seals when it reaches Config.DigestMax members or its flush_at passes, and
-- the adapter's Digests.FlushDigest enqueues exactly one delivery for it.
--
-- One OPEN window per (recipient, topic, channel) is a partial unique index --
-- a constraint the database holds, not a convention every code path has to
-- remember. Sealing takes a window out of that index, which is what lets a
-- burst larger than DigestMax open the next window while the full one waits.
-- ---------------------------------------------------------------------------
CREATE TABLE digest_buffer (
    id              uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    realm           text        NOT NULL,
    tenant_kind     text        NOT NULL,
    tenant_id       text        NOT NULL,
    recipient_kind  text        NOT NULL,
    recipient_id    text        NOT NULL,
    topic           text        NOT NULL,
    channel         text        NOT NULL,
    shard           smallint    NOT NULL CHECK (shard >= 0),
    -- The notifications folded into this digest, oldest first. Bounded by
    -- DigestMax, so appending never rewrites an unbounded array.
    member_ids      uuid[]      NOT NULL DEFAULT '{}',
    member_count    int         NOT NULL DEFAULT 0,
    opened_at       timestamptz NOT NULL DEFAULT now(),
    flush_at        timestamptz NOT NULL,
    sealed_at       timestamptz,
    flushed_at      timestamptz,
    CHECK (member_count = cardinality(member_ids)),
    CHECK (flushed_at IS NULL OR sealed_at IS NOT NULL)
);

CREATE UNIQUE INDEX digest_buffer_one_open_window_idx
    ON digest_buffer (realm, tenant_kind, tenant_id, recipient_kind, recipient_id, topic, channel)
    WHERE sealed_at IS NULL;

-- The flush tick: windows due in one shard.
CREATE INDEX digest_buffer_flush_idx
    ON digest_buffer (shard, flush_at) WHERE flushed_at IS NULL;

-- ---------------------------------------------------------------------------
-- channel_quotas — per (realm, tenant, channel) delivery budget.
--
-- The design names "isolate rate limits + provider keys per tenant so one noisy
-- host cannot starve another's email quota" as a prerequisite for running this
-- as a shared service, but defined no state for it. This is that state (D5).
-- The hot counter lives in Redis; this table holds the configured ceiling.
--
-- A row with an EMPTY tenant pair is the realm-wide default, applied to every
-- tenant without its own row -- so a tenant nobody configured still cannot
-- monopolize the drainers (D24).
-- ---------------------------------------------------------------------------
CREATE TABLE channel_quotas (
    realm            text        NOT NULL,
    tenant_kind      text        NOT NULL DEFAULT '',
    tenant_id        text        NOT NULL DEFAULT '',
    channel          text        NOT NULL,
    max_per_hour     int         NOT NULL CHECK (max_per_hour >= 0),
    max_per_day      int         NOT NULL CHECK (max_per_day >= 0),
    -- What to do when the budget is exhausted. 'drop_non_essential' still never
    -- drops an essential topic or a critical notification; those always defer.
    on_exhausted     text        NOT NULL DEFAULT 'defer'
                                 CHECK (on_exhausted IN ('defer', 'drop_non_essential')),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (realm, tenant_kind, tenant_id, channel),
    CHECK (max_per_day >= max_per_hour),
    CHECK ((tenant_kind = '') = (tenant_id = ''))
);

COMMIT;
