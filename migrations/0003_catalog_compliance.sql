-- 0003_catalog_compliance.sql — the data-backed implementations of the
-- host-supplied ports, and the erasure record.
--
-- Verified against PostgreSQL 16.
--
-- A library-mode host may implement TopicRegistry, TemplateRenderer and
-- AddressBook in its own code and leave these tables empty. Service mode
-- cannot: a standalone container has no host code compiled into it, so topics,
-- templates, addresses and provider settings must be DATA the service owns and
-- a producer manages over the API (D31).

BEGIN;

-- ---------------------------------------------------------------------------
-- notification_topics — the data-backed TopicRegistry.
-- ---------------------------------------------------------------------------
CREATE TABLE notification_topics (
    realm            text        NOT NULL,
    topic            text        NOT NULL,
    -- Delivered in parallel, each subject to preferences.
    default_channels text[]      NOT NULL DEFAULT '{}',
    -- Tried in order, one at a time, while each ends undelivered (D29).
    fallback         text[]      NOT NULL DEFAULT '{}',
    default_priority text        NOT NULL DEFAULT 'info'
                                 CHECK (default_priority IN ('info', 'warning', 'critical')),
    -- Cannot be disabled, never digested, no unsubscribe affordance (NR-015).
    essential        boolean     NOT NULL DEFAULT false,
    -- Which templates render this topic. '' means the topic name itself.
    template_ref     text        NOT NULL DEFAULT '',
    updated_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (realm, topic),
    -- A channel is either fanned out in parallel or tried as a fallback, not both.
    CHECK (NOT (default_channels && fallback))
);

-- ---------------------------------------------------------------------------
-- notification_templates — the data-backed TemplateRenderer's source (D27).
--
-- Versioned and immutable once written: a change is a new version, activated
-- by flipping `active`, so an in-flight retry never renders half of an edit.
-- A tenant pair makes a per-tenant override (branding); the empty pair is the
-- realm-wide template. Locale falls back language-tag → language → default.
-- ---------------------------------------------------------------------------
CREATE TABLE notification_templates (
    realm        text        NOT NULL,
    tenant_kind  text        NOT NULL DEFAULT '',
    tenant_id    text        NOT NULL DEFAULT '',
    template_ref text        NOT NULL,
    channel      text        NOT NULL,
    locale       text        NOT NULL,
    version      int         NOT NULL CHECK (version >= 1),
    engine       text        NOT NULL DEFAULT 'go-template' CHECK (engine IN ('go-template')),
    subject      text        NOT NULL DEFAULT '',
    body         text        NOT NULL,
    -- Structured fields for rich channels (Slack blocks, push payload).
    data         jsonb       NOT NULL DEFAULT '{}'::jsonb,
    active       boolean     NOT NULL DEFAULT false,
    created_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (realm, tenant_kind, tenant_id, template_ref, channel, locale, version),
    CHECK ((tenant_kind = '') = (tenant_id = ''))
);

CREATE UNIQUE INDEX notification_templates_one_active_idx
    ON notification_templates (realm, tenant_kind, tenant_id, template_ref, channel, locale)
    WHERE active;

-- ---------------------------------------------------------------------------
-- recipient_addresses — the data-backed AddressBook (D25, D31).
--
-- Many rows per (recipient, channel): a user with three phones has three push
-- addresses, and each becomes its own delivery with its own retry state.
-- Personal data, so recipient-scoped at the data layer like the inbox.
-- ---------------------------------------------------------------------------
CREATE TABLE recipient_addresses (
    realm           text        NOT NULL,
    tenant_kind     text        NOT NULL,
    tenant_id       text        NOT NULL,
    recipient_kind  text        NOT NULL,
    recipient_id    text        NOT NULL,
    channel         text        NOT NULL,
    -- Stable key of the normalized value; see data-model.md.
    address_key     text        NOT NULL,
    -- Email, E.164 number, device token, webhook URL.
    value           text        NOT NULL,
    locale          text        NOT NULL DEFAULT '',
    timezone        text        NOT NULL DEFAULT '',
    meta            jsonb       NOT NULL DEFAULT '{}'::jsonb,
    verified_at     timestamptz,
    updated_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (realm, tenant_kind, tenant_id, recipient_kind, recipient_id, channel, address_key),
    CHECK (realm <> '' AND tenant_kind <> '' AND tenant_id <> ''
           AND recipient_kind <> '' AND recipient_id <> '')
);

DO $$ BEGIN PERFORM notify_apply_recipient_scope('recipient_addresses'); END $$;

-- ---------------------------------------------------------------------------
-- channel_providers — which provider backs each channel, per realm or tenant.
--
-- Ordered by priority: the reference failover channel tries the next provider
-- on a retryable provider failure, inside one attempt (D29). Credentials are
-- referenced, never stored (security.md).
-- ---------------------------------------------------------------------------
CREATE TABLE channel_providers (
    realm       text        NOT NULL,
    tenant_kind text        NOT NULL DEFAULT '',
    tenant_id   text        NOT NULL DEFAULT '',
    channel     text        NOT NULL,
    provider    text        NOT NULL,     -- resend | ses | smtp | twilio | fcm | apns | slack | webhook | …
    priority    smallint    NOT NULL DEFAULT 0,
    -- Non-secret settings only: region, from-address, sender id.
    config      jsonb       NOT NULL DEFAULT '{}'::jsonb,
    -- A REFERENCE to the credential: 'env:NAME', 'file:/run/secrets/x', or a
    -- secret-manager URI. A bare value with no scheme is rejected, which catches
    -- the common mistake of pasting the key itself.
    secret_ref  text        NOT NULL DEFAULT ''
                            CHECK (secret_ref = '' OR secret_ref ~ '^[a-z][a-z0-9+.-]*:.+'),
    enabled     boolean     NOT NULL DEFAULT true,
    updated_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (realm, tenant_kind, tenant_id, channel, provider),
    CHECK ((tenant_kind = '') = (tenant_id = ''))
);

-- ---------------------------------------------------------------------------
-- erasure_requests — proof an erasure ran, without keeping who it was for (D35).
-- ---------------------------------------------------------------------------
CREATE TABLE erasure_requests (
    id           uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    realm        text        NOT NULL,
    -- sha256(canonical identity). Lets an auditor confirm a named subject was
    -- erased by recomputing the hash; cannot be reversed into the subject.
    subject_hash bytea       NOT NULL CHECK (octet_length(subject_hash) = 32),
    -- Rows removed per table.
    counts       jsonb       NOT NULL DEFAULT '{}'::jsonb,
    requested_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz
);

COMMIT;
