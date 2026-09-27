-- verify-inherited-constraint.sql — demonstrate that the INHERITED schema shape
-- cannot be constructed, so D2 is a finding rather than an opinion.
--
-- The originating design specified the inbox as BOTH partitioned by created_at
-- AND carrying a global unique index on (user_id, idem_key). PostgreSQL rejects
-- that combination, and that index is the backstop for NS-002.
--
-- This script is EXPECTED TO FAIL. `make verify-schema` asserts that it fails,
-- and fails the build if it ever succeeds -- because that would mean the
-- assumption behind D2 had changed and the decision needs revisiting.

CREATE TABLE inherited_notifications (
    id         uuid        NOT NULL,
    user_id    uuid        NOT NULL,
    idem_key   text        NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (user_id, idem_key)
) PARTITION BY RANGE (created_at);
