#!/bin/sh
# First boot of an empty volume: create the role the service runs as, apply the migrations, and hand the
# schema to that role.
#
# The service must NOT connect as a superuser or a BYPASSRLS role: tenant isolation is row-level security
# the schema enforces, and a role that skips it holds none of it, so notifyd refuses to start as one. The
# schema is built as the superuser (the migrations create extensions' worth of objects) and then owned by
# the application role, as the test kit does (internal/pgtest), which is the shape row-level security was
# proven in.
set -eu

OWNER="${NOTIFY_OWNER_ROLE:-notify_owner}"
PASSWORD="${NOTIFY_OWNER_PASSWORD:-owner}"
psql() { command psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" "$@"; }

psql -c "CREATE ROLE $OWNER LOGIN PASSWORD '$PASSWORD' NOSUPERUSER NOBYPASSRLS"
psql -c "GRANT USAGE, CREATE ON SCHEMA public TO $OWNER"
# What the service's startup preflight asks of its role (docs/operations.md): a transaction left open must not
# pin vacuum for ever, one runaway statement must not hold its snapshot, and the role must see other sessions'
# transactions for the age it reports to cover more than its own.
psql -c "ALTER ROLE $OWNER SET idle_in_transaction_session_timeout = '60s'"
psql -c "ALTER ROLE $OWNER SET statement_timeout = '120s'"
psql -c "GRANT pg_read_all_stats TO $OWNER"
for f in /migrations/*.sql; do
  echo "applying $f"
  psql -q -f "$f"
done
psql <<SQL
DO \$\$
DECLARE r record;
BEGIN
    FOR r IN SELECT c.oid::regclass AS t FROM pg_class c
              WHERE c.relnamespace = 'public'::regnamespace AND c.relkind IN ('r', 'p')
    LOOP
        EXECUTE format('ALTER TABLE %s OWNER TO $OWNER', r.t);
    END LOOP;
    FOR r IN SELECT p.oid::regprocedure AS f FROM pg_proc p
              WHERE p.pronamespace = 'public'::regnamespace
    LOOP
        EXECUTE format('ALTER FUNCTION %s OWNER TO $OWNER', r.f);
    END LOOP;
END \$\$;
SQL
