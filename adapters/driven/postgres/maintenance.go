package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/truongpx396/intel-notification/domain"
)

// leaseJobSQL takes or renews a job's single-owner lease (D22). The conflict
// update applies only when the lease has lapsed or the caller already holds it;
// otherwise it affects no row.
const leaseJobSQL = `
INSERT INTO notify_job_leases AS l (job, owner, lease_expires_at, last_started_at)
VALUES ($1, $2, now() + $3::bigint * interval '1 microsecond', now())
ON CONFLICT (job) DO UPDATE
   SET owner            = EXCLUDED.owner,
       lease_expires_at = EXCLUDED.lease_expires_at,
       last_started_at  = CASE WHEN l.owner = EXCLUDED.owner
                               THEN l.last_started_at ELSE EXCLUDED.last_started_at END
 WHERE l.lease_expires_at < now() OR l.owner = EXCLUDED.owner`

// TryLeaseJob implements ports.Maintenance.
func (s *Store) TryLeaseJob(ctx context.Context, job, owner string, ttl time.Duration) (bool, error) {
	if job == "" || owner == "" || ttl <= 0 {
		return false, fmt.Errorf("lease job: job and owner are required and ttl must be positive")
	}
	tag, err := s.pool.Exec(ctx, leaseJobSQL, job, owner, micros(ttl))
	if err != nil {
		return false, fmt.Errorf("lease job %s: %w", job, err)
	}
	return tag.RowsAffected() == 1, nil
}

// RehomeShards implements ports.Maintenance. It does not touch leases, and the
// claim never depended on shard ownership, so it is safe while claimers run
// (D11).
func (s *Store) RehomeShards(ctx context.Context, shards int) (int, error) {
	if shards < 1 {
		return 0, fmt.Errorf("rehome: shard count must be >= 1, got %d", shards)
	}
	moved := 0
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		for _, table := range []string{"notification_outbox", "digest_buffer"} {
			tag, err := tx.Exec(ctx,
				"UPDATE "+table+" SET shard = shard % $1 WHERE shard >= $1", shards)
			if err != nil {
				return err
			}
			moved += int(tag.RowsAffected())
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("rehome shards: %w", err)
	}
	return moved, nil
}

// expireIdemSQL deletes a batch of keys older than the window (D21). It joins on
// the primary key rather than ctid, which is not unique across the table's
// partitions.
const expireIdemSQL = `
WITH victims AS (
    SELECT realm, tenant_kind, tenant_id, recipient_kind, recipient_id, idem_key
      FROM notify_idem
     WHERE created_at < $1
     LIMIT $2
), gone AS (
    DELETE FROM notify_idem d
     USING victims v
     WHERE d.realm = v.realm AND d.tenant_kind = v.tenant_kind AND d.tenant_id = v.tenant_id
       AND d.recipient_kind = v.recipient_kind AND d.recipient_id = v.recipient_id
       AND d.idem_key = v.idem_key
    RETURNING 1
)
SELECT count(*) FROM gone`

// ExpireIdem implements ports.Maintenance.
func (s *Store) ExpireIdem(ctx context.Context, olderThan time.Time, batch int) (int, error) {
	var n int
	if err := s.pool.QueryRow(ctx, expireIdemSQL, olderThan, batch).Scan(&n); err != nil {
		return 0, fmt.Errorf("expire idempotency keys: %w", err)
	}
	return n, nil
}

// expireDigestsSQL deletes a batch of flushed windows whose delivery finished.
const expireDigestsSQL = `
WITH victims AS (
    SELECT d.id
      FROM digest_buffer d
     WHERE d.flushed_at < $1
       AND NOT EXISTS (SELECT 1 FROM notification_outbox o WHERE o.digest_id = d.id)
     LIMIT $2
), gone AS (
    DELETE FROM digest_buffer d USING victims v WHERE d.id = v.id RETURNING 1
)
SELECT count(*) FROM gone`

// ExpireDigests implements ports.Maintenance.
func (s *Store) ExpireDigests(ctx context.Context, olderThan time.Time, batch int) (int, error) {
	var n int
	if err := s.pool.QueryRow(ctx, expireDigestsSQL, olderThan, batch).Scan(&n); err != nil {
		return 0, fmt.Errorf("expire digests: %w", err)
	}
	return n, nil
}

// erasedTables is every table that holds a recipient's personal data. The
// schema suite (verify-schema TEST 5) fails when a table carrying recipient_id
// is added without being classified, and the erasure test checks each of these.
// channel_suppressions is deliberately absent (D35).
var erasedTables = []string{
	"notifications", "notify_idem", "notification_outbox", "notification_deliveries",
	"dead_letters", "digest_buffer", "notification_preferences", "notification_schedules",
	"recipient_addresses",
}

// Erase implements ports.Maintenance. It runs under the recipient's own scope,
// so row-level security applies to the erasure too.
func (s *Store) Erase(ctx context.Context, id domain.Identity) (map[string]int, error) {
	counts := make(map[string]int, len(erasedTables))
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := setScope(ctx, tx, id); err != nil {
			return err
		}
		for _, table := range erasedTables {
			tag, err := tx.Exec(ctx, "DELETE FROM "+table+
				" WHERE realm = $1 AND tenant_kind = $2 AND tenant_id = $3"+
				" AND recipient_kind = $4 AND recipient_id = $5", identityArgs(id)...)
			if err != nil {
				return fmt.Errorf("%s: %w", table, err)
			}
			counts[table] = int(tag.RowsAffected())
		}
		record, err := json.Marshal(counts)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `
INSERT INTO erasure_requests (realm, subject_hash, counts, completed_at)
VALUES ($1, $2, $3::jsonb, now())`, string(id.Realm), domain.SubjectHash(id), record)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("erase: %w", err)
	}
	return counts, nil
}

// partitionedTables are the range-partitioned tables provisioning keeps ahead
// of the clock.
var partitionedTables = []string{"notifications", "notification_deliveries", "dead_letters"}

// EnsurePartitions implements ports.Maintenance. For each month from the one
// containing from, for months months, it creates any missing monthly partition
// of the range-partitioned tables, and applies the recipient-scope policy to
// every new notifications partition: PostgreSQL does not carry a parent's
// row-level security to its partitions, so an unscoped partition queried by
// name would show every recipient's rows (D17). Bounds are UTC month starts.
//
// The pool's role needs CREATE on the schema. Returns the partitions created.
func (s *Store) EnsurePartitions(ctx context.Context, from time.Time, months int) ([]string, error) {
	if months < 1 {
		return nil, fmt.Errorf("ensure partitions: months must be >= 1, got %d", months)
	}
	from = from.UTC()
	first := time.Date(from.Year(), from.Month(), 1, 0, 0, 0, 0, time.UTC)
	var created []string
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		created = created[:0]
		for i := range months {
			lo := first.AddDate(0, i, 0)
			hi := lo.AddDate(0, 1, 0)
			for _, parent := range partitionedTables {
				name := fmt.Sprintf("%s_%04dm%02d", parent, lo.Year(), int(lo.Month()))
				var exists bool
				if err := tx.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", name).Scan(&exists); err != nil {
					return err
				}
				if exists {
					continue
				}
				ddl := fmt.Sprintf("CREATE TABLE %s PARTITION OF %s FOR VALUES FROM ('%s') TO ('%s')",
					pgx.Identifier{name}.Sanitize(), pgx.Identifier{parent}.Sanitize(),
					lo.Format("2006-01-02 15:04:05-07"), hi.Format("2006-01-02 15:04:05-07"))
				if _, err := tx.Exec(ctx, ddl); err != nil {
					return fmt.Errorf("%s: %w", name, err)
				}
				if parent == "notifications" {
					if _, err := tx.Exec(ctx, "SELECT notify_apply_recipient_scope($1::regclass)", name); err != nil {
						return fmt.Errorf("%s: scope: %w", name, err)
					}
				}
				created = append(created, name)
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("ensure partitions: %w", err)
	}
	return created, nil
}

// missingPartitionsSQL returns each table in $1 and month [$2[i], $3[i]) that no
// partition covers, as the table and the month's ordinal. A partition's bounds
// are read back from the catalog as "FOR VALUES FROM (x) TO (y)", each side a
// quoted timestamp or MINVALUE/MAXVALUE, and one table's partitions are merged
// with range_agg, so a month split across partitions is covered. A default
// partition is excluded: its bound parses to no range at all, which would read
// as unbounded, and a row it holds can never be retired (D33).
const missingPartitionsSQL = `
WITH covered AS (
    SELECT i.inhparent AS parent,
           range_agg(tstzrange(
               CASE WHEN b.v[1] = 'MINVALUE' THEN NULL ELSE btrim(b.v[1], '''')::timestamptz END,
               CASE WHEN b.v[2] = 'MAXVALUE' THEN NULL ELSE btrim(b.v[2], '''')::timestamptz END)) AS span
      FROM pg_inherits i
      JOIN pg_class c ON c.oid = i.inhrelid
      JOIN pg_partitioned_table p ON p.partrelid = i.inhparent
     CROSS JOIN LATERAL regexp_match(pg_get_expr(c.relpartbound, c.oid),
                                     '^FOR VALUES FROM \((.+)\) TO \((.+)\)$') AS b(v)
     WHERE c.oid <> p.partdefid
     GROUP BY i.inhparent
)
SELECT t.name, m.n
  FROM unnest($1::text[]) WITH ORDINALITY AS t(name, pos)
 CROSS JOIN unnest($2::timestamptz[], $3::timestamptz[]) WITH ORDINALITY AS m(lo, hi, n)
  LEFT JOIN covered cv ON cv.parent = t.name::regclass
 WHERE NOT coalesce(cv.span @> tstzrange(m.lo, m.hi), false)
 ORDER BY m.n, t.pos`

// unscopedSQL returns notifications, and every partition under it at any depth,
// that recipient scoping does not hold on (D17): row-level security not both
// enabled and forced; no permissive recipient_scope policy for all commands with
// the parent's predicate and check; or any other permissive policy, which
// PostgreSQL ORs with the scope and so widens it. A restrictive policy can only
// narrow the scope, so it is not a finding.
const unscopedSQL = `
WITH parent AS (
    SELECT pg_get_expr(p.polqual, p.polrelid)      AS qual,
           pg_get_expr(p.polwithcheck, p.polrelid) AS wcheck
      FROM pg_policy p
     WHERE p.polrelid = 'notifications'::regclass AND p.polname = 'recipient_scope'
)
SELECT t.relid::regclass::text
  FROM pg_partition_tree('notifications') t
  JOIN pg_class c ON c.oid = t.relid
 WHERE NOT (c.relrowsecurity AND c.relforcerowsecurity)
    OR NOT EXISTS (
           SELECT 1 FROM pg_policy p, parent
            WHERE p.polrelid = c.oid AND p.polname = 'recipient_scope'
              AND p.polpermissive AND p.polcmd = '*'
              AND pg_get_expr(p.polqual, p.polrelid) = parent.qual
              AND pg_get_expr(p.polwithcheck, p.polrelid) IS NOT DISTINCT FROM parent.wcheck)
    OR EXISTS (
           SELECT 1 FROM pg_policy p
            WHERE p.polrelid = c.oid AND p.polname <> 'recipient_scope' AND p.polpermissive)
 ORDER BY t.relid::regclass::text COLLATE "C"`

// CheckPartitions implements ports.Maintenance. It reads the catalog and
// nothing else, so it reports the partitions that exist, not the ones
// provisioning believes it made: a partition detached by hand still has its
// table, and is still missing. Months are UTC, like EnsurePartitions'.
func (s *Store) CheckPartitions(ctx context.Context, at time.Time) (domain.PartitionHealth, error) {
	at = at.UTC()
	this := time.Date(at.Year(), at.Month(), 1, 0, 0, 0, 0, time.UTC)
	months := []time.Time{this, this.AddDate(0, 1, 0)}
	ends := []time.Time{months[1], months[1].AddDate(0, 1, 0)}

	var h domain.PartitionHealth
	rows, err := s.pool.Query(ctx, missingPartitionsSQL, partitionedTables, months, ends)
	if err != nil {
		return h, fmt.Errorf("check partitions: missing: %w", err)
	}
	gaps, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (domain.PartitionGap, error) {
		var table string
		var n int
		if err := r.Scan(&table, &n); err != nil {
			return domain.PartitionGap{}, err
		}
		return domain.PartitionGap{Table: table, Month: months[n-1]}, nil
	})
	if err != nil {
		return h, fmt.Errorf("check partitions: missing: %w", err)
	}
	rows, err = s.pool.Query(ctx, unscopedSQL)
	if err != nil {
		return h, fmt.Errorf("check partitions: unscoped: %w", err)
	}
	unscoped, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return h, fmt.Errorf("check partitions: unscoped: %w", err)
	}
	h.Missing, h.Unscoped = gaps, unscoped
	return h, nil
}
