package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/truongpx396/intel-notification/domain"
)

// digestJoinSQL adds a member to the open window, sealing it — and pulling its
// flush time forward — when it reaches $9 members. A concurrent append that
// sealed the window first makes this match nothing, and the caller opens the
// next window.
const digestJoinSQL = `
UPDATE digest_buffer
   SET member_ids   = member_ids || $8::uuid,
       member_count = member_count + 1,
       sealed_at    = CASE WHEN member_count + 1 >= $9::int THEN now() END,
       flush_at     = CASE WHEN member_count + 1 >= $9::int THEN now() ELSE flush_at END
 WHERE realm = $1 AND tenant_kind = $2 AND tenant_id = $3
   AND recipient_kind = $4 AND recipient_id = $5
   AND topic = $6 AND channel = $7
   AND sealed_at IS NULL
RETURNING id::text`

// digestOpenSQL opens a window. ON CONFLICT against the one-open-window index
// means a concurrent opener wins cleanly — no error, no row — and the caller
// joins that window instead, without needing a savepoint inside a caller's
// transaction.
const digestOpenSQL = `
INSERT INTO digest_buffer (realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
                           topic, channel, shard, member_ids, member_count, flush_at, sealed_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, ARRAY[$9::uuid], 1,
        CASE WHEN $10::int = 1 THEN now()
             ELSE now() + $11::bigint * interval '1 microsecond' END,
        CASE WHEN $10::int = 1 THEN now() END)
ON CONFLICT (realm, tenant_kind, tenant_id, recipient_kind, recipient_id, topic, channel)
    WHERE sealed_at IS NULL DO NOTHING
RETURNING id::text`

// AppendDigest implements ports.Digests.
func (s *Store) AppendDigest(ctx context.Context, m domain.DigestMember, window time.Duration, max int) (string, error) {
	return appendDigest(ctx, s.pool, m, window, max)
}

// appendDigest runs in a pool or in the caller's transaction, so the persist
// path can fold a notification into a window atomically with its inbox row.
func appendDigest(ctx context.Context, q querier, m domain.DigestMember, window time.Duration, max int) (string, error) {
	if max < 1 {
		return "", fmt.Errorf("digest: max must be >= 1, got %d", max)
	}
	id := m.Identity
	scope := identityArgs(id)
	// Join, else open, else someone opened it between the two: join again. A
	// round is lost only when another appender sealed or opened a window between
	// our two statements, so exhausting the rounds needs pathological contention.
	for range 16 {
		var windowID string
		err := q.QueryRow(ctx, digestJoinSQL, append(scope, string(m.Topic), string(m.Channel),
			m.NotificationID, max)...).Scan(&windowID)
		if err == nil {
			return windowID, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return "", fmt.Errorf("digest join: %w", err)
		}
		err = q.QueryRow(ctx, digestOpenSQL, append(scope, string(m.Topic), string(m.Channel),
			int16(m.Shard), m.NotificationID, max, micros(window))...).Scan(&windowID)
		if err == nil {
			return windowID, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return "", fmt.Errorf("digest open: %w", err)
		}
	}
	return "", errors.New("digest: window contention did not settle")
}

const dueDigestsSQL = `
SELECT id::text FROM digest_buffer
 WHERE shard = $1 AND flushed_at IS NULL AND flush_at <= now()
 ORDER BY flush_at
 LIMIT $2`

// DueDigests implements ports.Digests.
func (s *Store) DueDigests(ctx context.Context, shard domain.Shard, max int) ([]string, error) {
	rows, err := s.pool.Query(ctx, dueDigestsSQL, int16(shard), max)
	if err != nil {
		return nil, fmt.Errorf("due digests: %w", err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("due digests: %w", err)
	}
	return ids, nil
}

// flushDigestSQL seals a window and enqueues its one delivery, conditionally on
// flushed_at IS NULL, so a duplicate tick is a no-op rather than a second digest
// (NS-005).
const flushDigestSQL = `
WITH w AS (
    UPDATE digest_buffer
       SET sealed_at = coalesce(sealed_at, now()), flushed_at = now()
     WHERE id = $1::uuid AND flushed_at IS NULL
    RETURNING *
), queued AS (
    INSERT INTO notification_outbox (digest_id, realm, tenant_kind, tenant_id,
                                     recipient_kind, recipient_id, topic, channel, shard)
    SELECT id, realm, tenant_kind, tenant_id, recipient_kind, recipient_id, topic, channel, shard
      FROM w
    RETURNING 1
)
SELECT count(*) FROM w`

// FlushDigest implements ports.Digests.
func (s *Store) FlushDigest(ctx context.Context, windowID string) (bool, error) {
	var n int
	if err := s.pool.QueryRow(ctx, flushDigestSQL, windowID).Scan(&n); err != nil {
		return false, fmt.Errorf("flush digest %s: %w", windowID, err)
	}
	return n == 1, nil
}
