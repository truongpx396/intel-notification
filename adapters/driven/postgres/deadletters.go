package postgres

import (
	"context"
	"errors"
	"fmt"
	"regexp"

	"github.com/jackc/pgx/v5"

	"github.com/truongpx396/intel-notification/domain"
)

// replayDeadLetterSQL re-drives one dead letter, in ONE statement (D37): the dead letter is
// marked replayed and a fresh delivery is enqueued from the queue row it kept, or
// neither happens. Of two statements racing for one dead letter, the second waits for
// the first's commit, finds replayed_at set, and matches nothing: a replay is once
// per dead letter however many callers there are.
//
// The fresh row starts over: attempts, deferrals, the lease and the last error are the
// column defaults, and it is due now. It keeps what makes it the same delivery: the
// notification or digest, the identity, the topic, the priority, the channel, the
// address it was bound to (so a channel dedupes it on the key it had, D25), its shard,
// its deadline (so a message that has expired is not sent late, D29) and the audit flag
// for a critical message sent through quiet hours. It carries no fallback chain: the
// chain ran when the delivery was parked, and running it twice would send on the
// fallback channel again.
//
// There is no ON CONFLICT. A delivery already pending for the same notification,
// channel and address breaks a unique index, and the statement rolls back whole, so
// replayed_at is not set for a replay that enqueued nothing.
//
//	$1 realm  $2 dead letter id
const replayDeadLetterSQL = `
WITH dead AS (
    UPDATE dead_letters
       SET replayed_at = now()
     WHERE id = $2::uuid AND realm = $1 AND source = 'outbox' AND replayed_at IS NULL
    RETURNING payload
), fresh AS (
    INSERT INTO notification_outbox (notification_id, notification_created_at, digest_id,
                                     realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
                                     topic, priority, channel, address_key, address, fallback,
                                     shard, deliver_before, quiet_hours_override)
    SELECT (payload ->> 'notification_id')::uuid, (payload ->> 'notification_created_at')::timestamptz,
           (payload ->> 'digest_id')::uuid,
           payload ->> 'realm', payload ->> 'tenant_kind', payload ->> 'tenant_id',
           payload ->> 'recipient_kind', payload ->> 'recipient_id',
           payload ->> 'topic', payload ->> 'priority', payload ->> 'channel',
           payload ->> 'address_key',
           CASE WHEN jsonb_typeof(payload -> 'address') = 'object' THEN payload -> 'address' END,
           '{}', (payload ->> 'shard')::smallint, (payload ->> 'deliver_before')::timestamptz,
           (payload ->> 'quiet_hours_override')::boolean
      FROM dead
    RETURNING 1
)
SELECT (SELECT count(*) FROM dead), (SELECT count(*) FROM fresh)`

// deadLetterLookupSQL says why a replay matched nothing: whether the dead letter exists in
// the realm, whether the bus parked it, and whether it was replayed already.
const deadLetterLookupSQL = `
SELECT source, replayed_at IS NOT NULL
  FROM dead_letters WHERE id = $2::uuid AND realm = $1
 LIMIT 1`

var uuidText = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// ReplayDeadLetter implements ports.Maintenance (T032, NR-007, NS-006).
func (s *Store) ReplayDeadLetter(ctx context.Context, realm domain.Realm, deadLetterID string) (bool, error) {
	if realm == "" || !uuidText.MatchString(deadLetterID) {
		// An id that cannot name a dead letter is one that names none, to the caller as
		// to a probe: the database is not asked to parse it.
		return false, fmt.Errorf("replay %q: %w", deadLetterID, domain.ErrUnknownDeadLetter)
	}
	var dead, fresh int
	if err := s.pool.QueryRow(ctx, replayDeadLetterSQL, string(realm), deadLetterID).Scan(&dead, &fresh); err != nil {
		return false, fmt.Errorf("replay dead letter %s: %w", deadLetterID, err)
	}
	if dead == 1 && fresh == 1 {
		return true, nil
	}

	var source string
	var replayed bool
	err := s.pool.QueryRow(ctx, deadLetterLookupSQL, string(realm), deadLetterID).Scan(&source, &replayed)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return false, fmt.Errorf("replay %q: %w", deadLetterID, domain.ErrUnknownDeadLetter)
	case err != nil:
		return false, fmt.Errorf("replay dead letter %s: %w", deadLetterID, err)
	case source != "outbox":
		return false, fmt.Errorf("replay %q: %w", deadLetterID, domain.ErrNotReplayable)
	case replayed:
		return false, nil // once per dead letter: this is the second call, and a no-op
	}
	return false, fmt.Errorf("replay dead letter %s: it matched but enqueued nothing", deadLetterID)
}
