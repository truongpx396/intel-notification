package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/truongpx396/intel-notification/domain"
)

// A broadcast is a row of notification_broadcasts: the request, the cursor of the next
// page to expand, a status, and a lease. Broadcast returns once the row commits; a worker
// then leases it, expands one page, and commits that page's notifications together with
// the cursor's advance in ONE transaction, so a crash resumes at the next page and a
// retry notifies nobody twice (NR-020, D7).

// createBroadcastSQL records a broadcast, idempotently on its key. The conflict target is
// the table's own primary key, so the realm, the tenant and the key are all in it: another
// tenant's broadcast under the same key is its own.
const createBroadcastSQL = `
INSERT INTO notification_broadcasts (realm, tenant_kind, tenant_id, idem_key, audience, recipients, request)
VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb)
ON CONFLICT (realm, tenant_kind, tenant_id, idem_key) DO NOTHING
RETURNING id::text`

// existingBroadcastSQL reads the id a replayed key was accepted as.
const existingBroadcastSQL = `
SELECT id::text FROM notification_broadcasts
 WHERE realm = $1 AND tenant_kind = $2 AND tenant_id = $3 AND idem_key = $4`

// leaseBroadcastSQL takes the oldest pending broadcast that no live lease holds, under a
// new token. SKIP LOCKED hands workers that look at once different broadcasts, and a lone
// one to exactly one of them. The lease is the database's clock, as every lease in the
// engine is. $1 is the lease in microseconds.
const leaseBroadcastSQL = `
UPDATE notification_broadcasts b
   SET lease_token = gen_random_uuid(),
       lease_expires_at = now() + $1::bigint * interval '1 microsecond'
 WHERE (b.realm, b.tenant_kind, b.tenant_id, b.idem_key) = (
         SELECT realm, tenant_kind, tenant_id, idem_key
           FROM notification_broadcasts
          WHERE status = 'pending'
            AND (lease_expires_at IS NULL OR lease_expires_at < now())
          ORDER BY created_at
          LIMIT 1
            FOR UPDATE SKIP LOCKED)
RETURNING b.id::text, b.realm, b.tenant_kind, b.tenant_id, b.idem_key, b.audience,
          b.recipients, b.request, b.cursor, b.lease_token::text`

// advanceBroadcastSQL is the fence and the advance in one statement. It matches only a
// pending broadcast whose lease token is the caller's, so a worker whose lease lapsed, or
// whose broadcast is done, matches nothing and its page is discarded. The lease is
// released with the commit, so the next page is for whoever looks first and no one waits
// for a lease the page no longer needs. The token is compared as text: a malformed one
// matches nothing, instead of failing the cast.
//
//	$1-$4 the broadcast's key  $5 the lease token  $6 the next cursor  $7 whether this was the last page
const advanceBroadcastSQL = `
UPDATE notification_broadcasts
   SET cursor = $6,
       status = CASE WHEN $7::boolean THEN 'completed' ELSE status END,
       completed_at = CASE WHEN $7::boolean THEN now() ELSE completed_at END,
       lease_token = NULL, lease_expires_at = NULL
 WHERE realm = $1 AND tenant_kind = $2 AND tenant_id = $3 AND idem_key = $4
   AND status = 'pending' AND lease_token::text = $5`

// countBroadcastSQL adds what the page wrote to the broadcast's count. It runs in the
// transaction that holds the row from advanceBroadcastSQL.
const countBroadcastSQL = `
UPDATE notification_broadcasts SET fanned = fanned + $5
 WHERE realm = $1 AND tenant_kind = $2 AND tenant_id = $3 AND idem_key = $4`

// broadcastTemplate is the request as the row's request column holds it: what every
// notification the broadcast expands to has in common. Empty fields are left out, so a
// request reads back as it was given: an unset map is nil, not an empty one.
type broadcastTemplate struct {
	Topic    string            `json:"topic"`
	Priority string            `json:"priority,omitempty"`
	Data     map[string]any    `json:"data,omitempty"`
	Title    string            `json:"title,omitempty"`
	Body     string            `json:"body,omitempty"`
	Payload  map[string]string `json:"payload,omitempty"`
}

type broadcastRecipient struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

// CreateBroadcast implements ports.Store. A request already recorded under its key is
// not recorded again, and is not changed: the first call is the broadcast. The receipt of
// a replay carries the id the first call got and Applied=false.
func (s *Store) CreateBroadcast(ctx context.Context, realm domain.Realm, b domain.BroadcastRequest) (domain.BroadcastReceipt, error) {
	if err := realm.Validate(); err != nil {
		return domain.BroadcastReceipt{}, fmt.Errorf("create broadcast: %w", err)
	}
	if b.Tenant.Kind == "" || b.Tenant.ID == "" {
		return domain.BroadcastReceipt{}, fmt.Errorf("create broadcast: tenant: %w", domain.ErrEmptyIdentity)
	}
	var audience *string
	if b.Audience != "" {
		audience = &b.Audience
	}
	var recipients []byte // nil is SQL NULL: with no audience either, the table's CHECK refuses the row
	if len(b.Recipients) > 0 {
		rs := make([]broadcastRecipient, len(b.Recipients))
		for i, r := range b.Recipients {
			rs[i] = broadcastRecipient{Kind: r.Kind, ID: r.ID}
		}
		var err error
		if recipients, err = json.Marshal(rs); err != nil {
			return domain.BroadcastReceipt{}, fmt.Errorf("create broadcast: recipients: %w", err)
		}
	}
	request, err := json.Marshal(broadcastTemplate{
		Topic: string(b.Topic), Priority: string(b.Priority), Data: b.Data,
		Title: b.Title, Body: b.Body, Payload: b.Payload,
	})
	if err != nil {
		return domain.BroadcastReceipt{}, fmt.Errorf("create broadcast: request: %w", err)
	}

	key := []any{string(realm), b.Tenant.Kind, b.Tenant.ID, b.IdemKey}
	// A conflicting row that expires between the failed insert and the read that names
	// it is simply gone: try the insert again. One expiry batch cannot do this to one
	// key repeatedly.
	for range 3 {
		var id string
		err := s.pool.QueryRow(ctx, createBroadcastSQL, append(key, audience, recipients, request)...).Scan(&id)
		if err == nil {
			return domain.BroadcastReceipt{BroadcastID: id, Applied: true}, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return domain.BroadcastReceipt{}, fmt.Errorf("create broadcast: %w", err)
		}
		err = s.pool.QueryRow(ctx, existingBroadcastSQL, key...).Scan(&id)
		if err == nil {
			return domain.BroadcastReceipt{BroadcastID: id}, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return domain.BroadcastReceipt{}, fmt.Errorf("create broadcast: replay: %w", err)
		}
	}
	return domain.BroadcastReceipt{}, errors.New("create broadcast: the broadcast kept changing under the insert")
}

// LeaseBroadcast implements ports.Store. The lease covers one page: the commit of the
// page releases it, and one that is never committed lapses at its ttl. The job carries the
// request and the cursor the next page starts at.
func (s *Store) LeaseBroadcast(ctx context.Context, ttl time.Duration) (*domain.BroadcastJob, error) {
	var (
		job       domain.BroadcastJob
		realm     string
		audience  *string
		recipient []byte
		request   []byte
	)
	err := s.pool.QueryRow(ctx, leaseBroadcastSQL, micros(ttl)).Scan(
		&job.ID, &realm, &job.Request.Tenant.Kind, &job.Request.Tenant.ID, &job.Request.IdemKey,
		&audience, &recipient, &request, &job.Cursor, &job.LeaseToken)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("lease broadcast: %w", err)
	}
	job.Realm = domain.Realm(realm)
	if audience != nil {
		job.Request.Audience = *audience
	}
	if recipient != nil {
		var rs []broadcastRecipient
		if err := json.Unmarshal(recipient, &rs); err != nil {
			return nil, fmt.Errorf("lease broadcast %s: recipients: %w", job.ID, err)
		}
		for _, r := range rs {
			job.Request.Recipients = append(job.Request.Recipients, domain.Recipient{Kind: r.Kind, ID: r.ID})
		}
	}
	var t broadcastTemplate
	if err := json.Unmarshal(request, &t); err != nil {
		return nil, fmt.Errorf("lease broadcast %s: request: %w", job.ID, err)
	}
	job.Request.Topic, job.Request.Priority = domain.Topic(t.Topic), domain.Priority(t.Priority)
	job.Request.Data, job.Request.Title, job.Request.Body, job.Request.Payload = t.Data, t.Title, t.Body, t.Payload
	return &job, nil
}

// CommitBroadcastPage implements ports.Store. In ONE transaction it matches the lease
// (the fence), advances the cursor and releases the lease, writes every notification of
// the page as Notify would, and counts what was written. Any failure rolls the whole page
// back, the cursor with it, and the lease is left to lapse: the next worker takes the page
// again from its own first recipient, and the guards of anything that did commit make the
// rest of it a replay.
//
// A page is refused, before anything is written, when it could not be right: a
// notification for a tenant other than the broadcast's (the page would write into an inbox
// the broadcast was not sent to), one keyed any way but by the broadcast's member key for
// its recipient (a retry would notify them again), one the planner's output could not be
// persisted for, or a page that is not the last and says not where the next begins (an
// empty cursor is where a broadcast starts, so it would not end).
func (s *Store) CommitBroadcastPage(ctx context.Context, job domain.BroadcastJob, page []domain.PageItem, nextCursor string, done bool) error {
	if !done && nextCursor == "" {
		return errors.New("commit broadcast page: a page that is not the last needs the cursor of the next, and an empty one is where a broadcast starts")
	}
	tenant, memberOf := job.Request.Tenant, job.Request.IdemKey
	for i, it := range page {
		id := domain.Identity{Realm: job.Realm, Tenant: tenant, Recipient: it.Notification.Recipient}
		if it.Notification.IdemKey != domain.BroadcastMemberKey(memberOf, it.Notification.Recipient) {
			return fmt.Errorf("commit broadcast page: item %d is not keyed by the broadcast's member key for its recipient", i)
		}
		// The identity is the broadcast's tenant's and the job's realm, so an item for any
		// other tenant fails here: validatePersist holds the identity and the notification
		// to one tenant and recipient.
		if err := validatePersist(id, it.Notification, it.Plan); err != nil {
			return fmt.Errorf("commit broadcast page: item %d: %w", i, err)
		}
	}

	key := []any{string(job.Realm), tenant.Kind, tenant.ID, memberOf, job.LeaseToken}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, advanceBroadcastSQL, append(key, nextCursor, done)...)
		if err != nil {
			return fmt.Errorf("commit broadcast page: advance: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return domain.ErrBroadcastLeaseLost
		}
		applied := 0
		for i, it := range page {
			id := domain.Identity{Realm: job.Realm, Tenant: tenant, Recipient: it.Notification.Recipient}
			receipt, err := s.persist(ctx, tx, id, it.Notification, it.Plan)
			if err != nil {
				return fmt.Errorf("commit broadcast page: item %d: %w", i, err)
			}
			if receipt.Applied {
				applied++ // a replay, the guard's, wrote nothing and is not counted
			}
		}
		if _, err := tx.Exec(ctx, countBroadcastSQL, append(key[:4:4], applied)...); err != nil {
			return fmt.Errorf("commit broadcast page: count: %w", err)
		}
		return nil
	})
}
