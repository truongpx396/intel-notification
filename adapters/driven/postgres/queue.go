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

// outboxColumns is the queue row as a claimer reads it. UUIDs are cast to text
// so scanning needs no codec beyond strings.
const outboxColumns = `
	id::text, notification_id::text, notification_created_at, digest_id::text,
	realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
	topic, priority, channel, address_key, address, fallback, shard,
	attempts, deferrals, deliver_before, quiet_hours_override, lease_token::text`

// claimSQL leases up to $2 due rows in shard $1 for $3 microseconds (D19).
//
// FOR UPDATE SKIP LOCKED makes it safe under any number of concurrent claimers:
// two workers on one shard, a rolling deploy, a shard-count change. The lease is
// lease_expires_at, so a worker that dies holding rows releases them when it
// lapses, and the fresh lease_token fences its late writes out. attempts is
// incremented here, at claim, so a delivery that crashes its worker every time
// still reaches the ceiling — counting on failure never counts a crash.
//
// The lease is NOT next_attempt_at. That column is indexed, so moving it would
// make every claim a non-HOT update writing an entry into all five indexes; the
// columns a claim does write are in none, so the update can stay in its page.
// The price is that a held row stays in the due range of the claim index and is
// skipped on the heap check. There are only as many as workers hold, spread over
// the shards (D19).
const claimSQL = `
WITH due AS (
    SELECT id AS due_id
      FROM notification_outbox
     WHERE shard = $1
       AND next_attempt_at <= now()
       AND (lease_expires_at IS NULL OR lease_expires_at <= now())
     ORDER BY next_attempt_at
     LIMIT $2
       FOR UPDATE SKIP LOCKED
)
UPDATE notification_outbox
   SET attempts         = attempts + 1,
       lease_token      = gen_random_uuid(),
       claimed_at       = now(),
       lease_expires_at = now() + $3::bigint * interval '1 microsecond'
  FROM due
 WHERE id = due.due_id
RETURNING` + outboxColumns

// Claim implements ports.Queue.
func (s *Store) Claim(ctx context.Context, shard domain.Shard, max int, lease time.Duration) ([]domain.Claim, error) {
	if max < 1 || lease <= 0 {
		return nil, fmt.Errorf("claim: max must be >= 1 and lease positive, got %d and %v", max, lease)
	}
	rows, err := s.pool.Query(ctx, claimSQL, int16(shard), max, micros(lease))
	if err != nil {
		return nil, fmt.Errorf("claim: %w", err)
	}
	claims, err := pgx.CollectRows(rows, scanClaim)
	if err != nil {
		return nil, fmt.Errorf("claim: %w", err)
	}
	return claims, nil
}

func scanClaim(row pgx.CollectableRow) (domain.Claim, error) {
	var (
		c                     domain.Claim
		e                     = &c.Entry
		notificationID        *string
		notificationCreatedAt *time.Time
		digestID              *string
		realm, topic          string
		priority, channel     string
		address               []byte
		fallback              []string
		shard                 int16
		lease                 *string
	)
	err := row.Scan(&e.ID, &notificationID, &notificationCreatedAt, &digestID,
		&realm, &e.Identity.Tenant.Kind, &e.Identity.Tenant.ID,
		&e.Identity.Recipient.Kind, &e.Identity.Recipient.ID,
		&topic, &priority, &channel, &e.AddressKey, &address, &fallback, &shard,
		&e.Attempts, &e.Deferrals, &e.DeliverBefore, &e.QuietHoursOverride, &lease)
	if err != nil {
		return c, err
	}
	e.Identity.Realm = domain.Realm(realm)
	e.Topic, e.Priority, e.Channel = domain.Topic(topic), domain.Priority(priority), domain.ChannelKind(channel)
	e.Shard = domain.Shard(shard)
	if notificationID != nil {
		e.NotificationID = *notificationID
	}
	if notificationCreatedAt != nil {
		e.NotificationCreatedAt = *notificationCreatedAt
	}
	if digestID != nil {
		e.DigestID = *digestID
	}
	for _, f := range fallback {
		e.Fallback = append(e.Fallback, domain.ChannelKind(f))
	}
	if address != nil {
		e.Address = new(domain.Address)
		if err := json.Unmarshal(address, e.Address); err != nil {
			return c, fmt.Errorf("queue row %s: address: %w", e.ID, err)
		}
	}
	if lease != nil {
		c.LeaseToken = *lease
	}
	return c, nil
}

// retrySQL records a transient failure and releases the lease. Fenced: it
// matches nothing unless $2 is the current lease token. The lease ends with the
// token, so a row that is due again at once is claimable at once.
const retrySQL = `
UPDATE notification_outbox
   SET next_attempt_at = $3, last_error = $4, lease_token = NULL, lease_expires_at = NULL
 WHERE id = $1::uuid AND lease_token = $2::uuid`

// Retry implements ports.Queue.
func (s *Store) Retry(ctx context.Context, c domain.Claim, next time.Time, lastError string) (bool, error) {
	tag, err := s.pool.Exec(ctx, retrySQL, c.Entry.ID, c.LeaseToken, next, lastError)
	if err != nil {
		return false, fmt.Errorf("retry %s: %w", c.Entry.ID, err)
	}
	return tag.RowsAffected() == 1, nil
}

// deferSQL records a deferral: due at $3, and the claim's attempt given back.
// Deferral is not failure, so it can never walk a delivery into a dead letter.
const deferSQL = `
UPDATE notification_outbox
   SET next_attempt_at  = $3,
       attempts         = greatest(attempts - 1, 0),
       deferrals        = deferrals + 1,
       last_error       = $4,
       lease_token      = NULL,
       lease_expires_at = NULL
 WHERE id = $1::uuid AND lease_token = $2::uuid`

// Defer implements ports.Queue.
func (s *Store) Defer(ctx context.Context, c domain.Claim, until time.Time, reason string) (bool, error) {
	tag, err := s.pool.Exec(ctx, deferSQL, c.Entry.ID, c.LeaseToken, until, reason)
	if err != nil {
		return false, fmt.Errorf("defer %s: %w", c.Entry.ID, err)
	}
	return tag.RowsAffected() == 1, nil
}

// historyColumns and historySelect copy a removed queue row into the delivery
// history; each statement appends the outcome columns it knows.
const historyColumns = `
	id, notification_id, notification_created_at, digest_id,
	realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
	topic, channel, address_key, attempts, deferrals, quiet_hours_override`

// finishSQL is the terminal transition, in ONE statement (D20). Data-modifying
// CTEs all run to completion against one snapshot, so the queue row leaves, the
// history row lands, and the dead letter and next fallback channel are written
// together or not at all.
//
//	$1 id  $2 lease token  $3 outcome  $4 provider message id  $5 detail
//	$6 write a dead letter  $7 continue the fallback chain
//
// $6 and $7 are the domain's decision (domain.DispositionOf); this statement only
// carries it out. A dead letter for an outcome the schema does not accept as one
// fails its check constraint rather than being written.
const finishSQL = `
WITH gone AS (
    DELETE FROM notification_outbox
     WHERE id = $1::uuid AND lease_token = $2::uuid
    RETURNING *
), history AS (
    INSERT INTO notification_deliveries (` + historyColumns + `,
                                         outcome, provider_message_id, detail)
    SELECT ` + historyColumns + `, $3::text, nullif($4::text, ''),
           coalesce(nullif($5::text, ''), last_error)
      FROM gone
    RETURNING 1
), dead AS (
    INSERT INTO dead_letters (id, realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
                              source, outbox_id, notification_id, digest_id, channel, reason,
                              payload, attempts, last_error)
    SELECT gen_random_uuid(), realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
           'outbox', id, notification_id, digest_id, channel, $3::text,
           to_jsonb(gone), attempts, coalesce(nullif($5::text, ''), last_error, $3::text)
      FROM gone
     WHERE $6::boolean
    RETURNING 1
), next_channel AS (
    INSERT INTO notification_outbox (notification_id, notification_created_at, digest_id,
                                     realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
                                     topic, priority, channel, fallback, shard, deliver_before,
                                     quiet_hours_override)
    SELECT notification_id, notification_created_at, digest_id,
           realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
           topic, priority, fallback[1], fallback[2:], shard, deliver_before,
           quiet_hours_override
      FROM gone
     WHERE $7::boolean AND cardinality(fallback) > 0
    ON CONFLICT (notification_id, channel, address_key)
        WHERE notification_id IS NOT NULL DO NOTHING
    RETURNING 1
)
SELECT count(*) FROM gone`

// Finish implements ports.Queue.
func (s *Store) Finish(ctx context.Context, c domain.Claim, f domain.Finish) (bool, error) {
	if f.Outcome == domain.OutcomeFannedOut || f.Outcome == "" {
		return false, fmt.Errorf("finish %s: outcome %q is not a finish; build it with domain.NewFinish", c.Entry.ID, f.Outcome)
	}
	var n int
	err := s.pool.QueryRow(ctx, finishSQL, c.Entry.ID, c.LeaseToken, string(f.Outcome),
		f.ProviderMessageID, f.Detail, f.Disposition.DeadLetter, f.Disposition.Fallback).Scan(&n)
	if err != nil {
		return false, fmt.Errorf("finish %s: %w", c.Entry.ID, err)
	}
	return n == 1, nil
}

// bindOneSQL binds a single address in place. The row stays leased, so the
// claimer goes on to deliver it. address_key = ” makes binding one-shot: once
// bound, the delivery key derived from the address never changes.
const bindOneSQL = `
UPDATE notification_outbox
   SET address_key = $3, address = $4::jsonb
 WHERE id = $1::uuid AND lease_token = $2::uuid AND address_key = ''`

// bindManySQL replaces an unbound row with one unleased row per address and
// records the original as fanned_out, in one statement (D25). Children carry no
// fallback chain: one device failing is not the channel failing.
//
//	$1 id  $2 lease token  $3 JSON array of {"key", "address"}  $4 address count
const bindManySQL = `
WITH gone AS (
    DELETE FROM notification_outbox
     WHERE id = $1::uuid AND lease_token = $2::uuid AND address_key = ''
    RETURNING *
), children AS (
    INSERT INTO notification_outbox (notification_id, notification_created_at, digest_id,
                                     realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
                                     topic, priority, channel, address_key, address, shard,
                                     deliver_before, quiet_hours_override)
    SELECT g.notification_id, g.notification_created_at, g.digest_id,
           g.realm, g.tenant_kind, g.tenant_id, g.recipient_kind, g.recipient_id,
           g.topic, g.priority, g.channel, a ->> 'key', a -> 'address', g.shard,
           g.deliver_before, g.quiet_hours_override
      FROM gone g CROSS JOIN jsonb_array_elements($3::jsonb) AS a
    RETURNING 1
), history AS (
    INSERT INTO notification_deliveries (` + historyColumns + `, outcome, detail)
    SELECT ` + historyColumns + `, 'fanned_out', $4::int || ' addresses'
      FROM gone
    RETURNING 1
)
SELECT count(*) FROM gone`

type boundAddress struct {
	Key     string         `json:"key"`
	Address domain.Address `json:"address"`
}

// BindAddresses implements ports.Queue.
func (s *Store) BindAddresses(ctx context.Context, c domain.Claim, addrs []domain.Address, keys []string) (domain.Binding, bool, error) {
	if len(addrs) == 0 {
		return domain.Binding{}, false, errors.New("bind: no addresses; finish the delivery as no_address instead")
	}
	if len(keys) != len(addrs) {
		return domain.Binding{}, false, fmt.Errorf("bind: %d addresses but %d keys", len(addrs), len(keys))
	}
	seen := make(map[string]bool, len(keys))
	for _, k := range keys {
		if k == "" || seen[k] {
			return domain.Binding{}, false, fmt.Errorf("bind: address keys must be non-empty and distinct, got %q", keys)
		}
		seen[k] = true
	}

	if len(addrs) == 1 {
		addr, err := json.Marshal(addrs[0])
		if err != nil {
			return domain.Binding{}, false, err
		}
		tag, err := s.pool.Exec(ctx, bindOneSQL, c.Entry.ID, c.LeaseToken, keys[0], addr)
		if err != nil {
			return domain.Binding{}, false, fmt.Errorf("bind %s: %w", c.Entry.ID, err)
		}
		if tag.RowsAffected() != 1 {
			return domain.Binding{}, false, nil
		}
		return domain.Binding{Addresses: 1, StillHeld: true}, true, nil
	}

	bound := make([]boundAddress, len(addrs))
	for i := range addrs {
		bound[i] = boundAddress{Key: keys[i], Address: addrs[i]}
	}
	payload, err := json.Marshal(bound)
	if err != nil {
		return domain.Binding{}, false, err
	}
	var n int
	if err := s.pool.QueryRow(ctx, bindManySQL, c.Entry.ID, c.LeaseToken, payload, len(addrs)).Scan(&n); err != nil {
		return domain.Binding{}, false, fmt.Errorf("bind %s: %w", c.Entry.ID, err)
	}
	if n != 1 {
		return domain.Binding{}, false, nil
	}
	return domain.Binding{Addresses: len(addrs), StillHeld: false}, true, nil
}

// deferTenantSQL moves a tenant's due backlog on one channel out of the claim
// range (D24). The lease check leaves rows a worker holds to that worker; a row
// whose lease has lapsed is deferred and its token cleared, which fences out the
// worker that let it lapse.
const deferTenantSQL = `
UPDATE notification_outbox
   SET next_attempt_at = $5, deferrals = deferrals + 1, lease_token = NULL, lease_expires_at = NULL
 WHERE realm = $1 AND tenant_kind = $2 AND tenant_id = $3 AND channel = $4
   AND next_attempt_at <= now()
   AND (lease_expires_at IS NULL OR lease_expires_at <= now())`

// DeferTenantChannel implements ports.Queue.
func (s *Store) DeferTenantChannel(ctx context.Context, realm domain.Realm, t domain.Tenant, ch domain.ChannelKind, until time.Time) (int, error) {
	tag, err := s.pool.Exec(ctx, deferTenantSQL, string(realm), t.Kind, t.ID, string(ch), until)
	if err != nil {
		return 0, fmt.Errorf("defer tenant %s/%s on %s: %w", t.Kind, t.ID, ch, err)
	}
	return int(tag.RowsAffected()), nil
}

const cancelLookupSQL = `
SELECT notification_id::text, notification_created_at
  FROM notify_idem
 WHERE realm = $1 AND tenant_kind = $2 AND tenant_id = $3
   AND recipient_kind = $4 AND recipient_id = $5 AND idem_key = $6`

const cancelMarkSQL = `
UPDATE notifications SET canceled_at = now()
 WHERE id = $1::uuid AND created_at = $2 AND canceled_at IS NULL`

// cancelDeliveriesSQL finishes every pending delivery of one notification that
// no claimer holds as canceled, and counts the ones that are held. A held
// delivery may already be at the provider; recording it as canceled would be a
// lie (D29). The in-flight count uses the complementary predicate rather than
// re-reading the table, because every part of the statement sees the same
// snapshot and would still see the deleted rows.
const cancelDeliveriesSQL = `
WITH gone AS (
    DELETE FROM notification_outbox
     WHERE notification_id = $1::uuid
       AND (lease_expires_at IS NULL OR lease_expires_at <= now())
    RETURNING *
), history AS (
    INSERT INTO notification_deliveries (` + historyColumns + `, outcome)
    SELECT ` + historyColumns + `, 'canceled' FROM gone
    RETURNING 1
)
SELECT (SELECT count(*) FROM gone),
       (SELECT count(*) FROM notification_outbox
         WHERE notification_id = $1::uuid
           AND lease_expires_at > now())`

// Cancel implements ports.Queue.
func (s *Store) Cancel(ctx context.Context, id domain.Identity, idemKey string) (domain.CancelReceipt, error) {
	var r domain.CancelReceipt
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var notificationID string
		var createdAt time.Time
		err := tx.QueryRow(ctx, cancelLookupSQL, append(identityArgs(id), idemKey)...).
			Scan(&notificationID, &createdAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		r.Matched = true
		if err := setScope(ctx, tx, id); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, cancelMarkSQL, notificationID, createdAt); err != nil {
			return err
		}
		return tx.QueryRow(ctx, cancelDeliveriesSQL, notificationID).Scan(&r.Canceled, &r.InFlight)
	})
	if err != nil {
		return domain.CancelReceipt{}, fmt.Errorf("cancel: %w", err)
	}
	return r, nil
}
