package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// persistGuardSQL writes the exactly-once guard (D2, D18). Its key is the whole
// identity plus the producer's key; a conflict means this key was already
// accepted, and nothing else is written. created_at is the store's clock, like the
// inbox row's, so a fake clock keeps the two together and expiry is judged on it.
const persistGuardSQL = `
INSERT INTO notify_idem (realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
                         idem_key, notification_id, notification_created_at, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7::uuid, $8, $8)
ON CONFLICT (realm, tenant_kind, tenant_id, recipient_kind, recipient_id, idem_key) DO NOTHING`

// replaySQL reads the notification a replayed key was accepted as, and whether it
// is visible in the inbox. Under the identity's scope, like every read of the
// inbox row.
const replaySQL = `
SELECT g.notification_id::text, coalesce(n.inbox_visible, false)
  FROM notify_idem g
  LEFT JOIN notifications n
         ON n.id = g.notification_id AND n.created_at = g.notification_created_at
 WHERE g.realm = $1 AND g.tenant_kind = $2 AND g.tenant_id = $3
   AND g.recipient_kind = $4 AND g.recipient_id = $5 AND g.idem_key = $6`

// persistInboxSQL writes the durable record (NR-001): the inbox entry and the
// deduplication record in one row. Scope must already be set; the policy's USING
// clause doubles as the check, so a row outside the scope cannot be written.
const persistInboxSQL = `
INSERT INTO notifications (id, realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
                           topic, priority, data, title, body, payload, attributes, idem_key,
                           inbox_visible, visible_from, occurred_at, created_at)
VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10, $11, $12::jsonb, $13::jsonb, $14,
        $15, $16, $17, $18)`

// persistQueueSQL writes one pending delivery per immediate or deferred channel,
// in one statement (NR-003). A row with no time is due when the database says it
// is: the claim compares against the database's now(), not an application clock.
//
//	$1 notification id  $2 its created_at  $3-$7 identity  $8 topic  $9 priority
//	$10 shard  $11 deliver_before  $12 JSON array of {channel, fallback, not_before, quiet}
const persistQueueSQL = `
INSERT INTO notification_outbox (notification_id, notification_created_at, realm, tenant_kind,
                                 tenant_id, recipient_kind, recipient_id, topic, priority,
                                 channel, fallback, shard, next_attempt_at, deliver_before,
                                 quiet_hours_override)
SELECT $1::uuid, $2::timestamptz, $3::text, $4::text, $5::text, $6::text, $7::text, $8::text,
       $9::text, d.channel,
       ARRAY(SELECT f.value FROM jsonb_array_elements_text(d.fallback) WITH ORDINALITY AS f(value, pos)
              ORDER BY f.pos),
       $10::smallint, coalesce(d.not_before, now()), $11::timestamptz, d.quiet
  FROM jsonb_to_recordset($12::jsonb)
       AS d(channel text, fallback jsonb, not_before timestamptz, quiet boolean)`

// persistDroppedSQL records each channel the quota policy dropped (D5): history
// only, with no queue row, so the notification's status can say what happened.
//
//	$1 notification id  $2 its created_at  $3-$7 identity  $8 topic  $9 recorded at
//	$10 JSON array of {channel, reason}
const persistDroppedSQL = `
INSERT INTO notification_deliveries (id, notification_id, notification_created_at, realm, tenant_kind,
                                     tenant_id, recipient_kind, recipient_id, topic, channel,
                                     address_key, outcome, attempts, detail, completed_at)
SELECT gen_random_uuid(), $1::uuid, $2::timestamptz, $3::text, $4::text, $5::text, $6::text,
       $7::text, $8::text, d.channel, '', 'dropped_quota', 0, d.reason, $9::timestamptz
  FROM jsonb_to_recordset($10::jsonb) AS d(channel text, reason text)`

// announceSQL tells every listener on the channel that work was committed. The payload
// is empty: a wake says "look", and carries nothing to count.
const announceSQL = `SELECT pg_notify($1, '')`

type queuedDelivery struct {
	Channel   string     `json:"channel"`
	Fallback  []string   `json:"fallback"`
	NotBefore *time.Time `json:"not_before"`
	Quiet     bool       `json:"quiet"`
}

type droppedDelivery struct {
	Channel string `json:"channel"`
	Reason  string `json:"reason"`
}

// PersistAndEnqueue implements ports.Store. Everything is written in one
// transaction, in this order: scope, the guard, the inbox row, the queue rows, the
// dropped-channel records, the digest appends. The guard comes first so that a
// replay is detected before anything else is attempted, and writes nothing.
//
// With a nil tx the store opens the transaction and commits it before returning,
// which is the acceptance point (D23). With a tx, which must be a pgx.Tx, it is
// the caller's: the store joins it and never commits it or rolls it back (D28). The
// scope it sets there is transaction-local, so it ends with the caller's transaction.
//
// n.Priority must be the effective priority. The store does not know the topic's
// default, and storing info for a notification the topic makes critical would let
// quiet hours defer it, so an unresolved priority is refused.
func (s *Store) PersistAndEnqueue(ctx context.Context, tx ports.Tx, id domain.Identity, n domain.Notification, plan domain.DeliveryPlan) (domain.Receipt, error) {
	if err := validatePersist(id, n, plan); err != nil {
		return domain.Receipt{}, err
	}
	if tx == nil {
		var r domain.Receipt
		err := pgx.BeginFunc(ctx, s.pool, func(own pgx.Tx) (err error) {
			r, err = s.persist(ctx, own, id, n, plan)
			return err
		})
		if err != nil {
			return domain.Receipt{}, err
		}
		return r, nil
	}
	callers, ok := tx.(pgx.Tx)
	if !ok {
		return domain.Receipt{}, fmt.Errorf("persist: tx is a %T, want a pgx.Tx", tx)
	}
	return s.persist(ctx, callers, id, n, plan)
}

func validatePersist(id domain.Identity, n domain.Notification, plan domain.DeliveryPlan) error {
	if err := id.Validate(); err != nil {
		return fmt.Errorf("persist: %w", err)
	}
	if err := n.Validate(); err != nil {
		return fmt.Errorf("persist: %w", err)
	}
	if id.Tenant != n.Tenant || id.Recipient != n.Recipient {
		return errors.New("persist: the identity's tenant and recipient are not the notification's")
	}
	if !n.Priority.Valid() {
		return fmt.Errorf("persist: priority %q is not resolved; the notifier resolves the topic's default before persisting", n.Priority)
	}
	if plan.Shard < 0 {
		return fmt.Errorf("persist: shard %d is negative", plan.Shard)
	}
	seen := make(map[domain.ChannelKind]bool, len(plan.Deliveries))
	for _, d := range plan.Deliveries {
		switch {
		case d.Channel == "":
			return errors.New("persist: a planned delivery has no channel")
		case seen[d.Channel]:
			return fmt.Errorf("persist: channel %q is planned twice; a notification has one delivery per channel", d.Channel)
		}
		seen[d.Channel] = true
		switch d.Mode {
		case domain.PlanImmediate, domain.PlanDropped:
		case domain.PlanDeferred:
			if d.NotBefore.IsZero() {
				return fmt.Errorf("persist: channel %q is deferred with no time to be due", d.Channel)
			}
		case domain.PlanDigest:
			if plan.DigestWindow <= 0 || plan.DigestMax < 1 {
				return fmt.Errorf("persist: channel %q is digested, but the plan has window %v and max %d",
					d.Channel, plan.DigestWindow, plan.DigestMax)
			}
		default:
			return fmt.Errorf("persist: channel %q has plan mode %d", d.Channel, d.Mode)
		}
	}
	return nil
}

// persist does the writes inside a transaction it does not own.
func (s *Store) persist(ctx context.Context, tx pgx.Tx, id domain.Identity, n domain.Notification, plan domain.DeliveryPlan) (domain.Receipt, error) {
	if err := setScope(ctx, tx, id); err != nil {
		return domain.Receipt{}, fmt.Errorf("persist: scope: %w", err)
	}
	now := s.clock.Now().Truncate(time.Microsecond)
	nid := s.ids.New()

	// A conflicting key whose guard row expires between the failed insert and the
	// read that names it is simply gone: try the insert again. One expiry batch
	// cannot do this repeatedly to one key.
	for range 3 {
		tag, err := tx.Exec(ctx, persistGuardSQL, append(identityArgs(id), n.IdemKey, nid, now)...)
		if err != nil {
			return domain.Receipt{}, fmt.Errorf("persist: guard: %w", err)
		}
		if tag.RowsAffected() == 1 {
			return s.persistApplied(ctx, tx, id, n, plan, nid, now)
		}
		var existing string
		var visible bool
		err = tx.QueryRow(ctx, replaySQL, append(identityArgs(id), n.IdemKey)...).Scan(&existing, &visible)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return domain.Receipt{}, fmt.Errorf("persist: replay: %w", err)
		}
		return domain.Receipt{NotificationID: existing, IdemKey: n.IdemKey, InboxVisible: visible}, nil
	}
	return domain.Receipt{}, errors.New("persist: the idempotency guard kept changing under the insert")
}

// persistApplied writes everything after the guard.
func (s *Store) persistApplied(ctx context.Context, tx pgx.Tx, id domain.Identity, n domain.Notification,
	plan domain.DeliveryPlan, nid string, now time.Time) (domain.Receipt, error) {

	data, err := jsonObject(n.Data)
	if err != nil {
		return domain.Receipt{}, fmt.Errorf("persist: data: %w", err)
	}
	payload, err := jsonObject(n.Payload)
	if err != nil {
		return domain.Receipt{}, fmt.Errorf("persist: payload: %w", err)
	}
	attributes, err := jsonObject(n.Attributes)
	if err != nil {
		return domain.Receipt{}, fmt.Errorf("persist: attributes: %w", err)
	}
	visibleFrom, occurredAt := plan.VisibleFrom, n.OccurredAt
	if visibleFrom.IsZero() {
		visibleFrom = now
	}
	if occurredAt.IsZero() {
		occurredAt = now
	}
	if _, err := tx.Exec(ctx, persistInboxSQL, nid, string(id.Realm), id.Tenant.Kind, id.Tenant.ID,
		id.Recipient.Kind, id.Recipient.ID, string(n.Topic), string(n.Priority), data, n.Title, n.Body,
		payload, attributes, n.IdemKey, plan.InboxVisible, visibleFrom, occurredAt, now); err != nil {
		return domain.Receipt{}, fmt.Errorf("persist: inbox row: %w", err)
	}

	var queued []queuedDelivery
	var dropped []droppedDelivery
	var digested []domain.ChannelKind
	for _, d := range plan.Deliveries {
		switch d.Mode {
		case domain.PlanImmediate, domain.PlanDeferred:
			q := queuedDelivery{Channel: string(d.Channel), Fallback: make([]string, 0, len(d.Fallback)), Quiet: d.QuietHoursOverride}
			for _, f := range d.Fallback {
				q.Fallback = append(q.Fallback, string(f))
			}
			if !d.NotBefore.IsZero() {
				at := d.NotBefore
				q.NotBefore = &at
			}
			queued = append(queued, q)
		case domain.PlanDropped:
			dropped = append(dropped, droppedDelivery{Channel: string(d.Channel), Reason: d.Reason})
		case domain.PlanDigest:
			digested = append(digested, d.Channel)
		}
	}

	identity := identityArgs(id)
	if len(queued) > 0 {
		rows, err := json.Marshal(queued)
		if err != nil {
			return domain.Receipt{}, err
		}
		var deliverBefore *time.Time
		if !n.DeliverBefore.IsZero() {
			deliverBefore = &n.DeliverBefore
		}
		args := append([]any{nid, now}, identity...)
		args = append(args, string(n.Topic), string(n.Priority), int16(plan.Shard), deliverBefore, rows)
		if _, err := tx.Exec(ctx, persistQueueSQL, args...); err != nil {
			return domain.Receipt{}, fmt.Errorf("persist: queue rows: %w", err)
		}
		if s.notify {
			// Delivered to a listener at the commit, and discarded with a rollback, so
			// it can never announce work that is not there (D22, D28).
			if _, err := tx.Exec(ctx, announceSQL, dispatchChannel); err != nil {
				return domain.Receipt{}, fmt.Errorf("persist: announce the work: %w", err)
			}
		}
	}
	if len(dropped) > 0 {
		for i := range dropped {
			if dropped[i].Reason == "" {
				dropped[i].Reason = "quota"
			}
		}
		rows, err := json.Marshal(dropped)
		if err != nil {
			return domain.Receipt{}, err
		}
		args := append([]any{nid, now}, identity...)
		args = append(args, string(n.Topic), now, rows)
		if _, err := tx.Exec(ctx, persistDroppedSQL, args...); err != nil {
			return domain.Receipt{}, fmt.Errorf("persist: dropped records: %w", err)
		}
	}
	for _, ch := range digested {
		m := domain.DigestMember{Identity: id, Topic: n.Topic, Channel: ch, Shard: plan.Shard, NotificationID: nid}
		if _, err := appendDigest(ctx, tx, m, plan.DigestWindow, plan.DigestMax); err != nil {
			return domain.Receipt{}, fmt.Errorf("persist: digest append on %s: %w", ch, err)
		}
	}

	return domain.Receipt{
		NotificationID: nid,
		IdemKey:        n.IdemKey,
		Applied:        true,
		InboxVisible:   plan.InboxVisible,
		Deliveries:     append([]domain.PlannedDelivery(nil), plan.Deliveries...),
	}, nil
}

// jsonObject encodes a map as a JSON object; a nil or empty map is {}, never null,
// so the column's default and its reads agree.
func jsonObject[V any](m map[string]V) ([]byte, error) {
	if len(m) == 0 {
		return []byte("{}"), nil
	}
	return json.Marshal(m)
}
