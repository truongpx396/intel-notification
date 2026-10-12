package postgres

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/truongpx396/intel-notification/domain"
)

const statusGuardSQL = `
SELECT notification_id::text, notification_created_at FROM notify_idem
 WHERE realm = $1 AND tenant_kind = $2 AND tenant_id = $3
   AND recipient_kind = $4 AND recipient_id = $5 AND idem_key = $6`

// statusCanceledSQL reads the inbox row's cancel flag under the identity's scope.
const statusCanceledSQL = `
SELECT canceled_at IS NOT NULL FROM notifications WHERE id = $1::uuid AND created_at = $2`

// statusWindowsSQL finds the digest windows a notification was folded into. A
// digested channel has no queue row of its own: its delivery is the window's.
const statusWindowsSQL = `
SELECT id::text, channel, flushed_at IS NOT NULL FROM digest_buffer
 WHERE realm = $1 AND tenant_kind = $2 AND tenant_id = $3
   AND recipient_kind = $4 AND recipient_id = $5 AND $6::uuid = ANY(member_ids)`

// statusPendingSQL and statusFinishedSQL read a notification's deliveries, and a
// digest window's, from the queue and from the history. $1 is a notification id and
// $2 an array of window ids; either side may match nothing.
const statusPendingSQL = `
SELECT channel, address_key, attempts FROM notification_outbox
 WHERE notification_id = $1::uuid OR digest_id = ANY($2::uuid[])`

const statusFinishedSQL = `
SELECT channel, address_key, outcome, attempts, coalesce(provider_message_id, ''),
       coalesce(provider_status, ''), completed_at
  FROM notification_deliveries
 WHERE notification_id = $1::uuid OR digest_id = ANY($2::uuid[])`

// Status implements ports.Store. It reads one snapshot of the notification's
// deliveries: those still pending, those finished, and, for a channel that was
// digested, the window's, so a delivery that finishes meanwhile is listed once.
// They come back ordered by channel and address, a pending delivery before a
// finished one.
func (s *Store) Status(ctx context.Context, id domain.Identity, idemKey string) (domain.NotificationStatus, error) {
	var st domain.NotificationStatus
	err := readScoped(ctx, s.pool, id, func(tx pgx.Tx) error {
		var createdAt time.Time
		err := tx.QueryRow(ctx, statusGuardSQL, append(identityArgs(id), idemKey)...).Scan(&st.NotificationID, &createdAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		st.Found = true
		err = tx.QueryRow(ctx, statusCanceledSQL, st.NotificationID, createdAt).Scan(&st.Canceled)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}

		rows, err := tx.Query(ctx, statusWindowsSQL, append(identityArgs(id), st.NotificationID)...)
		if err != nil {
			return err
		}
		var windows []string
		var open []domain.DeliveryStatus
		for rows.Next() {
			var w, channel string
			var flushed bool
			if err := rows.Scan(&w, &channel, &flushed); err != nil {
				rows.Close()
				return err
			}
			windows = append(windows, w)
			if !flushed { // still waiting in an open or sealed window
				open = append(open, domain.DeliveryStatus{Channel: domain.ChannelKind(channel), State: "pending"})
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		st.Deliveries = append(st.Deliveries, open...)

		pending, err := tx.Query(ctx, statusPendingSQL, st.NotificationID, windows)
		if err != nil {
			return err
		}
		for pending.Next() {
			d := domain.DeliveryStatus{State: "pending"}
			var channel string
			if err := pending.Scan(&channel, &d.AddressKey, &d.Attempts); err != nil {
				pending.Close()
				return err
			}
			d.Channel = domain.ChannelKind(channel)
			st.Deliveries = append(st.Deliveries, d)
		}
		pending.Close()
		if err := pending.Err(); err != nil {
			return err
		}

		finished, err := tx.Query(ctx, statusFinishedSQL, st.NotificationID, windows)
		if err != nil {
			return err
		}
		for finished.Next() {
			var d domain.DeliveryStatus
			var channel string
			if err := finished.Scan(&channel, &d.AddressKey, &d.State, &d.Attempts, &d.ProviderMessageID,
				&d.ProviderStatus, &d.CompletedAt); err != nil {
				finished.Close()
				return err
			}
			d.Channel = domain.ChannelKind(channel)
			st.Deliveries = append(st.Deliveries, d)
		}
		finished.Close()
		return finished.Err()
	})
	if err != nil {
		return domain.NotificationStatus{}, fmt.Errorf("status: %w", err)
	}
	slices.SortStableFunc(st.Deliveries, cmpDeliveries)
	return st, nil
}

// cmpDeliveries orders by channel, then address, then pending before finished, then
// by when it finished.
func cmpDeliveries(a, b domain.DeliveryStatus) int {
	if c := strings.Compare(string(a.Channel), string(b.Channel)); c != 0 {
		return c
	}
	if c := strings.Compare(a.AddressKey, b.AddressKey); c != 0 {
		return c
	}
	if (a.State == "pending") != (b.State == "pending") {
		if a.State == "pending" {
			return -1
		}
		return 1
	}
	return a.CompletedAt.Compare(b.CompletedAt)
}

// recordProviderStatusSQL finds a delivery by the provider's message id WITHIN its
// channel: two providers may mint the same id, and a bounce for one must not mark
// the other's delivery.
const recordProviderStatusSQL = `
UPDATE notification_deliveries SET provider_status = $3, provider_status_at = $4
 WHERE channel = $1 AND provider_message_id = $2`

// RecordProviderStatus implements ports.Store.
func (s *Store) RecordProviderStatus(ctx context.Context, ch domain.ChannelKind, providerMessageID, status string) error {
	if ch == "" || providerMessageID == "" || status == "" {
		return errors.New("record provider status: channel, provider message id and status are all required")
	}
	tag, err := s.pool.Exec(ctx, recordProviderStatusSQL, string(ch), providerMessageID, status,
		s.clock.Now().Truncate(time.Microsecond))
	if err != nil {
		return fmt.Errorf("record provider status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("record provider status: %s message %q: %w", ch, providerMessageID, domain.ErrUnknownDelivery)
	}
	return nil
}
