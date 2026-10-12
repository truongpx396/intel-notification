package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/truongpx396/intel-notification/domain"
)

// notificationColumns is the inbox row as the dispatcher and the inbox read it.
const notificationColumns = `
	tenant_kind, tenant_id, recipient_kind, recipient_id, topic, priority, data, title, body,
	payload, attributes, idem_key, occurred_at`

// loadNotificationSQL reads the one row a claim delivers. The claim carries the row's
// partition key, so the read prunes to a single partition (D17). Under the claim's
// scope, so a claim cannot read a row that is not its own.
const loadNotificationSQL = `
SELECT canceled_at IS NOT NULL,` + notificationColumns + `
  FROM notifications WHERE id = $1::uuid AND created_at = $2`

// loadWindowSQL reads a digest window's members, and only for the identity the claim
// names: the window table is not row-level scoped, so the identity is in the predicate.
const loadWindowSQL = `
SELECT member_ids::text[] FROM digest_buffer
 WHERE id = $1::uuid AND realm = $2 AND tenant_kind = $3 AND tenant_id = $4
   AND recipient_kind = $5 AND recipient_id = $6`

// loadMembersSQL reads the members that have not been canceled, in the order they
// joined the window. Under the scope, so a member id that is not the recipient's
// reads as absent.
const loadMembersSQL = `
SELECT` + notificationColumns + `
  FROM unnest($1::uuid[]) WITH ORDINALITY AS m(id, pos)
  JOIN notifications n ON n.id = m.id
 WHERE n.canceled_at IS NULL
 ORDER BY m.pos`

// Load implements ports.Store. It returns what the claim delivers, read under the
// claim's own identity: the notification of a notification claim, or the members of
// a digest claim, oldest first, without those that were canceled.
//
// canceled is true when there is nothing to send: the notification is canceled, or
// every member of the digest is, or the row is gone. A gone row is an erased
// recipient, or an inbox partition retired under a delivery; neither is a fault of
// the delivery, and retrying it would only dead-letter it, so the dispatcher
// finishes it as canceled. When canceled, n and digest are nil.
func (s *Store) Load(ctx context.Context, c domain.Claim) (*domain.Notification, []domain.Notification, bool, error) {
	e := c.Entry
	var (
		n        *domain.Notification
		members  []domain.Notification
		canceled bool
	)
	err := readScoped(ctx, s.pool, e.Identity, func(tx pgx.Tx) error {
		var err error
		switch {
		case e.NotificationID != "" && e.DigestID == "":
			if e.NotificationCreatedAt.IsZero() {
				return errors.New("the claim has no notification created_at to read the inbox row by")
			}
			n, canceled, err = loadNotification(ctx, tx, e.Identity, e.NotificationID, e.NotificationCreatedAt)
		case e.DigestID != "" && e.NotificationID == "":
			members, canceled, err = loadDigest(ctx, tx, e.Identity, e.DigestID)
		default:
			return fmt.Errorf("the claim names a notification %q and a digest %q; it must name exactly one",
				e.NotificationID, e.DigestID)
		}
		return err
	})
	if err != nil {
		return nil, nil, false, fmt.Errorf("load %s: %w", e.ID, err)
	}
	return n, members, canceled, nil
}

func loadNotification(ctx context.Context, tx pgx.Tx, id domain.Identity, nid string, createdAt time.Time) (*domain.Notification, bool, error) {
	var canceled bool
	n, err := scanNotification(func(dest ...any) error {
		return tx.QueryRow(ctx, loadNotificationSQL, nid, createdAt).Scan(append([]any{&canceled}, dest...)...)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, true, nil
	}
	if err != nil {
		return nil, false, err
	}
	if canceled {
		return nil, true, nil
	}
	return n, false, nil
}

func loadDigest(ctx context.Context, tx pgx.Tx, id domain.Identity, window string) ([]domain.Notification, bool, error) {
	var ids []string
	err := tx.QueryRow(ctx, loadWindowSQL, append([]any{window}, identityArgs(id)...)...).Scan(&ids)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, true, nil
	}
	if err != nil {
		return nil, false, err
	}
	rows, err := tx.Query(ctx, loadMembersSQL, ids)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	var members []domain.Notification
	for rows.Next() {
		n, err := scanNotification(rows.Scan)
		if err != nil {
			return nil, false, err
		}
		members = append(members, *n)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	if len(members) == 0 {
		return nil, true, nil
	}
	return members, false, nil
}

// scanNotification reads the columns of notificationColumns, in order, into a
// Notification. Empty maps come back nil: a notification with no data has none,
// rather than an empty object.
func scanNotification(scan func(dest ...any) error) (*domain.Notification, error) {
	var (
		n                          domain.Notification
		topic, priority            string
		data                       map[string]any
		payload, attributes        map[string]string
		tenantKind, tenantID       string
		recipientKind, recipientID string
	)
	if err := scan(&tenantKind, &tenantID, &recipientKind, &recipientID, &topic, &priority, &data, &n.Title,
		&n.Body, &payload, &attributes, &n.IdemKey, &n.OccurredAt); err != nil {
		return nil, err
	}
	n.Tenant = domain.Tenant{Kind: tenantKind, ID: tenantID}
	n.Recipient = domain.Recipient{Kind: recipientKind, ID: recipientID}
	n.Topic, n.Priority = domain.Topic(topic), domain.Priority(priority)
	if len(data) > 0 {
		n.Data = data
	}
	if len(payload) > 0 {
		n.Payload = payload
	}
	if len(attributes) > 0 {
		n.Attributes = attributes
	}
	return &n, nil
}
