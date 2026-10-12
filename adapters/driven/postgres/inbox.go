package postgres

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// Inbox implements ports.InboxStore over one pool, as the owner role, under the
// recipient's scope (D17). Every statement states the identity as well as relying on
// the forced row-level security, as Erase does: the policy is the guarantee, and a
// predicate the planner can use for the recipient index costs nothing.
type Inbox struct {
	pool *pgxpool.Pool
	settings
}

var _ ports.InboxStore = (*Inbox)(nil)

// NewInbox returns an Inbox over pool.
func NewInbox(pool *pgxpool.Pool, opts ...Option) *Inbox {
	return &Inbox{pool: pool, settings: newSettings(opts)}
}

// A page asks for DefaultPageSize rows when it names none, and never more than
// MaxPageSize: a client's limit is a request, not an allocation.
const (
	DefaultPageSize = 50
	MaxPageSize     = 200
)

// Every statement here takes the identity as $1-$5 and the instant the inbox is
// read at as $6.
//
// inboxShown is what the inbox shows (D26, D29): a row with an inbox-backed channel
// enabled, not canceled, whose send time has passed. Anything else is, to every
// method, absent.
const inboxShown = `inbox_visible AND canceled_at IS NULL AND visible_from <= $6`

// listStates is each state filter of a list. An unknown state is refused, so these
// fragments are the only SQL a caller's value can choose between.
var listStates = map[domain.InboxState]string{
	"":                   `archived_at IS NULL`,
	domain.InboxUnread:   `archived_at IS NULL AND read_at IS NULL`,
	domain.InboxRead:     `archived_at IS NULL AND read_at IS NOT NULL`,
	domain.InboxArchived: `archived_at IS NOT NULL`,
}

// listSQL is a page: $7 the topic ("" for any), $8 the row count to read. With a
// cursor, $9 is the created_at and $10 the id of the last row of the page before.
// The cursor is a keyset, so a notification that arrives between two pages shifts
// nothing, and ties in created_at are broken by id.
func listSQL(state string, withCursor bool) string {
	after := ``
	if withCursor {
		after = ` AND (created_at, id) < ($9::timestamptz, $10::uuid)`
	}
	return `
SELECT id::text, created_at, topic, priority, title, body, data, payload, seen_at, read_at, archived_at
  FROM notifications
 WHERE ` + identityPredicate + ` AND ` + inboxShown + ` AND ` + state + `
   AND ($7::text = '' OR topic = $7)` + after + `
 ORDER BY created_at DESC, id DESC
 LIMIT $8`
}

var (
	listFirstPage = map[domain.InboxState]string{}
	listNextPage  = map[domain.InboxState]string{}
)

func init() {
	for state, fragment := range listStates {
		listFirstPage[state] = listSQL(fragment, false)
		listNextPage[state] = listSQL(fragment, true)
	}
}

// List implements ports.InboxStore.
func (i *Inbox) List(ctx context.Context, id domain.Identity, q domain.ListQuery) (domain.Page, error) {
	if _, ok := listStates[q.State]; !ok {
		return domain.Page{}, fmt.Errorf("list: unknown state %q", q.State)
	}
	limit := q.Limit
	if limit <= 0 {
		limit = DefaultPageSize
	}
	limit = min(limit, MaxPageSize)

	args := append(identityArgs(id), i.clock.Now(), string(q.Topic), limit+1)
	sql := listFirstPage[q.State]
	if q.Cursor != "" {
		at, last, err := decodeCursor(q.Cursor)
		if err != nil {
			return domain.Page{}, err
		}
		args = append(args, at, last)
		sql = listNextPage[q.State]
	}

	var page domain.Page
	err := readScoped(ctx, i.pool, id, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, sql, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var (
				it                     domain.InboxItem
				topic, priority        string
				data                   map[string]any
				payload                map[string]string
				seen, read, archivedAt *time.Time
			)
			if err := rows.Scan(&it.ID, &it.CreatedAt, &topic, &priority, &it.Title, &it.Body, &data,
				&payload, &seen, &read, &archivedAt); err != nil {
				return err
			}
			it.Topic, it.Priority = domain.Topic(topic), domain.Priority(priority)
			if len(data) > 0 {
				it.Data = data
			}
			if len(payload) > 0 {
				it.Payload = payload
			}
			it.SeenAt, it.ReadAt, it.ArchivedAt = zeroIfNil(seen), zeroIfNil(read), zeroIfNil(archivedAt)
			page.Items = append(page.Items, it)
		}
		return rows.Err()
	})
	if err != nil {
		return domain.Page{}, fmt.Errorf("list: %w", err)
	}
	if len(page.Items) > limit { // one row past the page says there is a next one
		page.Items = page.Items[:limit]
		last := page.Items[limit-1]
		page.NextCursor = encodeCursor(last.CreatedAt, last.ID)
	}
	return page, nil
}

func zeroIfNil(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}

// encodeCursor and decodeCursor carry the keyset of the last row of a page. The
// cursor is opaque to a caller; a value this store did not issue is refused.
func encodeCursor(createdAt time.Time, id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatInt(createdAt.UnixMicro(), 10) + ":" + id))
}

func decodeCursor(cursor string) (time.Time, string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return time.Time{}, "", domain.ErrInvalidCursor
	}
	micros, id, ok := strings.Cut(string(raw), ":")
	if !ok || !uuidPattern.MatchString(id) {
		return time.Time{}, "", domain.ErrInvalidCursor
	}
	us, err := strconv.ParseInt(micros, 10, 64)
	if err != nil {
		return time.Time{}, "", domain.ErrInvalidCursor
	}
	return time.UnixMicro(us).UTC(), strings.ToLower(id), nil
}

var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// unreadSQL reads at most $7 rows from the partial unread index (D15). $7 is the
// limit plus one, so seeing it filled says there were more than the limit.
const unreadSQL = `
SELECT count(*) FROM (
    SELECT 1 FROM notifications
     WHERE ` + identityPredicate + ` AND ` + inboxShown + ` AND read_at IS NULL AND archived_at IS NULL
     LIMIT $7) AS unread`

// Unread implements ports.InboxStore.
func (i *Inbox) Unread(ctx context.Context, id domain.Identity, limit int) (domain.UnreadCount, error) {
	if limit < 1 {
		return domain.UnreadCount{}, fmt.Errorf("unread: limit must be >= 1, got %d", limit)
	}
	var n int
	err := readScoped(ctx, i.pool, id, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, unreadSQL, append(identityArgs(id), i.clock.Now(), limit+1)...).Scan(&n)
	})
	if err != nil {
		return domain.UnreadCount{}, fmt.Errorf("unread: %w", err)
	}
	return domain.UnreadCount{Count: min(n, limit), Capped: n > limit}, nil
}

// getSQL reads one notification the inbox shows. $7 is its id.
const getSQL = `
SELECT` + notificationColumns + `
  FROM notifications
 WHERE ` + identityPredicate + ` AND ` + inboxShown + ` AND id = $7::uuid`

// Get implements ports.InboxStore. An id that is not a UUID is not any
// notification's, so it is answered without asking the database.
func (i *Inbox) Get(ctx context.Context, id domain.Identity, notificationID string) (*domain.Notification, error) {
	if !uuidPattern.MatchString(notificationID) {
		return nil, nil
	}
	var n *domain.Notification
	err := readScoped(ctx, i.pool, id, func(tx pgx.Tx) error {
		var err error
		n, err = scanNotification(func(dest ...any) error {
			return tx.QueryRow(ctx, getSQL, append(identityArgs(id), i.clock.Now(), strings.ToLower(notificationID))...).Scan(dest...)
		})
		if errors.Is(err, pgx.ErrNoRows) {
			n, err = nil, nil
		}
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("get: %w", err)
	}
	return n, nil
}

// markSeenSQL marks, once, what the bell showed at $7: seen is the bell cleared,
// not the item read, and a second clear keeps the first time.
const markSeenSQL = `
UPDATE notifications SET seen_at = $6
 WHERE ` + identityPredicate + ` AND ` + inboxShown + ` AND seen_at IS NULL AND created_at <= $7`

// markAllReadSQL reads what the bell shows: what is archived was put away, and is
// left as it is.
const markAllReadSQL = `
UPDATE notifications SET read_at = $6
 WHERE ` + identityPredicate + ` AND ` + inboxShown + ` AND read_at IS NULL AND archived_at IS NULL`

// markIDsSQL changes one state on the named notifications ($7 is their ids) and
// reports how many of them the inbox shows. set is the assignment, and unlessAlready
// the test for the state already being so. An id already in the state is counted and
// left as it is, keeping the time it was set; one that is not the caller's, or not
// shown, is not counted, and the caller undoes the whole request.
func markIDsSQL(set, unlessAlready string) string {
	return `
WITH target AS (
    SELECT id, created_at, ` + unlessAlready + ` AS already
      FROM notifications
     WHERE ` + identityPredicate + ` AND ` + inboxShown + ` AND id = ANY($7::uuid[])
), changed AS (
    UPDATE notifications n SET ` + set + `
      FROM target t
     WHERE n.id = t.id AND n.created_at = t.created_at AND NOT t.already
    RETURNING 1
)
SELECT count(*) FROM target`
}

var (
	markReadSQL      = markIDsSQL(`read_at = $6`, `read_at IS NOT NULL`)
	markArchivedSQL  = markIDsSQL(`archived_at = $6`, `archived_at IS NOT NULL`)
	markUnarchiveSQL = markIDsSQL(`archived_at = NULL`, `archived_at IS NULL`)
)

// MarkSeen implements ports.InboxStore.
func (i *Inbox) MarkSeen(ctx context.Context, id domain.Identity, before time.Time) error {
	return i.write(ctx, id, markSeenSQL, before)
}

// MarkAllRead implements ports.InboxStore.
func (i *Inbox) MarkAllRead(ctx context.Context, id domain.Identity) error {
	return i.write(ctx, id, markAllReadSQL)
}

// write runs one UPDATE under the identity's scope.
func (i *Inbox) write(ctx context.Context, id domain.Identity, sql string, extra ...any) error {
	err := pgx.BeginFunc(ctx, i.pool, func(tx pgx.Tx) error {
		if err := setScope(ctx, tx, id); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, sql, append(append(identityArgs(id), i.clock.Now()), extra...)...)
		return err
	})
	if err != nil {
		return fmt.Errorf("inbox: %w", err)
	}
	return nil
}

// MarkRead implements ports.InboxStore.
func (i *Inbox) MarkRead(ctx context.Context, id domain.Identity, ids []string) error {
	return i.markIDs(ctx, id, ids, markReadSQL)
}

// Archive implements ports.InboxStore.
func (i *Inbox) Archive(ctx context.Context, id domain.Identity, ids []string, archived bool) error {
	if archived {
		return i.markIDs(ctx, id, ids, markArchivedSQL)
	}
	return i.markIDs(ctx, id, ids, markUnarchiveSQL)
}

// markIDs applies sql to the distinct ids in one transaction, and undoes it all if
// any is not the caller's. The error is the sentinel itself, naming no id, so the
// answer for an id that exists and one that does not is the same.
func (i *Inbox) markIDs(ctx context.Context, id domain.Identity, ids []string, sql string) error {
	distinct := make([]string, 0, len(ids))
	seen := make(map[string]bool, len(ids))
	for _, raw := range ids {
		if !uuidPattern.MatchString(raw) {
			return domain.ErrNotFound
		}
		if lower := strings.ToLower(raw); !seen[lower] {
			seen[lower] = true
			distinct = append(distinct, lower)
		}
	}
	if len(distinct) == 0 {
		return nil
	}
	err := pgx.BeginFunc(ctx, i.pool, func(tx pgx.Tx) error {
		if err := setScope(ctx, tx, id); err != nil {
			return err
		}
		var shown int
		if err := tx.QueryRow(ctx, sql, append(identityArgs(id), i.clock.Now(), distinct)...).Scan(&shown); err != nil {
			return err
		}
		if shown != len(distinct) {
			return domain.ErrNotFound
		}
		return nil
	})
	if errors.Is(err, domain.ErrNotFound) {
		return err
	}
	if err != nil {
		return fmt.Errorf("inbox: %w", err)
	}
	return nil
}
