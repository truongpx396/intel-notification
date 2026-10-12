package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// Inbox implements ports.InboxStore over one pool, as the owner role, under the
// recipient's scope (D17).
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

var errInboxNotBuilt = errors.New("not implemented")

// List implements ports.InboxStore.
func (i *Inbox) List(ctx context.Context, id domain.Identity, q domain.ListQuery) (domain.Page, error) {
	return domain.Page{}, errInboxNotBuilt
}

// Unread implements ports.InboxStore.
func (i *Inbox) Unread(ctx context.Context, id domain.Identity, limit int) (domain.UnreadCount, error) {
	return domain.UnreadCount{}, errInboxNotBuilt
}

// Get implements ports.InboxStore.
func (i *Inbox) Get(ctx context.Context, id domain.Identity, notificationID string) (*domain.Notification, error) {
	return nil, errInboxNotBuilt
}

// MarkSeen implements ports.InboxStore.
func (i *Inbox) MarkSeen(ctx context.Context, id domain.Identity, before time.Time) error {
	return errInboxNotBuilt
}

// MarkRead implements ports.InboxStore.
func (i *Inbox) MarkRead(ctx context.Context, id domain.Identity, ids []string) error {
	return errInboxNotBuilt
}

// MarkAllRead implements ports.InboxStore.
func (i *Inbox) MarkAllRead(ctx context.Context, id domain.Identity) error {
	return errInboxNotBuilt
}

// Archive implements ports.InboxStore.
func (i *Inbox) Archive(ctx context.Context, id domain.Identity, ids []string, archived bool) error {
	return errInboxNotBuilt
}
