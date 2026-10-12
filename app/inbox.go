package app

import (
	"context"
	"time"

	notify "github.com/truongpx396/intel-notification"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// Inbox is the recipient-facing use-case: ports.Inbox over the inbox store and the
// template renderer.
type Inbox struct{}

var _ ports.Inbox = (*Inbox)(nil)

// NewInbox returns an Inbox over d.
func NewInbox(cfg notify.Config, d Deps) (*Inbox, error) { return &Inbox{}, nil }

func (i *Inbox) List(ctx context.Context, id domain.Identity, q domain.ListQuery) (domain.Page, error) {
	return domain.Page{}, errNotImplemented
}

func (i *Inbox) Unread(ctx context.Context, id domain.Identity) (domain.UnreadCount, error) {
	return domain.UnreadCount{}, errNotImplemented
}

func (i *Inbox) MarkSeen(ctx context.Context, id domain.Identity, before time.Time) error {
	return errNotImplemented
}

func (i *Inbox) MarkRead(ctx context.Context, id domain.Identity, notificationIDs []string) error {
	return errNotImplemented
}

func (i *Inbox) MarkAllRead(ctx context.Context, id domain.Identity) error { return errNotImplemented }

func (i *Inbox) Archive(ctx context.Context, id domain.Identity, notificationIDs []string, archived bool) error {
	return errNotImplemented
}

func (i *Inbox) Preferences(ctx context.Context, id domain.Identity) ([]domain.ResolvedPreference, error) {
	return nil, errNotImplemented
}

func (i *Inbox) SetPreferences(ctx context.Context, id domain.Identity, p []domain.Preference) error {
	return errNotImplemented
}

func (i *Inbox) Schedule(ctx context.Context, id domain.Identity) (domain.DeliverySchedule, error) {
	return domain.DeliverySchedule{}, errNotImplemented
}

func (i *Inbox) SetSchedule(ctx context.Context, id domain.Identity, s domain.DeliverySchedule) error {
	return errNotImplemented
}

func (i *Inbox) Unsubscribe(ctx context.Context, token string) error { return errNotImplemented }
