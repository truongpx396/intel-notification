package app

import (
	"context"
	"errors"

	notify "github.com/truongpx396/intel-notification"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// Notifier is the engine's entry point for producers: ports.Notifier over a
// planner and the store.
type Notifier struct{}

var _ ports.Notifier = (*Notifier)(nil)

// NewNotifier returns a Notifier for cfg's realm over d.
func NewNotifier(cfg notify.Config, d Deps) (*Notifier, error) {
	return &Notifier{}, nil
}

// Notify implements ports.Notifier.
func (n *Notifier) Notify(ctx context.Context, nt domain.Notification) (domain.Receipt, error) {
	return domain.Receipt{}, errNotImplemented
}

// NotifyTx implements ports.Notifier.
func (n *Notifier) NotifyTx(ctx context.Context, tx ports.Tx, nt domain.Notification) (domain.Receipt, error) {
	return domain.Receipt{}, errNotImplemented
}

// Broadcast implements ports.Notifier. It is built with T042.
func (n *Notifier) Broadcast(ctx context.Context, b domain.BroadcastRequest) (domain.BroadcastReceipt, error) {
	return domain.BroadcastReceipt{}, errors.New("app: broadcast is not implemented yet (T042)")
}

// Cancel implements ports.Notifier.
func (n *Notifier) Cancel(ctx context.Context, c domain.CancelRequest) (domain.CancelReceipt, error) {
	return domain.CancelReceipt{}, errNotImplemented
}

// Status implements ports.Notifier.
func (n *Notifier) Status(ctx context.Context, s domain.StatusRequest) (domain.NotificationStatus, error) {
	return domain.NotificationStatus{}, errNotImplemented
}
