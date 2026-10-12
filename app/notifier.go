package app

import (
	"context"
	"errors"
	"fmt"

	notify "github.com/truongpx396/intel-notification"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// Notifier is the engine's entry point for producers: ports.Notifier over a planner
// and the store. It decides nothing about delivery itself (that is the planner's)
// and nothing about durability (that is the store's, in one transaction, guard
// first): it builds the identity, consults the optional pre-check, and reports.
//
// The realm is the engine's own, from its configuration. A Notification has no
// realm field and the Notifier reads none out of its free-form maps: what a
// producer writes there is never a routing input (NR-009).
type Notifier struct {
	cfg     notify.Config // the effective configuration
	planner *Planner
	store   ports.Store
	queue   ports.Queue
	pre     ports.PreCheck // nil: every Notify takes the durable path
}

var _ ports.Notifier = (*Notifier)(nil)

// NewNotifier returns a Notifier for cfg's realm over d. It refuses a configuration
// that does not validate, and names every port it needs and d leaves nil: the
// planner's four, the store and the queue. The pre-check is optional.
func NewNotifier(cfg notify.Config, d Deps) (*Notifier, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	var g gaps
	g.need("Channels", d.Channels == nil)
	g.need("Prefs", d.Prefs == nil)
	g.need("Queue", d.Queue == nil)
	g.need("Quotas", d.Quotas == nil)
	g.need("Store", d.Store == nil)
	g.need("Topics", d.Topics == nil)
	if err := g.err("Notifier"); err != nil {
		return nil, err
	}
	planner, err := NewPlanner(cfg, d)
	if err != nil {
		return nil, err
	}
	return &Notifier{cfg: cfg.WithDefaults(), planner: planner, store: d.Store, queue: d.Queue, pre: d.PreCheck}, nil
}

// Notify implements ports.Notifier. It persists and enqueues one notification in
// one transaction the store opens and commits, and returns after the commit, which
// is the acceptance point (D23). Only then is the pre-check written (D18).
func (n *Notifier) Notify(ctx context.Context, nt domain.Notification) (domain.Receipt, error) {
	return n.notify(ctx, nil, nt)
}

// NotifyTx implements ports.Notifier. It does the same inside the transaction the
// caller owns, which it never commits or rolls back (D28). It reads the pre-check
// and never writes it: the host may yet roll back, and an entry for a notification
// that was not committed would swallow the retry.
func (n *Notifier) NotifyTx(ctx context.Context, tx ports.Tx, nt domain.Notification) (domain.Receipt, error) {
	if tx == nil {
		return domain.Receipt{}, errors.New("notify: NotifyTx needs the caller's transaction; with none the engine would open and commit its own, which is Notify (D28)")
	}
	return n.notify(ctx, tx, nt)
}

// notify is the path both share. tx is nil for Notify.
//
// The pre-check is read first, and a hit answers the replay on its own: it was
// written only after a commit, under the durable guard's key, for no longer than
// the idempotency window, so a hit is a notification that was committed. The answer
// names it and nothing more (the entry holds an id, not whether the inbox shows it),
// and nothing is planned or persisted. A miss, and any fault in reading, is only a
// miss: the guard in the store's transaction is what decides whether a key is new,
// so a lost or empty pre-check costs a transaction and never a notification.
//
// A first apply by Notify is remembered once the store has committed. A replay is
// not: the receipt does not say how old the guard row is, and an entry that
// outlived it would answer "replay" past the window. A fault in writing is ignored.
func (n *Notifier) notify(ctx context.Context, tx ports.Tx, nt domain.Notification) (domain.Receipt, error) {
	if err := nt.Validate(); err != nil {
		return domain.Receipt{}, fmt.Errorf("notify: %w", err)
	}
	id := nt.Identity(n.cfg.Realm)
	key := domain.PreCheckKey(id, nt.IdemKey)

	if n.pre != nil {
		if notificationID, hit, err := n.pre.Seen(ctx, key); err == nil && hit {
			return domain.Receipt{NotificationID: notificationID, IdemKey: nt.IdemKey}, nil
		}
	}

	planned, err := n.planner.Plan(ctx, id, nt)
	if err != nil {
		return domain.Receipt{}, fmt.Errorf("notify: %w", err)
	}
	receipt, err := n.store.PersistAndEnqueue(ctx, tx, id, planned.Notification, planned.Plan)
	if err != nil {
		return domain.Receipt{}, fmt.Errorf("notify: %w", err)
	}

	if n.pre != nil && tx == nil && receipt.Applied {
		_ = n.pre.Remember(ctx, key, receipt.NotificationID, n.cfg.PreCheckTTL)
	}
	return receipt, nil
}

// Broadcast implements ports.Notifier. It is built with T042.
func (n *Notifier) Broadcast(context.Context, domain.BroadcastRequest) (domain.BroadcastReceipt, error) {
	return domain.BroadcastReceipt{}, errors.New("app: broadcast is not implemented yet (T042)")
}

// Cancel implements ports.Notifier. Pending deliveries stop and an in-flight one is
// reported, never recorded as canceled (D29). The pre-check is left alone: a
// canceled notification's key is still spent, so a producer's retry is a replay and
// not a second notification.
func (n *Notifier) Cancel(ctx context.Context, c domain.CancelRequest) (domain.CancelReceipt, error) {
	id, err := n.requestIdentity(c.Tenant, c.Recipient, c.IdemKey)
	if err != nil {
		return domain.CancelReceipt{}, fmt.Errorf("cancel: %w", err)
	}
	receipt, err := n.queue.Cancel(ctx, id, c.IdemKey)
	if err != nil {
		return domain.CancelReceipt{}, fmt.Errorf("cancel: %w", err)
	}
	return receipt, nil
}

// Status implements ports.Notifier: the state of every delivery of one
// notification by channel and address, for the asking tenant and recipient only
// (NR-032).
func (n *Notifier) Status(ctx context.Context, s domain.StatusRequest) (domain.NotificationStatus, error) {
	id, err := n.requestIdentity(s.Tenant, s.Recipient, s.IdemKey)
	if err != nil {
		return domain.NotificationStatus{}, fmt.Errorf("status: %w", err)
	}
	status, err := n.store.Status(ctx, id, s.IdemKey)
	if err != nil {
		return domain.NotificationStatus{}, fmt.Errorf("status: %w", err)
	}
	return status, nil
}

// requestIdentity is the identity a cancel or status request names, under the
// engine's realm, and the key it asks about. A component left empty would equal an
// unset scope (D17), so it is refused here, before any port is asked.
func (n *Notifier) requestIdentity(t domain.Tenant, r domain.Recipient, idemKey string) (domain.Identity, error) {
	id := domain.Identity{Realm: n.cfg.Realm, Tenant: t, Recipient: r}
	if err := id.Validate(); err != nil {
		return domain.Identity{}, err
	}
	if idemKey == "" {
		return domain.Identity{}, domain.ErrMissingIdem
	}
	return id, nil
}
