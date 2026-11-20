package notifytest

import (
	"context"
	"sync"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// ScriptedNotifier is a ports.Notifier that answers from functions and remembers what it was asked.
// It is the stand-in for the engine in a transport's unit tests (the gRPC server, the REST handlers),
// which assert on what crossed the seam and on what the transport does with an answer, and need no
// database to do it. The real Notifier is held to the contract by NotifierContract, and the transports
// are held to the same contract through it, so nothing here stands in for a guarantee.
//
// With no function set a method answers as a Notifier that accepts everything would: Notify applies
// and names the notification after its key, and the rest answer with the zero value.
type ScriptedNotifier struct {
	// Each On field, when set, answers its method. It is called without the lock held, so it may
	// block, which is how a test holds a call open.
	OnNotify    func(context.Context, domain.Notification) (domain.Receipt, error)
	OnNotifyTx  func(context.Context, ports.Tx, domain.Notification) (domain.Receipt, error)
	OnBroadcast func(context.Context, domain.BroadcastRequest) (domain.BroadcastReceipt, error)
	OnCancel    func(context.Context, domain.CancelRequest) (domain.CancelReceipt, error)
	OnStatus    func(context.Context, domain.StatusRequest) (domain.NotificationStatus, error)

	mu         sync.Mutex
	notifies   []domain.Notification
	notifyTxs  int
	broadcasts []domain.BroadcastRequest
	cancels    []domain.CancelRequest
	statuses   []domain.StatusRequest
}

var _ ports.Notifier = (*ScriptedNotifier)(nil)

// Notify implements ports.Notifier.
func (s *ScriptedNotifier) Notify(ctx context.Context, n domain.Notification) (domain.Receipt, error) {
	s.mu.Lock()
	s.notifies = append(s.notifies, n)
	s.mu.Unlock()
	if s.OnNotify != nil {
		return s.OnNotify(ctx, n)
	}
	return domain.Receipt{NotificationID: "n-" + n.IdemKey, IdemKey: n.IdemKey, Applied: true}, nil
}

// NotifyTx implements ports.Notifier.
func (s *ScriptedNotifier) NotifyTx(ctx context.Context, tx ports.Tx, n domain.Notification) (domain.Receipt, error) {
	s.mu.Lock()
	s.notifyTxs++
	s.mu.Unlock()
	if s.OnNotifyTx != nil {
		return s.OnNotifyTx(ctx, tx, n)
	}
	return domain.Receipt{NotificationID: "n-" + n.IdemKey, IdemKey: n.IdemKey, Applied: true}, nil
}

// Broadcast implements ports.Notifier.
func (s *ScriptedNotifier) Broadcast(ctx context.Context, b domain.BroadcastRequest) (domain.BroadcastReceipt, error) {
	s.mu.Lock()
	s.broadcasts = append(s.broadcasts, b)
	s.mu.Unlock()
	if s.OnBroadcast != nil {
		return s.OnBroadcast(ctx, b)
	}
	return domain.BroadcastReceipt{BroadcastID: "b-" + b.IdemKey, Applied: true}, nil
}

// Cancel implements ports.Notifier.
func (s *ScriptedNotifier) Cancel(ctx context.Context, c domain.CancelRequest) (domain.CancelReceipt, error) {
	s.mu.Lock()
	s.cancels = append(s.cancels, c)
	s.mu.Unlock()
	if s.OnCancel != nil {
		return s.OnCancel(ctx, c)
	}
	return domain.CancelReceipt{}, nil
}

// Status implements ports.Notifier.
func (s *ScriptedNotifier) Status(ctx context.Context, r domain.StatusRequest) (domain.NotificationStatus, error) {
	s.mu.Lock()
	s.statuses = append(s.statuses, r)
	s.mu.Unlock()
	if s.OnStatus != nil {
		return s.OnStatus(ctx, r)
	}
	return domain.NotificationStatus{}, nil
}

// Notified returns the notifications Notify was asked to accept, in order.
func (s *ScriptedNotifier) Notified() []domain.Notification {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]domain.Notification(nil), s.notifies...)
}

// TxCalls counts the calls to NotifyTx.
func (s *ScriptedNotifier) TxCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.notifyTxs
}

// Broadcasted returns the broadcasts Broadcast was asked to record, in order.
func (s *ScriptedNotifier) Broadcasted() []domain.BroadcastRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]domain.BroadcastRequest(nil), s.broadcasts...)
}

// Canceled returns the requests Cancel was asked, in order.
func (s *ScriptedNotifier) Canceled() []domain.CancelRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]domain.CancelRequest(nil), s.cancels...)
}

// StatusAsked returns the requests Status was asked, in order.
func (s *ScriptedNotifier) StatusAsked() []domain.StatusRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]domain.StatusRequest(nil), s.statuses...)
}
