// Package grpcclient is a remote ports.Notifier: it calls the notify.v1 Notifier service and maps the
// answer back to the engine's own types, so a producer cannot tell it from the in-process one
// (NR-025). One line changes a Go host from library mode to service mode.
//
// The deadline and the retries are the producer's: a call that failed is retried with the same
// idempotency key, which is what the key is for. The client retries nothing itself.
package grpcclient

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc"

	"github.com/truongpx396/intel-notification/adapters/driving/wire"
	"github.com/truongpx396/intel-notification/api/notifyv1"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// MaxBatch is the most notifications one NotifyBatch may carry; the service refuses more.
const MaxBatch = 500

// ErrBatchTooLarge is returned by NotifyBatch for more than MaxBatch notifications, without a call.
var ErrBatchTooLarge = errors.New("grpcclient: a batch carries at most 500 notifications")

// Client is a ports.Notifier over a gRPC connection.
type Client struct{ c notifyv1.NotifierClient }

var _ ports.Notifier = (*Client)(nil)

// New returns a Client over conn.
func New(conn grpc.ClientConnInterface) *Client { return &Client{c: notifyv1.NewNotifierClient(conn)} }

// Notify implements ports.Notifier. Data that a protobuf Struct cannot hold exactly is refused here,
// before any call (an integer beyond 2^53 would arrive as another number).
func (c *Client) Notify(ctx context.Context, n domain.Notification) (domain.Receipt, error) {
	req, err := wire.NotificationToProto(n)
	if err != nil {
		return domain.Receipt{}, fmt.Errorf("notify: %w", err)
	}
	resp, err := c.c.Notify(ctx, req)
	if err != nil {
		return domain.Receipt{}, wire.FromStatus(err)
	}
	return wire.ReceiptFromProto(resp, n.IdemKey), nil
}

// NotifyTx implements ports.Notifier. A remote Notifier cannot join a caller's transaction: the
// producer's database is not the service's, so the service cannot commit with it (D28). It opens no
// transaction, makes no call and touches nothing it was given.
func (c *Client) NotifyTx(context.Context, ports.Tx, domain.Notification) (domain.Receipt, error) {
	return domain.Receipt{}, domain.ErrTxUnsupported
}

// BatchResult is the answer to one notification of a NotifyBatch: its receipt, or why it was not
// accepted.
type BatchResult struct {
	Receipt domain.Receipt
	Err     error
}

// NotifyBatch sends up to MaxBatch notifications in one call, each accepted or refused on its own, and
// returns one result per notification in the order sent. The error is for the call as a whole; a
// notification the service refused is its own result's Err, a message and not a sentinel, because the
// wire carries the batch's refusals as text. A notification whose Data cannot cross is that result's
// Err, and is not sent.
func (c *Client) NotifyBatch(ctx context.Context, ns []domain.Notification) ([]BatchResult, error) {
	if len(ns) > MaxBatch {
		return nil, ErrBatchTooLarge
	}
	req := &notifyv1.NotifyBatchRequest{}
	results := make([]BatchResult, len(ns))
	sent := make([]int, 0, len(ns)) // the index in ns of each notification actually sent
	for i, n := range ns {
		p, err := wire.NotificationToProto(n)
		if err != nil {
			results[i].Err = fmt.Errorf("notify: %w", err)
			continue
		}
		req.Notifications = append(req.Notifications, p)
		sent = append(sent, i)
	}
	if len(sent) == 0 {
		return results, nil
	}
	resp, err := c.c.NotifyBatch(ctx, req)
	if err != nil {
		return nil, wire.FromStatus(err)
	}
	if len(resp.GetReceipts()) != len(sent) || len(resp.GetErrors()) != len(sent) {
		return nil, fmt.Errorf("grpcclient: the service answered %d receipts and %d errors for %d notifications",
			len(resp.GetReceipts()), len(resp.GetErrors()), len(sent))
	}
	for k, i := range sent {
		if msg := resp.GetErrors()[k]; msg != "" {
			results[i].Err = errors.New(msg)
			continue
		}
		results[i].Receipt = wire.ReceiptFromProto(resp.GetReceipts()[k], ns[i].IdemKey)
	}
	return results, nil
}

// Broadcast implements ports.Notifier.
func (c *Client) Broadcast(ctx context.Context, b domain.BroadcastRequest) (domain.BroadcastReceipt, error) {
	req, err := wire.BroadcastToProto(b)
	if err != nil {
		return domain.BroadcastReceipt{}, fmt.Errorf("broadcast: %w", err)
	}
	resp, err := c.c.Broadcast(ctx, req)
	if err != nil {
		return domain.BroadcastReceipt{}, wire.FromStatus(err)
	}
	return wire.BroadcastReceiptFromProto(resp), nil
}

// Cancel implements ports.Notifier.
func (c *Client) Cancel(ctx context.Context, r domain.CancelRequest) (domain.CancelReceipt, error) {
	resp, err := c.c.Cancel(ctx, wire.RefToProto(r))
	if err != nil {
		return domain.CancelReceipt{}, wire.FromStatus(err)
	}
	return wire.CancelReceiptFromProto(resp), nil
}

// Status implements ports.Notifier.
func (c *Client) Status(ctx context.Context, r domain.StatusRequest) (domain.NotificationStatus, error) {
	resp, err := c.c.Status(ctx, wire.RefToProto(r))
	if err != nil {
		return domain.NotificationStatus{}, wire.FromStatus(err)
	}
	return wire.StatusFromProto(resp), nil
}
