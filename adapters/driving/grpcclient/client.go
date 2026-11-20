// Package grpcclient is a remote ports.Notifier: it calls the notify.v1 Notifier service and maps the
// answer back to the engine's own types, so a producer cannot tell it from the in-process one
// (NR-025). One line changes a Go host from library mode to service mode.
package grpcclient

import (
	"context"
	"errors"

	"google.golang.org/grpc"

	"github.com/truongpx396/intel-notification/api/notifyv1"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

var errNotImplemented = errors.New("grpcclient: not implemented")

// Client is a ports.Notifier over a gRPC connection.
type Client struct{ c notifyv1.NotifierClient }

var _ ports.Notifier = (*Client)(nil)

// New returns a Client over conn.
func New(conn grpc.ClientConnInterface) *Client { return &Client{c: notifyv1.NewNotifierClient(conn)} }

func (c *Client) Notify(context.Context, domain.Notification) (domain.Receipt, error) {
	return domain.Receipt{}, errNotImplemented
}

func (c *Client) NotifyTx(context.Context, ports.Tx, domain.Notification) (domain.Receipt, error) {
	return domain.Receipt{}, errNotImplemented
}

func (c *Client) Broadcast(context.Context, domain.BroadcastRequest) (domain.BroadcastReceipt, error) {
	return domain.BroadcastReceipt{}, errNotImplemented
}

func (c *Client) Cancel(context.Context, domain.CancelRequest) (domain.CancelReceipt, error) {
	return domain.CancelReceipt{}, errNotImplemented
}

func (c *Client) Status(context.Context, domain.StatusRequest) (domain.NotificationStatus, error) {
	return domain.NotificationStatus{}, errNotImplemented
}

// BatchResult is the answer to one notification of a NotifyBatch.
type BatchResult struct {
	Receipt domain.Receipt
	Err     error
}

// NotifyBatch sends up to 500 notifications in one call, each accepted or refused on its own.
func (c *Client) NotifyBatch(context.Context, []domain.Notification) ([]BatchResult, error) {
	return nil, errNotImplemented
}
