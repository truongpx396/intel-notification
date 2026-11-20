// Package grpcserver serves the notify.v1 Notifier service over a ports.Notifier: the producer surface
// of service mode (contracts/notification-ports.md § Service surface).
//
// It is a translation and nothing more. Every rule a notification must meet is the Notifier's, so the
// service and the library refuse the same requests in the same words; the server adds only the two
// rules that belong to the wire: a key is required on every mutating call, and a batch is bounded.
package grpcserver

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/truongpx396/intel-notification/adapters/driving/wire"
	"github.com/truongpx396/intel-notification/api/notifyv1"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// MaxBatch is the most notifications one NotifyBatch may carry.
const MaxBatch = 500

// Server implements notifyv1.NotifierServer.
type Server struct {
	notifyv1.UnimplementedNotifierServer
	pick    func(context.Context) (ports.Notifier, error)
	onError func(error)
}

var _ notifyv1.NotifierServer = (*Server)(nil)

// Option configures a Server.
type Option func(*Server)

// WithErrorHandler is given every fault of the engine's own that the server hides from the producer:
// the real error, where the producer is told only that there was one.
func WithErrorHandler(h func(error)) Option { return func(s *Server) { s.onError = h } }

// New returns a Server over n. The realm is n's own: no message carries one (NR-009).
func New(n ports.Notifier, opts ...Option) *Server {
	return NewRouted(func(context.Context) (ports.Notifier, error) { return n, nil }, opts...)
}

// NewRouted returns a Server that asks pick, for every call, which Notifier serves it. It is service
// mode's: one process serves several realms, each with an engine of its own, and the realm is the
// caller's binding (auth.Route over what auth.RealmInterceptors derived), never the request's (NR-009).
// An error from pick is the call's answer, so a caller with no binding is refused before anything runs.
func NewRouted(pick func(context.Context) (ports.Notifier, error), opts ...Option) *Server {
	s := &Server{pick: pick, onError: func(error) {}}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Register adds the service to reg.
func (s *Server) Register(reg grpc.ServiceRegistrar) { notifyv1.RegisterNotifierServer(reg, s) }

func (s *Server) status(err error) error { return wire.ToStatus(err, s.onError) }

// missingKey is what a mutating call with no idem_key is told. The Notifier refuses it too; the server
// does not lean on that, because a Notifier behind it that forgot would accept a notification that
// nothing guards against a retry.
func missingKey(what string) error {
	return fmt.Errorf("%s: %w", what, domain.ErrMissingIdem)
}

// Notify implements notifyv1.NotifierServer.
func (s *Server) Notify(ctx context.Context, req *notifyv1.Notification) (*notifyv1.Receipt, error) {
	if req.GetIdemKey() == "" {
		return nil, s.status(missingKey("notify"))
	}
	engine, err := s.pick(ctx)
	if err != nil {
		return nil, s.status(err)
	}
	r, err := engine.Notify(ctx, wire.NotificationFromProto(req))
	if err != nil {
		return nil, s.status(err)
	}
	return wire.ReceiptToProto(r), nil
}

// NotifyBatch implements notifyv1.NotifierServer. A batch over MaxBatch is refused whole, before any
// of it is accepted, because a half-applied batch the producer cannot see the edge of is one it
// cannot retry. Within the limit each notification is its own: one the engine refuses does not stop
// the next, and receipts[i] and errors[i] answer notifications[i], with errors[i] empty when it was
// accepted, so a producer retries exactly the ones that failed.
func (s *Server) NotifyBatch(ctx context.Context, req *notifyv1.NotifyBatchRequest) (*notifyv1.NotifyBatchResponse, error) {
	items := req.GetNotifications()
	if len(items) > MaxBatch {
		return nil, wire.Reject(codes.InvalidArgument, wire.ReasonBatchTooLarge,
			fmt.Sprintf("a batch carries at most %d notifications, this one has %d", MaxBatch, len(items)))
	}
	engine, err := s.pick(ctx)
	if err != nil {
		return nil, s.status(err)
	}
	resp := &notifyv1.NotifyBatchResponse{Receipts: make([]*notifyv1.Receipt, len(items)), Errors: make([]string, len(items))}
	for i, item := range items {
		resp.Receipts[i] = &notifyv1.Receipt{}
		if item.GetIdemKey() == "" {
			resp.Errors[i] = missingKey("notify").Error()
			continue
		}
		r, err := engine.Notify(ctx, wire.NotificationFromProto(item))
		if err != nil {
			if ctx.Err() != nil { // the producer gave up: the rest of the batch is not worth starting
				return nil, s.status(err)
			}
			resp.Errors[i] = wire.Message(s.status(err))
			continue
		}
		resp.Receipts[i] = wire.ReceiptToProto(r)
	}
	return resp, nil
}

// Broadcast implements notifyv1.NotifierServer.
func (s *Server) Broadcast(ctx context.Context, req *notifyv1.BroadcastRequest) (*notifyv1.BroadcastReceipt, error) {
	if req.GetIdemKey() == "" {
		return nil, s.status(missingKey("broadcast"))
	}
	engine, err := s.pick(ctx)
	if err != nil {
		return nil, s.status(err)
	}
	r, err := engine.Broadcast(ctx, wire.BroadcastFromProto(req))
	if err != nil {
		return nil, s.status(err)
	}
	return wire.BroadcastReceiptToProto(r), nil
}

// Cancel implements notifyv1.NotifierServer.
func (s *Server) Cancel(ctx context.Context, req *notifyv1.NotificationRef) (*notifyv1.CancelReceipt, error) {
	if req.GetIdemKey() == "" {
		return nil, s.status(missingKey("cancel"))
	}
	engine, err := s.pick(ctx)
	if err != nil {
		return nil, s.status(err)
	}
	r, err := engine.Cancel(ctx, wire.RefFromProto(req))
	if err != nil {
		return nil, s.status(err)
	}
	return wire.CancelReceiptToProto(r), nil
}

// Status implements notifyv1.NotifierServer.
func (s *Server) Status(ctx context.Context, req *notifyv1.NotificationRef) (*notifyv1.NotificationStatus, error) {
	engine, err := s.pick(ctx)
	if err != nil {
		return nil, s.status(err)
	}
	st, err := engine.Status(ctx, wire.RefFromProto(req))
	if err != nil {
		return nil, s.status(err)
	}
	return wire.StatusToProto(st), nil
}
