// Package grpcserver serves the notify.v1 Notifier service over a ports.Notifier: the producer surface
// of service mode (contracts/notification-ports.md § Service surface).
package grpcserver

import (
	"google.golang.org/grpc"

	"github.com/truongpx396/intel-notification/api/notifyv1"
	"github.com/truongpx396/intel-notification/ports"
)

// MaxBatch is the most notifications one NotifyBatch may carry.
const MaxBatch = 500

// Server implements notifyv1.NotifierServer.
type Server struct {
	notifyv1.UnimplementedNotifierServer
	n       ports.Notifier
	onError func(error)
}

// Option configures a Server.
type Option func(*Server)

// WithErrorHandler is given every fault of the engine's own that the server hides from the producer.
func WithErrorHandler(h func(error)) Option { return func(s *Server) { s.onError = h } }

// New returns a Server over n.
func New(n ports.Notifier, opts ...Option) *Server {
	s := &Server{n: n, onError: func(error) {}}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Register adds the service to reg.
func (s *Server) Register(reg grpc.ServiceRegistrar) { notifyv1.RegisterNotifierServer(reg, s) }
