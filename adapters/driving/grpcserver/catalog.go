package grpcserver

import (
	"context"

	"google.golang.org/grpc"

	"github.com/truongpx396/intel-notification/api/notifyv1"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// RealmFunc is the realm of a call, which the transport derived from the caller and never from the
// request (NR-009): auth.RealmOf, in service mode.
type RealmFunc func(ctx context.Context) (domain.Realm, error)

// CatalogServer implements notifyv1.CatalogServer over ports.Catalog and ports.Admin: the management
// surface of service mode (D31).
type CatalogServer struct {
	notifyv1.UnimplementedCatalogServer
	c       ports.Catalog
	a       ports.Admin
	realm   RealmFunc
	onError func(error)
}

var _ notifyv1.CatalogServer = (*CatalogServer)(nil)

// CatalogOption configures a CatalogServer.
type CatalogOption func(*CatalogServer)

// WithCatalogErrorHandler is given every fault of the engine's own that the server hides from the caller.
func WithCatalogErrorHandler(h func(error)) CatalogOption {
	return func(s *CatalogServer) { s.onError = h }
}

// NewCatalog returns a CatalogServer. realm says which realm each call acts in.
func NewCatalog(c ports.Catalog, a ports.Admin, realm RealmFunc, opts ...CatalogOption) *CatalogServer {
	s := &CatalogServer{c: c, a: a, realm: realm, onError: func(error) {}}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Register adds the service to reg.
func (s *CatalogServer) Register(reg grpc.ServiceRegistrar) { notifyv1.RegisterCatalogServer(reg, s) }
