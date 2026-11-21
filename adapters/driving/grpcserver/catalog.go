package grpcserver

import (
	"context"

	"google.golang.org/grpc"

	"github.com/truongpx396/intel-notification/adapters/driving/wire"
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

func (s *CatalogServer) status(err error) error { return wire.ToStatus(err, s.onError) }

// UpsertTopic implements notifyv1.CatalogServer.
func (s *CatalogServer) UpsertTopic(ctx context.Context, req *notifyv1.Topic) (*notifyv1.Topic, error) {
	realm, err := s.realm(ctx)
	if err != nil {
		return nil, s.status(err)
	}
	topic, def := wire.TopicFromProto(req)
	if err := s.c.UpsertTopic(ctx, realm, topic, def); err != nil {
		return nil, s.status(err)
	}
	return wire.TopicToProto(topic, def), nil
}

// PutTemplate implements notifyv1.CatalogServer. The version in the request is ignored: the service
// assigns it.
func (s *CatalogServer) PutTemplate(ctx context.Context, req *notifyv1.Template) (*notifyv1.Template, error) {
	realm, err := s.realm(ctx)
	if err != nil {
		return nil, s.status(err)
	}
	stored, err := s.c.PutTemplate(ctx, realm, wire.TemplateFromProto(req))
	if err != nil {
		return nil, s.status(err)
	}
	return s.templateToProto(stored)
}

// ActivateTemplate implements notifyv1.CatalogServer.
func (s *CatalogServer) ActivateTemplate(ctx context.Context, req *notifyv1.TemplateVersion) (*notifyv1.Template, error) {
	realm, err := s.realm(ctx)
	if err != nil {
		return nil, s.status(err)
	}
	active, err := s.c.ActivateTemplate(ctx, realm, wire.TemplateVersionFromProto(req))
	if err != nil {
		return nil, s.status(err)
	}
	return s.templateToProto(active)
}

func (s *CatalogServer) templateToProto(t domain.Template) (*notifyv1.Template, error) {
	p, err := wire.TemplateToProto(t)
	if err != nil {
		return nil, s.status(err) // data the store holds that a Struct cannot: a fault, shown as one
	}
	return p, nil
}

// PutAddresses implements notifyv1.CatalogServer. The tenant and the recipient are the producer's data,
// and the realm is the binding's.
func (s *CatalogServer) PutAddresses(ctx context.Context, req *notifyv1.RecipientAddresses) (*notifyv1.RecipientAddresses, error) {
	realm, err := s.realm(ctx)
	if err != nil {
		return nil, s.status(err)
	}
	set := wire.AddressSetFromProto(req)
	id := domain.Identity{Realm: realm, Tenant: set.Tenant, Recipient: set.Recipient}
	stored, err := s.c.PutAddresses(ctx, id, set.Channel, set.Addresses)
	if err != nil {
		return nil, s.status(err)
	}
	set.Addresses = stored
	return wire.AddressSetToProto(set), nil
}

// SetTenantPreference implements notifyv1.CatalogServer.
func (s *CatalogServer) SetTenantPreference(ctx context.Context, req *notifyv1.TenantPreference) (*notifyv1.TenantPreference, error) {
	realm, err := s.realm(ctx)
	if err != nil {
		return nil, s.status(err)
	}
	pref := wire.TenantPreferenceFromProto(req)
	if err := s.a.SetTenantPreference(ctx, realm, pref); err != nil {
		return nil, s.status(err)
	}
	return wire.TenantPreferenceToProto(pref), nil
}

// Erase implements notifyv1.CatalogServer: one recipient's personal data, in the caller's realm (D35).
func (s *CatalogServer) Erase(ctx context.Context, req *notifyv1.EraseRequest) (*notifyv1.EraseReceipt, error) {
	realm, err := s.realm(ctx)
	if err != nil {
		return nil, s.status(err)
	}
	id := domain.Identity{
		Realm:     realm,
		Tenant:    domain.Tenant{Kind: req.GetTenant().GetKind(), ID: req.GetTenant().GetId()},
		Recipient: domain.Recipient{Kind: req.GetRecipient().GetKind(), ID: req.GetRecipient().GetId()},
	}
	removed, err := s.a.Erase(ctx, id)
	if err != nil {
		return nil, s.status(err)
	}
	return wire.EraseReceiptToProto(removed), nil
}
