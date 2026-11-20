package grpcclient

import (
	"context"
	"errors"

	"google.golang.org/grpc"

	"github.com/truongpx396/intel-notification/api/notifyv1"
	"github.com/truongpx396/intel-notification/domain"
)

// Catalog is a producer's client of the management service (D31). Its methods are ports.Catalog's and
// ports.Admin's without the realm, which the service derives from who is calling (NR-009): a client
// cannot name one, so it cannot write into another product's key space.
type Catalog struct{ c notifyv1.CatalogClient }

// NewCatalog returns a Catalog over conn.
func NewCatalog(conn grpc.ClientConnInterface) *Catalog {
	return &Catalog{c: notifyv1.NewCatalogClient(conn)}
}

var errNotImplemented = errors.New("not implemented")

// UpsertTopic registers topic with its defaults, replacing any earlier registration, and returns what
// was stored.
func (c *Catalog) UpsertTopic(ctx context.Context, topic domain.Topic, def domain.TopicDef) (domain.TopicDef, error) {
	return domain.TopicDef{}, errNotImplemented
}

// PutTemplate stores t as a new, inactive version and returns it with the number the service assigned.
func (c *Catalog) PutTemplate(ctx context.Context, t domain.Template) (domain.Template, error) {
	return domain.Template{}, errNotImplemented
}

// ActivateTemplate makes one stored version the one that renders and returns it.
func (c *Catalog) ActivateTemplate(ctx context.Context, v domain.TemplateVersion) (domain.Template, error) {
	return domain.Template{}, errNotImplemented
}

// PutAddresses replaces a recipient's addresses on one channel and returns what was stored.
func (c *Catalog) PutAddresses(ctx context.Context, tenant domain.Tenant, recipient domain.Recipient, ch domain.ChannelKind, addrs []domain.Address) ([]domain.Address, error) {
	return nil, errNotImplemented
}

// SetTenantPreference sets a tenant's default and lock for one (topic, channel) pair.
func (c *Catalog) SetTenantPreference(ctx context.Context, p domain.TenantPreference) error {
	return errNotImplemented
}

// Erase removes one recipient's personal data everywhere and returns the rows removed per table.
func (c *Catalog) Erase(ctx context.Context, tenant domain.Tenant, recipient domain.Recipient) (map[string]int, error) {
	return nil, errNotImplemented
}
