package grpcclient

import (
	"context"
	"fmt"

	"google.golang.org/grpc"

	"github.com/truongpx396/intel-notification/adapters/driving/wire"
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

// UpsertTopic registers topic with its defaults, replacing any earlier registration, and returns what
// was stored.
func (c *Catalog) UpsertTopic(ctx context.Context, topic domain.Topic, def domain.TopicDef) (domain.TopicDef, error) {
	resp, err := c.c.UpsertTopic(ctx, wire.TopicToProto(topic, def))
	if err != nil {
		return domain.TopicDef{}, wire.FromStatus(err)
	}
	_, stored := wire.TopicFromProto(resp)
	return stored, nil
}

// PutTemplate stores t as a new, inactive version and returns it with the number the service assigned.
// Structured data a protobuf Struct cannot hold exactly is refused here, before any call.
func (c *Catalog) PutTemplate(ctx context.Context, t domain.Template) (domain.Template, error) {
	req, err := wire.TemplateToProto(t)
	if err != nil {
		return domain.Template{}, fmt.Errorf("put template: %w", err)
	}
	resp, err := c.c.PutTemplate(ctx, req)
	if err != nil {
		return domain.Template{}, wire.FromStatus(err)
	}
	stored := wire.TemplateFromProto(resp)
	stored.Version = int(resp.GetVersion())
	return stored, nil
}

// ActivateTemplate makes one stored version the one that renders and returns it.
func (c *Catalog) ActivateTemplate(ctx context.Context, v domain.TemplateVersion) (domain.Template, error) {
	resp, err := c.c.ActivateTemplate(ctx, wire.TemplateVersionToProto(v))
	if err != nil {
		return domain.Template{}, wire.FromStatus(err)
	}
	active := wire.TemplateFromProto(resp)
	active.Version = int(resp.GetVersion())
	return active, nil
}

// PutAddresses replaces a recipient's addresses on one channel and returns what was stored.
func (c *Catalog) PutAddresses(ctx context.Context, tenant domain.Tenant, recipient domain.Recipient, ch domain.ChannelKind, addrs []domain.Address) ([]domain.Address, error) {
	resp, err := c.c.PutAddresses(ctx, wire.AddressSetToProto(wire.AddressSet{Tenant: tenant, Recipient: recipient, Channel: ch, Addresses: addrs}))
	if err != nil {
		return nil, wire.FromStatus(err)
	}
	stored := wire.AddressSetFromProto(resp)
	for i := range stored.Addresses {
		stored.Addresses[i].Channel = ch
	}
	return stored.Addresses, nil
}

// SetTenantPreference sets a tenant's default and lock for one (topic, channel) pair.
func (c *Catalog) SetTenantPreference(ctx context.Context, p domain.TenantPreference) error {
	if _, err := c.c.SetTenantPreference(ctx, wire.TenantPreferenceToProto(p)); err != nil {
		return wire.FromStatus(err)
	}
	return nil
}

// Erase removes one recipient's personal data everywhere and returns the rows removed per table.
func (c *Catalog) Erase(ctx context.Context, tenant domain.Tenant, recipient domain.Recipient) (map[string]int, error) {
	resp, err := c.c.Erase(ctx, &notifyv1.EraseRequest{
		Tenant:    &notifyv1.Tenant{Kind: tenant.Kind, Id: tenant.ID},
		Recipient: &notifyv1.Recipient{Kind: recipient.Kind, Id: recipient.ID},
	})
	if err != nil {
		return nil, wire.FromStatus(err)
	}
	return wire.EraseReceiptFromProto(resp), nil
}
