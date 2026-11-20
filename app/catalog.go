package app

import (
	"context"
	"errors"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// Catalog is ports.Catalog over the data-backed writers: the checks a registration must pass, and then
// the write (T049, D31).
type Catalog struct {
	channels  ports.ChannelRegistry
	topics    ports.TopicWriter
	templates ports.TemplateWriter
	addresses ports.AddressWriter
}

var _ ports.Catalog = (*Catalog)(nil)

// NewCatalog returns a Catalog over d, and names every port it needs and d leaves nil. It takes no
// Config: it holds no realm of its own, because service mode serves several and each call names one.
func NewCatalog(d Deps) (*Catalog, error) {
	return &Catalog{channels: d.Channels, topics: d.TopicWriter, templates: d.TemplateWriter, addresses: d.AddressWriter}, nil
}

// UpsertTopic implements ports.Catalog.
func (c *Catalog) UpsertTopic(context.Context, domain.Realm, domain.Topic, domain.TopicDef) error {
	return errors.New("not implemented")
}

// PutTemplate implements ports.Catalog.
func (c *Catalog) PutTemplate(context.Context, domain.Realm, domain.Template) (domain.Template, error) {
	return domain.Template{}, errors.New("not implemented")
}

// ActivateTemplate implements ports.Catalog.
func (c *Catalog) ActivateTemplate(context.Context, domain.Realm, domain.TemplateVersion) (domain.Template, error) {
	return domain.Template{}, errors.New("not implemented")
}

// PutAddresses implements ports.Catalog.
func (c *Catalog) PutAddresses(context.Context, domain.Identity, domain.ChannelKind, []domain.Address) ([]domain.Address, error) {
	return nil, errors.New("not implemented")
}
