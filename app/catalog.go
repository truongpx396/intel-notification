package app

import (
	"context"
	"fmt"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// Catalog is ports.Catalog over the data-backed writers: the checks a registration must pass, and then
// the write (T049, D31). What it checks is what the engine could not run later: a channel nobody
// registered, a template outside the restricted map, a set of addresses it cannot use. A producer is told
// at the call that stored it, and not by a delivery that failed hours afterwards.
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
	var g gaps
	g.need("Channels", d.Channels == nil)
	g.need("TopicWriter", d.TopicWriter == nil)
	g.need("TemplateWriter", d.TemplateWriter == nil)
	g.need("AddressWriter", d.AddressWriter == nil)
	if err := g.err("Catalog"); err != nil {
		return nil, err
	}
	return &Catalog{channels: d.Channels, topics: d.TopicWriter, templates: d.TemplateWriter, addresses: d.AddressWriter}, nil
}

// unknownChannel is the refusal of a channel nobody registered, naming the field it was found in.
func unknownChannel(field string, ch domain.ChannelKind) error {
	return &domain.ValidationError{Problems: []domain.Problem{{
		Field: field, Message: fmt.Sprintf("channel %q is not registered, so nothing could deliver on it", ch), Err: domain.ErrUnknownChannel,
	}}}
}

// UpsertTopic implements ports.Catalog.
func (c *Catalog) UpsertTopic(ctx context.Context, realm domain.Realm, topic domain.Topic, def domain.TopicDef) error {
	if err := domain.ValidateTopicRegistration(realm, topic, def); err != nil {
		return fmt.Errorf("upsert topic: %w", err)
	}
	for field, chs := range map[string][]domain.ChannelKind{"DefaultChannels": def.DefaultChannels, "Fallback": def.Fallback} {
		for _, ch := range chs {
			if _, ok := c.channels.Get(ch); !ok {
				return fmt.Errorf("upsert topic: %w", unknownChannel(field, ch))
			}
		}
	}
	return c.topics.UpsertTopic(ctx, realm, topic, def)
}

// PutTemplate implements ports.Catalog.
func (c *Catalog) PutTemplate(ctx context.Context, realm domain.Realm, t domain.Template) (domain.Template, error) {
	if err := realm.Validate(); err != nil {
		return domain.Template{}, fmt.Errorf("put template: %w", err)
	}
	if err := t.Validate(); err != nil {
		return domain.Template{}, fmt.Errorf("put template: %w", err)
	}
	if _, ok := c.channels.Get(t.Channel); !ok {
		return domain.Template{}, fmt.Errorf("put template: %w", unknownChannel("Channel", t.Channel))
	}
	return c.templates.PutTemplate(ctx, realm, t)
}

// ActivateTemplate implements ports.Catalog.
func (c *Catalog) ActivateTemplate(ctx context.Context, realm domain.Realm, v domain.TemplateVersion) (domain.Template, error) {
	if err := realm.Validate(); err != nil {
		return domain.Template{}, fmt.Errorf("activate template: %w", err)
	}
	return c.templates.ActivateTemplate(ctx, realm, v)
}

// PutAddresses implements ports.Catalog. Address equality is the channel's: the keys are made over what
// its normalizer says, as the dispatcher makes them when it resolves the same addresses (D25).
func (c *Catalog) PutAddresses(ctx context.Context, id domain.Identity, ch domain.ChannelKind, addrs []domain.Address) ([]domain.Address, error) {
	if err := id.Validate(); err != nil {
		return nil, fmt.Errorf("put addresses: %w", err)
	}
	if err := id.Realm.Validate(); err != nil {
		return nil, fmt.Errorf("put addresses: %w", err)
	}
	channel, ok := c.channels.Get(ch)
	if !ok {
		return nil, fmt.Errorf("put addresses: %w", unknownChannel("Channel", ch))
	}
	var normalize func(string) string
	if n, ok := channel.(ports.AddressNormalizer); ok {
		normalize = n.NormalizeAddress
	}
	kept, keys, err := domain.PrepareAddresses(ch, addrs, normalize)
	if err != nil {
		return nil, fmt.Errorf("put addresses: %w", err)
	}
	if err := c.addresses.PutAddresses(ctx, id, ch, kept, keys); err != nil {
		return nil, err
	}
	return kept, nil
}
