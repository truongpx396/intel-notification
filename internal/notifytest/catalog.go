package notifytest

import (
	"context"
	"sync"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// CatalogCall is one call the ScriptedCatalog or ScriptedAdmin received: which method, the realm it was
// given, and what it carried. The realm is what a transport test asserts on: it is derived from the
// caller and must never be the request's (NR-009).
type CatalogCall struct {
	Method string
	Realm  domain.Realm
	Topic  domain.Topic
	Def    domain.TopicDef
	Tpl    domain.Template
	Ver    domain.TemplateVersion
	ID     domain.Identity
	Ch     domain.ChannelKind
	Addrs  []domain.Address
	Pref   domain.TenantPreference
}

// ScriptedCatalog is a ports.Catalog that answers from functions and remembers what it was asked, the
// stand-in for the engine in the Catalog transport's tests (see ScriptedNotifier). With no function set a
// method accepts: a template is stored as version 1, and addresses come back as they were sent.
type ScriptedCatalog struct {
	OnUpsertTopic      func(context.Context, domain.Realm, domain.Topic, domain.TopicDef) error
	OnPutTemplate      func(context.Context, domain.Realm, domain.Template) (domain.Template, error)
	OnActivateTemplate func(context.Context, domain.Realm, domain.TemplateVersion) (domain.Template, error)
	OnPutAddresses     func(context.Context, domain.Identity, domain.ChannelKind, []domain.Address) ([]domain.Address, error)

	mu    sync.Mutex
	calls []CatalogCall
}

var _ ports.Catalog = (*ScriptedCatalog)(nil)

func (s *ScriptedCatalog) record(c CatalogCall) {
	s.mu.Lock()
	s.calls = append(s.calls, c)
	s.mu.Unlock()
}

// Calls returns every call received, in order.
func (s *ScriptedCatalog) Calls() []CatalogCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]CatalogCall(nil), s.calls...)
}

// UpsertTopic implements ports.Catalog.
func (s *ScriptedCatalog) UpsertTopic(ctx context.Context, realm domain.Realm, topic domain.Topic, def domain.TopicDef) error {
	s.record(CatalogCall{Method: "UpsertTopic", Realm: realm, Topic: topic, Def: def})
	if s.OnUpsertTopic != nil {
		return s.OnUpsertTopic(ctx, realm, topic, def)
	}
	return nil
}

// PutTemplate implements ports.Catalog.
func (s *ScriptedCatalog) PutTemplate(ctx context.Context, realm domain.Realm, t domain.Template) (domain.Template, error) {
	s.record(CatalogCall{Method: "PutTemplate", Realm: realm, Tpl: t})
	if s.OnPutTemplate != nil {
		return s.OnPutTemplate(ctx, realm, t)
	}
	t.Version = 1
	return t, nil
}

// ActivateTemplate implements ports.Catalog.
func (s *ScriptedCatalog) ActivateTemplate(ctx context.Context, realm domain.Realm, v domain.TemplateVersion) (domain.Template, error) {
	s.record(CatalogCall{Method: "ActivateTemplate", Realm: realm, Ver: v})
	if s.OnActivateTemplate != nil {
		return s.OnActivateTemplate(ctx, realm, v)
	}
	return domain.Template{Tenant: v.Tenant, Ref: v.Ref, Channel: v.Channel, Locale: v.Locale, Version: v.Version, Body: "stored"}, nil
}

// PutAddresses implements ports.Catalog.
func (s *ScriptedCatalog) PutAddresses(ctx context.Context, id domain.Identity, ch domain.ChannelKind, addrs []domain.Address) ([]domain.Address, error) {
	s.record(CatalogCall{Method: "PutAddresses", Realm: id.Realm, ID: id, Ch: ch, Addrs: addrs})
	if s.OnPutAddresses != nil {
		return s.OnPutAddresses(ctx, id, ch, addrs)
	}
	out := make([]domain.Address, len(addrs))
	for i, a := range addrs {
		a.Channel = ch
		out[i] = a
	}
	return out, nil
}

// ScriptedAdmin is a ports.Admin that remembers what it was asked and answers from functions: erasure
// removes nothing and a replay succeeds, unless a test says otherwise.
type ScriptedAdmin struct {
	OnErase               func(context.Context, domain.Identity) (map[string]int, error)
	OnSetTenantPreference func(context.Context, domain.Realm, domain.TenantPreference) error

	mu    sync.Mutex
	calls []CatalogCall
}

var _ ports.Admin = (*ScriptedAdmin)(nil)

// Calls returns every call received, in order.
func (s *ScriptedAdmin) Calls() []CatalogCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]CatalogCall(nil), s.calls...)
}

func (s *ScriptedAdmin) record(c CatalogCall) {
	s.mu.Lock()
	s.calls = append(s.calls, c)
	s.mu.Unlock()
}

// Erase implements ports.Admin.
func (s *ScriptedAdmin) Erase(ctx context.Context, id domain.Identity) (map[string]int, error) {
	s.record(CatalogCall{Method: "Erase", Realm: id.Realm, ID: id})
	if s.OnErase != nil {
		return s.OnErase(ctx, id)
	}
	return map[string]int{"notifications": 2}, nil
}

// ReplayDeadLetter implements ports.Admin.
func (s *ScriptedAdmin) ReplayDeadLetter(_ context.Context, realm domain.Realm, _ string) error {
	s.record(CatalogCall{Method: "ReplayDeadLetter", Realm: realm})
	return nil
}

// SetTenantPreference implements ports.Admin.
func (s *ScriptedAdmin) SetTenantPreference(ctx context.Context, realm domain.Realm, p domain.TenantPreference) error {
	s.record(CatalogCall{Method: "SetTenantPreference", Realm: realm, Pref: p})
	if s.OnSetTenantPreference != nil {
		return s.OnSetTenantPreference(ctx, realm, p)
	}
	return nil
}
