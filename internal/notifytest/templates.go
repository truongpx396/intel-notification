package notifytest

import (
	"context"
	"fmt"
	"maps"
	"strings"
	"sync"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

type templateKey struct {
	realm   domain.Realm
	tenant  domain.Tenant // the zero value for a realm-wide template
	ref     string
	channel domain.ChannelKind
	locale  string
}

// storedTemplate is one version of one template: a domain.Template a producer wrote, or, for what Put
// stores, content a test wants returned as it is.
type storedTemplate struct {
	tpl    domain.Template
	static *domain.RenderedContent
	active bool
}

// Templates is an in-memory ports.TemplateRenderer and ports.TemplateWriter, held to the same
// contract as the PostgreSQL one (TemplateRendererContract). It resolves which template a request
// means, by the locale ladder and then by tenant, and renders the stored text over the request's data
// through the domain's restricted engine.
//
// Two ways in, for two kinds of test. PutTemplate and ActivateTemplate are the Catalog's: versioned
// text a producer wrote, which the contract suite runs. Put stores content a dispatcher or planner
// test wants back exactly as it is, active at once, and is rendered by nothing.
type Templates struct {
	mu            sync.Mutex
	defaultLocale string
	m             map[templateKey][]*storedTemplate // index i is version i+1
}

var (
	_ ports.TemplateRenderer = (*Templates)(nil)
	_ ports.TemplateWriter   = (*Templates)(nil)
)

// NewTemplates returns an empty set. defaultLocale is where the locale ladder ends.
func NewTemplates(defaultLocale string) *Templates {
	return &Templates{defaultLocale: strings.ToLower(defaultLocale), m: map[templateKey][]*storedTemplate{}}
}

// Put stores content for (realm, ref, channel, locale), replacing anything stored there, and
// activates it. Locales compare case-insensitively, as BCP 47 tags do.
func (t *Templates) Put(realm domain.Realm, ref string, ch domain.ChannelKind, locale string, c domain.RenderedContent) {
	c.Data = maps.Clone(c.Data)
	t.mu.Lock()
	defer t.mu.Unlock()
	t.m[templateKey{realm: realm, ref: ref, channel: ch, locale: strings.ToLower(locale)}] =
		[]*storedTemplate{{static: &c, active: true}}
}

// PutTemplate implements ports.TemplateWriter.
func (t *Templates) PutTemplate(_ context.Context, realm domain.Realm, tpl domain.Template) (domain.Template, error) {
	if err := realm.Validate(); err != nil {
		return domain.Template{}, err
	}
	if err := tpl.Validate(); err != nil {
		return domain.Template{}, err
	}
	tpl.Locale = domain.NormalizeLocale(tpl.Locale)
	tpl.Data = maps.Clone(tpl.Data)
	t.mu.Lock()
	defer t.mu.Unlock()
	k := templateKey{realm, tpl.Tenant, tpl.Ref, tpl.Channel, tpl.Locale}
	tpl.Version = len(t.m[k]) + 1
	t.m[k] = append(t.m[k], &storedTemplate{tpl: tpl})
	return tpl, nil
}

// ActivateTemplate implements ports.TemplateWriter.
func (t *Templates) ActivateTemplate(_ context.Context, realm domain.Realm, v domain.TemplateVersion) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	versions := t.m[templateKey{realm, v.Tenant, v.Ref, v.Channel, domain.NormalizeLocale(v.Locale)}]
	if v.Version < 1 || v.Version > len(versions) {
		return fmt.Errorf("%w: realm %q, template %q, channel %q, locale %q, version %d",
			domain.ErrUnknownTemplate, realm, v.Ref, v.Channel, v.Locale, v.Version)
	}
	for i, s := range versions {
		s.active = i == v.Version-1
	}
	return nil
}

// Render implements ports.TemplateRenderer. It walks the locale ladder, the request's tag, then its
// language, then the default, and at each rung prefers the tenant's own template to the realm-wide
// one. It returns domain.ErrNoTemplate when no rung has an active template for the ref and channel.
// An empty TemplateRef means the topic's own name.
func (t *Templates) Render(_ context.Context, r domain.RenderRequest) (domain.RenderedContent, error) {
	ref := r.TemplateRef
	if ref == "" {
		ref = string(r.Topic)
	}
	tenants := []domain.Tenant{{}}
	if r.Tenant != (domain.Tenant{}) {
		tenants = []domain.Tenant{r.Tenant, {}}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, locale := range domain.LocaleLadder(r.Locale, t.defaultLocale) {
		for _, tenant := range tenants {
			for _, s := range t.m[templateKey{r.Realm, tenant, ref, r.Channel, locale}] {
				if !s.active {
					continue
				}
				if s.static != nil {
					c := *s.static
					c.Data = maps.Clone(c.Data)
					return c, nil
				}
				return s.tpl.Render(r)
			}
		}
	}
	return domain.RenderedContent{}, fmt.Errorf("%w: realm %q, template %q, channel %q, locale %q",
		domain.ErrNoTemplate, r.Realm, ref, r.Channel, r.Locale)
}
