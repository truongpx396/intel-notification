package notifytest

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

type templateKey struct {
	realm   domain.Realm
	ref     string
	channel domain.ChannelKind
	locale  string
}

// Templates is an in-memory ports.TemplateRenderer over static content. It
// resolves which template a request means — the template ref, the channel, and the
// locale ladder — and returns what the test stored for it; it does not render
// Data into the copy. That is what a dispatcher or planner test needs from a
// renderer, which is a template to be found or not.
//
// RenderRequest.Tenant is ignored: per-tenant branding overrides land with the
// data-backed renderer's table (T048), which this fake will run.
type Templates struct {
	mu            sync.Mutex
	defaultLocale string
	m             map[templateKey]domain.RenderedContent
}

var _ ports.TemplateRenderer = (*Templates)(nil)

// NewTemplates returns an empty set. defaultLocale is where the locale ladder ends.
func NewTemplates(defaultLocale string) *Templates {
	return &Templates{defaultLocale: strings.ToLower(defaultLocale), m: map[templateKey]domain.RenderedContent{}}
}

// Put stores content for (realm, ref, channel, locale), replacing any earlier one.
// Locales compare case-insensitively, as BCP 47 tags do.
func (t *Templates) Put(realm domain.Realm, ref string, ch domain.ChannelKind, locale string, c domain.RenderedContent) {
	c.Data = maps.Clone(c.Data)
	t.mu.Lock()
	defer t.mu.Unlock()
	t.m[templateKey{realm, ref, ch, strings.ToLower(locale)}] = c
}

// Render implements ports.TemplateRenderer. It tries the request's locale, then its
// language, then the default locale, and returns domain.ErrNoTemplate when none of
// the three has a template for the ref and channel. An empty TemplateRef means the
// topic's own name.
func (t *Templates) Render(_ context.Context, r domain.RenderRequest) (domain.RenderedContent, error) {
	ref := r.TemplateRef
	if ref == "" {
		ref = string(r.Topic)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, locale := range localeLadder(r.Locale, t.defaultLocale) {
		if c, ok := t.m[templateKey{r.Realm, ref, r.Channel, locale}]; ok {
			c.Data = maps.Clone(c.Data)
			return c, nil
		}
	}
	return domain.RenderedContent{}, fmt.Errorf("%w: realm %q, template %q, channel %q, locale %q",
		domain.ErrNoTemplate, r.Realm, ref, r.Channel, r.Locale)
}

// localeLadder is the locales to try, most specific first: the tag, its language,
// then the default, without repeats. A tag with several subtags goes straight to
// its language, as the contract's ladder does.
func localeLadder(locale, def string) []string {
	locale, def = strings.ToLower(locale), strings.ToLower(def)
	var ladder []string
	add := func(l string) {
		if l != "" && !slices.Contains(ladder, l) {
			ladder = append(ladder, l)
		}
	}
	add(locale)
	if language, _, hasRegion := strings.Cut(locale, "-"); hasRegion {
		add(language)
	}
	add(def)
	return ladder
}
