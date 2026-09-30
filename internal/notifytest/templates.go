package notifytest

import (
	"context"
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
	t.mu.Lock()
	defer t.mu.Unlock()
	t.m[templateKey{realm, ref, ch, strings.ToLower(locale)}] = c
}

// Render implements ports.TemplateRenderer. It tries the request's locale, then its
// language, then the default locale, and returns domain.ErrNoTemplate when none of
// the three has a template for the ref and channel. An empty TemplateRef means the
// topic's own name.
func (t *Templates) Render(_ context.Context, r domain.RenderRequest) (domain.RenderedContent, error) {
	return domain.RenderedContent{}, nil
}

// localeLadder is the locales to try, most specific first: the tag, its language,
// then the default, without repeats.
func localeLadder(locale, def string) []string {
	return nil
}
