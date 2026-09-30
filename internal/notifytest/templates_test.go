package notifytest

import (
	"errors"
	"slices"
	"testing"

	"github.com/truongpx396/intel-notification/domain"
)

func content(subject string) domain.RenderedContent {
	return domain.RenderedContent{Subject: subject, Body: subject + " body", TemplateVersion: 1}
}

func TestLocaleLadder(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		locale string
		def    string
		want   []string
	}{
		{"tag, language, default", "fr-CA", "en", []string{"fr-ca", "fr", "en"}},
		{"a bare language", "fr", "en", []string{"fr", "en"}},
		{"no locale", "", "en", []string{"en"}},
		{"the default itself", "en", "en", []string{"en"}},
		{"a tag whose language is the default", "en-GB", "en", []string{"en-gb", "en"}},
		{"case is folded", "FR-ca", "EN", []string{"fr-ca", "fr", "en"}},
		{"a tag with several subtags goes straight to its language", "zh-Hant-TW", "en", []string{"zh-hant-tw", "zh", "en"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := localeLadder(tc.locale, tc.def); !slices.Equal(got, tc.want) {
				t.Fatalf("localeLadder(%q, %q) = %v, want %v", tc.locale, tc.def, got, tc.want)
			}
		})
	}
}

func TestTemplatesRender(t *testing.T) {
	t.Parallel()
	const realm = domain.Realm("aisat")
	build := func() *Templates {
		tpl := NewTemplates("en")
		tpl.Put(realm, "invoice", "email", "en", content("invoice-en"))
		tpl.Put(realm, "invoice", "email", "fr", content("invoice-fr"))
		tpl.Put(realm, "invoice", "email", "fr-CA", content("invoice-fr-CA"))
		tpl.Put(realm, "invoice", "in_app", "en", content("invoice-inapp-en"))
		tpl.Put(realm, "welcome", "email", "en", content("welcome-en"))
		return tpl
	}
	req := func(f func(*domain.RenderRequest)) domain.RenderRequest {
		r := domain.RenderRequest{Realm: realm, Topic: "invoice_overdue", TemplateRef: "invoice", Channel: "email", Locale: "en"}
		f(&r)
		return r
	}
	cases := []struct {
		name    string
		request domain.RenderRequest
		want    string // the subject found; "" for ErrNoTemplate
		why     string
	}{
		{"the exact tag", req(func(r *domain.RenderRequest) { r.Locale = "fr-CA" }), "invoice-fr-CA", "the most specific match wins"},
		{"the tag, in another case", req(func(r *domain.RenderRequest) { r.Locale = "fr-ca" }), "invoice-fr-CA", "BCP 47 tags compare case-insensitively"},
		{"a tag with no template falls back to its language", req(func(r *domain.RenderRequest) { r.Locale = "fr-BE" }), "invoice-fr", ""},
		{"a language with no template falls back to the default", req(func(r *domain.RenderRequest) { r.Locale = "de" }), "invoice-en", ""},
		{"no locale is the default", req(func(r *domain.RenderRequest) { r.Locale = "" }), "invoice-en", ""},
		{"a tag whose language and default both exist prefers the language", req(func(r *domain.RenderRequest) { r.Locale = "fr-CH" }), "invoice-fr", ""},
		{"another channel has its own template", req(func(r *domain.RenderRequest) { r.Channel = "in_app" }), "invoice-inapp-en", ""},
		{"a channel with no template", req(func(r *domain.RenderRequest) { r.Channel = "sms" }), "", "nothing for sms at any locale"},
		{"an unknown ref", req(func(r *domain.RenderRequest) { r.TemplateRef = "nope" }), "", ""},
		{"another realm", req(func(r *domain.RenderRequest) { r.Realm = "other" }), "", "templates are per realm"},
		{"an empty TemplateRef is the topic's name",
			req(func(r *domain.RenderRequest) { r.TemplateRef = ""; r.Topic = "welcome" }), "welcome-en", ""},
		{"an explicit TemplateRef wins over the topic's name",
			req(func(r *domain.RenderRequest) { r.TemplateRef = "invoice"; r.Topic = "welcome" }), "invoice-en", ""},
		{"an empty TemplateRef for an unregistered topic",
			req(func(r *domain.RenderRequest) { r.TemplateRef = ""; r.Topic = "unregistered" }), "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := build().Render(t.Context(), tc.request)
			if tc.want == "" {
				if !errors.Is(err, domain.ErrNoTemplate) {
					t.Fatalf("Render = %+v, %v, want ErrNoTemplate: %s", got, err, tc.why)
				}
				return
			}
			if err != nil || got.Subject != tc.want {
				t.Fatalf("Render = %+v, %v, want subject %q: %s", got, err, tc.want, tc.why)
			}
		})
	}
}

// With no template at the default locale there is nowhere left to fall back to,
// even when another locale has one.
func TestTemplatesWithoutADefaultLocaleTemplate(t *testing.T) {
	t.Parallel()
	tpl := NewTemplates("en")
	tpl.Put("aisat", "invoice", "email", "fr", content("invoice-fr"))
	_, err := tpl.Render(t.Context(), domain.RenderRequest{Realm: "aisat", TemplateRef: "invoice", Channel: "email", Locale: "de"})
	if !errors.Is(err, domain.ErrNoTemplate) {
		t.Fatalf("err = %v, want ErrNoTemplate: de falls back to en, and there is no en", err)
	}
}

func TestTemplatesPutReplaces(t *testing.T) {
	t.Parallel()
	tpl := NewTemplates("en")
	tpl.Put("aisat", "invoice", "email", "en", content("first"))
	tpl.Put("aisat", "invoice", "email", "en", content("second"))
	got, err := tpl.Render(t.Context(), domain.RenderRequest{Realm: "aisat", TemplateRef: "invoice", Channel: "email", Locale: "en"})
	if err != nil || got.Subject != "second" {
		t.Fatalf("Render = %+v, %v, want the later Put", got, err)
	}
}

// The renderer returns content a test may edit; the stored template must not change
// with it.
func TestTemplatesDoNotShareMemoryWithCallers(t *testing.T) {
	t.Parallel()
	tpl := NewTemplates("en")
	c := domain.RenderedContent{Subject: "s", Data: map[string]any{"k": "v"}}
	tpl.Put("aisat", "invoice", "email", "en", c)
	c.Data["k"] = "tampered-after-put"

	req := domain.RenderRequest{Realm: "aisat", TemplateRef: "invoice", Channel: "email", Locale: "en"}
	got, _ := tpl.Render(t.Context(), req)
	if got.Data["k"] != "v" {
		t.Fatalf("a map the caller kept changed the stored template: %v", got.Data)
	}
	got.Data["k"] = "tampered-after-render"
	again, _ := tpl.Render(t.Context(), req)
	if again.Data["k"] != "v" {
		t.Fatalf("a map returned by Render changed the stored template: %v", again.Data)
	}
}
