package notifytest

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// The data-backed ports of service mode (T048, D31): a topic registry, a template renderer and an
// address book that a producer fills through the Catalog API instead of compiling into the process.
// Each contract is run by the in-memory fake in this package and by the PostgreSQL adapter, so
// neither can drift from what the other decided where the port leaves room.

// ---------------------------------------------------------------------------------------- topics

// TopicsWorld is an empty registry and the writer that fills it, for one test alone.
type TopicsWorld struct {
	Registry ports.TopicRegistry
	Writer   ports.TopicWriter
}

// TopicsFactory returns a world for t alone: a fresh fake, or an adapter over a database cloned for
// the test.
type TopicsFactory func(t *testing.T) TopicsWorld

const (
	realmA domain.Realm = "alpha"
	realmB domain.Realm = "beta"
)

func topicDef(mutate ...func(*domain.TopicDef)) domain.TopicDef {
	d := domain.TopicDef{
		DefaultChannels: []domain.ChannelKind{"in_app", "email"},
		Fallback:        []domain.ChannelKind{"sms"},
		DefaultPriority: domain.PriorityWarning,
		Essential:       true,
		TemplateRef:     "invoice-template",
	}
	for _, m := range mutate {
		m(&d)
	}
	return d
}

// TopicRegistryContract is the table every ports.TopicRegistry and ports.TopicWriter pair runs
// (NR-017): a topic registered at runtime is usable at once, with no migration and no restart; a
// registration is whole, per realm, and refused for the reasons a row would be.
func TopicRegistryContract(t *testing.T, newWorld TopicsFactory) {
	t.Helper()

	t.Run("a topic registered at runtime is found, with everything it was given (NR-017)", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		want := topicDef()
		if err := w.Writer.UpsertTopic(t.Context(), realmA, "invoice_overdue", want); err != nil {
			t.Fatal(err)
		}
		got, found, err := w.Registry.Lookup(t.Context(), realmA, "invoice_overdue")
		if err != nil || !found {
			t.Fatalf("Lookup = %+v, %v, %v, want the registered topic", got, found, err)
		}
		if !sameTopicDef(got, want) {
			t.Fatalf("Lookup = %+v, want %+v: every field the registration carried", got, want)
		}
	})

	t.Run("a topic nobody registered is not found, and that is not an error", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		got, found, err := w.Registry.Lookup(t.Context(), realmA, "nothing")
		if err != nil || found {
			t.Fatalf("Lookup = %+v, %v, %v, want found=false and no error", got, found, err)
		}
	})

	t.Run("registering again replaces the definition whole", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		mustUpsert(t, w, realmA, "invoice", topicDef())
		second := domain.TopicDef{DefaultChannels: []domain.ChannelKind{"sms"}, DefaultPriority: domain.PriorityInfo}
		mustUpsert(t, w, realmA, "invoice", second)
		got, _, err := w.Registry.Lookup(t.Context(), realmA, "invoice")
		if err != nil {
			t.Fatal(err)
		}
		if !sameTopicDef(got, second) {
			t.Fatalf("Lookup = %+v, want exactly %+v: nothing of the first registration is merged in", got, second)
		}
	})

	t.Run("a topic is per realm (NR-009)", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		mustUpsert(t, w, realmA, "invoice", topicDef())
		if _, found, err := w.Registry.Lookup(t.Context(), realmB, "invoice"); err != nil || found {
			t.Fatalf("Lookup in another realm = found %v, %v, want not found: realm %s's topic is not realm %s's", found, err, realmA, realmB)
		}
		other := topicDef(func(d *domain.TopicDef) {
			d.Essential = false
			d.DefaultChannels = []domain.ChannelKind{"push"}
			d.Fallback = nil
		})
		mustUpsert(t, w, realmB, "invoice", other)
		a, _, _ := w.Registry.Lookup(t.Context(), realmA, "invoice")
		b, _, _ := w.Registry.Lookup(t.Context(), realmB, "invoice")
		if !sameTopicDef(a, topicDef()) || !sameTopicDef(b, other) {
			t.Fatalf("realm A holds %+v and realm B %+v, want each its own", a, b)
		}
	})

	t.Run("List names the realm's topics, sorted, and no other realm's", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		for _, name := range []domain.Topic{"welcome", "invoice", "alert"} {
			mustUpsert(t, w, realmA, name, topicDef())
		}
		mustUpsert(t, w, realmB, "elsewhere", topicDef())
		got, err := w.Registry.List(t.Context(), realmA)
		if err != nil || !slices.Equal(got, []domain.Topic{"alert", "invoice", "welcome"}) {
			t.Fatalf("List = %v, %v, want [alert invoice welcome]", got, err)
		}
		none, err := w.Registry.List(t.Context(), "gamma")
		if err != nil || len(none) != 0 {
			t.Fatalf("List of a realm with no topics = %v, %v, want none", none, err)
		}
	})

	t.Run("a registration that would not validate is refused, and nothing is stored", func(t *testing.T) {
		t.Parallel()
		cases := []struct {
			name  string
			realm domain.Realm
			topic domain.Topic
			def   domain.TopicDef
		}{
			{"a channel twice", realmA, "t", topicDef(func(d *domain.TopicDef) { d.DefaultChannels = []domain.ChannelKind{"email", "email"} })},
			{"a channel both parallel and fallback", realmA, "t", topicDef(func(d *domain.TopicDef) { d.Fallback = []domain.ChannelKind{"email"} })},
			{"a priority that is none", realmA, "t", topicDef(func(d *domain.TopicDef) { d.DefaultPriority = "urgent" })},
			{"an empty channel kind", realmA, "t", topicDef(func(d *domain.TopicDef) { d.DefaultChannels = []domain.ChannelKind{""} })},
			{"no topic name", realmA, "", topicDef()},
			{"a realm that is no realm", "Not A Realm", "t", topicDef()},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				w := newWorld(t)
				err := w.Writer.UpsertTopic(t.Context(), tc.realm, tc.topic, tc.def)
				var v *domain.ValidationError
				if !errors.As(err, &v) {
					t.Fatalf("UpsertTopic = %v, want a *domain.ValidationError", err)
				}
				if names, _ := w.Registry.List(t.Context(), tc.realm); len(names) != 0 {
					t.Fatalf("List = %v after a refused registration, want nothing stored", names)
				}
			})
		}
	})

	t.Run("a refused re-registration leaves the registered one in force", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		mustUpsert(t, w, realmA, "invoice", topicDef())
		if err := w.Writer.UpsertTopic(t.Context(), realmA, "invoice", domain.TopicDef{DefaultPriority: "urgent"}); err == nil {
			t.Fatal("UpsertTopic of an invalid definition = nil, want an error")
		}
		got, found, _ := w.Registry.Lookup(t.Context(), realmA, "invoice")
		if !found || !sameTopicDef(got, topicDef()) {
			t.Fatalf("Lookup = %+v, %v, want the first registration untouched", got, found)
		}
	})

	t.Run("a topic with no channels is a topic, and its lists come back empty", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		def := domain.TopicDef{DefaultPriority: domain.PriorityInfo}
		mustUpsert(t, w, realmA, "inbox_only", def)
		got, found, err := w.Registry.Lookup(t.Context(), realmA, "inbox_only")
		if err != nil || !found || len(got.DefaultChannels) != 0 || len(got.Fallback) != 0 || got.Essential || got.TemplateRef != "" {
			t.Fatalf("Lookup = %+v, %v, %v, want a topic with no channels and the zero of every other field", got, found, err)
		}
	})

	t.Run("what Lookup returns is a copy", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		mustUpsert(t, w, realmA, "invoice", topicDef())
		got, _, _ := w.Registry.Lookup(t.Context(), realmA, "invoice")
		got.DefaultChannels[0] = "tampered"
		again, _, _ := w.Registry.Lookup(t.Context(), realmA, "invoice")
		if again.DefaultChannels[0] != "in_app" {
			t.Fatalf("a caller changed the registry through what Lookup returned: %+v", again)
		}
	})

	t.Run("the order of the channels is kept", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		def := topicDef(func(d *domain.TopicDef) {
			d.DefaultChannels = []domain.ChannelKind{"webhook", "in_app", "email"}
			d.Fallback = []domain.ChannelKind{"sms", "push", "slack"}
		})
		mustUpsert(t, w, realmA, "ordered", def)
		got, _, _ := w.Registry.Lookup(t.Context(), realmA, "ordered")
		if !sameTopicDef(got, def) {
			t.Fatalf("Lookup = %+v, want the channels in the order given: the fallback chain is tried in order (D29)", got)
		}
	})
}

func mustUpsert(t *testing.T, w TopicsWorld, realm domain.Realm, topic domain.Topic, def domain.TopicDef) {
	t.Helper()
	if err := w.Writer.UpsertTopic(t.Context(), realm, topic, def); err != nil {
		t.Fatalf("UpsertTopic(%s, %s): %v", realm, topic, err)
	}
}

func sameTopicDef(a, b domain.TopicDef) bool {
	return slices.Equal(a.DefaultChannels, b.DefaultChannels) && slices.Equal(a.Fallback, b.Fallback) &&
		a.DefaultPriority == b.DefaultPriority && a.Essential == b.Essential && a.TemplateRef == b.TemplateRef
}

// ------------------------------------------------------------------------------------- templates

// TemplatesWorld is an empty renderer and the writer that fills it, for one test alone. The renderer's
// locale ladder ends at DefaultLocale.
type TemplatesWorld struct {
	Renderer      ports.TemplateRenderer
	Writer        ports.TemplateWriter
	DefaultLocale string
}

// TemplatesFactory returns a world for t alone, with "en" as its default locale.
type TemplatesFactory func(t *testing.T) TemplatesWorld

var tenantT1 = domain.Tenant{Kind: "workspace", ID: "t1"}

func tpl(mutate ...func(*domain.Template)) domain.Template {
	d := domain.Template{Ref: "invoice", Channel: "email", Locale: "en", Subject: "Invoice {{ .number }}", Body: "Hello {{ .name }}"}
	for _, m := range mutate {
		m(&d)
	}
	return d
}

func renderReq(mutate ...func(*domain.RenderRequest)) domain.RenderRequest {
	r := domain.RenderRequest{Realm: realmA, Topic: "invoice", TemplateRef: "invoice", Channel: "email", Locale: "en",
		Data: map[string]any{"number": "INV-1", "name": "Ada"}}
	for _, m := range mutate {
		m(&r)
	}
	return r
}

// TemplateRendererContract is the table every ports.TemplateRenderer and ports.TemplateWriter pair
// runs (NR-019, D27): versions that are immutable and activated explicitly, a template refused when it
// is saved and not when it is sent, the locale ladder, a tenant's branding over the realm's, and the
// data rendered into the copy.
func TemplateRendererContract(t *testing.T, newWorld TemplatesFactory) {
	t.Helper()
	put := func(t *testing.T, w TemplatesWorld, realm domain.Realm, tpl domain.Template) domain.Template {
		t.Helper()
		stored, err := w.Writer.PutTemplate(t.Context(), realm, tpl)
		if err != nil {
			t.Fatalf("PutTemplate: %v", err)
		}
		return stored
	}
	activate := func(t *testing.T, w TemplatesWorld, realm domain.Realm, stored domain.Template) {
		t.Helper()
		got, err := w.Writer.ActivateTemplate(t.Context(), realm, domain.TemplateVersion{
			Tenant: stored.Tenant, Ref: stored.Ref, Channel: stored.Channel, Locale: stored.Locale, Version: stored.Version})
		if err != nil {
			t.Fatalf("ActivateTemplate: %v", err)
		}
		if got.Version != stored.Version || got.Subject != stored.Subject || got.Body != stored.Body || got.Ref != stored.Ref {
			t.Fatalf("ActivateTemplate = %+v, want the version it activated: %+v", got, stored)
		}
	}
	live := func(t *testing.T, w TemplatesWorld, realm domain.Realm, tpl domain.Template) {
		t.Helper()
		activate(t, w, realm, put(t, w, realm, tpl))
	}
	render := func(t *testing.T, w TemplatesWorld, mutate ...func(*domain.RenderRequest)) (domain.RenderedContent, error) {
		t.Helper()
		return w.Renderer.Render(t.Context(), renderReq(mutate...))
	}

	t.Run("a stored version renders for nobody until it is activated (D27)", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		stored := put(t, w, realmA, tpl())
		if got, err := render(t, w); !errors.Is(err, domain.ErrNoTemplate) {
			t.Fatalf("Render before activation = %+v, %v, want ErrNoTemplate", got, err)
		}
		activate(t, w, realmA, stored)
		got, err := render(t, w)
		if err != nil || got.Subject != "Invoice INV-1" || got.Body != "Hello Ada" || got.TemplateVersion != 1 {
			t.Fatalf("Render after activation = %+v, %v, want the version's copy rendered over the data", got, err)
		}
	})

	t.Run("a new version reaches no render until it is activated, and then replaces the old (D27)", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		v1 := put(t, w, realmA, tpl())
		activate(t, w, realmA, v1)
		v2 := put(t, w, realmA, tpl(func(d *domain.Template) { d.Subject = "Updated {{ .number }}" }))
		if v2.Version != 2 {
			t.Fatalf("second PutTemplate = version %d, want 2", v2.Version)
		}
		got, err := render(t, w)
		if err != nil || got.Subject != "Invoice INV-1" || got.TemplateVersion != 1 {
			t.Fatalf("Render with version 2 stored but not active = %+v, %v, want version 1: an edit reaches nobody until it is activated", got, err)
		}
		activate(t, w, realmA, v2)
		got, err = render(t, w)
		if err != nil || got.Subject != "Updated INV-1" || got.TemplateVersion != 2 {
			t.Fatalf("Render after activating version 2 = %+v, %v, want version 2", got, err)
		}
		// Going back is an activation too: a version is immutable and stays stored.
		activate(t, w, realmA, v1)
		got, err = render(t, w)
		if err != nil || got.TemplateVersion != 1 {
			t.Fatalf("Render after re-activating version 1 = %+v, %v, want version 1", got, err)
		}
	})

	t.Run("version numbers count up per template", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		a1 := put(t, w, realmA, tpl())
		a2 := put(t, w, realmA, tpl())
		french := put(t, w, realmA, tpl(func(d *domain.Template) { d.Locale = "fr" }))
		sms := put(t, w, realmA, tpl(func(d *domain.Template) { d.Channel = "sms" }))
		override := put(t, w, realmA, tpl(func(d *domain.Template) { d.Tenant = tenantT1 }))
		other := put(t, w, realmB, tpl())
		for _, c := range []struct {
			name string
			got  domain.Template
			want int
		}{
			{"first", a1, 1}, {"second", a2, 2}, {"another locale", french, 1}, {"another channel", sms, 1},
			{"a tenant's override", override, 1}, {"another realm", other, 1},
		} {
			if c.got.Version != c.want {
				t.Errorf("%s: PutTemplate = version %d, want %d: a template is numbered on its own", c.name, c.got.Version, c.want)
			}
		}
	})

	t.Run("a template that would not validate is refused when it is saved, and takes no version (D55)", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		for name, bad := range map[string]domain.Template{
			"a function outside the map": tpl(func(d *domain.Template) { d.Body = `{{ env "HOME" }}` }),
			"a syntax error":             tpl(func(d *domain.Template) { d.Body = `{{ .name ` }),
			"no ref":                     tpl(func(d *domain.Template) { d.Ref = "" }),
			"a locale that is no tag":    tpl(func(d *domain.Template) { d.Locale = "en US" }),
		} {
			if _, err := w.Writer.PutTemplate(t.Context(), realmA, bad); !errors.Is(err, domain.ErrInvalidTemplate) {
				t.Fatalf("PutTemplate(%s) = %v, want ErrInvalidTemplate", name, err)
			}
		}
		if stored := put(t, w, realmA, tpl()); stored.Version != 1 {
			t.Fatalf("PutTemplate after refusals = version %d, want 1: a refused template stored nothing", stored.Version)
		}
	})

	t.Run("activating a version nobody stored is ErrUnknownTemplate, and changes nothing", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		live(t, w, realmA, tpl())
		for name, v := range map[string]domain.TemplateVersion{
			"a version that is not stored": {Ref: "invoice", Channel: "email", Locale: "en", Version: 9},
			"version zero":                 {Ref: "invoice", Channel: "email", Locale: "en", Version: 0},
			"a template that is not there": {Ref: "other", Channel: "email", Locale: "en", Version: 1},
			"another channel":              {Ref: "invoice", Channel: "sms", Locale: "en", Version: 1},
			"another tenant's":             {Tenant: tenantT1, Ref: "invoice", Channel: "email", Locale: "en", Version: 1},
		} {
			if _, err := w.Writer.ActivateTemplate(t.Context(), realmA, v); !errors.Is(err, domain.ErrUnknownTemplate) {
				t.Errorf("ActivateTemplate(%s) = %v, want ErrUnknownTemplate", name, err)
			}
		}
		if _, err := w.Writer.ActivateTemplate(t.Context(), realmB, domain.TemplateVersion{Ref: "invoice", Channel: "email", Locale: "en", Version: 1}); !errors.Is(err, domain.ErrUnknownTemplate) {
			t.Errorf("ActivateTemplate in another realm = %v, want ErrUnknownTemplate: a template is per realm", err)
		}
		if got, err := render(t, w); err != nil || got.TemplateVersion != 1 {
			t.Fatalf("Render = %+v, %v, want version 1 still active", got, err)
		}
	})

	t.Run("locale falls back from the tag to its language to the default (NR-019)", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		for _, c := range []struct{ locale, subject string }{{"fr-CA", "fr-CA"}, {"fr", "fr"}, {"en", "en"}} {
			live(t, w, realmA, tpl(func(d *domain.Template) { d.Locale = c.locale; d.Subject = c.subject }))
		}
		for _, c := range []struct{ asked, want, why string }{
			{"fr-CA", "fr-CA", "the exact tag"},
			{"fr-ca", "fr-CA", "tags compare without regard to case"},
			{"FR-CA", "fr-CA", "tags compare without regard to case"},
			{"fr-BE", "fr", "a tag with no template falls back to its language"},
			{"fr", "fr", "the language"},
			{"de", "en", "a language with no template falls back to the default"},
			{"de-AT", "en", "and so does a tag whose language has none"},
			{"", "en", "no locale at all is the default's"},
			{"not a tag", "en", "a locale that is no tag is the default's, and does not fail the delivery"},
		} {
			got, err := render(t, w, func(r *domain.RenderRequest) { r.Locale = c.asked })
			if err != nil || got.Subject != c.want {
				t.Errorf("Render(locale %q) = %q, %v, want %q: %s", c.asked, got.Subject, err, c.want, c.why)
			}
		}
	})

	t.Run("nothing on the ladder is ErrNoTemplate, never a render of something else", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		live(t, w, realmA, tpl(func(d *domain.Template) { d.Locale = "fr" }))
		_, err := render(t, w, func(r *domain.RenderRequest) { r.Locale = "de" })
		if !errors.Is(err, domain.ErrNoTemplate) {
			t.Fatalf("Render = %v, want ErrNoTemplate: only fr is stored and the default, en, has nothing", err)
		}
	})

	t.Run("a tenant's own template beats the realm-wide one, for that tenant only (D27)", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		live(t, w, realmA, tpl(func(d *domain.Template) { d.Subject = "realm-wide" }))
		live(t, w, realmA, tpl(func(d *domain.Template) { d.Tenant = tenantT1; d.Subject = "t1's brand" }))
		for _, c := range []struct {
			tenant domain.Tenant
			want   string
		}{
			{tenantT1, "t1's brand"},
			{domain.Tenant{Kind: "workspace", ID: "t2"}, "realm-wide"},
			{domain.Tenant{Kind: "team", ID: "t1"}, "realm-wide"}, // the same id of another kind is another tenant
			{domain.Tenant{}, "realm-wide"},
		} {
			got, err := render(t, w, func(r *domain.RenderRequest) { r.Tenant = c.tenant })
			if err != nil || got.Subject != c.want {
				t.Errorf("Render for tenant %+v = %q, %v, want %q", c.tenant, got.Subject, err, c.want)
			}
		}
	})

	t.Run("the ladder is walked before the tenant: a language match outranks a tenant's default", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		live(t, w, realmA, tpl(func(d *domain.Template) { d.Locale = "fr"; d.Subject = "realm fr" }))
		live(t, w, realmA, tpl(func(d *domain.Template) { d.Tenant = tenantT1; d.Locale = "en"; d.Subject = "t1 en" }))
		got, err := render(t, w, func(r *domain.RenderRequest) { r.Tenant = tenantT1; r.Locale = "fr" })
		if err != nil || got.Subject != "realm fr" {
			t.Fatalf("Render = %q, %v, want the realm's French: a recipient reads French before they see a tenant's English", got.Subject, err)
		}
		got, err = render(t, w, func(r *domain.RenderRequest) { r.Tenant = tenantT1; r.Locale = "de" })
		if err != nil || got.Subject != "t1 en" {
			t.Fatalf("Render = %q, %v, want the tenant's English: at one rung the tenant's own wins", got.Subject, err)
		}
	})

	t.Run("a template is per realm, channel and ref", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		live(t, w, realmA, tpl())
		for name, mutate := range map[string]func(*domain.RenderRequest){
			"another realm":   func(r *domain.RenderRequest) { r.Realm = realmB },
			"another channel": func(r *domain.RenderRequest) { r.Channel = "sms" },
			"another ref":     func(r *domain.RenderRequest) { r.TemplateRef = "other" },
		} {
			if got, err := render(t, w, mutate); !errors.Is(err, domain.ErrNoTemplate) {
				t.Errorf("Render for %s = %+v, %v, want ErrNoTemplate", name, got, err)
			}
		}
	})

	t.Run("an empty template ref means the topic's own name", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		live(t, w, realmA, tpl(func(d *domain.Template) { d.Ref = "invoice_overdue" }))
		got, err := render(t, w, func(r *domain.RenderRequest) { r.Topic = "invoice_overdue"; r.TemplateRef = "" })
		if err != nil || got.Subject != "Invoice INV-1" {
			t.Fatalf("Render = %+v, %v, want the template named after the topic", got, err)
		}
		if _, err := render(t, w, func(r *domain.RenderRequest) { r.Topic = "other"; r.TemplateRef = "" }); !errors.Is(err, domain.ErrNoTemplate) {
			t.Fatalf("Render for another topic = %v, want ErrNoTemplate", err)
		}
	})

	t.Run("data a template needs and the producer did not send is a render error, not a miss", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		live(t, w, realmA, tpl())
		_, err := render(t, w, func(r *domain.RenderRequest) { r.Data = map[string]any{"number": "INV-1"} })
		if err == nil || errors.Is(err, domain.ErrNoTemplate) {
			t.Fatalf("Render = %v, want an error that is not ErrNoTemplate: a missing variable must not fall back to other copy and must not print \"<no value>\"", err)
		}
	})

	t.Run("structured data is rendered too, and what the producer sent is not run as text", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		live(t, w, realmA, tpl(func(d *domain.Template) {
			d.Channel = "slack"
			d.Data = map[string]any{"blocks": []any{map[string]any{"text": "Due {{ .number }}"}}, "static": "kept"}
		}))
		got, err := render(t, w, func(r *domain.RenderRequest) {
			r.Channel = "slack"
			r.Data = map[string]any{"number": "{{ env \"HOME\" }}", "name": "x"}
		})
		if err != nil {
			t.Fatal(err)
		}
		blocks, _ := got.Data["blocks"].([]any)
		block, _ := blocks[0].(map[string]any)
		if block["text"] != `Due {{ env "HOME" }}` || got.Data["static"] != "kept" {
			t.Fatalf("Data = %v, want the producer's text inserted as it is, never run", got.Data)
		}
	})

	t.Run("a digest renders over its members", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		live(t, w, realmA, tpl(func(d *domain.Template) {
			d.Subject = "{{ len .digest }} invoices"
			d.Body = `{{ range .digest }}{{ .data.number }};{{ end }}`
		}))
		got, err := render(t, w, func(r *domain.RenderRequest) {
			r.Data = nil
			r.Digest = []domain.Notification{{Topic: "invoice", Data: map[string]any{"number": "A"}}, {Topic: "invoice", Data: map[string]any{"number": "B"}}}
		})
		if err != nil || got.Subject != "2 invoices" || got.Body != "A;B;" {
			t.Fatalf("Render = %+v, %v", got, err)
		}
	})

	t.Run("a stored template is not changed by rendering it", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		live(t, w, realmA, tpl(func(d *domain.Template) { d.Data = map[string]any{"k": "{{ .name }}"} }))
		first, _ := render(t, w)
		second, err := render(t, w, func(r *domain.RenderRequest) { r.Data = map[string]any{"number": "2", "name": "Bob"} })
		if err != nil || first.Data["k"] != "Ada" || second.Data["k"] != "Bob" {
			t.Fatalf("renders = %v and %v, want each over its own data", first.Data, second.Data)
		}
	})

	t.Run("PutTemplate returns what it stored", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		got := put(t, w, realmA, tpl(func(d *domain.Template) { d.Locale = "FR-ca"; d.Tenant = tenantT1 }))
		if got.Version != 1 || got.Locale != "fr-ca" || got.Tenant != tenantT1 || !strings.Contains(got.Body, "{{") {
			t.Fatalf("PutTemplate = %+v, want version 1, the locale as it is compared, the tenant and the text as written", got)
		}
	})
}

// ------------------------------------------------------------------------------------- addresses

// AddressesWorld is an empty address book and the writer that fills it, for one test alone.
type AddressesWorld struct {
	Book   ports.AddressBook
	Writer ports.AddressWriter
}

// AddressesFactory returns a world for t alone.
type AddressesFactory func(t *testing.T) AddressesWorld

var (
	addrOwner = domain.Identity{Realm: "alpha", Tenant: domain.Tenant{Kind: "workspace", ID: "w1"}, Recipient: domain.Recipient{Kind: "user", ID: "u1"}}
)

// AddressBookContract is the table every ports.AddressBook and ports.AddressWriter pair runs (NR-028,
// D25): several addresses per (recipient, channel), each with state of its own; a write that replaces
// the set; an address that is the recipient's and nobody else's.
func AddressBookContract(t *testing.T, newWorld AddressesFactory) {
	t.Helper()
	put := func(t *testing.T, w AddressesWorld, id domain.Identity, ch domain.ChannelKind, addrs ...domain.Address) {
		t.Helper()
		if err := w.Writer.PutAddresses(t.Context(), id, ch, addrs, keysOf(ch, addrs)); err != nil {
			t.Fatalf("PutAddresses: %v", err)
		}
	}
	values := func(t *testing.T, w AddressesWorld, id domain.Identity, ch domain.ChannelKind) []string {
		t.Helper()
		got, err := w.Book.Resolve(t.Context(), id, ch)
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		out := make([]string, len(got))
		for i, a := range got {
			out[i] = a.Value
		}
		slices.Sort(out)
		return out
	}
	addr := func(value string) domain.Address { return domain.Address{Value: value} }

	t.Run("an identity with no addresses resolves to none, without error", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		got, err := w.Book.Resolve(t.Context(), addrOwner, "email")
		if err != nil || len(got) != 0 {
			t.Fatalf("Resolve = %v, %v, want no addresses and no error: a miss is terminal for the delivery, not a fault", got, err)
		}
	})

	t.Run("several addresses on one channel are all resolved, with what each carries (NR-028)", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		put(t, w, addrOwner, "push",
			domain.Address{Value: "token-phone", Locale: "fr", Timezone: "Europe/Paris", Meta: map[string]string{"platform": "ios"}},
			domain.Address{Value: "token-tablet", Locale: "en", Timezone: "America/New_York", Meta: map[string]string{"platform": "android"}},
			addr("token-watch"))
		got, err := w.Book.Resolve(t.Context(), addrOwner, "push")
		if err != nil || len(got) != 3 {
			t.Fatalf("Resolve = %+v, %v, want the three devices", got, err)
		}
		byValue := map[string]domain.Address{}
		for _, a := range got {
			byValue[a.Value] = a
			if a.Channel != "push" {
				t.Errorf("address %q reports channel %q, want push", a.Value, a.Channel)
			}
		}
		phone := byValue["token-phone"]
		if phone.Locale != "fr" || phone.Timezone != "Europe/Paris" || phone.Meta["platform"] != "ios" {
			t.Errorf("token-phone = %+v, want its locale, timezone and meta", phone)
		}
		if watch := byValue["token-watch"]; watch.Locale != "" || watch.Timezone != "" || len(watch.Meta) != 0 {
			t.Errorf("token-watch = %+v, want the zero of what it did not carry", watch)
		}
	})

	t.Run("a put replaces the set: what is not named is removed, what is, stays", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		put(t, w, addrOwner, "email", addr("a@x.test"), addr("b@x.test"))
		put(t, w, addrOwner, "email", domain.Address{Value: "b@x.test", Locale: "de"}, addr("c@x.test"))
		if got := values(t, w, addrOwner, "email"); !slices.Equal(got, []string{"b@x.test", "c@x.test"}) {
			t.Fatalf("addresses = %v, want [b@x.test c@x.test]", got)
		}
		resolved, _ := w.Book.Resolve(t.Context(), addrOwner, "email")
		for _, a := range resolved {
			if a.Value == "b@x.test" && a.Locale != "de" {
				t.Fatalf("b@x.test = %+v, want the new locale: an address already stored takes what the put carries", a)
			}
		}
	})

	t.Run("an empty put removes them all", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		put(t, w, addrOwner, "email", addr("a@x.test"))
		put(t, w, addrOwner, "email")
		if got := values(t, w, addrOwner, "email"); len(got) != 0 {
			t.Fatalf("addresses = %v, want none", got)
		}
	})

	t.Run("a channel's set is its own", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		put(t, w, addrOwner, "email", addr("a@x.test"))
		put(t, w, addrOwner, "sms", addr("+15550100"))
		put(t, w, addrOwner, "email", addr("b@x.test"))
		if got := values(t, w, addrOwner, "sms"); !slices.Equal(got, []string{"+15550100"}) {
			t.Fatalf("sms addresses = %v after replacing the email set, want them untouched", got)
		}
		if got := values(t, w, addrOwner, "email"); !slices.Equal(got, []string{"b@x.test"}) {
			t.Fatalf("email addresses = %v, want [b@x.test]", got)
		}
	})

	t.Run("an address is the recipient's, and nobody else's (NR-008)", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		put(t, w, addrOwner, "email", addr("owner@x.test"))
		others := map[string]domain.Identity{
			"another recipient":                   withRecipient(addrOwner, "user", "u2"),
			"the same id, another recipient kind": withRecipient(addrOwner, "device", "u1"),
			"the same recipient in another tenant": func() domain.Identity {
				i := addrOwner
				i.Tenant.ID = "w2"
				return i
			}(),
			"the same ids under another tenant kind": func() domain.Identity {
				i := addrOwner
				i.Tenant.Kind = "team"
				return i
			}(),
			"the same recipient in another realm": func() domain.Identity {
				i := addrOwner
				i.Realm = "beta"
				return i
			}(),
		}
		for name, id := range others {
			if got := values(t, w, id, "email"); len(got) != 0 {
				t.Errorf("%s resolves %v, want none: another identity's address is never one's own", name, got)
			}
		}
		put(t, w, others["another recipient"], "email", addr("u2@x.test"))
		if got := values(t, w, addrOwner, "email"); !slices.Equal(got, []string{"owner@x.test"}) {
			t.Fatalf("owner's addresses = %v after another recipient's put, want them untouched", got)
		}
	})

	t.Run("two addresses with one key are refused, and nothing changes", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		put(t, w, addrOwner, "email", addr("keep@x.test"))
		addrs := []domain.Address{addr("a@x.test"), addr("A@x.test")}
		same := domain.AddressKey("email", "a@x.test")
		if err := w.Writer.PutAddresses(t.Context(), addrOwner, "email", addrs, []string{same, same}); err == nil {
			t.Fatal("PutAddresses with two addresses of one key = nil, want an error")
		}
		if got := values(t, w, addrOwner, "email"); !slices.Equal(got, []string{"keep@x.test"}) {
			t.Fatalf("addresses = %v after a refused put, want the earlier set untouched", got)
		}
	})

	t.Run("keys that do not pair with the addresses are refused", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		if err := w.Writer.PutAddresses(t.Context(), addrOwner, "email", []domain.Address{addr("a@x.test")}, nil); err == nil {
			t.Fatal("PutAddresses with no keys = nil, want an error")
		}
	})

	t.Run("an address with no value is refused", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		if err := w.Writer.PutAddresses(t.Context(), addrOwner, "email", []domain.Address{addr("")}, []string{domain.AddressKey("email", "")}); err == nil {
			t.Fatal("PutAddresses of an empty value = nil, want an error")
		}
	})

	t.Run("an identity with an empty component is refused", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		for name, id := range map[string]domain.Identity{
			"no realm":     withRealm(addrOwner, ""),
			"no tenant":    withTenant(addrOwner, domain.Tenant{}),
			"no recipient": withRecipient(addrOwner, "", ""),
		} {
			err := w.Writer.PutAddresses(t.Context(), id, "email", []domain.Address{addr("a@x.test")}, keysOf("email", []domain.Address{addr("a@x.test")}))
			if !errors.Is(err, domain.ErrEmptyIdentity) {
				t.Errorf("PutAddresses(%s) = %v, want ErrEmptyIdentity: an empty component would equal an unset scope", name, err)
			}
		}
	})

	t.Run("what Resolve returns is a copy", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		put(t, w, addrOwner, "push", domain.Address{Value: "t", Meta: map[string]string{"k": "v"}})
		got, _ := w.Book.Resolve(t.Context(), addrOwner, "push")
		got[0].Meta["k"] = "tampered"
		again, _ := w.Book.Resolve(t.Context(), addrOwner, "push")
		if again[0].Meta["k"] != "v" {
			t.Fatalf("a caller changed the book through what Resolve returned: %+v", again)
		}
	})
}

func keysOf(ch domain.ChannelKind, addrs []domain.Address) []string {
	keys := make([]string, len(addrs))
	for i, a := range addrs {
		keys[i] = domain.AddressKey(ch, a.Value)
	}
	return keys
}

func withRecipient(i domain.Identity, kind, id string) domain.Identity {
	i.Recipient = domain.Recipient{Kind: kind, ID: id}
	return i
}

func withTenant(i domain.Identity, t domain.Tenant) domain.Identity {
	i.Tenant = t
	return i
}

func withRealm(i domain.Identity, r domain.Realm) domain.Identity {
	i.Realm = r
	return i
}
