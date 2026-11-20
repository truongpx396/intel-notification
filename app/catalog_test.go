package app_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/truongpx396/intel-notification/app"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/notifytest"
	"github.com/truongpx396/intel-notification/ports"
)

// T049, NR-017, NR-019, NR-028, D31. The Catalog checks a registration against what the engine can
// actually run, and only then writes it, under the realm it is given.

const (
	catRealm  domain.Realm = "alpha"
	catRealm2 domain.Realm = "beta"
)

// writes counts what reached the data-backed writers, so a refusal can be shown to have stopped before
// them and not merely to have been refused by them too.
type writes struct {
	mu        sync.Mutex
	topics    int
	templates int
	addresses int
	lastKeys  []string
	lastID    domain.Identity
}

type countingTopics struct {
	*notifytest.Topics
	w *writes
}

func (c countingTopics) UpsertTopic(ctx context.Context, r domain.Realm, t domain.Topic, d domain.TopicDef) error {
	c.w.mu.Lock()
	c.w.topics++
	c.w.mu.Unlock()
	return c.Topics.UpsertTopic(ctx, r, t, d)
}

type countingTemplates struct {
	*notifytest.Templates
	w *writes
}

func (c countingTemplates) PutTemplate(ctx context.Context, r domain.Realm, t domain.Template) (domain.Template, error) {
	c.w.mu.Lock()
	c.w.templates++
	c.w.mu.Unlock()
	return c.Templates.PutTemplate(ctx, r, t)
}

type countingAddresses struct {
	*notifytest.AddressBook
	w *writes
}

func (c countingAddresses) PutAddresses(ctx context.Context, id domain.Identity, ch domain.ChannelKind, a []domain.Address, keys []string) error {
	c.w.mu.Lock()
	c.w.addresses++
	c.w.lastKeys = slices.Clone(keys)
	c.w.lastID = id
	c.w.mu.Unlock()
	return c.AddressBook.PutAddresses(ctx, id, ch, a, keys)
}

type catalogFx struct {
	cat       *app.Catalog
	topics    *notifytest.Topics
	templates *notifytest.Templates
	book      *notifytest.AddressBook
	w         *writes
}

func newCatalogFx(t *testing.T) *catalogFx {
	t.Helper()
	probe := notifytest.NewProbe()
	email := notifytest.NewNormalizing(
		notifytest.NewChannel("email", domain.ChannelCapabilities{NeedsSubject: true, NeedsAddress: true, Dedup: domain.DedupProvider}, probe),
		func(v string) string {
			v = strings.TrimSpace(v)
			if at := strings.LastIndexByte(v, '@'); at >= 0 {
				return v[:at+1] + strings.ToLower(v[at+1:])
			}
			return v
		})
	channels := notifytest.NewRegistry(
		notifytest.NewChannel("in_app", domain.ChannelCapabilities{InboxBacked: true, Dedup: domain.DedupLocal}, probe),
		email,
		notifytest.NewChannel("sms", domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupNone}, probe))
	fx := &catalogFx{topics: notifytest.NewTopics(), templates: notifytest.NewTemplates("en"), book: notifytest.NewAddressBook(), w: &writes{}}
	cat, err := app.NewCatalog(app.Deps{
		Channels:       channels,
		TopicWriter:    countingTopics{fx.topics, fx.w},
		TemplateWriter: countingTemplates{fx.templates, fx.w},
		AddressWriter:  countingAddresses{fx.book, fx.w},
	})
	if err != nil {
		t.Fatalf("NewCatalog: %v", err)
	}
	fx.cat = cat
	return fx
}

func TestCatalogUpsertTopicRegistersInTheRealmItIsGiven(t *testing.T) {
	t.Parallel()
	fx := newCatalogFx(t)
	def := domain.TopicDef{DefaultChannels: []domain.ChannelKind{"in_app", "email"}, Fallback: []domain.ChannelKind{"sms"}, DefaultPriority: domain.PriorityWarning, TemplateRef: "inv"}
	if err := fx.cat.UpsertTopic(t.Context(), catRealm, "invoice", def); err != nil {
		t.Fatalf("UpsertTopic: %v", err)
	}
	got, found, _ := fx.topics.Lookup(t.Context(), catRealm, "invoice")
	if !found || got.TemplateRef != "inv" || !slices.Equal(got.Fallback, []domain.ChannelKind{"sms"}) {
		t.Fatalf("Lookup = %+v, %v, want the registration", got, found)
	}
	if _, found, _ := fx.topics.Lookup(t.Context(), catRealm2, "invoice"); found {
		t.Fatal("the topic is in another realm too: a registration lands in the realm it is given and nowhere else (NR-009)")
	}
	// And the other way round: a call for the second realm lands in the second and not the first, so the
	// realm is the call's and not one the Catalog knows.
	if err := fx.cat.UpsertTopic(t.Context(), catRealm2, "only_beta", def); err != nil {
		t.Fatalf("UpsertTopic: %v", err)
	}
	if _, found, _ := fx.topics.Lookup(t.Context(), catRealm2, "only_beta"); !found {
		t.Fatal("a registration for realm beta is not in realm beta")
	}
	if _, found, _ := fx.topics.Lookup(t.Context(), catRealm, "only_beta"); found {
		t.Fatal("a registration for realm beta is in realm alpha")
	}
}

func TestCatalogRefusesBeforeItWrites(t *testing.T) {
	t.Parallel()
	validTopic := domain.TopicDef{DefaultChannels: []domain.ChannelKind{"in_app"}, DefaultPriority: domain.PriorityInfo}
	cases := []struct {
		name string
		run  func(*catalogFx) error
		is   error // a sentinel the error wraps; nil means a *ValidationError
		in   string
		kind string // which writer must not have been reached
	}{
		{"a topic on a channel nobody registered", func(fx *catalogFx) error {
			return fx.cat.UpsertTopic(t.Context(), catRealm, "t", domain.TopicDef{DefaultChannels: []domain.ChannelKind{"in_app", "fax"}, DefaultPriority: domain.PriorityInfo})
		}, domain.ErrUnknownChannel, "fax", "topic"},
		{"a topic whose fallback is a channel nobody registered", func(fx *catalogFx) error {
			return fx.cat.UpsertTopic(t.Context(), catRealm, "t", domain.TopicDef{DefaultChannels: []domain.ChannelKind{"in_app"}, Fallback: []domain.ChannelKind{"pager"}, DefaultPriority: domain.PriorityInfo})
		}, domain.ErrUnknownChannel, "pager", "topic"},
		{"a topic that does not validate", func(fx *catalogFx) error {
			return fx.cat.UpsertTopic(t.Context(), catRealm, "t", domain.TopicDef{DefaultPriority: "urgent"})
		}, nil, "DefaultPriority", "topic"},
		{"a topic in a realm that is no realm", func(fx *catalogFx) error {
			return fx.cat.UpsertTopic(t.Context(), "", "t", validTopic)
		}, nil, "Realm", "topic"},
		{"a template for a channel nobody registered", func(fx *catalogFx) error {
			_, err := fx.cat.PutTemplate(t.Context(), catRealm, domain.Template{Ref: "r", Channel: "fax", Locale: "en", Body: "x"})
			return err
		}, domain.ErrUnknownChannel, "fax", "template"},
		{"a template that calls a function outside the map", func(fx *catalogFx) error {
			_, err := fx.cat.PutTemplate(t.Context(), catRealm, domain.Template{Ref: "r", Channel: "email", Locale: "en", Body: `{{ env "HOME" }}`})
			return err
		}, domain.ErrInvalidTemplate, "env", "template"},
		{"addresses on a channel nobody registered", func(fx *catalogFx) error {
			_, err := fx.cat.PutAddresses(t.Context(), catID, "fax", []domain.Address{{Value: "1"}})
			return err
		}, domain.ErrUnknownChannel, "fax", "addresses"},
		{"addresses of an identity with an empty component", func(fx *catalogFx) error {
			id := catID
			id.Recipient = domain.Recipient{}
			_, err := fx.cat.PutAddresses(t.Context(), id, "email", []domain.Address{{Value: "a@x.test"}})
			return err
		}, domain.ErrEmptyIdentity, "", "addresses"},
		{"addresses the engine cannot use", func(fx *catalogFx) error {
			_, err := fx.cat.PutAddresses(t.Context(), catID, "email", []domain.Address{{Value: "a@x.test", Timezone: "Mars/Olympus"}})
			return err
		}, nil, "Timezone", "addresses"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fx := newCatalogFx(t)
			err := tc.run(fx)
			if err == nil {
				t.Fatal("got nil, want a refusal")
			}
			if tc.is != nil && !errors.Is(err, tc.is) {
				t.Fatalf("err = %v, want it to wrap %v", err, tc.is)
			}
			var v *domain.ValidationError
			if tc.is == nil && !errors.As(err, &v) {
				t.Fatalf("err = %v, want a *domain.ValidationError", err)
			}
			if !strings.Contains(err.Error(), tc.in) {
				t.Fatalf("err = %q, want it to name %q", err, tc.in)
			}
			fx.w.mu.Lock()
			defer fx.w.mu.Unlock()
			reached := map[string]int{"topic": fx.w.topics, "template": fx.w.templates, "addresses": fx.w.addresses}[tc.kind]
			if reached != 0 {
				t.Fatalf("the %s writer was reached %d times by a refused call: a refusal comes before the write", tc.kind, reached)
			}
		})
	}
}

var catID = domain.Identity{Realm: catRealm, Tenant: domain.Tenant{Kind: "workspace", ID: "w1"}, Recipient: domain.Recipient{Kind: "user", ID: "u1"}}

func TestCatalogTemplatesAreVersionedAndActivatedExplicitly(t *testing.T) {
	t.Parallel()
	fx := newCatalogFx(t)
	stored, err := fx.cat.PutTemplate(t.Context(), catRealm, domain.Template{Ref: "invoice", Channel: "email", Locale: "en", Subject: "Hi {{ .name }}", Body: "Body"})
	if err != nil || stored.Version != 1 {
		t.Fatalf("PutTemplate = %+v, %v, want version 1", stored, err)
	}
	render := func(realm domain.Realm) error {
		_, err := fx.templates.Render(t.Context(), domain.RenderRequest{Realm: realm, TemplateRef: "invoice", Channel: "email", Locale: "en", Data: map[string]any{"name": "Ada"}})
		return err
	}
	if err := render(catRealm); !errors.Is(err, domain.ErrNoTemplate) {
		t.Fatalf("Render before activation = %v, want ErrNoTemplate", err)
	}
	active, err := fx.cat.ActivateTemplate(t.Context(), catRealm, domain.TemplateVersion{Ref: "invoice", Channel: "email", Locale: "en", Version: 1})
	if err != nil || active.Version != 1 || active.Subject != "Hi {{ .name }}" {
		t.Fatalf("ActivateTemplate = %+v, %v, want the version activated", active, err)
	}
	if err := render(catRealm); err != nil {
		t.Fatalf("Render after activation = %v", err)
	}
	if err := render(catRealm2); !errors.Is(err, domain.ErrNoTemplate) {
		t.Fatalf("Render in another realm = %v, want ErrNoTemplate: a template is stored under the realm it is given", err)
	}
	if _, err := fx.cat.ActivateTemplate(t.Context(), catRealm, domain.TemplateVersion{Ref: "invoice", Channel: "email", Locale: "en", Version: 9}); !errors.Is(err, domain.ErrUnknownTemplate) {
		t.Fatalf("ActivateTemplate of a version nobody stored = %v, want ErrUnknownTemplate", err)
	}
}

func TestCatalogPutAddressesKeysThemByTheChannelsNormalization(t *testing.T) {
	t.Parallel()
	fx := newCatalogFx(t)
	got, err := fx.cat.PutAddresses(t.Context(), catID, "email", []domain.Address{
		{Value: "Ada@EXAMPLE.test", Locale: "en"}, {Value: "Ada@example.TEST"}, {Value: "ada@work.test"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Value != "Ada@EXAMPLE.test" || got[0].Channel != "email" || got[1].Value != "ada@work.test" {
		t.Fatalf("PutAddresses = %+v, want the two distinct mailboxes, the first spelling kept", got)
	}
	want := []string{domain.AddressKey("email", "Ada@example.test"), domain.AddressKey("email", "ada@work.test")}
	fx.w.mu.Lock()
	defer fx.w.mu.Unlock()
	if !slices.Equal(fx.w.lastKeys, want) {
		t.Fatalf("keys written = %v, want the keys of what the email channel calls equal: %v", fx.w.lastKeys, want)
	}
	if fx.w.lastID != catID {
		t.Fatalf("written for %+v, want the identity it was asked for, realm included: %+v", fx.w.lastID, catID)
	}
	resolved, _ := fx.book.Resolve(t.Context(), catID, "email")
	if len(resolved) != 2 {
		t.Fatalf("the book resolves %+v, want the two addresses stored", resolved)
	}
	other := catID
	other.Realm = catRealm2
	if again, _ := fx.book.Resolve(t.Context(), other, "email"); len(again) != 0 {
		t.Fatalf("another realm resolves %+v, want none", again)
	}
}

func TestCatalogPutAddressesWithNoAddressesClearsTheChannel(t *testing.T) {
	t.Parallel()
	fx := newCatalogFx(t)
	if _, err := fx.cat.PutAddresses(t.Context(), catID, "sms", []domain.Address{{Value: "+15550100"}}); err != nil {
		t.Fatal(err)
	}
	got, err := fx.cat.PutAddresses(t.Context(), catID, "sms", nil)
	if err != nil || len(got) != 0 {
		t.Fatalf("PutAddresses(nil) = %v, %v, want an empty set", got, err)
	}
	if left, _ := fx.book.Resolve(t.Context(), catID, "sms"); len(left) != 0 {
		t.Fatalf("addresses left = %v, want none", left)
	}
}

func TestNewCatalogNamesEveryPortItNeeds(t *testing.T) {
	t.Parallel()
	_, err := app.NewCatalog(app.Deps{})
	if err == nil {
		t.Fatal("NewCatalog with no ports = nil, want an error")
	}
	for _, port := range []string{"Channels", "TopicWriter", "TemplateWriter", "AddressWriter"} {
		if !strings.Contains(err.Error(), "Deps."+port) {
			t.Errorf("error %q does not name Deps.%s", err, port)
		}
	}
}

var _ ports.Catalog = (*app.Catalog)(nil)
