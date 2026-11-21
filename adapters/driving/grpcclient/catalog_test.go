package grpcclient_test

import (
	"context"
	"errors"
	"testing"

	"github.com/truongpx396/intel-notification/adapters/driving/grpcclient"
	"github.com/truongpx396/intel-notification/adapters/driving/grpcserver"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/notifytest"
)

// T049, D31. A Go producer's client of the Catalog reads as the library does: the same values go in, the
// same sentinels come out, and no method takes a realm.

func catalogClient(t *testing.T) (*grpcclient.Catalog, *notifytest.ScriptedCatalog, *notifytest.ScriptedAdmin) {
	t.Helper()
	cat, admin := &notifytest.ScriptedCatalog{}, &notifytest.ScriptedAdmin{}
	srv := grpcserver.NewCatalog(cat, admin, func(context.Context) (domain.Realm, error) { return "alpha", nil })
	return grpcclient.NewCatalog(notifytest.NewBufconn(t, srv.Register)), cat, admin
}

var (
	tenant    = domain.Tenant{Kind: "workspace", ID: "w1"}
	recipient = domain.Recipient{Kind: "user", ID: "u1"}
)

func TestCatalogClientRoundTripsATopic(t *testing.T) {
	t.Parallel()
	c, cat, _ := catalogClient(t)
	def := domain.TopicDef{DefaultChannels: []domain.ChannelKind{"in_app", "email"}, Fallback: []domain.ChannelKind{"sms"}, DefaultPriority: domain.PriorityCritical, Essential: true, TemplateRef: "inv"}
	got, err := c.UpsertTopic(t.Context(), "invoice", def)
	if err != nil {
		t.Fatal(err)
	}
	if got.DefaultPriority != domain.PriorityCritical || !got.Essential || got.TemplateRef != "inv" || len(got.DefaultChannels) != 2 || got.Fallback[0] != "sms" {
		t.Fatalf("UpsertTopic = %+v, want what was stored", got)
	}
	if calls := cat.Calls(); len(calls) != 1 || calls[0].Realm != "alpha" || calls[0].Topic != "invoice" {
		t.Fatalf("the service was asked %+v", calls)
	}
}

func TestCatalogClientRoundTripsATemplateWithStructuredData(t *testing.T) {
	t.Parallel()
	c, cat, _ := catalogClient(t)
	cat.OnPutTemplate = func(_ context.Context, _ domain.Realm, tpl domain.Template) (domain.Template, error) {
		tpl.Version = 3
		return tpl, nil
	}
	in := domain.Template{
		Tenant: tenant, Ref: "inv", Channel: "slack", Locale: "en", Subject: "S", Body: "B {{ .x }}",
		Data: map[string]any{"blocks": []any{map[string]any{"text": "T {{ .x }}", "n": 2.0}}},
	}
	got, err := c.PutTemplate(t.Context(), in)
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != 3 || got.Tenant != tenant || got.Body != "B {{ .x }}" {
		t.Fatalf("PutTemplate = %+v, want it with the version the service assigned", got)
	}
	blocks, _ := got.Data["blocks"].([]any)
	if len(blocks) != 1 || blocks[0].(map[string]any)["text"] != "T {{ .x }}" {
		t.Fatalf("Data = %v, want the structured content back", got.Data)
	}
	active, err := c.ActivateTemplate(t.Context(), domain.TemplateVersion{Tenant: tenant, Ref: "inv", Channel: "slack", Locale: "en", Version: 3})
	if err != nil || active.Version != 3 || active.Tenant != tenant {
		t.Fatalf("ActivateTemplate = %+v, %v", active, err)
	}
}

func TestCatalogClientTemplateDataThatCannotCrossIsRefusedHere(t *testing.T) {
	t.Parallel()
	c, cat, _ := catalogClient(t)
	_, err := c.PutTemplate(t.Context(), domain.Template{Ref: "r", Channel: "email", Locale: "en", Body: "B", Data: map[string]any{"big": int64(1) << 60}})
	if err == nil {
		t.Fatal("PutTemplate of an integer beyond 2^53 = nil, want a refusal before the call: it would arrive as another number")
	}
	if len(cat.Calls()) != 0 {
		t.Fatal("a call was made, want none")
	}
}

func TestCatalogClientPutAddressesNamesNoRealm(t *testing.T) {
	t.Parallel()
	c, cat, _ := catalogClient(t)
	got, err := c.PutAddresses(t.Context(), tenant, recipient, "push", []domain.Address{{Value: "tok1", Meta: map[string]string{"p": "ios"}}, {Value: "tok2", Locale: "fr"}})
	if err != nil || len(got) != 2 || got[0].Channel != "push" || got[0].Meta["p"] != "ios" || got[1].Locale != "fr" {
		t.Fatalf("PutAddresses = %+v, %v, want the stored set", got, err)
	}
	if call := cat.Calls()[0]; call.ID.Realm != "alpha" || call.ID.Tenant != tenant || call.ID.Recipient != recipient {
		t.Fatalf("the service was asked %+v, want the recipient as sent and the realm the service's own", call)
	}
}

func TestCatalogClientSetsTenantPreferencesAndErases(t *testing.T) {
	t.Parallel()
	c, _, admin := catalogClient(t)
	if err := c.SetTenantPreference(t.Context(), domain.TenantPreference{Tenant: tenant, Topic: "invoice", Channel: "email", Enabled: false, Locked: true}); err != nil {
		t.Fatal(err)
	}
	removed, err := c.Erase(t.Context(), tenant, recipient)
	if err != nil || removed["notifications"] != 2 {
		t.Fatalf("Erase = %v, %v, want the rows removed per table", removed, err)
	}
	calls := admin.Calls()
	if len(calls) != 2 || !calls[0].Pref.Locked || calls[0].Pref.Enabled || calls[1].ID.Recipient != recipient {
		t.Fatalf("admin calls = %+v", calls)
	}
}

// A Go producer's errors.Is reads the same in service mode and in library mode (NR-025).
func TestCatalogClientTurnsRefusalsBackIntoTheLibrarysSentinels(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  error
		is   error
	}{
		{"a channel nobody registered", domain.ErrUnknownChannel, domain.ErrUnknownChannel},
		{"a template outside the map", domain.ErrInvalidTemplate, domain.ErrInvalidTemplate},
		{"a version nobody stored", domain.ErrUnknownTemplate, domain.ErrUnknownTemplate},
		{"a preference that is locked", domain.ErrPreferenceLocked, domain.ErrPreferenceLocked},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, cat, admin := catalogClient(t)
			cat.OnPutTemplate = func(context.Context, domain.Realm, domain.Template) (domain.Template, error) {
				return domain.Template{}, tc.err
			}
			admin.OnSetTenantPreference = func(context.Context, domain.Realm, domain.TenantPreference) error { return tc.err }
			if _, err := c.PutTemplate(t.Context(), domain.Template{Ref: "r", Channel: "email", Locale: "en", Body: "B"}); !errors.Is(err, tc.is) {
				t.Errorf("PutTemplate err = %v, want errors.Is %v", err, tc.is)
			}
			if err := c.SetTenantPreference(t.Context(), domain.TenantPreference{Tenant: tenant, Topic: "t", Channel: "email"}); !errors.Is(err, tc.is) {
				t.Errorf("SetTenantPreference err = %v, want errors.Is %v", err, tc.is)
			}
		})
	}
}
