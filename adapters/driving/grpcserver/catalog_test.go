package grpcserver_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/truongpx396/intel-notification/adapters/driving/auth"
	"github.com/truongpx396/intel-notification/adapters/driving/grpcserver"
	"github.com/truongpx396/intel-notification/api/notifyv1"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/notifytest"
	"github.com/truongpx396/intel-notification/ports"
)

// T049, NR-009, D31. The Catalog service: what a producer in any language manages, and the one rule
// that holds across all of it: the realm is the caller's binding and nothing the request says.

func testPrincipal(ctx context.Context) (string, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if v := md.Get("x-test-principal"); len(v) > 0 {
		return v[0], nil
	}
	return "", auth.ErrNoCredentials
}

type catalogWorld struct {
	cat    *notifytest.ScriptedCatalog
	admin  *notifytest.ScriptedAdmin
	client notifyv1.CatalogClient
	faults *[]error
}

func newCatalogWorld(t *testing.T) *catalogWorld {
	t.Helper()
	b, err := domain.ParseRealmBindings("producer-a=alpha,producer-b=beta")
	if err != nil {
		t.Fatal(err)
	}
	unary, stream := auth.RealmInterceptors(b, testPrincipal)
	w := &catalogWorld{cat: &notifytest.ScriptedCatalog{}, admin: &notifytest.ScriptedAdmin{}, faults: new([]error)}
	srv := grpcserver.NewCatalog(w.cat, w.admin, auth.RealmOf, grpcserver.WithCatalogErrorHandler(func(e error) { *w.faults = append(*w.faults, e) }))
	w.client = notifyv1.NewCatalogClient(notifytest.NewBufconn(t, srv.Register, grpc.ChainUnaryInterceptor(unary), grpc.ChainStreamInterceptor(stream)))
	return w
}

// as is a call's context, authenticated as a principal.
func as(principal string) context.Context {
	return metadata.AppendToOutgoingContext(context.Background(), "x-test-principal", principal)
}

var (
	pbTenant    = &notifyv1.Tenant{Kind: "workspace", Id: "w1"}
	pbRecipient = &notifyv1.Recipient{Kind: "user", Id: "u1"}
)

// NR-009. The same request from two producers lands in two realms, and each is the realm its producer is
// bound to; a realm the caller tries to name changes nothing.
func TestEachCallActsInTheRealmOfItsProducer(t *testing.T) {
	t.Parallel()
	w := newCatalogWorld(t)
	req := &notifyv1.Topic{Topic: "invoice", DefaultChannels: []string{"in_app"}}
	for producer, want := range map[string]domain.Realm{"producer-a": "alpha", "producer-b": "beta"} {
		ctx := metadata.AppendToOutgoingContext(as(producer), "x-notify-realm", "beta", "realm", "beta")
		if _, err := w.client.UpsertTopic(ctx, req); err != nil {
			t.Fatalf("%s: %v", producer, err)
		}
		calls := w.cat.Calls()
		if got := calls[len(calls)-1].Realm; got != want {
			t.Errorf("%s registered in realm %q, want %q: the realm is the binding's, whatever the caller says", producer, got, want)
		}
	}
}

func TestACallFromAnUnboundOrUnauthenticatedCallerNeverReachesTheCatalog(t *testing.T) {
	t.Parallel()
	w := newCatalogWorld(t)
	req := &notifyv1.Topic{Topic: "invoice"}
	if _, err := w.client.UpsertTopic(as("mallory"), req); status.Code(err) != codes.PermissionDenied {
		t.Errorf("unbound: %v, want PermissionDenied", err)
	}
	if _, err := w.client.UpsertTopic(context.Background(), req); status.Code(err) != codes.Unauthenticated {
		t.Errorf("no credentials: %v, want Unauthenticated", err)
	}
	if _, err := w.client.Erase(as("mallory"), &notifyv1.EraseRequest{Tenant: pbTenant, Recipient: pbRecipient}); status.Code(err) != codes.PermissionDenied {
		t.Errorf("erase, unbound: %v, want PermissionDenied", err)
	}
	if n := len(w.cat.Calls()) + len(w.admin.Calls()); n != 0 {
		t.Fatalf("%d calls reached the engine, want none: a caller with no binding changes nothing", n)
	}
}

func TestUpsertTopicMapsTheRegistrationAndDefaultsThePriority(t *testing.T) {
	t.Parallel()
	w := newCatalogWorld(t)
	resp, err := w.client.UpsertTopic(as("producer-a"), &notifyv1.Topic{
		Topic: "invoice_overdue", DefaultChannels: []string{"in_app", "email"}, Fallback: []string{"sms"}, Essential: true, TemplateRef: "inv",
	})
	if err != nil {
		t.Fatal(err)
	}
	got := w.cat.Calls()[0]
	want := domain.TopicDef{
		DefaultChannels: []domain.ChannelKind{"in_app", "email"}, Fallback: []domain.ChannelKind{"sms"},
		DefaultPriority: domain.PriorityInfo, Essential: true, TemplateRef: "inv",
	}
	if got.Topic != "invoice_overdue" || !slices.Equal(got.Def.DefaultChannels, want.DefaultChannels) || !slices.Equal(got.Def.Fallback, want.Fallback) ||
		got.Def.DefaultPriority != want.DefaultPriority || !got.Def.Essential || got.Def.TemplateRef != "inv" {
		t.Fatalf("the Catalog was given %+v %+v, want %+v: a priority left empty is info, as the table's column is", got.Topic, got.Def, want)
	}
	if resp.GetTopic() != "invoice_overdue" || resp.GetDefaultPriority() != "info" || !resp.GetEssential() {
		t.Fatalf("response = %v, want what was stored", resp)
	}
}

func TestPutTemplateAssignsTheVersionAndTakesTheTenantOrNone(t *testing.T) {
	t.Parallel()
	w := newCatalogWorld(t)
	w.cat.OnPutTemplate = func(_ context.Context, _ domain.Realm, tpl domain.Template) (domain.Template, error) {
		tpl.Version = 4
		return tpl, nil
	}
	resp, err := w.client.PutTemplate(as("producer-a"), &notifyv1.Template{
		TemplateRef: "inv", Channel: "email", Locale: "fr-CA", Version: 99, Subject: "S {{ .a }}", Body: "B",
	})
	if err != nil {
		t.Fatal(err)
	}
	call := w.cat.Calls()[0]
	if call.Tpl.Tenant != (domain.Tenant{}) || call.Tpl.Version != 0 && call.Tpl.Version != 99 {
		t.Fatalf("Catalog was given %+v, want an unset tenant to be realm-wide", call.Tpl)
	}
	if call.Tpl.Ref != "inv" || call.Tpl.Channel != "email" || call.Tpl.Locale != "fr-CA" || call.Tpl.Subject != "S {{ .a }}" {
		t.Fatalf("Catalog was given %+v, want the template as sent", call.Tpl)
	}
	if resp.GetVersion() != 4 {
		t.Fatalf("response version = %d, want the service's assignment, 4", resp.GetVersion())
	}
	if _, err := w.client.PutTemplate(as("producer-a"), &notifyv1.Template{Tenant: pbTenant, TemplateRef: "inv", Channel: "email", Locale: "en", Body: "B"}); err != nil {
		t.Fatal(err)
	}
	if got := w.cat.Calls()[1].Tpl.Tenant; got != (domain.Tenant{Kind: "workspace", ID: "w1"}) {
		t.Fatalf("tenant = %+v, want the tenant's override", got)
	}
}

func TestActivateTemplateNamesTheVersionAndReturnsTheTemplate(t *testing.T) {
	t.Parallel()
	w := newCatalogWorld(t)
	resp, err := w.client.ActivateTemplate(as("producer-b"), &notifyv1.TemplateVersion{TemplateRef: "inv", Channel: "email", Locale: "en", Version: 2})
	if err != nil {
		t.Fatal(err)
	}
	call := w.cat.Calls()[0]
	if call.Realm != "beta" || call.Ver.Ref != "inv" || call.Ver.Version != 2 {
		t.Fatalf("Catalog was given %+v", call)
	}
	if resp.GetVersion() != 2 || resp.GetBody() != "stored" {
		t.Fatalf("response = %v, want the template activated", resp)
	}
}

// NR-009. The recipient and tenant are the producer's to name, because they are its data; the realm is
// the binding's.
func TestPutAddressesTakesTheIdentityFromTheRequestAndTheRealmFromTheBinding(t *testing.T) {
	t.Parallel()
	w := newCatalogWorld(t)
	resp, err := w.client.PutAddresses(as("producer-b"), &notifyv1.RecipientAddresses{
		Tenant: pbTenant, Recipient: pbRecipient, Channel: "email",
		Addresses: []*notifyv1.Address{{Value: "a@x.test", Locale: "fr", Timezone: "Europe/Paris", Meta: map[string]string{"k": "v"}}, {Value: "b@x.test"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	call := w.cat.Calls()[0]
	wantID := domain.Identity{Realm: "beta", Tenant: domain.Tenant{Kind: "workspace", ID: "w1"}, Recipient: domain.Recipient{Kind: "user", ID: "u1"}}
	if call.ID != wantID || call.Ch != "email" || len(call.Addrs) != 2 || call.Addrs[0].Locale != "fr" || call.Addrs[0].Meta["k"] != "v" {
		t.Fatalf("Catalog was given %+v, want %+v and both addresses", call, wantID)
	}
	if resp.GetChannel() != "email" || len(resp.GetAddresses()) != 2 || resp.GetRecipient().GetId() != "u1" {
		t.Fatalf("response = %v, want the stored set", resp)
	}
}

func TestSetTenantPreferenceAndEraseGoToTheAdminInTheCallersRealm(t *testing.T) {
	t.Parallel()
	w := newCatalogWorld(t)
	pref := &notifyv1.TenantPreference{Tenant: pbTenant, Topic: "invoice", Channel: "email", Enabled: true, Locked: true}
	if got, err := w.client.SetTenantPreference(as("producer-a"), pref); err != nil || !got.GetLocked() {
		t.Fatalf("SetTenantPreference = %v, %v, want it echoed", got, err)
	}
	erased, err := w.client.Erase(as("producer-b"), &notifyv1.EraseRequest{Tenant: pbTenant, Recipient: pbRecipient})
	if err != nil || erased.GetRemoved()["notifications"] != 2 {
		t.Fatalf("Erase = %v, %v, want the rows removed per table", erased, err)
	}
	calls := w.admin.Calls()
	if len(calls) != 2 || calls[0].Realm != "alpha" || calls[0].Pref.Topic != "invoice" || !calls[0].Pref.Locked {
		t.Fatalf("admin calls = %+v, want the preference set in alpha", calls)
	}
	if calls[1].ID.Realm != "beta" || calls[1].ID.Recipient.ID != "u1" {
		t.Fatalf("erase = %+v, want the recipient erased in the caller's realm, beta", calls[1])
	}
}

func TestACatalogRefusalCrossesWithItsCodeAndReason(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		err    error
		code   codes.Code
		reason string
	}{
		{"a channel nobody registered", domain.ErrUnknownChannel, codes.InvalidArgument, "UNKNOWN_CHANNEL"},
		{"a template outside the map", domain.ErrInvalidTemplate, codes.InvalidArgument, "INVALID_TEMPLATE"},
		{"a version nobody stored", domain.ErrUnknownTemplate, codes.NotFound, "UNKNOWN_TEMPLATE"},
		{"a preference that is locked", domain.ErrPreferenceLocked, codes.FailedPrecondition, "PREFERENCE_LOCKED"},
		{"a request that breaks a rule", &domain.ValidationError{Problems: []domain.Problem{{Field: "Ref", Message: "is required"}}}, codes.InvalidArgument, "INVALID_REQUEST"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			w := newCatalogWorld(t)
			w.cat.OnPutTemplate = func(context.Context, domain.Realm, domain.Template) (domain.Template, error) {
				return domain.Template{}, tc.err
			}
			_, err := w.client.PutTemplate(as("producer-a"), &notifyv1.Template{TemplateRef: "r", Channel: "email", Locale: "en", Body: "B"})
			if status.Code(err) != tc.code || reasonOf(err) != tc.reason {
				t.Fatalf("err = %v (reason %q), want %v %s", err, reasonOf(err), tc.code, tc.reason)
			}
		})
	}
}

func TestACatalogFaultIsReportedAndShownAsOne(t *testing.T) {
	t.Parallel()
	w := newCatalogWorld(t)
	w.cat.OnUpsertTopic = func(context.Context, domain.Realm, domain.Topic, domain.TopicDef) error {
		return errors.New(`ERROR: relation "notification_topics" does not exist (host=db.internal)`)
	}
	_, err := w.client.UpsertTopic(as("producer-a"), &notifyv1.Topic{Topic: "t"})
	if status.Code(err) != codes.Internal || status.Convert(err).Message() != "notify: internal error" {
		t.Fatalf("err = %v, want Internal and no SQL text", err)
	}
	if len(*w.faults) != 1 {
		t.Fatalf("%d faults reported, want the real error handed to the operator's handler", len(*w.faults))
	}
}

// The producer surface in service mode: one server, an engine per realm, the realm the caller's.
func TestARoutedNotifierSendsInTheRealmOfTheCaller(t *testing.T) {
	t.Parallel()
	b, _ := domain.ParseRealmBindings("producer-a=alpha,producer-b=beta")
	unary, stream := auth.RealmInterceptors(b, testPrincipal)
	engines := map[domain.Realm]*notifytest.ScriptedNotifier{"alpha": {}, "beta": {}}
	route := auth.Route(map[domain.Realm]ports.Notifier{"alpha": engines["alpha"], "beta": engines["beta"]})
	srv := grpcserver.NewRouted(route)
	client := notifyv1.NewNotifierClient(notifytest.NewBufconn(t, srv.Register, grpc.ChainUnaryInterceptor(unary), grpc.ChainStreamInterceptor(stream)))

	if _, err := client.Notify(as("producer-a"), note("k-a")); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Notify(as("producer-b"), note("k-b")); err != nil {
		t.Fatal(err)
	}
	if got := engines["alpha"].Notified(); len(got) != 1 || got[0].IdemKey != "k-a" {
		t.Errorf("realm alpha's engine was asked %+v, want only producer-a's", got)
	}
	if got := engines["beta"].Notified(); len(got) != 1 || got[0].IdemKey != "k-b" {
		t.Errorf("realm beta's engine was asked %+v, want only producer-b's", got)
	}
	if _, err := client.Notify(as("mallory"), note("k-m")); status.Code(err) != codes.PermissionDenied {
		t.Errorf("an unbound producer's Notify = %v, want PermissionDenied", err)
	}
	if n := len(engines["alpha"].Notified()) + len(engines["beta"].Notified()); n != 2 {
		t.Errorf("%d notifications reached an engine, want 2: an unbound producer sends nothing", n)
	}
}
