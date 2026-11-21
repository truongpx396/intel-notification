package providers_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/truongpx396/intel-notification/adapters/driven/channel/providers"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/notifytest"
	"github.com/truongpx396/intel-notification/ports"
)

// T049, D31, D29. Channels are built from rows: each realm's providers in their priority order, a tenant's
// own over the realm's, and a credential that cannot be resolved costs that provider and nothing else.

type fakeStore struct {
	mu    sync.Mutex
	rows  []domain.ChannelProvider
	err   error
	reads int
	read  chan struct{} // signalled after every read, when set
}

func (f *fakeStore) Providers(context.Context) ([]domain.ChannelProvider, error) {
	f.mu.Lock()
	f.reads++
	rows, err, read := slices.Clone(f.rows), f.err, f.read
	f.mu.Unlock()
	if read != nil {
		select {
		case read <- struct{}{}:
		default:
		}
	}
	return rows, err
}

func (f *fakeStore) set(rows []domain.ChannelProvider, err error) {
	f.mu.Lock()
	f.rows, f.err = rows, err
	f.mu.Unlock()
}

type fakeSecrets map[string]string

func (f fakeSecrets) Resolve(_ context.Context, ref string) (string, error) {
	if v, ok := f[ref]; ok {
		return v, nil
	}
	return "", fmt.Errorf("secret %s: the environment variable is not set", ref)
}

// world is the fixture: rows, the credentials that resolve, and a factory per provider kind that records
// what it built.
type world struct {
	store   *fakeStore
	secrets fakeSecrets
	mu      sync.Mutex
	built   map[string]*notifytest.Scripted // by realm/tenant/provider
	specs   []providers.Spec
	mgr     *providers.Manager
}

func scope(realm domain.Realm, tenant domain.Tenant, provider string) string {
	return fmt.Sprintf("%s/%s:%s/%s", realm, tenant.Kind, tenant.ID, provider)
}

var emailCaps = domain.ChannelCapabilities{NeedsSubject: true, NeedsAddress: true, Dedup: domain.DedupProvider, DedupWindow: 24 * time.Hour}

func newWorld(t *testing.T, rows ...domain.ChannelProvider) *world {
	t.Helper()
	w := &world{store: &fakeStore{rows: rows}, secrets: fakeSecrets{}, built: map[string]*notifytest.Scripted{}}
	build := func(caps domain.ChannelCapabilities) providers.Factory {
		return func(s providers.Spec) (ports.Channel, error) {
			ch := notifytest.NewScripted(s.Channel, caps)
			w.mu.Lock()
			w.built[scope(s.Realm, s.Tenant, s.Provider)] = ch
			w.specs = append(w.specs, s)
			w.mu.Unlock()
			return ch, nil
		}
	}
	w.mgr = providers.NewManager(w.store, w.secrets, map[string]providers.Factory{
		"alpha-mail": build(emailCaps),
		"beta-mail":  build(domain.ChannelCapabilities{NeedsSubject: true, NeedsAddress: true, Dedup: domain.DedupNone}),
		"hook":       build(domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupNone}),
		"leaky": func(s providers.Spec) (ports.Channel, error) {
			return nil, fmt.Errorf("could not start with key %s", s.Secret)
		},
		"wrong-kind": func(s providers.Spec) (ports.Channel, error) {
			return notifytest.NewScripted("sms", emailCaps), nil
		},
	})
	return w
}

func row(realm domain.Realm, tenant domain.Tenant, channel domain.ChannelKind, provider string, priority int, ref string) domain.ChannelProvider {
	return domain.ChannelProvider{Realm: realm, Tenant: tenant, Channel: channel, Provider: provider, Priority: priority, SecretRef: ref,
		Config: map[string]any{"from": "n@x.test"}}
}

var (
	t1 = domain.Tenant{Kind: "workspace", ID: "t1"}
	t2 = domain.Tenant{Kind: "workspace", ID: "t2"}
)

func delivery(realm domain.Realm, tenant domain.Tenant) domain.Delivery {
	return domain.Delivery{
		Identity: domain.Identity{Realm: realm, Tenant: tenant, Recipient: domain.Recipient{Kind: "user", ID: "u1"}},
		Channel:  "email", Address: domain.Address{Channel: "email", Value: "a@x.test"}, IdemKey: "k",
	}
}

func (w *world) deliver(t *testing.T, d domain.Delivery) domain.DeliveryResult {
	t.Helper()
	ch, ok := w.mgr.Registry().Get(d.Channel)
	if !ok {
		t.Fatalf("no channel %q is registered", d.Channel)
	}
	res, err := ch.Deliver(t.Context(), d)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	return res
}

func (w *world) reload(t *testing.T) {
	t.Helper()
	if err := w.mgr.Reload(t.Context()); err != nil {
		t.Fatalf("Reload: %v", err)
	}
}

func (w *world) calls(realm domain.Realm, tenant domain.Tenant, provider string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	if ch := w.built[scope(realm, tenant, provider)]; ch != nil {
		return len(ch.Deliveries())
	}
	return -1 // never built
}

func TestAChannelTriesItsScopesProvidersInPriorityOrder(t *testing.T) {
	t.Parallel()
	w := newWorld(t,
		row("alpha", domain.Tenant{}, "email", "beta-mail", 20, ""),
		row("alpha", domain.Tenant{}, "email", "alpha-mail", 10, ""))
	w.reload(t)
	w.mu.Lock()
	w.built[scope("alpha", domain.Tenant{}, "alpha-mail")].Then(domain.DeliveryResult{Outcome: domain.Retry, Detail: "provider down"}, nil)
	w.mu.Unlock()
	res := w.deliver(t, delivery("alpha", t1))
	if res.Outcome != domain.Delivered {
		t.Fatalf("Deliver = %+v, want it delivered by the second provider after the first failed transiently (D29)", res)
	}
	if first, second := w.calls("alpha", domain.Tenant{}, "alpha-mail"), w.calls("alpha", domain.Tenant{}, "beta-mail"); first != 1 || second != 1 {
		t.Fatalf("the providers were called %d and %d times, want the priority-10 one first and then the other, once each", first, second)
	}
}

func TestATenantsOwnProvidersReplaceTheRealmsForThatTenantOnly(t *testing.T) {
	t.Parallel()
	w := newWorld(t,
		row("alpha", domain.Tenant{}, "email", "alpha-mail", 0, ""),
		row("alpha", t1, "email", "beta-mail", 0, ""))
	w.reload(t)
	for _, tenant := range []domain.Tenant{t1, t2, t1, t2, t2} {
		w.deliver(t, delivery("alpha", tenant))
	}
	if own, shared := w.calls("alpha", t1, "beta-mail"), w.calls("alpha", domain.Tenant{}, "alpha-mail"); own != 2 || shared != 3 {
		t.Fatalf("the tenant's provider sent %d and the realm's %d, want 2 and 3: a tenant that brings its own provider uses it alone, and another tenant never does", own, shared)
	}
}

// NR-009, D31. A realm with no provider of its own is not served by another realm's.
func TestARealmIsNeverServedByAnotherRealmsProvider(t *testing.T) {
	t.Parallel()
	w := newWorld(t, row("alpha", domain.Tenant{}, "email", "alpha-mail", 0, ""))
	w.reload(t)
	res := w.deliver(t, delivery("gamma", t1))
	if res.Outcome != domain.Retry || !strings.Contains(res.Detail, "gamma") || !strings.Contains(res.Detail, "email") {
		t.Fatalf("Deliver for a realm with no provider = %+v, want Retry naming the realm and channel: an operator can fix the rows, and a retry will find them", res)
	}
	if n := w.calls("alpha", domain.Tenant{}, "alpha-mail"); n != 0 {
		t.Fatalf("realm alpha's provider was called %d times for realm gamma's delivery, want none: its key is alpha's", n)
	}
}

// The task's named case: an unresolvable secret_ref marks the provider unavailable and does not crash the
// service.
func TestAnUnresolvableSecretMarksTheProviderUnavailableAndTheRestCarryOn(t *testing.T) {
	t.Parallel()
	w := newWorld(t,
		row("alpha", domain.Tenant{}, "email", "alpha-mail", 10, "env:MISSING_KEY"),
		row("alpha", domain.Tenant{}, "email", "beta-mail", 20, "env:GOOD_KEY"))
	w.secrets["env:GOOD_KEY"] = "good-credential"
	if err := w.mgr.Reload(t.Context()); err != nil {
		t.Fatalf("Reload = %v, want nil: a provider that cannot be built is reported, and does not stop the service", err)
	}
	var missing, good *providers.Status
	for _, s := range w.mgr.Report() {
		switch s.Provider {
		case "alpha-mail":
			missing = &s
		case "beta-mail":
			good = &s
		}
	}
	if missing == nil || missing.Available || missing.SecretRef != "env:MISSING_KEY" || !strings.Contains(missing.Reason, "env:MISSING_KEY") {
		t.Fatalf("report of the provider with no credential = %+v, want it unavailable, with the reference it was told to use and why not", missing)
	}
	if good == nil || !good.Available || good.Reason != "" {
		t.Fatalf("report of the other provider = %+v, want it available", good)
	}
	if res := w.deliver(t, delivery("alpha", t1)); res.Outcome != domain.Delivered {
		t.Fatalf("Deliver = %+v, want the provider that has its credential to carry the delivery", res)
	}
	if n := w.calls("alpha", domain.Tenant{}, "alpha-mail"); n != -1 {
		t.Fatal("the provider with no credential was built: it must not be used")
	}
}

func TestAKindWithNoUsableProviderIsNotRegistered(t *testing.T) {
	t.Parallel()
	w := newWorld(t,
		row("alpha", domain.Tenant{}, "email", "alpha-mail", 0, "env:MISSING"),
		row("alpha", domain.Tenant{}, "webhook", "hook", 0, "env:HOOK"))
	w.secrets["env:HOOK"] = "s3cret"
	w.reload(t)
	reg := w.mgr.Registry()
	if _, ok := reg.Get("email"); ok {
		t.Fatal("email is registered with no usable provider: a topic naming it would be accepted and could never be delivered")
	}
	if _, ok := reg.Get("webhook"); !ok {
		t.Fatal("webhook is not registered, want it: its provider has its credential")
	}
	if got := reg.Kinds(); !slices.Equal(got, []domain.ChannelKind{"webhook"}) {
		t.Fatalf("Kinds() = %v, want only the channels that can deliver", got)
	}
}

// D31, security.md. The credential reaches the factory and nothing else: not the report, not the error of
// a reload, not what a delivery says.
func TestACredentialIsHandedToTheFactoryAndAppearsNowhereElse(t *testing.T) {
	t.Parallel()
	const credential = "re_live_TheResolvedSecret999"
	w := newWorld(t,
		row("alpha", domain.Tenant{}, "email", "alpha-mail", 10, "env:KEY"),
		row("alpha", domain.Tenant{}, "email", "leaky", 20, "env:KEY"))
	w.secrets["env:KEY"] = credential
	if err := w.mgr.Reload(t.Context()); err != nil {
		t.Fatalf("Reload = %v", err)
	}
	w.mu.Lock()
	reached := slices.ContainsFunc(w.specs, func(s providers.Spec) bool { return s.Secret == credential })
	w.mu.Unlock()
	if !reached {
		t.Fatal("the factory was not given the resolved credential, so no provider could use it")
	}
	var seen []string
	for _, s := range w.mgr.Report() {
		seen = append(seen, fmt.Sprintf("%+v", s), s.Reason)
	}
	// The leaky factory put the key into its own error message; the manager must not pass that on.
	leaky := ""
	for _, s := range w.mgr.Report() {
		if s.Provider == "leaky" {
			leaky = s.Reason
			if s.Available || s.SecretRef != "env:KEY" {
				t.Fatalf("report of the leaky provider = %+v, want it unavailable and naming the reference", s)
			}
		}
	}
	if leaky == "" {
		t.Fatal("the provider whose factory failed has no reason in the report")
	}
	res := w.deliver(t, delivery("alpha", t1))
	seen = append(seen, res.Detail)
	for _, text := range seen {
		if strings.Contains(text, credential) {
			t.Fatalf("%q carries the credential", text)
		}
	}
	// And the same for the delivery of a realm nothing serves.
	if res := w.deliver(t, delivery("gamma", t1)); strings.Contains(res.Detail, credential) {
		t.Fatalf("a Retry detail %q carries the credential", res.Detail)
	}
}

func TestAProviderNobodyKnowsOrOfTheWrongKindIsUnavailable(t *testing.T) {
	t.Parallel()
	w := newWorld(t,
		row("alpha", domain.Tenant{}, "email", "no-such-provider", 0, ""),
		row("alpha", domain.Tenant{}, "email", "wrong-kind", 1, ""),
		row("alpha", domain.Tenant{}, "email", "alpha-mail", 2, ""))
	w.reload(t)
	reasons := map[string]providers.Status{}
	for _, s := range w.mgr.Report() {
		reasons[s.Provider] = s
	}
	if s := reasons["no-such-provider"]; s.Available || s.Reason == "" {
		t.Errorf("a provider kind nobody built: %+v, want it unavailable with a reason", s)
	}
	if s := reasons["wrong-kind"]; s.Available || !strings.Contains(s.Reason, "sms") {
		t.Errorf("a provider that builds another channel's kind: %+v, want it unavailable, naming the kind it built", s)
	}
	if s := reasons["alpha-mail"]; !s.Available {
		t.Errorf("the good provider: %+v, want it available", s)
	}
}

func TestReloadSwapsTheWholeConfigurationAndAFailedReadKeepsTheOld(t *testing.T) {
	t.Parallel()
	w := newWorld(t, row("alpha", domain.Tenant{}, "email", "alpha-mail", 0, ""))
	w.reload(t)
	if _, ok := w.mgr.Registry().Get("email"); !ok {
		t.Fatal("email is not registered after the first load")
	}
	w.store.set(nil, errors.New("connection refused"))
	if err := w.mgr.Reload(t.Context()); err == nil {
		t.Fatal("Reload with a failing store = nil, want the error")
	}
	if _, ok := w.mgr.Registry().Get("email"); !ok {
		t.Fatal("a failed read emptied the registry: the service would stop delivering because a database blinked")
	}
	w.store.set([]domain.ChannelProvider{row("alpha", domain.Tenant{}, "webhook", "hook", 0, "")}, nil)
	w.reload(t)
	reg := w.mgr.Registry()
	if _, ok := reg.Get("email"); ok {
		t.Error("email is still registered after its provider was removed")
	}
	if _, ok := reg.Get("webhook"); !ok {
		t.Error("webhook is not registered after its provider was added")
	}
}

func TestAChannelThatIsNotAProviderSurvivesEveryReload(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	w.mgr.Register(notifytest.NewScripted("in_app", domain.ChannelCapabilities{InboxBacked: true, Dedup: domain.DedupLocal}))
	w.reload(t)
	w.store.set([]domain.ChannelProvider{row("alpha", domain.Tenant{}, "email", "alpha-mail", 0, "")}, nil)
	w.reload(t)
	w.store.set(nil, nil)
	w.reload(t)
	if _, ok := w.mgr.Registry().Get("in_app"); !ok {
		t.Fatal("the in-app channel is gone: it has no provider row, and no reload may remove it")
	}
}

func TestARoutersCapabilitiesAreTheWeakestOfItsProviders(t *testing.T) {
	t.Parallel()
	w := newWorld(t,
		row("alpha", domain.Tenant{}, "email", "alpha-mail", 0, ""),
		row("beta", domain.Tenant{}, "email", "beta-mail", 0, ""))
	w.reload(t)
	ch, _ := w.mgr.Registry().Get("email")
	caps := ch.Capabilities()
	if caps.Dedup != domain.DedupNone || caps.DedupWindow != 0 || !caps.NeedsSubject || !caps.NeedsAddress {
		t.Fatalf("Capabilities = %+v, want the weakest dedup across every realm's provider (D32) and what any of them needs: the channel is one per kind, and cannot promise more than the weakest it may route to", caps)
	}
}

func TestReloadWhileDeliveringIsRaceFree(t *testing.T) {
	t.Parallel()
	w := newWorld(t, row("alpha", domain.Tenant{}, "email", "alpha-mail", 0, ""))
	w.reload(t)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				if ch, ok := w.mgr.Registry().Get("email"); ok {
					_, _ = ch.Deliver(t.Context(), delivery("alpha", t1))
				}
				_ = w.mgr.Report()
			}
		}()
	}
	for range 50 {
		_ = w.mgr.Reload(t.Context())
	}
	wg.Wait()
}

func TestRunReloadsOnEveryTickAndReportsAFailure(t *testing.T) {
	t.Parallel()
	w := newWorld(t, row("alpha", domain.Tenant{}, "email", "alpha-mail", 0, ""))
	w.store.read = make(chan struct{}, 8)
	ticks := make(chan time.Time)
	errs := make(chan error, 4)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.mgr.Run(ctx, ticks, func(err error) { errs <- err })
	}()
	waitRead := func() {
		t.Helper()
		select {
		case <-w.store.read:
		case <-time.After(5 * time.Second):
			t.Fatal("the store was not read after a tick")
		}
	}
	tick := func() {
		t.Helper()
		select {
		case ticks <- time.Now():
		case <-time.After(5 * time.Second):
			t.Fatal("Run is not receiving ticks")
		}
	}
	tick()
	waitRead()
	w.store.set(nil, errors.New("connection refused"))
	tick()
	waitRead()
	select {
	case err := <-errs:
		if !strings.Contains(err.Error(), "connection refused") {
			t.Fatalf("reported %v, want the store's error", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a failed reload was not reported")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return when its context ended")
	}
}

// A constructor that refuses a wiring mistake by panicking (email.New does) is a provider that cannot be
// built, and not a service that is down.
func TestAFactoryThatPanicsMakesItsProviderUnavailableAndNothingElse(t *testing.T) {
	t.Parallel()
	store := &fakeStore{rows: []domain.ChannelProvider{
		row("alpha", domain.Tenant{}, "email", "panics", 0, ""),
		row("alpha", domain.Tenant{}, "email", "fine", 1, ""),
	}}
	mgr := providers.NewManager(store, fakeSecrets{}, map[string]providers.Factory{
		"panics": func(providers.Spec) (ports.Channel, error) { panic("email: New needs a Mailer") },
		"fine": func(s providers.Spec) (ports.Channel, error) {
			return notifytest.NewScripted(s.Channel, emailCaps), nil
		},
	})
	if err := mgr.Reload(t.Context()); err != nil {
		t.Fatalf("Reload = %v, want nil: a panicking constructor is one provider's problem", err)
	}
	statuses := map[string]providers.Status{}
	for _, s := range mgr.Report() {
		statuses[s.Provider] = s
	}
	if s := statuses["panics"]; s.Available || !strings.Contains(s.Reason, "Mailer") {
		t.Fatalf("report of the panicking provider = %+v, want it unavailable with what it said", s)
	}
	if s := statuses["fine"]; !s.Available {
		t.Fatalf("report of the other provider = %+v, want it available", s)
	}
}
