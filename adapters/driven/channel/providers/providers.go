// Package providers builds the channels of service mode from the channel_providers rows (D31): which
// provider backs each channel for each realm, or for one tenant, and the reference to its credential. A
// host in library mode constructs its channels itself; a service has no host code, so its channels are
// data.
//
// Three rules hold throughout. A credential is resolved from its reference when the channel is built and
// is handed to the provider's constructor and to nothing else: no report, error or delivery result carries
// it. A provider whose credential cannot be resolved is marked unavailable, with the reference it was
// told to use and why not, and the service carries on with the rest. And a delivery is routed by the
// realm and tenant it belongs to, so one provider's key is never used for another realm's mail.
package providers

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/truongpx396/intel-notification/adapters/driven/channel/failover"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// Spec is what a Factory builds one provider's channel from. Secret is the resolved credential, and it
// reaches no one but the factory.
type Spec struct {
	Realm    domain.Realm
	Tenant   domain.Tenant
	Channel  domain.ChannelKind
	Provider string
	Config   map[string]any
	Secret   string
}

// Factory builds the channel of one provider kind. The channel's Kind must be the row's channel.
type Factory func(Spec) (ports.Channel, error)

// Status is what is known of one configured provider: where its credential is, whether it is in use, and
// if not, why. It never holds the credential.
type Status struct {
	Realm     domain.Realm
	Tenant    domain.Tenant
	Channel   domain.ChannelKind
	Provider  string
	SecretRef string
	Available bool
	Reason    string
}

// Manager builds the channel registry of a service from its provider rows and rebuilds it on Reload.
type Manager struct {
	store     ports.ProviderStore
	secrets   ports.SecretResolver
	factories map[string]Factory

	mu      sync.RWMutex
	fixed   map[domain.ChannelKind]ports.Channel // not a provider's: the in-app nudge
	routers map[domain.ChannelKind]*router
	report  []Status
}

// NewManager returns a Manager that reads store, resolves credentials with secrets and builds each
// provider with the Factory registered under its name. It builds nothing until Reload.
func NewManager(store ports.ProviderStore, secrets ports.SecretResolver, factories map[string]Factory) *Manager {
	return &Manager{
		store: store, secrets: secrets, factories: maps.Clone(factories),
		fixed: map[domain.ChannelKind]ports.Channel{}, routers: map[domain.ChannelKind]*router{},
	}
}

// Registry is the ports.ChannelRegistry the dispatcher and the planner read. It follows Reload.
func (m *Manager) Registry() ports.ChannelRegistry { return registry{m} }

// Register adds a channel that is not a provider's, such as the in-app nudge. It survives every Reload,
// and it is used in place of any provider rows for its kind.
func (m *Manager) Register(c ports.Channel) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fixed[c.Kind()] = c
}

type registry struct{ m *Manager }

func (r registry) Register(c ports.Channel) { r.m.Register(c) }

func (r registry) Get(kind domain.ChannelKind) (ports.Channel, bool) {
	r.m.mu.RLock()
	defer r.m.mu.RUnlock()
	if c, ok := r.m.fixed[kind]; ok {
		return c, true
	}
	if rt, ok := r.m.routers[kind]; ok {
		return rt, true
	}
	return nil, false
}

func (r registry) Kinds() []domain.ChannelKind {
	r.m.mu.RLock()
	defer r.m.mu.RUnlock()
	kinds := slices.Collect(maps.Keys(r.m.fixed))
	for kind := range r.m.routers {
		if _, fixed := r.m.fixed[kind]; !fixed {
			kinds = append(kinds, kind)
		}
	}
	slices.Sort(kinds)
	return kinds
}

// scopeKey is whose providers a chain holds: a realm's, or one tenant's of it.
type scopeKey struct {
	realm  domain.Realm
	tenant domain.Tenant
}

// router is the channel of one kind in a service: it holds the failover chain of every scope that has
// providers, and sends each delivery down the chain of the realm and tenant it belongs to. The registry
// has one channel per kind, so this is that channel, and what makes one provider's key never another
// realm's is that it routes by the delivery's own identity.
type router struct {
	kind   domain.ChannelKind
	scopes map[scopeKey]ports.Channel
	all    *failover.Channel // every provider, for the capabilities it must declare once for all scopes
}

var (
	_ ports.Channel           = (*router)(nil)
	_ ports.AddressNormalizer = (*router)(nil)
)

func (r *router) Kind() domain.ChannelKind { return r.kind }

// Capabilities is the weakest across every provider of every scope (D32): the registry holds one channel
// per kind, so it cannot promise more than the weakest delivery it may route.
func (r *router) Capabilities() domain.ChannelCapabilities { return r.all.Capabilities() }

func (r *router) NormalizeAddress(v string) string { return r.all.NormalizeAddress(v) }

// Deliver sends the delivery down its tenant's chain, or its realm's. A scope with no provider is a
// Retry that says so: an operator can add the row, and the next attempt finds it. Nothing is sent
// through another realm's provider.
func (r *router) Deliver(ctx context.Context, d domain.Delivery) (domain.DeliveryResult, error) {
	chain, ok := r.scopes[scopeKey{d.Identity.Realm, d.Identity.Tenant}]
	if !ok {
		chain, ok = r.scopes[scopeKey{d.Identity.Realm, domain.Tenant{}}]
	}
	if !ok {
		return domain.DeliveryResult{
			Outcome: domain.Retry,
			Detail:  fmt.Sprintf("providers: no provider is configured for channel %s in realm %s", r.kind, d.Identity.Realm),
		}, nil
	}
	return chain.Deliver(ctx, d)
}

// Reload reads the rows, resolves each credential, builds the channels and swaps them in together, so a
// delivery meets the old configuration or the new one and never a half of each. A row whose provider
// cannot be built is reported and left out; a read that fails leaves the configuration as it was and is
// returned, so a database that blinks does not stop a running service delivering.
func (m *Manager) Reload(ctx context.Context) error {
	rows, err := m.store.Providers(ctx)
	if err != nil {
		return fmt.Errorf("providers: read the provider rows: %w", err)
	}
	slices.SortStableFunc(rows, func(a, b domain.ChannelProvider) int {
		return cmp.Or(
			cmp.Compare(a.Realm, b.Realm), cmp.Compare(a.Tenant.Kind, b.Tenant.Kind), cmp.Compare(a.Tenant.ID, b.Tenant.ID),
			cmp.Compare(a.Channel, b.Channel), cmp.Compare(a.Priority, b.Priority), cmp.Compare(a.Provider, b.Provider))
	})
	type chain struct{ members []ports.Channel }
	var (
		report  = make([]Status, 0, len(rows))
		chains  = map[domain.ChannelKind]map[scopeKey]*chain{}
		members = map[domain.ChannelKind][]ports.Channel{}
	)
	for _, row := range rows {
		status := Status{Realm: row.Realm, Tenant: row.Tenant, Channel: row.Channel, Provider: row.Provider, SecretRef: row.SecretRef}
		ch, reason := m.build(ctx, row)
		if ch == nil {
			status.Reason = reason
			report = append(report, status)
			continue
		}
		status.Available = true
		report = append(report, status)
		if chains[row.Channel] == nil {
			chains[row.Channel] = map[scopeKey]*chain{}
		}
		key := scopeKey{row.Realm, row.Tenant}
		if chains[row.Channel][key] == nil {
			chains[row.Channel][key] = &chain{}
		}
		chains[row.Channel][key].members = append(chains[row.Channel][key].members, ch)
		members[row.Channel] = append(members[row.Channel], ch)
	}
	routers := make(map[domain.ChannelKind]*router, len(chains))
	for kind, byScope := range chains {
		rt := &router{kind: kind, scopes: make(map[scopeKey]ports.Channel, len(byScope)), all: failover.New(members[kind]...)}
		for key, c := range byScope {
			rt.scopes[key] = failover.New(c.members...)
		}
		routers[kind] = rt
	}
	m.mu.Lock()
	m.routers, m.report = routers, report
	m.mu.Unlock()
	return nil
}

// build makes one row's channel, or says why not. It never returns the credential, and a factory that
// panics (a constructor that refuses a wiring mistake by panicking) is a provider that is unavailable
// and not a service that is down.
func (m *Manager) build(ctx context.Context, row domain.ChannelProvider) (ports.Channel, string) {
	factory, ok := m.factories[row.Provider]
	if !ok {
		return nil, fmt.Sprintf("no provider named %q is built into this service", row.Provider)
	}
	secret := ""
	if row.SecretRef != "" {
		var err error
		if secret, err = m.secrets.Resolve(ctx, row.SecretRef); err != nil {
			return nil, fmt.Sprintf("the credential at %s could not be resolved: %s", row.SecretRef, redact(err.Error(), ""))
		}
	}
	spec := Spec{Realm: row.Realm, Tenant: row.Tenant, Channel: row.Channel, Provider: row.Provider, Config: maps.Clone(row.Config), Secret: secret}
	ch, err := construct(factory, spec)
	if err != nil {
		return nil, fmt.Sprintf("the provider could not be built: %s", redact(err.Error(), secret))
	}
	if ch.Kind() != row.Channel {
		return nil, fmt.Sprintf("the provider builds a channel of kind %q, but the row is for %q", ch.Kind(), row.Channel)
	}
	return ch, ""
}

// construct runs a factory and turns its panic into an error.
func construct(f Factory, s Spec) (ch ports.Channel, err error) {
	defer func() {
		if r := recover(); r != nil {
			ch, err = nil, fmt.Errorf("the constructor panicked: %v", r)
		}
	}()
	ch, err = f(s)
	if err == nil && ch == nil {
		err = errors.New("the factory returned no channel")
	}
	return ch, err
}

// redact removes the credential from a message that a provider's own constructor wrote: a factory that
// puts its key into its error must not pass it on to a report.
func redact(msg, secret string) string {
	if secret == "" {
		return msg
	}
	return strings.ReplaceAll(msg, secret, "[redacted]")
}

// Report lists every configured provider as the last Reload found it, in the order they are tried. It
// holds a reference to each credential and never the credential.
func (m *Manager) Report() []Status {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return slices.Clone(m.report)
}

// Run reloads on every tick until ctx ends, handing each failure to onError (nil: dropped). The
// configuration in force is untouched by a failure, so a service whose database blinks keeps delivering.
func (m *Manager) Run(ctx context.Context, ticks <-chan time.Time, onError func(error)) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticks:
			if err := m.Reload(ctx); err != nil && onError != nil {
				onError(err)
			}
		}
	}
}
