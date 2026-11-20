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
	"context"
	"errors"
	"time"

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
type Manager struct{}

// NewManager returns a Manager that reads store, resolves credentials with secrets and builds each
// provider with the Factory registered under its name.
func NewManager(store ports.ProviderStore, secrets ports.SecretResolver, factories map[string]Factory) *Manager {
	return &Manager{}
}

var errNotImplemented = errors.New("not implemented")

// Registry is the ports.ChannelRegistry the dispatcher and the planner read. It follows Reload.
func (m *Manager) Registry() ports.ChannelRegistry { return emptyRegistry{} }

// Register adds a channel that is not a provider's, such as the in-app nudge. It survives every Reload.
func (m *Manager) Register(c ports.Channel) {}

// Reload reads the rows, resolves each credential, builds the channels and swaps them in together.
func (m *Manager) Reload(ctx context.Context) error { return errNotImplemented }

// Report lists every configured provider as the last Reload found it.
func (m *Manager) Report() []Status { return nil }

// Run reloads on every tick until ctx ends, reporting each failure to onError.
func (m *Manager) Run(ctx context.Context, ticks <-chan time.Time, onError func(error)) {}

type emptyRegistry struct{}

func (emptyRegistry) Register(ports.Channel)                       {}
func (emptyRegistry) Get(domain.ChannelKind) (ports.Channel, bool) { return nil, false }
func (emptyRegistry) Kinds() []domain.ChannelKind                  { return nil }
