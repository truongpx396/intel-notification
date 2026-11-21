package postgres

import (
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/truongpx396/intel-notification/ports"
)

// All is every PostgreSQL-backed port over one pool, so the common wiring is one call and a handful of
// field reads (docs/integration-guide.md). Each field is its own port, and a host that implements one
// itself, its own AddressBook say, leaves that field unused.
type All struct {
	Store        ports.Store
	Queue        ports.Queue
	Digests      ports.Digests
	Maintenance  ports.Maintenance
	Poison       ports.PoisonSink // the dead letters the bus ingest parks a message as
	Inbox        ports.InboxStore
	Prefs        ports.PreferenceStore
	Suppressions ports.SuppressionStore
	Quotas       *Quotas // ports.QuotaPolicies and ports.QuotaLimits, over channel_quotas
	Topics       *Topics // ports.TopicRegistry and ports.TopicWriter
	Templates    *Templates
	Addresses    *Addresses
	Providers    *Providers // ports.ProviderStore, over channel_providers
}

// NewAll returns every port of this package over pool, each configured with opts. The pool's role
// should own the tables and must not be a superuser or BYPASSRLS (see Store).
func NewAll(pool *pgxpool.Pool, opts ...Option) *All {
	store := New(pool, opts...)
	return &All{
		Store: store, Queue: store, Digests: store, Maintenance: store, Poison: store,
		Inbox:        NewInbox(pool, opts...),
		Prefs:        NewPreferences(pool, opts...),
		Suppressions: NewSuppressions(pool, opts...),
		Quotas:       NewQuotas(pool),
		Topics:       NewTopics(pool, opts...),
		Templates:    NewTemplates(pool, opts...),
		Addresses:    NewAddresses(pool, opts...),
		Providers:    NewProviders(pool, opts...),
	}
}
