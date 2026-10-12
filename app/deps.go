// Package app holds the use-cases. They own the invariants: what a notification
// is planned to do, what a producer is told, what the dispatcher records. They
// depend on ports and domain and on the configuration, never on an adapter, and
// the dependency graph says so (.go-arch-lint.yml).
package app

import (
	"errors"

	"github.com/truongpx396/intel-notification/ports"
)

// Deps is everything the use-cases are built from: one field per port, the shape
// the contract fixes for New. Each constructor takes the whole struct and checks
// only the fields it uses, so a host that has built the notifier alone can say so
// and leave the dispatcher's ports empty.
type Deps struct {
	Channels      ports.ChannelRegistry
	Topics        ports.TopicRegistry
	Renderer      ports.TemplateRenderer
	Addresses     ports.AddressBook
	Audience      ports.AudienceResolver // nil: Broadcast accepts inline recipients only
	Store         ports.Store
	Inbox         ports.InboxStore
	Queue         ports.Queue
	Digests       ports.Digests
	Maintenance   ports.Maintenance
	Prefs         ports.PreferenceStore
	Suppressions  ports.SuppressionStore
	Quotas        ports.QuotaCounter
	QuotaPolicies ports.QuotaPolicies // nil: an exhausted budget always defers
	Stream        ports.StreamPublisher
	PreCheck      ports.PreCheck // nil: every Notify takes the durable path
	Clock         ports.Clock    // nil: the system clock
	IDs           ports.IDSource // nil: UUIDv7
}

var errNotImplemented = errors.New("not implemented")
