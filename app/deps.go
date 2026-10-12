// Package app holds the use-cases. They own the invariants: what a notification
// is planned to do, what a producer is told, what the dispatcher records. They
// depend on ports and domain and on the configuration, never on an adapter, and
// the dependency graph says so (.go-arch-lint.yml).
package app

import (
	"errors"
	"fmt"
	"strings"
	"time"

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

// systemClock is the default ports.Clock.
type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

// clockOf is d's clock, or the system clock when none was given.
func (d Deps) clockOf() ports.Clock {
	if d.Clock == nil {
		return systemClock{}
	}
	return d.Clock
}

// gaps collects the ports a constructor needs and d leaves nil, so a host fixes
// its wiring in one pass instead of one error per start.
type gaps []string

// need records that the port called name is required, and absent says it is nil.
func (g *gaps) need(name string, absent bool) {
	if absent {
		*g = append(*g, "Deps."+name)
	}
}

// err is nil when nothing is missing.
func (g gaps) err(use string) error {
	if len(g) == 0 {
		return nil
	}
	return fmt.Errorf("app: %s needs %s", use, strings.Join(g, ", "))
}

var errNotImplemented = errors.New("not implemented")
