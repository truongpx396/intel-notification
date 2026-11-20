// Package httpapi is the recipient-facing HTTP surface and the operator's: the inbox, preferences, the
// live stream, one-click unsubscribe, provider callbacks, the operator routes and health
// (contracts/rest-api.md). It is a translation over ports.Inbox, ports.Notifier and ports.Admin: every
// rule a request must meet is the engine's, and the surface adds only what belongs to HTTP.
package httpapi

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// IdentityFunc establishes who is calling, from the request alone.
type IdentityFunc func(*http.Request) (domain.Identity, error)

// OperatorFunc authenticates an operator and returns the realm they administer.
type OperatorFunc func(*http.Request) (domain.Realm, error)

// Check is one readiness check.
type Check struct {
	Name string
	Run  func(context.Context) error
}

// Deps is what the surface is built from.
type Deps struct {
	Inbox    ports.Inbox
	Notifier ports.Notifier
	Admin    ports.Admin
	Stream   ports.StreamSubscriber
	Identify IdentityFunc
	Operator OperatorFunc
	Webhooks map[string]ports.Webhook
	Ready    []Check
}

// Option configures the surface.
type Option func(*server)

type server struct {
	d Deps
}

// WithErrorHandler is given every fault of the engine's own that the surface hides from the caller.
func WithErrorHandler(func(error)) Option { return func(*server) {} }

// WithPreferencesURL is where an expired unsubscribe link sends a recipient.
func WithPreferencesURL(string) Option { return func(*server) {} }

// WithHeartbeat is how often an idle stream sends a comment, so a proxy does not close it.
func WithHeartbeat(time.Duration) Option { return func(*server) {} }

// PartitionsCheck is the readiness check that PostgreSQL answers and this month's partitions exist.
func PartitionsCheck(ports.Maintenance, ports.Clock) Check {
	return Check{Run: func(context.Context) error { return errors.New("httpapi: not implemented") }}
}

// New returns the surface.
func New(d Deps, opts ...Option) (http.Handler, error) {
	if d.Inbox == nil || d.Identify == nil {
		return nil, errors.New("httpapi: needs Deps.Inbox and Deps.Identify")
	}
	s := &server{d: d}
	for _, o := range opts {
		o(s)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNotImplemented) }), nil
}
