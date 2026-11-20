// Package httpapi is the recipient-facing HTTP surface and the operator's: the inbox, preferences, the
// live stream, one-click unsubscribe, provider callbacks, the operator routes and health
// (contracts/rest-api.md). It is a translation over ports.Inbox, ports.Notifier and ports.Admin: every
// rule a request must meet is the engine's, and the surface adds only what belongs to HTTP, which is
// who is asking, what a status code says, and what is never shown to a caller.
//
// Three rules run through all of it:
//
//   - The caller is the session. No route accepts a recipient, a tenant or a realm as a parameter, a
//     header or a body field (NR-008, NR-009); an operator body that names a realm is refused.
//   - A fault of the engine's own is reported to a handler and shown as "internal error", never as its
//     SQL text.
//   - Authentication failures are an empty 401, and another recipient's notification is a 404, so
//     nothing says whether an id exists.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/truongpx396/intel-notification/adapters/driving/auth"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// IdentityFunc establishes who is calling, from the request alone: the host's session in library mode,
// a verified recipient token in service mode. Any error is a 401. The route that carries its token in
// the query (the stream, because EventSource cannot set a header) is given the request with that token
// already moved to Authorization, so an IdentityFunc reads one place.
type IdentityFunc func(*http.Request) (domain.Identity, error)

// OperatorFunc authenticates an operator and returns the realm they administer: an operator
// credential, never a recipient token. The realm of an administrative call is this one and never a
// request's (NR-009). Any error is a 401.
type OperatorFunc func(*http.Request) (domain.Realm, error)

// Check is one readiness check: a name that says what failed, and the probe.
type Check struct {
	Name string
	Run  func(context.Context) error
}

// Deps is what the surface is built from. Inbox and Identify are required; every route whose
// dependency is absent is not served.
type Deps struct {
	Inbox ports.Inbox
	// Notifier serves the operator's broadcast. Required with Operator.
	Notifier ports.Notifier
	// Admin serves replay, erasure and tenant defaults. Required with Operator.
	Admin ports.Admin
	// Catalog serves PUT /admin/recipients/addresses, the REST twin of Catalog.PutAddresses. Required with
	// Operator to serve it; nil: it is not.
	Catalog ports.Catalog
	// Stream serves /notifications/stream; nil: it is not served.
	Stream   ports.StreamSubscriber
	Identify IdentityFunc
	// Operator serves every /admin route; nil: none is served.
	Operator OperatorFunc
	// Webhooks maps "channel/provider" to the callback endpoint behind /webhooks/{channel}/{provider}.
	Webhooks map[string]ports.Webhook
	// Ready are the readiness checks of /readyz; none: always ready.
	Ready []Check
}

// Option configures the surface.
type Option func(*server)

// WithErrorHandler is given every fault of the engine's own that the surface hides from the caller.
func WithErrorHandler(h func(error)) Option { return func(s *server) { s.onError = h } }

// WithPreferencesURL is where an expired unsubscribe link sends a recipient: the host's settings page.
func WithPreferencesURL(url string) Option { return func(s *server) { s.prefsURL = url } }

// WithHeartbeat is how often an idle stream sends a comment, so a proxy that closes silent connections
// does not close it. The default is 25 seconds.
func WithHeartbeat(d time.Duration) Option { return func(s *server) { s.heartbeat = d } }

type server struct {
	d         Deps
	onError   func(error)
	prefsURL  string
	heartbeat time.Duration
}

const (
	maxJSON         = 1 << 20 // a request's JSON body
	maxWebhook      = 1 << 20 // a provider's callback
	readyTimeout    = 3 * time.Second
	defaultBeat     = 25 * time.Second
	defaultPrefsURL = "/"
)

// New returns the surface. It names every dependency it needs and is not given.
func New(d Deps, opts ...Option) (http.Handler, error) {
	var missing []string
	if d.Inbox == nil {
		missing = append(missing, "Inbox")
	}
	if d.Identify == nil {
		missing = append(missing, "Identify")
	}
	if d.Operator != nil {
		if d.Notifier == nil {
			missing = append(missing, "Notifier (the operator's broadcast calls it)")
		}
		if d.Admin == nil {
			missing = append(missing, "Admin (replay, erasure and tenant defaults call it)")
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("httpapi: needs Deps.%v", missing)
	}
	s := &server{d: d, onError: func(error) {}, prefsURL: defaultPrefsURL, heartbeat: defaultBeat}
	for _, o := range opts {
		o(s)
	}

	mux := http.NewServeMux()
	// Recipient routes.
	mux.HandleFunc("GET /notifications", s.recipient(s.list))
	mux.HandleFunc("GET /notifications/unread-count", s.recipient(s.unread))
	mux.HandleFunc("POST /notifications/seen", s.recipient(s.seen))
	mux.HandleFunc("POST /notifications/read-all", s.recipient(s.readAll))
	mux.HandleFunc("POST /notifications/{id}/read", s.recipient(s.read))
	mux.HandleFunc("POST /notifications/{id}/archive", s.recipient(s.archive))
	mux.HandleFunc("GET /notifications/preferences", s.recipient(s.preferences))
	mux.HandleFunc("PUT /notifications/preferences", s.recipient(s.setPreferences))
	mux.HandleFunc("GET /notifications/schedule", s.recipient(s.schedule))
	mux.HandleFunc("PUT /notifications/schedule", s.recipient(s.setSchedule))
	if d.Stream != nil {
		mux.HandleFunc("GET /notifications/stream", s.stream)
	}
	// Unsubscribe is reached from a mail client and has no session: the token is the credential.
	mux.HandleFunc("GET /notifications/unsubscribe", s.unsubscribeConfirm)
	mux.HandleFunc("POST /notifications/unsubscribe", s.unsubscribe)
	// Operator routes.
	if d.Operator != nil {
		mux.HandleFunc("POST /admin/notifications/broadcast", s.operator(s.broadcast))
		mux.HandleFunc("POST /admin/notifications/dead-letters/{id}/replay", s.operator(s.replay))
		mux.HandleFunc("POST /admin/erasure", s.operator(s.erase))
		mux.HandleFunc("PUT /admin/tenants/{tenant_kind}/{tenant_id}/preferences", s.operator(s.tenantPreferences))
	}
	if len(d.Webhooks) > 0 {
		mux.HandleFunc("POST /webhooks/{channel}/{provider}", s.webhook)
	}
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /readyz", s.readyz)
	return mux, nil
}

// recipient runs h as the session's identity, or answers 401 with an empty body. The identity is what
// the host's session says and nothing in the request is consulted for it.
func (s *server) recipient(h func(http.ResponseWriter, *http.Request, domain.Identity)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, ok := s.identify(w, r)
		if !ok {
			return
		}
		h(w, r, id)
	}
}

func (s *server) identify(w http.ResponseWriter, r *http.Request) (domain.Identity, bool) {
	id, err := s.d.Identify(r)
	if err != nil || id.Validate() != nil {
		unauthorized(w)
		return domain.Identity{}, false
	}
	return id, true
}

func (s *server) operator(h func(http.ResponseWriter, *http.Request, domain.Realm)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		realm, err := s.d.Operator(r)
		if err != nil || realm.Validate() != nil {
			unauthorized(w)
			return
		}
		// The realm travels with the call, as a gRPC call's does, so a service of several realms routes to
		// the engine of the realm this credential administers and to no other.
		h(w, r.WithContext(auth.WithRealm(r.Context(), realm)), realm)
	}
}

func unauthorized(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", "Bearer")
	w.WriteHeader(http.StatusUnauthorized)
}

// ------------------------------------------------------------------------------------ responses

func writeJSON(w http.ResponseWriter, status int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	h := w.Header()
	h.Set("Content-Type", "application/json; charset=utf-8")
	h.Set("Cache-Control", "no-store")
	h.Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

type errorBody struct {
	Error string `json:"error"`
	// Row is the zero-based row of a list a refusal names, for a route that takes a list of rows.
	Row *int `json:"row,omitempty"`
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorBody{Error: msg})
}

// refusals are the errors that say a request cannot be accepted as it is, with words the caller can
// act on: they are composed from the request and the engine's rules, and carry nothing of the engine's
// own state.
var refusals = []error{
	domain.ErrUnknownTopic, domain.ErrUnknownChannel, domain.ErrPreferenceLocked, domain.ErrMissingIdem,
	domain.ErrTooManyRecipients, domain.ErrAudienceUnsupported, domain.ErrEmptyIdentity,
}

func isRefusal(err error) bool {
	var invalid *domain.ValidationError
	if errors.As(err, &invalid) {
		return true
	}
	for _, r := range refusals {
		if errors.Is(err, r) {
			return true
		}
	}
	return false
}

// fail answers an error from the engine. What the caller can act on is told in the engine's own words;
// what is not (a fault) is reported and shown as one.
func (s *server) fail(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, domain.ErrNotFound), errors.Is(err, domain.ErrUnknownDeadLetter):
		// Absent and someone else's are one answer, and it does not repeat the id (NR-008).
		writeError(w, http.StatusNotFound, "not found")
	case errors.Is(err, domain.ErrInvalidCursor):
		writeError(w, http.StatusBadRequest, "the cursor is not one this inbox issued")
	case errors.Is(err, domain.ErrNotReplayable):
		writeError(w, http.StatusConflict, "this dead letter holds no delivery to replay: the producer must resend it")
	case isRefusal(err):
		writeError(w, http.StatusUnprocessableEntity, err.Error())
	case errors.Is(err, context.Canceled):
		w.WriteHeader(http.StatusServiceUnavailable) // the caller is gone; nobody reads this
	default:
		s.onError(err)
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

// decode reads one JSON value from the body, strictly: a field the route does not know is an error, so
// a body that names a recipient, a tenant or a realm is refused and not quietly read as the session's.
func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxJSON)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	err := dec.Decode(v)
	if err == nil {
		if _, trailing := dec.Token(); !errors.Is(trailing, io.EOF) {
			err = errors.New("the body has more than one JSON value")
		}
	}
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeError(w, http.StatusRequestEntityTooLarge, "the body is too large")
			return false
		}
		writeError(w, http.StatusBadRequest, "the body is not the JSON this route takes: "+err.Error())
		return false
	}
	return true
}
