package httpapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/truongpx396/intel-notification/adapters/driving/auth"
	"github.com/truongpx396/intel-notification/adapters/driving/httpapi"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/notifytest"
	"github.com/truongpx396/intel-notification/ports"
)

// The handler tests of T044: over httptest, a fake Inbox, a fake Notifier and a fake Admin, no
// infrastructure. One row per line of the contract's error table, and the observable assertions that
// are about HTTP: what a caller is told, and what is never called.

const realm domain.Realm = "aisat"

var (
	alice = domain.Identity{Realm: realm, Tenant: domain.Tenant{Kind: "workspace", ID: "w1"}, Recipient: domain.Recipient{Kind: "user", ID: "alice"}}
	bob   = domain.Identity{Realm: realm, Tenant: domain.Tenant{Kind: "workspace", ID: "w1"}, Recipient: domain.Recipient{Kind: "user", ID: "bob"}}
)

// ---------------------------------------------------------------------------- fakes

type call struct {
	Method string
	ID     domain.Identity
	Args   []any
}

// inboxFake is a ports.Inbox that remembers each call and its identity and answers from the function
// set for the method, or with the zero value.
type inboxFake struct {
	mu    sync.Mutex
	calls []call

	onList          func(domain.ListQuery) (domain.Page, error)
	onGet           func(nid, locale string) (*domain.InboxItem, error)
	onUnread        func() (domain.UnreadCount, error)
	onMarkRead      func(ids []string) error
	onArchive       func(ids []string, archived bool) error
	onPreferences   func() ([]domain.ResolvedPreference, error)
	onSetPrefs      func([]domain.Preference) error
	onSetSchedule   func(domain.DeliverySchedule) error
	onSchedule      func() (domain.DeliverySchedule, error)
	onUnsubscribeFn func(token string) error
}

var _ ports.Inbox = (*inboxFake)(nil)

func (f *inboxFake) rec(method string, id domain.Identity, args ...any) {
	f.mu.Lock()
	f.calls = append(f.calls, call{method, id, args})
	f.mu.Unlock()
}

func (f *inboxFake) Calls() []call {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]call(nil), f.calls...)
}

func (f *inboxFake) List(_ context.Context, id domain.Identity, q domain.ListQuery) (domain.Page, error) {
	f.rec("List", id, q)
	if f.onList != nil {
		return f.onList(q)
	}
	return domain.Page{}, nil
}

func (f *inboxFake) Get(_ context.Context, id domain.Identity, nid, locale string) (*domain.InboxItem, error) {
	f.rec("Get", id, nid, locale)
	if f.onGet != nil {
		return f.onGet(nid, locale)
	}
	return nil, nil
}

func (f *inboxFake) Unread(_ context.Context, id domain.Identity) (domain.UnreadCount, error) {
	f.rec("Unread", id)
	if f.onUnread != nil {
		return f.onUnread()
	}
	return domain.UnreadCount{}, nil
}

func (f *inboxFake) MarkSeen(_ context.Context, id domain.Identity, before time.Time) error {
	f.rec("MarkSeen", id, before)
	return nil
}

func (f *inboxFake) MarkRead(_ context.Context, id domain.Identity, ids []string) error {
	f.rec("MarkRead", id, ids)
	if f.onMarkRead != nil {
		return f.onMarkRead(ids)
	}
	return nil
}

func (f *inboxFake) MarkAllRead(_ context.Context, id domain.Identity) error {
	f.rec("MarkAllRead", id)
	return nil
}

func (f *inboxFake) Archive(_ context.Context, id domain.Identity, ids []string, archived bool) error {
	f.rec("Archive", id, ids, archived)
	if f.onArchive != nil {
		return f.onArchive(ids, archived)
	}
	return nil
}

func (f *inboxFake) Preferences(_ context.Context, id domain.Identity) ([]domain.ResolvedPreference, error) {
	f.rec("Preferences", id)
	if f.onPreferences != nil {
		return f.onPreferences()
	}
	return nil, nil
}

func (f *inboxFake) SetPreferences(_ context.Context, id domain.Identity, p []domain.Preference) error {
	f.rec("SetPreferences", id, p)
	if f.onSetPrefs != nil {
		return f.onSetPrefs(p)
	}
	return nil
}

func (f *inboxFake) Schedule(_ context.Context, id domain.Identity) (domain.DeliverySchedule, error) {
	f.rec("Schedule", id)
	if f.onSchedule != nil {
		return f.onSchedule()
	}
	return domain.DeliverySchedule{}, nil
}

func (f *inboxFake) SetSchedule(_ context.Context, id domain.Identity, s domain.DeliverySchedule) error {
	f.rec("SetSchedule", id, s)
	if f.onSetSchedule != nil {
		return f.onSetSchedule(s)
	}
	return nil
}

func (f *inboxFake) Unsubscribe(_ context.Context, token string) error {
	f.rec("Unsubscribe", domain.Identity{}, token)
	if f.onUnsubscribeFn != nil {
		return f.onUnsubscribeFn(token)
	}
	return nil
}

// adminFake is a ports.Admin that remembers each call.
type adminFake struct {
	mu       sync.Mutex
	calls    []call
	onErase  func(domain.Identity) (map[string]int, error)
	onReplay func(id string) error
	onTenant func(domain.TenantPreference) error
}

var _ ports.Admin = (*adminFake)(nil)

func (a *adminFake) rec(method string, id domain.Identity, args ...any) {
	a.mu.Lock()
	a.calls = append(a.calls, call{method, id, args})
	a.mu.Unlock()
}

func (a *adminFake) Calls() []call {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]call(nil), a.calls...)
}

func (a *adminFake) Erase(_ context.Context, id domain.Identity) (map[string]int, error) {
	a.rec("Erase", id)
	if a.onErase != nil {
		return a.onErase(id)
	}
	return map[string]int{}, nil
}

func (a *adminFake) ReplayDeadLetter(_ context.Context, r domain.Realm, id string) error {
	a.rec("ReplayDeadLetter", domain.Identity{Realm: r}, id)
	if a.onReplay != nil {
		return a.onReplay(id)
	}
	return nil
}

func (a *adminFake) SetTenantPreference(_ context.Context, r domain.Realm, p domain.TenantPreference) error {
	a.rec("SetTenantPreference", domain.Identity{Realm: r}, p)
	if a.onTenant != nil {
		return a.onTenant(p)
	}
	return nil
}

// webhookFake verifies against a fixed secret and remembers what it parsed.
type webhookFake struct {
	mu       sync.Mutex
	verified []string
	handled  []string
	handle   func(body []byte) error
}

func (w *webhookFake) Verify(h map[string][]string, body []byte) error {
	w.mu.Lock()
	w.verified = append(w.verified, string(body))
	w.mu.Unlock()
	if len(h["X-Signature"]) != 1 || h["X-Signature"][0] != "good" {
		return errors.New("bad signature")
	}
	return nil
}

func (w *webhookFake) Handle(_ context.Context, body []byte) error {
	w.mu.Lock()
	w.handled = append(w.handled, string(body))
	w.mu.Unlock()
	if w.handle != nil {
		return w.handle(body)
	}
	return nil
}

// ---------------------------------------------------------------------------- fixture

type fixture struct {
	h        http.Handler
	inbox    *inboxFake
	notifier *notifytest.ScriptedNotifier
	admin    *adminFake
	catalog  *notifytest.ScriptedCatalog
	hook     *webhookFake
	errs     []error
	mu       sync.Mutex
	identify func(*http.Request) (domain.Identity, error)
}

// tokens maps a bearer token to an identity: what a host's session layer does. Anything else is no
// session.
var tokens = map[string]domain.Identity{"alice": alice, "bob": bob}

func newFixture(t *testing.T, opts ...httpapi.Option) *fixture {
	t.Helper()
	f := &fixture{inbox: &inboxFake{}, notifier: &notifytest.ScriptedNotifier{}, admin: &adminFake{}, catalog: &notifytest.ScriptedCatalog{}, hook: &webhookFake{}}
	f.identify = func(r *http.Request) (domain.Identity, error) {
		id, ok := tokens[strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")]
		if !ok {
			return domain.Identity{}, errors.New("no session")
		}
		return id, nil
	}
	opts = append([]httpapi.Option{httpapi.WithErrorHandler(func(err error) {
		f.mu.Lock()
		f.errs = append(f.errs, err)
		f.mu.Unlock()
	})}, opts...)
	h, err := httpapi.New(httpapi.Deps{
		Inbox: f.inbox, Notifier: f.notifier, Admin: f.admin, Catalog: f.catalog, Stream: notifytest.NewStream(notifytest.NewProbe()),
		Identify: func(r *http.Request) (domain.Identity, error) { return f.identify(r) },
		// An operator credential is its own: a recipient's token is not one.
		Operator: func(r *http.Request) (domain.Realm, error) {
			if r.Header.Get("Authorization") != "Bearer operator-secret" {
				return "", errors.New("not an operator")
			}
			return realm, nil
		},
		Webhooks: map[string]ports.Webhook{"email/resend": f.hook},
	}, opts...)
	if err != nil {
		t.Fatalf("httpapi.New: %v", err)
	}
	f.h = h
	return f
}

func (f *fixture) reported() []error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]error(nil), f.errs...)
}

func (f *fixture) do(method, path, token, body string, headers ...string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	rec := httptest.NewRecorder()
	f.h.ServeHTTP(rec, req)
	return rec
}

// nth is the inbox's nth call, or a failure that says it was not made: a test that indexed an empty
// list would panic, which shows nothing about the code under test.
func nth(t *testing.T, f *inboxFake, n int) call {
	t.Helper()
	calls := f.Calls()
	if len(calls) <= n {
		t.Fatalf("the inbox was called %d times, want at least %d: %v", len(calls), n+1, calls)
	}
	return calls[n]
}

func decode(t *testing.T, rec *httptest.ResponseRecorder, v any) {
	t.Helper()
	if err := json.Unmarshal(rec.Body.Bytes(), v); err != nil {
		t.Fatalf("body %q is not the JSON expected: %v", rec.Body.String(), err)
	}
}

// ---------------------------------------------------------------------------- authentication

// Every recipient route needs a session: a missing, expired or unverifiable token is a 401 with an
// empty body, and nothing behind the route is called.
func TestEveryRecipientRouteIs401WithoutASession(t *testing.T) {
	t.Parallel()
	routes := []struct{ method, path, body string }{
		{"GET", "/notifications", ""},
		{"GET", "/notifications/unread-count", ""},
		{"POST", "/notifications/seen", `{"before":"2026-03-04T10:00:00Z"}`},
		{"POST", "/notifications/0195f3a0-0000-7000-8000-000000000001/read", ""},
		{"POST", "/notifications/read-all", ""},
		{"POST", "/notifications/0195f3a0-0000-7000-8000-000000000001/archive", `{"archived":true}`},
		{"GET", "/notifications/preferences", ""},
		{"PUT", "/notifications/preferences", `[]`},
		{"GET", "/notifications/schedule", ""},
		{"PUT", "/notifications/schedule", `{}`},
		{"GET", "/notifications/stream", ""},
	}
	for _, rt := range routes {
		for _, token := range []string{"", "not-a-token"} {
			t.Run(rt.method+" "+rt.path+" with token "+token, func(t *testing.T) {
				t.Parallel()
				f := newFixture(t)
				rec := f.do(rt.method, rt.path, token, rt.body)
				if rec.Code != http.StatusUnauthorized || rec.Body.Len() != 0 {
					t.Errorf("status %d with body %q, want 401 and an empty body", rec.Code, rec.Body.String())
				}
				if got := f.inbox.Calls(); len(got) != 0 {
					t.Errorf("the inbox was called %v for a request with no session", got)
				}
			})
		}
	}
}

// No route accepts a recipient, a tenant or a realm as a parameter (NR-008): what names the caller is
// the session and nothing else. A request that carries another recipient's name in a header, the query
// or the body is served as the session's own.
func TestTheRecipientComesFromTheSessionAndNothingElse(t *testing.T) {
	t.Parallel()
	spoof := []string{
		"X-Recipient", "bob", "X-Recipient-Id", "bob", "X-Tenant", "w9", "X-Realm", "rival", "X-Forwarded-User", "bob",
	}
	cases := []struct{ method, path, body string }{
		{"GET", "/notifications?recipient=bob&recipient_id=bob&tenant=w9&realm=rival", ""},
		{"GET", "/notifications/unread-count?recipient=bob&tenant=w9", ""},
		{"POST", "/notifications/read-all?recipient=bob", ""},
		{"GET", "/notifications/preferences?recipient=bob", ""},
		{"PUT", "/notifications/preferences?recipient=bob", `[{"topic":"t","channel":"email","enabled":false}]`},
		{"GET", "/notifications/schedule?recipient=bob", ""},
	}
	for _, tc := range cases {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t)
			rec := f.do(tc.method, tc.path, "alice", tc.body, spoof...)
			if rec.Code >= 500 {
				t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
			}
			calls := f.inbox.Calls()
			if len(calls) == 0 {
				t.Fatalf("the inbox was never called (status %d: %s)", rec.Code, rec.Body.String())
			}
			for _, c := range calls {
				if c.ID != alice {
					t.Errorf("%s ran as %+v, want the session's %+v", c.Method, c.ID, alice)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------- inbox

func TestListPassesTheQueryAndShowsTheItems(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.inbox.onList = func(q domain.ListQuery) (domain.Page, error) {
		return domain.Page{
			Items: []domain.InboxItem{{
				ID: "n1", CreatedAt: time.Date(2026, 3, 4, 10, 0, 0, 0, time.UTC), Topic: "ingestion_complete", Priority: domain.PriorityInfo,
				Title: "Done", Body: "Your upload finished.", Data: map[string]any{"name": "a.pdf"}, Payload: map[string]string{"url": "/a"},
				ReadAt: time.Date(2026, 3, 4, 11, 0, 0, 0, time.UTC),
			}},
			NextCursor: "cursor-2",
		}, nil
	}
	rec := f.do("GET", "/notifications?limit=25&cursor=cursor-1&state=unread&topic=ingestion_complete&locale=fr-CA", "alice", "")
	if rec.Code != 200 {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	q := nth(t, f.inbox, 0).Args[0].(domain.ListQuery)
	if q.Limit != 25 || q.Cursor != "cursor-1" || q.State != domain.InboxUnread || q.Topic != "ingestion_complete" || q.Locale != "fr-CA" {
		t.Errorf("the inbox was asked %+v, want the query as sent", q)
	}
	var body struct {
		Items []map[string]any `json:"items"`
		Next  string           `json:"next_cursor"`
	}
	decode(t, rec, &body)
	if len(body.Items) != 1 || body.Next != "cursor-2" {
		t.Fatalf("body = %s, want one item and the next cursor", rec.Body.String())
	}
	item := body.Items[0]
	for k, want := range map[string]any{"id": "n1", "topic": "ingestion_complete", "priority": "info", "title": "Done", "body": "Your upload finished.",
		"created_at": "2026-03-04T10:00:00Z", "read_at": "2026-03-04T11:00:00Z"} {
		if item[k] != want {
			t.Errorf("item[%q] = %v, want %v", k, item[k], want)
		}
	}
	if _, set := item["seen_at"]; set {
		t.Error("an unset seen_at was sent as a time, and not left out")
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type = %q", ct)
	}
}

func TestAnEmptyListIsAnEmptyArrayNotNull(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	rec := f.do("GET", "/notifications", "alice", "")
	if !strings.Contains(rec.Body.String(), `"items":[]`) {
		t.Errorf("body = %s, want items as []", rec.Body.String())
	}
}

// The locale is the query's, or the Accept-Language header's best tag. A value that is not a language
// tag never reaches the renderer, whose template keys it would otherwise become.
func TestTheLocaleIsTheQueryOrTheBestAcceptLanguageTag(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name, path, accept, want string
		status                   int
	}{
		{"none asked", "/notifications", "", "", 200},
		{"the header's first", "/notifications", "fr-CA,fr;q=0.9,en;q=0.8", "fr-CA", 200},
		{"the header's highest q", "/notifications", "en;q=0.5, de;q=0.9", "de", 200},
		{"a wildcard is no language", "/notifications", "*", "", 200},
		{"a refused language is skipped", "/notifications", "xx;q=0, es;q=0.4", "es", 200},
		{"the query wins", "/notifications?locale=pt-BR", "fr", "pt-BR", 200},
		{"a header that is not a tag is ignored", "/notifications", "../../etc/passwd", "", 200},
		{"a query that is not a tag is refused", "/notifications?locale=../x", "", "", 400},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t)
			var headers []string
			if tc.accept != "" {
				headers = []string{"Accept-Language", tc.accept}
			}
			rec := f.do("GET", tc.path, "alice", "", headers...)
			if rec.Code != tc.status {
				t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
			}
			if tc.status == 200 {
				if got := nth(t, f.inbox, 0).Args[0].(domain.ListQuery).Locale; got != tc.want {
					t.Errorf("locale = %q, want %q", got, tc.want)
				}
			}
		})
	}
}

func TestABadQueryIs400AndNeverReachesTheInbox(t *testing.T) {
	t.Parallel()
	for _, path := range []string{"/notifications?limit=abc", "/notifications?limit=-1", "/notifications?state=deleted"} {
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t)
			if rec := f.do("GET", path, "alice", ""); rec.Code != 400 {
				t.Errorf("status %d, want 400", rec.Code)
			}
			if len(f.inbox.Calls()) != 0 {
				t.Error("the inbox was asked for a query that is not one")
			}
		})
	}
	f := newFixture(t)
	f.inbox.onList = func(domain.ListQuery) (domain.Page, error) { return domain.Page{}, domain.ErrInvalidCursor }
	if rec := f.do("GET", "/notifications?cursor=forged", "alice", ""); rec.Code != 400 {
		t.Errorf("a cursor the store did not issue = %d, want 400: it is client input", rec.Code)
	}
}

func TestUnreadIsTheBoundedCount(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.inbox.onUnread = func() (domain.UnreadCount, error) { return domain.UnreadCount{Count: 99, Capped: true}, nil }
	rec := f.do("GET", "/notifications/unread-count", "alice", "")
	var got map[string]any
	decode(t, rec, &got)
	if rec.Code != 200 || got["count"] != float64(99) || got["capped"] != true || len(got) != 2 {
		t.Errorf("%d %s, want {count: 99, capped: true}", rec.Code, rec.Body.String())
	}
}

func TestMarkingStateChangesIsIdempotentAndEmpty(t *testing.T) {
	t.Parallel()
	const nid = "0195f3a0-0000-7000-8000-000000000001"
	f := newFixture(t)

	if rec := f.do("POST", "/notifications/seen", "alice", `{"before":"2026-03-04T10:00:00Z"}`); rec.Code != 204 {
		t.Errorf("seen = %d %s, want 204", rec.Code, rec.Body.String())
	}
	if got := nth(t, f.inbox, 0); got.Method != "MarkSeen" || !got.Args[0].(time.Time).Equal(time.Date(2026, 3, 4, 10, 0, 0, 0, time.UTC)) {
		t.Errorf("MarkSeen call = %+v", got)
	}
	if rec := f.do("POST", "/notifications/"+nid+"/read", "alice", ""); rec.Code != 204 {
		t.Errorf("read = %d, want 204", rec.Code)
	}
	if rec := f.do("POST", "/notifications/read-all", "alice", ""); rec.Code != 204 {
		t.Errorf("read-all = %d, want 204", rec.Code)
	}
	if rec := f.do("POST", "/notifications/"+nid+"/archive", "alice", `{"archived":false}`); rec.Code != 204 {
		t.Errorf("archive = %d, want 204", rec.Code)
	}
	last := f.inbox.Calls()
	if len(last) != 4 {
		t.Fatalf("the inbox was called %d times, want 4: %v", len(last), last)
	}
	if c := last[1]; c.Method != "MarkRead" || fmt.Sprint(c.Args[0]) != "["+nid+"]" {
		t.Errorf("MarkRead call = %+v, want the id from the path", c)
	}
	if c := last[3]; c.Method != "Archive" || c.Args[1] != false {
		t.Errorf("Archive call = %+v, want archived=false as sent", c)
	}
}

func TestMarkingWithABadBodyIs400(t *testing.T) {
	t.Parallel()
	const nid = "0195f3a0-0000-7000-8000-000000000001"
	for name, rt := range map[string]struct{ path, body string }{
		"seen without a time":          {"/notifications/seen", `{}`},
		"seen with a time that is not": {"/notifications/seen", `{"before":"yesterday"}`},
		"seen with no JSON":            {"/notifications/seen", `not json`},
		"archive without the flag":     {"/notifications/" + nid + "/archive", `{}`},
		"archive with an unknown key":  {"/notifications/" + nid + "/archive", `{"archived":true,"recipient":"bob"}`},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t)
			if rec := f.do("POST", rt.path, "alice", rt.body); rec.Code != 400 {
				t.Errorf("status %d, want 400", rec.Code)
			}
			if len(f.inbox.Calls()) != 0 {
				t.Error("the inbox was called for a request that is not one")
			}
		})
	}
}

// Another recipient's notification is a 404, never a 403: a 403 would confirm the id exists (NR-008).
// The answer is the same for one that is not there, and it does not repeat the id.
func TestSomeoneElsesNotificationIs404AndSaysNothingAboutIt(t *testing.T) {
	t.Parallel()
	const nid = "0195f3a0-0000-7000-8000-000000000001"
	f := newFixture(t)
	f.inbox.onMarkRead = func([]string) error { return fmt.Errorf("mark read: %w", domain.ErrNotFound) }
	f.inbox.onArchive = func([]string, bool) error { return fmt.Errorf("archive: %w", domain.ErrNotFound) }
	for _, rt := range []struct{ path, body string }{{"/notifications/" + nid + "/read", ""}, {"/notifications/" + nid + "/archive", `{"archived":true}`}} {
		rec := f.do("POST", rt.path, "bob", rt.body)
		if rec.Code != http.StatusNotFound {
			t.Errorf("%s = %d, want 404 and never 403", rt.path, rec.Code)
		}
		if strings.Contains(rec.Body.String(), nid) {
			t.Errorf("the answer %q repeats the id", rec.Body.String())
		}
	}
}

// ---------------------------------------------------------------------------- preferences and schedule

func TestPreferencesShowTheSourceAndTheLockOfEachPair(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.inbox.onPreferences = func() ([]domain.ResolvedPreference, error) {
		return []domain.ResolvedPreference{
			{Topic: "ingestion_complete", Channel: "email", Enabled: false, Source: domain.SourceRecipient},
			{Topic: "credit_exhausted", Channel: "email", Enabled: true, Source: domain.SourceTopicDefault, Locked: true},
		}, nil
	}
	rec := f.do("GET", "/notifications/preferences", "alice", "")
	var got []map[string]any
	decode(t, rec, &got)
	if rec.Code != 200 || len(got) != 2 {
		t.Fatalf("%d %s, want an array of two", rec.Code, rec.Body.String())
	}
	want := []map[string]any{
		{"topic": "ingestion_complete", "channel": "email", "enabled": false, "source": "recipient", "locked": false},
		{"topic": "credit_exhausted", "channel": "email", "enabled": true, "source": "topic_default", "locked": true},
	}
	for i := range want {
		if fmt.Sprint(got[i]) != fmt.Sprint(want[i]) {
			t.Errorf("preference %d = %v, want %v", i, got[i], want[i])
		}
	}
}

func TestSetPreferencesPassesTheListOn(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	rec := f.do("PUT", "/notifications/preferences", "alice", `[{"topic":"ingestion_complete","channel":"email","enabled":false},{"topic":"weekly","channel":"in_app","enabled":true}]`)
	if rec.Code != 204 {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	got := nth(t, f.inbox, 0).Args[0].([]domain.Preference)
	want := []domain.Preference{{Topic: "ingestion_complete", Channel: "email", Enabled: false}, {Topic: "weekly", Channel: "in_app", Enabled: true}}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("SetPreferences got %v, want %v", got, want)
	}
}

// 422 names the value, the pair and the reason: an unknown topic or channel, an essential or locked pair
// (rest-api.md § Error semantics). The body is the engine's own words, so a settings screen can show it.
func TestARefusedPreferenceIs422AndSaysWhy(t *testing.T) {
	t.Parallel()
	refusal := func(msg string, sentinel error) error {
		return fmt.Errorf("set preferences: %w", &domain.ValidationError{Problems: []domain.Problem{{Field: "preferences[0]", Message: msg, Err: sentinel}}})
	}
	cases := map[string]error{
		"an unknown topic":   refusal(`topic "nope" is not registered`, domain.ErrUnknownTopic),
		"an unknown channel": refusal(`channel "pigeon" is not registered`, domain.ErrUnknownChannel),
		"an essential pair":  refusal(`topic "credit_exhausted" is essential: its "email" channel cannot be changed`, domain.ErrPreferenceLocked),
		"a locked pair":      refusal(`topic "t" on channel "email" is locked by the tenant`, domain.ErrPreferenceLocked),
	}
	for name, err := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t)
			f.inbox.onSetPrefs = func([]domain.Preference) error { return err }
			rec := f.do("PUT", "/notifications/preferences", "alice", `[{"topic":"t","channel":"email","enabled":false}]`)
			if rec.Code != http.StatusUnprocessableEntity {
				t.Fatalf("status %d, want 422", rec.Code)
			}
			var body struct{ Error string }
			decode(t, rec, &body)
			if !strings.Contains(body.Error, err.Error()) && !strings.Contains(rec.Body.String(), "is not registered") && !strings.Contains(rec.Body.String(), "cannot be changed") && !strings.Contains(rec.Body.String(), "locked") {
				t.Errorf("body %s does not say why", rec.Body.String())
			}
			for _, word := range []string{"nope", "pigeon", "credit_exhausted", "locked"} {
				if strings.Contains(err.Error(), word) && !strings.Contains(rec.Body.String(), word) {
					t.Errorf("body %s does not name %q", rec.Body.String(), word)
				}
			}
		})
	}
}

func TestAPreferenceBodyThatIsNotAListIs400(t *testing.T) {
	t.Parallel()
	for _, body := range []string{`{}`, `not json`, `[{"topic":"t","channel":"email"}]`, `[{"topic":"t","channel":"email","enabled":true,"extra":1}]`} {
		f := newFixture(t)
		if rec := f.do("PUT", "/notifications/preferences", "alice", body); rec.Code != 400 {
			t.Errorf("body %q = %d, want 400", body, rec.Code)
		}
		if len(f.inbox.Calls()) != 0 {
			t.Errorf("body %q reached the inbox", body)
		}
	}
}

func TestTheScheduleRoundTripsAndRestatesThatCriticalIgnoresQuietHours(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.inbox.onSchedule = func() (domain.DeliverySchedule, error) {
		return domain.DeliverySchedule{QuietStart: "22:00", QuietEnd: "07:00", Timezone: "Europe/Paris", Digest: 30 * time.Minute}, nil
	}
	rec := f.do("GET", "/notifications/schedule", "alice", "")
	var got map[string]any
	decode(t, rec, &got)
	if got["quiet_start"] != "22:00" || got["quiet_end"] != "07:00" || got["timezone"] != "Europe/Paris" || got["digest"] != "30m0s" {
		t.Errorf("GET schedule = %s", rec.Body.String())
	}

	rec = f.do("PUT", "/notifications/schedule", "alice", `{"quiet_start":"23:00","quiet_end":"06:00","timezone":"Asia/Tokyo","digest":"1h"}`)
	if rec.Code != 200 {
		t.Fatalf("PUT schedule = %d: %s", rec.Code, rec.Body.String())
	}
	set := nth(t, f.inbox, 1).Args[0].(domain.DeliverySchedule)
	if set != (domain.DeliverySchedule{QuietStart: "23:00", QuietEnd: "06:00", Timezone: "Asia/Tokyo", Digest: time.Hour}) {
		t.Errorf("SetSchedule got %+v", set)
	}
	decode(t, rec, &got)
	if got["critical_ignores_quiet_hours"] != true {
		t.Errorf("the response %s does not restate that critical notifications ignore quiet hours (D9): a client could present a false promise", rec.Body.String())
	}
}

func TestAnInvalidScheduleIs422AndAnUnreadableOneIs400(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.inbox.onSetSchedule = func(domain.DeliverySchedule) error {
		return fmt.Errorf("set schedule: %w", &domain.ValidationError{Problems: []domain.Problem{{Field: "Timezone", Message: `"Mars/Base" is not a time zone`}}})
	}
	rec := f.do("PUT", "/notifications/schedule", "alice", `{"timezone":"Mars/Base"}`)
	if rec.Code != 422 || !strings.Contains(rec.Body.String(), "Mars/Base") {
		t.Errorf("an invalid schedule = %d %s, want 422 naming the value", rec.Code, rec.Body.String())
	}
	if rec := f.do("PUT", "/notifications/schedule", "alice", `{"digest":"soon"}`); rec.Code != 400 {
		t.Errorf("a digest that is not a duration = %d, want 400", rec.Code)
	}
}

// ---------------------------------------------------------------------------- unsubscribe

// A mail scanner fetches every URL in a message. A GET of the link shows a page and changes nothing: it
// makes no call to the preference writer, nor to anything else (D34).
func TestAGetOfTheUnsubscribeLinkChangesNothing(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	rec := f.do("GET", "/notifications/unsubscribe?token=v1.k1.abc.def", "", "")
	if rec.Code != 200 || !strings.HasPrefix(rec.Header().Get("Content-Type"), "text/html") {
		t.Fatalf("GET = %d %s, want a page", rec.Code, rec.Header().Get("Content-Type"))
	}
	if got := f.inbox.Calls(); len(got) != 0 {
		t.Fatalf("a GET called %v: a link previewer would unsubscribe whoever received the mail", got)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `method="post"`) || !strings.Contains(body, "token=v1.k1.abc.def") {
		t.Errorf("the page %q has no form that posts the token", body)
	}
	if !strings.Contains(body, "<form") || strings.Contains(strings.ToLower(body), "<script") {
		t.Errorf("the page is meant to be a form and nothing that runs: %q", body)
	}
}

// The token travels in the link and is read from the link only: a mail client's one-click POST carries
// a form body of its own (List-Unsubscribe=One-Click), which names nothing.
func TestAOneClickPostDisablesTheOnePairTheTokenNames(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	rec := f.do("POST", "/notifications/unsubscribe?token=v1.k1.abc.def", "", "List-Unsubscribe=One-Click",
		"Content-Type", "application/x-www-form-urlencoded")
	if rec.Code != 200 {
		t.Fatalf("POST = %d %s, want 200: RFC 8058 wants a success", rec.Code, rec.Body.String())
	}
	got := f.inbox.Calls()
	if len(got) != 1 || got[0].Method != "Unsubscribe" || got[0].Args[0] != "v1.k1.abc.def" {
		t.Fatalf("calls %+v, want exactly one Unsubscribe with the link's token", got)
	}
	// A body that names another token is not read.
	f2 := newFixture(t)
	f2.do("POST", "/notifications/unsubscribe?token=v1.k1.abc.def", "", "token=v1.k1.other.other", "Content-Type", "application/x-www-form-urlencoded")
	if got := f2.inbox.Calls(); len(got) != 1 || got[0].Args[0] != "v1.k1.abc.def" {
		t.Errorf("calls %+v, want the query's token and not the body's", got)
	}
	// It needs no session.
	if rec.Code == http.StatusUnauthorized {
		t.Error("one-click unsubscribe asked for a session")
	}
}

func TestAnInvalidOrExpiredTokenIs410WithARouteToPreferences(t *testing.T) {
	t.Parallel()
	for name, err := range map[string]error{
		"invalid": fmt.Errorf("unsubscribe: %w", domain.ErrInvalidUnsubscribeToken),
		"expired": fmt.Errorf("unsubscribe: %w", domain.ErrExpiredUnsubscribeToken),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t, httpapi.WithPreferencesURL("https://app.example.test/settings/notifications"))
			f.inbox.onUnsubscribeFn = func(string) error { return err }
			rec := f.do("POST", "/notifications/unsubscribe?token=x", "", "")
			if rec.Code != http.StatusGone || !strings.HasPrefix(rec.Header().Get("Content-Type"), "text/html") {
				t.Fatalf("status %d %s, want a 410 page", rec.Code, rec.Header().Get("Content-Type"))
			}
			if !strings.Contains(rec.Body.String(), "https://app.example.test/settings/notifications") {
				t.Errorf("the page %q has no route to preferences", rec.Body.String())
			}
		})
	}
	f := newFixture(t)
	if rec := f.do("POST", "/notifications/unsubscribe", "", ""); rec.Code != http.StatusGone {
		t.Errorf("a link with no token = %d, want 410", rec.Code)
	}
	if got := f.inbox.Calls(); len(got) != 0 {
		t.Errorf("a link with no token reached the inbox: %v", got)
	}
}

// The token is in the URL, so the page must not leak it by Referer, and a mail client's page is
// content and not script.
func TestUnsubscribePagesDoNotLeakTheTokenAndRunNothing(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	for _, rec := range []*httptest.ResponseRecorder{
		f.do("GET", "/notifications/unsubscribe?token=abc", "", ""),
		f.do("POST", "/notifications/unsubscribe?token=abc", "", ""),
	} {
		h := rec.Header()
		if h.Get("Referrer-Policy") != "no-referrer" || h.Get("X-Content-Type-Options") != "nosniff" || h.Get("Cache-Control") != "no-store" ||
			!strings.Contains(h.Get("Content-Security-Policy"), "default-src 'none'") {
			t.Errorf("headers %v: want no-referrer, nosniff, no-store and a CSP that allows nothing", h)
		}
	}
}

func TestATokenIsEscapedIntoThePage(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	rec := f.do("GET", `/notifications/unsubscribe?token="><script>alert(1)</script>`, "", "")
	if strings.Contains(rec.Body.String(), "<script>alert") {
		t.Errorf("the token was written into the page unescaped: %s", rec.Body.String())
	}
}

func TestAFaultWhileUnsubscribingIs500AndIsReportedNotShown(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.inbox.onUnsubscribeFn = func(string) error { return errors.New("pq: connection to 10.0.0.5 refused") }
	rec := f.do("POST", "/notifications/unsubscribe?token=x", "", "")
	if rec.Code != 500 || strings.Contains(rec.Body.String(), "10.0.0.5") {
		t.Errorf("= %d %s, want a 500 that does not show the fault", rec.Code, rec.Body.String())
	}
	if len(f.reported()) != 1 {
		t.Errorf("reported %v, want the fault", f.reported())
	}
}

// ---------------------------------------------------------------------------- operator

// The operator routes want an operator credential, never a recipient token, and call nothing without one.
func TestOperatorRoutesAreClosedToEveryoneButAnOperator(t *testing.T) {
	t.Parallel()
	routes := []struct{ method, path, body string }{
		{"POST", "/admin/notifications/broadcast", `{"tenant":{"kind":"workspace","id":"w1"},"recipients":[{"kind":"user","id":"u1"}],"topic":"t","idem_key":"k"}`},
		{"POST", "/admin/notifications/dead-letters/7f1c7b0e-5a53-4f5c-9d1a-2b7a6b0f2f11/replay", ""},
		{"POST", "/admin/erasure", `{"tenant":{"kind":"workspace","id":"w1"},"recipient":{"kind":"user","id":"u1"}}`},
		{"PUT", "/admin/tenants/workspace/w1/preferences", `[]`},
		{"PUT", "/admin/recipients/addresses", `{"tenant":{"kind":"workspace","id":"w1"},"recipient":{"kind":"user","id":"u1"},"channel":"email","addresses":[]}`},
	}
	for _, rt := range routes {
		for _, token := range []string{"", "alice", "operator"} { // "alice" is a recipient's token
			t.Run(rt.method+" "+rt.path+" with "+token, func(t *testing.T) {
				t.Parallel()
				f := newFixture(t)
				rec := f.do(rt.method, rt.path, token, rt.body)
				if rec.Code != http.StatusUnauthorized || rec.Body.Len() != 0 {
					t.Errorf("status %d body %q, want 401 and empty", rec.Code, rec.Body.String())
				}
				if len(f.notifier.Broadcasted())+len(f.admin.Calls())+len(f.catalog.Calls()) != 0 {
					t.Error("something was called without an operator credential")
				}
			})
		}
	}
}

func TestBroadcastRecordsAndReturns202WithItsID(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.notifier.OnBroadcast = func(_ context.Context, b domain.BroadcastRequest) (domain.BroadcastReceipt, error) {
		return domain.BroadcastReceipt{BroadcastID: "b-7", Applied: true}, nil
	}
	rec := f.do("POST", "/admin/notifications/broadcast", "operator-secret", `{
		"tenant":{"kind":"workspace","id":"w1"},
		"recipients":[{"kind":"user","id":"u1"},{"kind":"user","id":"u2"}],
		"topic":"release_notes","priority":"warning","data":{"v":"1.2"},"title":"1.2","body":"Notes","payload":{"url":"/r"},"idem_key":"release:1.2"}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status %d %s, want 202", rec.Code, rec.Body.String())
	}
	var got map[string]any
	decode(t, rec, &got)
	if got["broadcast_id"] != "b-7" || got["applied"] != true {
		t.Errorf("body %s, want the broadcast's id and applied", rec.Body.String())
	}
	asked := f.notifier.Broadcasted()
	if len(asked) != 1 {
		t.Fatalf("Broadcast was called %d times", len(asked))
	}
	b := asked[0]
	if b.Tenant != (domain.Tenant{Kind: "workspace", ID: "w1"}) || len(b.Recipients) != 2 || b.Audience != "" || b.Topic != "release_notes" ||
		b.Priority != domain.PriorityWarning || b.Data["v"] != "1.2" || b.Title != "1.2" || b.Body != "Notes" || b.Payload["url"] != "/r" || b.IdemKey != "release:1.2" {
		t.Errorf("Broadcast was asked %+v, want the body as sent", b)
	}
}

func TestBroadcastByAudienceSelector(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	rec := f.do("POST", "/admin/notifications/broadcast", "operator-secret",
		`{"tenant":{"kind":"workspace","id":"w1"},"audience":"all-members","topic":"release_notes","idem_key":"r1"}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status %d %s", rec.Code, rec.Body.String())
	}
	if got := f.notifier.Broadcasted(); len(got) != 1 || got[0].Audience != "all-members" || len(got[0].Recipients) != 0 {
		t.Errorf("Broadcast was asked %+v, want the audience", got)
	}
}

// A broadcast with no key is 422 and says the key is what makes it exactly-once, and the surface says
// so itself: it does not lean on the engine behind it remembering to.
func TestABroadcastWithNoKeyIs422AndNeverReachesTheEngine(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	rec := f.do("POST", "/admin/notifications/broadcast", "operator-secret",
		`{"tenant":{"kind":"workspace","id":"w1"},"audience":"all","topic":"t"}`)
	if rec.Code != http.StatusUnprocessableEntity || !strings.Contains(rec.Body.String(), "idem_key") || !strings.Contains(rec.Body.String(), "exactly-once") {
		t.Errorf("= %d %s, want a 422 that says idem_key is required for exactly-once", rec.Code, rec.Body.String())
	}
	if len(f.notifier.Broadcasted()) != 0 {
		t.Error("a broadcast with no key reached the engine")
	}
}

func TestTheEnginesRefusalOfABroadcastIs422InItsOwnWords(t *testing.T) {
	t.Parallel()
	for name, err := range map[string]error{
		"over the inline limit": fmt.Errorf("broadcast: %w", &domain.ValidationError{Problems: []domain.Problem{{Field: "Recipients",
			Message: "5000 recipients, the limit is 1000: name an audience selector instead", Err: domain.ErrTooManyRecipients}}}),
		"an unknown topic": fmt.Errorf("broadcast: %w: %q", domain.ErrUnknownTopic, "nope"),
		"no audience":      fmt.Errorf("broadcast: %w", domain.ErrAudienceUnsupported),
		"a rule it breaks": fmt.Errorf("broadcast: %w", &domain.ValidationError{Problems: []domain.Problem{{Field: "Priority", Message: "urgent is not a priority"}}}),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t)
			f.notifier.OnBroadcast = func(context.Context, domain.BroadcastRequest) (domain.BroadcastReceipt, error) {
				return domain.BroadcastReceipt{}, err
			}
			rec := f.do("POST", "/admin/notifications/broadcast", "operator-secret",
				`{"tenant":{"kind":"workspace","id":"w1"},"audience":"all","topic":"t","idem_key":"k"}`)
			if rec.Code != http.StatusUnprocessableEntity {
				t.Fatalf("status %d, want 422", rec.Code)
			}
			if !strings.Contains(rec.Body.String(), strings.SplitN(strings.TrimPrefix(err.Error(), "broadcast: "), "\n", 2)[0][:12]) {
				t.Errorf("body %s does not carry the engine's words %q", rec.Body.String(), err)
			}
		})
	}
}

// A realm is never part of a request (NR-009): a body that names one is refused, and the realm any call
// runs under is the operator's.
func TestNoOperatorBodyMayNameARealm(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	rec := f.do("POST", "/admin/notifications/broadcast", "operator-secret",
		`{"realm":"rival","tenant":{"kind":"workspace","id":"w1"},"audience":"all","topic":"t","idem_key":"k"}`)
	if rec.Code != 400 {
		t.Errorf("a body with a realm = %d, want 400", rec.Code)
	}
	if len(f.notifier.Broadcasted()) != 0 {
		t.Error("a broadcast that named a realm reached the engine")
	}
}

func TestReplayingADeadLetter(t *testing.T) {
	t.Parallel()
	const id = "7f1c7b0e-5a53-4f5c-9d1a-2b7a6b0f2f11"
	f := newFixture(t)
	if rec := f.do("POST", "/admin/notifications/dead-letters/"+id+"/replay", "operator-secret", ""); rec.Code != 204 {
		t.Fatalf("status %d %s, want 204", rec.Code, rec.Body.String())
	}
	if c := f.admin.Calls(); len(c) != 1 || c[0].Method != "ReplayDeadLetter" || c[0].ID.Realm != realm || c[0].Args[0] != id {
		t.Errorf("calls %+v, want the replay of %s under the operator's realm", c, id)
	}
	for name, tc := range map[string]struct {
		err  error
		want int
	}{
		"unknown": {fmt.Errorf("replay: %w", domain.ErrUnknownDeadLetter), 404},
		"parked":  {fmt.Errorf("replay: %w", domain.ErrNotReplayable), 409},
	} {
		f := newFixture(t)
		f.admin.onReplay = func(string) error { return tc.err }
		if rec := f.do("POST", "/admin/notifications/dead-letters/"+id+"/replay", "operator-secret", ""); rec.Code != tc.want {
			t.Errorf("%s dead letter = %d, want %d", name, rec.Code, tc.want)
		}
	}
}

func TestErasingARecipientReportsTheRowsRemoved(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.admin.onErase = func(domain.Identity) (map[string]int, error) {
		return map[string]int{"notifications": 3, "notify_idem": 2}, nil
	}
	rec := f.do("POST", "/admin/erasure", "operator-secret", `{"tenant":{"kind":"workspace","id":"w1"},"recipient":{"kind":"user","id":"alice"}}`)
	var got struct{ Removed map[string]int }
	decode(t, rec, &got)
	if rec.Code != 200 || got.Removed["notifications"] != 3 || got.Removed["notify_idem"] != 2 {
		t.Errorf("= %d %s, want the rows removed per table", rec.Code, rec.Body.String())
	}
	if c := f.admin.Calls(); len(c) != 1 || c[0].ID != alice {
		t.Errorf("Erase was asked about %+v, want %+v under the operator's realm", c, alice)
	}
	if rec := f.do("POST", "/admin/erasure", "operator-secret", `{"tenant":{"kind":"workspace","id":"w1"},"recipient":{"kind":"user"}}`); rec.Code != http.StatusUnprocessableEntity {
		t.Errorf("an incomplete recipient = %d, want 422", rec.Code)
	}
	if len(f.admin.Calls()) != 1 {
		t.Error("an incomplete recipient was erased")
	}
}

func TestATenantsDefaultsAreSetUnderTheOperatorsRealmAndThePathsTenant(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	rec := f.do("PUT", "/admin/tenants/workspace/w1/preferences", "operator-secret",
		`[{"topic":"a","channel":"email","enabled":false,"locked":true},{"topic":"b","channel":"in_app","enabled":true,"locked":false}]`)
	if rec.Code != 204 {
		t.Fatalf("status %d %s", rec.Code, rec.Body.String())
	}
	calls := f.admin.Calls()
	if len(calls) != 2 || calls[0].ID.Realm != realm {
		t.Fatalf("calls %+v, want two, under the operator's realm", calls)
	}
	if p := calls[0].Args[0].(domain.TenantPreference); p != (domain.TenantPreference{Tenant: domain.Tenant{Kind: "workspace", ID: "w1"}, Topic: "a", Channel: "email", Enabled: false, Locked: true}) {
		t.Errorf("first default = %+v", p)
	}

	f2 := newFixture(t)
	f2.admin.onTenant = func(p domain.TenantPreference) error {
		if p.Topic == "b" {
			return fmt.Errorf("set tenant preference: %w", &domain.ValidationError{Problems: []domain.Problem{{Field: "Enabled", Message: `topic "b" is essential`, Err: domain.ErrPreferenceLocked}}})
		}
		return nil
	}
	rec = f2.do("PUT", "/admin/tenants/workspace/w1/preferences", "operator-secret",
		`[{"topic":"a","channel":"email","enabled":true,"locked":false},{"topic":"b","channel":"email","enabled":false,"locked":false}]`)
	if rec.Code != 422 || !strings.Contains(rec.Body.String(), `"row":1`) || !strings.Contains(rec.Body.String(), "essential") {
		t.Errorf("= %d %s, want a 422 that names the row that was refused and why", rec.Code, rec.Body.String())
	}
}

// ---------------------------------------------------------------------------- health

func TestHealthzChecksNothing(t *testing.T) {
	t.Parallel()
	var ran bool
	h, err := httpapi.New(httpapi.Deps{Inbox: &inboxFake{}, Identify: func(*http.Request) (domain.Identity, error) { return domain.Identity{}, errors.New("no") },
		Ready: []httpapi.Check{{Name: "postgres", Run: func(context.Context) error { ran = true; return errors.New("down") }}}})
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/healthz", nil))
	if rec.Code != 200 || ran {
		t.Errorf("/healthz = %d (a check ran: %v), want 200 with no dependency checked: an orchestrator must not restart a healthy process because the database blinked", rec.Code, ran)
	}
}

func TestReadyzIs200WhenEveryCheckPassesAnd503NamingTheOneThatFails(t *testing.T) {
	t.Parallel()
	mk := func(results map[string]error) http.Handler {
		var checks []httpapi.Check
		for _, name := range []string{"postgres", "partitions"} {
			checks = append(checks, httpapi.Check{Name: name, Run: func(ctx context.Context) error {
				if _, ok := ctx.Deadline(); !ok {
					return errors.New("a check ran with no deadline: a hung database would hang the probe")
				}
				return results[name]
			}})
		}
		h, err := httpapi.New(httpapi.Deps{Inbox: &inboxFake{}, Identify: func(*http.Request) (domain.Identity, error) { return domain.Identity{}, errors.New("no") }, Ready: checks})
		if err != nil {
			t.Fatal(err)
		}
		return h
	}
	rec := httptest.NewRecorder()
	mk(nil).ServeHTTP(rec, httptest.NewRequest("GET", "/readyz", nil))
	if rec.Code != 200 {
		t.Fatalf("/readyz with every check passing = %d %s", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	mk(map[string]error{"partitions": errors.New("no partition for 2026-03 in notifications at 10.0.0.5")}).ServeHTTP(rec, httptest.NewRequest("GET", "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "partitions") {
		t.Fatalf("/readyz with partitions failing = %d %s, want 503 naming the check", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "10.0.0.5") || strings.Contains(rec.Body.String(), "postgres") {
		t.Errorf("body %s names a check that passed or shows the fault's own text", rec.Body.String())
	}
}

type recMaintenance struct {
	ports.Maintenance
	health domain.PartitionHealth
	err    error
}

func (m recMaintenance) CheckPartitions(context.Context, time.Time) (domain.PartitionHealth, error) {
	return m.health, m.err
}

// Ready is "PostgreSQL answers and this month's partitions exist": next month's missing partition is an
// alarm to provision it and not a reason to stop serving.
func TestThePartitionsCheckWantsThisMonthsPartitions(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 3, 15, 10, 0, 0, 0, time.UTC)
	clock := notifytest.NewClock(now)
	month := func(m time.Month) domain.PartitionGap {
		return domain.PartitionGap{Table: domain.TableNotifications, Month: time.Date(2026, m, 1, 0, 0, 0, 0, time.UTC)}
	}
	cases := []struct {
		name   string
		m      recMaintenance
		wantOK bool
	}{
		{"all there", recMaintenance{}, true},
		{"this month missing", recMaintenance{health: domain.PartitionHealth{Missing: []domain.PartitionGap{month(time.March)}}}, false},
		{"only next month missing", recMaintenance{health: domain.PartitionHealth{Missing: []domain.PartitionGap{month(time.April)}}}, true},
		{"this month missing for another table", recMaintenance{health: domain.PartitionHealth{Missing: []domain.PartitionGap{{Table: domain.TableDeliveries, Month: month(time.March).Month}}}}, false},
		{"the database does not answer", recMaintenance{err: errors.New("connection refused")}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			check := httpapi.PartitionsCheck(tc.m, clock)
			if check.Name != "partitions" {
				t.Errorf("the check is named %q, want partitions", check.Name)
			}
			err := check.Run(t.Context())
			if (err == nil) != tc.wantOK {
				t.Errorf("Run = %v, want ok = %v", err, tc.wantOK)
			}
		})
	}
}

// ---------------------------------------------------------------------------- webhooks

// Signature first, parsing never, until it verifies (NR-032, D14): a forged bounce would suppress a real
// address. An unverified body is refused with an empty 401 and is never handed to the parser, whatever it
// holds, even when it is not JSON at all.
func TestAWebhookWithABadSignatureIs401AndItsBodyIsNeverParsed(t *testing.T) {
	t.Parallel()
	for _, body := range []string{`{"type":"email.bounced","data":{"to":["victim@example.test"]}}`, `{not json`, ``} {
		f := newFixture(t)
		rec := f.do("POST", "/webhooks/email/resend", "", body, "X-Signature", "forged")
		if rec.Code != http.StatusUnauthorized || rec.Body.Len() != 0 {
			t.Errorf("body %q = %d %q, want 401 and an empty body", body, rec.Code, rec.Body.String())
		}
		if len(f.hook.handled) != 0 {
			t.Errorf("body %q was handed to the parser although it did not verify", body)
		}
		if len(f.hook.verified) != 1 || f.hook.verified[0] != body {
			t.Errorf("Verify saw %q, want the raw body %q", f.hook.verified, body)
		}
	}
	f := newFixture(t)
	if rec := f.do("POST", "/webhooks/email/resend", "", `{}`); rec.Code != http.StatusUnauthorized || len(f.hook.handled) != 0 {
		t.Errorf("a callback with no signature header = %d, handled %v, want 401 and nothing handled", rec.Code, f.hook.handled)
	}
}

func TestAVerifiedWebhookIsHandedItsBodyExactly(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	body := `{"type":"email.delivered","data":{"email_id":"pm-1"}}`
	rec := f.do("POST", "/webhooks/email/resend", "", body, "X-Signature", "good")
	if rec.Code != 204 {
		t.Fatalf("status %d %s, want 204", rec.Code, rec.Body.String())
	}
	if len(f.hook.handled) != 1 || f.hook.handled[0] != body {
		t.Errorf("handled %q, want the body byte for byte: a signature is over the bytes", f.hook.handled)
	}
}

func TestAWebhookForADeliveryNotRecordedYetAsksTheProviderToRetry(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.hook.handle = func([]byte) error { return fmt.Errorf("record: %w", domain.ErrUnknownDelivery) }
	rec := f.do("POST", "/webhooks/email/resend", "", `{}`, "X-Signature", "good")
	if rec.Code < 400 || rec.Code == http.StatusUnauthorized {
		t.Errorf("status %d, want a non-2xx the provider will retry on: the callback can beat the delivery's own record", rec.Code)
	}
	f.hook.handle = func([]byte) error { return errors.New("db down") }
	if rec := f.do("POST", "/webhooks/email/resend", "", `{}`, "X-Signature", "good"); rec.Code != 500 || strings.Contains(rec.Body.String(), "db down") {
		t.Errorf("a fault = %d %s, want 500 that does not show it", rec.Code, rec.Body.String())
	}
}

func TestAWebhookForAProviderNobodyRegisteredIs404(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	if rec := f.do("POST", "/webhooks/sms/someone", "", `{}`, "X-Signature", "good"); rec.Code != http.StatusNotFound {
		t.Errorf("status %d, want 404", rec.Code)
	}
	if len(f.hook.verified) != 0 {
		t.Error("another provider's callback was checked against this one's secret")
	}
}

func TestAnOversizedWebhookIsRefusedUnread(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	rec := f.do("POST", "/webhooks/email/resend", "", strings.Repeat("x", 2<<20), "X-Signature", "good")
	if rec.Code != http.StatusRequestEntityTooLarge || len(f.hook.handled) != 0 {
		t.Errorf("2 MiB = %d, handled %d, want 413 and nothing handled", rec.Code, len(f.hook.handled))
	}
}

// ---------------------------------------------------------------------------- construction

func TestNewNamesWhatItNeeds(t *testing.T) {
	t.Parallel()
	ident := func(*http.Request) (domain.Identity, error) { return domain.Identity{}, errors.New("no") }
	if _, err := httpapi.New(httpapi.Deps{}); err == nil || !strings.Contains(err.Error(), "Inbox") || !strings.Contains(err.Error(), "Identify") {
		t.Errorf("New with nothing = %v, want it to name Inbox and Identify", err)
	}
	op := func(*http.Request) (domain.Realm, error) { return realm, nil }
	if _, err := httpapi.New(httpapi.Deps{Inbox: &inboxFake{}, Identify: ident, Operator: op}); err == nil ||
		!strings.Contains(err.Error(), "Notifier") || !strings.Contains(err.Error(), "Admin") {
		t.Errorf("New with an operator and no Notifier or Admin = %v, want it to name both: the operator routes call them", err)
	}
}

// Routes whose dependency is not wired are not served: a host that runs no stream has no /stream.
func TestARouteWhoseDependencyIsAbsentIsNotServed(t *testing.T) {
	t.Parallel()
	h, err := httpapi.New(httpapi.Deps{Inbox: &inboxFake{}, Identify: func(*http.Request) (domain.Identity, error) { return alice, nil }})
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/notifications/stream", "/admin/erasure", "/webhooks/email/resend"} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest("POST", path, nil))
		if rec.Code != http.StatusNotFound && rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s = %d, want it not served", path, rec.Code)
		}
	}
}

// NR-009, D31. The realm of an operator's call travels with it, so a service that serves several realms
// routes the broadcast to the engine of the realm the operator's credential administers (auth.Route), and a
// body that names another realm is still refused.
func TestTheOperatorsRealmTravelsWithTheCallToTheNotifier(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	var seen domain.Realm
	var ok bool
	f.notifier.OnBroadcast = func(ctx context.Context, _ domain.BroadcastRequest) (domain.BroadcastReceipt, error) {
		seen, ok = auth.RealmFrom(ctx)
		return domain.BroadcastReceipt{BroadcastID: "b1", Applied: true}, nil
	}
	rec := f.do("POST", "/admin/notifications/broadcast", "operator-secret",
		`{"tenant":{"kind":"workspace","id":"w1"},"recipients":[{"kind":"user","id":"u1"}],"topic":"t","idem_key":"k"}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body)
	}
	if !ok || seen != realm {
		t.Fatalf("the notifier's context carried realm %q (set=%v), want the operator's, %q", seen, ok, realm)
	}
}

// D31, NR-028, NR-009. The REST twin of Catalog.PutAddresses, for operators and the e2e suite: the recipient
// is the body's (it is the operator's data), and the realm is the credential's.
func TestAnOperatorReplacesARecipientsAddressesInTheirRealm(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	rec := f.do("PUT", "/admin/recipients/addresses", "operator-secret", `{
		"tenant":{"kind":"workspace","id":"w1"},"recipient":{"kind":"user","id":"u1"},"channel":"email",
		"addresses":[{"value":"a@x.test","locale":"fr","timezone":"Europe/Paris"},{"value":"b@x.test"}]
	}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body)
	}
	calls := f.catalog.Calls()
	if len(calls) != 1 {
		t.Fatalf("%d catalog calls, want 1", len(calls))
	}
	want := domain.Identity{Realm: realm, Tenant: domain.Tenant{Kind: "workspace", ID: "w1"}, Recipient: domain.Recipient{Kind: "user", ID: "u1"}}
	if calls[0].ID != want || calls[0].Ch != "email" || len(calls[0].Addrs) != 2 || calls[0].Addrs[0].Locale != "fr" || calls[0].Addrs[0].Timezone != "Europe/Paris" {
		t.Fatalf("catalog was given %+v, want the recipient as sent, in the operator's realm %q", calls[0], realm)
	}
	var got struct {
		Addresses []struct{ Value, Locale, Timezone string } `json:"addresses"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil || len(got.Addresses) != 2 || got.Addresses[0].Value != "a@x.test" || got.Addresses[0].Locale != "fr" {
		t.Fatalf("body = %s, %v, want the stored set", rec.Body, err)
	}
}

func TestAnAddressBodyThatNamesARealmIsRefusedAndNothingIsStored(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	rec := f.do("PUT", "/admin/recipients/addresses", "operator-secret", `{
		"realm":"another","tenant":{"kind":"workspace","id":"w1"},"recipient":{"kind":"user","id":"u1"},"channel":"email","addresses":[{"value":"a@x.test"}]
	}`)
	if rec.Code != http.StatusBadRequest || len(f.catalog.Calls()) != 0 {
		t.Fatalf("status %d after %d calls, want 400 and none: the realm is the operator's, never the body's (NR-009)", rec.Code, len(f.catalog.Calls()))
	}
}

func TestAnAddressSetTheEngineRefusesIs422WithItsReason(t *testing.T) {
	t.Parallel()
	for name, c := range map[string]struct {
		err  error
		want string
	}{
		"a channel nobody registered":  {&domain.ValidationError{Problems: []domain.Problem{{Field: "Channel", Message: `channel "fax" is not registered`, Err: domain.ErrUnknownChannel}}}, "fax"},
		"an address the engine cannot": {&domain.ValidationError{Problems: []domain.Problem{{Field: "Addresses[0].Timezone", Message: "is not an IANA time zone"}}}, "Timezone"},
		"an identity with no tenant":   {domain.ErrEmptyIdentity, "empty component"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t)
			f.catalog.OnPutAddresses = func(context.Context, domain.Identity, domain.ChannelKind, []domain.Address) ([]domain.Address, error) {
				return nil, c.err
			}
			rec := f.do("PUT", "/admin/recipients/addresses", "operator-secret", `{"tenant":{"kind":"workspace","id":"w1"},"recipient":{"kind":"user","id":"u1"},"channel":"email","addresses":[{"value":"a"}]}`)
			if rec.Code != http.StatusUnprocessableEntity || !strings.Contains(rec.Body.String(), c.want) {
				t.Fatalf("status %d %s, want 422 saying %q", rec.Code, rec.Body, c.want)
			}
		})
	}
}

func TestAnAddressFaultIsReportedAndShownAsOne(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.catalog.OnPutAddresses = func(context.Context, domain.Identity, domain.ChannelKind, []domain.Address) ([]domain.Address, error) {
		return nil, errors.New(`ERROR: relation "recipient_addresses" does not exist`)
	}
	rec := f.do("PUT", "/admin/recipients/addresses", "operator-secret", `{"tenant":{"kind":"workspace","id":"w1"},"recipient":{"kind":"user","id":"u1"},"channel":"email","addresses":[]}`)
	if rec.Code != http.StatusInternalServerError || strings.Contains(rec.Body.String(), "relation") || len(f.reported()) != 1 {
		t.Fatalf("status %d %s with %d reports, want a 500 that says nothing of the fault, and the fault reported", rec.Code, rec.Body, len(f.reported()))
	}
}
