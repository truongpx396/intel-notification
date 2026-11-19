package app_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	notify "github.com/truongpx396/intel-notification"
	"github.com/truongpx396/intel-notification/app"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/notifytest"
	"github.com/truongpx396/intel-notification/ports"
)

// T042: the broadcast front door and the expander, with no database. The Store behind them
// is a recording stand-in for its three broadcast methods that behaves the way the real one
// is specified to (a lease hands out the broadcast at its cursor, a commit advances it, a
// lost lease writes nothing), so a test can show what was asked of it and in which order.
// What PostgreSQL must hold (the fence, the one transaction per page) is the integration
// suites', in adapters/driven/postgres and in this package.

// ---------------------------------------------------------------- stand-ins --

type createdBroadcast struct {
	realm domain.Realm
	req   domain.BroadcastRequest
}

type committedPage struct {
	job   domain.BroadcastJob
	page  []domain.PageItem
	next  string
	done  bool
	token string
}

// runawayLeases is how many leases a case may take before the stand-in has no more to give.
const runawayLeases = 200

// bcastStore is the broadcast half of ports.Store.
type bcastStore struct {
	ports.Store
	ev *events
	mu sync.Mutex

	created   []createdBroadcast
	receipt   domain.BroadcastReceipt
	createErr error

	pending    []*domain.BroadcastJob // the broadcasts with pages left, oldest first
	leaseErr   error
	leases     int
	ttls       []time.Duration
	leaseTimes []time.Time

	commits     []committedPage
	attempts    int
	commitErrs  map[int]error // by the number of the attempt, from 1
	afterCommit func(n int)   // after the nth page committed
}

func (s *bcastStore) CreateBroadcast(_ context.Context, realm domain.Realm, b domain.BroadcastRequest) (domain.BroadcastReceipt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ev.add("create")
	s.created = append(s.created, createdBroadcast{realm, b})
	return s.receipt, s.createErr
}

func (s *bcastStore) LeaseBroadcast(_ context.Context, ttl time.Duration) (*domain.BroadcastJob, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ev.add("lease")
	s.leases++
	s.ttls = append(s.ttls, ttl)
	s.leaseTimes = append(s.leaseTimes, time.Now())
	if s.leaseErr != nil {
		return nil, s.leaseErr
	}
	// A worker that loops on one broadcast for ever is a regression, and a test must say
	// so by failing and not by hanging: past this many leases there is no more work.
	if len(s.pending) == 0 || s.leases > runawayLeases {
		return nil, nil
	}
	j := *s.pending[0]
	j.LeaseToken = fmt.Sprintf("tok-%d", s.leases)
	return &j, nil
}

func (s *bcastStore) CommitBroadcastPage(_ context.Context, job domain.BroadcastJob, page []domain.PageItem, next string, done bool) error {
	s.mu.Lock()
	s.attempts++
	n := s.attempts
	err := s.commitErrs[n]
	drop := func() {
		s.pending = slices.DeleteFunc(s.pending, func(p *domain.BroadcastJob) bool { return p.ID == job.ID })
	}
	switch {
	case errors.Is(err, domain.ErrBroadcastLeaseLost):
		drop() // another worker has it now
	case err != nil:
	default:
		s.ev.add("commit")
		s.commits = append(s.commits, committedPage{job, slices.Clone(page), next, done, job.LeaseToken})
		if done {
			drop()
		} else {
			for _, p := range s.pending {
				if p.ID == job.ID {
					p.Cursor = next
				}
			}
		}
	}
	after, committed := s.afterCommit, len(s.commits)
	s.mu.Unlock()
	if err == nil && after != nil {
		after(committed)
	}
	return err
}

// countWaker records Wake into the ordered log.
type countWaker struct{ ev *events }

func (w countWaker) Wake() { w.ev.add("wake") }

// resolverFunc is a stand-in AudienceResolver with exactly the answers a case scripts.
type resolverFunc func(cursor string) ([]domain.Recipient, string, error)

func (f resolverFunc) Resolve(_ context.Context, _ domain.Realm, _ domain.Tenant, _, cursor string) ([]domain.Recipient, string, error) {
	return f(cursor)
}

// bcastFx is one case's engine and its stand-ins.
type bcastFx struct {
	t        *testing.T
	ev       *events
	cfg      notify.Config
	topics   *notifytest.Topics
	store    *bcastStore
	audience ports.AudienceResolver // nil: an engine that expands inline lists only
	errs     *errLog
}

const bcastTopic domain.Topic = "ingestion_complete"

func newBcastFx(t *testing.T) *bcastFx {
	t.Helper()
	ev := &events{}
	f := &bcastFx{
		t: t, ev: ev,
		cfg:    notify.Config{Realm: planRealm, StoreDSN: "x", RedisURL: "y"},
		topics: notifytest.NewTopics(),
		store:  &bcastStore{ev: ev, receipt: domain.BroadcastReceipt{BroadcastID: "b-1", Applied: true}, commitErrs: map[int]error{}},
		errs:   &errLog{},
	}
	f.topics.Register(planRealm, bcastTopic, domain.TopicDef{
		DefaultChannels: []domain.ChannelKind{"in_app", "email"},
		DefaultPriority: domain.PriorityWarning,
	})
	return f
}

func (f *bcastFx) deps() app.Deps {
	probe := notifytest.NewProbe()
	return app.Deps{
		Channels: notifytest.NewRegistry(
			notifytest.NewChannel("in_app", domain.ChannelCapabilities{InboxBacked: true}, probe),
			notifytest.NewChannel("email", domain.ChannelCapabilities{NeedsAddress: true}, probe)),
		Topics: f.topics, Prefs: notifytest.NewPreferences(), Quotas: notifytest.NewQuotaCounter(),
		Store: f.store, Queue: &recQueue{ev: f.ev}, Audience: f.audience, Clock: notifytest.NewClock(noon),
	}
}

func (f *bcastFx) notifier() *app.Notifier {
	f.t.Helper()
	n, err := app.NewNotifier(f.cfg, f.deps())
	if err != nil {
		f.t.Fatalf("NewNotifier: %v", err)
	}
	return n
}

func (f *bcastFx) expander(opts ...app.BroadcastOption) *app.BroadcastExpander {
	f.t.Helper()
	d := f.deps()
	d.Waker = countWaker{f.ev}
	opts = append([]app.BroadcastOption{app.WithBroadcastErrorHandler(f.errs.add)}, opts...)
	e, err := app.NewBroadcastExpander(f.cfg, d, opts...)
	if err != nil {
		f.t.Fatalf("NewBroadcastExpander: %v", err)
	}
	return e
}

func users(n int) []domain.Recipient {
	out := make([]domain.Recipient, n)
	for i := range out {
		out[i] = domain.Recipient{Kind: "user", ID: fmt.Sprintf("u%d", i+1)}
	}
	return out
}

func request(r ...domain.Recipient) domain.BroadcastRequest {
	return domain.BroadcastRequest{
		Tenant: planTenant, Recipients: r, Topic: bcastTopic, IdemKey: "launch:2026",
		Data: map[string]any{"name": "Atlas"}, Title: "Atlas is live", Body: "Come and see.",
		Payload: map[string]string{"url": "/launch"},
	}
}

func audienceRequest() domain.BroadcastRequest {
	b := request()
	b.Recipients, b.Audience = nil, "everyone"
	return b
}

// pend puts a broadcast with pages left in the store.
func (f *bcastFx) pend(id string, req domain.BroadcastRequest, cursor string) {
	f.store.pending = append(f.store.pending, &domain.BroadcastJob{ID: id, Realm: planRealm, Request: req, Cursor: cursor})
}

func notificationIDsOf(c committedPage) []string {
	out := make([]string, len(c.page))
	for i, it := range c.page {
		out[i] = it.Notification.Recipient.ID
	}
	return out
}

// ---------------------------------------------------------------- the door --

// Broadcast records the request under the engine's realm, which no request carries, and
// answers with what the store said. It returns once the row is recorded: no page is
// expanded on the caller's time (NR-020).
func TestBroadcastRecordsTheRequestUnderTheEnginesRealm(t *testing.T) {
	t.Parallel()
	f := newBcastFx(t)
	req := request(users(3)...)

	got, err := f.notifier().Broadcast(t.Context(), req)
	if err != nil || got != (domain.BroadcastReceipt{BroadcastID: "b-1", Applied: true}) {
		t.Fatalf("Broadcast = %+v, %v, want the store's receipt", got, err)
	}
	if len(f.store.created) != 1 {
		t.Fatalf("%d broadcasts recorded, want 1", len(f.store.created))
	}
	if c := f.store.created[0]; c.realm != planRealm || !reflect.DeepEqual(c.req, req) {
		t.Errorf("recorded %+v under %q, want the request as given under the engine's realm %q", c.req, c.realm, planRealm)
	}
	if f.store.leases != 0 || len(f.store.commits) != 0 {
		t.Errorf("Broadcast leased %d and committed %d pages: expansion is the worker's, off the request path", f.store.leases, len(f.store.commits))
	}
}

// A retried broadcast is one the store has already accepted, and the caller is told so.
func TestBroadcastReportsAReplayAndAStoreFault(t *testing.T) {
	t.Parallel()
	f := newBcastFx(t)
	f.store.receipt = domain.BroadcastReceipt{BroadcastID: "b-1", Applied: false}
	if got, err := f.notifier().Broadcast(t.Context(), request(users(1)...)); err != nil || got.Applied || got.BroadcastID != "b-1" {
		t.Fatalf("Broadcast of a key already accepted = %+v, %v, want b-1 and Applied false", got, err)
	}

	boom := errors.New("the database is down")
	f.store.createErr = boom
	if _, err := f.notifier().Broadcast(t.Context(), request(users(1)...)); !errors.Is(err, boom) {
		t.Fatalf("Broadcast = %v, want the store's fault", err)
	}
}

// A request that cannot be expanded is refused where the producer can fix it, and nothing
// is recorded: a durable row for it would only be retried by a worker, forever.
func TestBroadcastRefusesWhatCannotBeExpandedAndRecordsNothing(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		cfg  func(*notify.Config)
		req  domain.BroadcastRequest
		is   error // a sentinel the refusal must match; nil when any refusal will do
		why  string
	}{
		{"no idempotency key", nil, func() domain.BroadcastRequest { b := request(users(1)...); b.IdemKey = ""; return b }(), domain.ErrMissingIdem, ""},
		{"an inline list over the limit", func(c *notify.Config) { c.MaxInlineRecipients = 3 }, request(users(4)...), domain.ErrTooManyRecipients,
			"NR-020: the configured limit, not a constant"},
		{"an inline list over the default limit", nil, request(users(1001)...), domain.ErrTooManyRecipients,
			"an unset MaxInlineRecipients is 1000, the effective configuration's value"},
		{"an audience with no resolver", nil, audienceRequest(), domain.ErrAudienceUnsupported,
			"Deps.Audience is nil: the engine accepts inline recipients only"},
		{"a topic nobody registered", nil, func() domain.BroadcastRequest { b := request(users(1)...); b.Topic = "nobody_registered"; return b }(), domain.ErrUnknownTopic,
			"every page would fail to plan, and a worker would retry them forever"},
		{"no audience and no recipients", nil, request(), nil, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newBcastFx(t)
			if tc.cfg != nil {
				tc.cfg(&f.cfg)
			}
			_, err := f.notifier().Broadcast(t.Context(), tc.req)
			if err == nil {
				t.Fatalf("Broadcast succeeded, want a refusal: %s", tc.why)
			}
			if tc.is != nil && !errors.Is(err, tc.is) {
				t.Errorf("Broadcast = %v, want it to match %v", err, tc.is)
			}
			if len(f.store.created) != 0 {
				t.Errorf("%d broadcasts recorded after a refusal, want none", len(f.store.created))
			}
		})
	}
}

// The limit is inclusive, and a selector is not counted against it.
func TestBroadcastAcceptsWhatTheLimitAllows(t *testing.T) {
	t.Parallel()
	f := newBcastFx(t)
	f.cfg.MaxInlineRecipients = 3
	if _, err := f.notifier().Broadcast(t.Context(), request(users(3)...)); err != nil {
		t.Errorf("Broadcast of exactly the limit = %v, want it recorded", err)
	}

	f = newBcastFx(t)
	f.audience = notifytest.NewAudiences(2)
	if _, err := f.notifier().Broadcast(t.Context(), audienceRequest()); err != nil {
		t.Errorf("Broadcast of an audience, with a resolver = %v, want it recorded", err)
	}
	if len(f.store.created) != 1 {
		t.Errorf("%d broadcasts recorded, want 1", len(f.store.created))
	}
}

// ------------------------------------------------------------ the expander --

// An inline list is expanded a page at a time, in order. Each recipient's notification is
// keyed from the broadcast's key and the recipient, carries the request's content, and is
// planned as a notification of its own would be: the topic's default priority and every
// channel it enables. The cursor of the page is committed with it, and the last page says
// it is the last (NR-020, D7).
func TestAnInlineListIsExpandedAPageAtATimeUnderMemberKeys(t *testing.T) {
	t.Parallel()
	f := newBcastFx(t)
	req := request(users(5)...)
	f.pend("b-1", req, "")

	pages, err := f.expander(app.WithBroadcastPage(2)).Expand(t.Context())
	if err != nil || pages != 3 {
		t.Fatalf("Expand = %d, %v, want 3 pages", pages, err)
	}

	want := []struct {
		who  []string
		next string
		done bool
	}{{[]string{"u1", "u2"}, "2", false}, {[]string{"u3", "u4"}, "4", false}, {[]string{"u5"}, "5", true}}
	if len(f.store.commits) != len(want) {
		t.Fatalf("%d pages committed, want %d", len(f.store.commits), len(want))
	}
	for i, w := range want {
		c := f.store.commits[i]
		if got := notificationIDsOf(c); !slices.Equal(got, w.who) {
			t.Errorf("page %d holds %v, want %v", i+1, got, w.who)
		}
		if c.next != w.next || c.done != w.done {
			t.Errorf("page %d committed cursor %q done=%v, want %q done=%v", i+1, c.next, c.done, w.next, w.done)
		}
		if c.job.ID != "b-1" || c.token == "" {
			t.Errorf("page %d committed under job %q token %q, want the leased job and its token", i+1, c.job.ID, c.token)
		}
		for _, it := range c.page {
			n := it.Notification
			if n.IdemKey != domain.BroadcastMemberKey("launch:2026", n.Recipient) {
				t.Errorf("%s: key %q is not the member key of the broadcast's", n.Recipient.ID, n.IdemKey)
			}
			if n.Tenant != planTenant || n.Topic != bcastTopic || n.Title != req.Title || n.Body != req.Body ||
				!reflect.DeepEqual(n.Data, req.Data) || !reflect.DeepEqual(n.Payload, req.Payload) {
				t.Errorf("%s: notification %+v does not carry the request's tenant, topic and content", n.Recipient.ID, n)
			}
			if n.Priority != domain.PriorityWarning {
				t.Errorf("%s: priority %q, want the topic's default %q", n.Recipient.ID, n.Priority, domain.PriorityWarning)
			}
			id := domain.Identity{Realm: planRealm, Tenant: planTenant, Recipient: n.Recipient}
			if it.Plan.Shard != domain.ShardFor(id, 16) || !it.Plan.InboxVisible || len(it.Plan.Deliveries) != 2 {
				t.Errorf("%s: plan %+v, want the recipient's shard, a visible inbox row and both channels", n.Recipient.ID, it.Plan)
			}
		}
	}
}

// A recipient's key does not depend on where in the audience they fall, nor on how the
// audience was cut into pages: a retry that pages differently still finds the same keys,
// so it still notifies nobody twice (NR-020).
func TestAMemberKeyDoesNotDependOnThePageThatCarriesIt(t *testing.T) {
	t.Parallel()
	keysFor := func(page int) map[string]string {
		f := newBcastFx(t)
		f.pend("b-1", request(users(5)...), "")
		if _, err := f.expander(app.WithBroadcastPage(page)).Expand(t.Context()); err != nil {
			t.Fatalf("Expand with pages of %d: %v", page, err)
		}
		keys := map[string]string{}
		for _, c := range f.store.commits {
			for _, it := range c.page {
				keys[it.Notification.Recipient.ID] = it.Notification.IdemKey
			}
		}
		return keys
	}
	base := keysFor(1)
	if len(base) != 5 {
		t.Fatalf("expanded %d recipients, want 5", len(base))
	}
	for _, page := range []int{2, 3, 5, 100} {
		if got := keysFor(page); !reflect.DeepEqual(got, base) {
			t.Errorf("pages of %d: keys %v, want those of pages of 1: %v", page, got, base)
		}
	}
}

// An audience is resolved from the job's own cursor, and the resolver's next cursor is
// what is committed. A worker that picks a broadcast up at a cursor asks for that page
// and no earlier one: that is what "resumes at the next page" means (NR-020).
func TestAnAudienceIsResolvedFromTheJobsCursorAndItsNextCursorIsCommitted(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		start string
		asked []string
		pages [][]string
		next  []string
	}{
		{"from the beginning", "", []string{"", "2", "4"}, [][]string{{"u1", "u2"}, {"u3", "u4"}, {"u5"}}, []string{"2", "4", ""}},
		{"resumed at the second page", "2", []string{"2", "4"}, [][]string{{"u3", "u4"}, {"u5"}}, []string{"4", ""}},
		{"resumed at the last page", "4", []string{"4"}, [][]string{{"u5"}}, []string{""}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newBcastFx(t)
			aud := notifytest.NewAudiences(2)
			aud.Set(planRealm, planTenant, "everyone", users(5)...)
			f.audience = aud
			f.pend("b-1", audienceRequest(), tc.start)

			if _, err := f.expander().Expand(t.Context()); err != nil {
				t.Fatalf("Expand: %v", err)
			}
			var asked []string
			for _, c := range aud.Calls() {
				asked = append(asked, c.Cursor)
				if c.Realm != planRealm || c.Tenant != planTenant || c.Audience != "everyone" {
					t.Errorf("resolver asked %+v, want the job's realm, tenant and selector", c)
				}
			}
			if !slices.Equal(asked, tc.asked) {
				t.Errorf("resolver asked for cursors %q, want %q", asked, tc.asked)
			}
			if len(f.store.commits) != len(tc.pages) {
				t.Fatalf("%d pages committed, want %d", len(f.store.commits), len(tc.pages))
			}
			for i, c := range f.store.commits {
				last := i == len(tc.pages)-1
				if !slices.Equal(notificationIDsOf(c), tc.pages[i]) || c.next != tc.next[i] || c.done != last {
					t.Errorf("page %d = %v next %q done=%v, want %v next %q done=%v",
						i+1, notificationIDsOf(c), c.next, c.done, tc.pages[i], tc.next[i], last)
				}
			}
		})
	}
}

// A resolver may end on an empty page, or hand back an empty one before more: a page with
// no one in it still advances the cursor, and only the empty cursor ends the broadcast.
func TestAnEmptyPageAdvancesTheCursorAndOnlyTheEmptyCursorEndsIt(t *testing.T) {
	t.Parallel()
	f := newBcastFx(t)
	script := map[string]struct {
		page []domain.Recipient
		next string
	}{
		"":   {nil, "c1"},
		"c1": {users(1), "c2"},
		"c2": {nil, ""},
	}
	f.audience = resolverFunc(func(cursor string) ([]domain.Recipient, string, error) {
		s, ok := script[cursor]
		if !ok {
			return nil, "", fmt.Errorf("unexpected cursor %q", cursor)
		}
		return s.page, s.next, nil
	})
	f.pend("b-1", audienceRequest(), "")

	if pages, err := f.expander().Expand(t.Context()); err != nil || pages != 3 {
		t.Fatalf("Expand = %d, %v, want 3 pages", pages, err)
	}
	for i, want := range []struct {
		n    int
		next string
		done bool
	}{{0, "c1", false}, {1, "c2", false}, {0, "", true}} {
		c := f.store.commits[i]
		if len(c.page) != want.n || c.next != want.next || c.done != want.done {
			t.Errorf("page %d = %d items next %q done=%v, want %d next %q done=%v", i+1, len(c.page), c.next, c.done, want.n, want.next, want.done)
		}
	}
}

// A fault in resolving a page commits nothing of it and ends the pass, so a fault that
// repeats does not spin: the next pass leases the broadcast again, at the same cursor.
func TestAFaultInAPageCommitsNothingAndEndsThePass(t *testing.T) {
	t.Parallel()
	boom := errors.New("the directory timed out")
	f := newBcastFx(t)
	aud := notifytest.NewAudiences(2)
	aud.Set(planRealm, planTenant, "everyone", users(3)...)
	aud.FailNext(boom)
	f.audience = aud
	f.pend("b-1", audienceRequest(), "")
	f.pend("b-2", audienceRequest(), "")

	pages, err := f.expander().Expand(t.Context())
	if !errors.Is(err, boom) || pages != 0 {
		t.Fatalf("Expand = %d, %v, want 0 pages and the directory's fault", pages, err)
	}
	if len(f.store.commits) != 0 || f.store.leases != 1 {
		t.Errorf("%d pages committed over %d leases, want none and one: a fault ends the pass", len(f.store.commits), f.store.leases)
	}

	// The next pass finds the broadcast where it was left.
	if _, err := f.expander().Expand(t.Context()); err != nil {
		t.Fatalf("the next pass = %v, want it to expand the broadcast", err)
	}
	if first := f.store.commits[0]; first.job.ID != "b-1" || first.job.Cursor != "" {
		t.Errorf("the next pass began with %q at cursor %q, want b-1 at its start", first.job.ID, first.job.Cursor)
	}
}

// A fault in planning one recipient fails the page and commits nothing of it, rather than a
// page committed short: the topic was withdrawn after the broadcast was recorded, and sending
// the recipients planned before the fault would notify some of an audience and never the rest.
func TestAFaultInPlanningFailsThePageAndCommitsNothing(t *testing.T) {
	t.Parallel()
	f := newBcastFx(t)
	req := request(users(3)...)
	req.Topic = "withdrawn_since" // the Notifier checked the topic when it recorded the broadcast
	f.pend("b-1", req, "")

	pages, err := f.expander().Expand(t.Context())
	if !errors.Is(err, domain.ErrUnknownTopic) || pages != 0 {
		t.Fatalf("Expand = %d, %v, want 0 pages and ErrUnknownTopic", pages, err)
	}
	if len(f.store.commits) != 0 || f.store.leases != 1 {
		t.Errorf("%d pages committed over %d leases, want none and one: a fault ends the pass", len(f.store.commits), f.store.leases)
	}
}

// A commit that fails is a fault too, and it is not counted as a page.
func TestAFailedCommitIsAFaultAndNotAPage(t *testing.T) {
	t.Parallel()
	boom := errors.New("the commit was refused")
	f := newBcastFx(t)
	f.store.commitErrs[1] = boom
	f.pend("b-1", request(users(4)...), "")

	pages, err := f.expander(app.WithBroadcastPage(2)).Expand(t.Context())
	if !errors.Is(err, boom) || pages != 0 || f.store.leases != 1 {
		t.Fatalf("Expand = %d pages, %v over %d leases, want none, the fault and a single lease", pages, err, f.store.leases)
	}
}

// A lease lost is another worker's page, not a fault of this one: it does not stop the
// pass, is not counted, and is not retried by the worker that lost it.
func TestALostLeaseIsNotAFaultAndThePassGoesOn(t *testing.T) {
	t.Parallel()
	f := newBcastFx(t)
	f.store.commitErrs[1] = domain.ErrBroadcastLeaseLost
	f.pend("b-1", request(users(1)...), "")
	f.pend("b-2", request(users(1)...), "")

	pages, err := f.expander().Expand(t.Context())
	if err != nil || pages != 1 {
		t.Fatalf("Expand = %d, %v, want 1 page and no error", pages, err)
	}
	if len(f.store.commits) != 1 || f.store.commits[0].job.ID != "b-2" {
		t.Errorf("committed %+v, want b-2's page only", f.store.commits)
	}
	if errs := f.errs.all(); len(errs) != 0 {
		t.Errorf("reported %v, want nothing: losing a lease is how a lapsed worker is told", errs)
	}
}

// A resolver that hands back the cursor it was given with a page would be asked for that
// page forever. It is refused as a fault, and nothing is committed.
func TestAResolverThatDoesNotAdvanceIsAFaultAndNotALoop(t *testing.T) {
	t.Parallel()
	f := newBcastFx(t)
	f.audience = resolverFunc(func(cursor string) ([]domain.Recipient, string, error) { return users(1), cursor, nil })
	f.pend("b-1", audienceRequest(), "c7")

	pages, err := f.expander().Expand(t.Context())
	if err == nil || pages != 0 || len(f.store.commits) != 0 {
		t.Fatalf("Expand = %d, %v with %d commits, want a fault and nothing committed", pages, err, len(f.store.commits))
	}
	if f.store.leases != 1 {
		t.Errorf("%d leases, want one: the pass ends at the fault", f.store.leases)
	}
}

// What a host's resolver returns is host data. A recipient with an empty component is
// skipped and reported, and the rest of the page goes out: one bad row must not hold a
// tenant's whole broadcast for ever, nor be silently lost.
func TestARecipientWithAnEmptyComponentIsSkippedAndReported(t *testing.T) {
	t.Parallel()
	f := newBcastFx(t)
	f.audience = resolverFunc(func(string) ([]domain.Recipient, string, error) {
		return []domain.Recipient{{Kind: "user", ID: "u1"}, {Kind: "user", ID: ""}, {Kind: "", ID: "u3"}, {Kind: "user", ID: "u4"}}, "", nil
	})
	f.pend("b-1", audienceRequest(), "")

	if pages, err := f.expander().Expand(t.Context()); err != nil || pages != 1 {
		t.Fatalf("Expand = %d, %v, want the page committed", pages, err)
	}
	if got := notificationIDsOf(f.store.commits[0]); !slices.Equal(got, []string{"u1", "u4"}) {
		t.Errorf("page holds %v, want the two valid recipients", got)
	}
	errs := f.errs.all()
	if len(errs) != 1 || !errors.Is(errs[0], domain.ErrEmptyIdentity) {
		t.Fatalf("reported %v, want one report that matches ErrEmptyIdentity", errs)
	}
}

// A broadcast is expanded in the realm it was recorded in, which the engine that recorded
// it may not be the one that expands: the topic is looked up there, and the identity of
// every recipient is under it.
func TestAPageIsPlannedInTheRealmOfTheBroadcast(t *testing.T) {
	t.Parallel()
	f := newBcastFx(t)
	f.topics.Register("product-b", "only_in_b", domain.TopicDef{
		DefaultChannels: []domain.ChannelKind{"in_app"}, DefaultPriority: domain.PriorityInfo,
	})
	req := request(users(1)...)
	req.Topic = "only_in_b"
	f.store.pending = append(f.store.pending, &domain.BroadcastJob{ID: "b-9", Realm: "product-b", Request: req})

	if pages, err := f.expander().Expand(t.Context()); err != nil || pages != 1 {
		t.Fatalf("Expand = %d, %v, want the page committed: the topic is registered in the broadcast's realm", pages, err)
	}
	it := f.store.commits[0].page[0]
	if want := domain.ShardFor(domain.Identity{Realm: "product-b", Tenant: planTenant, Recipient: it.Notification.Recipient}, 16); it.Plan.Shard != want {
		t.Errorf("shard %d, want %d: the identity is under the broadcast's realm", it.Plan.Shard, want)
	}
}

// A broadcast that names an audience, met by an engine with no resolver, is a fault of the
// wiring and says so; nothing is committed under a guess.
func TestAJobWithAnAudienceAndNoResolverIsAFault(t *testing.T) {
	t.Parallel()
	f := newBcastFx(t)
	f.pend("b-1", audienceRequest(), "")

	_, err := f.expander().Expand(t.Context())
	if !errors.Is(err, domain.ErrAudienceUnsupported) || len(f.store.commits) != 0 {
		t.Fatalf("Expand = %v with %d commits, want ErrAudienceUnsupported and none", err, len(f.store.commits))
	}
}

// The dispatcher of this process is woken after each page's commit, not at the end of the
// pass: the first page's deliveries are due at once, and a broadcast of a hundred thousand
// would otherwise leave them waiting for all of it. A pass that committed nothing wakes no
// one (D22).
func TestTheDispatcherIsWokenAfterEachPageAndNeverForNone(t *testing.T) {
	t.Parallel()
	f := newBcastFx(t)
	f.pend("b-1", request(users(5)...), "")
	if _, err := f.expander(app.WithBroadcastPage(2)).Expand(t.Context()); err != nil {
		t.Fatalf("Expand: %v", err)
	}
	want := []string{"lease", "commit", "wake", "lease", "commit", "wake", "lease", "commit", "wake", "lease"}
	if got := f.ev.all(); !slices.Equal(got, want) {
		t.Errorf("events %v, want %v", got, want)
	}

	idle := newBcastFx(t)
	if pages, err := idle.expander().Expand(t.Context()); err != nil || pages != 0 {
		t.Fatalf("Expand with nothing to do = %d, %v", pages, err)
	}
	if got := idle.ev.all(); !slices.Equal(got, []string{"lease"}) {
		t.Errorf("events %v on an idle pass, want one lease and no wake", got)
	}
}

// A worker told to stop does so between pages, without error: it never leaves a page
// half done, because the commit is the page's only write, and the next page is for
// whoever runs next (NR-020).
func TestAPassStopsBetweenPagesWhenItsContextEnds(t *testing.T) {
	t.Parallel()
	f := newBcastFx(t)
	f.pend("b-1", request(users(6)...), "")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	f.store.afterCommit = func(n int) {
		if n == 1 {
			cancel()
		}
	}

	pages, err := f.expander(app.WithBroadcastPage(2)).Expand(ctx)
	if err != nil || pages != 1 {
		t.Fatalf("Expand = %d, %v, want the one page committed and no error", pages, err)
	}
	if f.store.leases != 1 {
		t.Errorf("%d leases, want one: no page is taken after the context ended", f.store.leases)
	}
}

// The lease is the one it was configured with: it is what keeps another worker off a page
// while this one expands it, so it has to outlast the page.
func TestThePageLeaseIsTheConfiguredLength(t *testing.T) {
	t.Parallel()
	f := newBcastFx(t)
	f.pend("b-1", request(users(1)...), "")
	if _, err := f.expander(app.WithBroadcastLease(90 * time.Second)).Expand(t.Context()); err != nil {
		t.Fatalf("Expand: %v", err)
	}
	if want := []time.Duration{90 * time.Second, 90 * time.Second}; !slices.Equal(f.store.ttls, want) {
		t.Errorf("leased for %v, want %v", f.store.ttls, want)
	}
}

// ---------------------------------------------------------------- the loop --

// Run expands at once and then every interval, and returns nil when its context ends.
func TestBroadcastRunExpandsAtOnceAndThenEveryInterval(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newBcastFx(t)
		e := f.expander(app.WithBroadcastInterval(7 * time.Second))
		start := time.Now()

		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- e.Run(ctx) }()
		time.Sleep(15 * time.Second)
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("Run = %v, want nil when its context ends", err)
		}

		var offsets []time.Duration
		for _, at := range f.store.leaseTimes {
			offsets = append(offsets, at.Sub(start))
		}
		if want := []time.Duration{0, 7 * time.Second, 14 * time.Second}; !slices.Equal(offsets, want) {
			t.Fatalf("passes began at %v, want %v: at once, then every interval", offsets, want)
		}
	})
}

// A pass that failed is reported and the next one still runs: one bad pass is not the end
// of the job.
func TestBroadcastRunReportsAFailedPassAndGoesOn(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newBcastFx(t)
		boom := errors.New("the database hiccuped")
		f.store.leaseErr = boom
		e := f.expander(app.WithBroadcastInterval(time.Second))

		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- e.Run(ctx) }()
		time.Sleep(1500 * time.Millisecond)
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("Run = %v, want nil", err)
		}
		if errs := f.errs.all(); len(errs) != 2 || !errors.Is(errs[0], boom) {
			t.Fatalf("reported %v, want the fault of each of the two passes", errs)
		}
	})
}

// A context that has already ended is a worker told to stop before it started: no pass.
func TestBroadcastRunStopsAtOnceWhenItsContextHasEnded(t *testing.T) {
	t.Parallel()
	f := newBcastFx(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := f.expander().Run(ctx); err != nil {
		t.Fatalf("Run = %v, want nil", err)
	}
	if f.store.leases != 0 {
		t.Fatalf("%d leases after the context ended", f.store.leases)
	}
}

// ------------------------------------------------------------- the wiring --

// An expander names every port it needs and is not built from a configuration that does
// not validate, nor from an option that could not work. The audience resolver is the one
// port it may go without.
func TestNewBroadcastExpanderRefusesWhatCouldNotWork(t *testing.T) {
	t.Parallel()
	valid := notify.Config{Realm: planRealm, StoreDSN: "x", RedisURL: "y"}
	good := newBcastFx(t).deps()
	cases := []struct {
		name string
		cfg  notify.Config
		deps app.Deps
		opts []app.BroadcastOption
		want []string
	}{
		{"no ports", valid, app.Deps{}, nil, []string{"Deps.Channels", "Deps.Prefs", "Deps.Quotas", "Deps.Store", "Deps.Topics"}},
		{"a configuration that does not validate", notify.Config{StoreDSN: "x", RedisURL: "y"}, good, nil, []string{"Realm"}},
		{"a page of no one", valid, good, []app.BroadcastOption{app.WithBroadcastPage(0)}, []string{"page"}},
		{"a lease that has already ended", valid, good, []app.BroadcastOption{app.WithBroadcastLease(0)}, []string{"lease"}},
		{"an interval of no time", valid, good, []app.BroadcastOption{app.WithBroadcastInterval(0)}, []string{"interval"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := app.NewBroadcastExpander(tc.cfg, tc.deps, tc.opts...)
			if err == nil {
				t.Fatal("NewBroadcastExpander succeeded")
			}
			for _, w := range tc.want {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("error %q does not name %q", err, w)
				}
			}
		})
	}

	d := good
	d.Audience = nil
	if _, err := app.NewBroadcastExpander(valid, d); err != nil {
		t.Errorf("NewBroadcastExpander with no resolver = %v, want it built: it expands inline lists", err)
	}
}
