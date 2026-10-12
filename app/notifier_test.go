package app_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	notify "github.com/truongpx396/intel-notification"
	"github.com/truongpx396/intel-notification/app"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/notifytest"
	"github.com/truongpx396/intel-notification/ports"
)

// T022a: what NotifierContract does not reach, with no database. The ports behind
// the Notifier are recording stand-ins that say what was asked of them and in which
// order, so a test can show what was NOT called as well. A stand-in Store in a test
// file is not the fake Store the kit refuses to hold: it asserts call order and
// arguments, and claims nothing about leases or row-level security.

// ---------------------------------------------------------------- stand-ins --

type events struct {
	mu   sync.Mutex
	list []string
}

func (e *events) add(s string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.list = append(e.list, s)
}

func (e *events) all() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.Clone(e.list)
}

// recStore records what PersistAndEnqueue and Status were asked.
type recStore struct {
	ports.Store // the broadcast and callback methods are never called here
	ev          *events
	receipt     domain.Receipt
	err         error
	status      domain.NotificationStatus

	id   domain.Identity
	n    domain.Notification
	tx   ports.Tx
	plan domain.DeliveryPlan
}

func (s *recStore) PersistAndEnqueue(_ context.Context, tx ports.Tx, id domain.Identity, n domain.Notification, plan domain.DeliveryPlan) (domain.Receipt, error) {
	s.ev.add("persist")
	s.id, s.n, s.tx, s.plan = id, n, tx, plan
	return s.receipt, s.err
}

func (s *recStore) Status(_ context.Context, id domain.Identity, idemKey string) (domain.NotificationStatus, error) {
	s.ev.add("status:" + idemKey)
	s.id = id
	return s.status, s.err
}

// recQueue records Cancel.
type recQueue struct {
	ports.Queue
	ev      *events
	receipt domain.CancelReceipt
	id      domain.Identity
	key     string
}

func (q *recQueue) Cancel(_ context.Context, id domain.Identity, idemKey string) (domain.CancelReceipt, error) {
	q.ev.add("cancel:" + idemKey)
	q.id, q.key = id, idemKey
	return q.receipt, nil
}

// recPreCheck records Seen and Remember, and answers as it is told to.
type recPreCheck struct {
	ev          *events
	hitID       string // "" is a miss
	seenErr     error
	rememberErr error

	seen       []string
	remembered []notifytest.PreCheckWrite
}

func (p *recPreCheck) Seen(_ context.Context, key string) (string, bool, error) {
	p.ev.add("seen")
	p.seen = append(p.seen, key)
	if p.seenErr != nil {
		return "", false, p.seenErr
	}
	return p.hitID, p.hitID != "", nil
}

func (p *recPreCheck) Remember(_ context.Context, key, id string, ttl time.Duration) error {
	p.ev.add("remember")
	p.remembered = append(p.remembered, notifytest.PreCheckWrite{Key: key, NotificationID: id, TTL: ttl})
	return p.rememberErr
}

// noteFx is one case's Notifier and its stand-ins.
type noteFx struct {
	t      *testing.T
	ev     *events
	cfg    notify.Config
	topics *notifytest.Topics
	prefs  *notifytest.Preferences
	store  *recStore
	queue  *recQueue
	pre    *recPreCheck
	noPre  bool
}

func newNoteFx(t *testing.T) *noteFx {
	t.Helper()
	ev := &events{}
	f := &noteFx{
		t: t, ev: ev,
		cfg:    notify.Config{Realm: planRealm, StoreDSN: "x", RedisURL: "y", PreCheckTTL: 6 * time.Hour},
		topics: notifytest.NewTopics(),
		prefs:  notifytest.NewPreferences(),
		store:  &recStore{ev: ev, receipt: domain.Receipt{NotificationID: "n-1", IdemKey: "k1", Applied: true, InboxVisible: true}},
		queue:  &recQueue{ev: ev},
		pre:    &recPreCheck{ev: ev},
	}
	f.topics.Register(planRealm, "ingestion_complete", domain.TopicDef{
		DefaultChannels: []domain.ChannelKind{"in_app", "email"},
		DefaultPriority: domain.PriorityWarning,
	})
	return f
}

func (f *noteFx) build() *app.Notifier {
	f.t.Helper()
	probe := notifytest.NewProbe()
	reg := notifytest.NewRegistry(
		notifytest.NewChannel("in_app", domain.ChannelCapabilities{InboxBacked: true}, probe),
		notifytest.NewChannel("email", domain.ChannelCapabilities{NeedsAddress: true}, probe))
	d := app.Deps{
		Channels: reg, Topics: f.topics, Prefs: f.prefs, Quotas: notifytest.NewQuotaCounter(),
		Store: f.store, Queue: f.queue, Clock: notifytest.NewClock(noon),
	}
	if !f.noPre {
		d.PreCheck = f.pre
	}
	n, err := app.NewNotifier(f.cfg, d)
	if err != nil {
		f.t.Fatalf("NewNotifier: %v", err)
	}
	return n
}

func (f *noteFx) note(key string) domain.Notification {
	return domain.Notification{Tenant: planTenant, Recipient: planUser, Topic: "ingestion_complete", IdemKey: key}
}

// --------------------------------------------------------------- construction --

// NewNotifier names every port it needs and is not given, and refuses a
// configuration that does not validate: a realm that is not a realm would key
// every row of the engine wrongly (D1).
func TestNewNotifierNamesEveryMissingPortAndRefusesABadConfig(t *testing.T) {
	t.Parallel()
	_, err := app.NewNotifier(notify.Config{Realm: planRealm, StoreDSN: "x", RedisURL: "y"}, app.Deps{})
	if err == nil {
		t.Fatal("NewNotifier with no ports succeeded")
	}
	for _, port := range []string{"Deps.Channels", "Deps.Prefs", "Deps.Queue", "Deps.Quotas", "Deps.Store", "Deps.Topics"} {
		if !strings.Contains(err.Error(), port) {
			t.Errorf("error %q does not name %s", err, port)
		}
	}

	f := newNoteFx(t)
	for name, cfg := range map[string]notify.Config{
		"no realm":            {StoreDSN: "x", RedisURL: "y"},
		"a realm with spaces": {Realm: "Not A Realm", StoreDSN: "x", RedisURL: "y"},
	} {
		f.cfg = cfg
		probe := notifytest.NewProbe()
		_, err := app.NewNotifier(cfg, app.Deps{
			Channels: notifytest.NewRegistry(notifytest.NewChannel("in_app", domain.ChannelCapabilities{}, probe)),
			Topics:   f.topics, Prefs: f.prefs, Quotas: notifytest.NewQuotaCounter(), Store: f.store, Queue: f.queue,
		})
		var ce *notify.ConfigError
		if !errors.As(err, &ce) {
			t.Errorf("%s: NewNotifier = %v, want a *notify.ConfigError", name, err)
		}
	}
}

// ---------------------------------------------------------------- refusals --

// A notification, a cancel or a status request that cannot be right is refused
// before any port is touched: not the pre-check, not the topic registry, not the
// store. A control Notify first shows the Notifier works.
func TestRequestsThatCannotBeRightTouchNoPort(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		do   func(t *testing.T, n *app.Notifier, f *noteFx) error
		is   error
	}{
		{"Notify without an idempotency key", func(t *testing.T, n *app.Notifier, f *noteFx) error {
			t.Helper()
			_, err := n.Notify(t.Context(), f.note(""))
			return err
		}, domain.ErrMissingIdem},
		{"Notify without a tenant", func(t *testing.T, n *app.Notifier, f *noteFx) error {
			t.Helper()
			nt := f.note("k1")
			nt.Tenant = domain.Tenant{}
			_, err := n.Notify(t.Context(), nt)
			return err
		}, domain.ErrEmptyIdentity},
		{"NotifyTx without a transaction", func(t *testing.T, n *app.Notifier, f *noteFx) error {
			t.Helper()
			_, err := n.NotifyTx(t.Context(), nil, f.note("k1"))
			return err
		}, nil},
		{"NotifyTx with an invalid notification", func(t *testing.T, n *app.Notifier, f *noteFx) error {
			t.Helper()
			_, err := n.NotifyTx(t.Context(), struct{}{}, f.note(""))
			return err
		}, domain.ErrMissingIdem},
		{"Cancel without a key", func(t *testing.T, n *app.Notifier, f *noteFx) error {
			t.Helper()
			_, err := n.Cancel(t.Context(), domain.CancelRequest{Tenant: planTenant, Recipient: planUser})
			return err
		}, domain.ErrMissingIdem},
		{"Cancel without a recipient", func(t *testing.T, n *app.Notifier, f *noteFx) error {
			t.Helper()
			_, err := n.Cancel(t.Context(), domain.CancelRequest{Tenant: planTenant, IdemKey: "k1"})
			return err
		}, domain.ErrEmptyIdentity},
		{"Status without a tenant", func(t *testing.T, n *app.Notifier, f *noteFx) error {
			t.Helper()
			_, err := n.Status(t.Context(), domain.StatusRequest{Recipient: planUser, IdemKey: "k1"})
			return err
		}, domain.ErrEmptyIdentity},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newNoteFx(t)
			n := f.build()
			if _, err := n.Notify(t.Context(), f.note("control")); err != nil {
				t.Fatalf("control Notify: %v", err)
			}
			before := len(f.ev.all())

			err := tc.do(t, n, f)
			if err == nil || (tc.is != nil && !errors.Is(err, tc.is)) {
				t.Fatalf("got %v, want an error that is %v", err, tc.is)
			}
			if after := f.ev.all(); len(after) != before {
				t.Fatalf("a refused request touched ports: %v", after[before:])
			}
		})
	}
}

// An unregistered topic is refused with ErrUnknownTopic, and nothing is written or
// remembered for it (NR-017).
func TestAnUnknownTopicWritesAndRemembersNothing(t *testing.T) {
	t.Parallel()
	f := newNoteFx(t)
	n := f.build()
	nt := f.note("k1")
	nt.Topic = "never_registered"
	if _, err := n.Notify(t.Context(), nt); !errors.Is(err, domain.ErrUnknownTopic) {
		t.Fatalf("Notify = %v, want ErrUnknownTopic", err)
	}
	if got := f.ev.all(); !slices.Equal(got, []string{"seen"}) {
		t.Fatalf("events %v, want only the pre-check's read: nothing persisted, nothing remembered", got)
	}
}

// ------------------------------------------------------------------- pre-check --

// The pre-check is read before the transaction and written after it, in the durable
// guard's key space, for the configured ttl (D18). The order is the guarantee: an
// entry written first outlives a commit that failed, and swallows the retry.
func TestPreCheckIsReadBeforeAndWrittenAfterTheCommit(t *testing.T) {
	t.Parallel()
	f := newNoteFx(t)
	n := f.build()
	r, err := n.Notify(t.Context(), f.note("invoice:42:overdue"))
	if err != nil || !r.Applied {
		t.Fatalf("Notify = %+v, %v", r, err)
	}
	if got := f.ev.all(); !slices.Equal(got, []string{"seen", "persist", "remember"}) {
		t.Fatalf("events %v, want [seen persist remember]: the entry is written after the commit", got)
	}

	// The frozen vector (T008): the key is the one the durable guard's identity
	// hashes to, for this tenant and recipient.
	const want = "notify:applied:aisat:2006f991ca0e66c48051540129023c7fc5c3f7f77455b4f8a3b70bade98c8df1"
	if len(f.pre.seen) != 1 || f.pre.seen[0] != want {
		t.Errorf("read key %v, want %s", f.pre.seen, want)
	}
	if len(f.pre.remembered) != 1 {
		t.Fatalf("%d entries remembered, want 1", len(f.pre.remembered))
	}
	w := f.pre.remembered[0]
	if w.Key != want || w.NotificationID != "n-1" || w.TTL != 6*time.Hour {
		t.Errorf("remembered %+v, want key %s, id n-1 and the configured PreCheckTTL 6h", w, want)
	}
}

// A hit answers the replay without planning or opening a transaction, which is what
// reading before the transaction is for. The receipt names the first notification.
func TestPreCheckHitAnswersTheReplayWithoutTouchingTheStore(t *testing.T) {
	t.Parallel()
	for _, tx := range []struct {
		name string
		do   func(t *testing.T, n *app.Notifier, nt domain.Notification) (domain.Receipt, error)
	}{
		{"Notify", func(t *testing.T, n *app.Notifier, nt domain.Notification) (domain.Receipt, error) {
			t.Helper()
			return n.Notify(t.Context(), nt)
		}},
		{"NotifyTx", func(t *testing.T, n *app.Notifier, nt domain.Notification) (domain.Receipt, error) {
			t.Helper()
			return n.NotifyTx(t.Context(), struct{}{}, nt)
		}},
	} {
		t.Run(tx.name, func(t *testing.T) {
			t.Parallel()
			f := newNoteFx(t)
			f.pre.hitID = "n-first"
			r, err := tx.do(t, f.build(), f.note("k1"))
			if err != nil {
				t.Fatalf("%s: %v", tx.name, err)
			}
			if r.Applied || r.NotificationID != "n-first" || r.IdemKey != "k1" {
				t.Fatalf("receipt %+v, want a replay of n-first named with its key", r)
			}
			if got := f.ev.all(); !slices.Equal(got, []string{"seen"}) {
				t.Fatalf("events %v, want only the read: a hit plans nothing, persists nothing and writes nothing", got)
			}
		})
	}
}

// A pre-check that cannot answer is a miss: the durable path runs, and its answer
// is the answer. The fast path never blocks Notify (NR-004).
func TestPreCheckFaultsAreMisses(t *testing.T) {
	t.Parallel()
	f := newNoteFx(t)
	f.pre.seenErr = errors.New("redis: connection refused")
	f.pre.rememberErr = errors.New("redis: connection refused")
	r, err := f.build().Notify(t.Context(), f.note("k1"))
	if err != nil || !r.Applied || r.NotificationID != "n-1" {
		t.Fatalf("Notify with a failing pre-check = %+v, %v, want the durable answer", r, err)
	}
	if got := f.ev.all(); !slices.Equal(got, []string{"seen", "persist", "remember"}) {
		t.Fatalf("events %v, want the durable path taken and the entry still tried", got)
	}
}

// Nothing is remembered unless this call committed a first apply: not a replay, not
// a failed commit, and never anything for NotifyTx, whose commit is the host's and
// may yet be a rollback (D18, D28). The first row is the control: a first apply by
// Notify is remembered, so a Notifier that never remembers cannot pass the rest.
func TestNothingIsRememberedUnlessAFirstApplyCommitted(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		setup func(f *noteFx)
		do    func(t *testing.T, f *noteFx, n *app.Notifier) error
		want  int // entries remembered
	}{
		{"a first apply by Notify", func(f *noteFx) {},
			func(t *testing.T, f *noteFx, n *app.Notifier) error {
				t.Helper()
				_, err := n.Notify(t.Context(), f.note("k1"))
				return err
			}, 1},
		{"a replay the store reports", func(f *noteFx) { f.store.receipt.Applied = false },
			func(t *testing.T, f *noteFx, n *app.Notifier) error {
				t.Helper()
				_, err := n.Notify(t.Context(), f.note("k1"))
				return err
			}, 0},
		{"a store that fails", func(f *noteFx) { f.store.err = errors.New("commit failed") },
			func(t *testing.T, f *noteFx, n *app.Notifier) error {
				t.Helper()
				_, err := n.Notify(t.Context(), f.note("k1"))
				return err
			}, 0},
		{"NotifyTx applied inside the host's transaction", func(f *noteFx) {},
			func(t *testing.T, f *noteFx, n *app.Notifier) error {
				t.Helper()
				r, err := n.NotifyTx(t.Context(), struct{}{}, f.note("k1"))
				if err == nil && !r.Applied {
					t.Error("control: NotifyTx was not applied")
				}
				return err
			}, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newNoteFx(t)
			tc.setup(f)
			_ = tc.do(t, f, f.build())
			if len(f.pre.remembered) != tc.want {
				t.Fatalf("remembered %+v, want %d entries", f.pre.remembered, tc.want)
			}
		})
	}
}

// With no pre-check at all every Notify takes the durable path and stays correct
// (D18).
func TestNotifyWithNoPreCheckTakesTheDurablePath(t *testing.T) {
	t.Parallel()
	f := newNoteFx(t)
	f.noPre = true
	r, err := f.build().Notify(t.Context(), f.note("k1"))
	if err != nil || !r.Applied {
		t.Fatalf("Notify = %+v, %v", r, err)
	}
	if got := f.ev.all(); !slices.Equal(got, []string{"persist"}) {
		t.Fatalf("events %v, want only the store", got)
	}
}

// ---------------------------------------------------------- what reaches the store --

// The store gets the identity the engine built from its own realm and the
// notification's tenant and recipient, the plan, the priority resolved from the
// topic, and, for NotifyTx, the caller's transaction as it was given. Nothing a
// producer writes in a free-form map is an identity (NR-009).
func TestWhatReachesTheStore(t *testing.T) {
	t.Parallel()
	f := newNoteFx(t)
	n := f.build()
	nt := f.note("k1")
	nt.Attributes = map[string]string{"realm": "rival"}
	nt.Payload = map[string]string{"realm": "rival"}
	nt.Data = map[string]any{"realm": "rival"}
	if _, err := n.Notify(t.Context(), nt); err != nil {
		t.Fatal(err)
	}
	if want := (domain.Identity{Realm: planRealm, Tenant: planTenant, Recipient: planUser}); f.store.id != want {
		t.Errorf("identity %+v, want %+v", f.store.id, want)
	}
	if f.store.n.Priority != domain.PriorityWarning {
		t.Errorf("priority %q, want the topic's default, warning: the store refuses an unresolved one", f.store.n.Priority)
	}
	if f.store.tx != nil {
		t.Errorf("Notify passed tx %v, want nil so the store opens and commits its own (D23)", f.store.tx)
	}
	if got := channelKinds(f.store.plan); !slices.Equal(got, []string{"in_app", "email"}) {
		t.Errorf("plan channels %v, want in_app and email", got)
	}
	if !f.store.plan.InboxVisible {
		t.Error("the plan is not inbox-visible, with an inbox-backed channel enabled")
	}

	type hostTx struct{ name string }
	own := &hostTx{"the host's"}
	if _, err := n.NotifyTx(t.Context(), own, f.note("k2")); err != nil {
		t.Fatal(err)
	}
	if f.store.tx != ports.Tx(own) {
		t.Errorf("NotifyTx passed tx %v, want the caller's own %v", f.store.tx, own)
	}
}

func channelKinds(p domain.DeliveryPlan) []string {
	var out []string
	for _, d := range p.Deliveries {
		out = append(out, string(d.Channel))
	}
	return out
}

// A replay is reported as the store reports it: Applied false, naming the first
// notification. The Notifier does not make a replay look new (NR-002).
func TestAReplayIsReportedAsAReplay(t *testing.T) {
	t.Parallel()
	f := newNoteFx(t)
	f.store.receipt = domain.Receipt{NotificationID: "n-first", IdemKey: "k1", Applied: false, InboxVisible: true}
	r, err := f.build().Notify(t.Context(), f.note("k1"))
	if err != nil {
		t.Fatal(err)
	}
	if r.Applied || r.NotificationID != "n-first" || !r.InboxVisible {
		t.Fatalf("receipt %+v, want the store's replay, unchanged", r)
	}
}

// A store failure is the caller's to see, wrapped so it can still be matched.
func TestAStoreFailureReachesTheCaller(t *testing.T) {
	t.Parallel()
	f := newNoteFx(t)
	errBoom := errors.New("boom")
	f.store.err = errBoom
	if _, err := f.build().Notify(t.Context(), f.note("k1")); !errors.Is(err, errBoom) {
		t.Fatalf("Notify = %v, want the store's error", err)
	}
}

// Cancel and Status are the identity's own: the realm is the engine's, the tenant
// and recipient the request's (NR-030, NR-032).
func TestCancelAndStatusAskForTheEnginesRealm(t *testing.T) {
	t.Parallel()
	f := newNoteFx(t)
	f.queue.receipt = domain.CancelReceipt{Matched: true, Canceled: 2}
	f.store.status = domain.NotificationStatus{Found: true, NotificationID: "n-1"}
	n := f.build()
	want := domain.Identity{Realm: planRealm, Tenant: planTenant, Recipient: planUser}

	c, err := n.Cancel(t.Context(), domain.CancelRequest{Tenant: planTenant, Recipient: planUser, IdemKey: "k1"})
	if err != nil || c != f.queue.receipt {
		t.Fatalf("Cancel = %+v, %v, want the queue's receipt", c, err)
	}
	if f.queue.id != want || f.queue.key != "k1" {
		t.Errorf("Queue.Cancel asked for %+v key %q, want %+v key k1", f.queue.id, f.queue.key, want)
	}

	st, err := n.Status(t.Context(), domain.StatusRequest{Tenant: planTenant, Recipient: planUser, IdemKey: "k1"})
	if err != nil || !st.Found || st.NotificationID != "n-1" {
		t.Fatalf("Status = %+v, %v, want the store's", st, err)
	}
	if f.store.id != want {
		t.Errorf("Store.Status asked for %+v, want %+v", f.store.id, want)
	}
	if got := f.ev.all(); !slices.Contains(got, "status:k1") {
		t.Errorf("events %v, want Store.Status asked for the request's key k1", got)
	}
}

// Broadcast is built with T042; until then it says so rather than pretend.
func TestBroadcastIsNotBuiltYet(t *testing.T) {
	t.Parallel()
	f := newNoteFx(t)
	if _, err := f.build().Broadcast(t.Context(), domain.BroadcastRequest{}); err == nil {
		t.Fatal("Broadcast succeeded before it was built")
	}
}
