package httpapi_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/truongpx396/intel-notification/adapters/driving/httpapi"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/notifytest"
	"github.com/truongpx396/intel-notification/ports"
)

// The relay (D16): the stream subscribes from the session's identity and from nothing else, sends the
// bounded count before anything, and for each nudge loads the notification under that same identity,
// so a nudge that was forged, or misrouted, loads nothing and shows nothing.

// countingSubscriber wraps a StreamSubscriber and counts what the relay asked of it.
type countingSubscriber struct {
	ports.StreamSubscriber
	subscribed atomic.Int64
	closed     atomic.Int64
	closedCh   chan struct{}
	identities chan domain.Identity
}

type countedSub struct {
	ports.Subscription
	parent *countingSubscriber
}

func (s *countingSubscriber) Subscribe(ctx context.Context, id domain.Identity) (ports.Subscription, error) {
	s.subscribed.Add(1)
	select {
	case s.identities <- id:
	default:
	}
	sub, err := s.StreamSubscriber.Subscribe(ctx, id)
	if err != nil {
		return nil, err
	}
	return &countedSub{Subscription: sub, parent: s}, nil
}

func (c *countedSub) Close() error {
	c.parent.closed.Add(1)
	select {
	case c.parent.closedCh <- struct{}{}:
	default:
	}
	return c.Subscription.Close()
}

// streamFx serves the surface over a real listener, with the kit's in-memory stream behind it.
type streamFx struct {
	*fixture
	stream *notifytest.Stream
	subs   *countingSubscriber
	relay  *notifytest.Relay
	srv    *httptest.Server
}

func newStreamFx(t *testing.T, opts ...httpapi.Option) *streamFx {
	t.Helper()
	stream := notifytest.NewStream(notifytest.NewProbe())
	subs := &countingSubscriber{StreamSubscriber: stream, identities: make(chan domain.Identity, 16), closedCh: make(chan struct{}, 8)}
	f := newFixture(t)
	h, err := httpapi.New(httpapi.Deps{
		Inbox: f.inbox, Notifier: f.notifier, Admin: f.admin, Stream: subs,
		Identify: func(r *http.Request) (domain.Identity, error) { return f.identify(r) },
	}, opts...)
	if err != nil {
		t.Fatal(err)
	}
	f.h = h
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	name := func(id domain.Identity) string {
		for token, known := range tokens {
			if known == id {
				return token
			}
		}
		return "nobody"
	}
	return &streamFx{fixture: f, stream: stream, subs: subs, relay: notifytest.NewRelay(srv.URL, name), srv: srv}
}

const nid1 = "0195f3a0-0000-7000-8000-000000000001"

func (f *streamFx) item(id, title string) {
	f.inbox.onGet = func(n, _ string) (*domain.InboxItem, error) {
		if n != id {
			return nil, nil
		}
		return &domain.InboxItem{ID: id, Topic: "ingestion_complete", Title: title, Body: "b", Priority: domain.PriorityInfo}, nil
	}
}

// On every (re)connect the relay sends the authoritative bounded count first (D15): a missed push is
// healed by the next connect, and a client never has to trust a count it was pushed.
func TestTheStreamSendsTheBoundedCountBeforeAnythingElse(t *testing.T) {
	t.Parallel()
	f := newStreamFx(t)
	f.inbox.onUnread = func() (domain.UnreadCount, error) { return domain.UnreadCount{Count: 99, Capped: true}, nil }
	conn := f.relay.Connect(t, alice)
	if got := conn.NextCount(t); got != (notifytest.Count{Count: 99, Capped: true}) {
		t.Errorf("first event = %+v, want {99 true}", got)
	}
}

func TestANudgeLoadsTheItemUnderTheSessionAndSendsItWithTheNewCount(t *testing.T) {
	t.Parallel()
	f := newStreamFx(t)
	f.item(nid1, "Done")
	var unread atomic.Int64
	f.inbox.onUnread = func() (domain.UnreadCount, error) { return domain.UnreadCount{Count: int(unread.Add(1))}, nil }
	conn := f.relay.Connect(t, alice, "Accept-Language", "fr-CA,fr;q=0.9")
	conn.NextCount(t)

	if err := f.stream.Publish(t.Context(), alice, nid1); err != nil {
		t.Fatal(err)
	}
	got := conn.MustReceive(t, nid1)
	if got.Item.Topic != "ingestion_complete" || got.Item.Title != "Done" || got.Unread.Count != 2 {
		t.Errorf("event = %+v, want the item and the count after it (the second the inbox was asked for)", got)
	}
	var gets []call
	for _, c := range f.inbox.Calls() {
		if c.Method == "Get" {
			gets = append(gets, c)
		}
	}
	if len(gets) != 1 || gets[0].ID != alice || gets[0].Args[0] != nid1 || gets[0].Args[1] != "fr-CA" {
		t.Errorf("the inbox was asked %+v, want %s under the session's identity, in the viewer's locale", gets, nid1)
	}
}

// A nudge carries an id and nothing else; the item is loaded under the caller's scope, and one that loads
// nothing sends nothing. A forged nudge naming another recipient's notification is exactly that (NS-001).
func TestAForgedNudgeShowsNothing(t *testing.T) {
	t.Parallel()
	f := newStreamFx(t)
	f.inbox.onGet = func(string, string) (*domain.InboxItem, error) { return nil, nil } // the store returns nothing under alice's scope
	conn := f.relay.Connect(t, alice)
	conn.NextCount(t)

	if err := f.stream.Publish(t.Context(), alice, "0195f3a0-0000-7000-8000-00000000b0b0"); err != nil { // bob's notification
		t.Fatal(err)
	}
	conn.MustReceiveNothing(t, 300*time.Millisecond)
	var asked bool
	for _, c := range f.inbox.Calls() {
		if c.Method == "Get" {
			asked = true
			if c.ID != alice {
				t.Errorf("the nudge was loaded as %+v, want the session's own", c.ID)
			}
		}
	}
	if !asked {
		t.Error("the nudge was never loaded: the stream must re-read under the caller's scope, not trust the nudge")
	}
}

// The relay subscribes from the session's identity: not from a query parameter, a header or a path.
func TestTheStreamSubscribesFromTheSessionAndNotTheQuery(t *testing.T) {
	t.Parallel()
	f := newStreamFx(t)
	f.item(nid1, "Done")
	conn := f.relay.ConnectAt(t, alice, "/notifications/stream?recipient=bob&recipient_id=bob&tenant=w9&realm=rival",
		"X-Recipient", "bob", "X-Tenant", "w9")
	conn.NextCount(t)

	select {
	case id := <-f.subs.identities:
		if id != alice {
			t.Fatalf("subscribed as %+v, want the session's %+v", id, alice)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the relay never subscribed")
	}
	if err := f.stream.Publish(t.Context(), bob, nid1); err != nil { // what the query asked to hear
		t.Fatal(err)
	}
	conn.MustReceiveNothing(t, 300*time.Millisecond)
	if err := f.stream.Publish(t.Context(), alice, nid1); err != nil {
		t.Fatal(err)
	}
	conn.MustReceive(t, nid1)
}

// EventSource cannot set a header, so the token may travel in the query, on this route and no other.
func TestATokenInTheQueryIsAcceptedOnTheStreamAndNowhereElse(t *testing.T) {
	t.Parallel()
	f := newStreamFx(t)
	noHeader := notifytest.NewRelay(f.srv.URL, func(domain.Identity) string { return "" })

	conn := noHeader.ConnectAt(t, alice, "/notifications/stream?token=alice")
	conn.NextCount(t)

	for _, path := range []string{"/notifications?token=alice", "/notifications/unread-count?token=alice", "/notifications/preferences?token=alice"} {
		resp, err := http.Get(f.srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("GET %s = %d, want 401: a token in a URL is read on the stream only, because URLs are logged", path, resp.StatusCode)
		}
	}
	// A header, when there is one, is what is used.
	if got := noHeader.Status(t, bob, "/notifications/stream?token=nobody", "Authorization", "Bearer bob"); got != 200 {
		t.Errorf("a stream with a good header and a bad query token = %d, want 200: the header wins", got)
	}
}

func TestAStreamWithoutASessionSubscribesToNothing(t *testing.T) {
	t.Parallel()
	f := newStreamFx(t)
	noHeader := notifytest.NewRelay(f.srv.URL, func(domain.Identity) string { return "" })
	if got := noHeader.Status(t, alice, "/notifications/stream"); got != http.StatusUnauthorized {
		t.Errorf("status %d, want 401", got)
	}
	if n := f.subs.subscribed.Load(); n != 0 {
		t.Errorf("%d subscriptions for a caller with no session, want none", n)
	}
	if got := noHeader.Status(t, alice, "/notifications/stream?token=forged"); got != http.StatusUnauthorized || f.subs.subscribed.Load() != 0 {
		t.Errorf("a forged token = %d with %d subscriptions, want 401 and none", got, f.subs.subscribed.Load())
	}
}

func TestTheSubscriptionEndsWhenTheClientLeaves(t *testing.T) {
	t.Parallel()
	f := newStreamFx(t)
	conn := f.relay.Connect(t, alice)
	conn.NextCount(t)
	conn.Close()
	select {
	case <-f.subs.closedCh:
	case <-time.After(10 * time.Second):
		t.Fatal("the subscription was not closed after the client left: every disconnect would leak one")
	}
	if f.subs.closed.Load() != 1 {
		t.Errorf("closed %d times, want once", f.subs.closed.Load())
	}
}

func TestOneNotificationIsSentOnceHoweverManyTimesItIsNudged(t *testing.T) {
	t.Parallel()
	f := newStreamFx(t)
	f.item(nid1, "Done")
	conn := f.relay.Connect(t, alice)
	conn.NextCount(t)
	for range 3 {
		if err := f.stream.Publish(t.Context(), alice, nid1); err != nil {
			t.Fatal(err)
		}
	}
	conn.MustReceive(t, nid1)
	conn.MustReceiveNothing(t, 300*time.Millisecond)
	gets := 0
	for _, c := range f.inbox.Calls() {
		if c.Method == "Get" {
			gets++
		}
	}
	if gets != 1 {
		t.Errorf("the inbox was asked %d times, want once: a re-nudge is only a re-nudge (D16)", gets)
	}
}

type failingSubscriber struct{}

func (failingSubscriber) Subscribe(context.Context, domain.Identity) (ports.Subscription, error) {
	return nil, errors.New("redis: connection to 10.0.0.9 refused")
}

func TestAStreamThatCannotSubscribeIs503AndSaysNothingOfWhy(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	var reported atomic.Int64
	h, err := httpapi.New(httpapi.Deps{Inbox: f.inbox, Stream: failingSubscriber{}, Identify: f.identify},
		httpapi.WithErrorHandler(func(error) { reported.Add(1) }))
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("GET", "/notifications/stream", nil)
	req.Header.Set("Authorization", "Bearer alice")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable || strings.Contains(rec.Body.String(), "10.0.0.9") || reported.Load() != 1 {
		t.Errorf("= %d %q (reported %d), want 503 that hides the fault and reports it", rec.Code, rec.Body.String(), reported.Load())
	}
	if len(f.inbox.Calls()) != 0 {
		t.Error("the inbox was read for a stream that never started")
	}
}

// An idle stream says so, so a proxy that closes silent connections does not close it.
func TestAnIdleStreamSendsAHeartbeat(t *testing.T) {
	t.Parallel()
	f := newStreamFx(t, httpapi.WithHeartbeat(20*time.Millisecond))
	conn := f.relay.Connect(t, alice)
	conn.NextCount(t)
	if ev := conn.Next(t); ev.Name != ":" {
		t.Errorf("next event on an idle stream = %+v, want a comment", ev)
	}
}
