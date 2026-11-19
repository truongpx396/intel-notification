//go:build integration

package app_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/truongpx396/intel-notification/app"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/notifytest"
	"github.com/truongpx396/intel-notification/ports"
)

// T042: a broadcast, end to end on the Env's real store, queue and dispatcher, and cut
// short where a worker can be killed: between two pages, and inside one. The audience is
// the Env's, two members a page, so a five-member audience is three pages and every
// crash has a page before it and a page after.

const launch domain.Topic = "launch"

// broadcastEnv is an Env with an email channel, a topic that goes out on it, and n
// recipients of the tenant w1 who each have an address and are the audience "everyone".
func broadcastEnv(t *testing.T, n int) (*notifytest.Env, []domain.Recipient) {
	t.Helper()
	e := notifytest.NewEnv(t)
	e.Topics.Register(notifytest.Realm, launch, domain.TopicDef{DefaultChannels: []domain.ChannelKind{"email"}, DefaultPriority: domain.PriorityInfo})
	e.Templates.Put(notifytest.Realm, string(launch), "email", "en", domain.RenderedContent{Subject: "S", Body: "B"})
	e.AddChannel("email", domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupProvider, DedupWindow: time.Hour})
	rs := make([]domain.Recipient, n)
	for i := range rs {
		rs[i] = domain.Recipient{Kind: "user", ID: fmt.Sprintf("u%d", i+1)}
		id := domain.Identity{Realm: notifytest.Realm, Tenant: itTenant, Recipient: rs[i]}
		e.Addresses.Put(id, "email", domain.Address{Channel: "email", Value: fmt.Sprintf("u%d@example.com", i+1)})
	}
	e.Audiences.Set(notifytest.Realm, itTenant, "everyone", rs...)
	return e, rs
}

func everyone(key string) domain.BroadcastRequest {
	return domain.BroadcastRequest{Tenant: itTenant, Audience: "everyone", Topic: launch, IdemKey: key, Title: "Atlas is live"}
}

func announce(t *testing.T, e *notifytest.Env, req domain.BroadcastRequest) domain.BroadcastReceipt {
	t.Helper()
	r, err := e.Notifier.Broadcast(t.Context(), req)
	if err != nil {
		t.Fatalf("Broadcast(%s): %v", req.IdemKey, err)
	}
	return r
}

func expanderOn(t *testing.T, e *notifytest.Env, store ports.Store, opts ...app.BroadcastOption) *app.BroadcastExpander {
	t.Helper()
	d := e.BroadcastDeps()
	d.Store = store
	x, err := app.NewBroadcastExpander(e.Config, d, opts...)
	if err != nil {
		t.Fatalf("NewBroadcastExpander: %v", err)
	}
	return x
}

func expander(t *testing.T, e *notifytest.Env, opts ...app.BroadcastOption) *app.BroadcastExpander {
	t.Helper()
	return expanderOn(t, e, e.Store, opts...)
}

// killAfter is a Store that lets n pages commit and then ends the worker's context, as a
// process killed between two pages does: the page that committed is the database's, and
// the worker never asks for the next.
type killAfter struct {
	ports.Store
	n      int
	cancel context.CancelFunc
	done   atomic.Int32
}

func (s *killAfter) CommitBroadcastPage(ctx context.Context, job domain.BroadcastJob, page []domain.PageItem, next string, done bool) error {
	err := s.Store.CommitBroadcastPage(ctx, job, page, next, done)
	if err == nil && int(s.done.Add(1)) == s.n {
		s.cancel()
	}
	return err
}

// lapseBroadcasts ends every broadcast lease, as the lease of a worker that died does at
// its ttl. The fixture's connection is the admin's, which the code under test never has.
func lapseBroadcasts(t *testing.T, e *notifytest.Env) {
	t.Helper()
	if _, err := e.DB.Admin.Exec(t.Context(),
		`UPDATE notification_broadcasts SET lease_expires_at = now() - interval '1 second' WHERE lease_token IS NOT NULL`); err != nil {
		t.Fatal(err)
	}
}

type bRow struct {
	cursor string
	fanned int
	status string
	leased bool
}

func broadcastState(t *testing.T, e *notifytest.Env, key string) bRow {
	t.Helper()
	var r bRow
	if err := e.DB.Admin.QueryRow(t.Context(),
		`SELECT cursor, fanned, status, lease_token IS NOT NULL FROM notification_broadcasts WHERE idem_key = $1`, key).
		Scan(&r.cursor, &r.fanned, &r.status, &r.leased); err != nil {
		t.Fatalf("read broadcast %s: %v", key, err)
	}
	return r
}

// keysByRecipient reads what was stored: the idempotency key each recipient's inbox row
// carries, for one tenant, from the admin's connection.
func keysByRecipient(t *testing.T, e *notifytest.Env, tenantID string) map[string]string {
	t.Helper()
	rows, err := e.DB.Admin.Query(t.Context(), `SELECT recipient_id, idem_key FROM notifications WHERE tenant_id = $1`, tenantID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := map[string]string{}
	for rows.Next() {
		var who, key string
		if err := rows.Scan(&who, &key); err != nil {
			t.Fatal(err)
		}
		if _, dup := got[who]; dup {
			t.Errorf("%s has two inbox rows from one broadcast", who)
		}
		got[who] = key
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return got
}

func cursorsAsked(e *notifytest.Env) []string {
	var cs []string
	for _, c := range e.Audiences.Calls() {
		cs = append(cs, c.Cursor)
	}
	return cs
}

// ------------------------------------------------------- the whole journey --

// A broadcast returns once it is recorded, before a single notification exists: the caller
// is not held for the audience (NR-020). A worker then expands it a page at a time, and the
// dispatcher delivers what it queued, once to each recipient.
func TestABroadcastIsRecordedOnTheRequestPathAndReachesEveryRecipientOnce(t *testing.T) {
	t.Parallel()
	e, _ := broadcastEnv(t, 5)

	receipt := announce(t, e, everyone("launch:2026"))
	if !receipt.Applied || receipt.BroadcastID == "" {
		t.Fatalf("Broadcast = %+v, want applied with an id", receipt)
	}
	if n := count(t, e, `SELECT count(*) FROM notifications`); n != 0 {
		t.Fatalf("%d notifications before any worker ran, want none: expansion is off the request path", n)
	}
	if got := broadcastState(t, e, "launch:2026"); got != (bRow{status: "pending"}) {
		t.Fatalf("broadcast row = %+v, want pending at the start, unleased", got)
	}
	if n := len(e.Audiences.Calls()); n != 0 {
		t.Fatalf("the audience was resolved %d times on the request path, want none", n)
	}

	pages, err := expander(t, e).Expand(t.Context())
	if err != nil || pages != 3 {
		t.Fatalf("Expand = %d, %v, want 3 pages", pages, err)
	}
	if got := broadcastState(t, e, "launch:2026"); got != (bRow{cursor: "", fanned: 5, status: "completed"}) {
		t.Fatalf("broadcast row = %+v, want completed with 5 fanned", got)
	}
	if n := count(t, e, `SELECT count(*) FROM notifications`); n != 5 {
		t.Fatalf("%d notifications, want 5", n)
	}
	if n := count(t, e, `SELECT count(*) FROM notification_outbox`); n != 5 {
		t.Fatalf("%d queued deliveries, want 5: one email each", n)
	}

	if handled := drainAll(t, e); handled != 5 {
		t.Fatalf("the dispatcher handled %d deliveries, want 5", handled)
	}
	if sent := e.Probe.Total(); sent != 5 {
		t.Fatalf("%d emails reached the provider, want 5", sent)
	}
}

// What is stored under each recipient is the member key of the broadcast's key, whatever
// carried it there: an audience paged two at a time, or an inline list cut three ways.
func TestEveryRecipientIsStoredUnderTheMemberKeyWhateverThePaging(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		build func(rs []domain.Recipient) domain.BroadcastRequest
		opts  []app.BroadcastOption
	}{
		{"an audience paged by the resolver", func([]domain.Recipient) domain.BroadcastRequest { return everyone("launch:2026") }, nil},
		{"an inline list in pages of one", func(rs []domain.Recipient) domain.BroadcastRequest {
			r := everyone("launch:2026")
			r.Audience, r.Recipients = "", rs
			return r
		}, []app.BroadcastOption{app.WithBroadcastPage(1)}},
		{"an inline list in pages of three", func(rs []domain.Recipient) domain.BroadcastRequest {
			r := everyone("launch:2026")
			r.Audience, r.Recipients = "", rs
			return r
		}, []app.BroadcastOption{app.WithBroadcastPage(3)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e, rs := broadcastEnv(t, 5)
			announce(t, e, tc.build(rs))
			if _, err := expander(t, e, tc.opts...).Expand(t.Context()); err != nil {
				t.Fatalf("Expand: %v", err)
			}
			want := map[string]string{}
			for _, r := range rs {
				want[r.ID] = domain.BroadcastMemberKey("launch:2026", r)
			}
			if got := keysByRecipient(t, e, itTenant.ID); !maps.Equal(got, want) {
				t.Fatalf("stored keys %v, want %v", got, want)
			}
		})
	}
}

// ---------------------------------------------------------- the two crashes --

// A kill between pages. The worker is stopped once the first page has committed. A new
// worker resumes at the page after it: it asks the resolver for the cursor the first page
// committed and never for the first page again, and when it is done every recipient has
// one notification (NR-020, NS: a crash mid-broadcast resumes at the next page).
func TestACrashBetweenPagesResumesAtTheNextPage(t *testing.T) {
	t.Parallel()
	e, rs := broadcastEnv(t, 5)
	announce(t, e, everyone("launch:2026"))

	ctx, kill := context.WithCancel(t.Context())
	defer kill()
	dying := expanderOn(t, e, &killAfter{Store: e.Store, n: 1, cancel: kill})
	if pages, err := dying.Expand(ctx); err != nil || pages != 1 {
		t.Fatalf("the worker that was killed: Expand = %d, %v, want the one page it committed", pages, err)
	}
	if n := count(t, e, `SELECT count(*) FROM notifications`); n != 2 {
		t.Fatalf("%d notifications after the kill, want the 2 of the first page", n)
	}
	if got := broadcastState(t, e, "launch:2026"); got.cursor != "2" || got.fanned != 2 || got.status != "pending" {
		t.Fatalf("broadcast row = %+v, want pending at cursor 2 with 2 fanned", got)
	}

	if pages, err := expander(t, e).Expand(t.Context()); err != nil || pages != 2 {
		t.Fatalf("the worker that resumed: Expand = %d, %v, want the other 2 pages", pages, err)
	}
	if got, want := cursorsAsked(e), []string{"", "2", "4"}; !slices.Equal(got, want) {
		t.Fatalf("the resolver was asked for cursors %q, want %q: the first page is asked for once, not again after the kill", got, want)
	}
	if got := keysByRecipient(t, e, itTenant.ID); len(got) != len(rs) {
		t.Fatalf("%d recipients notified, want %d, each once", len(got), len(rs))
	}
	if got := broadcastState(t, e, "launch:2026"); got.status != "completed" || got.fanned != 5 {
		t.Fatalf("broadcast row = %+v, want completed with 5 fanned", got)
	}
	if handled := drainAll(t, e); handled != 5 || e.Probe.Total() != 5 {
		t.Fatalf("the dispatcher handled %d deliveries and %d reached the provider, want 5 and 5", handled, e.Probe.Total())
	}
}

// A kill inside a page's transaction: the second page's third write fails, as a process
// dying at that statement would leave it. The page rolls back as a whole (its first write
// and the cursor's advance with it), the first page stands, and no one else starts the
// page while the dead worker's lease is live. When it lapses, the page is taken again from
// its own first recipient, and the broadcast is finished with nobody notified twice.
func TestACrashInsideAPageRollsTheWholePageBackAndTheNextWorkerRedoesIt(t *testing.T) {
	t.Parallel()
	e, rs := broadcastEnv(t, 5)
	announce(t, e, everyone("launch:2026"))

	if _, err := e.DB.Admin.Exec(t.Context(), `
CREATE FUNCTION kill_mid_page() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.recipient_id = 'u4' THEN RAISE EXCEPTION 'killed mid-page'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER kill_mid_page BEFORE INSERT ON notifications FOR EACH ROW EXECUTE FUNCTION kill_mid_page();`); err != nil {
		t.Fatalf("fixture: %v", err)
	}

	// The second page is u3 and u4: u3 is written, u4 kills the transaction.
	pages, err := expander(t, e).Expand(t.Context())
	if err == nil || pages != 1 {
		t.Fatalf("Expand = %d, %v, want the first page committed and the second's fault", pages, err)
	}
	if got := keysByRecipient(t, e, itTenant.ID); len(got) != 2 || got["u3"] != "" {
		t.Fatalf("notified %v, want u1 and u2 only: u3's write was in the page that rolled back", got)
	}
	if n := count(t, e, `SELECT count(*) FROM notify_idem`); n != 2 {
		t.Fatalf("%d guards, want 2: the rolled-back page's guards go with it", n)
	}
	if got := broadcastState(t, e, "launch:2026"); got != (bRow{cursor: "2", fanned: 2, status: "pending", leased: true}) {
		t.Fatalf("broadcast row = %+v, want cursor 2, 2 fanned and the dead worker's lease still held", got)
	}

	if _, err := e.DB.Admin.Exec(t.Context(), `DROP TRIGGER kill_mid_page ON notifications`); err != nil {
		t.Fatal(err)
	}
	if pages, err := expander(t, e).Expand(t.Context()); err != nil || pages != 0 {
		t.Fatalf("a worker that looked before the lease lapsed: Expand = %d, %v, want nothing to take", pages, err)
	}
	lapseBroadcasts(t, e)
	if pages, err := expander(t, e).Expand(t.Context()); err != nil || pages != 2 {
		t.Fatalf("the worker that took over: Expand = %d, %v, want the 2 pages left", pages, err)
	}
	if got, want := cursorsAsked(e), []string{"", "2", "2", "4"}; !slices.Equal(got, want) {
		t.Fatalf("the resolver was asked for cursors %q, want %q: the failed page again from its own cursor, never from the start", got, want)
	}
	if got := keysByRecipient(t, e, itTenant.ID); len(got) != len(rs) {
		t.Fatalf("%d recipients notified, want %d, each once", len(got), len(rs))
	}
	if n := count(t, e, `SELECT count(*) FROM notifications`); n != 5 {
		t.Fatalf("%d notifications, want 5", n)
	}
}

// ------------------------------------------------------------------ retries --

// A retried broadcast double-notifies nobody: the producer's second call before the first
// was expanded, and a third after it was done, are replays that record nothing, and what
// is queued and sent is one delivery per recipient (NR-020, invariant 10).
func TestARetriedBroadcastDoubleNotifiesNobody(t *testing.T) {
	t.Parallel()
	e, _ := broadcastEnv(t, 5)

	first := announce(t, e, everyone("launch:2026"))
	retry := announce(t, e, everyone("launch:2026"))
	if retry.Applied || retry.BroadcastID != first.BroadcastID {
		t.Fatalf("the retry = %+v, want not applied and the first call's id %s", retry, first.BroadcastID)
	}
	if _, err := expander(t, e).Expand(t.Context()); err != nil {
		t.Fatalf("Expand: %v", err)
	}

	late := announce(t, e, everyone("launch:2026"))
	if late.Applied || late.BroadcastID != first.BroadcastID {
		t.Fatalf("the retry after completion = %+v, want not applied and the first call's id", late)
	}
	if pages, err := expander(t, e).Expand(t.Context()); err != nil || pages != 0 {
		t.Fatalf("Expand after the retry = %d, %v, want nothing to do", pages, err)
	}
	if n := count(t, e, `SELECT count(*) FROM notifications`); n != 5 {
		t.Fatalf("%d notifications, want 5: one per recipient", n)
	}
	if handled := drainAll(t, e); handled != 5 || e.Probe.Total() != 5 {
		t.Fatalf("handled %d deliveries and %d reached the provider, want 5 and 5", handled, e.Probe.Total())
	}
}

// Workers that run together over one broadcast write each page once: the lease gives each
// page to one of them, and the fence discards the page of one that lost it. The pages
// they report add up to the broadcast's own, and no recipient is notified twice.
func TestWorkersRunningTogetherNotifyEveryRecipientOnce(t *testing.T) {
	t.Parallel()
	e, rs := broadcastEnv(t, 9)
	announce(t, e, everyone("launch:2026"))

	const workers = 4
	var (
		wg    sync.WaitGroup
		total atomic.Int32
	)
	errs := make(chan error, workers)
	for range workers {
		x := expander(t, e) // built here: a helper that fails the test must not run on another goroutine
		wg.Go(func() {
			n, err := x.Expand(t.Context())
			total.Add(int32(n))
			errs <- err
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("a worker's pass failed: %v", err)
		}
	}
	// Four pages of two and one of one: whoever took each, it was committed once.
	if got := int(total.Load()); got != 5 {
		t.Errorf("the workers committed %d pages between them, want 5: each page once", got)
	}
	if got := keysByRecipient(t, e, itTenant.ID); len(got) != len(rs) {
		t.Fatalf("%d recipients notified, want %d, each once", len(got), len(rs))
	}
	if got := broadcastState(t, e, "launch:2026"); got.status != "completed" || got.fanned != 9 {
		t.Fatalf("broadcast row = %+v, want completed with 9 fanned", got)
	}
}

// A second broadcast to the same people under another key is another set of member keys,
// and notifies them again: the key is the broadcast's identity, and nothing else is.
func TestADifferentBroadcastKeyNotifiesTheSamePeopleAgain(t *testing.T) {
	t.Parallel()
	e, _ := broadcastEnv(t, 3)
	for _, key := range []string{"launch:2026", "launch:2026:reminder"} {
		announce(t, e, everyone(key))
		if _, err := expander(t, e).Expand(t.Context()); err != nil {
			t.Fatalf("Expand(%s): %v", key, err)
		}
	}
	if n := count(t, e, `SELECT count(*) FROM notifications`); n != 6 {
		t.Fatalf("%d notifications, want 6: three people, two broadcasts", n)
	}
}

// A recipient who was already notified by their own Notify under the broadcast's member
// key is not notified again by the broadcast: the guard is one and the same.
func TestABroadcastAndATargetedNotifySharingAKeyNotifyOnce(t *testing.T) {
	t.Parallel()
	e, rs := broadcastEnv(t, 3)
	if r, err := e.Notifier.Notify(t.Context(), domain.Notification{
		Tenant: itTenant, Recipient: rs[1], Topic: launch, Title: "Atlas is live", OccurredAt: notifytest.Epoch,
		IdemKey: domain.BroadcastMemberKey("launch:2026", rs[1]),
	}); err != nil || !r.Applied {
		t.Fatalf("Notify = %+v, %v", r, err)
	}

	announce(t, e, everyone("launch:2026"))
	if _, err := expander(t, e).Expand(t.Context()); err != nil {
		t.Fatalf("Expand: %v", err)
	}
	if n := count(t, e, `SELECT count(*) FROM notifications`); n != 3 {
		t.Fatalf("%d notifications, want 3: u2 was already notified under the broadcast's key for them", n)
	}
	if got := broadcastState(t, e, "launch:2026"); got.fanned != 2 {
		t.Fatalf("fanned = %d, want 2: u2's was a replay", got.fanned)
	}
}

// --------------------------------------------------------------- the limits --

// An inline list over MaxInlineRecipients is refused where it is sent, and nothing is
// recorded: a list that size is an audience selector for the host to resolve (NR-020).
func TestAnInlineListOverTheLimitIsRefusedAndRecordsNothing(t *testing.T) {
	t.Parallel()
	e, _ := broadcastEnv(t, 0)
	limit := e.Config.WithDefaults().MaxInlineRecipients

	over := everyone("launch:2026")
	over.Audience = ""
	for i := range limit + 1 {
		over.Recipients = append(over.Recipients, domain.Recipient{Kind: "user", ID: fmt.Sprintf("m%d", i)})
	}
	if _, err := e.Notifier.Broadcast(t.Context(), over); !errors.Is(err, domain.ErrTooManyRecipients) {
		t.Fatalf("Broadcast of %d = %v, want ErrTooManyRecipients", limit+1, err)
	}
	if n := count(t, e, `SELECT count(*) FROM notification_broadcasts`); n != 0 {
		t.Fatalf("%d broadcasts recorded after the refusal, want none", n)
	}

	over.Recipients = over.Recipients[:limit]
	if r, err := e.Notifier.Broadcast(t.Context(), over); err != nil || !r.Applied {
		t.Fatalf("Broadcast of exactly %d = %+v, %v, want it recorded", limit, r, err)
	}
}

// Two tenants broadcast under one key to a selector of the same name and the same
// recipient ids. They are two broadcasts, each to its own tenant's members: the tenant is
// in the key of the broadcast and in the guard of every notification it expands to (D17).
func TestTwoTenantsBroadcastingUnderOneKeyDoNotMeet(t *testing.T) {
	t.Parallel()
	e, rs := broadcastEnv(t, 3)
	other := domain.Tenant{Kind: "workspace", ID: "w2"}
	e.Audiences.Set(notifytest.Realm, other, "everyone", rs[:2]...)
	for _, r := range rs[:2] {
		id := domain.Identity{Realm: notifytest.Realm, Tenant: other, Recipient: r}
		e.Addresses.Put(id, "email", domain.Address{Channel: "email", Value: r.ID + "@other.example.com"})
	}

	a := announce(t, e, everyone("launch:2026"))
	b := everyone("launch:2026")
	b.Tenant = other
	bReceipt := announce(t, e, b)
	if !bReceipt.Applied || bReceipt.BroadcastID == a.BroadcastID {
		t.Fatalf("the second tenant's broadcast = %+v, want its own", bReceipt)
	}
	if _, err := expander(t, e).Expand(t.Context()); err != nil {
		t.Fatalf("Expand: %v", err)
	}
	if got := len(keysByRecipient(t, e, "w1")); got != 3 {
		t.Errorf("w1 notified %d, want 3", got)
	}
	if got := len(keysByRecipient(t, e, "w2")); got != 2 {
		t.Errorf("w2 notified %d, want 2", got)
	}
	if n := count(t, e, `SELECT count(*) FROM notifications`); n != 5 {
		t.Errorf("%d notifications, want 5: the two tenants' u1 and u2 are not the same recipient", n)
	}
}
