//go:build integration

package postgres

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"testing"

	"github.com/truongpx396/intel-notification/domain"
)

// T042: the broadcast methods of the Store against a real PostgreSQL. A broadcast is a
// durable row; a worker leases it at a cursor and commits one page with the cursor's
// advance, in one transaction, fenced by the lease token (NR-020, D7). The failures that
// matter are a worker that dies, between pages and inside one, and one whose lease lapsed
// and which writes anyway, and none of them can be held by anything but the database.

const bTopic domain.Topic = "ingestion_complete"

var bTenant = domain.Tenant{Kind: "workspace", ID: "w1"}

func inline(key string, recipients ...string) domain.BroadcastRequest {
	req := domain.BroadcastRequest{Tenant: bTenant, Topic: bTopic, IdemKey: key}
	for _, r := range recipients {
		req.Recipients = append(req.Recipients, domain.Recipient{Kind: "user", ID: r})
	}
	return req
}

func selector(key, audience string) domain.BroadcastRequest {
	return domain.BroadcastRequest{Tenant: bTenant, Topic: bTopic, IdemKey: key, Audience: audience}
}

// pageOf is a page of notifications under the broadcast key, as the expander builds one:
// each recipient's key derived from the broadcast's, one email delivery each.
func pageOf(key string, recipients ...string) []domain.PageItem {
	var items []domain.PageItem
	for _, r := range recipients {
		rc := domain.Recipient{Kind: "user", ID: r}
		id := domain.Identity{Realm: "aisat", Tenant: bTenant, Recipient: rc}
		items = append(items, domain.PageItem{
			Notification: domain.Notification{
				Tenant: bTenant, Recipient: rc, Topic: bTopic, Priority: domain.PriorityInfo,
				Title: "Atlas is live", IdemKey: domain.BroadcastMemberKey(key, rc),
			},
			Plan: domain.DeliveryPlan{
				Shard: domain.ShardFor(id, 16), InboxVisible: true,
				Deliveries: []domain.PlannedDelivery{{Channel: "email", Mode: domain.PlanImmediate}},
			},
		})
	}
	return items
}

func (e *env) createBroadcast(req domain.BroadcastRequest) domain.BroadcastReceipt {
	e.t.Helper()
	r, err := e.s.CreateBroadcast(e.t.Context(), "aisat", req)
	if err != nil {
		e.t.Fatalf("CreateBroadcast(%s): %v", req.IdemKey, err)
	}
	return r
}

func (e *env) leaseBroadcast() *domain.BroadcastJob {
	e.t.Helper()
	job, err := e.s.LeaseBroadcast(e.t.Context(), lease)
	if err != nil {
		e.t.Fatalf("LeaseBroadcast: %v", err)
	}
	return job
}

// lapseBroadcastLeases ends every broadcast lease, as the lease of a worker that died does
// at its ttl.
func (e *env) lapseBroadcastLeases() {
	e.t.Helper()
	if _, err := e.db.Admin.Exec(e.t.Context(),
		`UPDATE notification_broadcasts SET lease_expires_at = now() - interval '1 second' WHERE lease_token IS NOT NULL`); err != nil {
		e.t.Fatal(err)
	}
}

type broadcastRow struct {
	cursor string
	fanned int
	status string
	leased bool
	done   bool
}

func (e *env) broadcastRow(key string) broadcastRow {
	e.t.Helper()
	var r broadcastRow
	if err := e.db.Admin.QueryRow(e.t.Context(), `
SELECT cursor, fanned, status, lease_token IS NOT NULL, completed_at IS NOT NULL
  FROM notification_broadcasts WHERE idem_key = $1`, key).Scan(&r.cursor, &r.fanned, &r.status, &r.leased, &r.done); err != nil {
		e.t.Fatalf("read broadcast %s: %v", key, err)
	}
	return r
}

// written counts what a page leaves behind: the inbox rows, the guards and the queue rows.
func (e *env) written() (notifications, guards, queued int) {
	e.t.Helper()
	return e.count(`SELECT count(*) FROM notifications`), e.count(`SELECT count(*) FROM notify_idem`),
		e.count(`SELECT count(*) FROM notification_outbox`)
}

func (e *env) commit(job *domain.BroadcastJob, page []domain.PageItem, next string, done bool) error {
	e.t.Helper()
	return e.s.CommitBroadcastPage(e.t.Context(), *job, page, next, done)
}

// ---------------------------------------------------------------- creating --

// A broadcast is accepted once per (realm, tenant, key): a retry finds its row and is told
// so, with the id the first call got. The tenant and the realm are in the key, so another
// tenant's broadcast under the same key is its own (D17).
func TestCreateBroadcastIsIdempotentOnItsKeyWithinATenant(t *testing.T) {
	t.Parallel()
	e := newEnv(t)

	first := e.createBroadcast(inline("launch", "u1", "u2"))
	if !first.Applied || first.BroadcastID == "" {
		t.Fatalf("first CreateBroadcast = %+v, want applied with an id", first)
	}
	replay := e.createBroadcast(inline("launch", "u1", "u2"))
	if replay.Applied || replay.BroadcastID != first.BroadcastID {
		t.Fatalf("replay = %+v, want not applied and the first call's id %s", replay, first.BroadcastID)
	}
	if n := e.count(`SELECT count(*) FROM notification_broadcasts`); n != 1 {
		t.Fatalf("%d rows after a replay, want 1", n)
	}

	other := inline("launch", "u1")
	other.Tenant = domain.Tenant{Kind: "workspace", ID: "w2"}
	if r := e.createBroadcast(other); !r.Applied || r.BroadcastID == first.BroadcastID {
		t.Fatalf("another tenant's broadcast under the same key = %+v, want its own", r)
	}
	if r, err := e.s.CreateBroadcast(t.Context(), "other-realm", inline("launch", "u1")); err != nil || !r.Applied || r.BroadcastID == first.BroadcastID {
		t.Fatalf("another realm's broadcast under the same key = %+v, %v, want its own", r, err)
	}
	if n := e.count(`SELECT count(*) FROM notification_broadcasts`); n != 3 {
		t.Fatalf("%d rows, want 3", n)
	}
}

// A replay does not touch what the first call recorded: a producer that retries with a
// different list does not change who the broadcast goes to. The key is the identity.
func TestCreateBroadcastReplayDoesNotChangeTheRecordedBroadcast(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.createBroadcast(inline("launch", "u1", "u2"))
	if r := e.createBroadcast(inline("launch", "u9")); r.Applied {
		t.Fatalf("replay = %+v, want not applied", r)
	}
	job := e.leaseBroadcast()
	if job == nil || len(job.Request.Recipients) != 2 || job.Request.Recipients[0].ID != "u1" {
		t.Fatalf("leased %+v, want the first call's two recipients", job)
	}
}

// The schema allows one source of recipients. A request with none, or with both, is an
// error of the store's, and no row is left behind. (The domain refuses it first; this is
// the floor under it.)
func TestCreateBroadcastRefusesARequestWithoutExactlyOneSource(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	both := inline("both", "u1")
	both.Audience = "everyone"
	for name, req := range map[string]domain.BroadcastRequest{"none": inline("none"), "both": both} {
		if _, err := e.s.CreateBroadcast(t.Context(), "aisat", req); err == nil {
			t.Errorf("%s: CreateBroadcast succeeded, want an error", name)
		}
	}
	if n := e.count(`SELECT count(*) FROM notification_broadcasts`); n != 0 {
		t.Fatalf("%d rows after the refusals, want none", n)
	}
}

// ----------------------------------------------------------------- leasing --

// A worker is handed the broadcast as it was recorded, at the cursor it stands at, under a
// token that is its own.
func TestALeasedBroadcastCarriesItsRequestIntact(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	req := inline("launch", "u1", "u2")
	req.Priority = domain.PriorityCritical
	req.Data = map[string]any{"name": "Atlas", "tags": []any{"a", "b"}, "nested": map[string]any{"n": float64(3)}}
	req.Title, req.Body = "Atlas is live", "Come and see."
	req.Payload = map[string]string{"url": "/launch"}
	receipt := e.createBroadcast(req)

	job := e.leaseBroadcast()
	if job == nil {
		t.Fatal("LeaseBroadcast = nil, want the broadcast")
	}
	if job.ID != receipt.BroadcastID || job.Realm != "aisat" || job.Cursor != "" || job.LeaseToken == "" {
		t.Errorf("job = %+v, want the receipt's id, realm aisat, an empty cursor and a token", job)
	}
	if !reflect.DeepEqual(job.Request, req) {
		t.Errorf("job.Request = %+v, want %+v", job.Request, req)
	}

	aud := selector("selector", "everyone")
	e.createBroadcast(aud)
	next := e.leaseBroadcast()
	if next == nil || !reflect.DeepEqual(next.Request, aud) || next.Request.Recipients != nil {
		t.Errorf("leased %+v, want the audience broadcast with no recipients", next)
	}
}

// A broadcast is held by one worker at a time, and a worker that died frees it when its
// lease lapses, at the cursor the last commit left.
func TestABroadcastIsLeasedToOneWorkerAtATimeAndFreedWhenTheLeaseLapses(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	if job := e.leaseBroadcast(); job != nil {
		t.Fatalf("LeaseBroadcast with nothing recorded = %+v, want nil", job)
	}
	e.createBroadcast(inline("launch", "u1"))

	first := e.leaseBroadcast()
	if first == nil {
		t.Fatal("LeaseBroadcast = nil, want the broadcast")
	}
	if again := e.leaseBroadcast(); again != nil {
		t.Fatalf("a second LeaseBroadcast while the lease is live = %+v, want nil", again)
	}
	e.lapseBroadcastLeases()
	second := e.leaseBroadcast()
	if second == nil || second.ID != first.ID || second.LeaseToken == first.LeaseToken {
		t.Fatalf("after the lease lapsed: %+v, want the same broadcast under a new token (was %s)", second, first.LeaseToken)
	}
}

// Workers that look at the same moment do not take the same broadcast: SKIP LOCKED hands
// each a different one, and a lone broadcast to exactly one of them.
func TestWorkersContendingForOneBroadcastTakeItOnce(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.createBroadcast(inline("launch", "u1"))

	const workers = 12
	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		taken []string
		errs  []error
	)
	for range workers {
		wg.Go(func() {
			job, err := e.s.LeaseBroadcast(t.Context(), lease)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
			}
			if job != nil {
				taken = append(taken, job.LeaseToken)
			}
		})
	}
	wg.Wait()
	if len(errs) > 0 || len(taken) != 1 {
		t.Fatalf("%d workers took the broadcast (errors %v), want exactly one", len(taken), errs)
	}
}

func TestTheOldestPendingBroadcastIsLeasedFirst(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	for i, key := range []string{"first", "second", "third"} {
		e.createBroadcast(inline(key, "u1"))
		if _, err := e.db.Admin.Exec(t.Context(),
			`UPDATE notification_broadcasts SET created_at = created_at + $2::int * interval '1 minute' WHERE idem_key = $1`, key, i); err != nil {
			t.Fatal(err)
		}
	}
	var got []string
	for range 3 {
		job := e.leaseBroadcast()
		if job == nil {
			t.Fatal("LeaseBroadcast = nil, want the next broadcast")
		}
		got = append(got, job.Request.IdemKey)
	}
	if want := []string{"first", "second", "third"}; !slices.Equal(got, want) {
		t.Fatalf("leased in the order %v, want %v: the oldest waits longest", got, want)
	}
}

// ------------------------------------------------------------ committing --

// A page and the cursor's advance are one transaction: the notifications, their guards and
// their queue rows are there together with the new cursor, the count, and a lease that has
// ended, so the next page may be taken at once by anyone.
func TestCommitBroadcastPageWritesThePageAndAdvancesTheCursorTogether(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.createBroadcast(selector("launch", "everyone"))
	job := e.leaseBroadcast()

	if err := e.commit(job, pageOf("launch", "u1", "u2"), "c2", false); err != nil {
		t.Fatalf("CommitBroadcastPage: %v", err)
	}
	if n, g, q := e.written(); n != 2 || g != 2 || q != 2 {
		t.Fatalf("wrote %d notifications, %d guards and %d queue rows, want 2 of each", n, g, q)
	}
	for _, r := range []string{"u1", "u2"} {
		want := domain.BroadcastMemberKey("launch", domain.Recipient{Kind: "user", ID: r})
		if n := e.count(`SELECT count(*) FROM notifications WHERE recipient_id = $1 AND idem_key = $2 AND realm = 'aisat' AND tenant_id = 'w1'`, r, want); n != 1 {
			t.Errorf("%s: %d inbox rows under the member key, want 1", r, n)
		}
	}
	if got := e.broadcastRow("launch"); got != (broadcastRow{cursor: "c2", fanned: 2, status: "pending"}) {
		t.Fatalf("broadcast row = %+v, want cursor c2, 2 fanned, pending, no lease", got)
	}
	again := e.leaseBroadcast()
	if again == nil || again.Cursor != "c2" || again.LeaseToken == job.LeaseToken {
		t.Fatalf("next lease = %+v, want the broadcast at cursor c2 under a new token, with no wait for the old lease to lapse", again)
	}
}

// The last page completes the broadcast: it is finished, stamped, and never leased again,
// and its token no longer commits anything.
func TestTheLastPageCompletesTheBroadcast(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.createBroadcast(inline("launch", "u1", "u2", "u3"))
	job := e.leaseBroadcast()

	if err := e.commit(job, pageOf("launch", "u1", "u2", "u3"), "", true); err != nil {
		t.Fatalf("CommitBroadcastPage: %v", err)
	}
	if got := e.broadcastRow("launch"); got != (broadcastRow{cursor: "", fanned: 3, status: "completed", done: true}) {
		t.Fatalf("broadcast row = %+v, want completed, stamped, 3 fanned, no lease", got)
	}
	if again := e.leaseBroadcast(); again != nil {
		t.Fatalf("a completed broadcast was leased: %+v", again)
	}
	if err := e.commit(job, pageOf("launch", "u4"), "", true); !errors.Is(err, domain.ErrBroadcastLeaseLost) {
		t.Fatalf("a commit under the token of a completed broadcast = %v, want ErrBroadcastLeaseLost", err)
	}
	if n, _, _ := e.written(); n != 3 {
		t.Fatalf("%d notifications, want the 3 of the page: nothing may be added to a completed broadcast", n)
	}
}

// An empty last page, which a resolver may end on, still completes the broadcast.
func TestAnEmptyPageStillAdvancesAndCompletes(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.createBroadcast(selector("launch", "everyone"))
	job := e.leaseBroadcast()
	if err := e.commit(job, nil, "c1", false); err != nil {
		t.Fatalf("an empty page: %v", err)
	}
	if got := e.broadcastRow("launch"); got.cursor != "c1" || got.fanned != 0 || got.status != "pending" {
		t.Fatalf("broadcast row = %+v, want the cursor advanced over an empty page", got)
	}
	job = e.leaseBroadcast()
	if err := e.commit(job, nil, "", true); err != nil {
		t.Fatalf("the empty last page: %v", err)
	}
	if got := e.broadcastRow("launch"); got.status != "completed" || !got.done {
		t.Fatalf("broadcast row = %+v, want completed", got)
	}
}

// The fence: only the worker that holds the lease may commit, and the page of one that does
// not is discarded whole. A lapsed worker that wakes up and writes would otherwise advance
// the cursor past pages another worker is still expanding, or write a page twice.
func TestCommitBroadcastPageIsFencedByTheLeaseToken(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		job  func(e *env, held *domain.BroadcastJob) domain.BroadcastJob
		why  string
	}{
		{"a token that was never issued", func(_ *env, held *domain.BroadcastJob) domain.BroadcastJob {
			j := *held
			j.LeaseToken = "11111111-1111-4111-8111-111111111111"
			return j
		}, ""},
		{"no token", func(_ *env, held *domain.BroadcastJob) domain.BroadcastJob {
			j := *held
			j.LeaseToken = ""
			return j
		}, ""},
		{"another tenant's broadcast under the same key", func(_ *env, held *domain.BroadcastJob) domain.BroadcastJob {
			j := *held
			j.Request.Tenant = domain.Tenant{Kind: "workspace", ID: "w2"}
			return j
		}, "the tenant is in the broadcast's key (D17)"},
		{"another realm's", func(_ *env, held *domain.BroadcastJob) domain.BroadcastJob {
			j := *held
			j.Realm = "other-realm"
			return j
		}, "the realm is in the broadcast's key (D1)"},
		{"the token of a worker whose lease lapsed and was taken", func(e *env, held *domain.BroadcastJob) domain.BroadcastJob {
			e.lapseBroadcastLeases()
			if taken := e.leaseBroadcast(); taken == nil {
				e.t.Fatal("the lapsed broadcast was not leased again")
			}
			return *held
		}, "the stale worker's write is discarded (the fence)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			e.createBroadcast(selector("launch", "everyone"))
			held := e.leaseBroadcast()
			stale := tc.job(e, held)

			err := e.s.CommitBroadcastPage(t.Context(), stale, pageOf("launch", "u1", "u2"), "c2", false)
			if !errors.Is(err, domain.ErrBroadcastLeaseLost) {
				t.Fatalf("CommitBroadcastPage = %v, want ErrBroadcastLeaseLost: %s", err, tc.why)
			}
			if n, g, q := e.written(); n != 0 || g != 0 || q != 0 {
				t.Errorf("a refused commit wrote %d notifications, %d guards and %d queue rows, want nothing", n, g, q)
			}
			if got := e.broadcastRow("launch"); got.cursor != "" || got.fanned != 0 || got.status != "pending" {
				t.Errorf("broadcast row = %+v after a refused commit, want it untouched", got)
			}
		})
	}
}

// A worker that lost its lease mid-page and one that took the page over both commit it: the
// page is written once, by the holder, and the loser learns it lost.
func TestAStaleWorkerAndTheHolderCommitTheSamePageOnce(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.createBroadcast(selector("launch", "everyone"))
	stale := e.leaseBroadcast()
	e.lapseBroadcastLeases()
	holder := e.leaseBroadcast()

	if err := e.commit(stale, pageOf("launch", "u1", "u2"), "c2", false); !errors.Is(err, domain.ErrBroadcastLeaseLost) {
		t.Fatalf("the stale worker's commit = %v, want ErrBroadcastLeaseLost", err)
	}
	if err := e.commit(holder, pageOf("launch", "u1", "u2"), "c2", false); err != nil {
		t.Fatalf("the holder's commit: %v", err)
	}
	if n, g, q := e.written(); n != 2 || g != 2 || q != 2 {
		t.Fatalf("wrote %d notifications, %d guards and %d queue rows, want 2 of each: the page is written once", n, g, q)
	}
	if got := e.broadcastRow("launch"); got.fanned != 2 || got.cursor != "c2" {
		t.Fatalf("broadcast row = %+v, want the page counted once", got)
	}
}

// A kill inside the page's transaction: the third notification of a page cannot be written,
// and the first two, the guards, the queue rows and the cursor's advance go with it, because
// they were one transaction. The lease is still held, so no one else starts the page until
// it lapses; then the page is taken again from where it began, and goes through whole.
func TestAPageThatFailsInsideItsTransactionRollsBackAsAWhole(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.createBroadcast(selector("launch", "everyone"))
	job := e.leaseBroadcast()

	if _, err := e.db.Admin.Exec(t.Context(), `
CREATE FUNCTION kill_mid_page() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.recipient_id = 'u3' THEN RAISE EXCEPTION 'killed mid-page'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER kill_mid_page BEFORE INSERT ON notifications FOR EACH ROW EXECUTE FUNCTION kill_mid_page();`); err != nil {
		t.Fatalf("fixture: %v", err)
	}

	err := e.commit(job, pageOf("launch", "u1", "u2", "u3", "u4"), "c4", false)
	if err == nil || errors.Is(err, domain.ErrBroadcastLeaseLost) {
		t.Fatalf("CommitBroadcastPage = %v, want the fault of the write, which is not a lost lease", err)
	}
	if n, g, q := e.written(); n != 0 || g != 0 || q != 0 {
		t.Fatalf("the failed page left %d notifications, %d guards and %d queue rows, want nothing: a page rolls back as a whole", n, g, q)
	}
	if got := e.broadcastRow("launch"); got != (broadcastRow{cursor: "", fanned: 0, status: "pending", leased: true}) {
		t.Fatalf("broadcast row = %+v, want the cursor where the page began, and the lease still held", got)
	}
	if again := e.leaseBroadcast(); again != nil {
		t.Fatalf("the broadcast was leased while the failed page's lease was live: %+v", again)
	}

	if _, err := e.db.Admin.Exec(t.Context(), `DROP TRIGGER kill_mid_page ON notifications`); err != nil {
		t.Fatal(err)
	}
	e.lapseBroadcastLeases()
	retry := e.leaseBroadcast()
	if retry == nil || retry.Cursor != "" {
		t.Fatalf("after the lease lapsed: %+v, want the broadcast at the cursor the failed page began at", retry)
	}
	if err := e.commit(retry, pageOf("launch", "u1", "u2", "u3", "u4"), "c4", false); err != nil {
		t.Fatalf("the page, again: %v", err)
	}
	if n, g, q := e.written(); n != 4 || g != 4 || q != 4 {
		t.Fatalf("wrote %d notifications, %d guards and %d queue rows, want 4 of each", n, g, q)
	}
}

// A kill between pages: the page that committed stays, and the broadcast resumes at the
// cursor it left, not at the start. The worker that died holding the next page's lease
// leaves nothing behind but a lease that lapses.
func TestABroadcastResumesAtTheCursorOfItsLastCommittedPage(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.createBroadcast(selector("launch", "everyone"))

	first := e.leaseBroadcast()
	if err := e.commit(first, pageOf("launch", "u1", "u2"), "c2", false); err != nil {
		t.Fatal(err)
	}
	dying := e.leaseBroadcast() // takes the second page, and is killed before it commits
	if dying == nil || dying.Cursor != "c2" {
		t.Fatalf("second lease = %+v, want cursor c2", dying)
	}
	e.lapseBroadcastLeases()

	resumed := e.leaseBroadcast()
	if resumed == nil || resumed.Cursor != "c2" {
		t.Fatalf("after the kill: %+v, want the broadcast at cursor c2: not restarted from the first page", resumed)
	}
	if err := e.commit(resumed, pageOf("launch", "u3", "u4"), "", true); err != nil {
		t.Fatal(err)
	}
	if n, _, _ := e.written(); n != 4 {
		t.Fatalf("%d notifications, want 4: each recipient once", n)
	}
	if got := e.broadcastRow("launch"); got.status != "completed" || got.fanned != 4 {
		t.Fatalf("broadcast row = %+v, want completed with 4 fanned", got)
	}
}

// A resolver whose membership moves while it pages may hand the same recipient back on two
// pages. The second is the guard's replay: one notification, one set of queue rows, and the
// count is of what was written, not of what was handed in.
func TestARecipientOnTwoPagesIsNotifiedOnce(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.createBroadcast(selector("launch", "everyone"))

	job := e.leaseBroadcast()
	if err := e.commit(job, pageOf("launch", "u1", "u2"), "c2", false); err != nil {
		t.Fatal(err)
	}
	job = e.leaseBroadcast()
	if err := e.commit(job, pageOf("launch", "u2", "u3"), "", true); err != nil {
		t.Fatal(err)
	}
	if n, g, q := e.written(); n != 3 || g != 3 || q != 3 {
		t.Fatalf("wrote %d notifications, %d guards and %d queue rows, want 3 of each: u2 once", n, g, q)
	}
	if got := e.broadcastRow("launch"); got.fanned != 3 {
		t.Fatalf("fanned = %d, want 3: the replay of u2 is not counted", got.fanned)
	}
}

// Nobody is notified twice by a broadcast retried under the same key, after the first was
// done: its member keys are the same, so its guards are taken. A different broadcast to
// the same people is a different set of keys, and does notify them.
func TestAMemberIsNotifiedOncePerBroadcastKey(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.createBroadcast(inline("launch", "u1", "u2"))
	job := e.leaseBroadcast()
	if err := e.commit(job, pageOf("launch", "u1", "u2"), "", true); err != nil {
		t.Fatal(err)
	}

	// The same key again is a replay, and records nothing new to expand.
	if r := e.createBroadcast(inline("launch", "u1", "u2")); r.Applied {
		t.Fatalf("a retried broadcast was applied: %+v", r)
	}
	if again := e.leaseBroadcast(); again != nil {
		t.Fatalf("a retried broadcast was leased: %+v", again)
	}

	e.createBroadcast(inline("launch-2", "u1", "u2"))
	job = e.leaseBroadcast()
	if err := e.commit(job, pageOf("launch-2", "u1", "u2"), "", true); err != nil {
		t.Fatal(err)
	}
	if n, _, _ := e.written(); n != 4 {
		t.Fatalf("%d notifications, want 4: two broadcasts to two people", n)
	}
}

// A page writes into the tenant and realm its broadcast was recorded under and no other:
// an item for another tenant is refused with the whole page, so a broadcast cannot put a
// notification into a tenant's inbox it was not sent to (NR-008).
func TestAPageCannotWriteOutsideItsBroadcastsTenant(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.createBroadcast(selector("launch", "everyone"))
	job := e.leaseBroadcast()

	page := pageOf("launch", "u1", "u2")
	page[1].Notification.Tenant = domain.Tenant{Kind: "workspace", ID: "w2"}
	if err := e.commit(job, page, "c2", false); err == nil || errors.Is(err, domain.ErrBroadcastLeaseLost) {
		t.Fatalf("CommitBroadcastPage = %v, want a refusal that is not a lost lease", err)
	}
	if n, g, q := e.written(); n != 0 || g != 0 || q != 0 {
		t.Fatalf("a refused page wrote %d notifications, %d guards and %d queue rows, want nothing", n, g, q)
	}
	if got := e.broadcastRow("launch"); got.cursor != "" || got.fanned != 0 {
		t.Fatalf("broadcast row = %+v, want it untouched", got)
	}
}

// The notifications of a page are written under the broadcast's realm, which is the
// job's, and are the recipients' own: they are visible in the inbox under the scope that
// reads them, and under no other.
func TestAPageIsWrittenUnderTheBroadcastsRealmAndTenant(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	if _, err := e.s.CreateBroadcast(t.Context(), "product-b", inline("launch", "u1")); err != nil {
		t.Fatal(err)
	}
	job := e.leaseBroadcast()
	if job == nil || job.Realm != "product-b" {
		t.Fatalf("leased %+v, want the product-b broadcast", job)
	}
	if err := e.commit(job, pageOf("launch", "u1"), "", true); err != nil {
		t.Fatal(err)
	}
	if n := e.count(`SELECT count(*) FROM notifications WHERE realm = 'product-b' AND tenant_id = 'w1' AND recipient_id = 'u1'`); n != 1 {
		t.Fatalf("%d notifications under product-b, want 1", n)
	}
	if n := e.count(`SELECT count(*) FROM notifications WHERE realm = 'aisat'`); n != 0 {
		t.Fatalf("%d notifications under aisat, want none", n)
	}
}

// A page is bounded work for one transaction, and it is what the caller decides: a page of
// many is still one transaction, and still all or nothing.
func TestALargePageIsOneTransaction(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.createBroadcast(selector("launch", "everyone"))
	job := e.leaseBroadcast()

	var who []string
	for i := range 300 {
		who = append(who, fmt.Sprintf("u%d", i))
	}
	if err := e.commit(job, pageOf("launch", who...), "c300", false); err != nil {
		t.Fatalf("CommitBroadcastPage of 300: %v", err)
	}
	if n, g, q := e.written(); n != 300 || g != 300 || q != 300 {
		t.Fatalf("wrote %d notifications, %d guards and %d queue rows, want 300 of each", n, g, q)
	}
	if got := e.broadcastRow("launch"); got.fanned != 300 || got.cursor != "c300" {
		t.Fatalf("broadcast row = %+v, want 300 fanned at cursor c300", got)
	}
}
