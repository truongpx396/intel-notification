//go:build integration

package app_test

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	notify "github.com/truongpx396/intel-notification"
	"github.com/truongpx396/intel-notification/app"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/notifytest"
	"github.com/truongpx396/intel-notification/ports"
)

// T035 and T034a: the digest flush job, end to end on the Env's real store, queue
// and dispatcher. A window's flush time is the database's now(), as every due time
// in the queue is, so a test that needs a window to be due ages it with the admin
// connection, which a fixture may use and the code under test never does.

// digestEnv is an Env whose recipient asked for an hourly digest and has an email
// address, with a scripted email channel that remembers what it was asked to send.
func digestEnv(t *testing.T) (*notifytest.Env, domain.Identity, *notifytest.Scripted) {
	t.Helper()
	e, id := emailEnv(t)
	email := notifytest.NewScripted("email", domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupProvider, DedupWindow: time.Hour})
	e.Channels.Register(email)
	if err := e.Prefs.SetSchedule(t.Context(), id, domain.DeliverySchedule{Digest: time.Hour}); err != nil {
		t.Fatal(err)
	}
	return e, id, email
}

func newFlusher(t *testing.T, e *notifytest.Env, opts ...app.DigestOption) *app.DigestFlusher {
	t.Helper()
	return newFlusherOn(t, e, e.Digests, opts...)
}

func newFlusherOn(t *testing.T, e *notifytest.Env, digests ports.Digests, opts ...app.DigestOption) *app.DigestFlusher {
	t.Helper()
	f, err := app.NewDigestFlusher(e.Config, app.Deps{Digests: digests, Maintenance: e.Maintenance, Clock: e.Clock}, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func sendNote(t *testing.T, e *notifytest.Env, id domain.Identity, n domain.Notification) {
	t.Helper()
	n.Tenant, n.Recipient, n.OccurredAt = id.Tenant, id.Recipient, notifytest.Epoch
	if n.Title == "" {
		n.Title = "Done"
	}
	if r, err := e.Notifier.Notify(t.Context(), n); err != nil || !r.Applied {
		t.Fatalf("Notify(%s) = %+v, %v", n.IdemKey, r, err)
	}
}

// ageWindows makes every window still open due now, as a window is once its hour has
// passed.
func ageWindows(t *testing.T, e *notifytest.Env) {
	t.Helper()
	if _, err := e.DB.Admin.Exec(t.Context(), `UPDATE digest_buffer SET flush_at = now() - interval '1 minute' WHERE flushed_at IS NULL`); err != nil {
		t.Fatal(err)
	}
}

// lapseLease ends every job lease, as the lease of a worker that died does at its ttl.
func lapseLease(t *testing.T, e *notifytest.Env) {
	t.Helper()
	if _, err := e.DB.Admin.Exec(t.Context(), `UPDATE notify_job_leases SET lease_expires_at = now() - interval '1 second'`); err != nil {
		t.Fatal(err)
	}
}

// drainAll runs the dispatcher over every shard until nothing is due, and returns
// how many deliveries it handled.
func drainAll(t *testing.T, e *notifytest.Env) int {
	t.Helper()
	handled := 0
	for shard := range e.Config.WithDefaults().Shards {
		for {
			n, err := e.Dispatcher.Drain(t.Context(), domain.Shard(shard), 100)
			if err != nil {
				t.Fatalf("Drain(shard %d): %v", shard, err)
			}
			handled += n
			if n == 0 {
				break
			}
		}
	}
	return handled
}

func randomUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

// ------------------------------------------------------------------ T035 --

// A burst of N notifications for one recipient and topic yields ceil(N / DigestMax)
// deliveries on an email channel, however N falls against the window, and every
// notification is still a record of its own (NS-005, NR-014).
func TestABurstYieldsOneDigestPerDigestMaxMembers(t *testing.T) {
	t.Parallel()
	max := notify.Config{}.WithDefaults().DigestMax
	cases := []struct {
		name    string
		n       int
		digests int
	}{
		{"one short of a full window", max - 1, 1},
		{"exactly a full window", max, 1},
		{"one more than a full window", max + 1, 2},
		{"two full windows and one", 2*max + 1, 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e, id, email := digestEnv(t)
			for i := range tc.n {
				sendNote(t, e, id, domain.Notification{Topic: "t", IdemKey: fmt.Sprintf("k%d", i)})
			}
			if got := count(t, e, `SELECT count(*) FROM notifications`); got != tc.n {
				t.Fatalf("%d notifications persisted, want %d: a digest coalesces deliveries, never records", got, tc.n)
			}
			if got := count(t, e, `SELECT count(*) FROM notification_outbox`); got != 0 {
				t.Fatalf("%d deliveries queued by the burst, want none: they wait in windows", got)
			}
			ageWindows(t, e)

			flushed, err := newFlusher(t, e).Flush(t.Context())
			if err != nil || flushed != tc.digests {
				t.Fatalf("Flush = %d, %v, want %d windows flushed", flushed, err, tc.digests)
			}
			if got := drainAll(t, e); got != tc.digests {
				t.Fatalf("the dispatcher handled %d deliveries, want %d: ceil(%d / %d)", got, tc.digests, tc.n, max)
			}

			sent := email.Deliveries()
			if len(sent) != tc.digests {
				t.Fatalf("the channel was asked %d times, want %d", len(sent), tc.digests)
			}
			members := map[string]bool{}
			for _, d := range sent {
				if len(d.Digest) > max {
					t.Errorf("a digest carries %d members, over DigestMax %d", len(d.Digest), max)
				}
				for _, m := range d.Digest {
					if members[m.IdemKey] {
						t.Errorf("notification %s is in two digests", m.IdemKey)
					}
					members[m.IdemKey] = true
				}
			}
			if len(members) != tc.n {
				t.Errorf("the digests carry %d distinct notifications, want all %d", len(members), tc.n)
			}
		})
	}
}

// A critical notification, and one on an essential topic, is never folded into a window
// however many arrive: each is its own delivery, at once (NR-014).
func TestACriticalOrEssentialBurstIsNeverDigested(t *testing.T) {
	t.Parallel()
	max := notify.Config{}.WithDefaults().DigestMax
	n := max + 1
	cases := []struct {
		name string
		note domain.Notification
		def  domain.TopicDef
	}{
		{"critical", domain.Notification{Topic: "alert", Priority: domain.PriorityCritical},
			domain.TopicDef{DefaultChannels: []domain.ChannelKind{"email"}, DefaultPriority: domain.PriorityInfo}},
		{"critical by the topic's own default", domain.Notification{Topic: "alert"},
			domain.TopicDef{DefaultChannels: []domain.ChannelKind{"email"}, DefaultPriority: domain.PriorityCritical}},
		{"essential", domain.Notification{Topic: "alert"},
			domain.TopicDef{DefaultChannels: []domain.ChannelKind{"email"}, DefaultPriority: domain.PriorityInfo, Essential: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e, id, email := digestEnv(t)
			e.Topics.Register(notifytest.Realm, "alert", tc.def)
			e.Templates.Put(notifytest.Realm, "alert", "email", "en", domain.RenderedContent{Subject: "S", Body: "B"})
			for i := range n {
				note := tc.note
				note.IdemKey = fmt.Sprintf("k%d", i)
				sendNote(t, e, id, note)
			}
			if got := count(t, e, `SELECT count(*) FROM digest_buffer`); got != 0 {
				t.Fatalf("%d windows opened, want none: %s notifications are never digested", got, tc.name)
			}
			if got := count(t, e, `SELECT count(*) FROM notification_outbox`); got != n {
				t.Fatalf("%d deliveries queued, want %d, one each", got, n)
			}
			ageWindows(t, e)
			if flushed, err := newFlusher(t, e).Flush(t.Context()); err != nil || flushed != 0 {
				t.Fatalf("Flush = %d, %v, want nothing to flush", flushed, err)
			}
			drainAll(t, e)
			if sent := email.Deliveries(); len(sent) != n {
				t.Fatalf("the channel was asked %d times, want %d: one per notification", len(sent), n)
			}
			for _, d := range email.Deliveries() {
				if len(d.Digest) != 0 || d.DigestID != "" {
					t.Fatalf("a delivery carries a digest (%d members), want the notification itself", len(d.Digest))
				}
			}
		})
	}
}

// ----------------------------------------------------------------- T034a --

// Two flushers of one window produce one delivery, and only the one holding the job's
// lease runs (D22). The window's flush is also idempotent in the store, so this does
// not rest on the lease alone: the lease is shown by the next test.
func TestTwoFlushersOfOneWindowProduceOneDelivery(t *testing.T) {
	t.Parallel()
	e, id, email := digestEnv(t)
	for i := range 3 {
		sendNote(t, e, id, domain.Notification{Topic: "t", IdemKey: fmt.Sprintf("k%d", i)})
	}
	ageWindows(t, e)

	var (
		wg      sync.WaitGroup
		flushed atomic.Int64
	)
	for i := range 4 {
		f := newFlusher(t, e, app.WithFlushOwner(fmt.Sprintf("worker-%d", i)))
		wg.Go(func() {
			n, err := f.Flush(t.Context())
			if err != nil {
				t.Errorf("Flush: %v", err)
			}
			flushed.Add(int64(n))
		})
	}
	wg.Wait()

	if flushed.Load() != 1 {
		t.Fatalf("the flushers flushed %d windows between them, want 1", flushed.Load())
	}
	if got := count(t, e, `SELECT count(*) FROM notification_outbox WHERE digest_id IS NOT NULL`); got != 1 {
		t.Fatalf("%d digest deliveries queued, want 1", got)
	}
	drainAll(t, e)
	if sent := email.Deliveries(); len(sent) != 1 || len(sent[0].Digest) != 3 {
		t.Fatalf("the channel was asked %d times, want once with the 3 members", len(sent))
	}
}

// A flusher that does not hold the job's lease flushes nothing, and the window waits
// for whoever does: a lease is how a job is single-owner, so it is the lease that is
// asserted, not the idempotence of the flush (D22).
func TestAFlusherWithoutTheLeaseFlushesNothing(t *testing.T) {
	t.Parallel()
	e, id, _ := digestEnv(t)
	sendNote(t, e, id, domain.Notification{Topic: "t", IdemKey: "k1"})
	ageWindows(t, e)
	if held, err := e.Maintenance.TryLeaseJob(t.Context(), app.JobDigestFlush, "another-worker", time.Hour); err != nil || !held {
		t.Fatalf("TryLeaseJob = %v, %v, want the lease taken for the fixture", held, err)
	}

	flushed, err := newFlusher(t, e).Flush(t.Context())
	if err != nil || flushed != 0 {
		t.Fatalf("Flush = %d, %v, want nothing: another worker holds the lease", flushed, err)
	}
	if got := count(t, e, `SELECT count(*) FROM digest_buffer WHERE flushed_at IS NULL`); got != 1 {
		t.Fatalf("%d windows still waiting, want the one a flusher without the lease left alone", got)
	}

	lapseLease(t, e)
	if flushed, err := newFlusher(t, e).Flush(t.Context()); err != nil || flushed != 1 {
		t.Fatalf("Flush after the lease lapsed = %d, %v, want the window flushed", flushed, err)
	}
}

// panickingDigests stands in for a worker that dies at its flush: the window was found
// due and the process went down before it could be flushed.
type panickingDigests struct{ ports.Digests }

func (panickingDigests) FlushDigest(context.Context, string) (bool, error) {
	panic("the worker died at the flush")
}

// failingDigests fails the flush of one window, once.
type failingDigests struct {
	ports.Digests
	mu   sync.Mutex
	fail string
	err  error
}

func (f *failingDigests) FlushDigest(ctx context.Context, id string) (bool, error) {
	f.mu.Lock()
	failing := id == f.fail && f.err != nil
	err := f.err
	if failing {
		f.err = nil
	}
	f.mu.Unlock()
	if failing {
		return false, err
	}
	return f.Digests.FlushDigest(ctx, id)
}

// A window that came due while the worker holding the job died is flushed by the next
// run, once the dead worker's lease has lapsed: nothing in the job remembers which
// windows it meant to flush, so an overdue one is simply due (NR-014).
func TestAWindowOverdueAfterACrashIsFlushedByTheNextRun(t *testing.T) {
	t.Parallel()
	e, id, email := digestEnv(t)
	sendNote(t, e, id, domain.Notification{Topic: "t", IdemKey: "k1"})
	sendNote(t, e, id, domain.Notification{Topic: "t", IdemKey: "k2"})
	ageWindows(t, e)

	doomed := newFlusherOn(t, e, panickingDigests{e.Digests}, app.WithFlushOwner("doomed"))
	func() {
		defer func() { _ = recover() }()
		_, _ = doomed.Flush(t.Context())
		t.Error("the flusher survived a worker that died at the flush")
	}()
	if got := count(t, e, `SELECT count(*) FROM digest_buffer WHERE flushed_at IS NULL`); got != 1 {
		t.Fatalf("%d windows waiting after the crash, want the one it never flushed", got)
	}

	if flushed, err := newFlusher(t, e, app.WithFlushOwner("successor")).Flush(t.Context()); err != nil || flushed != 0 {
		t.Fatalf("Flush while the dead worker's lease is live = %d, %v, want nothing", flushed, err)
	}
	lapseLease(t, e)
	if flushed, err := newFlusher(t, e, app.WithFlushOwner("successor")).Flush(t.Context()); err != nil || flushed != 1 {
		t.Fatalf("Flush after the lease lapsed = %d, %v, want the overdue window flushed", flushed, err)
	}
	drainAll(t, e)
	if sent := email.Deliveries(); len(sent) != 1 || len(sent[0].Digest) != 2 {
		t.Fatalf("the channel was asked %d times, want once with both members", len(sent))
	}
}

// One window that cannot be flushed does not hold the others back, and is flushed by
// the next pass: a fault in one is reported, never a reason to skip the rest.
func TestAWindowThatFailsToFlushDoesNotHoldBackTheOthers(t *testing.T) {
	t.Parallel()
	e, _, _ := digestEnv(t)
	ctx := t.Context()
	var windows []string
	for i := range 3 {
		recipient := domain.Recipient{Kind: "user", ID: fmt.Sprintf("burst-%d", i)}
		w, err := e.Digests.AppendDigest(ctx, domain.DigestMember{
			Identity: domain.Identity{Realm: notifytest.Realm, Tenant: itTenant, Recipient: recipient},
			Topic:    "t", Channel: "email", Shard: 4, NotificationID: randomUUID(),
		}, time.Hour, 100)
		if err != nil {
			t.Fatal(err)
		}
		windows = append(windows, w)
	}
	ageWindows(t, e)

	boom := errors.New("the database hiccuped")
	flaky := &failingDigests{Digests: e.Digests, fail: windows[0], err: boom}
	f := newFlusherOn(t, e, flaky)
	flushed, err := f.Flush(ctx)
	if flushed != 2 || !errors.Is(err, boom) {
		t.Fatalf("Flush = %d, %v, want the 2 healthy windows flushed and the fault returned", flushed, err)
	}
	if flushed, err := f.Flush(ctx); err != nil || flushed != 1 {
		t.Fatalf("the next Flush = %d, %v, want the window that failed", flushed, err)
	}
	if got := count(t, e, `SELECT count(*) FROM notification_outbox WHERE digest_id IS NOT NULL`); got != 3 {
		t.Fatalf("%d digest deliveries, want 3, one per window", got)
	}
}

// A pass flushes every shard and every due window in it, however many batches that is.
func TestAPassFlushesEveryShardAndEveryBatch(t *testing.T) {
	t.Parallel()
	e, _, _ := digestEnv(t)
	ctx := t.Context()
	shards := e.Config.WithDefaults().Shards
	const perShard = 5
	for shard := range shards {
		for i := range perShard {
			_, err := e.Digests.AppendDigest(ctx, domain.DigestMember{
				Identity: domain.Identity{Realm: notifytest.Realm, Tenant: itTenant,
					Recipient: domain.Recipient{Kind: "user", ID: fmt.Sprintf("s%d-u%d", shard, i)}},
				Topic: "t", Channel: "email", Shard: domain.Shard(shard), NotificationID: randomUUID(),
			}, time.Hour, 100)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	ageWindows(t, e)

	// A batch of two against five due per shard: three batches in every shard.
	flushed, err := newFlusher(t, e, app.WithFlushBatch(2)).Flush(ctx)
	want := shards * perShard
	if err != nil || flushed != want {
		t.Fatalf("Flush = %d, %v, want all %d windows of %d shards", flushed, err, want, shards)
	}
	if got := count(t, e, `SELECT count(*) FROM digest_buffer WHERE flushed_at IS NULL`); got != 0 {
		t.Fatalf("%d windows left unflushed", got)
	}
}

// A digest whose members were all canceled before the flush ends canceled, and is
// not sent (NR-014, D4).
func TestADigestWhoseMembersAreAllCanceledEndsCanceled(t *testing.T) {
	t.Parallel()
	e, id, email := digestEnv(t)
	keys := []string{"k1", "k2"}
	for _, k := range keys {
		sendNote(t, e, id, domain.Notification{Topic: "t", IdemKey: k})
	}
	for _, k := range keys {
		if _, err := e.Notifier.Cancel(t.Context(), domain.CancelRequest{Tenant: id.Tenant, Recipient: id.Recipient, IdemKey: k}); err != nil {
			t.Fatal(err)
		}
	}
	ageWindows(t, e)

	if flushed, err := newFlusher(t, e).Flush(t.Context()); err != nil || flushed != 1 {
		t.Fatalf("Flush = %d, %v, want the window flushed: the flusher does not look inside it", flushed, err)
	}
	drainAll(t, e)

	if got := count(t, e, `SELECT count(*) FROM notification_deliveries WHERE outcome = 'canceled'`); got != 1 {
		t.Fatalf("%d deliveries ended canceled, want the digest", got)
	}
	if got := count(t, e, `SELECT count(*) FROM notification_deliveries WHERE outcome = 'delivered'`); got != 0 {
		t.Fatalf("%d deliveries ended delivered, want none", got)
	}
	if sent := email.Deliveries(); len(sent) != 0 {
		t.Fatalf("the channel was asked %d times, want none: an empty digest is not sent", len(sent))
	}
}

// Members come back in the order they joined, minus the canceled one.
func TestADigestOmitsACanceledMemberAfterTheFlush(t *testing.T) {
	t.Parallel()
	e, id, email := digestEnv(t)
	for _, k := range []string{"k1", "k2", "k3"} {
		sendNote(t, e, id, domain.Notification{Topic: "t", IdemKey: k, Title: "Upload " + k})
	}
	ageWindows(t, e)
	if flushed, err := newFlusher(t, e).Flush(t.Context()); err != nil || flushed != 1 {
		t.Fatalf("Flush = %d, %v", flushed, err)
	}
	// Canceled after the flush and before the send: the render reads members when it sends.
	if _, err := e.Notifier.Cancel(t.Context(), domain.CancelRequest{Tenant: id.Tenant, Recipient: id.Recipient, IdemKey: "k2"}); err != nil {
		t.Fatal(err)
	}
	drainAll(t, e)

	sent := email.Deliveries()
	if len(sent) != 1 {
		t.Fatalf("the channel was asked %d times, want once", len(sent))
	}
	var titles []string
	for _, m := range sent[0].Digest {
		titles = append(titles, m.Title)
	}
	if !slices.Equal(titles, []string{"Upload k1", "Upload k3"}) {
		t.Fatalf("members = %v, want k1 and k3 in the order they joined, without the canceled k2", titles)
	}
}
