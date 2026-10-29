package app_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	notify "github.com/truongpx396/intel-notification"
	"github.com/truongpx396/intel-notification/app"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// T034a: the digest flush job's own behaviour, with no database. The Digests and the
// Maintenance behind it are recording stand-ins that script what is due and who holds
// the lease; what a flush does to a window, and what a lease is, are the PostgreSQL
// suites' (TestDigestWindows, TestTryLeaseJob), and the job end to end is in
// digest_integration_test.go.

// flushDigests scripts the windows that are due, and removes one when it is flushed,
// as the store does.
type flushDigests struct {
	ports.Digests

	mu      sync.Mutex
	due     map[domain.Shard][]string
	reads   []flushRead
	flushed []string
	// readErr fails the next DueDigests; flushErr fails the flush of the windows it names;
	// taken answers false for the windows it names, as a flush another worker made first does.
	readErr  error
	flushErr map[string]error
	taken    map[string]bool
}

type flushRead struct {
	at    time.Time
	shard domain.Shard
	max   int
}

func (d *flushDigests) DueDigests(_ context.Context, shard domain.Shard, max int) ([]string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.reads = append(d.reads, flushRead{time.Now(), shard, max})
	if err := d.readErr; err != nil {
		d.readErr = nil
		return nil, err
	}
	ids := d.due[shard]
	return slices.Clone(ids[:min(max, len(ids))]), nil
}

func (d *flushDigests) FlushDigest(_ context.Context, id string) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.flushErr[id]; err != nil {
		return false, err
	}
	for shard, ids := range d.due {
		d.due[shard] = slices.DeleteFunc(ids, func(w string) bool { return w == id })
	}
	if d.taken[id] {
		return false, nil
	}
	d.flushed = append(d.flushed, id)
	return true, nil
}

func (d *flushDigests) shardsRead() []domain.Shard {
	d.mu.Lock()
	defer d.mu.Unlock()
	var out []domain.Shard
	for _, r := range d.reads {
		out = append(out, r.shard)
	}
	return out
}

// flushLeases answers TryLeaseJob from a script and remembers each ask.
type flushLeases struct {
	ports.Maintenance

	mu    sync.Mutex
	asks  []leaseAsk
	grant func(ask int) bool
	err   error
}

type leaseAsk struct {
	job, owner string
	ttl        time.Duration
}

func (l *flushLeases) TryLeaseJob(_ context.Context, job, owner string, ttl time.Duration) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.asks = append(l.asks, leaseAsk{job, owner, ttl})
	if l.err != nil {
		return false, l.err
	}
	return l.grant == nil || l.grant(len(l.asks)), nil
}

func (l *flushLeases) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.asks)
}

type wakeCount struct{ n atomic.Int64 }

func (w *wakeCount) Wake() { w.n.Add(1) }

type flushFx struct {
	cfg     notify.Config
	digests *flushDigests
	leases  *flushLeases
	wakes   *wakeCount
	errs    *errLog
}

func newFlushFx(shards int) *flushFx {
	return &flushFx{
		cfg:     notify.Config{Realm: planRealm, StoreDSN: "x", RedisURL: "y", Shards: shards},
		digests: &flushDigests{due: map[domain.Shard][]string{}, flushErr: map[string]error{}, taken: map[string]bool{}},
		leases:  &flushLeases{},
		wakes:   &wakeCount{},
		errs:    &errLog{},
	}
}

func (f *flushFx) build(t *testing.T, opts ...app.DigestOption) *app.DigestFlusher {
	t.Helper()
	opts = append([]app.DigestOption{app.WithFlushOwner("me"), app.WithFlushErrorHandler(f.errs.add)}, opts...)
	fl, err := app.NewDigestFlusher(f.cfg, app.Deps{Digests: f.digests, Maintenance: f.leases, Waker: f.wakes}, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return fl
}

// ------------------------------------------------------------ the lease --

// The job's lease is taken before any window is read, under the job's name, the
// flusher's own owner name and a ttl that lapses (D22): a worker that does not hold
// it reads and flushes nothing.
func TestTheFlushJobHoldsItsLeaseBeforeReadingAnyWindow(t *testing.T) {
	t.Parallel()
	f := newFlushFx(2)
	f.digests.due[0] = []string{"w1"}
	f.leases.grant = func(int) bool { return false }

	flushed, err := f.build(t).Flush(t.Context())
	if err != nil || flushed != 0 {
		t.Fatalf("Flush = %d, %v, want nothing and no error: another worker holds the lease", flushed, err)
	}
	if reads := f.digests.shardsRead(); len(reads) != 0 {
		t.Fatalf("read the due windows of shards %v without the lease", reads)
	}
	if len(f.digests.flushed) != 0 {
		t.Fatalf("flushed %v without the lease", f.digests.flushed)
	}
	want := leaseAsk{job: app.JobDigestFlush, owner: "me"}
	if len(f.leases.asks) == 0 || f.leases.asks[0].job != want.job || f.leases.asks[0].owner != want.owner || f.leases.asks[0].ttl <= 0 {
		t.Fatalf("lease asks = %+v, want the job %q for owner %q with a ttl that lapses", f.leases.asks, want.job, want.owner)
	}
}

// A lease that cannot be asked for is a fault of the pass, not a lease held.
func TestAFlushJobThatCannotAskForTheLeaseReportsItAndFlushesNothing(t *testing.T) {
	t.Parallel()
	f := newFlushFx(1)
	f.digests.due[0] = []string{"w1"}
	boom := errors.New("the database is away")
	f.leases.err = boom

	flushed, err := f.build(t).Flush(t.Context())
	if flushed != 0 || !errors.Is(err, boom) {
		t.Fatalf("Flush = %d, %v, want nothing flushed and the fault returned", flushed, err)
	}
	if reads := f.digests.shardsRead(); len(reads) != 0 {
		t.Fatalf("read the due windows of shards %v without knowing it held the lease", reads)
	}
}

// The lease is renewed as the pass goes, one shard at a time, and a pass that finds
// it has lost the lease stops where it is: the worker that holds it now owns the rest.
func TestTheFlushJobRenewsItsLeaseBetweenShardsAndStopsWhenItLosesIt(t *testing.T) {
	t.Parallel()
	f := newFlushFx(4)
	for shard := range domain.Shard(4) {
		f.digests.due[shard] = []string{fmt.Sprintf("w%d", shard)}
	}
	f.leases.grant = func(ask int) bool { return ask <= 2 } // shards 0 and 1, then lost

	flushed, err := f.build(t).Flush(t.Context())
	if err != nil || flushed != 2 {
		t.Fatalf("Flush = %d, %v, want the two windows read while the lease was held", flushed, err)
	}
	if got, want := f.digests.shardsRead(), []domain.Shard{0, 1}; !slices.Equal(got, want) {
		t.Fatalf("read shards %v, want %v: nothing is read after the lease is lost", got, want)
	}
	if got := f.leases.count(); got != 3 {
		t.Fatalf("%d lease asks, want 3: one before each shard read, the third refused", got)
	}
}

// ----------------------------------------------------------- the pass --

// A pass reads a batch at a time until a short one, and a window another worker
// flushed first is not counted as this one's, nor does it wake anyone.
func TestAPassReadsBatchesUntilAShortOneAndCountsOnlyItsOwnFlushes(t *testing.T) {
	t.Parallel()
	f := newFlushFx(1)
	f.digests.due[0] = []string{"w1", "w2", "w3", "w4", "w5"}
	f.digests.taken["w3"] = true

	flushed, err := f.build(t, app.WithFlushBatch(2)).Flush(t.Context())
	if err != nil || flushed != 4 {
		t.Fatalf("Flush = %d, %v, want 4: w3 was flushed by someone else", flushed, err)
	}
	var asked []int
	for _, r := range f.digests.reads {
		asked = append(asked, r.max)
	}
	if !slices.Equal(asked, []int{2, 2, 2}) {
		t.Fatalf("batches asked for %v, want 2 each time, until a short batch (3 reads: 2, 2, 1 found)", asked)
	}
	if got := f.wakes.n.Load(); got != 1 {
		t.Fatalf("%d wakes, want 1 for the pass", got)
	}
}

// A window that will not flush is reported, and a batch in which nothing could be
// flushed is not read again: a fault that repeats must end the pass, not spin it.
func TestAPassThatCanFlushNothingDoesNotSpin(t *testing.T) {
	t.Parallel()
	f := newFlushFx(1)
	boom := errors.New("will not flush")
	f.digests.due[0] = []string{"w1", "w2"}
	f.digests.flushErr["w1"], f.digests.flushErr["w2"] = boom, boom

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	flushed, err := f.build(t, app.WithFlushBatch(2)).Flush(ctx)
	if flushed != 0 || !errors.Is(err, boom) {
		t.Fatalf("Flush = %d, %v, want nothing flushed and the fault returned", flushed, err)
	}
	if reads := len(f.digests.shardsRead()); reads != 1 {
		t.Fatalf("%d reads of the shard, want 1: a full batch that none of could be flushed is not read again", reads)
	}
}

// A fault reading one shard does not end the pass: the other shards are read, and the
// fault is returned with what was flushed.
func TestAShardThatCannotBeReadDoesNotEndThePass(t *testing.T) {
	t.Parallel()
	f := newFlushFx(3)
	f.digests.due[1], f.digests.due[2] = []string{"w1"}, []string{"w2"}
	boom := errors.New("shard 0 is away")
	f.digests.readErr = boom // the first read, which is shard 0

	flushed, err := f.build(t).Flush(t.Context())
	if flushed != 2 || !errors.Is(err, boom) {
		t.Fatalf("Flush = %d, %v, want the 2 windows of the healthy shards and the fault", flushed, err)
	}
}

// Whether a worker is woken depends on whether this pass queued a delivery: the
// dispatcher of this process claims it at once and not at its next poll (D22).
func TestAFlushWakesTheDispatcherOnlyWhenItQueuedWork(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		due   []string
		taken []string
		wakes int64
	}{
		{"nothing due", nil, nil, 0},
		{"a window flushed", []string{"w1"}, nil, 1},
		{"several windows flushed wake once", []string{"w1", "w2", "w3"}, nil, 1},
		{"every window was flushed by someone else first", []string{"w1"}, []string{"w1"}, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newFlushFx(1)
			f.digests.due[0] = slices.Clone(tc.due)
			for _, w := range tc.taken {
				f.digests.taken[w] = true
			}
			if _, err := f.build(t).Flush(t.Context()); err != nil {
				t.Fatal(err)
			}
			if got := f.wakes.n.Load(); got != tc.wakes {
				t.Fatalf("%d wakes, want %d", got, tc.wakes)
			}
		})
	}
}

// ------------------------------------------------------------- the loop --

// Run flushes at once, then every interval, and returns nil when its context ends.
func TestRunFlushesAtOnceAndThenEveryInterval(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFlushFx(1)
		fl := f.build(t, app.WithFlushInterval(7*time.Second))
		start := time.Now()

		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- fl.Run(ctx) }()
		time.Sleep(15 * time.Second)
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("Run = %v, want nil when its context ends", err)
		}

		var offsets []time.Duration
		for _, r := range f.digests.reads {
			offsets = append(offsets, r.at.Sub(start))
		}
		want := []time.Duration{0, 7 * time.Second, 14 * time.Second}
		if !slices.Equal(offsets, want) {
			t.Fatalf("passes began at %v, want %v: at once, then every interval", offsets, want)
		}
	})
}

// A pass that failed is reported and the next one still runs: the job is a loop, and
// one bad pass is not its end.
func TestRunReportsAFailedPassAndGoesOn(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFlushFx(1)
		boom := errors.New("the database hiccuped")
		f.digests.readErr = boom
		f.digests.due[0] = []string{"w1"}
		fl := f.build(t, app.WithFlushInterval(time.Second))

		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- fl.Run(ctx) }()
		time.Sleep(1500 * time.Millisecond)
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("Run = %v, want nil", err)
		}

		if errs := f.errs.all(); len(errs) != 1 || !errors.Is(errs[0], boom) {
			t.Fatalf("reported %v, want the one fault", errs)
		}
		if !slices.Equal(f.digests.flushed, []string{"w1"}) {
			t.Fatalf("flushed %v, want w1 on the pass after the fault", f.digests.flushed)
		}
	})
}

// A context that has already ended is a worker told to stop before it started: no pass.
func TestRunStopsAtOnceWhenItsContextHasEnded(t *testing.T) {
	t.Parallel()
	f := newFlushFx(1)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := f.build(t).Run(ctx); err != nil {
		t.Fatalf("Run = %v, want nil", err)
	}
	if reads := f.digests.shardsRead(); len(reads) != 0 {
		t.Fatalf("read shards %v after the context ended", reads)
	}
}

// ------------------------------------------------------------ the wiring --

// A flusher names every port it needs and is not built from a configuration that does
// not validate, nor from an option that could not work.
func TestNewDigestFlusherRefusesWhatCouldNotWork(t *testing.T) {
	t.Parallel()
	valid := notify.Config{Realm: planRealm, StoreDSN: "x", RedisURL: "y"}
	deps := app.Deps{Digests: &flushDigests{}, Maintenance: &flushLeases{}}
	cases := []struct {
		name string
		cfg  notify.Config
		deps app.Deps
		opts []app.DigestOption
		want []string
	}{
		{"no ports", valid, app.Deps{}, nil, []string{"Deps.Digests", "Deps.Maintenance"}},
		{"no maintenance", valid, app.Deps{Digests: &flushDigests{}}, nil, []string{"Deps.Maintenance"}},
		{"no digests", valid, app.Deps{Maintenance: &flushLeases{}}, nil, []string{"Deps.Digests"}},
		{"a configuration that does not validate", notify.Config{StoreDSN: "x", RedisURL: "y"}, deps, nil, []string{"Realm"}},
		{"an interval that is not positive", valid, deps, []app.DigestOption{app.WithFlushInterval(0)}, []string{"interval"}},
		{"a batch of none", valid, deps, []app.DigestOption{app.WithFlushBatch(0)}, []string{"batch"}},
		{"an owner with no name", valid, deps, []app.DigestOption{app.WithFlushOwner("")}, []string{"owner"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := app.NewDigestFlusher(tc.cfg, tc.deps, tc.opts...)
			if err == nil {
				t.Fatal("NewDigestFlusher succeeded, want an error")
			}
			for _, w := range tc.want {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("error %q does not name %q", err, w)
				}
			}
		})
	}

	if _, err := app.NewDigestFlusher(valid, deps); err != nil {
		t.Fatalf("NewDigestFlusher with the defaults: %v", err)
	}
}

// Two flushers built with no owner name do not share one: a shared name would let
// each renew the other's lease, and the job would run twice.
func TestFlushersBuiltWithoutAnOwnerNameDoNotShareOne(t *testing.T) {
	t.Parallel()
	owners := map[string]bool{}
	for range 8 {
		f := newFlushFx(1)
		fl, err := app.NewDigestFlusher(f.cfg, app.Deps{Digests: f.digests, Maintenance: f.leases})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fl.Flush(t.Context()); err != nil {
			t.Fatal(err)
		}
		owners[f.leases.asks[0].owner] = true
	}
	if len(owners) != 8 {
		t.Fatalf("8 flushers made %d distinct owner names: %v", len(owners), owners)
	}
}
