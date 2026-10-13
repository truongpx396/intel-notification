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
	"github.com/truongpx396/intel-notification/internal/notifytest"
	"github.com/truongpx396/intel-notification/ports"
)

// T030a: the dispatcher's loop, with no database and no sleeps. The tests run inside
// a testing/synctest bubble, where time is virtual: a wait of 400ms costs nothing
// and is exact, so a test can say when each pass happened to the millisecond. The
// queue behind it is a recording stand-in in this file (the kit refuses a fake
// Queue, and this one claims nothing about leases): it scripts what a claim returns
// and says when it was asked.

// loopQueue records every Claim, and finishes whatever it is handed.
type loopQueue struct {
	ports.Queue
	shards int

	mu       sync.Mutex
	calls    []loopCall
	script   func(pass int, shard domain.Shard) ([]domain.Claim, error)
	onClaim  func()
	finished []domain.Finish
}

type loopCall struct {
	at    time.Time
	shard domain.Shard
}

func (q *loopQueue) Claim(_ context.Context, shard domain.Shard, _ int, _ time.Duration) ([]domain.Claim, error) {
	q.mu.Lock()
	idx := len(q.calls)
	q.calls = append(q.calls, loopCall{at: time.Now(), shard: shard})
	script, onClaim := q.script, q.onClaim
	q.mu.Unlock()
	if onClaim != nil {
		onClaim()
	}
	if script == nil {
		return nil, nil
	}
	return script(idx/q.shards, shard)
}

func (q *loopQueue) Finish(_ context.Context, _ domain.Claim, f domain.Finish) (bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.finished = append(q.finished, f)
	return true, nil
}

// passTimes is the instant each pass began: the time of its first claim. A pass
// visits every shard once.
func (q *loopQueue) passTimes() []time.Time {
	q.mu.Lock()
	defer q.mu.Unlock()
	var out []time.Time
	for i := 0; i < len(q.calls); i += q.shards {
		out = append(out, q.calls[i].at)
	}
	return out
}

func (q *loopQueue) shardsOf(pass int) []domain.Shard {
	q.mu.Lock()
	defer q.mu.Unlock()
	var out []domain.Shard
	for _, c := range q.calls[pass*q.shards : (pass+1)*q.shards] {
		out = append(out, c.shard)
	}
	return out
}

func gaps(times []time.Time) []time.Duration {
	var out []time.Duration
	for i := 1; i < len(times); i++ {
		out = append(out, times[i].Sub(times[i-1]))
	}
	return out
}

func ms(n ...int) []time.Duration {
	out := make([]time.Duration, len(n))
	for i, v := range n {
		out[i] = time.Duration(v) * time.Millisecond
	}
	return out
}

func equalDurations(a, b []time.Duration) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// expiredClaim is a claim the dispatcher finishes on sight, so a pass has work
// without a store: its delivery deadline is past.
func expiredClaim(id string, clock *notifytest.Clock) domain.Claim {
	past := clock.Now().Add(-time.Hour)
	return domain.Claim{LeaseToken: "token-" + id, Entry: domain.OutboxEntry{
		ID: id, NotificationID: "n-" + id, Channel: "email", Attempts: 1, DeliverBefore: &past,
		Identity: domain.Identity{Realm: "aisat", Tenant: domain.Tenant{Kind: "workspace", ID: "w1"},
			Recipient: domain.Recipient{Kind: "user", ID: "u1"}},
	}}
}

type loopFx struct {
	t     *testing.T
	q     *loopQueue
	clock *notifytest.Clock
	cfg   notify.Config
	deps  app.Deps
	errs  *errLog
}

type errLog struct {
	mu   sync.Mutex
	list []error
}

func (l *errLog) add(err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.list = append(l.list, err)
}

func (l *errLog) all() []error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]error(nil), l.list...)
}

// stubStore and stubSuppressions are ports a loop test never reaches: calling any
// method on them panics on the nil embedded interface, which is the point.
type (
	stubStore        struct{ ports.Store }
	stubSuppressions struct{ ports.SuppressionStore }
)

func newLoopFx(t *testing.T) *loopFx {
	t.Helper()
	clock := notifytest.NewClock(noon)
	f := &loopFx{t: t, clock: clock, errs: &errLog{},
		q: &loopQueue{shards: 3},
		cfg: notify.Config{Realm: planRealm, StoreDSN: "x", RedisURL: "y", Shards: 3, ClaimBatch: 2,
			PollInterval: 400 * time.Millisecond}}
	f.deps = app.Deps{
		Channels: notifytest.NewRegistry(), Topics: notifytest.NewTopics(), Renderer: notifytest.NewTemplates("en"),
		Addresses: notifytest.NewAddressBook(), Store: stubStore{}, Queue: f.q, Suppressions: stubSuppressions{},
		Quotas: notifytest.NewQuotaCounter(), Clock: clock,
	}
	return f
}

func (f *loopFx) build(opts ...app.DispatcherOption) *app.Dispatcher {
	f.t.Helper()
	all := append([]app.DispatcherOption{
		app.WithRand(func() float64 { return 0 }), // every pass starts at shard 0
		app.WithErrorHandler(f.errs.add),
	}, opts...)
	d, err := app.NewDispatcher(f.cfg, f.deps, all...)
	if err != nil {
		f.t.Fatalf("NewDispatcher: %v", err)
	}
	return d
}

// run starts d and returns a stop that cancels it and waits for Run to return.
func run(t *testing.T, d *app.Dispatcher) (stop func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()
	return func() error {
		cancel()
		return <-done
	}
}

// ------------------------------------------------------------ construction --

// NewDispatcher names every port it needs and is not given, refuses a
// configuration that does not validate, and refuses LISTEN/NOTIFY with nothing to
// listen on: a deployment that turned it on and gets polling alone would not know.
func TestNewDispatcherNamesEveryMissingPortAndRefusesABadConfig(t *testing.T) {
	t.Parallel()
	valid := notify.Config{Realm: planRealm, StoreDSN: "x", RedisURL: "y"}

	_, err := app.NewDispatcher(valid, app.Deps{})
	if err == nil {
		t.Fatal("NewDispatcher with no ports succeeded")
	}
	for _, port := range []string{"Deps.Addresses", "Deps.Channels", "Deps.Queue", "Deps.Quotas", "Deps.Renderer",
		"Deps.Store", "Deps.Suppressions", "Deps.Topics"} {
		if !strings.Contains(err.Error(), port) {
			t.Errorf("error %q does not name %s", err, port)
		}
	}
	for _, optional := range []string{"Deps.Metrics", "Deps.Steps", "Deps.Waker", "Deps.Listener", "Deps.QuotaPolicies", "Deps.Clock"} {
		if strings.Contains(err.Error(), optional) {
			t.Errorf("error %q names %s, which is optional", err, optional)
		}
	}

	f := newLoopFx(t)
	if _, err := app.NewDispatcher(valid, f.deps); err != nil {
		t.Fatalf("a complete Deps was refused: %v", err)
	}
	var ce *notify.ConfigError
	if _, err := app.NewDispatcher(notify.Config{StoreDSN: "x", RedisURL: "y"}, f.deps); !errors.As(err, &ce) {
		t.Errorf("a configuration with no realm: %v, want a *notify.ConfigError", err)
	}

	listening := valid
	listening.ListenNotify = true
	if _, err := app.NewDispatcher(listening, f.deps); err == nil || !strings.Contains(err.Error(), "Deps.Listener") {
		t.Errorf("ListenNotify with no Listener: %v, want an error naming Deps.Listener", err)
	}
}

// Drain refuses what cannot be a batch before it touches the queue.
func TestDrainRefusesAnImpossibleBatch(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		shard domain.Shard
		max   int
	}{
		"a batch of none":            {0, 0},
		"a negative batch":           {0, -1},
		"a negative shard":           {-1, 10},
		"a normal batch (a control)": {0, 10},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newLoopFx(t)
			d := f.build()
			n, err := d.Drain(t.Context(), tc.shard, tc.max)
			f.q.mu.Lock()
			asked := len(f.q.calls)
			f.q.mu.Unlock()
			if name == "a normal batch (a control)" {
				if err != nil || n != 0 || asked != 1 {
					t.Fatalf("Drain = %d, %v after %d claims, want an empty batch from one claim", n, err, asked)
				}
				return
			}
			if err == nil || n != 0 || asked != 0 {
				t.Fatalf("Drain = %d, %v after %d claims, want an error and no claim", n, err, asked)
			}
		})
	}
}

// A claim that fails is the batch's error, and nothing was processed.
func TestDrainReturnsAClaimFault(t *testing.T) {
	t.Parallel()
	f := newLoopFx(t)
	boom := errors.New("connection refused")
	f.q.script = func(int, domain.Shard) ([]domain.Claim, error) { return nil, boom }
	n, err := f.build().Drain(t.Context(), 0, 10)
	if !errors.Is(err, boom) || n != 0 {
		t.Fatalf("Drain = %d, %v, want the claim's error and nothing processed", n, err)
	}
}

// -------------------------------------------------------------- the loop --

// Idle, the dispatcher polls with a growing wait that stops at PollInterval, and it
// visits every shard in each pass (D22).
func TestRunBacksOffToPollIntervalWhenIdle(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newLoopFx(t)
		stop := run(t, f.build())
		time.Sleep(3 * time.Second)
		if err := stop(); err != nil {
			t.Fatalf("Run = %v, want nil when its context ends", err)
		}

		got := gaps(f.q.passTimes())
		if len(got) < 9 {
			t.Fatalf("only %d gaps in three seconds: %v", len(got), got)
		}
		want := ms(10, 20, 40, 80, 160, 320, 400, 400, 400)
		if !equalDurations(got[:9], want) {
			t.Fatalf("gaps between idle passes = %v, want %v: the wait doubles from the floor and stops at PollInterval", got[:9], want)
		}
		if shards := f.q.shardsOf(0); len(shards) != 3 || shards[0] != 0 || shards[1] != 1 || shards[2] != 2 {
			t.Fatalf("a pass visited shards %v, want every shard once", shards)
		}
	})
}

// A pass starts at the shard the random source chooses and goes round, so workers
// started together do not all contend for the same shard first.
func TestAPassStartsAtTheShardTheRandomSourceChooses(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		rnd  float64
		want []domain.Shard
	}{
		{0, []domain.Shard{0, 1, 2}},
		{0.5, []domain.Shard{1, 2, 0}},
		{0.99, []domain.Shard{2, 0, 1}},
	} {
		t.Run(fmt.Sprint(tc.rnd), func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newLoopFx(t)
				stop := run(t, f.build(app.WithRand(func() float64 { return tc.rnd })))
				time.Sleep(time.Millisecond)
				_ = stop()
				if got := f.q.shardsOf(0); !slices.Equal(got, tc.want) {
					t.Fatalf("the first pass visited %v, want %v", got, tc.want)
				}
			})
		})
	}
}

// A pass that finds work resets the wait to the floor (D22): the next arrival is
// likely close behind, and a dispatcher that kept waiting at the ceiling would add
// PollInterval to every one.
func TestRunResetsThePollIntervalOnWork(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newLoopFx(t)
		f.q.script = func(pass int, shard domain.Shard) ([]domain.Claim, error) {
			if pass == 8 && shard == 0 { // after the wait has reached its ceiling
				return []domain.Claim{expiredClaim("c1", f.clock)}, nil
			}
			return nil, nil
		}
		stop := run(t, f.build())
		time.Sleep(5 * time.Second)
		_ = stop()

		got := gaps(f.q.passTimes())
		want := ms(10, 20, 40, 80, 160, 320, 400, 400, 10, 20, 40, 80)
		if len(got) < len(want) || !equalDurations(got[:len(want)], want) {
			t.Fatalf("gaps = %v, want %v: after the pass that found work the wait starts again from the floor", got, want)
		}
		f.q.mu.Lock()
		defer f.q.mu.Unlock()
		if len(f.q.finished) != 1 || f.q.finished[0].Outcome != domain.OutcomeExpired {
			t.Fatalf("finished = %+v, want the one expired claim handled", f.q.finished)
		}
	})
}

// A full batch means more is waiting: the next pass is at once, with no wait at all.
func TestRunClaimsAgainAtOnceAfterAFullBatch(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newLoopFx(t)
		f.q.script = func(pass int, shard domain.Shard) ([]domain.Claim, error) {
			if pass < 3 && shard == 1 { // ClaimBatch is 2
				return []domain.Claim{expiredClaim("a", f.clock), expiredClaim("b", f.clock)}, nil
			}
			return nil, nil
		}
		stop := run(t, f.build())
		time.Sleep(time.Second)
		_ = stop()

		got := gaps(f.q.passTimes())
		if len(got) < 4 || got[0] != 0 || got[1] != 0 || got[2] != 0 || got[3] != 10*time.Millisecond {
			t.Fatalf("gaps = %v, want three full batches back to back, then the floor", got)
		}
	})
}

// A pass that could not even claim is idle as far as the wait goes, so a database
// that is down is not hammered at the floor, and the fault is reported once per pass.
func TestRunBacksOffFromAFaultingQueueAndTellsTheHandler(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newLoopFx(t)
		boom := errors.New("connection refused")
		var failing atomic.Bool
		failing.Store(true)
		f.q.script = func(int, domain.Shard) ([]domain.Claim, error) {
			if failing.Load() {
				return nil, boom
			}
			return nil, nil
		}
		stop := run(t, f.build())
		time.Sleep(100 * time.Millisecond)
		synctest.Wait()
		failing.Store(false)
		time.Sleep(time.Second)
		_ = stop()

		got := gaps(f.q.passTimes())
		if len(got) < 3 || got[0] != 10*time.Millisecond || got[1] != 20*time.Millisecond || got[2] != 40*time.Millisecond {
			t.Fatalf("gaps = %v, want the wait to grow while the queue faults", got)
		}
		errs := f.errs.all()
		if len(errs) == 0 || !errors.Is(errs[0], boom) {
			t.Fatalf("handler saw %v, want the claim's fault", errs)
		}
	})
}

// Run returns nil when its context ends, whether it is mid-wait or about to wait.
func TestRunReturnsNilWhenItsContextEnds(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"while idle at the ceiling", "straight away"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newLoopFx(t)
				d := f.build()
				if name == "straight away" {
					ctx, cancel := context.WithCancel(t.Context())
					cancel()
					if err := d.Run(ctx); err != nil {
						t.Fatalf("Run with a context already done = %v, want nil", err)
					}
					return
				}
				stop := run(t, d)
				time.Sleep(2 * time.Second)
				synctest.Wait()
				if err := stop(); err != nil {
					t.Fatalf("Run = %v, want nil", err)
				}
			})
		})
	}
}

// ------------------------------------------------------------------ waking --

// Wake gets an idle dispatcher claiming at once (D22), and the wait that follows
// starts from the floor.
func TestWakeClaimsAtOnceWhenIdle(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newLoopFx(t)
		d := f.build()
		stop := run(t, d)
		time.Sleep(3 * time.Second) // idle at the ceiling
		synctest.Wait()
		before := len(f.q.passTimes())

		woke := time.Now()
		d.Wake()
		synctest.Wait()
		passes := f.q.passTimes()
		if len(passes) != before+1 || !passes[before].Equal(woke) {
			t.Fatalf("passes %d → %d, want one new pass at the instant of the wake %v, not at the next poll", before, len(passes), woke)
		}
		time.Sleep(time.Second)
		_ = stop()
		if got := gaps(f.q.passTimes()[before:]); len(got) < 2 || got[0] != 10*time.Millisecond || got[1] != 20*time.Millisecond {
			t.Fatalf("gaps after the wake = %v, want the wait to restart from the floor", got)
		}
	})
}

// A wake that arrives while a pass is running is not lost: the next wait is cut
// short. The commit that wakes the dispatcher can land at any moment, including
// just after its last claim came back empty.
func TestWakeDuringAPassIsNotLost(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newLoopFx(t)
		d := f.build()
		f.q.mu.Lock()
		f.q.onClaim = func() {
			f.q.mu.Lock()
			first := len(f.q.calls) == 1
			f.q.mu.Unlock()
			if first {
				d.Wake() // while the first pass is still claiming
			}
		}
		f.q.mu.Unlock()
		stop := run(t, d)
		time.Sleep(time.Second)
		_ = stop()

		got := gaps(f.q.passTimes())
		if len(got) < 2 || got[0] != 0 {
			t.Fatalf("gaps = %v, want the pass after the wake to follow at once", got)
		}
	})
}

// Wake never blocks, however often it is called with nobody waiting.
func TestWakeNeverBlocks(t *testing.T) {
	t.Parallel()
	f := newLoopFx(t)
	d := f.build()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 1000 {
			d.Wake()
		}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Wake blocked with nothing listening")
	}
}

// A Notify that commits in a process that runs a dispatcher wakes it at once, so an
// idle dispatcher does not make the producer's notification wait for the next poll
// (D22, NS-004). The Notifier is the real one over recording stand-ins.
func TestNotifyWakesAnIdleDispatcher(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		loop := newLoopFx(t)
		d := loop.build()
		stop := run(t, d)
		time.Sleep(3 * time.Second)
		synctest.Wait()
		before := len(loop.q.passTimes())

		f := newNoteFx(t)
		f.waker = d
		woke := time.Now()
		if _, err := f.build().Notify(t.Context(), f.note("k1")); err != nil {
			t.Fatalf("Notify: %v", err)
		}
		synctest.Wait()
		passes := loop.q.passTimes()
		if len(passes) != before+1 || !passes[before].Equal(woke) {
			t.Fatalf("passes %d → %d: Notify did not get the idle dispatcher claiming at once", before, len(passes))
		}
		_ = stop()
	})
}

// ------------------------------------------------------------ LISTEN/NOTIFY --

// listenSource is a ports.WakeSource a test signals by hand.
type listenSource struct {
	mu    sync.Mutex
	calls int
	ctx   context.Context
	ch    chan struct{}
	err   error
}

func (l *listenSource) Listen(ctx context.Context) (<-chan struct{}, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls++
	l.ctx = ctx
	if l.err != nil {
		return nil, l.err
	}
	return l.ch, nil
}

func (l *listenSource) listenCalls() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.calls
}

// LISTEN/NOTIFY is off by default (D22): a transactional NOTIFY serializes the
// commit of every transaction that issues one, so it is an operator's choice. A
// Listener in Deps does not turn it on; the configuration does.
func TestListenNotifyIsOffByDefault(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newLoopFx(t)
		src := &listenSource{ch: make(chan struct{}, 1)}
		f.deps.Listener = src
		if f.cfg.ListenNotify {
			t.Fatal("the fixture's configuration has ListenNotify on")
		}
		if (notify.Config{}).WithDefaults().ListenNotify {
			t.Fatal("ListenNotify is on in the defaults")
		}
		stop := run(t, f.build())
		time.Sleep(time.Second)
		src.ch <- struct{}{} // a signal nobody should be hearing
		synctest.Wait()
		_ = stop()
		if n := src.listenCalls(); n != 0 {
			t.Fatalf("Listen was called %d times with ListenNotify off", n)
		}
	})
}

// With it on, the dispatcher listens once, for as long as it runs, and a signal
// gets it claiming at once.
func TestListenNotifyWakesAnIdleDispatcherWhenOn(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newLoopFx(t)
		f.cfg.ListenNotify = true
		src := &listenSource{ch: make(chan struct{}, 1)}
		f.deps.Listener = src
		stop := run(t, f.build())
		time.Sleep(3 * time.Second)
		synctest.Wait()
		before := len(f.q.passTimes())

		signalled := time.Now()
		src.ch <- struct{}{}
		synctest.Wait()
		passes := f.q.passTimes()
		if len(passes) != before+1 || !passes[before].Equal(signalled) {
			t.Fatalf("passes %d → %d: a signal from another session did not get the idle dispatcher claiming at once", before, len(passes))
		}
		_ = stop()
		if n := src.listenCalls(); n != 1 {
			t.Fatalf("Listen was called %d times, want once", n)
		}
		if src.ctx.Err() == nil {
			t.Fatal("Listen's context was still live after Run returned: the connection behind it would leak")
		}
	})
}

// LISTEN is a latency optimisation and polling is the guarantee: a listener that
// fails to start, or that ends, leaves the dispatcher polling, and the handler hears.
func TestAListenerThatFailsOrEndsLeavesTheDispatcherPolling(t *testing.T) {
	t.Parallel()
	cases := map[string]func(*listenSource){
		"cannot start": func(l *listenSource) { l.err = errors.New("LISTEN refused") },
		"ends":         func(l *listenSource) { close(l.ch) },
	}
	for name, setup := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newLoopFx(t)
				f.cfg.ListenNotify = true
				src := &listenSource{ch: make(chan struct{}, 1)}
				setup(src)
				f.deps.Listener = src
				stop := run(t, f.build())
				time.Sleep(2 * time.Second)
				_ = stop()

				got := gaps(f.q.passTimes())
				if len(got) < 8 || !equalDurations(got[:8], ms(10, 20, 40, 80, 160, 320, 400, 400)) {
					t.Fatalf("gaps = %v: with the listener gone the dispatcher must go on polling, and not spin on a closed channel", got)
				}
				errs := f.errs.all()
				if len(errs) != 1 || !strings.Contains(strings.ToLower(errs[0].Error()), "listen") {
					t.Fatalf("handler saw %v, want one error saying the listener is gone", errs)
				}
			})
		})
	}
}
