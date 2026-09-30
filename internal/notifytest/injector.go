package notifytest

import (
	"context"
	"fmt"
	"sync"

	"github.com/truongpx396/intel-notification/domain"
)

// Crash is the value an injected crash panics with. A test recovers it with
// [Survive] to stand in for the worker process dying at that step.
type Crash struct {
	Step domain.DispatchStep
}

// Error implements error.
func (c Crash) Error() string { return "" }

// Survive runs fn and returns the Crash it panicked with, or nil if it returned.
// Any other panic propagates: only an injected crash is an expected death.
func Survive(fn func()) (crash *Crash) {
	defer func() { _ = recover() }()
	fn()
	return nil
}

// Stall is a worker held at a step. Reached is closed when the worker arrives;
// it stays held until Release, or until its context ends, as it does when the
// worker is shut down.
type Stall struct {
	reached  chan struct{}
	released chan struct{}
	once     sync.Once
}

func newStall() *Stall {
	return &Stall{reached: make(chan struct{}), released: make(chan struct{})}
}

// Reached is closed once the worker is held.
func (s *Stall) Reached() <-chan struct{} { return s.reached }

// Release lets the held worker go on. It is safe to call more than once.
func (s *Stall) Release() {}

// fault is one armed fault. A nil stall means a panic.
type fault struct {
	stall *Stall
}

// Injector holds a worker at, or crashes it at, a named step of the dispatcher.
// The dispatcher calls Reach at each of its nine steps; a fault armed for the step
// fires on the next Reach there, once, and disarms. Faults armed for the same step
// fire in the order they were armed, one per Reach.
//
// Every method is safe for concurrent use, and a nil *Injector is a no-op, so the
// dispatcher can call it unconditionally.
type Injector struct {
	mu    sync.Mutex
	armed map[domain.DispatchStep][]fault
	fired map[domain.DispatchStep]int
}

// NewInjector returns an injector with nothing armed.
func NewInjector() *Injector {
	return &Injector{armed: map[domain.DispatchStep][]fault{}, fired: map[domain.DispatchStep]int{}}
}

// PanicAt arms a crash: the next worker to reach step panics with a [Crash]. It
// panics for a step that does not exist, because a fault that could never fire
// would leave a crash test passing without crashing anything.
func (i *Injector) PanicAt(step domain.DispatchStep) { mustBeAStep(step) }

// StallAt arms a hold: the next worker to reach step blocks there until the
// returned Stall is released or its context ends. It panics for a step that does
// not exist.
func (i *Injector) StallAt(step domain.DispatchStep) *Stall {
	mustBeAStep(step)
	return newStall()
}

// Reach is what the dispatcher calls at a step. It returns at once unless a fault
// is armed for that step.
func (i *Injector) Reach(ctx context.Context, step domain.DispatchStep) {}

// Fired counts the faults that have fired at step.
func (i *Injector) Fired(step domain.DispatchStep) int { return 0 }

// Pending counts the faults armed and not yet fired, so a test can assert that
// the crash it planned really happened.
func (i *Injector) Pending() int { return 0 }

func mustBeAStep(step domain.DispatchStep) {
	if !step.Valid() {
		panic(fmt.Sprintf("notifytest: %v is not a dispatcher step", step))
	}
}
