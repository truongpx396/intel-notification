package notifytest

import (
	"context"
	"errors"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/truongpx396/intel-notification/domain"
)

// A crash fires once, at the step it was armed for and no other, then disarms.
// Every step is tried, so a step the injector forgot to handle shows.
func TestInjectorPanicFiresOnceAtTheNamedStep(t *testing.T) {
	t.Parallel()
	for _, armed := range domain.AllDispatchSteps() {
		t.Run(armed.String(), func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			inj := NewInjector()
			inj.PanicAt(armed)
			if got := inj.Pending(); got != 1 {
				t.Fatalf("Pending() = %d after arming one fault, want 1", got)
			}

			for _, step := range domain.AllDispatchSteps() {
				crash := Survive(func() { inj.Reach(ctx, step) })
				switch {
				case step != armed && crash != nil:
					t.Fatalf("armed at %v, but the worker crashed at %v", armed, step)
				case step == armed && crash == nil:
					t.Fatalf("armed at %v, but the worker passed it", armed)
				case step == armed && crash.Step != armed:
					t.Fatalf("crashed at %v, want the crash to name %v", crash.Step, armed)
				}
			}

			if crash := Survive(func() { inj.Reach(ctx, armed) }); crash != nil {
				t.Fatalf("the fault fired a second time at %v", armed)
			}
			if got := inj.Fired(armed); got != 1 {
				t.Fatalf("Fired(%v) = %d, want 1", armed, got)
			}
			if got := inj.Pending(); got != 0 {
				t.Fatalf("Pending() = %d after the fault fired, want 0", got)
			}
		})
	}
}

// Fired counts per step, so a test can tell which crash happened.
func TestInjectorFiredIsPerStep(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	inj := NewInjector()
	inj.PanicAt(domain.StepDeliver)
	inj.PanicAt(domain.StepRecord)
	Survive(func() { inj.Reach(ctx, domain.StepDeliver) })
	if d, r := inj.Fired(domain.StepDeliver), inj.Fired(domain.StepRecord); d != 1 || r != 0 {
		t.Fatalf("Fired(deliver) = %d, Fired(record) = %d, want 1 and 0", d, r)
	}
	if got := inj.Pending(); got != 1 {
		t.Fatalf("Pending() = %d, want the record fault still armed", got)
	}
}

// Faults armed for one step fire in the order they were armed, one per Reach: the
// shape of "the first worker crashes, and its replacement is held". A crash armed
// before a hold must fire first; fired the other way round, the first worker would
// be the one held.
func TestInjectorFaultsAtOneStepFireInTheOrderArmed(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel() // frees a worker the test finds held when it should not be
		inj := NewInjector()
		inj.PanicAt(domain.StepDeliver)
		stall := inj.StallAt(domain.StepDeliver)

		var crash *Crash
		first := make(chan struct{})
		go func() {
			crash = Survive(func() { inj.Reach(ctx, domain.StepDeliver) })
			close(first)
		}()
		synctest.Wait()
		select {
		case <-first:
		default:
			t.Fatal("the first Reach was held, but the crash was armed before the hold")
		}
		if crash == nil {
			t.Fatal("the first Reach should have crashed: it was armed first")
		}

		second := make(chan struct{})
		go func() {
			inj.Reach(ctx, domain.StepDeliver)
			close(second)
		}()
		synctest.Wait()
		select {
		case <-stall.Reached():
		default:
			t.Fatal("the second Reach should have been held: it was armed second")
		}
		select {
		case <-second:
			t.Fatal("the second Reach passed the step while it was held")
		default:
		}
		stall.Release()
		synctest.Wait()
		select {
		case <-second:
		default:
			t.Fatal("Release did not let the second worker go on")
		}

		if Survive(func() { inj.Reach(ctx, domain.StepDeliver) }) != nil {
			t.Fatal("a third Reach crashed, with nothing left armed")
		}
		if got := inj.Fired(domain.StepDeliver); got != 2 {
			t.Fatalf("Fired(deliver) = %d, want 2", got)
		}
	})
}

// A held worker stays held until released. synctest.Wait returns once every
// goroutine is blocked, so "still held" is asserted without sleeping.
func TestInjectorStallHoldsTheWorkerUntilReleased(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		inj := NewInjector()
		stall := inj.StallAt(domain.StepQuota)
		done := make(chan struct{})
		go func() {
			inj.Reach(context.Background(), domain.StepQuota)
			close(done)
		}()

		synctest.Wait()
		select {
		case <-stall.Reached():
		default:
			t.Fatal("the worker never reached the stall")
		}
		select {
		case <-done:
			t.Fatal("the worker passed the step while it was held")
		default:
		}

		stall.Release()
		stall.Release() // releasing twice is harmless
		synctest.Wait()
		select {
		case <-done:
		default:
			t.Fatal("Release did not let the worker go on")
		}
		if got := inj.Fired(domain.StepQuota); got != 1 {
			t.Fatalf("Fired(quota) = %d, want 1", got)
		}
	})
}

// A worker being shut down must not hang on a hold nobody will release.
func TestInjectorStallEndsWhenTheWorkersContextDoes(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		inj := NewInjector()
		stall := inj.StallAt(domain.StepRender)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			inj.Reach(ctx, domain.StepRender)
			close(done)
		}()

		synctest.Wait()
		select {
		case <-stall.Reached():
		default:
			t.Fatal("the worker never reached the stall")
		}
		cancel()
		synctest.Wait()
		select {
		case <-done:
		default:
			t.Fatal("the held worker ignored its canceled context")
		}
	})
}

// A hold at one step does not slow the others, and stays armed until a worker
// gets there.
func TestInjectorStallOnlyHoldsTheNamedStep(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		inj := NewInjector()
		inj.StallAt(domain.StepDeliver)
		done := make(chan struct{})
		go func() {
			for _, step := range domain.AllDispatchSteps() {
				if step != domain.StepDeliver {
					inj.Reach(context.Background(), step)
				}
			}
			close(done)
		}()
		synctest.Wait()
		select {
		case <-done:
		default:
			t.Fatal("a hold armed for deliver held a worker at another step")
		}
		if got := inj.Pending(); got != 1 {
			t.Fatalf("Pending() = %d, want the deliver hold still armed", got)
		}
	})
}

func TestSurvive(t *testing.T) {
	t.Parallel()

	t.Run("a crash is returned", func(t *testing.T) {
		t.Parallel()
		crash := Survive(func() { panic(Crash{Step: domain.StepLoad}) })
		if crash == nil || crash.Step != domain.StepLoad {
			t.Fatalf("Survive returned %+v, want the load crash", crash)
		}
	})

	t.Run("a worker that returns has not crashed", func(t *testing.T) {
		t.Parallel()
		if crash := Survive(func() {}); crash != nil {
			t.Fatalf("Survive returned %+v for a function that returned", crash)
		}
	})

	t.Run("any other panic propagates", func(t *testing.T) {
		t.Parallel()
		defer func() {
			if r := recover(); r != "a real bug" {
				t.Fatalf("recovered %v, want the original panic to propagate", r)
			}
		}()
		Survive(func() { panic("a real bug") })
		t.Fatal("Survive swallowed a panic that was not a Crash")
	})
}

func TestCrashIsAnErrorNamingItsStep(t *testing.T) {
	t.Parallel()
	var err error = Crash{Step: domain.StepRecord}
	var c Crash
	if !errors.As(err, &c) || c.Step != domain.StepRecord {
		t.Fatalf("errors.As did not recover the crash from %v", err)
	}
	if msg := err.Error(); !strings.Contains(msg, "record") {
		t.Fatalf("Error() = %q, want it to name the step", msg)
	}
}

// A nil injector is what the dispatcher holds when no test armed anything.
func TestNilInjectorIsQuiet(t *testing.T) {
	t.Parallel()
	var inj *Injector
	for _, step := range domain.AllDispatchSteps() {
		inj.Reach(t.Context(), step)
	}
	if inj.Fired(domain.StepClaim) != 0 || inj.Pending() != 0 {
		t.Fatal("a nil injector reported a fault")
	}
}

// Arming a step that does not exist would leave a crash test passing without
// crashing anything, so it is a programmer error.
func TestInjectorRefusesAStepThatDoesNotExist(t *testing.T) {
	t.Parallel()
	for _, arm := range []struct {
		name string
		fn   func(*Injector, domain.DispatchStep)
	}{
		{"PanicAt", func(i *Injector, s domain.DispatchStep) { i.PanicAt(s) }},
		{"StallAt", func(i *Injector, s domain.DispatchStep) { i.StallAt(s) }},
	} {
		for _, step := range []domain.DispatchStep{0, 10, -1} {
			t.Run(arm.name+"/"+step.String(), func(t *testing.T) {
				t.Parallel()
				defer func() {
					if recover() == nil {
						t.Fatalf("%s(%v) did not panic", arm.name, step)
					}
				}()
				arm.fn(NewInjector(), step)
			})
		}
	}
}
