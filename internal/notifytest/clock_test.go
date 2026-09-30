package notifytest

import (
	"sync"
	"testing"
	"time"
)

var t0 = time.Date(2031, 3, 1, 12, 0, 0, 0, time.UTC)

// The clock reads what it was given and moves only when told. The start is
// years from the wall clock, so a clock that read the real time could not pass.
func TestClockMovesOnlyWhenTold(t *testing.T) {
	t.Parallel()
	c := NewClock(t0)

	if got := c.Now(); !got.Equal(t0) {
		t.Fatalf("Now() = %v, want the start %v", got, t0)
	}
	if got := c.Now(); !got.Equal(t0) {
		t.Fatalf("a second Now() = %v: reading the clock moved it", got)
	}

	want := t0.Add(90 * time.Second)
	if got := c.Advance(90 * time.Second); !got.Equal(want) {
		t.Fatalf("Advance returned %v, want %v", got, want)
	}
	if got := c.Now(); !got.Equal(want) {
		t.Fatalf("Now() after Advance = %v, want %v", got, want)
	}

	c.Advance(0)
	if got := c.Now(); !got.Equal(want) {
		t.Fatalf("Advance(0) moved the clock to %v", got)
	}

	c.Advance(time.Nanosecond)
	if got := c.Now(); !got.Equal(want.Add(time.Nanosecond)) {
		t.Fatalf("Advance(1ns) left the clock at %v", got)
	}
}

func TestClockSet(t *testing.T) {
	t.Parallel()
	c := NewClock(t0)
	later := t0.Add(48 * time.Hour)
	c.Set(later)
	if got := c.Now(); !got.Equal(later) {
		t.Fatalf("Now() after Set = %v, want %v", got, later)
	}
	c.Set(t0)
	if got := c.Now(); !got.Equal(t0) {
		t.Fatalf("Set may jump backwards, and Now() = %v, want %v", got, t0)
	}
}

func TestClockRefusesToRunBackwards(t *testing.T) {
	t.Parallel()
	c := NewClock(t0)
	defer func() {
		if recover() == nil {
			t.Fatal("Advance(-1ns) did not panic")
		}
		if got := c.Now(); !got.Equal(t0) {
			t.Fatalf("a refused Advance still moved the clock to %v", got)
		}
	}()
	c.Advance(-time.Nanosecond)
}

// Workers read the clock while the test advances it. Under -race this fails on
// an unsynchronized clock, and the total shows no advance was lost.
func TestClockIsSafeForConcurrentUse(t *testing.T) {
	t.Parallel()
	const goroutines, each = 32, 100
	c := NewClock(t0)
	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range each {
				c.Advance(time.Nanosecond)
				_ = c.Now()
			}
		}()
	}
	wg.Wait()
	if want := t0.Add(goroutines * each * time.Nanosecond); !c.Now().Equal(want) {
		t.Fatalf("Now() = %v, want %v: an Advance was lost", c.Now(), want)
	}
}
