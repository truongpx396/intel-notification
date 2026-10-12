package notifytest

import (
	"testing"
	"time"

	"github.com/truongpx396/intel-notification/ports"
)

// The fake runs the same table as the Redis adapter (T023), so what it decides
// where the contract leaves room is decided for both.
func TestPreCheckFakeRunsTheContract(t *testing.T) {
	t.Parallel()
	PreCheckContract(t, func(*testing.T) ports.PreCheck { return NewPreCheck(NewClock(t0)) })
}

// An entry lapses after its ttl on the fake clock: present one nanosecond before
// it, gone at it. Without the lapse the fake would hold a key past the
// idempotency window, which is exactly what the real pre-check must not do (D18).
func TestPreCheckEntryLapsesAtItsTTL(t *testing.T) {
	t.Parallel()
	const ttl = time.Hour
	cases := []struct {
		name     string
		advance  time.Duration
		wantSeen bool
	}{
		{"at once", 0, true},
		{"one nanosecond before the ttl", ttl - 1, true},
		{"at the ttl", ttl, false},
		{"after the ttl", ttl + time.Minute, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			clock := NewClock(t0)
			p := NewPreCheck(clock)
			if err := p.Remember(t.Context(), "k1", "n1", ttl); err != nil {
				t.Fatalf("Remember: %v", err)
			}
			clock.Advance(tc.advance)
			id, ok, err := p.Seen(t.Context(), "k1")
			if err != nil || ok != tc.wantSeen {
				t.Fatalf("Seen after %v = %q, %v, %v, want seen = %v", tc.advance, id, ok, err, tc.wantSeen)
			}
		})
	}
}

// Each Remember is kept with the instant it happened, so a suite can see whether
// the Notifier wrote after the commit it follows rather than before it.
func TestPreCheckKeepsEachWriteWithItsInstant(t *testing.T) {
	t.Parallel()
	clock := NewClock(t0)
	p := NewPreCheck(clock)

	if got := p.Writes(); len(got) != 0 {
		t.Fatalf("a new pre-check has %d writes", len(got))
	}
	if err := p.Remember(t.Context(), "k1", "n1", time.Hour); err != nil {
		t.Fatalf("Remember: %v", err)
	}
	clock.Advance(time.Minute)
	if err := p.Remember(t.Context(), "k2", "n2", 2*time.Hour); err != nil {
		t.Fatalf("Remember: %v", err)
	}

	want := []PreCheckWrite{
		{Key: "k1", NotificationID: "n1", TTL: time.Hour, At: t0},
		{Key: "k2", NotificationID: "n2", TTL: 2 * time.Hour, At: t0.Add(time.Minute)},
	}
	got := p.Writes()
	if len(got) != len(want) {
		t.Fatalf("Writes = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i].Key != want[i].Key || got[i].NotificationID != want[i].NotificationID ||
			got[i].TTL != want[i].TTL || !got[i].At.Equal(want[i].At) {
			t.Fatalf("write %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// A read is not a write, and a refused write is not one either: Writes lists what
// was stored, which is what a test of "no entry was written" counts.
func TestPreCheckReadsAndRefusedWritesAreNotWrites(t *testing.T) {
	t.Parallel()
	p := NewPreCheck(NewClock(t0))
	_, _, _ = p.Seen(t.Context(), "k1")
	if err := p.Remember(t.Context(), "k1", "n1", 0); err == nil {
		t.Fatal("Remember with a zero ttl succeeded")
	}
	if got := p.Writes(); len(got) != 0 {
		t.Fatalf("Writes = %+v, want none after a read and a refused write", got)
	}
}
