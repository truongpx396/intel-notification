package postgres

import (
	"slices"
	"testing"
	"time"
)

// stubClock is a ports.Clock for the unit tests of this package.
type stubClock struct{ now time.Time }

func (c *stubClock) Now() time.Time { return c.now }

// TestUUIDv7 holds the layout against vectors computed independently of the code,
// by integer arithmetic on the 128-bit value (RFC 9562 §5.7): 48 bits of Unix
// milliseconds, version 7, 74 random bits with the variant in the middle.
func TestUUIDv7(t *testing.T) {
	t.Parallel()
	sep := time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC) // 1789430400000 ms
	cases := []struct {
		name    string
		at      time.Time
		entropy [10]byte
		want    string
		why     string
	}{
		{"a known instant and entropy", sep, [10]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			"01a0a25d-0400-7102-8304-05060708090a", "the version and variant bits overwrite the entropy's own"},
		{"all-ones entropy", sep, [10]byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255},
			"01a0a25d-0400-7fff-bfff-ffffffffffff", "the version stays 7 and the variant stays 10xx"},
		{"all-zero entropy", sep, [10]byte{},
			"01a0a25d-0400-7000-8000-000000000000", ""},
		{"the Unix epoch", time.Unix(0, 0), [10]byte{},
			"00000000-0000-7000-8000-000000000000", ""},
		{"sub-millisecond time is dropped, not rounded", sep.Add(999 * time.Microsecond), [10]byte{},
			"01a0a25d-0400-7000-8000-000000000000", "the field is milliseconds"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := uuidV7(tc.at, tc.entropy); got != tc.want {
				t.Fatalf("uuidV7 = %s, want %s: %s", got, tc.want, tc.why)
			}
		})
	}
}

// ports.IDSource's contract is that identifiers sort by creation time, so a later
// id is never smaller than an earlier one, and ids from one instant differ.
func TestUUIDSourceSortsByTimeAndDoesNotRepeat(t *testing.T) {
	t.Parallel()
	clock := &stubClock{now: time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)}
	src := uuidSource{clock: clock}

	var ids []string
	for range 5 {
		ids = append(ids, src.New())
		clock.now = clock.now.Add(time.Millisecond)
	}
	if !slices.IsSorted(ids) {
		t.Errorf("ids drawn a millisecond apart do not sort by creation time: %v", ids)
	}

	seen := map[string]bool{}
	for range 1000 {
		id := src.New() // one instant
		if seen[id] {
			t.Fatalf("id %s drawn twice within one millisecond", id)
		}
		seen[id] = true
	}
}
