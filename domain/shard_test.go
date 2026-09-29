package domain

import (
	"strconv"
	"testing"
)

// Frozen vectors computed independently (zlib.crc32 over the canonical
// encoding). The shard is stored on each row and never recomputed, so a change
// here would only mis-spread new rows — but it would still be a silent change.
func TestShardFor(t *testing.T) {
	t.Parallel()
	w2 := u1
	w2.Tenant.ID = "w2"
	cases := []struct {
		name string
		id   Identity
		n    int
		want Shard
	}{
		{"u1 of 16", u1, 16, 0},
		{"u1 of 7", u1, 7, 2},
		{"u1 of 1", u1, 1, 0},
		{"same user, other tenant, of 16", w2, 16, 14},
		{"non-positive count is one shard", u1, 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := ShardFor(tc.id, tc.n); got != tc.want {
				t.Fatalf("ShardFor(%s, %d) = %d, want %d", tc.id.Canonical(), tc.n, got, tc.want)
			}
		})
	}
}

func TestShardForStaysInRangeAndSpreads(t *testing.T) {
	t.Parallel()
	const n = 16
	seen := make(map[Shard]int)
	id := u1
	for i := range 4000 {
		id.Recipient.ID = "user-" + strconv.Itoa(i)
		s := ShardFor(id, n)
		if s < 0 || s >= n {
			t.Fatalf("shard %d out of range for %s", s, id.Canonical())
		}
		seen[s]++
	}
	// One large tenant must spread over every shard (D11), not land on one.
	for s := range Shard(n) {
		if seen[s] < 4000/n/2 {
			t.Errorf("shard %d got %d of 4000 recipients; the spread is badly skewed", s, seen[s])
		}
	}
}
