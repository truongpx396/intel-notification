package domain

import "hash/crc32"

// Shard is a contention hint for claimers, not a correctness boundary (D11). It
// is computed once, stored on the queue row, and never recomputed, so changing
// the shard count re-maps nothing in flight.
type Shard int16

// ShardFor is crc32(identity.Canonical()) mod n. It hashes the recipient within
// its tenant rather than the tenant alone, so the largest tenant is spread across
// every shard instead of confined to one. n < 1 is treated as 1.
func ShardFor(id Identity, n int) Shard {
	if n < 1 {
		n = 1
	}
	return Shard(crc32.ChecksumIEEE([]byte(id.Canonical())) % uint32(n))
}
