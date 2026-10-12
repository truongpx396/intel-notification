package postgres

import (
	"crypto/rand"
	"encoding/hex"
	"time"

	"github.com/truongpx396/intel-notification/ports"
)

// uuidV7 builds a version 7 UUID (RFC 9562 §5.7): 48 bits of Unix milliseconds,
// the version, and the rest random with the variant bits set. Ids from different
// milliseconds sort by time, which keeps the inbox's primary key (id, created_at)
// appending to the right of its index instead of landing anywhere in it.
// Sub-millisecond time is dropped, not rounded.
func uuidV7(at time.Time, entropy [10]byte) string {
	var b [16]byte
	ms := uint64(at.UnixMilli())
	for i := range 6 {
		b[i] = byte(ms >> (8 * (5 - i)))
	}
	copy(b[6:], entropy[:])
	b[6] = b[6]&0x0f | 0x70 // version 7
	b[8] = b[8]&0x3f | 0x80 // variant 10
	var out [36]byte
	hex.Encode(out[0:8], b[0:4])
	out[8] = '-'
	hex.Encode(out[9:13], b[4:6])
	out[13] = '-'
	hex.Encode(out[14:18], b[6:8])
	out[18] = '-'
	hex.Encode(out[19:23], b[8:10])
	out[23] = '-'
	hex.Encode(out[24:36], b[10:16])
	return string(out[:])
}

var _ ports.IDSource = uuidSource{}

// uuidSource is the default ports.IDSource: a UUIDv7 stamped from clock.
type uuidSource struct{ clock ports.Clock }

// New implements ports.IDSource.
func (u uuidSource) New() string {
	var entropy [10]byte
	_, _ = rand.Read(entropy[:]) // never fails: the runtime crashes first
	return uuidV7(u.clock.Now(), entropy)
}
