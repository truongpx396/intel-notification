package redis_test

import (
	"strings"
	"testing"
)

// crc16 is CRC16/XMODEM, the function Redis Cluster hashes keys with. It is written
// here from the cluster specification, not taken from the code under test, so that a
// stream key's slot is judged by something independent of it.
func crc16(b []byte) uint16 {
	var crc uint16
	for _, x := range b {
		crc ^= uint16(x) << 8
		for range 8 {
			if crc&0x8000 != 0 {
				crc = crc<<1 ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}

// slot is the hash slot of key: of the text between the first "{" and the next "}"
// when that is not empty, else of the whole key.
func slot(key string) int {
	if open := strings.IndexByte(key, '{'); open >= 0 {
		if end := strings.IndexByte(key[open+1:], '}'); end > 0 {
			key = key[open+1 : open+1+end]
		}
	}
	return int(crc16([]byte(key)) % 16384)
}

// The helper is held to the cluster specification's own examples, or a wrong one
// would pass the stream key and fail nothing.
func TestSlotHelperMatchesTheClusterSpecification(t *testing.T) {
	t.Parallel()
	if got := crc16([]byte("123456789")); got != 0x31C3 {
		t.Fatalf("crc16(123456789) = %#x, want 0x31c3, the check value in the Redis Cluster specification", got)
	}
	cases := []struct {
		a, b string
		same bool
		why  string
	}{
		{"{user1000}.following", "{user1000}.followers", true, "the specification's example: one tag, one slot"},
		{"foo{}{bar}", "foo{}{bar}x", false, "an empty tag hashes the whole key"},
		{"foo{{bar}}zap", "{bar", true, "the tag is between the first { and the next }"},
		{"foo{bar}{zap}", "bar", true, "only the first tag counts"},
	}
	for _, tc := range cases {
		if got := slot(tc.a) == slot(tc.b); got != tc.same {
			t.Errorf("slot(%q) == slot(%q) is %v, want %v: %s", tc.a, tc.b, got, tc.same, tc.why)
		}
	}
}
