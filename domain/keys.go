package domain

import (
	"crypto/sha256"
	"encoding/hex"
)

// ChannelKind selects a channel implementation. The engine never branches on
// one; it reads the channel's capabilities.
type ChannelKind string

// Every string or hash derived from an identity is defined here, over
// [Canonical], so that no two call sites can derive the same key differently
// (D18). Each function has a frozen test vector.

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// AddressKey is an address's stable identity on one channel:
// hex(sha256(Canonical(channel, normalized)))[:32]. normalized is the channel's
// normalization of the raw value (a lowercased email domain, an E.164 number).
func AddressKey(ch ChannelKind, normalized string) string {
	return sha256Hex(Canonical(string(ch), normalized))[:32]
}

// DeliveryIdemKey is what a channel dedupes on (D25). It is distinct per address
// and per digest, identical across re-drives of one delivery, and distinct
// across realms. digestID is "" for a notification; idemKey is "" for a digest.
func DeliveryIdemKey(id Identity, idemKey, digestID string, ch ChannelKind, addressKey string) string {
	subject := idemKey
	if digestID != "" {
		subject = "digest:" + digestID
	}
	return sha256Hex(Canonical(string(id.Realm), id.Tenant.Kind, id.Tenant.ID,
		id.Recipient.Kind, id.Recipient.ID, subject, string(ch), addressKey))
}

// BroadcastMemberKey derives recipient r's idempotency key from a broadcast's
// key, so a retried broadcast notifies nobody twice (NR-020).
func BroadcastMemberKey(broadcastIdemKey string, r Recipient) string {
	return sha256Hex(Canonical("broadcast", broadcastIdemKey, r.Kind, r.ID))
}

// SuppressionHash keys channel_suppressions. The address itself is never stored,
// so a suppression can outlive the recipient's erasure (D35).
func SuppressionHash(ch ChannelKind, normalized string) []byte {
	sum := sha256.Sum256([]byte(Canonical(string(ch), normalized)))
	return sum[:]
}

// SubjectHash records an erasure without keeping who it was for (D35).
func SubjectHash(id Identity) []byte {
	sum := sha256.Sum256([]byte(id.Canonical()))
	return sum[:]
}

// PreCheckKey is the Redis pre-check key: the durable guard's key space, hashed
// (D18). No hash tag: it is a single-key operation, and tagging by realm would
// put a whole product's traffic on one cluster slot.
func PreCheckKey(id Identity, idemKey string) string {
	return "notify:applied:" + string(id.Realm) + ":" +
		sha256Hex(Canonical(string(id.Realm), id.Tenant.Kind, id.Tenant.ID,
			id.Recipient.Kind, id.Recipient.ID, idemKey))
}

// StreamKey is the live nudge channel for one identity (D16). The braces are a
// Redis Cluster hash tag, so sharded pub/sub keeps one recipient on one slot.
func StreamKey(id Identity) string {
	return "notify:inbox:{" + sha256Hex(id.Canonical()) + "}"
}
