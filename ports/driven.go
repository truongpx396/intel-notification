// Package ports holds the interfaces at the hexagon's edges. The use-cases in
// app/ depend on these and on domain/, never on an adapter.
//
// This file declares the driven ports whose PostgreSQL implementation exists:
// the queue's state transitions, digest windows, and the single-owner
// maintenance operations. The rest of the driven surface in
// specs/001-notification-core/contracts/notification-ports.md — the persist and
// read paths, preferences, templates, addresses, channels — lands with the tasks
// that implement it.
package ports

import (
	"context"
	"time"

	"github.com/truongpx396/intel-notification/domain"
)

// Queue is the delivery queue's state machine (D19, D20, D25). Every method that
// takes a Claim is FENCED: it changes state only while the claim's lease token is
// current, and reports ok=false — having written nothing — when the lease was
// lost to another claimer. A caller that gets ok=false must drop the delivery:
// the row's current holder owns it.
type Queue interface {
	// Claim leases up to max due deliveries in one shard, skipping rows another
	// claimer holds. Each claim counts as an attempt, so a delivery that crashes
	// its worker every time still reaches the ceiling.
	Claim(ctx context.Context, shard domain.Shard, max int, lease time.Duration) ([]domain.Claim, error)

	// Retry records a transient failure: the delivery is due again at next, and
	// the lease is released.
	Retry(ctx context.Context, c domain.Claim, next time.Time, lastError string) (ok bool, err error)

	// Defer records a quota or quiet-hours deferral: due again at until, and the
	// claim's attempt is given back, so deferral never walks a delivery into the
	// dead-letter table.
	Defer(ctx context.Context, c domain.Claim, until time.Time, reason string) (ok bool, err error)

	// Finish is the terminal transition: in one transaction it removes the queue
	// row, appends it to the delivery history, and — as f.Disposition says —
	// writes a dead letter and enqueues the next fallback channel. The store
	// decides none of that itself (D37).
	Finish(ctx context.Context, c domain.Claim, f domain.Finish) (ok bool, err error)

	// BindAddresses pins an unbound claim to its resolved addresses. One address
	// binds in place and the claim stays held; several replace the row with one
	// unleased row per address, recorded as fanned_out. keys[i] is addrs[i]'s
	// domain.AddressKey. An empty addrs is an error: finish as no_address instead.
	BindAddresses(ctx context.Context, c domain.Claim, addrs []domain.Address, keys []string) (b domain.Binding, ok bool, err error)

	// DeferTenantChannel moves a tenant's whole due backlog on one channel to
	// until, so a tenant that exhausted its quota stops occupying the head of
	// every shard (D24). Rows under a live lease are left to their claimer.
	DeferTenantChannel(ctx context.Context, realm domain.Realm, t domain.Tenant, ch domain.ChannelKind, until time.Time) (int, error)

	// Cancel withdraws one notification: its inbox row is marked canceled and
	// every pending delivery no claimer holds becomes canceled. Deliveries under
	// a live lease are reported as in flight, never recorded as canceled (D29).
	Cancel(ctx context.Context, id domain.Identity, idemKey string) (domain.CancelReceipt, error)
}

// Digests holds coalescing windows (D4).
type Digests interface {
	// AppendDigest folds a notification into its recipient's open window for
	// (topic, channel), opening one if none is open and sealing it at max
	// members. Returns the window id.
	AppendDigest(ctx context.Context, m domain.DigestMember, window time.Duration, max int) (string, error)

	// DueDigests lists windows in one shard whose flush time has passed.
	DueDigests(ctx context.Context, shard domain.Shard, max int) ([]string, error)

	// FlushDigest seals a window and enqueues its one delivery. A second call is
	// a no-op that returns false, so a duplicate tick never sends two digests.
	FlushDigest(ctx context.Context, windowID string) (queued bool, err error)
}

// Maintenance holds the single-owner jobs' operations (D11, D21, D22, D35).
type Maintenance interface {
	// TryLeaseJob takes or renews the lease for a scheduled job. True when owner
	// now holds it; false while another owner's lease is live.
	TryLeaseJob(ctx context.Context, job, owner string, ttl time.Duration) (bool, error)

	// RehomeShards folds rows in shards >= shards into live ones, after the
	// shard count is lowered. Safe while claimers run.
	RehomeShards(ctx context.Context, shards int) (int, error)

	// ExpireIdem deletes up to batch idempotency keys created before olderThan.
	ExpireIdem(ctx context.Context, olderThan time.Time, batch int) (int, error)

	// ExpireDigests deletes up to batch flushed windows older than olderThan
	// whose delivery has finished.
	ExpireDigests(ctx context.Context, olderThan time.Time, batch int) (int, error)

	// Erase removes one recipient's personal data from every table that holds
	// it, in one transaction, and records the erasure by hash. Suppressions are
	// kept: they hold address hashes, and dropping them would resume mailing
	// someone who complained. Returns rows removed per table.
	Erase(ctx context.Context, id domain.Identity) (map[string]int, error)

	// EnsurePartitions creates each missing monthly partition of the
	// range-partitioned tables, for months months from the one containing from,
	// and scopes every new notifications partition (D17). Returns the partitions
	// created; a second run creates none.
	EnsurePartitions(ctx context.Context, from time.Time, months int) ([]string, error)

	// CheckPartitions reports, from the catalog alone, each range-partitioned
	// table with no partition covering the month containing at or the next one,
	// and notifications or any of its partitions that recipient scoping does not
	// hold on. It changes nothing; the report is what the notify.partition.missing
	// and notify.partition.unscoped alarms fire on.
	CheckPartitions(ctx context.Context, at time.Time) (domain.PartitionHealth, error)
}
