package app

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"time"

	notify "github.com/truongpx396/intel-notification"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

const (
	// JobDigestFlush is the notify_job_leases row the digest flush takes (D22).
	JobDigestFlush = "digest_flush"

	// defaultFlushInterval is how long Run waits between passes. A window is minutes or
	// hours long, so a flush this much late is not one the recipient can see, and a pass
	// is one indexed read per shard.
	defaultFlushInterval = 5 * time.Second

	// defaultFlushBatch is how many due windows a pass reads from a shard at a time.
	defaultFlushBatch = 100

	// minFlushLease is the least time the job's lease lives for. The lease is renewed
	// before every batch, so this only has to outlast one batch of flushes (a statement
	// each) and the gap between two passes.
	minFlushLease = 30 * time.Second
)

// DigestFlusher is the job that turns a closed digest window into its one delivery
// (NR-014, D4). The appends are not its: a notification is folded into its window in the
// transaction that accepts it, and what a digest says is rendered when the dispatcher
// sends it, from the members that are still not canceled. This is the part between, the
// window's flush time: it finds the windows whose time has passed and flushes them.
//
// It is single-owner (D22). Each batch of windows is read while holding the job's lease
// in notify_job_leases, taken or renewed first, so any number of workers may run one and
// only the holder reads. That is also what a worker that died leaves behind: a lease that
// lapses, after which the next worker's pass finds its windows still due. The job keeps
// no memory of what it meant to flush, so a window that came due while no one was
// running is not skipped, only late. The flush itself is idempotent in the store, so a
// lease lost mid-batch costs nothing worse than a window flushed once by each of two
// workers, which is flushed once.
type DigestFlusher struct {
	digests ports.Digests
	jobs    ports.Maintenance
	waker   ports.Waker // nil: no dispatcher in this process to wake

	shards   int
	owner    string
	interval time.Duration
	batch    int
	onError  func(error)
}

// DigestOption changes how a DigestFlusher is built.
type DigestOption func(*DigestFlusher)

// WithFlushInterval sets how long Run waits between passes. The default is 5 seconds.
func WithFlushInterval(d time.Duration) DigestOption {
	return func(f *DigestFlusher) { f.interval = d }
}

// WithFlushBatch sets how many due windows a pass reads from a shard at a time. The
// default is 100.
func WithFlushBatch(n int) DigestOption {
	return func(f *DigestFlusher) { f.batch = n }
}

// WithFlushOwner sets the name the flusher holds the job's lease under, which
// notify_job_leases shows an operator. The default names the host and process and adds
// a random suffix, so two flushers never share one: a shared name would let each renew
// the other's lease, and the job would run twice.
func WithFlushOwner(owner string) DigestOption {
	return func(f *DigestFlusher) { f.owner = owner }
}

// WithFlushErrorHandler sets where a fault a pass survives is reported. The default
// discards them.
func WithFlushErrorHandler(h func(error)) DigestOption {
	return func(f *DigestFlusher) { f.onError = h }
}

// NewDigestFlusher returns a DigestFlusher for cfg over d. It refuses a configuration
// that does not validate, names every port it needs and d leaves nil, and refuses an
// option that could not work. Deps.Waker is optional.
func NewDigestFlusher(cfg notify.Config, d Deps, opts ...DigestOption) (*DigestFlusher, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	var g gaps
	g.need("Digests", d.Digests == nil)
	g.need("Maintenance", d.Maintenance == nil)
	if err := g.err("DigestFlusher"); err != nil {
		return nil, err
	}
	f := &DigestFlusher{
		digests: d.Digests, jobs: d.Maintenance, waker: d.Waker,
		shards: cfg.WithDefaults().Shards, owner: defaultFlushOwner(),
		interval: defaultFlushInterval, batch: defaultFlushBatch, onError: func(error) {},
	}
	for _, opt := range opts {
		opt(f)
	}
	switch {
	case f.interval <= 0:
		return nil, fmt.Errorf("app: DigestFlusher interval %v is not positive", f.interval)
	case f.batch < 1:
		return nil, fmt.Errorf("app: DigestFlusher batch %d is not at least one window", f.batch)
	case f.owner == "":
		return nil, errors.New("app: DigestFlusher owner has no name, and the job's lease is held under one")
	case f.onError == nil:
		f.onError = func(error) {}
	}
	return f, nil
}

func defaultFlushOwner() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	var suffix [4]byte
	_, _ = rand.Read(suffix[:])
	return fmt.Sprintf("%s/%d/%s", host, os.Getpid(), hex.EncodeToString(suffix[:]))
}

// leaseTTL is how long a lease lives for: long enough that the gap between two passes
// does not lapse it, so a worker that runs keeps the job.
func (f *DigestFlusher) leaseTTL() time.Duration { return max(minFlushLease, 3*f.interval) }

// Flush makes one pass and returns how many windows it flushed. It visits every shard in
// order and reads each in batches until one comes back short, taking or renewing the
// job's lease before every batch and stopping, without error, when another worker holds
// it. A window that will not flush, or a shard that cannot be read, is reported in the
// error and does not stop the others; a batch in which nothing could be flushed is not
// read again, so a fault that repeats ends the pass and does not spin it. A pass that
// flushed anything wakes the dispatcher of this process.
func (f *DigestFlusher) Flush(ctx context.Context) (int, error) {
	var (
		flushed int
		errs    []error
	)
pass:
	for shard := range f.shards {
		for ctx.Err() == nil {
			held, err := f.jobs.TryLeaseJob(ctx, JobDigestFlush, f.owner, f.leaseTTL())
			if err != nil {
				errs = append(errs, fmt.Errorf("lease the %s job: %w", JobDigestFlush, err))
				break pass
			}
			if !held {
				break pass
			}
			n, more, err := f.flushBatch(ctx, domain.Shard(shard))
			flushed += n
			if err != nil {
				errs = append(errs, err)
			}
			if !more {
				break
			}
		}
	}
	if flushed > 0 && f.waker != nil {
		f.waker.Wake()
	}
	return flushed, errors.Join(errs...)
}

// flushBatch reads one batch of a shard's due windows and flushes each. It reports how
// many it queued, and whether another batch is worth reading: this one was full and some
// of it could be flushed.
func (f *DigestFlusher) flushBatch(ctx context.Context, shard domain.Shard) (flushed int, more bool, err error) {
	ids, err := f.digests.DueDigests(ctx, shard, f.batch)
	if err != nil {
		return 0, false, fmt.Errorf("read the due digests of shard %d: %w", shard, err)
	}
	var (
		errs       []error
		progressed bool
	)
	for _, id := range ids {
		queued, ferr := f.digests.FlushDigest(ctx, id)
		if ferr != nil {
			errs = append(errs, ferr)
			continue
		}
		progressed = true
		if queued {
			flushed++ // false: another worker flushed it first, and the delivery is theirs
		}
	}
	return flushed, len(ids) >= f.batch && progressed, errors.Join(errs...)
}

// Run flushes at once and then every interval, until ctx ends, and returns nil. A pass
// that failed is reported to the error handler and the loop goes on. Run is one loop;
// more workers are more loops, and the lease keeps them from flushing the same windows.
func (f *DigestFlusher) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		if _, err := f.Flush(ctx); err != nil && ctx.Err() == nil {
			f.onError(err)
		}
		timer := time.NewTimer(f.interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
	return nil
}
