package app

import (
	"context"
	"errors"
	"time"

	notify "github.com/truongpx396/intel-notification"
)

// JobDigestFlush is the notify_job_leases row the digest flush takes (D22).
const JobDigestFlush = "digest_flush"

// DigestFlusher is the leased job that turns a closed digest window into its one delivery.
type DigestFlusher struct{}

// DigestOption changes how a DigestFlusher is built.
type DigestOption func(*DigestFlusher)

// WithFlushInterval sets how long Run waits between passes.
func WithFlushInterval(time.Duration) DigestOption { return func(*DigestFlusher) {} }

// WithFlushBatch sets how many due windows a pass reads from a shard at a time.
func WithFlushBatch(int) DigestOption { return func(*DigestFlusher) {} }

// WithFlushOwner sets the name the flusher holds the job's lease under.
func WithFlushOwner(string) DigestOption { return func(*DigestFlusher) {} }

// WithFlushErrorHandler sets where a fault the flusher survives is reported.
func WithFlushErrorHandler(func(error)) DigestOption { return func(*DigestFlusher) {} }

// NewDigestFlusher returns a DigestFlusher for cfg over d.
func NewDigestFlusher(notify.Config, Deps, ...DigestOption) (*DigestFlusher, error) {
	return &DigestFlusher{}, nil
}

// Flush makes one pass.
func (*DigestFlusher) Flush(context.Context) (int, error) { return 0, errors.New("not implemented") }

// Run flushes until ctx ends.
func (*DigestFlusher) Run(context.Context) error { return errors.New("not implemented") }
