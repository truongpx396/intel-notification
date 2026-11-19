package app

import (
	"context"
	"time"

	notify "github.com/truongpx396/intel-notification"
)

// BroadcastExpander is the job that turns a durable broadcast into notifications, a page
// at a time (NR-020, D7). T042's stub: it compiles so the suite can start, and says so
// until T041 builds it.
type BroadcastExpander struct{}

// BroadcastOption changes how a BroadcastExpander is built.
type BroadcastOption func(*BroadcastExpander)

// WithBroadcastPage sets how many inline recipients a page holds.
func WithBroadcastPage(n int) BroadcastOption { return func(*BroadcastExpander) {} }

// WithBroadcastLease sets how long the lease on one page lives for.
func WithBroadcastLease(d time.Duration) BroadcastOption { return func(*BroadcastExpander) {} }

// WithBroadcastInterval sets how long Run waits between passes.
func WithBroadcastInterval(d time.Duration) BroadcastOption { return func(*BroadcastExpander) {} }

// WithBroadcastErrorHandler sets where a fault a pass survives is reported.
func WithBroadcastErrorHandler(h func(error)) BroadcastOption { return func(*BroadcastExpander) {} }

// NewBroadcastExpander returns a BroadcastExpander for cfg over d.
func NewBroadcastExpander(cfg notify.Config, d Deps, opts ...BroadcastOption) (*BroadcastExpander, error) {
	return &BroadcastExpander{}, nil
}

// Expand makes one pass and returns how many pages it committed.
func (e *BroadcastExpander) Expand(ctx context.Context) (int, error) {
	return 0, errNotBuilt("BroadcastExpander.Expand", "T041")
}

// Run expands at once and then every interval, until ctx ends.
func (e *BroadcastExpander) Run(ctx context.Context) error {
	return errNotBuilt("BroadcastExpander.Run", "T041")
}
