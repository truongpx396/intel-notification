package app

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	notify "github.com/truongpx396/intel-notification"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

const (
	// defaultBroadcastPage is how many inline recipients a page holds. A page is one
	// transaction, so it is bounded work: a few hundred planned notifications and their
	// rows, and a crash redoes at most this many. An audience's pages are the resolver's.
	defaultBroadcastPage = 500

	// defaultBroadcastLease is how long the lease on one page lives for. It only has to
	// outlast one page (a resolver call, a plan for each recipient and the commit): the
	// commit releases it, and a worker that dies holding it leaves the page waiting this
	// long.
	defaultBroadcastLease = 2 * time.Minute

	// defaultBroadcastInterval is how long Run waits between passes. A pass that found
	// work goes on until there is none, so this is only how soon a new broadcast is seen.
	defaultBroadcastInterval = time.Second
)

// BroadcastExpander is the job that turns a durable broadcast into notifications, a page at
// a time (NR-020, D7). Notifier.Broadcast only records the request; this is the worker that
// resolves who it is for and notifies each of them, off the caller's time.
//
// A page is one lease, one resolver call and one transaction. The worker leases the next
// broadcast with pages left at its cursor, asks for the page's recipients (a slice of the
// inline list, or the AudienceResolver's next page), plans each of them as Notify would,
// and commits the page's notifications together with the cursor's advance, fenced by the
// lease token. So a crash between pages resumes at the next page, a crash inside one rolls
// the page back as a whole and it is taken again from its own first recipient, a worker
// whose lease lapsed has its page discarded, and a recipient is notified once however the
// pages fall: each one's key derives from the broadcast's, by [domain.BroadcastMemberKey],
// and never from the page that carried them.
//
// It keeps no memory of what it meant to do, and any number of workers may run one: the
// lease on the page is what keeps two off it, and the database is what resumes it.
type BroadcastExpander struct {
	planner  *Planner
	store    ports.Store
	audience ports.AudienceResolver // nil: inline lists only
	waker    ports.Waker            // nil: no dispatcher in this process to wake

	page     int
	lease    time.Duration
	interval time.Duration
	onError  func(error)
}

// BroadcastOption changes how a BroadcastExpander is built.
type BroadcastOption func(*BroadcastExpander)

// WithBroadcastPage sets how many recipients of an inline list a page holds. The default is
// 500. An audience's page is whatever its resolver returns.
func WithBroadcastPage(n int) BroadcastOption {
	return func(e *BroadcastExpander) { e.page = n }
}

// WithBroadcastLease sets how long the lease on one page lives for. It must outlast the
// page: a lease that lapses mid-page has the page discarded at its commit, and taken again
// by another worker. The default is 2 minutes.
func WithBroadcastLease(d time.Duration) BroadcastOption {
	return func(e *BroadcastExpander) { e.lease = d }
}

// WithBroadcastInterval sets how long Run waits between passes. The default is 1 second.
func WithBroadcastInterval(d time.Duration) BroadcastOption {
	return func(e *BroadcastExpander) { e.interval = d }
}

// WithBroadcastErrorHandler sets where a fault a pass survives is reported: a pass that
// failed, and a recipient the resolver returned that could not be notified. The default
// discards them.
func WithBroadcastErrorHandler(h func(error)) BroadcastOption {
	return func(e *BroadcastExpander) { e.onError = h }
}

// NewBroadcastExpander returns a BroadcastExpander for cfg over d. It refuses a
// configuration that does not validate, names every port it needs and d leaves nil, and
// refuses an option that could not work. Deps.Audience is optional: without it the
// expander takes inline lists only. Deps.Waker is optional too, and is told after each
// page that deliveries are due.
func NewBroadcastExpander(cfg notify.Config, d Deps, opts ...BroadcastOption) (*BroadcastExpander, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	var g gaps
	g.need("Channels", d.Channels == nil)
	g.need("Prefs", d.Prefs == nil)
	g.need("Quotas", d.Quotas == nil)
	g.need("Store", d.Store == nil)
	g.need("Topics", d.Topics == nil)
	if err := g.err("BroadcastExpander"); err != nil {
		return nil, err
	}
	planner, err := NewPlanner(cfg, d)
	if err != nil {
		return nil, err
	}
	e := &BroadcastExpander{
		planner: planner, store: d.Store, audience: d.Audience, waker: d.Waker,
		page: defaultBroadcastPage, lease: defaultBroadcastLease, interval: defaultBroadcastInterval,
		onError: func(error) {},
	}
	for _, opt := range opts {
		opt(e)
	}
	switch {
	case e.page < 1:
		return nil, fmt.Errorf("app: BroadcastExpander page %d is not at least one recipient", e.page)
	case e.lease <= 0:
		return nil, fmt.Errorf("app: BroadcastExpander lease %v is not positive", e.lease)
	case e.interval <= 0:
		return nil, fmt.Errorf("app: BroadcastExpander interval %v is not positive", e.interval)
	case e.onError == nil:
		e.onError = func(error) {}
	}
	return e, nil
}

// Expand makes one pass and returns how many pages it committed. It leases the next
// broadcast with pages left, expands one page of it and commits it, and goes on until
// nothing is left to lease or ctx ends, which it does between pages and without error: a
// page is never left half done, because its commit is its only write.
//
// The first fault ends the pass, so a fault that repeats does not spin it: the page's lease
// lapses and the next pass takes the page again at its cursor, with nothing of it written.
// A commit that finds the lease lost is not a fault, and is not counted: the worker that
// holds the lease has the page. The dispatcher of this process is woken after each page's
// commit, and not at the end of the pass, because the first page's deliveries are due at
// once and a broadcast of a hundred thousand would otherwise leave them waiting for the
// rest of it (D22).
func (e *BroadcastExpander) Expand(ctx context.Context) (int, error) {
	pages := 0
	for ctx.Err() == nil {
		job, err := e.store.LeaseBroadcast(ctx, e.lease)
		if err != nil {
			return pages, fmt.Errorf("lease a broadcast: %w", err)
		}
		if job == nil {
			return pages, nil
		}
		page, next, done, err := e.expandPage(ctx, job)
		if err != nil {
			return pages, err
		}
		err = e.store.CommitBroadcastPage(ctx, *job, page, next, done)
		if errors.Is(err, domain.ErrBroadcastLeaseLost) {
			continue
		}
		if err != nil {
			return pages, fmt.Errorf("commit the page of broadcast %s at cursor %q: %w", job.ID, job.Cursor, err)
		}
		pages++
		if e.waker != nil {
			e.waker.Wake()
		}
	}
	return pages, nil
}

// expandPage resolves one page of job's recipients and plans a notification for each. It
// returns the page, the cursor the page after it begins at, and whether this is the last.
func (e *BroadcastExpander) expandPage(ctx context.Context, job *domain.BroadcastJob) ([]domain.PageItem, string, bool, error) {
	recipients, next, done, err := e.resolve(ctx, job)
	if err != nil {
		return nil, "", false, fmt.Errorf("resolve the page of broadcast %s at cursor %q: %w", job.ID, job.Cursor, err)
	}

	req := job.Request
	items := make([]domain.PageItem, 0, len(recipients))
	skipped := 0
	for _, r := range recipients {
		// What an AudienceResolver returns is the host's. A recipient with an empty
		// component would equal an unset scope (D17), and one such must not hold a
		// tenant's whole broadcast for ever, nor be lost without a word: it is skipped
		// and reported.
		if r.Kind == "" || r.ID == "" {
			skipped++
			continue
		}
		n := domain.Notification{
			Tenant: req.Tenant, Recipient: r, Topic: req.Topic, Priority: req.Priority,
			Data: req.Data, Title: req.Title, Body: req.Body, Payload: req.Payload,
			IdemKey: domain.BroadcastMemberKey(req.IdemKey, r),
		}
		planned, err := e.planner.Plan(ctx, n.Identity(job.Realm), n)
		if err != nil {
			return nil, "", false, fmt.Errorf("plan broadcast %s for %s/%s: %w", job.ID, r.Kind, r.ID, err)
		}
		items = append(items, domain.PageItem{Notification: planned.Notification, Plan: planned.Plan})
	}
	if skipped > 0 {
		e.onError(fmt.Errorf("broadcast %s, page at cursor %q: skipped %d recipient(s) with an empty kind or id: %w",
			job.ID, job.Cursor, skipped, domain.ErrEmptyIdentity))
	}
	return items, next, done, nil
}

// resolve returns the recipients of the page at job's cursor, the cursor of the next, and
// whether this is the last page. An inline list is paged here, by offset, and its last page
// is the one that reaches the end of the list. An audience is the resolver's, whose empty
// next cursor is the end. A resolver that hands back the cursor it was given with a page
// would be asked for that page for ever, so it is a fault.
func (e *BroadcastExpander) resolve(ctx context.Context, job *domain.BroadcastJob) (recipients []domain.Recipient, next string, done bool, err error) {
	req := job.Request
	if len(req.Recipients) > 0 {
		offset := 0
		if job.Cursor != "" {
			n, err := strconv.Atoi(job.Cursor)
			if err != nil || n < 0 || n > len(req.Recipients) {
				return nil, "", false, fmt.Errorf("cursor %q is not an offset into the %d recipients", job.Cursor, len(req.Recipients))
			}
			offset = n
		}
		end := min(offset+e.page, len(req.Recipients))
		return req.Recipients[offset:end], strconv.Itoa(end), end == len(req.Recipients), nil
	}
	if e.audience == nil {
		return nil, "", false, domain.ErrAudienceUnsupported
	}
	recipients, next, err = e.audience.Resolve(ctx, job.Realm, req.Tenant, req.Audience, job.Cursor)
	if err != nil {
		return nil, "", false, err
	}
	if next != "" && next == job.Cursor {
		return nil, "", false, fmt.Errorf("the resolver returned the cursor %q it was given, so the broadcast would not advance", next)
	}
	return recipients, next, next == "", nil
}

// Run expands at once and then every interval, until ctx ends, and returns nil. A pass that
// failed is reported to the error handler and the loop goes on. Run is one loop; more
// workers are more loops, and the page lease keeps them off the same page.
func (e *BroadcastExpander) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		if _, err := e.Expand(ctx); err != nil && ctx.Err() == nil {
			e.onError(err)
		}
		timer := time.NewTimer(e.interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
	return nil
}
