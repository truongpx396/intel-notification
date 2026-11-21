//go:build integration

package app_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/truongpx396/intel-notification/app"
	"github.com/truongpx396/intel-notification/internal/notifytest"
)

// T050, T042, D31. The host's audience, reached through the Directory callback over gRPC, expanded by the
// real worker over the real store: a host that does not answer in time fails the page it was asked for, the
// pages before it stand, and the broadcast resumes at the page and not at the start, with every recipient
// notified once.

func TestADirectoryTimeoutFailsThePageAndTheBroadcastResumesAtIt(t *testing.T) {
	t.Parallel()
	e, rs := broadcastEnv(t, 5)
	// The timeout is what the hung page costs, and it must be long enough that a healthy first page on a loaded
	// runner (the race detector, PostgreSQL and many tests at once) is not mistaken for a host that did not
	// answer: 250 ms was exceeded there once, on PostgreSQL 17's job, by the first page.
	host, directory := notifytest.NewHostDirectory(t, e, 3*time.Second)
	host.Stall("2") // the host hangs on the second page
	deps := e.BroadcastDeps()
	deps.Audience = directory
	worker, err := app.NewBroadcastExpander(e.Config, deps)
	if err != nil {
		t.Fatal(err)
	}
	announce(t, e, everyone("launch:2026"))

	pages, err := worker.Expand(t.Context())
	if !errors.Is(err, context.DeadlineExceeded) || pages != 1 {
		t.Fatalf("Expand = %d, %v, want the first page committed and the second failed by the host's timeout", pages, err)
	}
	if n := count(t, e, `SELECT count(*) FROM notifications`); n != 2 {
		t.Fatalf("%d notifications after the timeout, want the 2 of the first page and nothing of the failed one", n)
	}
	if got := broadcastState(t, e, "launch:2026"); got.cursor != "2" || got.fanned != 2 || got.status != "pending" {
		t.Fatalf("broadcast row = %+v, want pending at cursor 2 with 2 fanned: it resumes at the page, not at the start", got)
	}

	host.Stall("-") // the host recovers; the dead pass's lease lapses
	lapseBroadcasts(t, e)
	if pages, err := worker.Expand(t.Context()); err != nil || pages != 2 {
		t.Fatalf("the next pass: Expand = %d, %v, want the other 2 pages", pages, err)
	}
	if got := broadcastState(t, e, "launch:2026"); got.status != "completed" || got.fanned != 5 {
		t.Fatalf("broadcast row = %+v, want completed with 5 fanned", got)
	}
	if got := keysByRecipient(t, e, itTenant.ID); len(got) != len(rs) {
		t.Fatalf("%d recipients notified, want %d, each once", len(got), len(rs))
	}
	// The stalled question never reached the host's store, so it was asked for the first page, then, after
	// the recovery, for the page that failed, at its own cursor, and then the last. Never the first again.
	if got, want := cursorsAsked(e), []string{"", "2", "4"}; !slices.Equal(got, want) {
		t.Fatalf("the host's store was asked for cursors %q, want %q: the first page is not asked for again after the timeout", got, want)
	}
	if handled := drainAll(t, e); handled != 5 || e.Probe.Total() != 5 {
		t.Fatalf("the dispatcher handled %d deliveries and %d reached the provider, want 5 and 5", handled, e.Probe.Total())
	}
}
