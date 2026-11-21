package grpcclient_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/truongpx396/intel-notification/adapters/driving/grpcclient"
	"github.com/truongpx396/intel-notification/api/notifyv1"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/notifytest"
)

// T050, D31, D7. The host's Directory: asked a page at a time, for the realm that is asking, with a bound
// on how long it may take and on what it may answer.

type directoryServer struct {
	notifyv1.UnimplementedDirectoryServer
	on        func(context.Context, *notifyv1.ResolveAudienceRequest) (*notifyv1.ResolveAudienceResponse, error)
	asked     chan *notifyv1.ResolveAudienceRequest
	remaining chan time.Duration // the time left on the deadline each call arrived with; a call with none sends a day
}

func (d *directoryServer) ResolveAudience(ctx context.Context, req *notifyv1.ResolveAudienceRequest) (*notifyv1.ResolveAudienceResponse, error) {
	select {
	case d.asked <- req:
	default:
	}
	left := 24 * time.Hour
	if dl, ok := ctx.Deadline(); ok {
		left = time.Until(dl)
	}
	select {
	case d.remaining <- left:
	default:
	}
	return d.on(ctx, req)
}

func serveDirectory(t *testing.T, on func(context.Context, *notifyv1.ResolveAudienceRequest) (*notifyv1.ResolveAudienceResponse, error)) (grpc.ClientConnInterface, *directoryServer) {
	t.Helper()
	srv := &directoryServer{on: on, asked: make(chan *notifyv1.ResolveAudienceRequest, 16), remaining: make(chan time.Duration, 16)}
	return notifytest.NewBufconn(t, func(reg grpc.ServiceRegistrar) { notifyv1.RegisterDirectoryServer(reg, srv) }), srv
}

func page(ids ...string) *notifyv1.ResolveAudienceResponse {
	r := &notifyv1.ResolveAudienceResponse{}
	for _, id := range ids {
		r.Recipients = append(r.Recipients, &notifyv1.Recipient{Kind: "user", Id: id})
	}
	return r
}

func TestDirectoryAsksForAPageAndReturnsItWithTheNextCursor(t *testing.T) {
	t.Parallel()
	conn, srv := serveDirectory(t, func(_ context.Context, req *notifyv1.ResolveAudienceRequest) (*notifyv1.ResolveAudienceResponse, error) {
		r := page("u1", "u2")
		r.NextCursor = "after-u2"
		return r, nil
	})
	d := grpcclient.NewDirectory(map[domain.Realm]grpc.ClientConnInterface{"alpha": conn})
	got, next, err := d.Resolve(t.Context(), "alpha", domain.Tenant{Kind: "workspace", ID: "w1"}, "everyone", "cursor-1")
	if err != nil || next != "after-u2" || len(got) != 2 || got[0] != (domain.Recipient{Kind: "user", ID: "u1"}) {
		t.Fatalf("Resolve = %v, %q, %v, want the host's page and its next cursor", got, next, err)
	}
	asked := <-srv.asked
	if asked.GetTenant().GetKind() != "workspace" || asked.GetTenant().GetId() != "w1" || asked.GetAudience() != "everyone" || asked.GetCursor() != "cursor-1" {
		t.Fatalf("the host was asked %v, want the tenant, the selector and the cursor, as the engine has them", asked)
	}
}

// The empty cursor ends the audience: it is how a host says it is done, and the client does not turn it
// into anything else.
func TestDirectoryPassesTheEndOfTheAudienceThrough(t *testing.T) {
	t.Parallel()
	conn, _ := serveDirectory(t, func(context.Context, *notifyv1.ResolveAudienceRequest) (*notifyv1.ResolveAudienceResponse, error) {
		return page("u9"), nil
	})
	d := grpcclient.NewDirectory(map[domain.Realm]grpc.ClientConnInterface{"alpha": conn})
	got, next, err := d.Resolve(t.Context(), "alpha", domain.Tenant{Kind: "w", ID: "1"}, "all", "")
	if err != nil || next != "" || len(got) != 1 {
		t.Fatalf("Resolve = %v, %q, %v, want the last page and no next cursor", got, next, err)
	}
}

// NR-009. A realm is served by its own host's Directory, and a realm with none cannot resolve audiences.
func TestDirectoryRoutesByRealmAndAnUnservedRealmHasNone(t *testing.T) {
	t.Parallel()
	connA, srvA := serveDirectory(t, func(context.Context, *notifyv1.ResolveAudienceRequest) (*notifyv1.ResolveAudienceResponse, error) {
		return page("a1"), nil
	})
	connB, srvB := serveDirectory(t, func(context.Context, *notifyv1.ResolveAudienceRequest) (*notifyv1.ResolveAudienceResponse, error) {
		return page("b1"), nil
	})
	d := grpcclient.NewDirectory(map[domain.Realm]grpc.ClientConnInterface{"alpha": connA, "beta": connB})
	tenant := domain.Tenant{Kind: "w", ID: "1"}
	if got, _, err := d.Resolve(t.Context(), "alpha", tenant, "all", ""); err != nil || got[0].ID != "a1" {
		t.Fatalf("alpha: %v, %v", got, err)
	}
	if got, _, err := d.Resolve(t.Context(), "beta", tenant, "all", ""); err != nil || got[0].ID != "b1" {
		t.Fatalf("beta: %v, %v", got, err)
	}
	if got, _, err := d.Resolve(t.Context(), "gamma", tenant, "all", ""); !errors.Is(err, domain.ErrAudienceUnsupported) || got != nil {
		t.Fatalf("gamma: %v, %v, want ErrAudienceUnsupported: no host serves it", got, err)
	}
	if len(srvA.asked) != 1 || len(srvB.asked) != 1 {
		t.Fatalf("alpha's host was asked %d times and beta's %d, want once each", len(srvA.asked), len(srvB.asked))
	}
}

// The task's case: a Directory timeout fails the page. Nothing partial is returned, so the broadcast
// resumes at the page and does not commit a short one.
func TestADirectoryThatDoesNotAnswerInTimeFailsThePageAndReturnsNothing(t *testing.T) {
	t.Parallel()
	conn, srv := serveDirectory(t, func(ctx context.Context, req *notifyv1.ResolveAudienceRequest) (*notifyv1.ResolveAudienceResponse, error) {
		if req.GetCursor() == "warm" {
			return page("u1"), nil // answers at once: it only brings the connection up
		}
		<-ctx.Done() // the host never answers
		return nil, ctx.Err()
	})
	const timeout = 100 * time.Millisecond
	d := grpcclient.NewDirectory(map[domain.Realm]grpc.ClientConnInterface{"alpha": conn}, grpcclient.WithDirectoryTimeout(timeout))
	tenant := domain.Tenant{Kind: "w", ID: "1"}
	// The connection is dialled lazily and is slow when the machine is busy, and a deadline that runs out before
	// the host has seen the call proves nothing about the host. So the connection is brought up first, with a
	// call that is answered, and the timed call then measures the host's silence and not the dial.
	warmCtx, cancelWarm := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancelWarm()
	if _, _, err := d.Resolve(warmCtx, "alpha", tenant, "all", "warm"); err != nil {
		t.Fatalf("the warm-up call = %v", err)
	}
	<-srv.remaining
	// The caller's own deadline is far away: it is only here so that a client that set no timeout of its
	// own fails the check below after a few seconds and does not wait for ever.
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	got, next, err := d.Resolve(ctx, "alpha", tenant, "all", "c")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Resolve = %v, %q, %v, want an error that is a deadline", got, next, err)
	}
	select {
	case left := <-srv.remaining:
		if left > timeout {
			t.Fatalf("the host was asked with %s left on the call, want at most the %s the client is configured with: a slow host must not hold a worker beyond it", left, timeout)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the host never saw the call: the deadline ran out before it was made")
	}
	if got != nil || next != "" {
		t.Fatalf("Resolve returned %v and %q with an error, want nothing: a page is whole or it is not a page", got, next)
	}
}

func TestADirectoryFaultFailsThePageWithTheHostsStatus(t *testing.T) {
	t.Parallel()
	for name, serverErr := range map[string]error{
		"the host is down":    status.Error(codes.Unavailable, "no route to host"),
		"the host refuses":    status.Error(codes.PermissionDenied, "not for you"),
		"an unknown audience": status.Error(codes.NotFound, "no such audience"),
	} {
		conn, _ := serveDirectory(t, func(context.Context, *notifyv1.ResolveAudienceRequest) (*notifyv1.ResolveAudienceResponse, error) {
			return nil, serverErr
		})
		d := grpcclient.NewDirectory(map[domain.Realm]grpc.ClientConnInterface{"alpha": conn})
		got, next, err := d.Resolve(t.Context(), "alpha", domain.Tenant{Kind: "w", ID: "1"}, "all", "")
		if err == nil || got != nil || next != "" {
			t.Errorf("%s: Resolve = %v, %q, %v, want an error and nothing else", name, got, next, err)
		}
		if status.Code(errors.Unwrap(err)) != status.Code(serverErr) && status.Code(err) != status.Code(serverErr) {
			t.Errorf("%s: the error %v does not carry the host's code %v, which an operator reads", name, err, status.Code(serverErr))
		}
	}
}

// What a host returns is the host's: an identity with an empty component would equal an unset scope
// (D17), so a page that holds one is refused whole, and so is one larger than the engine will take.
func TestADirectoryPageThatCannotBeUsedIsRefusedWhole(t *testing.T) {
	t.Parallel()
	tooMany := &notifyv1.ResolveAudienceResponse{}
	for i := range grpcclient.MaxDirectoryPage + 1 {
		tooMany.Recipients = append(tooMany.Recipients, &notifyv1.Recipient{Kind: "user", Id: strings.Repeat("x", 1) + string(rune('a'+i%26)) + string(rune('a'+i/26%26)) + string(rune('a'+i/676))})
	}
	cases := map[string]*notifyv1.ResolveAudienceResponse{
		"a recipient with no id":   {Recipients: []*notifyv1.Recipient{{Kind: "user", Id: "u1"}, {Kind: "user", Id: ""}}},
		"a recipient with no kind": {Recipients: []*notifyv1.Recipient{{Kind: "", Id: "u1"}}},
		"a nil recipient":          {Recipients: []*notifyv1.Recipient{nil}},
		"a page too large":         tooMany,
	}
	for name, resp := range cases {
		conn, _ := serveDirectory(t, func(context.Context, *notifyv1.ResolveAudienceRequest) (*notifyv1.ResolveAudienceResponse, error) {
			return resp, nil
		})
		d := grpcclient.NewDirectory(map[domain.Realm]grpc.ClientConnInterface{"alpha": conn})
		got, next, err := d.Resolve(t.Context(), "alpha", domain.Tenant{Kind: "w", ID: "1"}, "all", "")
		if err == nil || got != nil || next != "" {
			t.Errorf("%s: Resolve = %d recipients, %q, %v, want it refused whole", name, len(got), next, err)
		}
	}
}

func TestADirectoryPageMayBeEmpty(t *testing.T) {
	t.Parallel()
	conn, _ := serveDirectory(t, func(context.Context, *notifyv1.ResolveAudienceRequest) (*notifyv1.ResolveAudienceResponse, error) {
		return &notifyv1.ResolveAudienceResponse{}, nil
	})
	d := grpcclient.NewDirectory(map[domain.Realm]grpc.ClientConnInterface{"alpha": conn})
	got, next, err := d.Resolve(t.Context(), "alpha", domain.Tenant{Kind: "w", ID: "1"}, "nobody", "")
	if err != nil || len(got) != 0 || next != "" {
		t.Fatalf("Resolve = %v, %q, %v, want an empty audience and no error", got, next, err)
	}
}
