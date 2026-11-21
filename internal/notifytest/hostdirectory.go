//go:build integration

package notifytest

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"

	"github.com/truongpx396/intel-notification/adapters/driving/grpcclient"
	"github.com/truongpx396/intel-notification/api/notifyv1"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// HostDirectory is a host's Directory service over gRPC, answering from an Env's Audiences, and hanging on
// the cursor it is told to, as a host that stalls on one query would. It lives here, and not in the test
// that uses it, so that a test in the core's packages can reach the Directory client without importing grpc.
type HostDirectory struct {
	notifyv1.UnimplementedDirectoryServer
	audiences *Audiences
	stallAt   atomic.Value // string: the cursor to hang on; "-" for none
}

// NewHostDirectory serves e's Audiences as the host of Realm, and returns the host together with the
// Directory client a service would hold for it, whose calls give up after timeout.
func NewHostDirectory(tb testing.TB, e *Env, timeout time.Duration) (*HostDirectory, ports.AudienceResolver) {
	tb.Helper()
	host := &HostDirectory{audiences: e.Audiences}
	host.Stall("-")
	conn := NewBufconn(tb, func(reg grpc.ServiceRegistrar) { notifyv1.RegisterDirectoryServer(reg, host) })
	// A connection dials lazily, and a client whose timeout is a few milliseconds must not spend it on the dial: on a
	// loaded machine the first page failed by the host's "timeout" before the host had been asked anything.
	conn.Connect()
	ready, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for state := conn.GetState(); state != connectivity.Ready; state = conn.GetState() {
		if !conn.WaitForStateChange(ready, state) {
			tb.Fatalf("the host's Directory connection did not come up: %v", state)
		}
	}
	return host, grpcclient.NewDirectory(map[domain.Realm]grpc.ClientConnInterface{Realm: conn}, grpcclient.WithDirectoryTimeout(timeout))
}

// Stall makes the host hang on the question that carries cursor, until the caller gives up; "-" recovers it.
func (h *HostDirectory) Stall(cursor string) { h.stallAt.Store(cursor) }

// ResolveAudience implements notifyv1.DirectoryServer.
func (h *HostDirectory) ResolveAudience(ctx context.Context, req *notifyv1.ResolveAudienceRequest) (*notifyv1.ResolveAudienceResponse, error) {
	if stall, _ := h.stallAt.Load().(string); stall == req.GetCursor() {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	got, next, err := h.audiences.Resolve(ctx, Realm, domain.Tenant{Kind: req.GetTenant().GetKind(), ID: req.GetTenant().GetId()}, req.GetAudience(), req.GetCursor())
	if err != nil {
		return nil, err
	}
	resp := &notifyv1.ResolveAudienceResponse{NextCursor: next}
	for _, r := range got {
		resp.Recipients = append(resp.Recipients, &notifyv1.Recipient{Kind: r.Kind, Id: r.ID})
	}
	return resp, nil
}
