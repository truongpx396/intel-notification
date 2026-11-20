package notifytest

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// NewBufconn serves what register adds to a gRPC server on an in-memory listener and returns a
// client connection to it, so a transport's tests run the whole gRPC stack (framing, status codes,
// deadlines) with no port and no network. Both ends stop when tb ends.
//
// A connection dials lazily, so a test that never calls the server never starts it.
func NewBufconn(tb testing.TB, register func(grpc.ServiceRegistrar), opts ...grpc.ServerOption) *grpc.ClientConn {
	tb.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(opts...)
	register(srv)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = srv.Serve(lis) // returns when srv.Stop below closes the listener
	}()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		tb.Fatalf("notifytest: dial the in-memory gRPC server: %v", err)
	}
	tb.Cleanup(func() {
		_ = conn.Close()
		srv.Stop()
		<-done
	})
	return conn
}
