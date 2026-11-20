//go:build integration

package grpcclient_test

import (
	"os"
	"testing"

	"github.com/truongpx396/intel-notification/adapters/driving/grpcclient"
	"github.com/truongpx396/intel-notification/adapters/driving/grpcserver"
	"github.com/truongpx396/intel-notification/internal/notifytest"
)

func TestMain(m *testing.M) { os.Exit(notifytest.Main(m, notifytest.WithRedis())) }

// wireEnv is an Env whose Notifier is the gRPC client of a server over the Env's own Notifier, all in
// memory. The suite's fault injection and its reads of the database still reach the engine behind the
// server, and its calls go through the whole gRPC stack.
func wireEnv(opts ...notifytest.Option) func(t *testing.T) *notifytest.Env {
	return func(t *testing.T) *notifytest.Env {
		t.Helper()
		e := notifytest.NewEnv(t, opts...)
		e.Notifier = grpcclient.New(notifytest.NewBufconn(t, grpcserver.New(e.Notifier).Register))
		return e
	}
}

// NotifierContract (T024) over gRPC (T047): the same suite as the in-process run, on an Env whose
// Notifier is the client, with the NotifyTx cases swapped for the one a remote Notifier owes (NR-025).
func TestNotifierContractOverGRPC(t *testing.T) {
	t.Parallel()
	notifytest.NotifierContract(t, wireEnv(), notifytest.Remote())
}

// ...and on the Redis pre-check, as T023 does for the in-process run.
func TestNotifierContractOverGRPCOnRedis(t *testing.T) {
	t.Parallel()
	notifytest.NotifierContract(t, wireEnv(notifytest.WithRedis()), notifytest.Remote())
}
