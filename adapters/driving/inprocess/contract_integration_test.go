//go:build integration

package inprocess_test

import (
	"os"
	"testing"

	"github.com/truongpx396/intel-notification/adapters/driving/inprocess"
	"github.com/truongpx396/intel-notification/internal/notifytest"
)

func TestMain(m *testing.M) { os.Exit(notifytest.Main(m)) }

// NotifierContract (T024) through the in-process transport, unchanged: the Notifier a library host
// holds on the Engine is the one the suite already holds (NR-025).
func TestNotifierContractThroughTheInProcessEngine(t *testing.T) {
	t.Parallel()
	notifytest.NotifierContract(t, func(t *testing.T) *notifytest.Env {
		t.Helper()
		e := notifytest.NewEnv(t)
		engine := inprocess.Engine{Notifier: e.Notifier, Dispatcher: e.Dispatcher}
		e.Notifier = engine.Notifier
		return e
	})
}
