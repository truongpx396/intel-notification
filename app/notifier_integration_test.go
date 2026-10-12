//go:build integration

package app_test

import (
	"os"
	"testing"

	"github.com/truongpx396/intel-notification/internal/notifytest"
)

func TestMain(m *testing.M) { os.Exit(notifytest.Main(m)) }

// NotifierContract (T024) against the in-process Notifier, on an Env whose
// pre-check is the in-memory one. T023 runs it again on Redis.
func TestNotifierContract(t *testing.T) {
	t.Parallel()
	notifytest.NotifierContract(t, func(t *testing.T) *notifytest.Env {
		t.Helper()
		return notifytest.NewEnv(t)
	})
}
