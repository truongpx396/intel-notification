package app_test

import (
	"strings"
	"testing"

	notify "github.com/truongpx396/intel-notification"
	"github.com/truongpx396/intel-notification/app"
	"github.com/truongpx396/intel-notification/internal/notifytest"
	"github.com/truongpx396/intel-notification/ports"
)

// NewInbox names every port it needs and is not given, and refuses a configuration
// that does not validate.
func TestNewInboxNamesEveryMissingPortAndRefusesABadConfig(t *testing.T) {
	t.Parallel()
	cfg := notify.Config{Realm: planRealm, StoreDSN: "x", RedisURL: "y"}
	_, err := app.NewInbox(cfg, app.Deps{})
	if err == nil {
		t.Fatal("NewInbox with no ports succeeded")
	}
	for _, port := range []string{"Deps.Channels", "Deps.Inbox", "Deps.Prefs", "Deps.Renderer", "Deps.Topics"} {
		if !strings.Contains(err.Error(), port) {
			t.Errorf("error %q does not name %s", err, port)
		}
	}
	_, err = app.NewInbox(notify.Config{}, app.Deps{
		Channels: notifytest.NewRegistry(), Inbox: struct{ ports.InboxStore }{}, Prefs: notifytest.NewPreferences(), Renderer: notifytest.NewTemplates("en"), Topics: notifytest.NewTopics()})
	if err == nil {
		t.Fatal("NewInbox with an empty configuration succeeded")
	}
}
