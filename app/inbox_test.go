package app_test

import (
	"strings"
	"testing"

	notify "github.com/truongpx396/intel-notification"
	"github.com/truongpx396/intel-notification/app"
	"github.com/truongpx396/intel-notification/domain"
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
	for _, port := range []string{"Deps.Channels", "Deps.Inbox", "Deps.Renderer", "Deps.Topics"} {
		if !strings.Contains(err.Error(), port) {
			t.Errorf("error %q does not name %s", err, port)
		}
	}
	_, err = app.NewInbox(notify.Config{}, app.Deps{
		Channels: notifytest.NewRegistry(), Inbox: struct{ ports.InboxStore }{}, Renderer: notifytest.NewTemplates("en"), Topics: notifytest.NewTopics()})
	if err == nil {
		t.Fatal("NewInbox with an empty configuration succeeded")
	}
}

// The preference, schedule and unsubscribe methods of ports.Inbox belong to the REST
// surface's tasks (T026, T044). Until then they say so: one that returned nil would
// tell a recipient their choice was saved.
func TestTheInboxMethodsNotBuiltYetSaySo(t *testing.T) {
	t.Parallel()
	inbox, err := app.NewInbox(notify.Config{Realm: planRealm, StoreDSN: "x", RedisURL: "y"}, app.Deps{
		Channels: notifytest.NewRegistry(), Inbox: struct{ ports.InboxStore }{}, Renderer: notifytest.NewTemplates("en"), Topics: notifytest.NewTopics()})
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	if _, err := inbox.Preferences(ctx, planID); err == nil {
		t.Error("Preferences returned no error")
	}
	if err := inbox.SetPreferences(ctx, planID, []domain.Preference{{Topic: "t", Channel: "email"}}); err == nil {
		t.Error("SetPreferences returned no error")
	}
	if _, err := inbox.Schedule(ctx, planID); err == nil {
		t.Error("Schedule returned no error")
	}
	if err := inbox.SetSchedule(ctx, planID, domain.DeliverySchedule{}); err == nil {
		t.Error("SetSchedule returned no error")
	}
	if err := inbox.Unsubscribe(ctx, "token"); err == nil {
		t.Error("Unsubscribe returned no error")
	}
}
