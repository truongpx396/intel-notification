package app_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	notify "github.com/truongpx396/intel-notification"
	"github.com/truongpx396/intel-notification/app"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// T032: the operator's replay, with no database. The Maintenance behind it is a
// recording stand-in that says what it was asked; the statement's own guarantees (once
// per dead letter, however many callers) are the PostgreSQL suite's.

type recMaintenance struct {
	ports.Maintenance
	ev       *events
	replayed bool
	err      error

	realm domain.Realm
	id    string
}

func (m *recMaintenance) ReplayDeadLetter(_ context.Context, realm domain.Realm, id string) (bool, error) {
	m.ev.add("replay:" + id)
	m.realm, m.id = realm, id
	return m.replayed, m.err
}

func adminFx(t *testing.T, replayed bool, err error) (*app.Admin, *recMaintenance, *events) {
	t.Helper()
	ev := &events{}
	m := &recMaintenance{ev: ev, replayed: replayed, err: err}
	a, aerr := app.NewAdmin(notify.Config{Realm: planRealm, StoreDSN: "x", RedisURL: "y"},
		app.Deps{Maintenance: m, Waker: recWaker{ev: ev}})
	if aerr != nil {
		t.Fatalf("NewAdmin: %v", aerr)
	}
	return a, m, ev
}

func TestNewAdminNamesTheMissingPortAndRefusesABadConfig(t *testing.T) {
	t.Parallel()
	_, err := app.NewAdmin(notify.Config{Realm: planRealm, StoreDSN: "x", RedisURL: "y"}, app.Deps{})
	if err == nil || !strings.Contains(err.Error(), "Deps.Maintenance") {
		t.Fatalf("NewAdmin with no ports = %v, want an error naming Deps.Maintenance", err)
	}
	var ce *notify.ConfigError
	_, err = app.NewAdmin(notify.Config{StoreDSN: "x", RedisURL: "y"}, app.Deps{Maintenance: &recMaintenance{ev: &events{}}})
	if !errors.As(err, &ce) {
		t.Fatalf("NewAdmin with no realm = %v, want a *notify.ConfigError", err)
	}
}

// A replay asks the store, for the realm given and the id given, and a replay that
// enqueued work wakes the dispatcher of this process; one that did not (a second call)
// wakes nothing, because there is nothing new to claim.
func TestReplayDeadLetterAsksTheStoreAndWakesOnlyForNewWork(t *testing.T) {
	t.Parallel()
	const id = "7f1c7b0e-5a53-4f5c-9d1a-2b7a6b0f2f11"
	cases := []struct {
		name     string
		replayed bool
		want     []string
	}{
		{"the first replay", true, []string{"replay:" + id, "wake"}},
		{"a second replay is a no-op, not an error", false, []string{"replay:" + id}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			a, m, ev := adminFx(t, tc.replayed, nil)
			if err := a.ReplayDeadLetter(t.Context(), "acme", id); err != nil {
				t.Fatalf("ReplayDeadLetter = %v, want nil", err)
			}
			if m.realm != "acme" || m.id != id {
				t.Errorf("the store was asked for %q in %q, want %s in acme: the realm is the caller's, not the config's (service mode serves several)", m.id, m.realm, id)
			}
			if got := ev.all(); !slices.Equal(got, tc.want) {
				t.Fatalf("events %v, want %v", got, tc.want)
			}
		})
	}
}

// What the store refuses reaches the caller as the sentinel it can map, and wakes
// nothing. A request that cannot be right never reaches the store.
func TestReplayDeadLetterRefusals(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		realm   domain.Realm
		id      string
		storeEr error
		want    error
		asked   bool
	}{
		{"an unknown dead letter", "aisat", "7f1c7b0e-5a53-4f5c-9d1a-2b7a6b0f2f11", domain.ErrUnknownDeadLetter, domain.ErrUnknownDeadLetter, true},
		{"one parked by the bus", "aisat", "7f1c7b0e-5a53-4f5c-9d1a-2b7a6b0f2f11", domain.ErrNotReplayable, domain.ErrNotReplayable, true},
		{"a store fault", "aisat", "7f1c7b0e-5a53-4f5c-9d1a-2b7a6b0f2f11", fmt.Errorf("connection refused"), nil, true},
		{"no realm", "", "7f1c7b0e-5a53-4f5c-9d1a-2b7a6b0f2f11", nil, nil, false},
		{"a realm that is not one", "Not A Realm", "7f1c7b0e-5a53-4f5c-9d1a-2b7a6b0f2f11", nil, nil, false},
		{"no id", "aisat", "", nil, domain.ErrUnknownDeadLetter, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			a, _, ev := adminFx(t, false, tc.storeEr)
			err := a.ReplayDeadLetter(t.Context(), tc.realm, tc.id)
			if err == nil || (tc.want != nil && !errors.Is(err, tc.want)) {
				t.Fatalf("ReplayDeadLetter = %v, want an error that is %v", err, tc.want)
			}
			asked := len(ev.all()) > 0
			if asked != tc.asked {
				t.Fatalf("the store was asked = %v, want %v (events %v)", asked, tc.asked, ev.all())
			}
			if slices.Contains(ev.all(), "wake") {
				t.Fatal("a refused replay woke the dispatcher")
			}
		})
	}
}

// The parts of the operator surface that belong to later tasks say so, because a nil
// would tell an operator that a recipient was erased.
func TestTheAdminMethodsNotBuiltYetSaySo(t *testing.T) {
	t.Parallel()
	a, _, _ := adminFx(t, false, nil)
	if _, err := a.Erase(t.Context(), domain.Identity{}); err == nil || !strings.Contains(err.Error(), "T040") {
		t.Errorf("Erase = %v, want an error naming T040", err)
	}
	if err := a.SetTenantPreference(t.Context(), "aisat", domain.TenantPreference{}); err == nil || !strings.Contains(err.Error(), "T044") {
		t.Errorf("SetTenantPreference = %v, want an error naming T044", err)
	}
}
