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

	// Erase: what it answers, and who it was asked about.
	counts   map[string]int
	eraseErr error
	erased   []domain.Identity
}

func (m *recMaintenance) Erase(_ context.Context, id domain.Identity) (map[string]int, error) {
	m.ev.add("erase:" + id.Recipient.ID)
	m.erased = append(m.erased, id)
	return m.counts, m.eraseErr
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

// The part of the operator surface that belongs to a later task says so, because a nil
// would tell an operator that a tenant's default was set.
func TestTheAdminMethodsNotBuiltYetSaySo(t *testing.T) {
	t.Parallel()
	a, _, _ := adminFx(t, false, nil)
	if err := a.SetTenantPreference(t.Context(), "aisat", domain.TenantPreference{}); err == nil || !strings.Contains(err.Error(), "T044") {
		t.Errorf("SetTenantPreference = %v, want an error naming T044", err)
	}
}

// ---------------------------------------------------------------- erasure --

var erasable = domain.Identity{
	Realm:     "aisat",
	Tenant:    domain.Tenant{Kind: "workspace", ID: "w1"},
	Recipient: domain.Recipient{Kind: "user", ID: "u9"},
}

// T040: erasure asks the store for the identity it was given, once, and hands back what the
// store removed so the operator can see it (D35). That nothing of the recipient is left,
// in any table the catalog lists, and that the suppression is, are the PostgreSQL suite's
// (TestErase) and NS-012's end to end (TestNS012...); here it is that the use-case
// does not stand between them.
func TestEraseHandsTheIdentityToTheStoreAndReturnsWhatItRemoved(t *testing.T) {
	t.Parallel()
	a, m, ev := adminFx(t, false, nil)
	m.counts = map[string]int{"notifications": 3, "notification_outbox": 1, "recipient_addresses": 2}

	got, err := a.Erase(t.Context(), erasable)
	if err != nil {
		t.Fatalf("Erase = %v, want nil", err)
	}
	if !slices.Equal(ev.all(), []string{"erase:u9"}) || len(m.erased) != 1 || m.erased[0] != erasable {
		t.Fatalf("the store was asked %v about %v, want one erasure of %v", ev.all(), m.erased, erasable)
	}
	if fmt.Sprint(got) != fmt.Sprint(m.counts) {
		t.Fatalf("Erase returned %v, want what the store removed: %v", got, m.counts)
	}
}

// An identity with an empty component never reaches the store. The schema would refuse
// the scope too, but an erasure that fails halfway through a table list is worse than one
// that never starts, and an empty component is the one that equals an unset scope setting.
func TestEraseRefusesAnIdentityWithAnEmptyComponentBeforeTouchingTheStore(t *testing.T) {
	t.Parallel()
	cases := map[string]func(*domain.Identity){
		"no realm":          func(i *domain.Identity) { i.Realm = "" },
		"no tenant kind":    func(i *domain.Identity) { i.Tenant.Kind = "" },
		"no tenant id":      func(i *domain.Identity) { i.Tenant.ID = "" },
		"no recipient kind": func(i *domain.Identity) { i.Recipient.Kind = "" },
		"no recipient id":   func(i *domain.Identity) { i.Recipient.ID = "" },
	}
	for name, blank := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			a, _, ev := adminFx(t, false, nil)
			id := erasable
			blank(&id)
			counts, err := a.Erase(t.Context(), id)
			if !errors.Is(err, domain.ErrEmptyIdentity) || counts != nil {
				t.Fatalf("Erase = %v, %v, want domain.ErrEmptyIdentity and no counts", counts, err)
			}
			if got := ev.all(); len(got) != 0 {
				t.Fatalf("the store was asked %v for an identity it cannot scope", got)
			}
		})
	}
}

// A store that fails does not report an erasure: no counts, the fault wrapped so the
// caller can see what failed, and nothing to suggest the recipient is gone.
func TestEraseReportsAStoreFailureAndNoCounts(t *testing.T) {
	t.Parallel()
	a, m, _ := adminFx(t, false, nil)
	boom := errors.New("deadlock detected")
	m.eraseErr = boom
	m.counts = map[string]int{"notifications": 3} // what a store must not leak when it failed

	counts, err := a.Erase(t.Context(), erasable)
	if !errors.Is(err, boom) || counts != nil {
		t.Fatalf("Erase = %v, %v, want the fault and no counts: an erasure that failed has erased nothing", counts, err)
	}
}
