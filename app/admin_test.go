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

	// IdemKeys: the keys it holds for any identity, ascending, and what it was asked. A store
	// that "ignores after" answers every ask from the start, as a defective one would.
	keys        []string
	keysErr     error
	ignoreAfter bool
	asks        []keyAsk
}

// keyAsk is one IdemKeys call.
type keyAsk struct {
	after string
	limit int
}

// errRunaway ends a fake's answers after too many asks, so a loop that never ends fails
// the test instead of hanging it.
var errRunaway = errors.New("the store was asked for keys without end")

func (m *recMaintenance) IdemKeys(_ context.Context, _ domain.Identity, after string, limit int) ([]string, error) {
	m.asks = append(m.asks, keyAsk{after: after, limit: limit})
	if m.keysErr != nil {
		return nil, m.keysErr
	}
	if len(m.asks) > 500 {
		return nil, errRunaway
	}
	var page []string
	for _, k := range m.keys {
		if len(page) == limit {
			break
		}
		if m.ignoreAfter || k > after {
			page = append(page, k)
		}
	}
	return page, nil
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

// erasureFx is an Admin that has a pre-check, over a store holding keys for the identity.
func erasureFx(t *testing.T, keys ...string) (*app.Admin, *recMaintenance, *recPreCheck, *events) {
	t.Helper()
	ev := &events{}
	m := &recMaintenance{ev: ev, keys: slices.Sorted(slices.Values(keys)), counts: map[string]int{"notify_idem": len(keys)}}
	pre := &recPreCheck{ev: ev}
	a, err := app.NewAdmin(notify.Config{Realm: planRealm, StoreDSN: "x", RedisURL: "y"},
		app.Deps{Maintenance: m, PreCheck: pre})
	if err != nil {
		t.Fatalf("NewAdmin: %v", err)
	}
	return a, m, pre, ev
}

// erasePageSize is how many keys Erase asks the store for at a time, read off the one ask an
// erasure of a recipient with no keys makes, so the tests need not know the constant.
func erasePageSize(t *testing.T) int {
	t.Helper()
	a, m, _, _ := erasureFx(t)
	if _, err := a.Erase(t.Context(), erasable); err != nil || len(m.asks) != 1 || m.asks[0].limit < 2 {
		t.Fatalf("probing the page size: %v, asks %+v, want one ask with a limit of at least 2", err, m.asks)
	}
	return m.asks[0].limit
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

// T040b, D51: the pre-check holds an entry for each key the recipient's notifications were
// accepted under, and answers "replay" to a producer that tries one again. Erasure removes the
// guard rows those entries mirror, so an entry left behind would answer for a notification
// that is gone. Erase therefore names each of the recipient's keys to the pre-check before it
// erases, and it forgets BEFORE: an entry forgotten for a row that survives only costs a
// miss, and one left for a row that is gone is the wrong answer. The entry's key is the one
// the Notifier wrote it under, built from the identity and the producer's key.
func TestEraseForgetsThePreCheckEntryOfEachOfTheRecipientsKeysBeforeErasing(t *testing.T) {
	t.Parallel()
	a, m, pre, ev := erasureFx(t, "weekly:W39", "invoice:42", "k1")

	if _, err := a.Erase(t.Context(), erasable); err != nil {
		t.Fatalf("Erase = %v, want nil", err)
	}
	want := []string{
		"forget:" + domain.PreCheckKey(erasable, "invoice:42"),
		"forget:" + domain.PreCheckKey(erasable, "k1"),
		"forget:" + domain.PreCheckKey(erasable, "weekly:W39"),
		"erase:u9",
	}
	if got := ev.all(); !slices.Equal(got, want) {
		t.Fatalf("the log is\n  %q\nwant each key forgotten, under the recipient's identity, and then the erasure:\n  %q", got, want)
	}
	if len(m.erased) != 1 || len(pre.forgotten) != 3 {
		t.Fatalf("erased %d times and forgot %d keys, want once and 3", len(m.erased), len(pre.forgotten))
	}
}

// A recipient can hold more keys than one answer carries, and every one of them is forgotten,
// whatever the page size: the first page's end is not the end. The page size is read off the
// first ask, and the cases sit on either side of its multiples.
func TestEraseForgetsEveryKeyWhateverThePageSize(t *testing.T) {
	t.Parallel()
	page := erasePageSize(t)

	for _, n := range []int{0, 1, page - 1, page, page + 1, 2 * page, 2*page + 1} {
		t.Run(fmt.Sprintf("%d keys", n), func(t *testing.T) {
			t.Parallel()
			keys := make([]string, n)
			for i := range keys {
				keys[i] = fmt.Sprintf("k%07d", i)
			}
			a, m, pre, ev := erasureFx(t, keys...)

			if _, err := a.Erase(t.Context(), erasable); err != nil {
				t.Fatalf("Erase = %v, want nil", err)
			}
			seen := map[string]int{}
			for _, k := range pre.forgotten {
				seen[k]++
			}
			for _, k := range keys {
				if c := seen[domain.PreCheckKey(erasable, k)]; c != 1 {
					t.Fatalf("key %s was forgotten %d times, want once", k, c)
				}
			}
			if len(pre.forgotten) != n {
				t.Fatalf("%d entries forgotten for %d keys", len(pre.forgotten), n)
			}
			if got := ev.all(); got[len(got)-1] != "erase:u9" || len(m.erased) != 1 {
				t.Fatalf("the erasure was not last and once: erased %d times, log ends %q", len(m.erased), got[len(got)-1])
			}
			// Each ask begins where the page before it ended, and none asks for more than a page.
			if want := n/page + 1; len(m.asks) != want {
				t.Fatalf("the store was asked %d times, want %d: a short page is the last, and a full one is followed by another ask", len(m.asks), want)
			}
			for i, ask := range m.asks {
				wantAfter := ""
				if i > 0 {
					wantAfter = keys[i*page-1]
				}
				if ask.after != wantAfter || ask.limit != page {
					t.Fatalf("ask %d = %+v, want after %q and limit %d", i, ask, wantAfter, page)
				}
			}
		})
	}
}

// With no pre-check there is nothing to forget, so the keys are not read: the erasure of a
// deployment that runs without Redis costs what it did.
func TestEraseWithoutAPreCheckDoesNotReadTheKeys(t *testing.T) {
	t.Parallel()
	a, m, ev := adminFx(t, false, nil)
	m.keys = []string{"k1", "k2"}

	if _, err := a.Erase(t.Context(), erasable); err != nil {
		t.Fatalf("Erase = %v, want nil", err)
	}
	if len(m.asks) != 0 || !slices.Equal(ev.all(), []string{"erase:u9"}) {
		t.Fatalf("the store was asked for keys %+v and the log is %q, want the erasure alone", m.asks, ev.all())
	}
}

// Failing to name the keys fails the erasure before anything is erased: an erasure that could
// not account for its own pre-check entries has not finished its work, and nothing has been
// forgotten that the caller would have to take on trust.
func TestEraseThatCannotReadTheKeysErasesNothing(t *testing.T) {
	t.Parallel()
	a, m, pre, ev := erasureFx(t, "k1")
	boom := errors.New("connection reset")
	m.keysErr = boom

	counts, err := a.Erase(t.Context(), erasable)
	if !errors.Is(err, boom) || counts != nil {
		t.Fatalf("Erase = %v, %v, want the fault and no counts", counts, err)
	}
	if len(m.erased) != 0 || len(pre.forgotten) != 0 || len(ev.all()) != 0 {
		t.Fatalf("erased %d times, forgot %d keys, log %q: want none of it", len(m.erased), len(pre.forgotten), ev.all())
	}
}

// A pre-check that refuses the call (a fault in its own store is not a refusal: see the
// port) stops the erasure before it begins, for the same reason, and no counts come back.
func TestEraseWhoseForgetIsRefusedErasesNothing(t *testing.T) {
	t.Parallel()
	a, m, pre, _ := erasureFx(t, "k1")
	boom := errors.New("pre-check misconfigured")
	pre.forgetErr = boom

	counts, err := a.Erase(t.Context(), erasable)
	if !errors.Is(err, boom) || counts != nil {
		t.Fatalf("Erase = %v, %v, want the refusal and no counts", counts, err)
	}
	if len(m.erased) != 0 {
		t.Fatalf("the store erased the recipient %d times after the pre-check refused", len(m.erased))
	}
}

// A store that answers every ask from the start would have the loop forget the same page for
// ever. The loop stops at the first answer that does not move past the last key it has.
func TestEraseStopsWhenTheStoreDoesNotAdvanceThroughItsKeys(t *testing.T) {
	t.Parallel()
	keys := make([]string, erasePageSize(t)+5)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%07d", i)
	}
	a, m, _, _ := erasureFx(t, keys...)
	m.ignoreAfter = true

	counts, err := a.Erase(t.Context(), erasable)
	if err == nil || errors.Is(err, errRunaway) || counts != nil {
		t.Fatalf("Erase = %v, %v, want an error that says the keys do not advance, and no counts", counts, err)
	}
	if len(m.asks) > 2 || len(m.erased) != 0 {
		t.Fatalf("the store was asked %d times and erased %d: want at most two asks and no erasure", len(m.asks), len(m.erased))
	}
}
