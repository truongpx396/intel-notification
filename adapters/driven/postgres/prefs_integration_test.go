//go:build integration

package postgres_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/truongpx396/intel-notification/adapters/driven/postgres"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/notifytest"
	"github.com/truongpx396/intel-notification/internal/pgtest"
	"github.com/truongpx396/intel-notification/ports"
)

// T018. This is an external test package: notifytest imports the adapter for its Env,
// so an internal one would be an import cycle. The package's TestMain, in
// main_integration_test.go, starts the database for both.

// The PostgreSQL store runs the table the in-memory fake runs (T013a, T018): the
// resolution matrix of D30 and NR-011 read from real rows, the channel set, the
// essential topics, the scoping, the upserts and the schedule's round trip.
func TestPreferencesRunTheStoreContract(t *testing.T) {
	t.Parallel()
	notifytest.PreferenceStoreContract(t, func(t *testing.T) ports.PreferenceStore {
		t.Helper()
		return postgres.NewPreferences(pgtest.New(t).Owner)
	})
}

var alice = domain.Identity{
	Realm:     "aisat",
	Tenant:    domain.Tenant{Kind: "workspace", ID: "w1"},
	Recipient: domain.Recipient{Kind: "user", ID: "alice"},
}

// NS-001, D17. A choice is stored under the recipient's scope: the owner sees it
// through that scope and only that scope, whatever the store's own predicates say.
func TestPreferencesAreStoredUnderTheRecipientsScope(t *testing.T) {
	t.Parallel()
	db := pgtest.New(t)
	p := postgres.NewPreferences(db.Owner)
	other := alice
	other.Recipient.ID = "bob"

	if err := p.Set(t.Context(), alice, []domain.Preference{{Topic: "invite", Channel: "email", Enabled: false}}); err != nil {
		t.Fatal(err)
	}
	if err := p.SetSchedule(t.Context(), alice, domain.DeliverySchedule{Digest: time.Minute}); err != nil {
		t.Fatal(err)
	}
	seen := func(id *domain.Identity, table string) int {
		t.Helper()
		var n int
		err := pgx.BeginFunc(t.Context(), db.Owner, func(tx pgx.Tx) error {
			if id != nil {
				if _, err := tx.Exec(t.Context(), `
SELECT set_config('notify.realm', $1, true), set_config('notify.tenant_kind', $2, true),
       set_config('notify.tenant_id', $3, true), set_config('notify.recipient_kind', $4, true),
       set_config('notify.recipient_id', $5, true)`,
					string(id.Realm), id.Tenant.Kind, id.Tenant.ID, id.Recipient.Kind, id.Recipient.ID); err != nil {
					return err
				}
			}
			return tx.QueryRow(t.Context(), "SELECT count(*) FROM "+table).Scan(&n)
		})
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	for _, table := range []string{"notification_preferences", "notification_schedules"} {
		for name, c := range map[string]struct {
			id   *domain.Identity
			want int
		}{"her own scope": {&alice, 1}, "another recipient's scope": {&other, 0}, "no scope": {nil, 0}} {
			if got := seen(c.id, table); got != c.want {
				t.Errorf("%s as the owner under %s: %d rows, want %d", table, name, got, c.want)
			}
		}
	}
}

// Many writers on one pair are one row, not an error: the upsert is the guard.
func TestPreferencesConcurrentSetsOfOnePairAreOneRow(t *testing.T) {
	t.Parallel()
	db := pgtest.New(t)
	p := postgres.NewPreferences(db.Owner)
	const writers = 16
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for i := range writers {
		wg.Go(func() {
			errs <- p.Set(t.Context(), alice, []domain.Preference{{Topic: "invite", Channel: "email", Enabled: i%2 == 0}})
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("a concurrent Set failed: %v", err)
		}
	}
	var n int
	if err := db.Admin.QueryRow(t.Context(), `SELECT count(*) FROM notification_preferences`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("%d rows after %d writers on one pair (%v), want 1", n, writers, err)
	}
}

// A call may name many pairs; they are one write, and a repeated pair in it takes
// the later value instead of failing the whole call.
func TestPreferencesSetManyPairsAtOnce(t *testing.T) {
	t.Parallel()
	db := pgtest.New(t)
	p := postgres.NewPreferences(db.Owner)
	var prefs []domain.Preference
	for i := range 300 {
		prefs = append(prefs, domain.Preference{Topic: domain.Topic(fmt.Sprintf("t%d", i%100)), Channel: "email", Enabled: i%3 != 0})
	}
	if err := p.Set(t.Context(), alice, prefs); err != nil {
		t.Fatal(err)
	}
	var rows, enabled int
	if err := db.Admin.QueryRow(t.Context(),
		`SELECT count(*), count(*) FILTER (WHERE enabled) FROM notification_preferences`).Scan(&rows, &enabled); err != nil {
		t.Fatal(err)
	}
	// 100 topics, each named three times; the last of each triple (i = 200..299) decides.
	want := 0
	for i := 200; i < 300; i++ {
		if i%3 != 0 {
			want++
		}
	}
	if rows != 100 || enabled != want {
		t.Fatalf("%d rows, %d enabled; want 100 rows with %d enabled (the last mention of each topic)", rows, enabled, want)
	}
}

// The schedule keeps what it was given, down to a time with seconds, and a quiet
// window that crosses midnight is two clock times, not a range the store judges.
func TestScheduleKeepsTheClockTimesItWasGiven(t *testing.T) {
	t.Parallel()
	p := postgres.NewPreferences(pgtest.New(t).Owner)
	for _, s := range []domain.DeliverySchedule{
		{QuietStart: "22:00", QuietEnd: "07:00", Timezone: "America/Sao_Paulo"},
		{QuietStart: "00:00", QuietEnd: "23:59", Timezone: "UTC"},
		{QuietStart: "08:30:15", QuietEnd: "09:45:30", Timezone: "Asia/Kolkata", Digest: time.Hour},
		{QuietStart: "22:00"}, // a start with no end is stored as given
		{Timezone: "Europe/Paris"},
	} {
		if err := p.SetSchedule(t.Context(), alice, s); err != nil {
			t.Fatalf("SetSchedule(%+v): %v", s, err)
		}
		if got, err := p.Schedule(t.Context(), alice); err != nil || got != s {
			t.Fatalf("Schedule = %+v, %v, want the round trip %+v", got, err, s)
		}
	}
	// A time that is not a time is the database's to refuse, and nothing is stored.
	if err := p.SetSchedule(t.Context(), alice, domain.DeliverySchedule{QuietStart: "25:99"}); err == nil {
		t.Fatal("a quiet start of 25:99 was accepted")
	}
	if got, _ := p.Schedule(t.Context(), alice); got.QuietStart != "" || got.Timezone != "Europe/Paris" {
		t.Fatalf("a refused SetSchedule changed the schedule to %+v", got)
	}
}
