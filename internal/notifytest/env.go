//go:build integration

package notifytest

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/truongpx396/intel-notification/adapters/driven/postgres"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/pgtest"
	"github.com/truongpx396/intel-notification/ports"
)

// Epoch is where every Env's fake clock starts. It is fixed, so a test's times do
// not depend on the day it runs, and Main provisions the partitions around it.
var Epoch = time.Date(2031, 3, 1, 12, 0, 0, 0, time.UTC)

// Realm is the realm Env's fixtures use.
const Realm domain.Realm = "aisat"

// Main starts the shared PostgreSQL and runs the tests. Every suite that uses Env
// calls it from TestMain:
//
//	func TestMain(m *testing.M) { os.Exit(notifytest.Main(m)) }
//
// The template every test's database is cloned from has partitions for the
// current month and the next, for rows the database dates itself, and for the
// months around Epoch, for rows a test dates from its fake clock.
func Main(m *testing.M) int {
	return pgtest.Main(m, func(ctx context.Context, owner *pgxpool.Pool) error {
		return prepare(ctx, owner, time.Now())
	})
}

// prepare provisions, as the owner, the month containing each of now and Epoch and
// the month after it. The migrations create a fixed few months, which stop
// covering the real now one day; this keeps the suite from failing that day.
func prepare(ctx context.Context, owner *pgxpool.Pool, now time.Time) error {
	store := postgres.New(owner)
	for _, around := range []time.Time{now, Epoch} {
		if _, err := store.EnsurePartitions(ctx, around, 2); err != nil {
			return err
		}
	}
	return nil
}

// Env is one test's world: a private PostgreSQL database with the real queue,
// digest and maintenance ports over it, a fake clock, a fault injector, a probe,
// a channel registry, and the in-memory read-side fakes. Queue and Store are
// never faked (see the package comment).
//
// Redis arrives with T023, and the Notifier, Dispatcher and Store with the tasks
// that build them; each lands here as one more field.
type Env struct {
	DB *pgtest.DB

	Clock    *Clock
	Faults   *Injector
	Probe    *Probe
	Channels *Registry

	Topics    *Topics
	Prefs     *Preferences
	Quotas    *QuotaCounter
	Addresses *AddressBook
	Templates *Templates

	Queue       ports.Queue
	Digests     ports.Digests
	Maintenance ports.Maintenance
}

// NewEnv returns an Env over a database cloned for tb alone, which is dropped when
// tb ends. The code under test is handed DB.Owner, never DB.Admin.
func NewEnv(tb testing.TB) *Env {
	tb.Helper()
	db := pgtest.New(tb)
	store := postgres.New(db.Owner)
	return &Env{
		DB:          db,
		Clock:       NewClock(Epoch),
		Faults:      NewInjector(),
		Probe:       NewProbe(),
		Channels:    NewRegistry(),
		Topics:      NewTopics(),
		Prefs:       NewPreferences(),
		Quotas:      NewQuotaCounter(),
		Addresses:   NewAddressBook(),
		Templates:   NewTemplates("en"),
		Queue:       store,
		Digests:     store,
		Maintenance: store,
	}
}

// AddChannel registers a fake channel of the given kind, reporting to the Env's
// Probe, and returns it. It is the one line NS-008 counts: a channel is
// registered, and nothing else changes.
func (e *Env) AddChannel(kind domain.ChannelKind, caps domain.ChannelCapabilities) *Channel {
	ch := NewChannel(kind, caps, e.Probe)
	e.Channels.Register(ch)
	return ch
}
