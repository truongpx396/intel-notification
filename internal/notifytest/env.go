//go:build integration

package notifytest

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	notify "github.com/truongpx396/intel-notification"
	"github.com/truongpx396/intel-notification/adapters/driven/postgres"
	"github.com/truongpx396/intel-notification/adapters/driven/redis"
	"github.com/truongpx396/intel-notification/app"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/pgtest"
	"github.com/truongpx396/intel-notification/internal/redistest"
	"github.com/truongpx396/intel-notification/ports"
)

// Epoch is where every Env's fake clock starts. It is fixed, so a test's times do
// not depend on the day it runs, and Main provisions the partitions around it.
var Epoch = time.Date(2031, 3, 1, 12, 0, 0, 0, time.UTC)

// Realm is the realm Env's fixtures use.
const Realm domain.Realm = "aisat"

// Option changes what Main starts and what NewEnv builds.
type Option func(*options)

type options struct{ redis bool }

func newOptions(opts []Option) options {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// WithRedis puts Redis in the world. Passed to Main it starts the shared Redis
// beside the shared PostgreSQL; passed to NewEnv it builds the Env's Notifier on
// the Redis pre-check, over a Redis database of the test's own, instead of the
// in-memory one. A suite that runs a contract on both passes it to Main once and
// to NewEnv for the Redis run.
func WithRedis() Option { return func(o *options) { o.redis = true } }

// Main starts the shared PostgreSQL and runs the tests. Every suite that uses Env
// calls it from TestMain:
//
//	func TestMain(m *testing.M) { os.Exit(notifytest.Main(m)) }
//
// A suite that also runs on Redis calls notifytest.Main(m, notifytest.WithRedis()).
//
// The template every test's database is cloned from has partitions for the
// current month and the next, for rows the database dates itself, and for the
// months around Epoch, for rows a test dates from its fake clock.
func Main(m *testing.M, opts ...Option) int {
	run := func() int {
		return pgtest.Main(m, func(ctx context.Context, owner *pgxpool.Pool) error {
			return prepare(ctx, owner, time.Now())
		})
	}
	if newOptions(opts).redis {
		return redistest.Run(run)
	}
	return run()
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
// digest, maintenance and store ports over it, a fake clock, a fault injector, a
// probe, a channel registry, and the in-memory read-side fakes. Queue and Store
// are never faked (see the package comment).
//
// The store dates what it persists from the fake clock, so a test controls which
// partition a row lands in. The queue's due times stay the database's now(): a
// claim is decided inside one statement, which the fake clock cannot reach.
//
// The Notifier is the real one, over the Env's own pieces: the real Store, the
// fakes, the fake clock and a pre-check, in memory or, under WithRedis, in Redis.
// The Dispatcher arrives with the task that builds it, as one more field.
type Env struct {
	DB *pgtest.DB

	// Config is the configuration the Notifier was built with: the Env's realm and
	// the library defaults. The two connection strings are placeholders, because
	// the Env hands the engine its adapters ready-made and nothing dials them.
	Config notify.Config

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
	// Inbox is the real recipient read and write path, on the Env's clock.
	Inbox ports.InboxStore
	// Store is the real one behind a FaultStore, so a test can fail the next
	// commit (FailNextCommit) and a Notify still goes through the port's own seam.
	Store *FaultStore

	// PreCheck is the pre-check the Notifier is built with: the in-memory one, or
	// under WithRedis the Redis adapter over Redis. Memory is the in-memory one as
	// its own type, for a test that wants the instant each entry was written; it is
	// nil under WithRedis.
	PreCheck ports.PreCheck
	Memory   *PreCheck
	// Redis is the test's own Redis database; nil unless the Env was built WithRedis.
	Redis *redistest.DB

	// Notifier is the use-case under test.
	Notifier ports.Notifier
}

// NewEnv returns an Env over a database cloned for tb alone, which is dropped when
// tb ends. The code under test is handed DB.Owner, never DB.Admin.
func NewEnv(tb testing.TB, opts ...Option) *Env {
	tb.Helper()
	o := newOptions(opts)
	db := pgtest.New(tb)
	clock := NewClock(Epoch)
	store := postgres.New(db.Owner, postgres.WithClock(clock))
	e := &Env{
		DB:          db,
		Config:      notify.Config{Realm: Realm, StoreDSN: "notifytest", RedisURL: "notifytest"},
		Clock:       clock,
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
		Inbox:       postgres.NewInbox(db.Owner, postgres.WithClock(clock)),
		Store: NewFaultStore(store, func(ctx context.Context) (Transaction, error) {
			tx, err := db.Owner.Begin(ctx)
			if err != nil {
				return nil, err
			}
			return tx, nil
		}),
		Memory: NewPreCheck(clock),
	}
	e.PreCheck = e.Memory
	if o.redis {
		e.Redis, e.Memory = redistest.New(tb), nil
		e.PreCheck = redis.NewPreCheck(e.Redis.Client, redis.WithMaxTTL(e.Config.WithDefaults().IdempotencyWindow))
	}
	notifier, err := app.NewNotifier(e.Config, app.Deps{
		Channels:      e.Channels,
		Topics:        e.Topics,
		Prefs:         e.Prefs,
		Quotas:        e.Quotas,
		QuotaPolicies: e.Quotas,
		Store:         e.Store,
		Queue:         e.Queue,
		PreCheck:      e.PreCheck,
		Clock:         clock,
	})
	if err != nil {
		tb.Fatalf("notifytest: build the Notifier: %v", err)
	}
	e.Notifier = notifier
	return e
}

// AddChannel registers a fake channel of the given kind, reporting to the Env's
// Probe, and returns it. It is the one line NS-008 counts: a channel is
// registered, and nothing else changes.
func (e *Env) AddChannel(kind domain.ChannelKind, caps domain.ChannelCapabilities) *Channel {
	ch := NewChannel(kind, caps, e.Probe)
	e.Channels.Register(ch)
	return ch
}
