// Package postgres is the PostgreSQL implementation of the driven ports.
//
// Every state transition is plain SQL in this package, executed as one
// statement or inside one transaction, and tested against a real PostgreSQL by
// the integration suite (`make test-integration`). Where a transition must do
// several writes atomically, it is a single statement with data-modifying CTEs,
// so atomicity does not depend on transaction handling in Go.
//
// The store executes decisions; it does not make them. Which terminal outcomes
// are dead letters and which continue a fallback chain is domain policy
// (domain.DispositionOf), passed in on every Finish (D37).
package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// Store implements the queue, digest, maintenance and persist-and-read ports over
// one pool. The pool's role should own the tables and must NOT be a superuser or
// BYPASSRLS: the store relies on forced row-level security, and sets the
// recipient scope per transaction wherever it reads or writes recipient-scoped
// tables.
type Store struct {
	pool *pgxpool.Pool
	settings
}

var (
	_ ports.Queue       = (*Store)(nil)
	_ ports.Digests     = (*Store)(nil)
	_ ports.Maintenance = (*Store)(nil)
	_ ports.Store       = (*Store)(nil)
)

// settings are what every store in this package is built with.
type settings struct {
	clock  ports.Clock
	ids    ports.IDSource
	notify bool
}

// Option configures a store of this package.
type Option func(*settings)

// WithClock sets the clock the persist and read paths stamp rows with and judge
// visibility by: a notification's created_at, the instant a scheduled one
// becomes visible. The default is the system clock. The queue's own due times
// stay the database's now(), because a claim is decided inside one statement.
func WithClock(c ports.Clock) Option {
	return func(s *settings) { s.clock = c }
}

// WithIDSource sets where the persist path takes a notification's id from. The
// default is a UUIDv7 stamped from the store's clock, so ids sort by creation time.
func WithIDSource(ids ports.IDSource) Option {
	return func(s *settings) { s.ids = ids }
}

// WithNotify makes the persist path issue a NOTIFY on dispatchChannel inside the same
// transaction as the queue rows it writes, so a [Listener] in another process hears of
// the work at its commit and not before (D22). It is off by default, and
// Config.ListenNotify is how an operator turns it on: PostgreSQL serializes the commit
// of every transaction that issued a NOTIFY behind a database-wide lock, which becomes
// the bottleneck at the write rates this engine targets.
func WithNotify() Option {
	return func(s *settings) { s.notify = true }
}

func newSettings(opts []Option) settings {
	s := settings{clock: systemClock{}}
	for _, opt := range opts {
		opt(&s)
	}
	if s.ids == nil {
		s.ids = uuidSource{clock: s.clock}
	}
	return s
}

// New returns a Store over pool.
func New(pool *pgxpool.Pool, opts ...Option) *Store {
	return &Store{pool: pool, settings: newSettings(opts)}
}

// systemClock is the default ports.Clock.
type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

// querier is what both a pool and a transaction offer, so a helper can run in
// either.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// setScopeSQL sets the recipient scope for the CURRENT TRANSACTION only
// (set_config's third argument). A session-level SET would outlive the
// transaction on a pooled connection and hand the next request this
// recipient's rows (D17).
const setScopeSQL = `
SELECT set_config('notify.realm',          $1, true),
       set_config('notify.tenant_kind',    $2, true),
       set_config('notify.tenant_id',      $3, true),
       set_config('notify.recipient_kind', $4, true),
       set_config('notify.recipient_id',   $5, true)`

func setScope(ctx context.Context, tx pgx.Tx, id domain.Identity) error {
	if err := id.Validate(); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, setScopeSQL, string(id.Realm), id.Tenant.Kind, id.Tenant.ID,
		id.Recipient.Kind, id.Recipient.ID)
	return err
}

// readScoped runs fn in one read-only transaction, under id's scope, at
// REPEATABLE READ: every statement in it sees the same snapshot, so a delivery
// that finishes meanwhile is read as pending or as finished, never as both.
func readScoped(ctx context.Context, pool *pgxpool.Pool, id domain.Identity, fn func(pgx.Tx) error) error {
	return pgx.BeginTxFunc(ctx, pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly},
		func(tx pgx.Tx) error {
			if err := setScope(ctx, tx, id); err != nil {
				return err
			}
			return fn(tx)
		})
}

// identityPredicate matches one recipient's rows, taking the identity as $1-$5, the
// order identityArgs gives it. Statements on a recipient-scoped table state it as
// well as relying on the policy: the policy is the guarantee, and the predicate lets
// the planner use the recipient index.
const identityPredicate = `realm = $1 AND tenant_kind = $2 AND tenant_id = $3 AND recipient_kind = $4 AND recipient_id = $5`

// identityArgs is the (realm, tenant, recipient) argument list most statements
// start with.
func identityArgs(id domain.Identity) []any {
	return []any{string(id.Realm), id.Tenant.Kind, id.Tenant.ID, id.Recipient.Kind, id.Recipient.ID}
}

// micros passes a duration to SQL as a bigint count of microseconds, written as
// `$n::bigint * interval '1 microsecond'`, which is exact and needs no codec.
func micros(d time.Duration) int64 { return d.Microseconds() }
