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

// Store implements the queue, digest and maintenance ports over one pool. The
// pool's role should own the tables and must NOT be a superuser or BYPASSRLS:
// the store relies on forced row-level security, and sets the recipient scope
// per transaction wherever it reads or writes recipient-scoped tables.
type Store struct {
	pool *pgxpool.Pool
}

var (
	_ ports.Queue       = (*Store)(nil)
	_ ports.Digests     = (*Store)(nil)
	_ ports.Maintenance = (*Store)(nil)
)

// New returns a Store over pool.
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

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

// identityArgs is the (realm, tenant, recipient) argument list most statements
// start with.
func identityArgs(id domain.Identity) []any {
	return []any{string(id.Realm), id.Tenant.Kind, id.Tenant.ID, id.Recipient.Kind, id.Recipient.ID}
}

// micros passes a duration to SQL as a bigint count of microseconds, written as
// `$n::bigint * interval '1 microsecond'`, which is exact and needs no codec.
func micros(d time.Duration) int64 { return d.Microseconds() }
