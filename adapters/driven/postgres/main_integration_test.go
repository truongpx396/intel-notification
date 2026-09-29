//go:build integration

package postgres

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/pgtest"
)

// TestMain starts one PostgreSQL for the package. The template every test
// clones gets partitions covering this month and the next through
// EnsurePartitions itself, so the suite does not start failing the day the
// bootstrap partitions run out.
func TestMain(m *testing.M) {
	os.Exit(pgtest.Main(m, func(ctx context.Context, owner *pgxpool.Pool) error {
		_, err := New(owner).EnsurePartitions(ctx, time.Now(), 2)
		return err
	}))
}

// fixtureDay is inside a bootstrap partition, so fixtures never depend on the
// clock.
var fixtureDay = time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)

const lease = 5 * time.Minute

func ident(tenant, recipient string) domain.Identity {
	return domain.Identity{
		Realm:     "aisat",
		Tenant:    domain.Tenant{Kind: "workspace", ID: tenant},
		Recipient: domain.Recipient{Kind: "user", ID: recipient},
	}
}

// env is one test's database, store and fixture helpers.
type env struct {
	t  *testing.T
	db *pgtest.DB
	s  *Store
}

func newEnv(t *testing.T) *env {
	t.Helper()
	db := pgtest.New(t)
	return &env{t: t, db: db, s: New(db.Owner)}
}

// notification persists an inbox row and its guard, as the superuser, and
// returns the notification id.
func (e *env) notification(id domain.Identity, idemKey string) string {
	e.t.Helper()
	var nid string
	err := pgx.BeginFunc(e.t.Context(), e.db.Admin, func(tx pgx.Tx) error {
		if err := tx.QueryRow(e.t.Context(), `
INSERT INTO notifications (id, realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
                           topic, title, idem_key, created_at)
VALUES (gen_random_uuid(), $1, $2, $3, $4, $5, 'ingestion_complete', 'fixture', $6, $7)
RETURNING id::text`, append(identityArgs(id), idemKey, fixtureDay)...).Scan(&nid); err != nil {
			return err
		}
		_, err := tx.Exec(e.t.Context(), `
INSERT INTO notify_idem (realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
                         idem_key, notification_id, notification_created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7::uuid, $8)`, append(identityArgs(id), idemKey, nid, fixtureDay)...)
		return err
	})
	if err != nil {
		e.t.Fatalf("fixture notification: %v", err)
	}
	return nid
}

// row describes a queue row fixture. Zero values mean: due now, no fallback.
type row struct {
	id           domain.Identity
	notification string
	shard        domain.Shard
	channel      string
	fallback     []string
	dueIn        time.Duration // negative ⇒ already overdue
}

// enqueue writes a queue row and returns its id.
func (e *env) enqueue(r row) string {
	e.t.Helper()
	if r.fallback == nil {
		r.fallback = []string{}
	}
	var oid string
	err := e.db.Admin.QueryRow(e.t.Context(), `
INSERT INTO notification_outbox (notification_id, notification_created_at, realm, tenant_kind,
                                 tenant_id, recipient_kind, recipient_id, topic, channel,
                                 shard, fallback, next_attempt_at)
VALUES ($6::uuid, $7, $1, $2, $3, $4, $5, 'ingestion_complete', $8, $9, $10,
        now() + $11::bigint * interval '1 microsecond')
RETURNING id::text`, append(identityArgs(r.id), r.notification, fixtureDay, r.channel,
		int16(r.shard), r.fallback, micros(r.dueIn))...).Scan(&oid)
	if err != nil {
		e.t.Fatalf("fixture queue row: %v", err)
	}
	return oid
}

// claimOne claims the single due row in shard, failing unless there is exactly one.
func (e *env) claimOne(shard domain.Shard) domain.Claim {
	e.t.Helper()
	claims, err := e.s.Claim(e.t.Context(), shard, 10, lease)
	if err != nil {
		e.t.Fatal(err)
	}
	if len(claims) != 1 {
		e.t.Fatalf("claimed %d rows from shard %d, want 1", len(claims), shard)
	}
	return claims[0]
}

// count runs a count query as the superuser.
func (e *env) count(sql string, args ...any) int {
	e.t.Helper()
	var n int
	if err := e.db.Admin.QueryRow(e.t.Context(), sql, args...).Scan(&n); err != nil {
		e.t.Fatalf("count %q: %v", sql, err)
	}
	return n
}

func (e *env) queued(outboxID string) bool {
	return e.count(`SELECT count(*) FROM notification_outbox WHERE id = $1::uuid`, outboxID) == 1
}

// lapseLeases ends every lease in shard, as a worker dying would.
func (e *env) lapseLeases(shard domain.Shard) {
	e.t.Helper()
	if _, err := e.db.Admin.Exec(e.t.Context(),
		`UPDATE notification_outbox SET next_attempt_at = now() - interval '1 second' WHERE shard = $1`,
		int16(shard)); err != nil {
		e.t.Fatal(err)
	}
}

func mustFinish(t *testing.T, o domain.TerminalOutcome, providerID, detail string) domain.Finish {
	t.Helper()
	f, err := domain.NewFinish(o, providerID, detail)
	if err != nil {
		t.Fatal(err)
	}
	return f
}
