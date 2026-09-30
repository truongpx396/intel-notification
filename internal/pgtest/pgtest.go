//go:build integration

// Package pgtest runs integration tests against a real PostgreSQL started by
// Testcontainers.
//
// One container serves a whole test binary. Each TEST gets its own database,
// cloned from a template that already holds the migrations, the non-superuser
// owner role and any prepared state, so every test can call t.Parallel() and no
// test can see another's rows. Cloning a template takes tens of milliseconds;
// re-running migrations per test would take seconds.
//
// Tests run as the table owner — neither superuser nor BYPASSRLS — because that
// is the role the engine runs as, and the only one that proves forced row-level
// security holds. A superuser pool on the same database is available for
// fixtures that must bypass it.
package pgtest

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/truongpx396/intel-notification/migrations"
)

const (
	// Image is the PostgreSQL the schema is verified against.
	Image = "postgres:16-alpine"

	ownerRole     = "notify_owner"
	ownerPassword = "owner"
	templateDB    = "notify_template"
)

// Prepare runs against the template, as the owner, before it is frozen — for
// state every test needs, such as partitions covering the current month.
type Prepare func(ctx context.Context, owner *pgxpool.Pool) error

type cluster struct {
	container *tcpostgres.PostgresContainer
	super     *pgxpool.Config // superuser, maintenance database
	admin     *pgxpool.Pool
	mu        sync.Mutex // serializes CREATE DATABASE … TEMPLATE
	seq       atomic.Int64
}

var shared *cluster

// Main starts the container, builds the template, runs the tests and tears
// everything down. Call it from TestMain:
//
//	func TestMain(m *testing.M) { os.Exit(pgtest.Main(m, prepare)) }
func Main(m *testing.M, prepare Prepare) int {
	ctx := context.Background()
	c, err := start(ctx, prepare)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pgtest: %v\n", err)
		if c != nil {
			c.stop()
		}
		return 1
	}
	shared = c
	code := m.Run()
	c.stop()
	return code
}

func start(ctx context.Context, prepare Prepare) (*cluster, error) {
	// Parallel tests each hold their own pools.
	args := []string{"postgres", "-c", "max_connections=500"}
	// fsync=off keeps the suite fast, and makes a commit free. That is wrong for
	// a throughput benchmark, where the commit is much of the cost, so `make
	// bench` sets PGTEST_DURABLE=1 to leave fsync on.
	if os.Getenv("PGTEST_DURABLE") == "" {
		args = append(args, "-c", "fsync=off")
	}
	// PGTEST_SETTINGS passes extra server settings, space separated, for a run that
	// tunes the server: PGTEST_SETTINGS="max_wal_size=16GB checkpoint_timeout=15min".
	for _, setting := range strings.Fields(os.Getenv("PGTEST_SETTINGS")) {
		args = append(args, "-c", setting)
	}
	ctr, err := tcpostgres.Run(ctx, Image,
		tcpostgres.WithDatabase("postgres"),
		tcpostgres.WithUsername("postgres"),
		tcpostgres.WithPassword("postgres"),
		testcontainers.WithCmd(args...),
		tcpostgres.BasicWaitStrategies(),
	)
	c := &cluster{container: ctr}
	if err != nil {
		return c, fmt.Errorf("start %s: %w", Image, err)
	}
	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return c, err
	}
	if c.super, err = pgxpool.ParseConfig(dsn); err != nil {
		return c, err
	}
	c.super.MaxConns = 4
	if c.admin, err = pgxpool.NewWithConfig(ctx, c.super); err != nil {
		return c, err
	}
	for _, stmt := range []string{
		fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD '%s' NOSUPERUSER NOBYPASSRLS", ownerRole, ownerPassword),
		"CREATE DATABASE " + templateDB,
	} {
		if _, err := c.admin.Exec(ctx, stmt); err != nil {
			return c, fmt.Errorf("%s: %w", stmt, err)
		}
	}
	if err := c.buildTemplate(ctx); err != nil {
		return c, fmt.Errorf("build template: %w", err)
	}
	if prepare != nil {
		owner, err := c.pool(ctx, templateDB, true)
		if err != nil {
			return c, err
		}
		err = prepare(ctx, owner)
		owner.Close()
		if err != nil {
			return c, fmt.Errorf("prepare template: %w", err)
		}
	}
	return c, nil
}

// buildTemplate applies every migration as the superuser, then hands every
// table, partition and function to the owner role.
func (c *cluster) buildTemplate(ctx context.Context) error {
	cfg := c.super.ConnConfig.Copy()
	cfg.Database = templateDB
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()

	// PostgreSQL 15+ no longer lets PUBLIC create in the public schema; the owner
	// needs CREATE to provision partitions.
	if _, err := conn.Exec(ctx, "GRANT USAGE, CREATE ON SCHEMA public TO "+ownerRole); err != nil {
		return err
	}
	names, err := fs.Glob(migrations.FS, "*.sql")
	if err != nil {
		return err
	}
	slices.Sort(names)
	for _, name := range names {
		sql, err := fs.ReadFile(migrations.FS, name)
		if err != nil {
			return err
		}
		// The simple protocol runs a whole file, with its BEGIN/COMMIT, as-is.
		if _, err := conn.PgConn().Exec(ctx, string(sql)).ReadAll(); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}
	_, err = conn.Exec(ctx, `
DO $$
DECLARE r record;
BEGIN
    FOR r IN SELECT c.oid::regclass AS t FROM pg_class c
              WHERE c.relnamespace = 'public'::regnamespace AND c.relkind IN ('r', 'p')
    LOOP
        EXECUTE format('ALTER TABLE %s OWNER TO `+ownerRole+`', r.t);
    END LOOP;
    FOR r IN SELECT p.oid::regprocedure AS f FROM pg_proc p
              WHERE p.pronamespace = 'public'::regnamespace
    LOOP
        EXECUTE format('ALTER FUNCTION %s OWNER TO `+ownerRole+`', r.f);
    END LOOP;
END $$`)
	return err
}

func (c *cluster) pool(ctx context.Context, database string, asOwner bool) (*pgxpool.Pool, error) {
	cfg := c.super.Copy()
	cfg.ConnConfig.Database = database
	cfg.MaxConns = 4
	if asOwner {
		cfg.ConnConfig.User, cfg.ConnConfig.Password = ownerRole, ownerPassword
	}
	return pgxpool.NewWithConfig(ctx, cfg)
}

// clone creates database name from the template. The template must have no
// connections at that instant; a backend that is still exiting after a pool
// closed shows up as object_in_use, which is retried.
func (c *cluster) clone(ctx context.Context, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	stmt := fmt.Sprintf("CREATE DATABASE %s TEMPLATE %s", pgx.Identifier{name}.Sanitize(), templateDB)
	for attempt := 0; ; attempt++ {
		_, err := c.admin.Exec(ctx, stmt)
		var pgErr *pgconn.PgError
		if err == nil || attempt == 50 || !errors.As(err, &pgErr) || pgErr.Code != "55006" {
			return err
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (c *cluster) stop() {
	if c.admin != nil {
		c.admin.Close()
	}
	if c.container != nil {
		_ = testcontainers.TerminateContainer(c.container)
	}
}

// DB is one test's private database.
type DB struct {
	Name string
	// Owner is the non-superuser table owner: the role the engine runs as.
	Owner *pgxpool.Pool
	// Admin is a superuser on the same database, for fixtures that must bypass
	// row-level security. Never hand it to the code under test.
	Admin *pgxpool.Pool
}

// New clones the template into a database for tb alone and drops it when tb ends.
func New(tb testing.TB) *DB {
	tb.Helper()
	if shared == nil {
		tb.Fatal("pgtest: call pgtest.Main from TestMain")
	}
	ctx := tb.Context()
	name := fmt.Sprintf("t_%d", shared.seq.Add(1))
	if err := shared.clone(ctx, name); err != nil {
		tb.Fatalf("pgtest: clone template: %v", err)
	}
	owner, err := shared.pool(ctx, name, true)
	if err != nil {
		tb.Fatalf("pgtest: owner pool: %v", err)
	}
	admin, err := shared.pool(ctx, name, false)
	if err != nil {
		owner.Close()
		tb.Fatalf("pgtest: admin pool: %v", err)
	}
	tb.Cleanup(func() {
		owner.Close()
		admin.Close()
		_, _ = shared.admin.Exec(context.Background(),
			"DROP DATABASE IF EXISTS "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)")
	})
	return &DB{Name: name, Owner: owner, Admin: admin}
}
