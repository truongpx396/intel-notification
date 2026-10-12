//go:build integration

// Package redistest runs integration tests against a real Redis started by
// Testcontainers.
//
// One container serves a whole test binary. Each TEST gets a logical database of
// its own on it (SELECT n), so every test can call t.Parallel() and no test can see
// another's keys, as pgtest does with a cloned PostgreSQL database. The server is
// started with enough databases for that, and with persistence off: nothing here is
// meant to outlive its test.
package redistest

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"

	goredis "github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

// Image is the Redis the pre-check, stream and quota counters are verified against.
const Image = "redis:7-alpine"

// databases is how many logical databases the server offers: one per test, with
// room to spare. The default is 16, which a parallel suite exhausts.
const databases = 4096

type server struct {
	container *tcredis.RedisContainer
	opts      *goredis.Options
	seq       atomic.Int64
}

var shared *server

// Main starts the container, runs the tests and tears it down. Call it from
// TestMain when the package needs Redis and nothing else:
//
//	func TestMain(m *testing.M) { os.Exit(redistest.Main(m)) }
//
// A package that needs PostgreSQL too starts both through notifytest.Main.
func Main(m *testing.M) int { return Run(m.Run) }

// Run starts the container, calls run, and tears the container down.
func Run(run func() int) int {
	ctx := context.Background()
	s, err := start(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "redistest: %v\n", err)
		if s != nil {
			s.stop()
		}
		return 1
	}
	shared = s
	code := run()
	s.stop()
	return code
}

func start(ctx context.Context) (*server, error) {
	ctr, err := tcredis.Run(ctx, Image,
		testcontainers.WithCmd("redis-server", "--databases", fmt.Sprint(databases), "--save", "", "--appendonly", "no"))
	s := &server{container: ctr}
	if err != nil {
		return s, fmt.Errorf("start %s: %w", Image, err)
	}
	uri, err := ctr.ConnectionString(ctx)
	if err != nil {
		return s, err
	}
	if s.opts, err = goredis.ParseURL(uri); err != nil {
		return s, err
	}
	return s, nil
}

func (s *server) stop() {
	if s.container != nil {
		_ = testcontainers.TerminateContainer(s.container)
	}
}

// DB is one test's private Redis database.
type DB struct {
	// Client is connected to the test's database. Hand it to the code under test.
	Client *goredis.Client
	// Index is the logical database number.
	Index int
}

// New returns a client on a database no other test uses, emptied, and closes it
// when tb ends.
func New(tb testing.TB) *DB {
	tb.Helper()
	if shared == nil {
		tb.Fatal("redistest: call redistest.Main, or notifytest.Main(m, notifytest.WithRedis()), from TestMain")
	}
	idx := int(shared.seq.Add(1)) % databases
	opts := *shared.opts
	opts.DB = idx
	client := goredis.NewClient(&opts)
	if err := client.FlushDB(tb.Context()).Err(); err != nil {
		_ = client.Close()
		tb.Fatalf("redistest: flush database %d: %v", idx, err)
	}
	tb.Cleanup(func() {
		_ = client.FlushDB(context.Background()).Err()
		_ = client.Close()
	})
	return &DB{Client: client, Index: idx}
}
