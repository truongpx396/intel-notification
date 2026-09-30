//go:build integration

package postgres

// Throughput benchmarks for the queue: what one PostgreSQL primary sustains for
// the accept path and the claim -> finish cycle. Run with `make bench`; how to
// read the numbers is in docs/testing.md.
//
// These answer "where is the ceiling, and what is it made of", not "did it
// regress": every run reports throughput, latency percentiles, WAL bytes and the
// share of HOT updates, so a change (batching, fillfactor, an index) is judged on
// the number that moved.

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/pgtest"
)

const (
	// benchShards is Config.Shards' default, so the spread matches a stock deployment.
	benchShards = 16
	// benchDeadline bounds a run, so a claim that never returns fails the benchmark
	// instead of hanging it.
	benchDeadline = 10 * time.Minute
)

// ---------------------------------------------------------------- benchmarks --

// BenchmarkQueueCycle drains a backlog of due deliveries the way a dispatcher
// does: claim a batch from a shard, finish each delivery, repeat. The provider
// call is free, so this is the ceiling PostgreSQL puts on delivery, not what a
// real provider will allow.
//
// Each configuration runs with and without the BindAddresses step that every
// address-bearing channel (email, SMS, push) performs between claim and finish;
// in-app has no address to bind. Binding is a non-HOT update however the lease is
// kept, because address_key is in a unique index, so with it at most half of a
// delivery's updates can be HOT.
//
// b.N is the backlog. Run it with -benchtime=<N>x; a duration would re-seed the
// table several times.
func BenchmarkQueueCycle(b *testing.B) {
	for _, workers := range []int{1, 4, 16} {
		for _, batch := range []int{10, 100} {
			for _, bind := range []bool{false, true} {
				b.Run(fmt.Sprintf("workers=%d/batch=%d/bind=%t", workers, batch, bind), func(b *testing.B) {
					benchQueueCycle(b, workers, batch, bind)
				})
			}
		}
	}
}

func benchQueueCycle(b *testing.B, workers, batch int, bind bool) {
	b.Helper()
	n := b.N
	db := pgtest.New(b)
	benchLogServer(b, db)
	benchSeed(b, db, n)
	store, pool := benchStore(b, db, workers)
	delivered, err := domain.NewFinish(domain.OutcomeDelivered, "bench", "")
	if err != nil {
		b.Fatal(err)
	}
	address := []domain.Address{{Channel: "email", Value: "bench@example.com"}}

	type worker struct {
		shard, idle         int
		claim, bind, finish []time.Duration
	}
	state := make([]worker, workers)
	for w := range state {
		state[w].shard = w % benchShards // spread the starting shards, as replicas would
	}
	var finished atomic.Int64

	wal := benchWAL(b, db)
	b.ResetTimer()
	elapsed := benchDrive(b, workers, func(ctx context.Context, w int) (bool, error) {
		if finished.Load() >= int64(n) {
			return true, nil
		}
		s := &state[w]
		t0 := time.Now()
		claims, err := store.Claim(ctx, domain.Shard(s.shard), batch, lease)
		if err != nil {
			return false, err
		}
		if len(claims) == 0 {
			// This shard is drained (or held by another worker): try the next, and
			// pause once every shard has come up empty, as the dispatcher backs off.
			s.shard = (s.shard + 1) % benchShards
			if s.idle++; s.idle >= benchShards {
				s.idle = 0
				time.Sleep(time.Millisecond)
			}
			return false, nil
		}
		s.idle = 0
		s.claim = append(s.claim, time.Since(t0)) // empty claims are not samples: they would flatter the percentiles
		for _, c := range claims {
			if bind {
				tb := time.Now()
				_, ok, err := store.BindAddresses(ctx, c, address, []string{"k"})
				if err != nil {
					return false, err
				}
				if !ok {
					return false, fmt.Errorf("bind %s: fenced out of a lease nobody else held", c.Entry.ID)
				}
				s.bind = append(s.bind, time.Since(tb))
			}
			t1 := time.Now()
			ok, err := store.Finish(ctx, c, delivered)
			if err != nil {
				return false, err
			}
			if !ok {
				return false, fmt.Errorf("finish %s: fenced out of a lease nobody else held", c.Entry.ID)
			}
			s.finish = append(s.finish, time.Since(t1))
		}
		finished.Add(int64(len(claims)))
		return false, nil
	})
	b.StopTimer()

	benchAssertCount(b, db, 0, `SELECT count(*) FROM notification_outbox`)
	benchAssertCount(b, db, n, `SELECT count(*) FROM notification_deliveries WHERE outcome = 'delivered'`)

	var claimLat, bindLat, finishLat []time.Duration
	for _, s := range state {
		claimLat, bindLat, finishLat = append(claimLat, s.claim...), append(bindLat, s.bind...), append(finishLat, s.finish...)
	}
	b.ReportMetric(float64(n)/elapsed.Seconds(), "deliveries/s")
	benchPercentiles(b, "claim", claimLat)
	benchPercentiles(b, "bind", bindLat)
	benchPercentiles(b, "finish", finishLat)
	b.ReportMetric(float64(benchWALSince(b, db, wal))/float64(n), "wal_B/delivery")
	// Every delivery is claimed exactly once (and bound once, if binding), and
	// finishing deletes rather than updates, so the outbox saw a known number of updates.
	updates := int64(n)
	if bind {
		updates *= 2
	}
	pool.Close() // a closing backend flushes its statistics
	if upd, hot, ok := benchOutboxUpdates(b, db, updates); ok {
		b.ReportMetric(100*float64(hot)/float64(upd), "hot_upd_%")
	}
}

// BenchmarkClaimLatencyWithBacklog measures one Claim against a table that holds
// a large backlog of due rows: the plan's "claim p95 < 20 ms with 1 M pending
// rows" row. b.N is the number of claims, each of `batch` rows, so the backlog
// must hold (b.N + shards) * batch rows.
//
//	BENCH_BACKLOG=1000000 go test -tags integration -run '^$' \
//	    -bench ClaimLatencyWithBacklog -benchtime=1000x ./adapters/driven/postgres
func BenchmarkClaimLatencyWithBacklog(b *testing.B) {
	backlog := benchBacklog(b)
	b.Run(fmt.Sprintf("backlog=%d/workers=4/batch=100", backlog), func(b *testing.B) {
		const workers, batch = 4, 100
		if need := (b.N + benchShards) * batch; need > backlog {
			b.Fatalf("%d claims of %d rows need a backlog of %d; raise BENCH_BACKLOG (now %d) or lower -benchtime",
				b.N, batch, need, backlog)
		}
		db := pgtest.New(b)
		benchLogServer(b, db)
		benchSeed(b, db, backlog)
		store, _ := benchStore(b, db, workers)

		b.ResetTimer()
		elapsed, latencies := benchForEach(b, workers, b.N, func(ctx context.Context, i int) error {
			claims, err := store.Claim(ctx, domain.Shard(i%benchShards), batch, lease)
			if err != nil {
				return err
			}
			if len(claims) != batch {
				return fmt.Errorf("claim %d returned %d rows, want %d: the backlog ran dry", i, len(claims), batch)
			}
			return nil
		})
		b.StopTimer()

		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "claims/s")
		benchPercentiles(b, "claim", latencies)
	})
}

// BenchmarkAcceptProxy measures the write footprint of accepting one notification:
// under the recipient's scope, one transaction that inserts the inbox row, the
// idempotency guard and two queue rows (email and in-app), as one statement with
// data-modifying CTEs — the repository's convention for multi-write transitions.
//
// It is a PROXY. Notifier.PersistAndEnqueue does not exist yet, and the real one
// also resolves preferences, peeks quota and may append to a digest, so treat the
// result as an upper bound on accept throughput, and replace this with the real
// call when it lands.
func BenchmarkAcceptProxy(b *testing.B) {
	for _, workers := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			n := b.N
			db := pgtest.New(b)
			benchLogServer(b, db)
			_, pool := benchStore(b, db, workers)
			wal := benchWAL(b, db)

			b.ResetTimer()
			elapsed, latencies := benchForEach(b, workers, n, func(ctx context.Context, i int) error {
				id := ident("w"+strconv.Itoa(i%50), "u"+strconv.Itoa(i))
				return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
					if err := setScope(ctx, tx, id); err != nil {
						return err
					}
					_, err := tx.Exec(ctx, acceptProxySQL, append(identityArgs(id),
						"k"+strconv.Itoa(i), fixtureDay, int16(domain.ShardFor(id, benchShards)),
						[]string{"email", "in_app"}, benchUUID())...)
					return err
				})
			})
			b.StopTimer()

			benchAssertCount(b, db, n, `SELECT count(*) FROM notifications`)
			benchAssertCount(b, db, n, `SELECT count(*) FROM notify_idem`)
			benchAssertCount(b, db, 2*n, `SELECT count(*) FROM notification_outbox`)
			b.ReportMetric(float64(n)/elapsed.Seconds(), "accepts/s")
			benchPercentiles(b, "accept", latencies)
			b.ReportMetric(float64(benchWALSince(b, db, wal))/float64(n), "wal_B/accept")
		})
	}
}

// $1-$5 identity, $6 idem key, $7 created_at, $8 shard, $9 channels, $10 notification
// id. The caller supplies the id so that a test can follow one notification through
// the queue (the soak test measures end-to-end latency this way).
const acceptProxySQL = `
WITH n AS (
    INSERT INTO notifications (id, realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
                               topic, title, idem_key, created_at)
    VALUES ($10::uuid, $1, $2, $3, $4, $5, 'ingestion_complete', 'bench', $6, $7)
    RETURNING id, created_at
), guard AS (
    INSERT INTO notify_idem (realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
                             idem_key, notification_id, notification_created_at)
    SELECT $1::text, $2::text, $3::text, $4::text, $5::text, $6::text, n.id, n.created_at FROM n
)
INSERT INTO notification_outbox (notification_id, notification_created_at, realm, tenant_kind,
                                 tenant_id, recipient_kind, recipient_id, topic, channel, shard)
SELECT n.id, n.created_at, $1::text, $2::text, $3::text, $4::text, $5::text,
       'ingestion_complete', ch, $8::smallint
  FROM n, unnest($9::text[]) AS ch`

// --------------------------------------------------------------- fixtures ----

// benchSeedSQL bulk-inserts $1 due queue rows spread evenly over $3 shards and
// 50 tenants, oldest first. It writes no inbox rows: Finish and Claim never read
// them, and the accept path has its own benchmark.
const benchSeedSQL = `
INSERT INTO notification_outbox (notification_id, notification_created_at, realm, tenant_kind,
                                 tenant_id, recipient_kind, recipient_id, topic, channel, shard,
                                 next_attempt_at)
SELECT gen_random_uuid(), $2, 'aisat', 'workspace', 'w' || (g % 50), 'user', 'u' || g,
       'ingestion_complete', 'email', (g % $3::bigint)::smallint,
       now() - interval '1 minute' - g * interval '1 microsecond'
  FROM generate_series(1, $1::bigint) AS g`

// benchSeed fills the queue with n due rows, then analyzes and checkpoints so
// every run starts from the same planner statistics and the same WAL position
// rather than paying for the seed's write-back inside the timed region.
func benchSeed(b *testing.B, db *pgtest.DB, n int) {
	b.Helper()
	ctx := b.Context()
	if _, err := db.Admin.Exec(ctx, benchSeedSQL, n, fixtureDay, benchShards); err != nil {
		b.Fatalf("seed %d queue rows: %v", n, err)
	}
	for _, stmt := range []string{`ANALYZE notification_outbox`, `CHECKPOINT`} {
		if _, err := db.Admin.Exec(ctx, stmt); err != nil {
			b.Fatalf("%s: %v", stmt, err)
		}
	}
}

// benchStore returns a Store whose pool holds one warm connection per worker.
// pgtest's own pools are capped at four, which would make a sixteen-worker run
// measure the pool rather than the database.
func benchStore(tb testing.TB, db *pgtest.DB, workers int) (*Store, *pgxpool.Pool) {
	tb.Helper()
	cfg := db.Owner.Config().Copy()
	cfg.MaxConns, cfg.MinConns = int32(workers), int32(workers)
	pool, err := pgxpool.NewWithConfig(tb.Context(), cfg)
	if err != nil {
		tb.Fatalf("worker pool: %v", err)
	}
	tb.Cleanup(pool.Close)
	return New(pool), pool
}

// benchUUID returns a random version 4 UUID.
func benchUUID() string {
	var u [16]byte
	if _, err := rand.Read(u[:]); err != nil {
		panic(err) // crypto/rand does not fail on supported platforms
	}
	u[6], u[8] = u[6]&0x0f|0x40, u[8]&0x3f|0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", u[0:4], u[4:6], u[6:8], u[8:10], u[10:])
}

func benchBacklog(b *testing.B) int {
	b.Helper()
	v := os.Getenv("BENCH_BACKLOG")
	if v == "" {
		return 200_000
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		b.Fatalf("BENCH_BACKLOG=%q: want a positive integer", v)
	}
	return n
}

// benchLogServer records what the numbers were measured on. fsync=off hides the
// cost of a commit, which is where batching and group commit earn their keep, so
// it is called out.
func benchLogServer(tb testing.TB, db *pgtest.DB) {
	tb.Helper()
	var version, fsync, syncCommit, maxWAL, checkpointEvery string
	err := db.Admin.QueryRow(tb.Context(), `
SELECT current_setting('server_version'), current_setting('fsync'), current_setting('synchronous_commit'),
       current_setting('max_wal_size'), current_setting('checkpoint_timeout')`).
		Scan(&version, &fsync, &syncCommit, &maxWAL, &checkpointEvery)
	if err != nil {
		tb.Fatalf("server settings: %v", err)
	}
	tb.Logf("postgres %s in a container, fsync=%s, synchronous_commit=%s, max_wal_size=%s, checkpoint_timeout=%s",
		version, fsync, syncCommit, maxWAL, checkpointEvery)
	if fsync == "off" {
		tb.Log("fsync=off: commits are free, so absolute numbers are inflated; compare runs, and set PGTEST_DURABLE=1 for fsync-on")
	}
}

// ------------------------------------------------------------------- driver ---

// benchDrive runs step on `workers` goroutines until every one retires by
// returning done, and stops them all at the first error or at benchDeadline. It
// returns the wall time and fails the benchmark on error, so it must be called
// from the benchmark's own goroutine.
func benchDrive(b *testing.B, workers int, step func(ctx context.Context, w int) (done bool, err error)) time.Duration {
	b.Helper()
	ctx, cancel := context.WithTimeout(b.Context(), benchDeadline)
	defer cancel()
	var (
		wg       sync.WaitGroup
		failOnce sync.Once
		failure  error
	)
	start := time.Now()
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				done, err := step(ctx, w)
				if err != nil {
					failOnce.Do(func() { failure = err; cancel() })
					return
				}
				if done {
					return
				}
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	if failure == nil {
		failure = ctx.Err() // the deadline, if it is what stopped the workers
	}
	if failure != nil {
		b.Fatalf("benchmark aborted after %v: %v", elapsed.Round(time.Millisecond), failure)
	}
	return elapsed
}

// benchForEach runs op once for each index in [0, n) across `workers`
// goroutines and returns the wall time and every call's latency.
func benchForEach(b *testing.B, workers, n int, op func(ctx context.Context, i int) error) (time.Duration, []time.Duration) {
	b.Helper()
	var next atomic.Int64
	perWorker := make([][]time.Duration, workers)
	elapsed := benchDrive(b, workers, func(ctx context.Context, w int) (bool, error) {
		i := int(next.Add(1)) - 1
		if i >= n {
			return true, nil
		}
		t0 := time.Now()
		if err := op(ctx, i); err != nil {
			return false, err
		}
		perWorker[w] = append(perWorker[w], time.Since(t0))
		return false, nil
	})
	return elapsed, slices.Concat(perWorker...)
}

// ------------------------------------------------------------------ metrics ---

// benchPercentiles reports p50, p95 and p99 of samples as <name>_p50_ms and so on.
func benchPercentiles(b *testing.B, name string, samples []time.Duration) {
	b.Helper()
	if len(samples) == 0 {
		return
	}
	sorted := slices.Clone(samples)
	slices.Sort(sorted)
	for _, p := range []struct {
		label string
		q     float64
	}{{"p50", 0.50}, {"p95", 0.95}, {"p99", 0.99}} {
		b.ReportMetric(float64(benchQuantile(sorted, p.q))/float64(time.Millisecond), name+"_"+p.label+"_ms")
	}
}

// benchQuantile is the nearest-rank quantile q of samples, which must be sorted.
func benchQuantile(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	at := max(int(q*float64(len(sorted))+0.5)-1, 0)
	return sorted[min(at, len(sorted)-1)]
}

// benchWAL returns the current WAL position. WAL is cluster-wide and this
// benchmark is the cluster's only client, so the difference across a run is the
// run's write volume, including the full-page images the first touch of each page
// after benchSeed's checkpoint costs.
func benchWAL(tb testing.TB, db *pgtest.DB) string {
	tb.Helper()
	var lsn string
	if err := db.Admin.QueryRow(tb.Context(), `SELECT pg_current_wal_lsn()::text`).Scan(&lsn); err != nil {
		tb.Fatalf("wal position: %v", err)
	}
	return lsn
}

func benchWALSince(tb testing.TB, db *pgtest.DB, from string) int64 {
	tb.Helper()
	var bytes int64
	err := db.Admin.QueryRow(tb.Context(),
		`SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), $1::pg_lsn)::bigint`, from).Scan(&bytes)
	if err != nil {
		tb.Fatalf("wal volume: %v", err)
	}
	return bytes
}

// benchOutboxUpdates reads the outbox's update counters once they add up to
// want. Statistics reach the shared counters when a backend flushes them, which
// an idle backend may delay, so it polls; ok is false if they never settle, and
// the caller then reports nothing rather than a wrong ratio.
func benchOutboxUpdates(b *testing.B, db *pgtest.DB, want int64) (updates, hot int64, ok bool) {
	b.Helper()
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); time.Sleep(250 * time.Millisecond) {
		err := db.Admin.QueryRow(b.Context(), `
SELECT coalesce(n_tup_upd, 0), coalesce(n_tup_hot_upd, 0)
  FROM pg_stat_user_tables WHERE relname = 'notification_outbox'`).Scan(&updates, &hot)
		if err != nil {
			b.Fatalf("outbox update statistics: %v", err)
		}
		if updates >= want {
			return updates, hot, true
		}
	}
	b.Logf("outbox update statistics never reached %d (saw %d); hot_upd_%% not reported", want, updates)
	return 0, 0, false
}

func benchAssertCount(tb testing.TB, db *pgtest.DB, want int, sql string) {
	tb.Helper()
	var got int
	if err := db.Admin.QueryRow(tb.Context(), sql).Scan(&got); err != nil {
		tb.Fatalf("%s: %v", sql, err)
	}
	if got != want {
		tb.Fatalf("%s = %d, want %d: the run did not do the work it reports", sql, got, want)
	}
}
