//go:build integration

package postgres

// Soak test: the queue under sustained, steady load, watched over time.
//
// The benchmarks drain a fixed backlog in seconds, which cannot show the failure a
// PostgreSQL-backed queue is known for: dead tuples and index bloat building up
// faster than autovacuum clears them, so latency and table size drift while
// throughput still looks fine. This test runs producers and dispatcher-style
// workers together at a fixed rate, samples the queue table every few seconds, and
// can hold one long-running transaction open part-way through. That is the usual way
// a queue table bloats in production: vacuum cannot reclaim a row version that any
// open snapshot might still see.
//
// It FAILS on lost or duplicated work and on a backlog that does not drain. Drift is
// reported and flagged, not failed: how much is too much is a judgment for the
// person reading the table.
//
// It is skipped unless SOAK_DURATION is set. `make soak` sets it and turns fsync on.
//
//	SOAK_DURATION  how long producers run, e.g. 10m (required)
//	SOAK_RATE      notifications accepted per second; each becomes two deliveries (1000)
//	SOAK_WORKERS   dispatcher-style workers (8)
//	SOAK_BATCH     deliveries per claim (100)
//	SOAK_POLL      idle poll interval, Config.PollInterval's default (500ms)
//	SOAK_SAMPLE    sampling interval (10s)
//	SOAK_HOLD      how long the long transaction stays open; 0 disables it (a fifth of
//	               SOAK_DURATION). It opens a third of the way in.

import (
	"context"
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

// soakProducers is the number of concurrent accept loops. Each runs at rate/soakProducers.
const soakProducers = 8

// soakE2EBound is NS-004's bound on in-app delivery, accept to visible, at p95. The soak's
// end-to-end time covers both channels and no provider or relay, so it is a proxy, but a
// queue that cannot meet it on its own cannot meet it with them.
const soakE2EBound = 5 * time.Second

type soakConfig struct {
	duration time.Duration
	rate     int
	workers  int
	batch    int
	poll     time.Duration
	sample   time.Duration
	holdAt   time.Duration // from the start
	hold     time.Duration // 0 disables the long transaction
}

func soakConfigFromEnv(t *testing.T) soakConfig {
	t.Helper()
	if os.Getenv("SOAK_DURATION") == "" {
		t.Skip("set SOAK_DURATION (for example 10m) or run `make soak`")
	}
	cfg := soakConfig{
		duration: soakDurationEnv(t, "SOAK_DURATION", 0),
		rate:     soakIntEnv(t, "SOAK_RATE", 1000),
		workers:  soakIntEnv(t, "SOAK_WORKERS", 8),
		batch:    soakIntEnv(t, "SOAK_BATCH", 100),
		poll:     soakDurationEnv(t, "SOAK_POLL", 500*time.Millisecond),
		sample:   soakDurationEnv(t, "SOAK_SAMPLE", 10*time.Second),
	}
	cfg.hold = soakDurationEnv(t, "SOAK_HOLD", cfg.duration/5)
	cfg.holdAt = cfg.duration / 3
	if cfg.rate < soakProducers {
		t.Fatalf("SOAK_RATE=%d: at least %d, one per producer", cfg.rate, soakProducers)
	}
	if cfg.hold > 0 && cfg.holdAt+cfg.hold > cfg.duration {
		t.Fatalf("SOAK_HOLD=%v opens at %v and would outlast SOAK_DURATION=%v", cfg.hold, cfg.holdAt, cfg.duration)
	}
	return cfg
}

func soakDurationEnv(t *testing.T, name string, def time.Duration) time.Duration {
	t.Helper()
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < 0 || (d == 0 && name != "SOAK_HOLD") {
		t.Fatalf("%s=%q: want a duration such as 10m", name, v)
	}
	return d
}

func soakIntEnv(t *testing.T, name string, def int) int {
	t.Helper()
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		t.Fatalf("%s=%q: want a positive integer", name, v)
	}
	return n
}

func TestSoak(t *testing.T) {
	t.Parallel()
	cfg := soakConfigFromEnv(t)
	db := pgtest.New(t)
	benchLogServer(t, db)
	store, _ := benchStore(t, db, cfg.workers)
	_, acceptPool := benchStore(t, db, soakProducers)
	s := &soakRun{cfg: cfg, db: db, store: store, acceptPool: acceptPool}

	hold := "none"
	if cfg.hold > 0 {
		hold = fmt.Sprintf("open %v for %v", cfg.holdAt, cfg.hold)
	}
	t.Logf("soak: %v at %d notifications/s (%d deliveries/s), %d workers, batch %d, poll %v, long transaction: %s",
		cfg.duration, cfg.rate, 2*cfg.rate, cfg.workers, cfg.batch, cfg.poll, hold)

	runCtx, cancelRun := context.WithCancel(t.Context())
	defer cancelRun()
	var (
		failOnce sync.Once
		failure  error
	)
	fail := func(err error) { failOnce.Do(func() { failure = err; cancelRun() }) }

	wal0 := benchWAL(t, db)
	base, err := s.dbStats(runCtx)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	deadline := start.Add(cfg.duration)

	sampleCtx, stopSampling := context.WithCancel(runCtx)
	defer stopSampling()
	consumeCtx, stopConsuming := context.WithCancel(runCtx)
	defer stopConsuming()
	var sampling, holders, consuming, producing sync.WaitGroup

	var rows []soakRow
	sampling.Add(1)
	go func() {
		defer sampling.Done()
		var err error
		if rows, err = s.sampleLoop(sampleCtx, t.Logf, start, base); err != nil {
			fail(err)
		}
	}()
	holders.Add(1)
	go func() {
		defer holders.Done()
		if err := s.holdSnapshot(runCtx); err != nil {
			fail(err)
		}
	}()
	for w := range cfg.workers {
		consuming.Add(1)
		go func() {
			defer consuming.Done()
			if err := s.consume(consumeCtx, w); err != nil {
				fail(err)
			}
		}()
	}
	for p := range soakProducers {
		producing.Add(1)
		go func() {
			defer producing.Done()
			if err := s.produce(runCtx, p, deadline); err != nil {
				fail(err)
			}
		}()
	}

	producing.Wait()
	produced := s.accepted.Load()
	t.Logf("producers stopped at %v with %d notifications accepted; draining the backlog",
		time.Since(start).Round(time.Second), produced)
	drainFor := max(2*time.Minute, cfg.duration/2)
	drainBy := time.Now().Add(drainFor)
	for s.finished.Load() < 2*s.accepted.Load() && runCtx.Err() == nil && time.Now().Before(drainBy) {
		soakSleep(runCtx, 200*time.Millisecond)
	}
	drained := s.finished.Load() == 2*s.accepted.Load()
	stopConsuming()
	consuming.Wait()
	holders.Wait()
	stopSampling()
	sampling.Wait()

	if failure != nil {
		t.Fatalf("soak aborted: %v", failure)
	}
	elapsed := time.Since(start)
	walBytes := benchWALSince(t, db, wal0)
	soakSummary(t, cfg, rows, s.accepted.Load(), walBytes, elapsed)

	if !drained {
		t.Fatalf("the backlog did not drain: %d of %d deliveries finished within %v of the producers stopping",
			s.finished.Load(), 2*s.accepted.Load(), drainFor)
	}
	acc := int(s.accepted.Load())
	benchAssertCount(t, db, 0, `SELECT count(*) FROM notification_outbox`)
	benchAssertCount(t, db, acc, `SELECT count(*) FROM notifications`)
	benchAssertCount(t, db, acc, `SELECT count(*) FROM notify_idem`)
	benchAssertCount(t, db, 2*acc, `SELECT count(*) FROM notification_deliveries WHERE outcome = 'delivered'`)
	benchAssertCount(t, db, 2*acc, `SELECT count(DISTINCT (notification_id, channel)) FROM notification_deliveries`)
}

// soakRun is one soak: the shared counters and the window the sampler drains.
type soakRun struct {
	cfg        soakConfig
	db         *pgtest.DB
	store      *Store
	acceptPool *pgxpool.Pool

	seq      atomic.Int64
	accepted atomic.Int64 // notifications committed
	finished atomic.Int64 // deliveries finished
	holding  atomic.Bool  // the long transaction is open
	sentAt   sync.Map     // "<notification id>/<channel>" -> time.Time the accept began

	mu       sync.Mutex
	claimLat []time.Duration // this window
	e2eLat   []time.Duration // this window: accept began -> delivery finished
}

// produce accepts notifications at rate/soakProducers until deadline. It is open
// loop: if the database cannot keep up it falls behind and the achieved rate drops,
// which is the finding, rather than slowing the schedule to match. Accepts use the
// run's own context, not one that ends at the deadline, so that a transaction is
// never cancelled mid-commit and left uncounted.
func (s *soakRun) produce(ctx context.Context, p int, deadline time.Time) error {
	interval := time.Second * soakProducers / time.Duration(s.cfg.rate)
	next := time.Now().Add(interval * time.Duration(p) / soakProducers) // stagger the producers
	for ctx.Err() == nil && next.Before(deadline) {
		if !soakSleep(ctx, time.Until(next)) {
			return nil
		}
		next = next.Add(interval)

		i := s.seq.Add(1)
		id := ident("w"+strconv.FormatInt(i%50, 10), "u"+strconv.FormatInt(i, 10))
		nid := benchUUID()
		began := time.Now()
		s.sentAt.Store(nid+"/email", began)
		s.sentAt.Store(nid+"/in_app", began)
		err := pgx.BeginFunc(ctx, s.acceptPool, func(tx pgx.Tx) error {
			if err := setScope(ctx, tx, id); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, acceptProxySQL, append(identityArgs(id),
				"k"+strconv.FormatInt(i, 10), fixtureDay, int16(domain.ShardFor(id, benchShards)),
				[]string{"email", "in_app"}, nid)...)
			return err
		})
		if err != nil {
			return fmt.Errorf("accept %d: %w", i, err)
		}
		s.accepted.Add(1)
	}
	return nil
}

// consume is a dispatcher-style worker: claim a batch, bind an address for email (in-app
// has none), finish each delivery, and poll again at once after a batch, backing off
// to the poll interval once every shard has come up empty.
func (s *soakRun) consume(ctx context.Context, w int) error {
	delivered, err := domain.NewFinish(domain.OutcomeDelivered, "soak", "")
	if err != nil {
		return err
	}
	address := []domain.Address{{Channel: "email", Value: "soak@example.com"}}
	shard, idle := w%benchShards, 0
	for ctx.Err() == nil {
		t0 := time.Now()
		claims, err := s.store.Claim(ctx, domain.Shard(shard), s.cfg.batch, lease)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("claim: %w", err)
		}
		if len(claims) == 0 {
			shard = (shard + 1) % benchShards
			if idle++; idle >= benchShards {
				idle = 0
				soakSleep(ctx, s.cfg.poll)
			}
			continue
		}
		idle = 0
		claimTook := time.Since(t0)
		e2e := make([]time.Duration, 0, len(claims))
		for _, c := range claims {
			if c.Entry.Channel == "email" {
				_, ok, err := s.store.BindAddresses(ctx, c, address, []string{"k"})
				if err != nil {
					return fmt.Errorf("bind %s: %w", c.Entry.ID, err)
				}
				if !ok {
					return fmt.Errorf("bind %s: fenced out of a lease nobody else held", c.Entry.ID)
				}
			}
			ok, err := s.store.Finish(ctx, c, delivered)
			if err != nil {
				return fmt.Errorf("finish %s: %w", c.Entry.ID, err)
			}
			if !ok {
				return fmt.Errorf("finish %s: fenced out of a lease nobody else held", c.Entry.ID)
			}
			if at, found := s.sentAt.LoadAndDelete(c.Entry.NotificationID + "/" + string(c.Entry.Channel)); found {
				began, _ := at.(time.Time)
				e2e = append(e2e, time.Since(began))
			}
		}
		s.finished.Add(int64(len(claims)))
		s.mu.Lock()
		s.claimLat = append(s.claimLat, claimTook)
		s.e2eLat = append(s.e2eLat, e2e...)
		s.mu.Unlock()
	}
	return nil
}

// holdSnapshot opens a REPEATABLE READ transaction and keeps it open. Its snapshot
// pins the oldest row version any query might still need, so vacuum cannot reclaim
// anything newer while it lasts: the classic cause of queue-table bloat.
func (s *soakRun) holdSnapshot(ctx context.Context) error {
	if s.cfg.hold <= 0 || !soakSleep(ctx, s.cfg.holdAt) {
		return nil
	}
	tx, err := s.db.Admin.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return fmt.Errorf("open the long transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }() // roll back even if ctx ended
	if _, err := tx.Exec(ctx, `SELECT txid_current_snapshot()`); err != nil {
		return fmt.Errorf("take the long transaction's snapshot: %w", err)
	}
	s.holding.Store(true)
	soakSleep(ctx, s.cfg.hold)
	s.holding.Store(false)
	return nil
}

// soakStats is the queue table as the database reports it.
type soakStats struct {
	heap, index int64 // bytes
	dead        int64
	updates     int64 // cumulative
	hot         int64 // cumulative
	vacuums     int64 // cumulative autovacuum runs
	wal         string
}

func (s *soakRun) dbStats(ctx context.Context) (soakStats, error) {
	var st soakStats
	err := s.db.Admin.QueryRow(ctx, `
SELECT pg_relation_size('notification_outbox'), pg_indexes_size('notification_outbox'),
       coalesce(max(n_dead_tup), 0), coalesce(max(n_tup_upd), 0), coalesce(max(n_tup_hot_upd), 0),
       coalesce(max(autovacuum_count), 0), pg_current_wal_lsn()::text
  FROM pg_stat_user_tables WHERE relname = 'notification_outbox'`).
		Scan(&st.heap, &st.index, &st.dead, &st.updates, &st.hot, &st.vacuums, &st.wal)
	if err != nil {
		return st, fmt.Errorf("queue table statistics: %w", err)
	}
	return st, nil
}

// soakRow is one sample: the window since the last, and the table as it stands.
type soakRow struct {
	at                  time.Duration
	acceptRate, delRate float64 // per second, over the window
	backlog             int64
	claimP95, e2eP95    time.Duration
	stats               soakStats
	hotPct              float64 // over the window; -1 if nothing was updated
	walKBps             float64
	holding, draining   bool
}

const soakHeader = "  t(s)  acc/s  del/s  backlog  e2e_p95ms  claim_p95ms  heap_MB  idx_MB     dead  HOT%  WAL_KB/s  vac  note"

func (r soakRow) String() string {
	hot, note := "-", ""
	if r.hotPct >= 0 {
		hot = fmt.Sprintf("%.0f", r.hotPct)
	}
	switch {
	case r.holding:
		note = "long transaction open"
	case r.draining:
		note = "producers stopped"
	}
	return fmt.Sprintf("%6.0f %6.0f %6.0f %8d %10.0f %12.1f %8.1f %7.1f %8d %5s %9.0f %4d  %s",
		r.at.Seconds(), r.acceptRate, r.delRate, r.backlog, ms(r.e2eP95), ms(r.claimP95),
		float64(r.stats.heap)/1e6, float64(r.stats.index)/1e6, r.stats.dead, hot, r.walKBps,
		r.stats.vacuums, note)
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

// sampleLoop samples every cfg.sample until ctx ends, logging each row as it is taken.
func (s *soakRun) sampleLoop(ctx context.Context, logf func(string, ...any), start time.Time, prev soakStats) ([]soakRow, error) {
	logf("%s", soakHeader)
	var (
		rows                []soakRow
		prevAt              = start
		prevAcc, prevFin    int64
		ticker              = time.NewTicker(s.cfg.sample)
		productionEndsAfter = s.cfg.duration
	)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return rows, nil
		case now := <-ticker.C:
			st, err := s.dbStats(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return rows, nil
				}
				return rows, err
			}
			s.mu.Lock()
			claim, e2e := s.claimLat, s.e2eLat
			s.claimLat, s.e2eLat = nil, nil
			s.mu.Unlock()
			slices.Sort(claim)
			slices.Sort(e2e)

			acc, fin := s.accepted.Load(), s.finished.Load()
			window := now.Sub(prevAt).Seconds()
			row := soakRow{
				at:         now.Sub(start),
				acceptRate: float64(acc-prevAcc) / window,
				delRate:    float64(fin-prevFin) / window,
				backlog:    2*acc - fin,
				claimP95:   benchQuantile(claim, 0.95),
				e2eP95:     benchQuantile(e2e, 0.95),
				stats:      st,
				hotPct:     -1,
				holding:    s.holding.Load(),
				draining:   now.Sub(start) > productionEndsAfter,
			}
			if upd := st.updates - prev.updates; upd > 0 {
				row.hotPct = 100 * float64(st.hot-prev.hot) / float64(upd)
			}
			if err := s.db.Admin.QueryRow(ctx,
				`SELECT pg_wal_lsn_diff($1::pg_lsn, $2::pg_lsn)::float8 / 1024.0 / $3::float8`,
				st.wal, prev.wal, window).Scan(&row.walKBps); err != nil && ctx.Err() == nil {
				return rows, fmt.Errorf("wal rate: %w", err)
			}
			rows = append(rows, row)
			logf("%s", row)
			prev, prevAt, prevAcc, prevFin = st, now, acc, fin
		}
	}
}

// soakSummary reports the run and raises a flag for each kind of drift. The
// comparison is the first third of the samples against the last third, so with the
// default long transaction (a third of the way in, a fifth of the run long) the last
// third is the recovery.
func soakSummary(t *testing.T, cfg soakConfig, rows []soakRow, accepted int64, walBytes int64, elapsed time.Duration) {
	t.Helper()
	var steady []soakRow
	for _, r := range rows {
		if !r.draining {
			steady = append(steady, r)
		}
	}
	achieved := float64(accepted) / cfg.duration.Seconds()
	t.Logf("soak summary")
	t.Logf("  accepted        %d notifications in %v: %.0f/s achieved of %d/s asked, %d deliveries",
		accepted, elapsed.Round(time.Second), achieved, cfg.rate, 2*accepted)
	if accepted > 0 {
		t.Logf("  WAL             %.1f KB per notification (both deliveries included)", float64(walBytes)/float64(accepted)/1024)
	}
	if len(rows) == 0 {
		t.Logf("  no samples: the run was shorter than SOAK_SAMPLE")
		return
	}
	last := rows[len(rows)-1]
	var peakHeap, peakIndex, peakDead, peakDeadHeld int64
	for _, r := range rows {
		peakHeap, peakIndex = max(peakHeap, r.stats.heap), max(peakIndex, r.stats.index)
		peakDead = max(peakDead, r.stats.dead)
		if r.holding {
			peakDeadHeld = max(peakDeadHeld, r.stats.dead)
		}
	}
	t.Logf("  outbox heap     first %.1f MB, peak %.1f MB, end %.1f MB", float64(rows[0].stats.heap)/1e6,
		float64(peakHeap)/1e6, float64(last.stats.heap)/1e6)
	t.Logf("  outbox indexes  first %.1f MB, peak %.1f MB, end %.1f MB", float64(rows[0].stats.index)/1e6,
		float64(peakIndex)/1e6, float64(last.stats.index)/1e6)
	t.Logf("  dead tuples     peak %d (%d while the long transaction was open)", peakDead, peakDeadHeld)
	if last.stats.updates > 0 {
		t.Logf("  HOT updates     %.0f%% overall, %d autovacuum runs", 100*float64(last.stats.hot)/float64(last.stats.updates),
			last.stats.vacuums)
	}

	// The worst windows, not only the trend: a stall in the middle of a run can leave the
	// last third healthier than the first, and a trend alone would call that an improvement.
	var worstBacklog, worstLatency soakRow
	var worstClaim time.Duration
	slow, starved := 0, 0
	for _, r := range steady {
		if r.backlog > worstBacklog.backlog {
			worstBacklog = r
		}
		if r.e2eP95 > worstLatency.e2eP95 {
			worstLatency = r
		}
		worstClaim = max(worstClaim, r.claimP95)
		if r.e2eP95 > soakE2EBound {
			slow++
		}
		if r.acceptRate < 0.8*float64(cfg.rate) {
			starved++
		}
	}
	t.Logf("  worst windows   end-to-end p95 %.1f s at t=%.0fs, backlog %d at t=%.0fs, claim p95 %.1f ms",
		worstLatency.e2eP95.Seconds(), worstLatency.at.Seconds(), worstBacklog.backlog, worstBacklog.at.Seconds(), ms(worstClaim))

	flags := 0
	warn := func(format string, args ...any) {
		flags++
		t.Logf("  WARN            %s", fmt.Sprintf(format, args...))
	}
	if slow > 0 {
		warn("end-to-end p95 was above %v (NS-004's bound) in %d of %d samples, peaking at %.1f s at t=%.0fs",
			soakE2EBound, slow, len(steady), worstLatency.e2eP95.Seconds(), worstLatency.at.Seconds())
	}
	if starved > 0 {
		warn("accepts fell below 80%% of the %d/s asked in %d of %d samples: the database stalled the producers "+
			"(the run still averages the full rate, because producers catch up afterwards)", cfg.rate, starved, len(steady))
	}
	if achieved < 0.95*float64(cfg.rate) {
		warn("the database sustained %.0f accepts/s, below the %d/s asked: the load, not the queue, was the limit", achieved, cfg.rate)
	}
	if len(steady) < 6 {
		t.Logf("  drift           not assessed: fewer than 6 samples while producing (raise SOAK_DURATION or lower SOAK_SAMPLE)")
	} else {
		third := len(steady) / 3
		first, lastThird := steady[:third], steady[len(steady)-third:]
		for _, m := range []struct {
			name  string
			of    func(soakRow) time.Duration
			floor time.Duration // a doubling below this is noise: the poll interval alone is 500 ms
		}{
			{"claim p95", func(r soakRow) time.Duration { return r.claimP95 }, 10 * time.Millisecond},
			{"e2e p95", func(r soakRow) time.Duration { return r.e2eP95 }, time.Second},
		} {
			a, b := soakMedian(first, m.of), soakMedian(lastThird, m.of)
			t.Logf("  %-15s first third %.1f ms, last third %.1f ms", m.name, ms(a), ms(b))
			if a > 0 && b > 2*a && b > m.floor {
				warn("%s grew %.1fx between the first and last third of the run", m.name, float64(b)/float64(a))
			}
		}
		// Growth is compared as a rate, not against the heap's size: a table that keeps
		// growing at a steady rate looks like a shrinking percentage as it gets bigger.
		firstRate, lastRate := soakHeapMBps(first), soakHeapMBps(lastThird)
		grown := float64(lastThird[len(lastThird)-1].stats.heap-lastThird[0].stats.heap) / 1e6
		t.Logf("  %-15s growing %.2f MB/s in the first third, %.2f MB/s in the last (%.1f MB)", "outbox heap",
			firstRate, lastRate, grown)
		if grown > 5 && lastRate > 0.5*firstRate {
			warn("the outbox heap was still growing at %.2f MB/s in the last third (%.2f MB/s at the start): "+
				"dead tuples are being made faster than vacuum reclaims them", lastRate, firstRate)
		}
	}
	if flags == 0 {
		t.Logf("  no drift flags raised")
	}
}

// soakHeapMBps is how fast the outbox heap grew across rows, in MB per second.
func soakHeapMBps(rows []soakRow) float64 {
	if len(rows) < 2 {
		return 0
	}
	first, last := rows[0], rows[len(rows)-1]
	return float64(last.stats.heap-first.stats.heap) / 1e6 / (last.at - first.at).Seconds()
}

func soakMedian(rows []soakRow, of func(soakRow) time.Duration) time.Duration {
	v := make([]time.Duration, 0, len(rows))
	for _, r := range rows {
		if d := of(r); d > 0 {
			v = append(v, d)
		}
	}
	slices.Sort(v)
	return benchQuantile(v, 0.5)
}

// soakSleep waits for d and reports whether it did, or false if ctx ended first.
func soakSleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
