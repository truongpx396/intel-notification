package domain

import (
	"fmt"
	"time"
)

// DatabaseConditions is what Maintenance.DatabaseConditions reads from the database
// the queue lives in: the settings and the activity the queue's health depends on
// (NR-037, D38). It is a reading, not a verdict. What counts as a problem is
// [AssessDatabase]'s, so the rule is one place and a test can drive it with no database.
type DatabaseConditions struct {
	// At is the database's own clock when the reading was taken. Ages and rates are
	// measured against it, never against the host's, so a skewed host clock cannot
	// make a transaction look old.
	At time.Time

	// MaxWALSize is max_wal_size in bytes.
	MaxWALSize int64
	// IdleInTransactionTimeout and StatementTimeout are what the connected role runs
	// with, after any role or database override. Zero means the timeout is off.
	IdleInTransactionTimeout time.Duration
	StatementTimeout         time.Duration

	// OldestTransactionAge is how long the oldest transaction in this database that
	// pins vacuum has been open: one that holds a snapshot or has been given a
	// transaction id. Zero when there is none, or when none can be seen.
	OldestTransactionAge time.Duration
	// SeesAllSessions is false when the connected role cannot read other roles'
	// sessions (neither a superuser nor a member of pg_read_all_stats), in which case
	// OldestTransactionAge covers its own sessions only.
	SeesAllSessions bool

	// WALBytes is the WAL the server has written since WALStatsReset, as pg_stat_wal
	// counts it. A zero WALStatsReset means the server does not say.
	WALBytes      int64
	WALStatsReset time.Time

	// CheckpointsTimed and CheckpointsRequested count the checkpoints the server has
	// taken on its schedule and because the WAL filled, since its statistics were reset.
	CheckpointsTimed     int64
	CheckpointsRequested int64
}

// The codes of the warnings [AssessDatabase] raises.
const (
	WarnNoIdleInTransactionTimeout = "idle_in_transaction_session_timeout_off"
	WarnNoStatementTimeout         = "statement_timeout_off"
	WarnOldTransaction             = "old_transaction"
	WarnBlindToOtherSessions       = "other_sessions_not_visible"
	WarnMaxWALSizeUndersized       = "max_wal_size_undersized"
)

// PreflightWarning is one condition the queue depends on that is not as it should be.
// It is a warning and never a failure: the engine runs on a database that has every
// one of them, and the operator learns of it before its effect (NR-037).
type PreflightWarning struct {
	Code    string // one of the Warn constants, for a dashboard to key on
	Message string // what is wrong and what to do, for a person to read
}

// PreflightLimits are the thresholds [AssessDatabase] judges by.
type PreflightLimits struct {
	// OldestTransaction is how old an open transaction may be before it is reported:
	// one older than this is pinning vacuum, so the queue will bloat. The soak that
	// measured the damage held one for two minutes; 60 seconds is the alarm
	// docs/operations.md names, to be tuned to the write rate.
	OldestTransaction time.Duration
	// WALHeadroom is how much of the observed peak WAL rate max_wal_size must hold.
	// PostgreSQL forces a checkpoint at roughly a third of max_wal_size, and a forced
	// checkpoint stalls the queue (D38), so max_wal_size is sized to fifteen minutes.
	WALHeadroom time.Duration
}

// DefaultPreflightLimits are the thresholds docs/operations.md states.
func DefaultPreflightLimits() PreflightLimits {
	return PreflightLimits{OldestTransaction: 60 * time.Second, WALHeadroom: 15 * time.Minute}
}

// AssessDatabase judges a reading against limits and returns the warnings it earns,
// in a fixed order: the role's timeouts, the transactions, then the WAL. walRate is the
// peak WAL rate observed, in bytes per second ([WALRate]); zero means none is known,
// and max_wal_size is then not judged, because there is no rate to size it against.
// A healthy reading returns none.
func AssessDatabase(c DatabaseConditions, walRate float64, limits PreflightLimits) []PreflightWarning {
	var ws []PreflightWarning
	warn := func(code, format string, args ...any) {
		ws = append(ws, PreflightWarning{Code: code, Message: fmt.Sprintf(format, args...)})
	}
	if c.IdleInTransactionTimeout == 0 {
		warn(WarnNoIdleInTransactionTimeout, "the connected role has no idle_in_transaction_session_timeout, "+
			"so a transaction left open by a host session pins vacuum until someone ends it; set one on the role (D38)")
	}
	if c.StatementTimeout == 0 {
		warn(WarnNoStatementTimeout, "the connected role has no statement_timeout, "+
			"so one runaway statement holds its snapshot, and vacuum back, for as long as it runs; set one on the role (D38)")
	}
	if c.OldestTransactionAge > limits.OldestTransaction {
		warn(WarnOldTransaction, "a transaction has been open for %s, longer than %s, and pins vacuum: "+
			"the queue's tables will bloat and latency will follow; find it in pg_stat_activity and end it (D38)",
			c.OldestTransactionAge.Round(time.Second), limits.OldestTransaction)
	}
	if !c.SeesAllSessions {
		warn(WarnBlindToOtherSessions, "the connected role cannot see other roles' sessions, "+
			"so the age of the oldest transaction covers its own only; grant it pg_read_all_stats to see the rest")
	}
	// need is zero when no rate is known, and no size is under zero.
	if need := walRate * limits.WALHeadroom.Seconds(); float64(c.MaxWALSize) < need {
		warn(WarnMaxWALSizeUndersized, "max_wal_size is %s, under the %s that %s of the peak WAL rate observed "+
			"(%s/s) takes; PostgreSQL forces a checkpoint at about a third of it, and forced checkpoints stall the queue: "+
			"raise max_wal_size (D38)",
			sizeString(float64(c.MaxWALSize)), sizeString(need), limits.WALHeadroom, sizeString(walRate))
	}
	return ws
}

// sizeString writes a byte count in the binary unit an operator sets max_wal_size in.
func sizeString(b float64) string {
	const kib = 1024.0
	for _, u := range []string{"B", "KiB", "MiB", "GiB"} {
		if b < kib {
			return fmt.Sprintf("%.1f %s", b, u)
		}
		b /= kib
	}
	return fmt.Sprintf("%.1f TiB", b)
}

// WALSample is the WAL counter at an instant.
type WALSample struct {
	Bytes int64
	At    time.Time
}

// WALRate turns successive readings of the WAL counter into the peak rate the server
// has been seen to write at. The zero value is ready to use and has seen nothing. It
// does no I/O and is not safe for concurrent use.
//
// The rate over one interval is the bytes written in it over its length, and the peak
// is the greatest of them: a quiet minute after a burst must not hide the burst. The
// first reading has no interval before it, so it gives the average since the server
// began counting, which understates a burst but is better than no rate at all.
type WALRate struct {
	prev *WALSample
	peak float64
}

// Observe takes one reading and returns the peak rate seen so far, in bytes per second.
// A reading that does not move time forward, or one whose counter went backwards
// because the server's statistics were reset, gives no interval and is only
// remembered; a reading with no counter is ignored.
func (r *WALRate) Observe(c DatabaseConditions) float64 {
	if c.WALBytes <= 0 {
		return r.peak
	}
	cur := WALSample{Bytes: c.WALBytes, At: c.At}
	defer func() { r.prev = &cur }()

	if r.prev == nil {
		// No interval yet: the average since the server began counting.
		if !c.WALStatsReset.IsZero() {
			r.take(c.WALBytes, c.At.Sub(c.WALStatsReset))
		}
	} else {
		// A counter that went backwards was reset: the difference is negative, which
		// the peak never takes, so the reading is only remembered.
		r.take(cur.Bytes-r.prev.Bytes, cur.At.Sub(r.prev.At))
	}
	return r.peak
}

// take raises the peak to bytes written over elapsed, when elapsed is a real interval.
func (r *WALRate) take(bytes int64, elapsed time.Duration) {
	if elapsed <= 0 {
		return
	}
	r.peak = max(r.peak, float64(bytes)/elapsed.Seconds())
}
