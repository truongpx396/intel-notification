package domain

import (
	"math"
	"slices"
	"strings"
	"testing"
	"time"
)

// T040a: the database conditions the queue depends on, judged. The rule is pure, so
// it is a table over readings; reading the conditions from PostgreSQL is the
// adapter's (TestDatabaseConditions) and turning the verdict into a warning and a
// gauge is the job's (app's TestPreflight...).

const (
	gib = int64(1) << 30
	mib = int64(1) << 20
)

// healthy is a reading with nothing to report: both timeouts set, no old transaction,
// the role sees every session, and 16 GiB of WAL against a known rate of 5 MiB/s,
// which is 4.5 GiB for fifteen minutes.
func healthy() DatabaseConditions {
	return DatabaseConditions{
		At:                       time.Date(2031, 3, 1, 12, 0, 0, 0, time.UTC),
		MaxWALSize:               16 * gib,
		IdleInTransactionTimeout: 30 * time.Second,
		StatementTimeout:         time.Minute,
		SeesAllSessions:          true,
	}
}

func codes(ws []PreflightWarning) []string {
	var out []string
	for _, w := range ws {
		out = append(out, w.Code)
	}
	return out
}

func TestAssessDatabase(t *testing.T) {
	t.Parallel()
	limits := DefaultPreflightLimits()
	fiveMiB := float64(5 * mib)
	cases := []struct {
		name    string
		mutate  func(*DatabaseConditions)
		walRate float64
		want    []string
		why     string
	}{
		{name: "a healthy configuration produces none", walRate: fiveMiB},
		{name: "a role with no idle-in-transaction timeout", walRate: fiveMiB,
			mutate: func(c *DatabaseConditions) { c.IdleInTransactionTimeout = 0 },
			want:   []string{WarnNoIdleInTransactionTimeout}},
		{name: "a role with no statement timeout", walRate: fiveMiB,
			mutate: func(c *DatabaseConditions) { c.StatementTimeout = 0 },
			want:   []string{WarnNoStatementTimeout}},
		{name: "a role with neither timeout", walRate: fiveMiB,
			mutate: func(c *DatabaseConditions) { c.IdleInTransactionTimeout, c.StatementTimeout = 0, 0 },
			want:   []string{WarnNoIdleInTransactionTimeout, WarnNoStatementTimeout},
			why:    "each timeout is its own warning, in the order the operations guide lists them"},
		{name: "a transaction older than the limit", walRate: fiveMiB,
			mutate: func(c *DatabaseConditions) { c.OldestTransactionAge = 61 * time.Second },
			want:   []string{WarnOldTransaction}},
		{name: "a transaction exactly at the limit is not yet old", walRate: fiveMiB,
			mutate: func(c *DatabaseConditions) { c.OldestTransactionAge = 60 * time.Second },
			why:    "the alarm is for one older than 60 seconds"},
		{name: "a recent transaction", walRate: fiveMiB,
			mutate: func(c *DatabaseConditions) { c.OldestTransactionAge = 5 * time.Second }},
		{name: "a role that cannot see other sessions", walRate: fiveMiB,
			mutate: func(c *DatabaseConditions) { c.SeesAllSessions = false },
			want:   []string{WarnBlindToOtherSessions},
			why:    "a transaction age read through a blind role is a clean bill of health it has not earned"},
		{name: "max_wal_size under fifteen minutes of the peak rate", walRate: fiveMiB,
			mutate: func(c *DatabaseConditions) { c.MaxWALSize = gib },
			want:   []string{WarnMaxWALSizeUndersized},
			why:    "the default 1 GiB against 5 MiB/s is 205 seconds"},
		{name: "max_wal_size at exactly fifteen minutes of the peak rate", walRate: fiveMiB,
			mutate: func(c *DatabaseConditions) { c.MaxWALSize = 5 * mib * 15 * 60 },
			why:    "at least fifteen minutes"},
		{name: "one byte under fifteen minutes", walRate: fiveMiB,
			mutate: func(c *DatabaseConditions) { c.MaxWALSize = 5*mib*15*60 - 1 },
			want:   []string{WarnMaxWALSizeUndersized}},
		{name: "no known rate judges no size", walRate: 0,
			mutate: func(c *DatabaseConditions) { c.MaxWALSize = mib },
			why:    "with no rate there is nothing to size it against, and a guess would be an alarm on a quiet database"},
		{name: "every condition at once, in order", walRate: fiveMiB,
			mutate: func(c *DatabaseConditions) {
				c.IdleInTransactionTimeout, c.StatementTimeout = 0, 0
				c.OldestTransactionAge = time.Hour
				c.SeesAllSessions = false
				c.MaxWALSize = gib
			},
			want: []string{WarnNoIdleInTransactionTimeout, WarnNoStatementTimeout, WarnOldTransaction,
				WarnBlindToOtherSessions, WarnMaxWALSizeUndersized}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := healthy()
			if tc.mutate != nil {
				tc.mutate(&c)
			}
			ws := AssessDatabase(c, tc.walRate, limits)
			if got := codes(ws); !slices.Equal(got, tc.want) {
				t.Fatalf("AssessDatabase = %v, want %v: %s", got, tc.want, tc.why)
			}
			for _, w := range ws {
				if w.Message == "" {
					t.Errorf("warning %s has no message: an operator is told what to do about it", w.Code)
				}
			}
		})
	}
}

// A warning says what it measured, so the person reading it does not have to go and
// look: the age it found and the size it wants.
func TestAssessDatabaseWarningsNameTheirNumbers(t *testing.T) {
	t.Parallel()
	c := healthy()
	c.OldestTransactionAge = 95 * time.Second
	c.MaxWALSize = gib
	ws := AssessDatabase(c, float64(5*mib), DefaultPreflightLimits())
	messages := map[string]string{}
	for _, w := range ws {
		messages[w.Code] = w.Message
	}
	for code, wants := range map[string][]string{
		WarnOldTransaction:       {"1m35s"},
		WarnMaxWALSizeUndersized: {"1.0 GiB", "4.4 GiB"},
	} {
		for _, want := range wants {
			if !strings.Contains(messages[code], want) {
				t.Errorf("%s message = %q, want it to name %q", code, messages[code], want)
			}
		}
	}
}

func TestWALRate(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2031, 3, 1, 12, 0, 0, 0, time.UTC)
	reading := func(at time.Time, bytes int64, reset time.Time) DatabaseConditions {
		return DatabaseConditions{At: at, WALBytes: bytes, WALStatsReset: reset}
	}
	steps := []struct { // sequential on purpose: each reading is judged against the last
		name string
		c    DatabaseConditions
		want float64
		why  string
	}{
		{"the first reading is the average since the counter began",
			reading(t0, 600*mib, t0.Add(-10*time.Minute)), float64(mib),
			"600 MiB over the ten minutes since the reset"},
		{"the next is the rate over the interval between them",
			reading(t0.Add(time.Minute), 600*mib+300*mib, t0.Add(-10*time.Minute)), 5 * float64(mib),
			"300 MiB in 60 seconds, which beats the average"},
		{"a quieter interval does not lower the peak",
			reading(t0.Add(2*time.Minute), 900*mib+60*mib, t0.Add(-10*time.Minute)), 5 * float64(mib),
			"60 MiB in 60 seconds is 1 MiB/s, and a quiet minute must not hide the burst"},
		{"a busier interval raises it",
			reading(t0.Add(3*time.Minute), 960*mib+600*mib, t0.Add(-10*time.Minute)), 10 * float64(mib), ""},
		{"a reading that does not move time forward gives no interval",
			reading(t0.Add(3*time.Minute), 960*mib+900*mib, t0.Add(-10*time.Minute)), 10 * float64(mib),
			"the same instant twice is a division by zero, not an infinite rate"},
		{"a counter that went backwards was reset, and gives no rate",
			reading(t0.Add(4*time.Minute), 100*mib, t0.Add(4*time.Minute-time.Second)), 10 * float64(mib),
			"the peak stands: the interval is unknowable, and 100 MiB in the second since the reset is not a rate to claim"},
	}
	var r WALRate
	for _, st := range steps {
		if got := r.Observe(st.c); math.Abs(got-st.want) > 1 {
			t.Fatalf("%s: peak = %.0f B/s, want %.0f: %s", st.name, got, st.want, st.why)
		}
	}
}

func TestWALRateIgnoresAReadingWithNoCounter(t *testing.T) {
	t.Parallel()
	var r WALRate
	if got := r.Observe(DatabaseConditions{At: time.Date(2031, 3, 1, 12, 0, 0, 0, time.UTC)}); got != 0 {
		t.Fatalf("a reading with no counter gave a rate of %.0f B/s, want none", got)
	}
}

func TestWALRateOfAServerThatDoesNotSayWhenItBeganCounting(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2031, 3, 1, 12, 0, 0, 0, time.UTC)
	var r WALRate
	if got := r.Observe(DatabaseConditions{At: t0, WALBytes: gib}); got != 0 {
		t.Fatalf("a first reading with no reset time gave %.0f B/s, want none: there is no interval to divide by", got)
	}
	if got := r.Observe(DatabaseConditions{At: t0.Add(time.Minute), WALBytes: gib + 60*mib}); math.Abs(got-float64(mib)) > 1 {
		t.Fatalf("the second reading gave %.0f B/s, want 1 MiB/s over the interval", got)
	}
}
