package domain

import (
	"testing"
	"time"
)

// A drop is the one place a notification is discarded for load, so it is a table:
// drop_non_essential drops only what is neither essential nor critical, by the
// priority that counts, which is the topic's own when the notification sets none
// (D5, NR-023).
func TestQuotaPolicyDrops(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		policy   QuotaPolicy
		def      TopicDef
		priority Priority
		want     bool
		why      string
	}{
		{"defer never drops", QuotaDefer, TopicDef{DefaultPriority: PriorityInfo}, PriorityInfo, false, "deferring discards nothing"},
		{"an unset policy never drops", "", TopicDef{DefaultPriority: PriorityInfo}, PriorityInfo, false, "only the named policy drops"},
		{"info on an ordinary topic", QuotaDropNonEssential, TopicDef{DefaultPriority: PriorityInfo}, PriorityInfo, true, ""},
		{"warning on an ordinary topic", QuotaDropNonEssential, TopicDef{DefaultPriority: PriorityInfo}, PriorityWarning, true, ""},
		{"critical on an ordinary topic", QuotaDropNonEssential, TopicDef{DefaultPriority: PriorityInfo}, PriorityCritical, false,
			"a critical notification always defers"},
		{"info on an essential topic", QuotaDropNonEssential, TopicDef{DefaultPriority: PriorityInfo, Essential: true}, PriorityInfo, false,
			"an essential topic always defers"},
		{"warning on an essential topic", QuotaDropNonEssential, TopicDef{DefaultPriority: PriorityInfo, Essential: true}, PriorityWarning, false, ""},
		{"critical on an essential topic", QuotaDropNonEssential, TopicDef{DefaultPriority: PriorityInfo, Essential: true}, PriorityCritical, false, ""},
		{"no priority on a topic that defaults to info", QuotaDropNonEssential, TopicDef{DefaultPriority: PriorityInfo}, "", true, ""},
		{"no priority on a topic that defaults to critical", QuotaDropNonEssential, TopicDef{DefaultPriority: PriorityCritical}, "", false,
			"critical only by its topic's default is still critical"},
		{"info on a topic that defaults to critical", QuotaDropNonEssential, TopicDef{DefaultPriority: PriorityCritical}, PriorityInfo, true,
			"the notification's own priority wins over the topic's"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.policy.Drops(tc.def, tc.priority); got != tc.want {
				t.Fatalf("%q.Drops(%+v, %q) = %v, want %v: %s", tc.policy, tc.def, tc.priority, got, tc.want, tc.why)
			}
		})
	}
}

func at(y int, m time.Month, d, h, min, sec, nsec int) time.Time {
	return time.Date(y, m, d, h, min, sec, nsec, time.UTC)
}

// The windows are the UTC hour and the UTC day, whatever zone the instant is
// expressed in, and an instant on a boundary belongs to the window that starts there.
func TestQuotaWindows(t *testing.T) {
	t.Parallel()
	zone := time.FixedZone("UTC+7", 7*3600)
	cases := []struct {
		name       string
		w          QuotaWindow
		t          time.Time
		start, end time.Time
	}{
		{"an hour", QuotaHour, at(2031, 3, 1, 12, 34, 56, 7), at(2031, 3, 1, 12, 0, 0, 0), at(2031, 3, 1, 13, 0, 0, 0)},
		{"the first instant of an hour", QuotaHour, at(2031, 3, 1, 12, 0, 0, 0), at(2031, 3, 1, 12, 0, 0, 0), at(2031, 3, 1, 13, 0, 0, 0)},
		{"the last instant of an hour", QuotaHour, at(2031, 3, 1, 12, 59, 59, 999999999), at(2031, 3, 1, 12, 0, 0, 0), at(2031, 3, 1, 13, 0, 0, 0)},
		{"the last hour of a day ends the day", QuotaHour, at(2031, 3, 1, 23, 30, 0, 0), at(2031, 3, 1, 23, 0, 0, 0), at(2031, 3, 2, 0, 0, 0, 0)},
		{"a day", QuotaDay, at(2031, 3, 1, 12, 34, 56, 7), at(2031, 3, 1, 0, 0, 0, 0), at(2031, 3, 2, 0, 0, 0, 0)},
		{"midnight starts a day", QuotaDay, at(2031, 3, 2, 0, 0, 0, 0), at(2031, 3, 2, 0, 0, 0, 0), at(2031, 3, 3, 0, 0, 0, 0)},
		{"the end of February", QuotaDay, at(2031, 2, 28, 23, 59, 59, 0), at(2031, 2, 28, 0, 0, 0, 0), at(2031, 3, 1, 0, 0, 0, 0)},
		{"an instant in another zone is judged in UTC (hour)", QuotaHour, time.Date(2031, 3, 1, 19, 30, 0, 0, zone), at(2031, 3, 1, 12, 0, 0, 0), at(2031, 3, 1, 13, 0, 0, 0)},
		{"an instant in another zone is judged in UTC (day)", QuotaDay, time.Date(2031, 3, 1, 3, 0, 0, 0, zone), at(2031, 2, 28, 0, 0, 0, 0), at(2031, 3, 1, 0, 0, 0, 0)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.w.Start(tc.t); !got.Equal(tc.start) {
				t.Errorf("Start(%v) = %v, want %v", tc.t, got, tc.start)
			}
			if got := tc.w.End(tc.t); !got.Equal(tc.end) {
				t.Errorf("End(%v) = %v, want %v", tc.t, got, tc.end)
			}
		})
	}
}

// A budget is spent when either window has reached its ceiling, and its units return
// at the end of the longest window that is spent: a delivery deferred to the end of the
// hour would find the day still spent, and be deferred again in a loop (D24).
func TestQuotaLimitsStatus(t *testing.T) {
	t.Parallel()
	now := at(2031, 3, 1, 12, 34, 56, 0)
	hourEnd, dayEnd := at(2031, 3, 1, 13, 0, 0, 0), at(2031, 3, 2, 0, 0, 0, 0)
	cases := []struct {
		name      string
		limits    QuotaLimits
		hour, day int
		exhausted bool
		resetAt   time.Time
		why       string
	}{
		{"nothing spent", QuotaLimits{PerHour: 10, PerDay: 100}, 0, 0, false, hourEnd, "a budget with units left reports the end of the hour"},
		{"one unit left in the hour", QuotaLimits{PerHour: 10, PerDay: 100}, 9, 9, false, hourEnd, ""},
		{"the hour is spent", QuotaLimits{PerHour: 10, PerDay: 100}, 10, 10, true, hourEnd, "reached, not exceeded: ten units are ten takes"},
		{"the hour is over-spent", QuotaLimits{PerHour: 10, PerDay: 100}, 12, 12, true, hourEnd, "a ceiling lowered under a count is spent"},
		{"the day is spent and the hour is not", QuotaLimits{PerHour: 10, PerDay: 100}, 3, 100, true, dayEnd, "the hour resetting gives nothing back"},
		{"both are spent", QuotaLimits{PerHour: 10, PerDay: 20}, 10, 20, true, dayEnd, "the longer wins"},
		{"a zero hour allows none", QuotaLimits{PerHour: 0, PerDay: 100}, 0, 0, true, hourEnd, "a row that says zero silences the channel"},
		{"a zero day allows none", QuotaLimits{PerHour: 0, PerDay: 0}, 0, 0, true, dayEnd, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			exhausted, resetAt := tc.limits.Status(now, tc.hour, tc.day)
			if exhausted != tc.exhausted || !resetAt.Equal(tc.resetAt) {
				t.Fatalf("%+v.Status(%v, %d, %d) = %v, %v, want %v, %v: %s",
					tc.limits, now, tc.hour, tc.day, exhausted, resetAt, tc.exhausted, tc.resetAt, tc.why)
			}
		})
	}
}
