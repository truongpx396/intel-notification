package domain

import (
	"fmt"
	"sync"
	"time"

	// The zone database travels with the engine, so quiet hours end at the same
	// instant on a host with no zoneinfo, and on one with an old copy of it.
	_ "time/tzdata"
)

// clockSeconds reads "15:04" or "15:04:05" as the seconds since local midnight.
// The store keeps a schedule's seconds, so the two forms are one format.
func clockSeconds(s string) (int, bool) {
	for _, layout := range [...]string{"15:04:05", "15:04"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.Hour()*3600 + t.Minute()*60 + t.Second(), true
		}
	}
	return 0, false
}

// zones remembers the zones that loaded. LoadLocation reads the zone database on
// every call and the planner asks once per channel per notification; only a zone
// that exists is kept, so what a caller can add is bounded by the database's size.
var zones sync.Map // name → *time.Location

// zone is the schedule's time zone: the IANA name, or UTC for none, which is what
// the schedule table defaults to. "Local" is refused: it is whatever zone the host
// runs in, so two replicas would end quiet hours at different instants.
func (s DeliverySchedule) zone() (*time.Location, error) {
	if s.Timezone == "Local" {
		return nil, fmt.Errorf("%q is the host's zone, not an IANA name", s.Timezone)
	}
	if loc, ok := zones.Load(s.Timezone); ok {
		return loc.(*time.Location), nil
	}
	loc, err := time.LoadLocation(s.Timezone)
	if err != nil {
		return nil, err
	}
	zones.Store(s.Timezone, loc)
	return loc, nil
}

// Validate reports every rule s breaks, as a *ValidationError, or nil. A quiet-hours
// time is "15:04" or "15:04:05" on a 24-hour clock; the window is both ends or
// neither, and does not start where it ends (that is an empty window or a whole
// day, and a schedule cannot say which); the time zone, when set, is one the zone
// database knows; and a digest cadence is not negative. The zero schedule is valid:
// immediate delivery, no quiet hours.
func (s DeliverySchedule) Validate() error {
	var v ValidationError
	start, startOK := 0, true
	if s.QuietStart != "" {
		if start, startOK = clockSeconds(s.QuietStart); !startOK {
			v.add("QuietStart", fmt.Sprintf("%q is not a time of day, such as 22:00 or 22:00:30", s.QuietStart), nil)
		}
	}
	end, endOK := 0, true
	if s.QuietEnd != "" {
		if end, endOK = clockSeconds(s.QuietEnd); !endOK {
			v.add("QuietEnd", fmt.Sprintf("%q is not a time of day, such as 07:00 or 07:00:30", s.QuietEnd), nil)
		}
	}
	switch {
	case s.QuietStart != "" && s.QuietEnd == "":
		v.add("QuietEnd", "is required when QuietStart is set: half a window ends nothing", nil)
	case s.QuietStart == "" && s.QuietEnd != "":
		v.add("QuietStart", "is required when QuietEnd is set: half a window starts nothing", nil)
	case startOK && endOK && s.QuietStart != "" && start == end:
		v.add("QuietEnd", fmt.Sprintf("%q is where the window starts: it would be empty or last all day", s.QuietEnd), nil)
	}
	if _, err := s.zone(); err != nil {
		v.add("Timezone", fmt.Sprintf("%q is not a time zone the zone database knows", s.Timezone), nil)
	}
	if s.Digest < 0 {
		v.add("Digest", fmt.Sprintf("%v is negative; 0 means immediate delivery", s.Digest), nil)
	}
	return v.err()
}

// QuietUntil reports whether at falls inside the schedule's quiet hours and, if so,
// the instant they end. The window is read on the wall clock of the schedule's own
// zone, its start inclusive and its end exclusive, and may wrap midnight; the end
// is the next time that clock reads the window's end, so a window that spans a
// change of clock is an hour shorter or longer in elapsed time than it reads. A
// schedule with no usable window is never quiet: the planner would rather send now
// than guess when a broken window ends (see [DeliverySchedule.Validate] for what
// makes one usable). A window that starts where it ends is empty here, as the
// comparisons below leave it, though Validate refuses it as ambiguous.
func (s DeliverySchedule) QuietUntil(at time.Time) (end time.Time, quiet bool) {
	start, startOK := clockSeconds(s.QuietStart)
	until, endOK := clockSeconds(s.QuietEnd)
	loc, err := s.zone()
	if !startOK || !endOK || err != nil {
		return time.Time{}, false
	}

	local := at.In(loc)
	now := local.Hour()*3600 + local.Minute()*60 + local.Second()
	wraps := start > until
	if wraps {
		quiet = now >= start || now < until
	} else {
		quiet = now >= start && now < until
	}
	if !quiet {
		return time.Time{}, false
	}
	// The window ends today, unless it wraps and began before midnight.
	days := 0
	if wraps && now >= start {
		days = 1
	}
	y, m, d := local.Date()
	return time.Date(y, m, d+days, until/3600, until%3600/60, until%60, 0, loc), true
}
