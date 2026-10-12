package domain

import (
	"errors"
	"testing"
	"time"
)

// Validate rejects what the planner could not apply, one changed field per row, and
// accepts everything the preference store round-trips (T009 left it here, T021).
func TestDeliveryScheduleValidate(t *testing.T) {
	t.Parallel()
	valid := func() DeliverySchedule {
		return DeliverySchedule{QuietStart: "22:00", QuietEnd: "07:00", Timezone: "Europe/Paris", Digest: 15 * time.Minute}
	}
	cases := []struct {
		name       string
		mutate     func(*DeliverySchedule)
		wantFields []string // the fields reported; nil means valid
		why        string
	}{
		{"a complete schedule", func(*DeliverySchedule) {}, nil, ""},
		{"the zero schedule: immediate, no quiet hours", func(s *DeliverySchedule) { *s = DeliverySchedule{} }, nil, "a recipient who never set one"},
		{"seconds are a clock time too", func(s *DeliverySchedule) { s.QuietStart, s.QuietEnd = "23:30:15", "06:15:45" }, nil,
			"the store keeps a schedule's seconds (T018)"},
		{"midnight and the last minute", func(s *DeliverySchedule) { s.QuietStart, s.QuietEnd = "00:00", "23:59" }, nil, ""},
		{"no time zone means UTC", func(s *DeliverySchedule) { s.Timezone = "" }, nil, "the schedule table defaults to UTC"},
		{"UTC by name", func(s *DeliverySchedule) { s.Timezone = "UTC" }, nil, ""},
		{"a digest cadence with no quiet hours", func(s *DeliverySchedule) { s.QuietStart, s.QuietEnd = "", "" }, nil, ""},
		{"quiet hours with no digest", func(s *DeliverySchedule) { s.Digest = 0 }, nil, ""},

		{"an hour past 23", func(s *DeliverySchedule) { s.QuietStart = "24:00" }, []string{"QuietStart"}, ""},
		{"minutes past 59", func(s *DeliverySchedule) { s.QuietStart = "22:60" }, []string{"QuietStart"}, ""},
		{"seconds past 59", func(s *DeliverySchedule) { s.QuietEnd = "07:00:61" }, []string{"QuietEnd"}, ""},
		{"a 12-hour time", func(s *DeliverySchedule) { s.QuietStart = "10pm" }, []string{"QuietStart"}, ""},
		{"an hour alone", func(s *DeliverySchedule) { s.QuietEnd = "7" }, []string{"QuietEnd"}, ""},
		{"not a time at all", func(s *DeliverySchedule) { s.QuietEnd = "morning" }, []string{"QuietEnd"}, ""},
		{"a start with no end", func(s *DeliverySchedule) { s.QuietEnd = "" }, []string{"QuietEnd"}, "half a window"},
		{"an end with no start", func(s *DeliverySchedule) { s.QuietStart = "" }, []string{"QuietStart"}, "half a window"},
		{"a window that starts where it ends", func(s *DeliverySchedule) { s.QuietEnd = "22:00" }, []string{"QuietEnd"},
			"empty or all day: the schedule cannot say which"},
		{"a time zone the database does not know", func(s *DeliverySchedule) { s.Timezone = "Mars/Olympus_Mons" }, []string{"Timezone"}, ""},
		{"Local, which is the host's", func(s *DeliverySchedule) { s.Timezone = "Local" }, []string{"Timezone"},
			"two replicas would disagree about when quiet hours end"},
		{"a bad time zone with no quiet hours", func(s *DeliverySchedule) {
			s.QuietStart, s.QuietEnd, s.Timezone = "", "", "Nowhere/Land"
		}, []string{"Timezone"}, "a typo is caught when it is written, not when quiet hours are first set"},
		{"a negative digest cadence", func(s *DeliverySchedule) { s.Digest = -time.Second }, []string{"Digest"}, ""},
		{"several faults are all reported", func(s *DeliverySchedule) {
			s.QuietStart, s.Timezone, s.Digest = "25:00", "Nowhere/Land", -1
		}, []string{"QuietStart", "Timezone", "Digest"}, "a caller fixes a request in one pass"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := valid()
			tc.mutate(&s)
			err := s.Validate()
			if tc.wantFields == nil {
				if err != nil {
					t.Fatalf("Validate(%+v) = %v, want valid", s, err)
				}
				return
			}
			var ve *ValidationError
			if !errors.As(err, &ve) {
				t.Fatalf("Validate(%+v) = %v, want a *ValidationError for %v: %s", s, err, tc.wantFields, tc.why)
			}
			var got []string
			for _, p := range ve.Problems {
				got = append(got, p.Field)
			}
			if len(got) != len(tc.wantFields) {
				t.Fatalf("Validate(%+v) reports %v, want %v: %s", s, got, tc.wantFields, tc.why)
			}
			for i := range got {
				if got[i] != tc.wantFields[i] {
					t.Fatalf("Validate(%+v) reports %v, want %v: %s", s, got, tc.wantFields, tc.why)
				}
			}
		})
	}
}

// QuietUntil answers in the schedule's own zone, with the window's start inclusive
// and its end exclusive, across midnight and across a change of clock.
func TestDeliveryScheduleQuietUntil(t *testing.T) {
	t.Parallel()
	utc := func(y int, m time.Month, d, h, mi, s int) time.Time { return time.Date(y, m, d, h, mi, s, 0, time.UTC) }
	paris, err := time.LoadLocation("Europe/Paris")
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name      string
		s         DeliverySchedule
		at        time.Time
		wantQuiet bool
		wantEnd   time.Time
		why       string
	}{
		{"inside a window within one day", DeliverySchedule{QuietStart: "11:00", QuietEnd: "13:00", Timezone: "UTC"},
			utc(2031, 3, 1, 12, 0, 0), true, utc(2031, 3, 1, 13, 0, 0), ""},
		{"the start is inside", DeliverySchedule{QuietStart: "11:00", QuietEnd: "13:00", Timezone: "UTC"},
			utc(2031, 3, 1, 11, 0, 0), true, utc(2031, 3, 1, 13, 0, 0), "quiet from the first second of the window"},
		{"the second before the start is not", DeliverySchedule{QuietStart: "11:00", QuietEnd: "13:00", Timezone: "UTC"},
			utc(2031, 3, 1, 10, 59, 59), false, time.Time{}, ""},
		{"the end is outside", DeliverySchedule{QuietStart: "11:00", QuietEnd: "13:00", Timezone: "UTC"},
			utc(2031, 3, 1, 13, 0, 0), false, time.Time{}, "a delivery due at the end instant is not deferred again"},
		{"the instant before the end is inside", DeliverySchedule{QuietStart: "11:00", QuietEnd: "13:00", Timezone: "UTC"},
			time.Date(2031, 3, 1, 12, 59, 59, 999_999_999, time.UTC), true, utc(2031, 3, 1, 13, 0, 0), "sub-second part does not leak past the end"},
		{"after the window", DeliverySchedule{QuietStart: "11:00", QuietEnd: "13:00", Timezone: "UTC"},
			utc(2031, 3, 1, 15, 0, 0), false, time.Time{}, ""},

		{"a window that wraps midnight, before it", DeliverySchedule{QuietStart: "22:00", QuietEnd: "07:00", Timezone: "UTC"},
			utc(2031, 3, 1, 23, 30, 0), true, utc(2031, 3, 2, 7, 0, 0), "ends tomorrow morning"},
		{"a window that wraps midnight, after it", DeliverySchedule{QuietStart: "22:00", QuietEnd: "07:00", Timezone: "UTC"},
			utc(2031, 3, 2, 2, 0, 0), true, utc(2031, 3, 2, 7, 0, 0), "ends this morning"},
		{"a window that wraps midnight, at midnight", DeliverySchedule{QuietStart: "22:00", QuietEnd: "07:00", Timezone: "UTC"},
			utc(2031, 3, 2, 0, 0, 0), true, utc(2031, 3, 2, 7, 0, 0), ""},
		{"a window that wraps midnight, by day", DeliverySchedule{QuietStart: "22:00", QuietEnd: "07:00", Timezone: "UTC"},
			utc(2031, 3, 1, 12, 0, 0), false, time.Time{}, ""},
		{"a window that wraps midnight, at its end", DeliverySchedule{QuietStart: "22:00", QuietEnd: "07:00", Timezone: "UTC"},
			utc(2031, 3, 2, 7, 0, 0), false, time.Time{}, ""},
		{"a window that wraps the end of the month", DeliverySchedule{QuietStart: "22:00", QuietEnd: "07:00", Timezone: "UTC"},
			utc(2031, 3, 31, 23, 0, 0), true, utc(2031, 4, 1, 7, 0, 0), ""},

		{"the zone decides, not UTC", DeliverySchedule{QuietStart: "22:00", QuietEnd: "07:00", Timezone: "Asia/Tokyo"},
			utc(2031, 3, 1, 14, 0, 0), true, utc(2031, 3, 1, 22, 0, 0),
			"14:00 UTC is 23:00 in Tokyo, and 07:00 Tokyo is 22:00 UTC"},
		{"the same instant is outside in another zone", DeliverySchedule{QuietStart: "22:00", QuietEnd: "07:00", Timezone: "UTC"},
			utc(2031, 3, 1, 14, 0, 0), false, time.Time{}, ""},
		{"no time zone means UTC", DeliverySchedule{QuietStart: "11:00", QuietEnd: "13:00"},
			utc(2031, 3, 1, 12, 0, 0), true, utc(2031, 3, 1, 13, 0, 0), ""},
		{"the instant's own location does not matter", DeliverySchedule{QuietStart: "11:00", QuietEnd: "13:00", Timezone: "UTC"},
			utc(2031, 3, 1, 12, 0, 0).In(paris), true, utc(2031, 3, 1, 13, 0, 0), ""},

		{"seconds count", DeliverySchedule{QuietStart: "23:30:15", QuietEnd: "06:15:45", Timezone: "UTC"},
			utc(2031, 3, 1, 23, 30, 14), false, time.Time{}, ""},
		{"seconds count: the start second", DeliverySchedule{QuietStart: "23:30:15", QuietEnd: "06:15:45", Timezone: "UTC"},
			utc(2031, 3, 1, 23, 30, 15), true, utc(2031, 3, 2, 6, 15, 45), ""},

		// Paris moves to summer time at 02:00 on 2031-03-30, so the night of the 29th
		// to the 30th lasts one hour less: 22:00 CET is 21:00Z, 07:00 CEST is 05:00Z.
		{"a window that spans the change to summer time", DeliverySchedule{QuietStart: "22:00", QuietEnd: "07:00", Timezone: "Europe/Paris"},
			utc(2031, 3, 29, 22, 0, 0), true, utc(2031, 3, 30, 5, 0, 0),
			"07:00 on the 30th is UTC+2; adding the window's nine hours to the start would say 06:00Z"},
		// ... and the night of the change back lasts one hour more: 2031-10-26, 03:00
		// CEST becomes 02:00 CET, so 07:00 CET is 06:00Z.
		{"a window that spans the change back to winter time", DeliverySchedule{QuietStart: "22:00", QuietEnd: "07:00", Timezone: "Europe/Paris"},
			utc(2031, 10, 25, 21, 0, 0), true, utc(2031, 10, 26, 6, 0, 0),
			"07:00 on the 26th is UTC+1"},
		{"a window inside the repeated hour", DeliverySchedule{QuietStart: "02:00", QuietEnd: "03:00", Timezone: "Europe/Paris"},
			utc(2031, 10, 26, 0, 30, 0), true, utc(2031, 10, 26, 2, 0, 0),
			"00:30Z is 02:30 CEST, the first 02:30 of the night; 03:00 is 02:00Z"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			end, quiet := tc.s.QuietUntil(tc.at)
			if quiet != tc.wantQuiet {
				t.Fatalf("QuietUntil(%v) quiet = %v, want %v: %s", tc.at, quiet, tc.wantQuiet, tc.why)
			}
			if !end.Equal(tc.wantEnd) {
				t.Fatalf("QuietUntil(%v) ends %v, want %v: %s", tc.at, end.UTC(), tc.wantEnd.UTC(), tc.why)
			}
		})
	}
}

// A schedule with no quiet hours, or one that does not validate, is never quiet:
// the planner sends now rather than guess when a broken window ends.
func TestDeliveryScheduleQuietUntilNeverQuietWithoutAUsableWindow(t *testing.T) {
	t.Parallel()
	at := time.Date(2031, 3, 1, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		s    DeliverySchedule
	}{
		{"the zero schedule", DeliverySchedule{}},
		{"a digest and no window", DeliverySchedule{Digest: time.Hour}},
		{"a malformed start", DeliverySchedule{QuietStart: "noon", QuietEnd: "13:00"}},
		{"an unknown zone", DeliverySchedule{QuietStart: "11:00", QuietEnd: "13:00", Timezone: "Nowhere/Land"}},
		{"a window with no end", DeliverySchedule{QuietStart: "11:00"}},
		{"a window that starts where it ends", DeliverySchedule{QuietStart: "12:00", QuietEnd: "12:00"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if end, quiet := tc.s.QuietUntil(at); quiet || !end.IsZero() {
				t.Fatalf("QuietUntil = %v, %v, want not quiet", end, quiet)
			}
		})
	}
}

// A zone that loaded is remembered, and a name is remembered as itself: asking for
// one zone and then another, and then the first again, answers each in its own
// time, never the last one loaded.
func TestDeliveryScheduleRemembersEachZoneUnderItsOwnName(t *testing.T) {
	t.Parallel()
	at := time.Date(2031, 3, 1, 12, 0, 0, 0, time.UTC)
	// 12:00 UTC is 17:45 in Kathmandu (UTC+5:45), 12:00 in UTC and 21:00 in Tokyo.
	cases := []struct {
		zone     string
		start    string
		wantEnd  time.Time
		wantFrom string
	}{
		{"Asia/Kathmandu", "17:00", time.Date(2031, 3, 1, 18, 0, 0, 0, time.UTC).Add(-5*time.Hour - 45*time.Minute), "after the 18:00 end, local"},
		{"", "11:00", time.Date(2031, 3, 1, 13, 0, 0, 0, time.UTC), "UTC for no zone"},
		{"Asia/Tokyo", "20:00", time.Date(2031, 3, 1, 22, 0, 0, 0, time.UTC).Add(-9 * time.Hour), "after the 22:00 end, local"},
		{"Asia/Kathmandu", "17:00", time.Date(2031, 3, 1, 18, 0, 0, 0, time.UTC).Add(-5*time.Hour - 45*time.Minute), "asked a second time"},
		{"", "11:00", time.Date(2031, 3, 1, 13, 0, 0, 0, time.UTC), "asked a second time"},
	}
	for i, tc := range cases {
		end := "18:00"
		switch tc.zone {
		case "":
			end = "13:00"
		case "Asia/Tokyo":
			end = "22:00"
		}
		s := DeliverySchedule{QuietStart: tc.start, QuietEnd: end, Timezone: tc.zone}
		got, quiet := s.QuietUntil(at)
		if !quiet || !got.Equal(tc.wantEnd) {
			t.Fatalf("ask %d, zone %q: QuietUntil = %v, %v, want quiet until %v (%s)", i+1, tc.zone, got.UTC(), quiet, tc.wantEnd, tc.wantFrom)
		}
	}
}
