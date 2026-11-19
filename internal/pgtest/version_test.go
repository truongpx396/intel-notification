package pgtest

import (
	"strings"
	"testing"
)

func TestImageFrom(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, env, want string }{
		{"unset", "", DefaultImage},
		{"a newer major", "postgres:17-alpine", "postgres:17-alpine"},
		{"an exact tag", "postgres:16.4-alpine", "postgres:16.4-alpine"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := imageFrom(tc.env); got != tc.want {
				t.Fatalf("imageFrom(%q) = %q, want %q", tc.env, got, tc.want)
			}
		})
	}
}

// The guard's job is to refuse a run that is not the one it was asked for, so what it says
// has to name both majors: the person reading a red CI leg needs to see which way it went wrong.
func TestCheckMajor(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		expect  string
		version int
		wantErr []string // empty: no error; otherwise every phrase the error must contain
	}{
		{"no expectation checks nothing", "", 160004, nil},
		{"the expected major", "16", 160004, nil},
		{"the expected major on a newer server", "17", 170002, nil},
		{"a server older than asked for", "17", 160004, []string{"17", "16"}},
		{"a server newer than asked for", "16", 170002, []string{"16", "17"}},
		{"an expectation that is not a number", "seventeen", 170002, []string{"seventeen"}},
		{"a number with a minor is not a major", "17.2", 170002, []string{"17.2"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := checkMajor(tc.expect, tc.version)
			if len(tc.wantErr) == 0 {
				if err != nil {
					t.Fatalf("checkMajor(%q, %d) = %v, want nil", tc.expect, tc.version, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("checkMajor(%q, %d) = nil, want an error naming %v", tc.expect, tc.version, tc.wantErr)
			}
			for _, phrase := range tc.wantErr {
				if !strings.Contains(err.Error(), phrase) {
					t.Fatalf("checkMajor(%q, %d) = %q, want it to name %q", tc.expect, tc.version, err, phrase)
				}
			}
		})
	}
}
