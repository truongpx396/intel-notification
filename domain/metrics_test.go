package domain

import "testing"

// These names are what docs/operations.md tells an operator to alarm on, and what a
// dashboard is built from. Renaming one silently disarms the alarm, so they are
// frozen here as literals and not derived from the constants they test.
func TestMetricNamesAreFrozen(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct{ got, want string }{
		"lease lost":  {MetricLeaseLost, "notify.lease.lost"},
		"dead letter": {MetricDeadLetter, "notify.dlq.dead.count"},
		"channel":     {LabelChannel, "channel"},
		"reason":      {LabelReason, "reason"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if tc.got != tc.want {
				t.Fatalf("%s = %q, want %q", name, tc.got, tc.want)
			}
		})
	}
}
