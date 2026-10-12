package domain

import "testing"

// SuppressionReason is the vocabulary of channel_suppressions.reason (D13). The
// store refuses any other value before it reaches the table's check, so the error
// names the reason and not a constraint.
func TestSuppressionReasonValid(t *testing.T) {
	t.Parallel()
	cases := []struct {
		reason SuppressionReason
		want   bool
	}{
		{SuppressHardBounce, true},
		{SuppressComplaint, true},
		{SuppressUnsubscribe, true},
		{SuppressInvalidAddress, true},
		{SuppressTokenRevoked, true},
		{SuppressSoftBounce, true},
		{"", false},
		{"bounce", false},
		{"HARD_BOUNCE", false},
		{" hard_bounce", false},
	}
	for _, tc := range cases {
		t.Run(string(tc.reason), func(t *testing.T) {
			t.Parallel()
			if got := tc.reason.Valid(); got != tc.want {
				t.Fatalf("SuppressionReason(%q).Valid() = %v, want %v", tc.reason, got, tc.want)
			}
		})
	}
}
