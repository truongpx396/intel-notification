package domain

import "testing"

// The dead-letter and fallback policy (D8, D12, D29, D37). This table is the
// rule; the store executes whatever a Finish says and decides nothing itself.
func TestDispositionOf(t *testing.T) {
	t.Parallel()
	cases := []struct {
		outcome TerminalOutcome
		want    Disposition
		why     string
	}{
		{OutcomeDelivered, Disposition{}, "nothing more to do"},
		{OutcomeSuppressed, Disposition{Fallback: true}, "a dead address: correct, but try the next channel"},
		{OutcomeNoAddress, Disposition{Fallback: true}, "no address here: correct, but try the next channel"},
		{OutcomeMaxAttempts, Disposition{DeadLetter: true, Fallback: true}, "a genuine failure"},
		{OutcomeRejected, Disposition{DeadLetter: true, Fallback: true}, "a genuine failure"},
		{OutcomeExpired, Disposition{}, "never sent late, on this channel or another"},
		{OutcomeCanceled, Disposition{}, "withdrawn by the producer"},
		{OutcomeDroppedQuota, Disposition{}, "dropped by the tenant's own policy"},
	}
	for _, tc := range cases {
		t.Run(string(tc.outcome), func(t *testing.T) {
			t.Parallel()
			if got := DispositionOf(tc.outcome); got != tc.want {
				t.Fatalf("DispositionOf(%s) = %+v, want %+v: %s", tc.outcome, got, tc.want, tc.why)
			}
		})
	}
}

func TestNewFinish(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		outcome TerminalOutcome
		wantErr bool
	}{
		{"applies the policy", OutcomeMaxAttempts, false},
		{"fanned_out is recorded by binding, not a finish", OutcomeFannedOut, true},
		{"an unknown outcome is refused", "whatever", true},
		{"the empty outcome is refused", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f, err := NewFinish(tc.outcome, "prov-1", "detail")
			if (err != nil) != tc.wantErr {
				t.Fatalf("NewFinish(%q) error = %v, want error = %v", tc.outcome, err, tc.wantErr)
			}
			if err == nil && (f.Disposition != DispositionOf(tc.outcome) || f.Detail != "detail" ||
				f.ProviderMessageID != "prov-1") {
				t.Fatalf("NewFinish did not carry the outcome and its policy: %+v", f)
			}
		})
	}
}
