package domain

import (
	"slices"
	"strconv"
	"testing"
)

// The step names are what a failing crash-point test prints, so they are
// frozen: they are the contract's own words for the nine steps.
func TestDispatchStepString(t *testing.T) {
	t.Parallel()
	cases := []struct {
		step DispatchStep
		want string
	}{
		{StepClaim, "claim"},
		{StepExpiry, "expiry"},
		{StepLoad, "load"},
		{StepAddress, "address"},
		{StepQuota, "quota"},
		{StepRender, "render"},
		{StepDeliver, "deliver"},
		{StepRecord, "record"},
		{StepFence, "fence"},
		{0, "step(0)"},
		{10, "step(10)"},
		{-1, "step(-1)"},
	}
	for _, tc := range cases {
		t.Run(tc.want, func(t *testing.T) {
			t.Parallel()
			if got := tc.step.String(); got != tc.want {
				t.Fatalf("DispatchStep(%d).String() = %q, want %q", int(tc.step), got, tc.want)
			}
		})
	}
}

func TestDispatchStepValid(t *testing.T) {
	t.Parallel()
	cases := []struct {
		step DispatchStep
		want bool
	}{
		{0, false}, {1, true}, {5, true}, {9, true}, {10, false}, {-1, false},
	}
	for _, tc := range cases {
		t.Run(strconv.Itoa(int(tc.step)), func(t *testing.T) {
			t.Parallel()
			if got := tc.step.Valid(); got != tc.want {
				t.Fatalf("DispatchStep(%d).Valid() = %v, want %v", int(tc.step), got, tc.want)
			}
		})
	}
}

// There are nine, in the order the dispatcher runs them, and every one is valid
// and has its own name.
func TestAllDispatchSteps(t *testing.T) {
	t.Parallel()
	steps := AllDispatchSteps()
	want := []DispatchStep{StepClaim, StepExpiry, StepLoad, StepAddress, StepQuota, StepRender,
		StepDeliver, StepRecord, StepFence}
	if !slices.Equal(steps, want) {
		t.Fatalf("AllDispatchSteps() = %v, want %v", steps, want)
	}
	seen := map[string]bool{}
	for _, s := range steps {
		if !s.Valid() {
			t.Errorf("%d is listed but not valid", int(s))
		}
		if seen[s.String()] {
			t.Errorf("two steps are both named %q", s)
		}
		seen[s.String()] = true
	}
	// A caller may sort or truncate the result; the next call must not see it.
	steps[0] = StepFence
	if got := AllDispatchSteps()[0]; got != StepClaim {
		t.Fatalf("AllDispatchSteps shares its backing array: the first step is now %v", got)
	}
}
