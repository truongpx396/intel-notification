package domain

// DispatchStep is one of the nine steps of the dispatcher's algorithm
// (notification-ports.md § The dispatcher, step by step). The steps are named so
// a test can hold a worker at one of them, or crash it there, and watch what the
// rest of the system does (T033).
type DispatchStep int

const (
	StepClaim   DispatchStep = iota + 1 // 1. claim a batch from a shard
	StepExpiry                          // 2. expired?
	StepLoad                            // 3. load the notification or digest members
	StepAddress                         // 4. resolve and bind addresses
	StepQuota                           // 5. take quota
	StepRender                          // 6. render
	StepDeliver                         // 7. deliver
	StepRecord                          // 8. record the outcome
	StepFence                           // 9. fenced? a lost lease is discarded
)

// AllDispatchSteps returns the nine steps in the order the dispatcher runs them.
func AllDispatchSteps() []DispatchStep { return nil }

// Valid reports whether s is one of the nine steps.
func (s DispatchStep) Valid() bool { return false }

// String names the step as the contract does.
func (s DispatchStep) String() string { return "" }
