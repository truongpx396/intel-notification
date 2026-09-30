package notifytest

import (
	"context"
	"sync"
	"time"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

type budget struct {
	remaining int
	resetAt   time.Time
}

// QuotaCounter is an in-memory ports.QuotaCounter. A key with no budget is
// unlimited, as it is when the real counter has lost its state: quota fails open,
// never on correctness (D5, D24). A test gives a key a budget with SetBudget.
type QuotaCounter struct {
	mu      sync.Mutex
	budgets map[domain.QuotaKey]*budget
	taken   map[domain.QuotaKey]int
}

var _ ports.QuotaCounter = (*QuotaCounter)(nil)

// NewQuotaCounter returns a counter with every key unlimited.
func NewQuotaCounter() *QuotaCounter {
	return &QuotaCounter{budgets: map[domain.QuotaKey]*budget{}, taken: map[domain.QuotaKey]int{}}
}

// SetBudget gives k n units to take until the window resets at resetAt, replacing
// any earlier budget. The window does not roll by itself: a test that wants the
// next one calls SetBudget again.
func (q *QuotaCounter) SetBudget(k domain.QuotaKey, n int, resetAt time.Time) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.budgets[k] = &budget{remaining: n, resetAt: resetAt}
}

// Lose forgets every budget, as losing Redis does. Every key is unlimited again.
func (q *QuotaCounter) Lose() {}

// Taken is how many units have been taken from k.
func (q *QuotaCounter) Taken(k domain.QuotaKey) int { return 0 }

// Peek implements ports.QuotaCounter. It consumes nothing.
func (q *QuotaCounter) Peek(_ context.Context, k domain.QuotaKey) (bool, time.Time, error) {
	return false, time.Time{}, nil
}

// Take implements ports.QuotaCounter. It consumes one unit if any is left.
func (q *QuotaCounter) Take(_ context.Context, k domain.QuotaKey) (bool, time.Time, error) {
	return false, time.Time{}, nil
}
