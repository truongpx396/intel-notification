package notifytest

import (
	"context"
	"errors"
	"sync"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// ErrInjectedCommit is what a notify returns when its commit was failed on
// purpose by [FaultStore.FailNextCommit].
var ErrInjectedCommit = errors.New("notifytest: injected commit failure")

// Transaction is the part of a transaction the wrapper drives. A pgx.Tx satisfies
// it.
type Transaction interface {
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// BeginFunc opens a transaction for the wrapper to own.
type BeginFunc func(ctx context.Context) (Transaction, error)

// FaultStore wraps a ports.Store so a test can make the next commit fail. It does
// so through the port's own seam, with nothing added to the adapter: a notify
// with no caller transaction is turned into one the wrapper owns — it opens the
// transaction, hands it to the inner store as a caller's, and then commits it,
// or, when a failure is armed, rolls it back and reports ErrInjectedCommit. The
// inner store has done all its writes by then, so this is the failure that
// matters: a commit that does not happen after the work was done (D18).
//
// A transaction the caller supplies (NotifyTx) passes straight through, because
// the caller owns its commit.
type FaultStore struct {
	ports.Store

	begin BeginFunc

	mu       sync.Mutex
	failNext int
}

var _ ports.Store = (*FaultStore)(nil)

// NewFaultStore wraps inner. begin opens the transactions the wrapper owns: for a
// pgx pool, `func(ctx) (Transaction, error) { return pool.Begin(ctx) }`.
func NewFaultStore(inner ports.Store, begin BeginFunc) *FaultStore {
	return &FaultStore{Store: inner, begin: begin}
}

// FailNextCommit arms one failure: the next commit the wrapper would make is
// rolled back instead, and reported as ErrInjectedCommit. Calling it twice arms
// two. A failure fires only at a commit, so an inner error, or a transaction the
// caller owns, leaves it armed.
func (s *FaultStore) FailNextCommit() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failNext++
}

// consumeFailure spends one armed failure, if there is one.
func (s *FaultStore) consumeFailure() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failNext == 0 {
		return false
	}
	s.failNext--
	return true
}

// PersistAndEnqueue implements ports.Store.
func (s *FaultStore) PersistAndEnqueue(ctx context.Context, tx ports.Tx, id domain.Identity, n domain.Notification, plan domain.DeliveryPlan) (domain.Receipt, error) {
	if tx != nil {
		return s.Store.PersistAndEnqueue(ctx, tx, id, n, plan)
	}

	own, err := s.begin(ctx)
	if err != nil {
		return domain.Receipt{}, err
	}
	// A canceled ctx must not stop the rollback that frees the connection.
	rollback := func() { _ = own.Rollback(context.WithoutCancel(ctx)) }

	receipt, err := s.Store.PersistAndEnqueue(ctx, own, id, n, plan)
	if err != nil {
		rollback()
		return domain.Receipt{}, err
	}
	if s.consumeFailure() {
		rollback()
		return domain.Receipt{}, ErrInjectedCommit
	}
	if err := own.Commit(ctx); err != nil {
		return domain.Receipt{}, err
	}
	return receipt, nil
}
