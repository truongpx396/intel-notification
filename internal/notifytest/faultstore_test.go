package notifytest

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

type fakeTx struct {
	mu                 sync.Mutex
	commits, rollbacks int
	commitErr          error
}

func (f *fakeTx) Commit(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commits++
	return f.commitErr
}

func (f *fakeTx) Rollback(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rollbacks++
	return nil
}

func (f *fakeTx) counts() (commits, rollbacks int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.commits, f.rollbacks
}

// innerStore records what the wrapper hands the store it wraps.
type innerStore struct {
	ports.Store
	calls   int
	gotTx   ports.Tx
	err     error
	receipt domain.Receipt
}

func (s *innerStore) PersistAndEnqueue(_ context.Context, tx ports.Tx, _ domain.Identity, _ domain.Notification, _ domain.DeliveryPlan) (domain.Receipt, error) {
	s.calls++
	s.gotTx = tx
	return s.receipt, s.err
}

func (s *innerStore) Status(context.Context, domain.Identity, string) (domain.NotificationStatus, error) {
	return domain.NotificationStatus{Found: true, NotificationID: "from-inner"}, nil
}

// fixture is a wrapper over a recording store, with every transaction it opens
// kept for inspection.
type fixture struct {
	inner *innerStore
	store *FaultStore
	txs   []*fakeTx
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{inner: &innerStore{receipt: domain.Receipt{NotificationID: "n1", Applied: true}}}
	f.store = NewFaultStore(f.inner, func(context.Context) (Transaction, error) {
		tx := &fakeTx{}
		f.txs = append(f.txs, tx)
		return tx, nil
	})
	return f
}

// tx returns the i'th transaction the wrapper opened, failing the test, rather than
// panicking, if it opened fewer.
func (f *fixture) tx(t *testing.T, i int) *fakeTx {
	t.Helper()
	if i >= len(f.txs) {
		t.Fatalf("the wrapper opened %d transactions, want at least %d", len(f.txs), i+1)
	}
	return f.txs[i]
}

func (f *fixture) notify(ctx context.Context, tx ports.Tx) (domain.Receipt, error) {
	return f.store.PersistAndEnqueue(ctx, tx, domain.Identity{}, domain.Notification{}, domain.DeliveryPlan{})
}

// Without a failure armed the wrapper is invisible: the store's work is committed
// in a transaction the wrapper opened, and the store's receipt comes back.
func TestFaultStoreCommitsWhenNothingIsArmed(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	r, err := f.notify(t.Context(), nil)
	if err != nil {
		t.Fatalf("Notify failed with nothing armed: %v", err)
	}
	if r.NotificationID != "n1" || !r.Applied {
		t.Fatalf("receipt = %+v, want the inner store's", r)
	}
	if len(f.txs) != 1 {
		t.Fatalf("the wrapper opened %d transactions, want 1", len(f.txs))
	}
	if f.inner.gotTx != ports.Tx(f.tx(t, 0)) {
		t.Fatalf("the inner store was given %v, want the wrapper's transaction", f.inner.gotTx)
	}
	if c, r := f.tx(t, 0).counts(); c != 1 || r != 0 {
		t.Fatalf("commits = %d, rollbacks = %d, want 1 and 0", c, r)
	}
}

// The armed failure is a commit that does not happen after the work was done: the
// inner store wrote inside the transaction, and the transaction is rolled back.
func TestFaultStoreFailsTheNextCommitOnce(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.store.FailNextCommit()

	r, err := f.notify(t.Context(), nil)
	if !errors.Is(err, ErrInjectedCommit) {
		t.Fatalf("err = %v, want ErrInjectedCommit", err)
	}
	if r.NotificationID != "" || r.Applied {
		t.Fatalf("receipt = %+v, want none: nothing was accepted", r)
	}
	if f.inner.calls != 1 || f.inner.gotTx != ports.Tx(f.tx(t, 0)) {
		t.Fatalf("the inner store must have done its writes, inside the wrapper's transaction: calls = %d", f.inner.calls)
	}
	if c, rb := f.tx(t, 0).counts(); c != 0 || rb != 1 {
		t.Fatalf("commits = %d, rollbacks = %d, want 0 and 1", c, rb)
	}

	// It fired once: the retry goes through, as a producer's retry would.
	if _, err := f.notify(t.Context(), nil); err != nil {
		t.Fatalf("the retry failed too: %v", err)
	}
	if c, rb := f.tx(t, 1).counts(); c != 1 || rb != 0 {
		t.Fatalf("retry: commits = %d, rollbacks = %d, want 1 and 0", c, rb)
	}
}

func TestFaultStoreFailureCountsAccumulate(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.store.FailNextCommit()
	f.store.FailNextCommit()
	for i := range 2 {
		if _, err := f.notify(t.Context(), nil); !errors.Is(err, ErrInjectedCommit) {
			t.Fatalf("call %d: err = %v, want ErrInjectedCommit", i+1, err)
		}
	}
	if _, err := f.notify(t.Context(), nil); err != nil {
		t.Fatalf("the third call failed, with both failures spent: %v", err)
	}
}

// NotifyTx: the host owns the transaction and its commit, so the wrapper neither
// opens one nor ends the host's, and an armed failure waits for a notify that
// commits.
func TestFaultStorePassesACallersTransactionThrough(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.store.FailNextCommit()
	hosts := &fakeTx{}

	r, err := f.notify(t.Context(), hosts)
	if err != nil || r.NotificationID != "n1" {
		t.Fatalf("NotifyTx = %+v, %v, want the inner store's receipt and no error", r, err)
	}
	if f.inner.gotTx != ports.Tx(hosts) {
		t.Fatalf("the inner store was given %v, want the host's transaction untouched", f.inner.gotTx)
	}
	if len(f.txs) != 0 {
		t.Fatalf("the wrapper opened %d transactions for a caller-owned one", len(f.txs))
	}
	if c, rb := hosts.counts(); c != 0 || rb != 0 {
		t.Fatalf("the host's transaction saw commits = %d, rollbacks = %d: the wrapper must never end it", c, rb)
	}

	// The failure armed earlier is still waiting, and fires at the next commit.
	if _, err := f.notify(t.Context(), nil); !errors.Is(err, ErrInjectedCommit) {
		t.Fatalf("err = %v, want the armed failure to fire at the next Notify", err)
	}
}

// An error before the commit is the inner store's own, and the wrapper rolls its
// transaction back rather than leave it open. The armed failure is not spent: it
// fires at a commit.
func TestFaultStoreRollsBackWhenTheInnerStoreFails(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.inner.err = errors.New("inner failed")
	f.store.FailNextCommit()

	_, err := f.notify(t.Context(), nil)
	if !errors.Is(err, f.inner.err) || errors.Is(err, ErrInjectedCommit) {
		t.Fatalf("err = %v, want the inner store's error", err)
	}
	if c, rb := f.tx(t, 0).counts(); c != 0 || rb != 1 {
		t.Fatalf("commits = %d, rollbacks = %d, want 0 and 1", c, rb)
	}

	f.inner.err = nil
	if _, err := f.notify(t.Context(), nil); !errors.Is(err, ErrInjectedCommit) {
		t.Fatalf("err = %v, want the failure still armed after an inner error", err)
	}
}

func TestFaultStoreReportsAFailedCommit(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	boom := errors.New("connection lost")
	f.store = NewFaultStore(f.inner, func(context.Context) (Transaction, error) {
		return &fakeTx{commitErr: boom}, nil
	})
	if _, err := f.notify(t.Context(), nil); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the commit's own error", err)
	}
}

func TestFaultStoreReportsAFailedBegin(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	boom := errors.New("pool exhausted")
	f.store = NewFaultStore(f.inner, func(context.Context) (Transaction, error) { return nil, boom })
	if _, err := f.notify(t.Context(), nil); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want Begin's error", err)
	}
	if f.inner.calls != 0 {
		t.Fatalf("the inner store was called %d times with no transaction to write in", f.inner.calls)
	}
}

// Everything but the persist passes straight through to the wrapped store.
func TestFaultStoreDelegatesTheRest(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	st, err := f.store.Status(t.Context(), domain.Identity{}, "k")
	if err != nil || st.NotificationID != "from-inner" {
		t.Fatalf("Status = %+v, %v, want the inner store's answer", st, err)
	}
}
