package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/truongpx396/intel-notification/ports"
)

// dispatchChannel is the PostgreSQL notification channel a commit of work is announced
// on. A notification carries no payload: it says only "look".
const dispatchChannel = "notify_dispatch"

// Listener implements ports.WakeSource over one dedicated connection (D22). It is the
// listening end of [WithNotify], and a latency optimisation over polling: it can fail,
// and the dispatcher polls on.
//
// LISTEN is a property of a session, so this needs a direct connection to PostgreSQL.
// Behind a transaction-pooling proxy the registration lands on whichever backend the
// proxy chose and is lost with it, which is the same reason the engine's job leases are
// rows and not advisory locks.
type Listener struct {
	pool *pgxpool.Pool
}

var _ ports.WakeSource = (*Listener)(nil)

// NewListener returns a Listener that takes its connection from pool.
func NewListener(pool *pgxpool.Pool) *Listener { return &Listener{pool: pool} }

// Listen implements ports.WakeSource. It returns once the session is listening, so a
// commit after it returns is heard. The connection is taken out of the pool for good and
// closed when ctx ends, not returned: a pooled connection that still LISTENs would be
// handed to the next caller with a registration nobody reads. The channel is closed when
// ctx ends or the connection is lost. A signal is dropped when one is already waiting.
func (l *Listener) Listen(ctx context.Context) (<-chan struct{}, error) {
	acquired, err := l.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("listen: %w", err)
	}
	conn := acquired.Hijack()
	if _, err := conn.Exec(ctx, "LISTEN "+dispatchChannel); err != nil {
		_ = conn.Close(context.WithoutCancel(ctx))
		return nil, fmt.Errorf("listen: %w", err)
	}

	wake := make(chan struct{}, 1)
	go func() {
		defer close(wake)
		defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
		for {
			if _, err := conn.WaitForNotification(ctx); err != nil {
				return // the context ended, or the connection is gone
			}
			select {
			case wake <- struct{}{}:
			default:
			}
		}
	}()
	return wake, nil
}
