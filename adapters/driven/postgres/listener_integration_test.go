//go:build integration

package postgres

import (
	"context"
	"testing"
	"time"
)

// D22. A commit of work announces itself, so a dispatcher in another process claims
// at once instead of at its next poll. It is opt-in on both ends: the store issues the
// NOTIFY only when built WithNotify, and the dispatcher listens only when
// Config.ListenNotify is on.

const wakeDeadline = 15 * time.Second

func recvWake(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case _, ok := <-ch:
		if !ok {
			t.Fatalf("the listener ended while waiting for %s", what)
		}
	case <-time.After(wakeDeadline):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// The commit of a notification with work to do wakes a listener, and the listener's
// channel is closed when its context ends.
func TestACommitOfWorkWakesAListener(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	notifying := New(e.db.Owner, WithClock(e.clock), WithNotify())
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	wake, err := NewListener(e.db.Owner).Listen(ctx)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}

	id := ident("w1", "u1")
	if _, err := notifying.PersistAndEnqueue(t.Context(), nil, id, notif(id, "k1"), planOf(3, immediate("email"))); err != nil {
		t.Fatal(err)
	}
	recvWake(t, wake, "the commit's wake")

	cancel()
	select {
	case _, ok := <-wake:
		if ok {
			// A signal that was already waiting; the close follows it.
			select {
			case _, ok = <-wake:
			case <-time.After(wakeDeadline):
				t.Fatal("timed out waiting for the channel to close")
			}
		}
		if ok {
			t.Fatal("the channel delivered a value after its context ended")
		}
	case <-time.After(wakeDeadline):
		t.Fatal("timed out waiting for the channel to close")
	}
}

// Signals coalesce: many commits before the listener looks are one wake, because the
// wake says "look" and carries nothing to count.
func TestSeveralCommitsAreOneWakeNotABacklog(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	notifying := New(e.db.Owner, WithClock(e.clock), WithNotify())
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	wake, err := NewListener(e.db.Owner).Listen(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"k1", "k2", "k3"} {
		id := ident("w1", "u-"+key)
		if _, err := notifying.PersistAndEnqueue(t.Context(), nil, id, notif(id, key), planOf(3, immediate("email"))); err != nil {
			t.Fatal(err)
		}
	}
	recvWake(t, wake, "a wake")
	// What is left over is at most the one more the buffer could hold; never three.
	extra := 0
	for {
		select {
		case <-wake:
			extra++
			continue
		case <-time.After(200 * time.Millisecond):
		}
		break
	}
	if extra > 1 {
		t.Fatalf("%d further wakes for three commits, want them coalesced", extra)
	}
}

// Nothing announces work that is not committed, or that was not asked to be
// announced. Read from a bare LISTEN session, so each notification is seen on its own
// and nothing coalesces: a marker sent last is the first thing it hears if, and only
// if, nothing before it was announced.
func TestNothingIsAnnouncedUnlessCommittedAndAskedFor(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	conn, err := e.db.Owner.Acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if _, err := conn.Exec(t.Context(), "LISTEN "+dispatchChannel); err != nil {
		t.Fatal(err)
	}
	quiet := New(e.db.Owner, WithClock(e.clock))
	loud := New(e.db.Owner, WithClock(e.clock), WithNotify())

	next := func(what string) *string {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), wakeDeadline)
		defer cancel()
		n, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			t.Fatalf("waiting for %s: %v", what, err)
		}
		return &n.Payload
	}
	marker := func(label string) {
		t.Helper()
		if _, err := e.db.Admin.Exec(t.Context(), `SELECT pg_notify($1, $2)`, dispatchChannel, label); err != nil {
			t.Fatal(err)
		}
	}

	a, b, c, d := ident("w1", "ua"), ident("w1", "ub"), ident("w1", "uc"), ident("w1", "ud")

	// A store not asked to announce does not.
	if _, err := quiet.PersistAndEnqueue(t.Context(), nil, a, notif(a, "k"), planOf(3, immediate("email"))); err != nil {
		t.Fatal(err)
	}
	// A notification with no delivery to make has nothing to announce.
	if _, err := loud.PersistAndEnqueue(t.Context(), nil, b, notif(b, "k"), planOf(3)); err != nil {
		t.Fatal(err)
	}
	// A caller's transaction that rolls back announces nothing: the NOTIFY is part of it.
	tx, err := e.db.Owner.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := loud.PersistAndEnqueue(t.Context(), tx, c, notif(c, "k"), planOf(3, immediate("email"))); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	marker("first")
	if got := next("the first marker"); *got != "first" {
		t.Fatalf("heard %q before the marker: something announced work it should not have", *got)
	}

	// And a store asked to announce does, with no payload, inside the caller's own
	// transaction too, once that commits.
	if _, err := loud.PersistAndEnqueue(t.Context(), nil, d, notif(d, "k"), planOf(3, immediate("email"))); err != nil {
		t.Fatal(err)
	}
	if got := next("the commit's notification"); *got != "" {
		t.Fatalf("heard %q, want a notification with no payload", *got)
	}
	tx, err = e.db.Owner.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := loud.PersistAndEnqueue(t.Context(), tx, a, notif(a, "k2"), planOf(3, immediate("sms"))); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := next("the caller's commit"); *got != "" {
		t.Fatalf("heard %q, want a notification with no payload", *got)
	}
}

// The listener holds a connection of its own and gives it back for good when it ends:
// a connection returned to the pool still LISTENing would be handed to the next caller
// with a registration nobody reads.
func TestAListenerClosesItsConnectionWhenItEnds(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	listening := func() int {
		// The server is shared by every test of the package, each in a database of its own.
		return e.count(`SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()
		                 AND query LIKE 'LISTEN ` + dispatchChannel + `%' AND pid <> pg_backend_pid()`)
	}
	ctx, cancel := context.WithCancel(t.Context())
	wake, err := NewListener(e.db.Owner).Listen(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n := listening(); n != 1 {
		t.Fatalf("%d sessions listening, want the one", n)
	}
	cancel()
	select {
	case <-wake:
	case <-time.After(wakeDeadline):
		t.Fatal("timed out waiting for the channel to close")
	}
	deadline := time.Now().Add(wakeDeadline)
	for listening() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%d sessions still listening after the listener ended", listening())
		}
		time.Sleep(20 * time.Millisecond)
	}
}
