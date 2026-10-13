//go:build integration

package redis_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/truongpx396/intel-notification/adapters/driven/redis"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/notifytest"
	"github.com/truongpx396/intel-notification/internal/redistest"
)

func identity(realm domain.Realm, tenant, recipient string) domain.Identity {
	return domain.Identity{
		Realm:     realm,
		Tenant:    domain.Tenant{Kind: "workspace", ID: tenant},
		Recipient: domain.Recipient{Kind: "user", ID: recipient},
	}
}

// Three identities that differ in one component each, with the stream key and hash
// slot of each, computed with Python's hashlib over the canonical encoding in
// data-model.md and the CRC16 above, not by this repository's code.
var streams = []struct {
	name string
	id   domain.Identity
	key  string
	slot int
}{
	{"workspace w1, user u1", identity("aisat", "w1", "u1"),
		"notify:inbox:{f0072fa3f1ee37a2e7c239330a27dc874f5d80b52732d40e27fcc3eae00dae7c}", 1522},
	{"workspace w2, the same user id u1", identity("aisat", "w2", "u1"),
		"notify:inbox:{abd15b04d221127f0d182760f126c4f1fa873bd7565c6b7c2b7232f2f23154ca}", 740},
	{"workspace w1, another user u2", identity("aisat", "w1", "u2"),
		"notify:inbox:{67608aaa44e9d8fbd21447ace746d27d66dbe64f5b806ffd68c08a0ab4f41d97}", 5903},
}

const wait = 10 * time.Second

func newStream(t *testing.T) (*redis.Stream, *goredis.Client) {
	t.Helper()
	db := redistest.New(t)
	return redis.NewStream(db.Client), db.Client
}

// ownRealm is a realm no other test uses. Redis keeps pub/sub outside the key
// space: a channel is shared by every database on the server, so redistest's
// private database does not keep one test from hearing another's nudges. A test
// that publishes does it as an identity of its own, derived from its database.
func ownRealm(t *testing.T, rdb *goredis.Client) domain.Realm {
	t.Helper()
	return domain.Realm(fmt.Sprintf("t%d", rdb.Options().DB))
}

// rawSubscribe listens on one literal shard channel through plain go-redis, with
// the subscription confirmed before it returns, so what it hears is what was
// published to that exact name and nothing the stream chose for it.
func rawSubscribe(t *testing.T, rdb *goredis.Client, channel string) <-chan *goredis.Message {
	t.Helper()
	ps := rdb.SSubscribe(t.Context(), channel)
	t.Cleanup(func() { _ = ps.Close() })
	if _, err := ps.Receive(t.Context()); err != nil {
		t.Fatalf("subscribe %s: %v", channel, err)
	}
	return ps.Channel()
}

func nextMessage(t *testing.T, ch <-chan *goredis.Message) *goredis.Message {
	t.Helper()
	select {
	case m, ok := <-ch:
		if !ok {
			t.Fatal("the subscription closed before a message arrived")
		}
		return m
	case <-time.After(wait):
		t.Fatalf("no message within %v", wait)
		return nil
	}
}

// The Redis stream runs the table the in-memory stream runs (notifytest.StreamContract), so what a
// relay's unit test sees of the live path is what Redis gives it: isolation between identities, order,
// no replay, Close and context semantics, and the refusal of a nudge of nothing.
func TestStreamRunsTheStreamContract(t *testing.T) {
	t.Parallel()
	notifytest.StreamContract(t, func(t *testing.T) notifytest.LiveStream {
		t.Helper()
		s, _ := newStream(t)
		return s
	})
}

// The keys are the frozen ones, which are literal names every other test must stay
// off (ownRealm), so this is one sequence with the server to itself.
//
// First, what Redis holds: the channel a subscription made is read back from the
// server and not from the code that named it, and its slot is the slot of its hash
// tag, so sharded pub/sub keeps a recipient's stream on one node. Then what is
// published: a nudge arrives on the identity's key and no other, whichever
// component the others differ in, and carries the notification id and nothing else
// (D16, NR-024).
func TestStreamUsesTheFrozenKeys(t *testing.T) {
	t.Parallel()
	s, rdb := newStream(t)

	for _, st := range streams {
		sub, err := s.Subscribe(t.Context(), st.id)
		if err != nil {
			t.Fatalf("%s: Subscribe: %v", st.name, err)
		}
		channels, err := rdb.PubSubShardChannels(t.Context(), st.key).Result()
		if err != nil {
			t.Fatalf("PUBSUB SHARDCHANNELS: %v", err)
		}
		if len(channels) != 1 || channels[0] != st.key {
			t.Fatalf("%s: Redis holds %q, want exactly %q", st.name, channels, st.key)
		}
		if got := slot(channels[0]); got != st.slot {
			t.Fatalf("%s: the key is on slot %d, want %d, the slot of its hash tag: the braces were dropped", st.name, got, st.slot)
		}
		if err := sub.Close(); err != nil {
			t.Fatalf("%s: Close: %v", st.name, err)
		}
	}

	heard := map[string]<-chan *goredis.Message{}
	for _, st := range streams {
		heard[st.key] = rawSubscribe(t, rdb, st.key)
	}
	for _, pub := range streams {
		if err := s.Publish(t.Context(), pub.id, "n-1"); err != nil {
			t.Fatalf("%s: Publish: %v", pub.name, err)
		}
		if m := nextMessage(t, heard[pub.key]); m.Channel != pub.key || m.Payload != "n-1" {
			t.Fatalf("%s: heard %q on %q, want exactly the id n-1 on %q", pub.name, m.Payload, m.Channel, pub.key)
		}
		// A misrouted copy would be a listener's first message, ahead of its sentinel.
		for _, other := range streams {
			if other.key == pub.key {
				continue
			}
			if err := rdb.SPublish(t.Context(), other.key, "sentinel").Err(); err != nil {
				t.Fatalf("SPUBLISH sentinel: %v", err)
			}
			if m := nextMessage(t, heard[other.key]); m.Payload != "sentinel" {
				t.Fatalf("%s heard %q, the nudge published for %s", other.name, m.Payload, pub.name)
			}
		}
	}
}

// A subscription ends when its context ends or it is closed, and leaves nothing
// subscribed on the server.
func TestSubscriptionEnds(t *testing.T) {
	t.Parallel()
	numSub := func(t *testing.T, rdb *goredis.Client, key string) int64 {
		t.Helper()
		n, err := rdb.PubSubShardNumSub(t.Context(), key).Result()
		if err != nil {
			t.Fatalf("PUBSUB SHARDNUMSUB: %v", err)
		}
		return n[key]
	}
	drained := func(t *testing.T, ids <-chan string) {
		t.Helper()
		select {
		case _, ok := <-ids:
			if ok {
				t.Fatal("a nudge arrived on an ended subscription")
			}
		case <-time.After(wait):
			t.Fatal("IDs() still open after the subscription ended")
		}
	}

	t.Run("the context ends", func(t *testing.T) {
		t.Parallel()
		s, rdb := newStream(t)
		id := identity(ownRealm(t, rdb), "w1", "u1")
		ctx, cancel := context.WithCancel(t.Context())
		sub, err := s.Subscribe(ctx, id)
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		if got := numSub(t, rdb, domain.StreamKey(id)); got != 1 {
			t.Fatalf("subscribers on the key = %d, want 1", got)
		}
		cancel()
		drained(t, sub.IDs())
	})
	t.Run("Close is called, twice", func(t *testing.T) {
		t.Parallel()
		s, rdb := newStream(t)
		id := identity(ownRealm(t, rdb), "w1", "u1")
		sub, err := s.Subscribe(t.Context(), id)
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		if err := sub.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if err := sub.Close(); err != nil {
			t.Fatalf("the second Close = %v, want nil", err)
		}
		select {
		case _, ok := <-sub.IDs():
			if ok {
				t.Fatal("a nudge arrived after Close")
			}
		default:
			t.Fatal("IDs() was still open when Close returned: Close waits for the pump to stop")
		}
		if got := numSub(t, rdb, domain.StreamKey(id)); got != 0 {
			t.Fatalf("subscribers on the key after Close = %d, want 0: Close returns when the server has dropped it", got)
		}
	})
}

// A fault no retry fixes is permanent, so the in-app channel rejects it at once;
// every other fault is the transient kind (D10, D12).
func TestPublishClassifiesWhatARetryCannotFix(t *testing.T) {
	t.Parallel()
	t.Run("the server refuses the command for this user", func(t *testing.T) {
		t.Parallel()
		db := redistest.New(t)
		name := fmt.Sprintf("nopublish-%d", db.Index)
		if err := db.Client.Do(t.Context(), "ACL", "SETUSER", name, "on", ">pw", "~*", "&*", "+@all", "-spublish").Err(); err != nil {
			t.Fatalf("ACL SETUSER: %v", err)
		}
		t.Cleanup(func() { _ = db.Client.Do(t.Context(), "ACL", "DELUSER", name).Err() })
		opts := *db.Client.Options()
		opts.Username, opts.Password = name, "pw"
		limited := goredis.NewClient(&opts)
		defer limited.Close()

		err := redis.NewStream(limited).Publish(t.Context(), identity(ownRealm(t, db.Client), "w1", "u1"), "n-1")
		if !errors.Is(err, domain.ErrPermanent) {
			t.Fatalf("Publish refused by the server = %v, want an error wrapping ErrPermanent", err)
		}
	})
	t.Run("the server refuses the subscription", func(t *testing.T) {
		t.Parallel()
		db := redistest.New(t)
		name := fmt.Sprintf("nosubscribe-%d", db.Index)
		if err := db.Client.Do(t.Context(), "ACL", "SETUSER", name, "on", ">pw", "~*", "&*", "+@all", "-ssubscribe").Err(); err != nil {
			t.Fatalf("ACL SETUSER: %v", err)
		}
		t.Cleanup(func() { _ = db.Client.Do(t.Context(), "ACL", "DELUSER", name).Err() })
		opts := *db.Client.Options()
		opts.Username, opts.Password = name, "pw"
		limited := goredis.NewClient(&opts)
		defer limited.Close()

		sub, err := redis.NewStream(limited).Subscribe(t.Context(), identity(ownRealm(t, db.Client), "w1", "u1"))
		if err == nil {
			_ = sub.Close()
			t.Fatal("Subscribe refused by the server succeeded: a relay would sit on a stream that never delivers")
		}
		if !errors.Is(err, domain.ErrPermanent) {
			t.Fatalf("Subscribe refused by the server = %v, want an error wrapping ErrPermanent", err)
		}
	})
	t.Run("the server cannot be reached", func(t *testing.T) {
		t.Parallel()
		db := redistest.New(t)
		s := redis.NewStream(db.Client)
		if err := db.Client.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		err := s.Publish(t.Context(), identity(ownRealm(t, db.Client), "w1", "u1"), "n-1")
		if err == nil || errors.Is(err, domain.ErrPermanent) {
			t.Fatalf("Publish on a closed client = %v, want a transient error: the server may be back for the retry", err)
		}
	})
}
