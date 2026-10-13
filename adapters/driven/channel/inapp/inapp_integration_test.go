//go:build integration

package inapp_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/truongpx396/intel-notification/adapters/driven/channel/inapp"
	"github.com/truongpx396/intel-notification/adapters/driven/redis"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/redistest"
)

func TestMain(m *testing.M) { os.Exit(redistest.Main(m)) }

func nextID(t *testing.T, sub *redis.Subscription) string {
	t.Helper()
	select {
	case id, ok := <-sub.IDs():
		if !ok {
			t.Fatal("the subscription closed before a nudge arrived")
		}
		return id
	case <-time.After(10 * time.Second):
		t.Fatal("no nudge within 10s")
		return ""
	}
}

// The channel and the stream, together, as the dispatcher and the relay meet them:
// the delivery's nudge reaches a subscriber of that identity, as the notification
// id and nothing else, and not the subscriber of the same recipient id in another
// tenant (D16, NS-001). The identities are the test's own: Redis pub/sub is shared
// by every database on the server.
func TestInAppNudgeReachesOnlyTheDeliverysSubscribers(t *testing.T) {
	t.Parallel()
	db := redistest.New(t)
	stream := redis.NewStream(db.Client)
	ch := inapp.New(stream)

	realm := domain.Realm(fmt.Sprintf("t%d", db.Index))
	own := domain.Identity{Realm: realm, Tenant: domain.Tenant{Kind: "workspace", ID: "w1"}, Recipient: domain.Recipient{Kind: "user", ID: "u1"}}
	other := own
	other.Tenant.ID = "w2"

	a, err := stream.Subscribe(t.Context(), own)
	if err != nil {
		t.Fatalf("Subscribe own: %v", err)
	}
	defer a.Close()
	b, err := stream.Subscribe(t.Context(), other)
	if err != nil {
		t.Fatalf("Subscribe other: %v", err)
	}
	defer b.Close()

	d := delivery()
	d.Identity = own
	if r, err := ch.Deliver(t.Context(), d); err != nil || r.Outcome != domain.Delivered {
		t.Fatalf("Deliver = %+v, %v, want Delivered", r, err)
	}
	if err := stream.Publish(t.Context(), other, "n-sentinel"); err != nil {
		t.Fatalf("Publish sentinel: %v", err)
	}
	if got := nextID(t, a); got != d.NotificationID {
		t.Fatalf("the recipient's subscriber heard %q, want exactly the id %q and no title, body or count", got, d.NotificationID)
	}
	if got := nextID(t, b); got != "n-sentinel" {
		t.Fatalf("the other tenant's subscriber heard %q first, want only its own sentinel: the nudge crossed the tenant boundary", got)
	}
}

// A Redis that refuses the command is a refusal no retry fixes, and the channel
// says so (Rejected), instead of leaving the dispatcher to burn every attempt.
func TestInAppRejectsWhatTheServerRefusesForGood(t *testing.T) {
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

	d := delivery()
	d.Identity.Realm = domain.Realm(fmt.Sprintf("t%d", db.Index))
	r, err := inapp.New(redis.NewStream(limited)).Deliver(t.Context(), d)
	if err != nil || r.Outcome != domain.Rejected || r.Detail == "" {
		t.Fatalf("Deliver = %+v, %v, want Rejected with the server's reason", r, err)
	}
}
