//go:build integration

package webhook_test

import (
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/truongpx396/intel-notification/adapters/driven/channel/webhook"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/notifytest"
)

func TestMain(m *testing.M) { os.Exit(notifytest.Main(m)) }

// withWebhook is an Env with the third channel registered. The registration is the one
// line NS-008 counts: nothing in fan-out, persistence or preferences is told of it.
func withWebhook(t *testing.T) *notifytest.Env {
	t.Helper()
	e := notifytest.NewEnv(t)
	wh, err := webhook.New(secret, webhook.WithAllowHTTP(), webhook.WithAllowPrivateNetworks())
	if err != nil {
		t.Fatal(err)
	}
	e.Channels.Register(wh) // <- the registration line
	return e
}

// T033a, NS-008, NR-016. The two suites that hold the engine to its guarantees are run
// unchanged with a third channel registered, and pass: a registered channel that the
// topics in them do not name changes nothing, and the registration changed no code in
// app/, in adapters/driven/postgres or in preferences. The evidence that it is one file
// and one line is this change's `git diff --stat`.
func TestTheContractsPassWithAThirdChannelRegistered(t *testing.T) {
	t.Parallel()
	t.Run("NotifierContract", func(t *testing.T) {
		t.Parallel()
		notifytest.NotifierContract(t, withWebhook)
	})
	t.Run("DispatcherContract", func(t *testing.T) {
		t.Parallel()
		notifytest.DispatcherContract(t, withWebhook)
	})
}

// And a topic that names it is delivered through the whole engine: planned beside the
// others, queued, claimed, bound to the URL the address book holds, rendered, sent
// signed with its own delivery key, and recorded, with no code that has heard of
// "webhook" between Notify and the endpoint.
func TestAThirdChannelIsDeliveredThroughTheWholeEnginePath(t *testing.T) {
	t.Parallel()
	e := withWebhook(t)
	probe := notifytest.NewProbe()
	srv := newReceiver(t, probe)
	id := domain.Identity{Realm: notifytest.Realm, Tenant: domain.Tenant{Kind: "workspace", ID: "w1"}, Recipient: domain.Recipient{Kind: "user", ID: "u1"}}
	e.AddChannel("in_app", domain.ChannelCapabilities{InboxBacked: true, Dedup: domain.DedupLocal})
	e.AddChannel("email", domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupProvider, DedupWindow: time.Hour})
	e.Addresses.Put(id, "email", domain.Address{Channel: "email", Value: "u1@example.com"})
	e.Addresses.Put(id, "webhook", domain.Address{Channel: "webhook", Value: strings.Replace(srv.URL, "http://", "HTTP://", 1) + "/in?token=s3cret"})
	e.Topics.Register(notifytest.Realm, "ingestion_complete", domain.TopicDef{
		DefaultChannels: []domain.ChannelKind{"in_app", "email", "webhook"}, DefaultPriority: domain.PriorityInfo})
	e.Templates.Put(notifytest.Realm, "ingestion_complete", "webhook", "en",
		domain.RenderedContent{Subject: "Upload done", Body: "Your upload finished.", Data: map[string]any{"files": 3}})

	n := domain.Notification{Tenant: id.Tenant, Recipient: id.Recipient, Topic: "ingestion_complete", IdemKey: "k1", Title: "Done"}
	receipt, err := e.Notifier.Notify(t.Context(), n)
	if err != nil || !receipt.Applied || len(receipt.Deliveries) != 3 {
		t.Fatalf("Notify = %+v, %v, want three channels planned", receipt, err)
	}
	if handled, err := e.Dispatcher.Drain(t.Context(), domain.ShardFor(id, e.Config.WithDefaults().Shards), 100); err != nil || handled != 3 {
		t.Fatalf("Drain = %d, %v, want the three deliveries", handled, err)
	}

	reqs := srv.got()
	if len(reqs) != 1 {
		t.Fatalf("the endpoint got %d requests, want one", len(reqs))
	}
	normalized := newChannel(t).NormalizeAddress(srv.URL + "/in?token=s3cret")
	wantKey := domain.DeliveryIdemKey(id, "k1", "", "webhook", domain.AddressKey("webhook", normalized))
	if got := reqs[0].Header.Get("Idempotency-Key"); got != wantKey {
		t.Fatalf("Idempotency-Key = %s, want %s", got, wantKey)
	}
	if reqs[0].RawQuery != "token=s3cret" || reqs[0].Path != "/in" {
		t.Fatalf("the endpoint was called at %s?%s, want the address as the book holds it", reqs[0].Path, reqs[0].RawQuery)
	}

	status, err := e.Notifier.Status(t.Context(), domain.StatusRequest{Tenant: id.Tenant, Recipient: id.Recipient, IdemKey: "k1"})
	if err != nil || !status.Found {
		t.Fatalf("Status = %+v, %v", status, err)
	}
	delivered := map[domain.ChannelKind]bool{}
	for _, d := range status.Deliveries {
		if d.State == "delivered" {
			delivered[d.Channel] = true
		}
	}
	if !delivered["in_app"] || !delivered["email"] || !delivered["webhook"] {
		t.Fatalf("status = %+v, want all three channels delivered", status.Deliveries)
	}
}

// The endpoint's answers drive the engine as any channel's do: a 503 is retried, and a
// 410 suppresses the address, so the next notification is never sent to it.
func TestAThirdChannelsAnswersDriveTheEngine(t *testing.T) {
	t.Parallel()
	e := withWebhook(t)
	srv := newReceiver(t, nil)
	id := domain.Identity{Realm: notifytest.Realm, Tenant: domain.Tenant{Kind: "workspace", ID: "w1"}, Recipient: domain.Recipient{Kind: "user", ID: "u1"}}
	// The address is spelled with an upper-case scheme. The channel defines address
	// equality, so it is one address however it is spelled: one delivery key, and one
	// suppression, written and checked under the channel's own normalization.
	e.Addresses.Put(id, "webhook", domain.Address{Channel: "webhook", Value: strings.Replace(srv.URL, "http://", "HTTP://", 1) + "/in"})
	e.Topics.Register(notifytest.Realm, "t", domain.TopicDef{DefaultChannels: []domain.ChannelKind{"webhook"}, DefaultPriority: domain.PriorityInfo})
	shard := domain.ShardFor(id, e.Config.WithDefaults().Shards)
	send := func(key string) {
		t.Helper()
		if r, err := e.Notifier.Notify(t.Context(), domain.Notification{Tenant: id.Tenant, Recipient: id.Recipient, Topic: "t", IdemKey: key, Title: "T"}); err != nil || !r.Applied {
			t.Fatalf("Notify(%s) = %+v, %v", key, r, err)
		}
	}
	count := func(sql string) int {
		t.Helper()
		var n int
		if err := e.DB.Admin.QueryRow(t.Context(), sql).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	send("k1")
	srv.answer(reply{status: http.StatusServiceUnavailable, header: map[string]string{"Retry-After": "3600"}})
	if _, err := e.Dispatcher.Drain(t.Context(), shard, 10); err != nil {
		t.Fatal(err)
	}
	var next time.Time
	if err := e.DB.Admin.QueryRow(t.Context(), `SELECT next_attempt_at FROM notification_outbox`).Scan(&next); err != nil {
		t.Fatalf("the delivery is not waiting for its retry: %v", err)
	}
	if want := notifytest.Epoch.Add(time.Hour); !next.Equal(want) {
		t.Fatalf("retry due %v, want %v: the endpoint's Retry-After is the floor", next, want)
	}

	if _, err := e.DB.Admin.Exec(t.Context(), `UPDATE notification_outbox SET next_attempt_at = now() - interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	srv.answer(reply{status: http.StatusGone})
	if _, err := e.Dispatcher.Drain(t.Context(), shard, 10); err != nil {
		t.Fatal(err)
	}
	if got := count(`SELECT count(*) FROM notification_deliveries WHERE outcome = 'suppressed'`); got != 1 {
		t.Fatalf("%d suppressed, want 1", got)
	}
	if got := count(`SELECT count(*) FROM channel_suppressions WHERE channel = 'webhook' AND expires_at IS NULL`); got != 1 {
		t.Fatalf("%d permanent webhook suppressions, want 1", got)
	}

	before := len(srv.got())
	send("k2")
	if _, err := e.Dispatcher.Drain(t.Context(), shard, 10); err != nil {
		t.Fatal(err)
	}
	if got := len(srv.got()); got != before {
		t.Fatalf("the endpoint got %d further requests, want none: its address was suppressed", got-before)
	}
	if got := count(`SELECT count(*) FROM notification_deliveries WHERE outcome = 'suppressed'`); got != 2 {
		t.Fatalf("%d suppressed, want the second notification suppressed too", got)
	}
}
