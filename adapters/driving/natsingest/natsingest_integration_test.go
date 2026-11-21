//go:build integration

package natsingest_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	natscontainer "github.com/testcontainers/testcontainers-go/modules/nats"

	"github.com/truongpx396/intel-notification/adapters/driven/postgres"
	"github.com/truongpx396/intel-notification/adapters/driving/natsingest"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/notifytest"
	"github.com/truongpx396/intel-notification/ports"
)

func TestMain(m *testing.M) { os.Exit(notifytest.Main(m)) }

// T052, NR-035, D23. The ingest over the real Notifier and a real PostgreSQL, on the Env's realm: a
// message the bus delivers more than once, whether because the commit failed or because the acknowledgement
// was lost, notifies once.

type ingestWorld struct {
	e      *notifytest.Env
	ingest *natsingest.Ingest
	log    *log
	pub    *fakePub
}

const ingestRealm = notifytest.Realm

func newIngestWorld(t *testing.T) *ingestWorld {
	t.Helper()
	e := notifytest.NewEnv(t)
	e.AddChannel("in_app", domain.ChannelCapabilities{InboxBacked: true, Dedup: domain.DedupLocal})
	e.Topics.Register(notifytest.Realm, "invoice_overdue", domain.TopicDef{DefaultChannels: []domain.ChannelKind{"in_app"}, DefaultPriority: domain.PriorityInfo})
	w := &ingestWorld{e: e, log: &log{}}
	w.pub = &fakePub{log: w.log}
	w.ingest = natsingest.New(map[domain.Realm]ports.Notifier{ingestRealm: e.Notifier}, postgres.New(e.DB.Owner), w.pub,
		natsingest.WithMaxDeliver(5), natsingest.WithRetryDelay(time.Millisecond))
	return w
}

func (w *ingestWorld) count(t *testing.T, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := w.e.DB.Admin.QueryRow(t.Context(), sql, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// redeliver is the bus: it hands the message to the ingest again until the message is settled, as JetStream
// does for one that is not acknowledged, counting the deliveries.
func (w *ingestWorld) redeliver(t *testing.T, m *fakeMsg) int {
	t.Helper()
	for m.delivered = 1; m.delivered <= 8; m.delivered++ {
		w.ingest.Handle(t.Context(), m)
		if m.settled != "" {
			return int(m.delivered)
		}
	}
	t.Fatalf("the message was not settled in 8 deliveries: %v", w.log.all())
	return 0
}

func (w *ingestWorld) message(key string) *fakeMsg {
	m := &fakeMsg{log: w.log, subject: subjectFor(ingestRealm, tenant), seq: 1, data: body(func(p map[string]any) { p["idem_key"] = key })}
	return m
}

// NR-035: a kill after the message was handled and before the commit is a redelivery, with no second
// notification. The commit fails here, as a process dying inside the transaction leaves it: nothing was
// written, and the message is redelivered.
func TestAKillBeforeTheCommitIsARedeliveryAndNotifiesOnce(t *testing.T) {
	t.Parallel()
	w := newIngestWorld(t)
	w.e.Store.FailNextCommit()
	m := w.message("invoice:42:overdue")
	if deliveries := w.redeliver(t, m); deliveries != 2 || m.settled != "ack" {
		t.Fatalf("settled %q after %d deliveries, want an acknowledgement on the second: the first commit failed", m.settled, deliveries)
	}
	if n := w.count(t, `SELECT count(*) FROM notifications WHERE realm = $1`, string(ingestRealm)); n != 1 {
		t.Fatalf("%d notifications, want 1", n)
	}
	if n := w.count(t, `SELECT count(*) FROM dead_letters`); n != 0 {
		t.Fatalf("%d dead letters, want none: a failed commit is a retry, not poison", n)
	}
}

// NR-035: a kill after the commit and before the acknowledgement is a redelivery that Notify replays: the
// message is acknowledged on the next delivery, and there is still one notification.
func TestALostAcknowledgementIsAReplayAndNotifiesOnce(t *testing.T) {
	t.Parallel()
	w := newIngestWorld(t)
	m := w.message("invoice:42:overdue")
	m.ackFailures = 2 // the acknowledgement is lost twice
	if deliveries := w.redeliver(t, m); deliveries != 3 || m.settled != "ack" {
		t.Fatalf("settled %q after %d deliveries, want an acknowledgement on the third", m.settled, deliveries)
	}
	if n := w.count(t, `SELECT count(*) FROM notifications WHERE realm = $1`, string(ingestRealm)); n != 1 {
		t.Fatalf("%d notifications after three deliveries, want 1: Notify replays the key", n)
	}
	if n := w.count(t, `SELECT count(*) FROM notification_outbox`); n != 1 {
		t.Fatalf("%d queue rows after three deliveries, want 1", n)
	}
}

// bus-subjects.md: a message without idem_key reaches dead_letters as poison, and the stream goes on.
func TestAMessageWithoutAnIdemKeyReachesDeadLettersAsPoisonAndTheStreamGoesOn(t *testing.T) {
	t.Parallel()
	w := newIngestWorld(t)
	bad := &fakeMsg{log: w.log, subject: subjectFor(ingestRealm, tenant), seq: 1, data: body(func(p map[string]any) { delete(p, "idem_key") })}
	if w.redeliver(t, bad); bad.settled != "term" {
		t.Fatalf("poison settled %q, want it terminated", bad.settled)
	}
	good := w.message("invoice:43:overdue")
	good.seq = 2
	if w.redeliver(t, good); good.settled != "ack" {
		t.Fatalf("the message behind the poison one settled %q, want it delivered", good.settled)
	}
	var source, reason string
	if err := w.e.DB.Admin.QueryRow(t.Context(), `SELECT source, reason FROM dead_letters`).Scan(&source, &reason); err != nil || source != "bus" || reason != "poison" {
		t.Fatalf("dead letter = %q %q, %v, want a bus poison", source, reason, err)
	}
	if n := w.count(t, `SELECT count(*) FROM notifications WHERE realm = $1`, string(ingestRealm)); n != 1 {
		t.Fatalf("%d notifications, want only the good one: poison is never delivered unguarded", n)
	}
}

// The same, over a real JetStream: messages published to a stream, consumed through a durable consumer, a
// poison one told on the DLQ subject, and a notification whose acknowledgement was lost redelivered by the
// server, and notified once.
func TestOverARealJetStreamAMessageDeliveredTwiceNotifiesOnceAndPoisonDoesNotBlock(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
	defer cancel()
	ctr, err := natscontainer.Run(ctx, "nats:2.11-alpine")
	if err != nil {
		t.Fatalf("start NATS: %v", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })
	url, err := ctr.ConnectionString(ctx)
	if err != nil {
		t.Fatal(err)
	}
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "NOTIFY", Subjects: []string{"notify.ingest.>"}, Replicas: 1}); err != nil {
		t.Fatalf("create the stream: %v", err)
	}
	consumer, err := js.CreateOrUpdateConsumer(ctx, "NOTIFY", jetstream.ConsumerConfig{
		Durable: "notify-" + string(ingestRealm), FilterSubject: natsingest.SubjectPrefix + "." + string(ingestRealm) + ".*",
		AckPolicy: jetstream.AckExplicitPolicy, MaxDeliver: 5, AckWait: 2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	dlq, err := js.CreateOrUpdateConsumer(ctx, "NOTIFY", jetstream.ConsumerConfig{FilterSubject: natsingest.DLQPrefix + ".*", AckPolicy: jetstream.AckNonePolicy})
	if err != nil {
		t.Fatal(err)
	}

	w := newIngestWorld(t)
	subject := subjectFor(ingestRealm, tenant)
	for _, data := range [][]byte{
		body(func(p map[string]any) { p["idem_key"] = "k1" }),
		body(func(p map[string]any) { delete(p, "idem_key") }), // poison
		body(func(p map[string]any) { p["idem_key"] = "k2" }),
	} {
		if _, err := js.Publish(ctx, subject, data); err != nil {
			t.Fatal(err)
		}
	}

	// First process: it notifies and is then "killed" before it acknowledges anything (its context is
	// cancelled inside Notify, after the commit), as a process dying between the commit and the ack is.
	killed := make(chan struct{})
	first := natsingest.New(map[domain.Realm]ports.Notifier{ingestRealm: &killAfterCommit{Notifier: w.e.Notifier, kill: killed}},
		postgres.New(w.e.DB.Owner), js, natsingest.WithMaxDeliver(5), natsingest.WithRetryDelay(100*time.Millisecond))
	runCtx, killFirst := context.WithCancel(ctx)
	firstDone := make(chan error, 1)
	go func() { firstDone <- first.Run(runCtx, consumer) }()
	select {
	case <-killed:
	case <-time.After(30 * time.Second):
		t.Fatal("the first process did not handle a message")
	}
	killFirst()
	<-firstDone

	// Second process: the server redelivers what was never acknowledged (after AckWait), Notify replays, and
	// every message is settled.
	second := natsingest.New(map[domain.Realm]ports.Notifier{ingestRealm: w.e.Notifier}, postgres.New(w.e.DB.Owner), js,
		natsingest.WithMaxDeliver(5), natsingest.WithRetryDelay(100*time.Millisecond))
	runCtx2, stop := context.WithCancel(ctx)
	defer stop()
	secondDone := make(chan error, 1)
	go func() { secondDone <- second.Run(runCtx2, consumer) }()

	deadline := time.Now().Add(60 * time.Second)
	for {
		notified := w.count(t, `SELECT count(*) FROM notifications WHERE realm = $1`, string(ingestRealm))
		parked := w.count(t, `SELECT count(*) FROM dead_letters WHERE source = 'bus'`)
		if notified == 2 && parked == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("after 60s: %d notifications and %d parked, want 2 and 1", notified, parked)
		}
		time.Sleep(100 * time.Millisecond)
	}
	// Give any redelivery a chance to over-notify, then count again: it must still be two.
	time.Sleep(3 * time.Second)
	if n := w.count(t, `SELECT count(*) FROM notifications WHERE realm = $1`, string(ingestRealm)); n != 2 {
		t.Fatalf("%d notifications after redeliveries, want exactly 2 (k1 and k2, once each)", n)
	}
	// The poison is parked first and told after, so the DLQ message is waited for until a deadline and not for
	// a fixed time: a loaded machine told it late once, and a regression must fail and not hang.
	var told int
	for told == 0 && time.Now().Before(deadline.Add(60*time.Second)) {
		msgs, err := dlq.Fetch(1, jetstream.FetchMaxWait(2*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		for msg := range msgs.Messages() {
			told++
			if msg.Subject() != natsingest.DLQPrefix+"."+string(ingestRealm) {
				t.Errorf("the DLQ message is on %q, want the realm's DLQ subject", msg.Subject())
			}
		}
	}
	if told != 1 {
		t.Fatalf("%d messages on the DLQ subject, want 1", told)
	}
	stop()
	<-secondDone
}

// killAfterCommit is a Notifier that commits the notification and then reports the context cancelled, so the
// ingest settles nothing: the process died between the commit and the acknowledgement.
type killAfterCommit struct {
	ports.Notifier
	kill chan struct{}
	done bool
}

func (k *killAfterCommit) Notify(ctx context.Context, n domain.Notification) (domain.Receipt, error) {
	r, err := k.Notifier.Notify(ctx, n)
	if err == nil && !k.done {
		k.done = true
		close(k.kill)
		return domain.Receipt{}, context.Canceled
	}
	return r, err
}
