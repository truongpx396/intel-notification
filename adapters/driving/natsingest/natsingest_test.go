package natsingest_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/truongpx396/intel-notification/adapters/driving/natsingest"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/notifytest"
	"github.com/truongpx396/intel-notification/ports"
)

// T052, NR-035, NR-009, D23. A message is a Notify call that arrived on a stream, acknowledged only after it
// committed, in the realm of its subject, and never able to block the stream behind it.

// event is one thing that happened, in order, so a test can say what came before what.
type event struct{ what string }

type log struct {
	mu     sync.Mutex
	events []string
}

func (l *log) add(format string, args ...any) {
	l.mu.Lock()
	l.events = append(l.events, fmt.Sprintf(format, args...))
	l.mu.Unlock()
}

func (l *log) all() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.events...)
}

func (l *log) index(what string) int {
	for i, e := range l.all() {
		if e == what {
			return i
		}
	}
	return -1
}

// fakeMsg is a jetstream.Msg that records how it was settled.
type fakeMsg struct {
	jetstream.Msg
	log       *log
	subject   string
	data      []byte
	delivered uint64
	seq       uint64
	ackErr    error
	// ackFailures is how many acknowledgements are lost before one gets through: a process that dies, or a
	// network that drops, between the commit and the ack.
	ackFailures int
	settled     string // "ack" or "term" once the bus has been told the message is done
}

func (m *fakeMsg) Subject() string { return m.subject }
func (m *fakeMsg) Data() []byte    { return m.data }
func (m *fakeMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{NumDelivered: m.delivered, Stream: "NOTIFY", Sequence: jetstream.SequencePair{Stream: m.seq}}, nil
}
func (m *fakeMsg) Headers() nats.Header { return nil }
func (m *fakeMsg) Reply() string        { return "" }
func (m *fakeMsg) Ack() error           { return m.doubleAck() }
func (m *fakeMsg) DoubleAck(context.Context) error {
	return m.doubleAck()
}

func (m *fakeMsg) doubleAck() error {
	m.log.add("ack")
	if m.ackFailures > 0 {
		m.ackFailures--
		return errors.New("the acknowledgement was lost")
	}
	if m.ackErr == nil {
		m.settled = "ack"
	}
	return m.ackErr
}
func (m *fakeMsg) Nak() error                       { m.log.add("nak"); return nil }
func (m *fakeMsg) NakWithDelay(time.Duration) error { m.log.add("nak"); return nil }
func (m *fakeMsg) InProgress() error                { return nil }
func (m *fakeMsg) Term() error                      { m.log.add("term"); m.settled = "term"; return nil }
func (m *fakeMsg) TermWithReason(string) error      { m.log.add("term"); return nil }

type fakeSink struct {
	log  *log
	err  error
	mu   sync.Mutex
	rows []domain.PoisonMessage
}

func (s *fakeSink) RecordPoison(_ context.Context, m domain.PoisonMessage) error {
	if s.err != nil {
		return s.err
	}
	s.log.add("poison recorded")
	s.mu.Lock()
	s.rows = append(s.rows, m)
	s.mu.Unlock()
	return nil
}

type fakePub struct {
	log  *log
	err  error
	mu   sync.Mutex
	sent map[string][]byte
}

func (p *fakePub) Publish(_ context.Context, subject string, payload []byte, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	if p.err != nil {
		return nil, p.err
	}
	p.log.add("dlq published")
	p.mu.Lock()
	if p.sent == nil {
		p.sent = map[string][]byte{}
	}
	p.sent[subject] = payload
	p.mu.Unlock()
	return &jetstream.PubAck{}, nil
}

type world struct {
	log      *log
	alpha    *notifytest.ScriptedNotifier
	beta     *notifytest.ScriptedNotifier
	sink     *fakeSink
	pub      *fakePub
	ingest   *natsingest.Ingest
	faults   []error
	faultsMu sync.Mutex
}

func newWorld(t *testing.T, opts ...natsingest.Option) *world {
	t.Helper()
	w := &world{log: &log{}, alpha: &notifytest.ScriptedNotifier{}, beta: &notifytest.ScriptedNotifier{}}
	w.sink, w.pub = &fakeSink{log: w.log}, &fakePub{log: w.log}
	w.alpha.OnNotify = func(_ context.Context, n domain.Notification) (domain.Receipt, error) {
		w.log.add("notified alpha %s", n.IdemKey)
		return domain.Receipt{NotificationID: "n1", Applied: true}, nil
	}
	w.beta.OnNotify = func(_ context.Context, n domain.Notification) (domain.Receipt, error) {
		w.log.add("notified beta %s", n.IdemKey)
		return domain.Receipt{NotificationID: "n1", Applied: true}, nil
	}
	opts = append([]natsingest.Option{natsingest.WithErrorHandler(func(e error) {
		w.faultsMu.Lock()
		w.faults = append(w.faults, e)
		w.faultsMu.Unlock()
	})}, opts...)
	w.ingest = natsingest.New(map[domain.Realm]ports.Notifier{"alpha": w.alpha, "beta": w.beta}, w.sink, w.pub, opts...)
	return w
}

var tenant = domain.Tenant{Kind: "workspace", ID: "w1"}

func subjectFor(realm domain.Realm, t domain.Tenant) string {
	return natsingest.SubjectPrefix + "." + string(realm) + "." + domain.TenantToken(t)
}

func body(mutate func(map[string]any)) []byte {
	m := map[string]any{
		"tenant": map[string]string{"kind": "workspace", "id": "w1"}, "recipient": map[string]string{"kind": "user", "id": "u1"},
		"topic": "invoice_overdue", "idem_key": "invoice:42:overdue", "title": "T",
	}
	if mutate != nil {
		mutate(m)
	}
	out, _ := json.Marshal(m)
	return out
}

func (w *world) msg(subject string, data []byte) *fakeMsg {
	return &fakeMsg{log: w.log, subject: subject, data: data, delivered: 1, seq: 7}
}

func TestAMessageIsANotifyCallAcknowledgedAfterItReturns(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	w.ingest.Handle(t.Context(), w.msg(subjectFor("alpha", tenant), body(nil)))
	got := w.log.all()
	if len(got) != 2 || got[0] != "notified alpha invoice:42:overdue" || got[1] != "ack" {
		t.Fatalf("events = %v, want the notification, then the acknowledgement: the ack is the commit's (NR-035)", got)
	}
}

// The acknowledgement is not sent while the commit has not happened.
func TestTheAcknowledgementWaitsForTheCommit(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	entered, release := make(chan struct{}), make(chan struct{})
	w.alpha.OnNotify = func(context.Context, domain.Notification) (domain.Receipt, error) {
		close(entered)
		<-release // the transaction has not committed
		return domain.Receipt{Applied: true}, nil
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.ingest.Handle(t.Context(), w.msg(subjectFor("alpha", tenant), body(nil)))
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the notifier was not called")
	}
	if got := w.log.all(); len(got) != 0 {
		t.Fatalf("events = %v while Notify had not returned, want none: acknowledging before the commit loses the notification if the commit fails", got)
	}
	close(release)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Handle did not return")
	}
	if got := w.log.all(); len(got) != 1 || got[0] != "ack" {
		t.Fatalf("events = %v, want the acknowledgement once Notify returned", got)
	}
}

// A replay, which is what a redelivery after a lost acknowledgement is, is acknowledged like a success.
func TestAReplayIsAcknowledgedLikeASuccess(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	w.alpha.OnNotify = func(context.Context, domain.Notification) (domain.Receipt, error) {
		return domain.Receipt{Applied: false}, nil
	}
	w.ingest.Handle(t.Context(), w.msg(subjectFor("alpha", tenant), body(nil)))
	if got := w.log.all(); len(got) != 1 || got[0] != "ack" {
		t.Fatalf("events = %v, want an acknowledgement: nothing is left to do for a message already applied", got)
	}
}

// NR-009. The realm is the subject's; a realm in the body is not read.
func TestTheRealmIsTheSubjectsAndNeverTheBodys(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	w.ingest.Handle(t.Context(), w.msg(subjectFor("alpha", tenant), body(func(m map[string]any) { m["realm"] = "beta" })))
	if n := len(w.beta.Notified()); n != 0 {
		t.Fatalf("realm beta's engine was asked %d times, want none: the body's realm is not the subject's", n)
	}
	if n := len(w.alpha.Notified()); n != 1 {
		t.Fatalf("realm alpha's engine was asked %d times, want once", n)
	}
}

func TestEveryFieldOfThePayloadReachesTheNotification(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	after, before, at := time.Date(2031, 3, 1, 9, 0, 0, 0, time.UTC), time.Date(2031, 3, 1, 10, 0, 0, 0, time.UTC), time.Date(2031, 3, 1, 8, 0, 0, 0, time.UTC)
	w.ingest.Handle(t.Context(), w.msg(subjectFor("alpha", tenant), body(func(m map[string]any) {
		m["priority"], m["body"] = "critical", "B"
		m["data"] = map[string]any{"number": "INV-42", "n": 2}
		m["payload"] = map[string]string{"doc": "d1"}
		m["attributes"] = map[string]string{"trace": "t1"}
		m["occurred_at"], m["deliver_after"], m["deliver_before"] = at, after, before
	})))
	got := w.alpha.Notified()
	if len(got) != 1 {
		t.Fatalf("%d notifications, want 1", len(got))
	}
	n := got[0]
	if n.Tenant != tenant || n.Recipient != (domain.Recipient{Kind: "user", ID: "u1"}) || n.Topic != "invoice_overdue" || n.Priority != domain.PriorityCritical ||
		n.Title != "T" || n.Body != "B" || n.IdemKey != "invoice:42:overdue" || n.Data["number"] != "INV-42" || n.Payload["doc"] != "d1" ||
		n.Attributes["trace"] != "t1" || !n.OccurredAt.Equal(at) || !n.DeliverAfter.Equal(after) || !n.DeliverBefore.Equal(before) {
		t.Fatalf("notification = %+v, want every field of the payload", n)
	}
}

// bus-subjects.md: a message that fails validation, or has no idem_key, is poison, and does not block the
// stream. It is parked, told to operators, and terminated: never redelivered, never delivered unguarded.
func TestAMessageThatCanNeverSucceedIsParkedAndTerminated(t *testing.T) {
	t.Parallel()
	good := subjectFor("alpha", tenant)
	cases := []struct {
		name    string
		subject string
		data    []byte
		notify  func(context.Context, domain.Notification) (domain.Receipt, error)
		reason  string
	}{
		{"no idem_key", good, body(func(m map[string]any) { delete(m, "idem_key") }), nil, "idem_key"},
		{"an empty idem_key", good, body(func(m map[string]any) { m["idem_key"] = "" }), nil, "idem_key"},
		{"not JSON", good, []byte("not json"), nil, "JSON"},
		{"a JSON value that is not an object", good, []byte(`["a"]`), nil, "JSON"},
		{"no tenant", good, body(func(m map[string]any) { delete(m, "tenant") }), nil, "tenant"},
		{"a tenant with an empty id", good, body(func(m map[string]any) { m["tenant"] = map[string]string{"kind": "workspace", "id": ""} }), nil, "tenant"},
		{"no recipient", good, body(func(m map[string]any) { delete(m, "recipient") }), nil, "recipient"},
		{"a tenant the subject's token does not name", subjectFor("alpha", domain.Tenant{Kind: "workspace", ID: "someone-else"}), body(nil), nil, "tenant token"},
		{"a realm nothing serves", subjectFor("gamma", tenant), body(nil), nil, "realm"},
		{"a topic nobody registered", good, body(nil), func(context.Context, domain.Notification) (domain.Receipt, error) {
			return domain.Receipt{}, fmt.Errorf("notify: plan: %w", domain.ErrUnknownTopic)
		}, "topic"},
		{"a notification the engine refuses", good, body(nil), func(context.Context, domain.Notification) (domain.Receipt, error) {
			return domain.Receipt{}, &domain.ValidationError{Problems: []domain.Problem{{Field: "Priority", Message: "is not a priority"}}}
		}, "Priority"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			if tc.notify != nil {
				w.alpha.OnNotify = tc.notify
			}
			m := w.msg(tc.subject, tc.data)
			m.delivered = 3
			w.ingest.Handle(t.Context(), m)
			events := w.log.all()
			if i, j, k := w.log.index("poison recorded"), w.log.index("dlq published"), w.log.index("term"); !(i >= 0 && j > i && k > j) {
				t.Fatalf("events = %v, want it recorded, then told to operators, then terminated: never terminated unrecorded", events)
			}
			if w.log.index("ack") >= 0 || w.log.index("nak") >= 0 {
				t.Fatalf("events = %v, want no acknowledgement and no redelivery of poison", events)
			}
			if tc.notify == nil && len(w.alpha.Notified()) != 0 {
				t.Fatalf("%d notifications reached the engine, want none: poison is never delivered unguarded", len(w.alpha.Notified()))
			}
			w.sink.mu.Lock()
			defer w.sink.mu.Unlock()
			if len(w.sink.rows) != 1 {
				t.Fatalf("%d dead letters, want 1", len(w.sink.rows))
			}
			row := w.sink.rows[0]
			if row.Attempts != 3 || row.Key != "NOTIFY:7" || row.Subject != tc.subject || string(row.Body) != string(tc.data) || !strings.Contains(row.Reason, tc.reason) {
				t.Fatalf("dead letter = %+v, want the attempts, a key from the stream and sequence, the subject and body as they arrived, and a reason naming %q", row, tc.reason)
			}
		})
	}
}

// The DLQ message names the realm of the subject, and holds what arrived.
func TestPoisonIsToldToOperatorsOnTheRealmsDLQSubject(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	w.ingest.Handle(t.Context(), w.msg(subjectFor("alpha", tenant), []byte("garbage")))
	w.pub.mu.Lock()
	defer w.pub.mu.Unlock()
	payload, ok := w.pub.sent["notify.ingest.dlq.alpha"]
	if !ok || len(w.pub.sent) != 1 {
		t.Fatalf("published to %v, want notify.ingest.dlq.alpha", w.pub.sent)
	}
	var dlq struct{ Subject, Reason, Body string }
	if err := json.Unmarshal(payload, &dlq); err != nil || dlq.Body != "garbage" || dlq.Subject != subjectFor("alpha", tenant) || dlq.Reason == "" {
		t.Fatalf("dlq message = %s, %v, want the subject, the reason and the body", payload, err)
	}
}

// A subject that names no realm cannot be parked (a dead letter has a realm) or announced (the DLQ subject is
// the realm's). A credential bound to a realm cannot have published it, so it is a mis-configuration: it is
// terminated, so it stops being redelivered, and reported to the operator through the error handler. "dlq" is
// not a realm an ingest serves: its subjects are where poison is told.
func TestAMessageOfNoRealmIsTerminatedAndReportedNotParked(t *testing.T) {
	t.Parallel()
	token := domain.TenantToken(tenant)
	for _, subject := range []string{
		"notify.ingest.Not_A_Realm." + token,
		"notify.ingest.alpha", "notify.ingest.alpha." + token + ".extra", "notify.ingest", "",
		"other.prefix.alpha." + token,
		"notify.ingest.dlq.alpha",
	} {
		t.Run(subject, func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			w.ingest.Handle(t.Context(), w.msg(subject, body(nil)))
			if got := w.log.all(); len(got) != 1 || got[0] != "term" {
				t.Fatalf("events = %v, want it terminated and nothing else", got)
			}
			w.pub.mu.Lock()
			published := len(w.pub.sent)
			w.pub.mu.Unlock()
			w.faultsMu.Lock()
			defer w.faultsMu.Unlock()
			if published != 0 || len(w.faults) != 1 {
				t.Fatalf("published %d, faults %v, want nothing published and the subject reported", published, w.faults)
			}
		})
	}
}

// A transient failure is a redelivery: the message is not acknowledged and not parked, and it does not
// stop the stream.
func TestATransientFailureIsRedeliveredNotAcknowledgedAndNotParked(t *testing.T) {
	t.Parallel()
	w := newWorld(t, natsingest.WithMaxDeliver(5))
	w.alpha.OnNotify = func(context.Context, domain.Notification) (domain.Receipt, error) {
		return domain.Receipt{}, errors.New("commit: connection reset by peer")
	}
	m := w.msg(subjectFor("alpha", tenant), body(nil))
	m.delivered = 2
	w.ingest.Handle(t.Context(), m)
	if got := w.log.all(); len(got) != 1 || got[0] != "nak" {
		t.Fatalf("events = %v, want a redelivery and nothing else: the commit failed, so the message is not done", got)
	}
}

// A message that fails transiently for ever is given up on at MaxDeliver, and is then poison.
func TestAMessageThatFailsAtEveryDeliveryIsPoisonAtMaxDeliver(t *testing.T) {
	t.Parallel()
	w := newWorld(t, natsingest.WithMaxDeliver(5))
	w.alpha.OnNotify = func(context.Context, domain.Notification) (domain.Receipt, error) {
		return domain.Receipt{}, errors.New("commit: connection reset by peer")
	}
	m := w.msg(subjectFor("alpha", tenant), body(nil))
	m.delivered = 5
	w.ingest.Handle(t.Context(), m)
	if i, j, k := w.log.index("poison recorded"), w.log.index("dlq published"), w.log.index("term"); !(i >= 0 && j > i && k > j) {
		t.Fatalf("events = %v, want it parked and terminated at the last delivery, so it stops being redelivered", w.log.all())
	}
	w.sink.mu.Lock()
	defer w.sink.mu.Unlock()
	if len(w.sink.rows) != 1 || w.sink.rows[0].Attempts != 5 || !strings.Contains(w.sink.rows[0].Reason, "5") {
		t.Fatalf("dead letter = %+v, want the 5 attempts named", w.sink.rows)
	}
}

// Nothing is terminated that was not recorded: a sink or a publisher that is down is a redelivery.
func TestAMessageIsNeverTerminatedUnrecorded(t *testing.T) {
	t.Parallel()
	for name, set := range map[string]func(*world){
		"the dead-letter table is down": func(w *world) { w.sink.err = errors.New("connection refused") },
		"the DLQ subject is down":       func(w *world) { w.pub.err = errors.New("no responders") },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			set(w)
			w.ingest.Handle(t.Context(), w.msg(subjectFor("alpha", tenant), []byte("garbage")))
			if w.log.index("term") >= 0 {
				t.Fatalf("events = %v: a message was terminated that nobody recorded", w.log.all())
			}
			if w.log.index("nak") < 0 {
				t.Fatalf("events = %v, want it redelivered, to try again", w.log.all())
			}
		})
	}
}

// A shutdown is not a failure: a message the process was in the middle of is neither acknowledged nor
// parked, and the bus redelivers it.
func TestAShutdownMidMessageSettlesNothing(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	ctx, cancel := context.WithCancel(t.Context())
	w.alpha.OnNotify = func(context.Context, domain.Notification) (domain.Receipt, error) {
		cancel()
		return domain.Receipt{}, context.Canceled
	}
	w.ingest.Handle(ctx, w.msg(subjectFor("alpha", tenant), body(nil)))
	if got := w.log.all(); len(got) != 0 {
		t.Fatalf("events = %v, want none: the bus redelivers what was not acknowledged", got)
	}
}

// A poison message does not stop the one behind it.
func TestAPoisonMessageDoesNotBlockTheNext(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	w.ingest.Handle(t.Context(), w.msg(subjectFor("alpha", tenant), []byte("garbage")))
	w.ingest.Handle(t.Context(), w.msg(subjectFor("alpha", tenant), body(nil)))
	if w.log.index("notified alpha invoice:42:overdue") < 0 {
		t.Fatalf("events = %v, want the message behind the poison one delivered", w.log.all())
	}
}

type fakeConsumer struct {
	msgs  []jetstream.Msg
	stops chan struct{}
}

type fakeConsume struct{ closed chan struct{} }

func (f fakeConsume) Stop()                   { close(f.closed) }
func (f fakeConsume) Drain()                  { close(f.closed) }
func (f fakeConsume) Closed() <-chan struct{} { return f.closed }

func (c *fakeConsumer) Consume(h jetstream.MessageHandler, _ ...jetstream.PullConsumeOpt) (jetstream.ConsumeContext, error) {
	for _, m := range c.msgs {
		h(m)
	}
	return fakeConsume{closed: c.stops}, nil
}

func TestRunConsumesUntilTheContextEnds(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	c := &fakeConsumer{msgs: []jetstream.Msg{w.msg(subjectFor("alpha", tenant), body(nil))}, stops: make(chan struct{})}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- w.ingest.Run(ctx, c) }()
	deadline := time.After(5 * time.Second)
	for w.log.index("ack") < 0 {
		select {
		case <-deadline:
			t.Fatal("the message was not handled")
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run = %v, want nil on a context that ended", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return when its context ended")
	}
	select {
	case <-c.stops:
	default:
		t.Fatal("the consumption was not stopped")
	}
}
