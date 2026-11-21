// Package natsingest is the optional ingest of producers that publish a notification to a bus instead of
// calling the service: it consumes NATS JetStream and calls Notifier.Notify (D23, NR-035,
// contracts/bus-subjects.md). The core has no bus port, and nothing here makes a rule of its own: a message
// is a Notify call that happens to arrive on a stream.
//
// Three rules carry it. A message is acknowledged only after Notify has returned, which is after
// PostgreSQL committed, so a crash anywhere before that is a redelivery and Notify's idempotency makes the
// redelivery harmless. The realm is the subject's and never the payload's (NR-009). And a message that can
// never succeed is parked as a dead letter and told to the operators on the DLQ subject, and never blocks
// the stream behind it.
package natsingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// Publisher is where a poison message is told to operators: jetstream.JetStream satisfies it.
type Publisher interface {
	Publish(ctx context.Context, subject string, payload []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error)
}

// Consumer is the pull consumer the ingest reads: jetstream.Consumer satisfies it.
type Consumer interface {
	Consume(handler jetstream.MessageHandler, opts ...jetstream.PullConsumeOpt) (jetstream.ConsumeContext, error)
}

// SubjectPrefix is the subject every ingest message is published under: notify.ingest.<realm>.<tenant_token>.
const SubjectPrefix = "notify.ingest"

// DLQPrefix is the subject poison is told on: notify.ingest.dlq.<realm>. Its realm token is "dlq", which is
// why no realm of that name is served by an ingest.
const DLQPrefix = SubjectPrefix + ".dlq"

const (
	defaultMaxDeliver = 10
	defaultRetryDelay = 5 * time.Second
	ackTimeout        = 5 * time.Second
)

// Ingest turns JetStream messages into Notify calls.
type Ingest struct {
	notifiers  map[domain.Realm]ports.Notifier
	sink       ports.PoisonSink
	pub        Publisher
	maxDeliver int
	retryDelay time.Duration
	onError    func(error)
}

// Option configures an Ingest.
type Option func(*Ingest)

// WithMaxDeliver sets how many deliveries a message that keeps failing transiently gets before it is given
// up on as poison. It should be the consumer's own MaxDeliver. The default is 10.
func WithMaxDeliver(n int) Option { return func(i *Ingest) { i.maxDeliver = n } }

// WithRetryDelay sets how long a failure waits before the bus redelivers the message. The default is 5s.
func WithRetryDelay(d time.Duration) Option { return func(i *Ingest) { i.retryDelay = d } }

// WithErrorHandler is given every fault of the ingest's own, for an operator's log.
func WithErrorHandler(h func(error)) Option { return func(i *Ingest) { i.onError = h } }

// New returns an Ingest that calls the Notifier of each realm, parks poison messages in sink and tells pub.
func New(notifiers map[domain.Realm]ports.Notifier, sink ports.PoisonSink, pub Publisher, opts ...Option) *Ingest {
	i := &Ingest{notifiers: notifiers, sink: sink, pub: pub, maxDeliver: defaultMaxDeliver, retryDelay: defaultRetryDelay, onError: func(error) {}}
	for _, opt := range opts {
		opt(i)
	}
	return i
}

// parseSubject is the realm and tenant token of notify.ingest.<realm>.<tenant_token>. A subject of any other
// shape names no realm. "dlq" is reserved for the subjects poison is told on.
func parseSubject(subject string) (realm domain.Realm, tenantToken string, ok bool) {
	parts := strings.Split(subject, ".")
	if len(parts) != 4 || parts[0]+"."+parts[1] != SubjectPrefix || parts[2] == "dlq" || parts[3] == "" {
		return "", "", false
	}
	if domain.Realm(parts[2]).Validate() != nil {
		return "", "", false
	}
	return domain.Realm(parts[2]), parts[3], true
}

// payload is a notification as a producer publishes it. A field this does not name, a realm included, is
// not read.
type payload struct {
	Tenant struct {
		Kind string `json:"kind"`
		ID   string `json:"id"`
	} `json:"tenant"`
	Recipient struct {
		Kind string `json:"kind"`
		ID   string `json:"id"`
	} `json:"recipient"`
	Topic         string            `json:"topic"`
	Priority      string            `json:"priority"`
	Data          map[string]any    `json:"data"`
	Title         string            `json:"title"`
	Body          string            `json:"body"`
	Payload       map[string]string `json:"payload"`
	IdemKey       string            `json:"idem_key"`
	OccurredAt    time.Time         `json:"occurred_at"`
	DeliverAfter  time.Time         `json:"deliver_after"`
	DeliverBefore time.Time         `json:"deliver_before"`
	Attributes    map[string]string `json:"attributes"`
}

// refusal is why a message can never succeed.
type refusal struct{ reason string }

func (r refusal) Error() string { return r.reason }

func refuse(format string, args ...any) error { return refusal{fmt.Sprintf(format, args...)} }

// decode turns a body into a notification, or says why it never could be one. The tenant token of the subject
// is checked against the payload's tenant: the token only routes, and a publisher whose credentials are bound
// to one tenant's subjects must not be able to name another tenant in the body.
func decode(data []byte, tenantToken string) (domain.Notification, error) {
	var p payload
	if err := json.Unmarshal(data, &p); err != nil {
		return domain.Notification{}, refuse("the body is not a JSON notification: %v", err)
	}
	switch {
	case p.IdemKey == "":
		return domain.Notification{}, refuse("idem_key is required: without it there is no exactly-once identity, and a message is never delivered unguarded")
	case p.Tenant.Kind == "" || p.Tenant.ID == "":
		return domain.Notification{}, refuse("tenant: kind and id must both be set")
	case p.Recipient.Kind == "" || p.Recipient.ID == "":
		return domain.Notification{}, refuse("recipient: kind and id must both be set")
	}
	tenant := domain.Tenant{Kind: p.Tenant.Kind, ID: p.Tenant.ID}
	if got := domain.TenantToken(tenant); got != tenantToken {
		return domain.Notification{}, refuse("tenant token: the subject names %q and the payload's tenant has %q", tenantToken, got)
	}
	n := domain.Notification{
		Tenant: tenant, Recipient: domain.Recipient{Kind: p.Recipient.Kind, ID: p.Recipient.ID},
		Topic: domain.Topic(p.Topic), Priority: domain.Priority(p.Priority), Data: p.Data, Title: p.Title, Body: p.Body,
		Payload: p.Payload, Attributes: p.Attributes, IdemKey: p.IdemKey,
		OccurredAt: p.OccurredAt, DeliverAfter: p.DeliverAfter, DeliverBefore: p.DeliverBefore,
	}
	if err := n.Validate(); err != nil {
		return domain.Notification{}, refusal{err.Error()}
	}
	return n, nil
}

// neverSucceeds reports whether err is a refusal that retrying cannot fix: a notification the engine will not
// accept as it is. Anything else (a database that is down, a deadline) is transient.
func neverSucceeds(err error) bool {
	var invalid *domain.ValidationError
	return errors.As(err, &invalid) ||
		errors.Is(err, domain.ErrUnknownTopic) || errors.Is(err, domain.ErrMissingIdem) ||
		errors.Is(err, domain.ErrEmptyIdentity) || errors.Is(err, domain.ErrUnknownChannel)
}

// Handle processes one message and settles it: acknowledged once Notify has returned, redelivered after a
// transient failure, or parked and terminated when it can never succeed. A shutdown mid-message settles
// nothing, and the bus redelivers it.
func (i *Ingest) Handle(ctx context.Context, msg jetstream.Msg) {
	meta, err := msg.Metadata()
	if err != nil {
		i.onError(fmt.Errorf("natsingest: a message on %q has no metadata: %w", msg.Subject(), err))
		_ = msg.NakWithDelay(i.retryDelay)
		return
	}
	attempts := int(meta.NumDelivered) //nolint:gosec // a delivery count
	m := domain.PoisonMessage{
		Key: fmt.Sprintf("%s:%d", meta.Stream, meta.Sequence.Stream), Subject: msg.Subject(), Body: msg.Data(), Attempts: attempts,
	}

	realm, token, ok := parseSubject(msg.Subject())
	if !ok {
		// No realm to park it under and no DLQ subject to tell: the subject does not follow the contract, and a
		// credential bound to a realm could not have published it. It is ended and reported.
		i.onError(fmt.Errorf("natsingest: terminated a message on %q, which names no realm (notify.ingest.<realm>.<tenant_token>)", msg.Subject()))
		_ = msg.Term()
		return
	}
	m.Realm = realm

	notifier, ok := i.notifiers[realm]
	if !ok {
		i.park(ctx, msg, m, fmt.Sprintf("realm %q is not served by this ingest", realm))
		return
	}
	n, err := decode(msg.Data(), token)
	if err != nil {
		i.park(ctx, msg, m, err.Error())
		return
	}

	// The commit. Notify returns after PostgreSQL committed (D23), so what follows is after it.
	if _, err := notifier.Notify(ctx, n); err != nil {
		switch {
		case ctx.Err() != nil || errors.Is(err, context.Canceled):
			// A shutdown: the bus redelivers what was not acknowledged.
		case neverSucceeds(err):
			i.park(ctx, msg, m, err.Error())
		case attempts >= i.maxDeliver:
			i.park(ctx, msg, m, fmt.Sprintf("gave up after %d deliveries: %v", attempts, err))
		default:
			i.onError(fmt.Errorf("natsingest: notify for %s failed (delivery %d of %d): %w", m.Key, attempts, i.maxDeliver, err))
			_ = msg.NakWithDelay(i.retryDelay)
		}
		return
	}
	// A replay (Applied=false) is acknowledged like a first apply: it was committed before, and nothing is
	// left to do. If this acknowledgement is lost the bus redelivers, and Notify replays again.
	ackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), ackTimeout)
	defer cancel()
	if err := msg.DoubleAck(ackCtx); err != nil {
		i.onError(fmt.Errorf("natsingest: the acknowledgement of %s was not confirmed; the bus will redeliver and Notify will replay: %w", m.Key, err))
	}
}

// dlqMessage is what operators are told about a poison message.
type dlqMessage struct {
	Subject  string `json:"subject"`
	Key      string `json:"key"`
	Reason   string `json:"reason"`
	Attempts int    `json:"attempts"`
	Body     string `json:"body"`
}

// park gives up on a message: it is recorded as a dead letter, told to operators, and only then terminated.
// Nothing is terminated that was not recorded, so a table or a subject that is down is a redelivery, and the
// message is parked when they are back.
func (i *Ingest) park(ctx context.Context, msg jetstream.Msg, m domain.PoisonMessage, reason string) {
	m.Reason = reason
	ctx = context.WithoutCancel(ctx)
	if err := i.sink.RecordPoison(ctx, m); err != nil {
		i.onError(fmt.Errorf("natsingest: could not record the poison message %s (%s): %w", m.Key, reason, err))
		_ = msg.NakWithDelay(i.retryDelay)
		return
	}
	body, _ := json.Marshal(dlqMessage{Subject: m.Subject, Key: m.Key, Reason: reason, Attempts: m.Attempts, Body: string(m.Body)})
	if _, err := i.pub.Publish(ctx, DLQPrefix+"."+string(m.Realm), body); err != nil {
		i.onError(fmt.Errorf("natsingest: could not tell the operators of poison message %s: %w", m.Key, err))
		_ = msg.NakWithDelay(i.retryDelay)
		return
	}
	i.onError(fmt.Errorf("natsingest: parked %s as poison: %s", m.Key, reason))
	_ = msg.Term()
}

// Run consumes until ctx ends. It returns an error only when the consumption itself ended without ctx
// ending: a connection that cannot be kept.
func (i *Ingest) Run(ctx context.Context, c Consumer) error {
	cc, err := c.Consume(func(msg jetstream.Msg) { i.Handle(ctx, msg) })
	if err != nil {
		return fmt.Errorf("natsingest: start consuming: %w", err)
	}
	select {
	case <-ctx.Done():
		cc.Stop()
		return nil
	case <-cc.Closed():
		if ctx.Err() != nil {
			return nil
		}
		return errors.New("natsingest: the consumption ended")
	}
}
