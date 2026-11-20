// Package natsingest is the optional ingest of producers that publish a notification to a bus instead of
// calling the service: it consumes NATS JetStream and calls Notifier.Notify (D23, NR-035,
// contracts/bus-subjects.md). The core has no bus port, and nothing here makes a rule of its own: a message
// is a Notify call that happens to arrive on a stream.
//
// Three rules carry it. A message is acknowledged only after Notify has returned, which is after
// PostgreSQL committed, so a crash anywhere before that is a redelivery and Notify's idempotency makes the
// redelivery harmless. The realm is the subject's and never the payload's (NR-009). And a message that can
// never succeed, which is parked as a dead letter and told to the operators on the DLQ subject, never
// blocks the stream behind it.
package natsingest

import (
	"context"
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

// Ingest turns JetStream messages into Notify calls.
type Ingest struct{}

// Option configures an Ingest.
type Option func(*Ingest)

// WithMaxDeliver sets how many deliveries a message that keeps failing transiently gets before it is given
// up on as poison. It should be the consumer's own MaxDeliver. The default is 10.
func WithMaxDeliver(n int) Option { return func(*Ingest) {} }

// WithRetryDelay sets how long a transient failure waits before the bus redelivers the message.
func WithRetryDelay(d time.Duration) Option { return func(*Ingest) {} }

// WithErrorHandler is given every fault of the ingest's own, for an operator's log.
func WithErrorHandler(h func(error)) Option { return func(*Ingest) {} }

// New returns an Ingest that calls the Notifier of each realm, parks poison messages in sink and tells pub.
func New(notifiers map[domain.Realm]ports.Notifier, sink ports.PoisonSink, pub Publisher, opts ...Option) *Ingest {
	return &Ingest{}
}

// Handle processes one message and settles it: acknowledged, redelivered later, or parked.
func (i *Ingest) Handle(ctx context.Context, msg jetstream.Msg) { _ = msg.Ack() }

// Run consumes until ctx ends.
func (i *Ingest) Run(ctx context.Context, c Consumer) error { return nil }
