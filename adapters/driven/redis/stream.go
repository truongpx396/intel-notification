package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	goredis "github.com/redis/go-redis/v9"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// Stream is the Redis ports.StreamPublisher (D16) and the subscribing end the
// relay reads.
//
// A nudge is the notification id and nothing else, published with SPUBLISH on
// domain.StreamKey: the hash of the whole identity, in a Redis Cluster hash tag.
// So a recipient's stream is one shard channel on one slot, a subscriber reaches
// only the identity it subscribed as, and the relay loads whatever a nudge names
// under that same identity, which makes a misrouted or forged one show nothing.
//
// It is at-most-once and a fast path, never the record (D15): a nudge nobody hears
// is not a fault, and the unread count is recomputed from the store on every
// connect. What Publish does return as an error is for the caller to classify: one
// that wraps domain.ErrPermanent no retry will fix, and any other is transient.
type Stream struct {
	rdb goredis.UniversalClient
}

var (
	_ ports.StreamPublisher  = (*Stream)(nil)
	_ ports.StreamSubscriber = (*Stream)(nil)
)

// NewStream returns a stream over rdb. A cluster client routes by the key's slot;
// a single node needs Redis 7.0 for sharded pub/sub.
func NewStream(rdb goredis.UniversalClient) *Stream { return &Stream{rdb: rdb} }

// Publish implements ports.StreamPublisher. An identity with an empty component
// or an empty notification id has nothing to nudge and is refused as permanent:
// the key of an identity is only a scope when every part of it is set.
func (s *Stream) Publish(ctx context.Context, id domain.Identity, notificationID string) error {
	if err := id.Validate(); err != nil {
		return fmt.Errorf("redis stream: %w: %w", domain.ErrPermanent, err)
	}
	if notificationID == "" {
		return fmt.Errorf("redis stream: %w: no notification id to nudge", domain.ErrPermanent)
	}
	if err := s.rdb.SPublish(ctx, domain.StreamKey(id), notificationID).Err(); err != nil {
		return fmt.Errorf("redis stream: publish: %w", classify(err))
	}
	return nil
}

// classify marks the faults a retry cannot fix, and only those a Redis server
// itself reported: an ACL that refuses the command for this user, and a server
// that does not have it (sharded pub/sub arrived in Redis 7.0). Both wait on an
// operator, so they are dead-lettered with their reason at once. Everything else,
// a dropped connection, a loading dataset, a failover, a moved slot, passes with
// the error as it came, for the caller to retry.
func classify(err error) error {
	if err == nil {
		return nil
	}
	var server goredis.Error
	if !errors.As(err, &server) {
		return err
	}
	msg := err.Error()
	if strings.HasPrefix(msg, "NOPERM") || strings.HasPrefix(msg, "ERR unknown command") {
		return fmt.Errorf("%w: %w", domain.ErrPermanent, err)
	}
	return err
}

// subscription is one relay's live view of one identity's nudges: the notification
// ids published to the identity's stream, from the moment Subscribe returned.
type subscription struct {
	ps      *goredis.PubSub
	key     string
	ids     chan string
	closing chan struct{} // closed by Close, to stop a send nobody is reading
	done    chan struct{} // closed when the pump has stopped
	once    sync.Once
	err     error
}

// IDs delivers the notification id of each nudge, in the order published, and is
// closed when the subscription ends: its context ended, it was closed, or the
// connection could not be kept.
func (s *subscription) IDs() <-chan string { return s.ids }

// Close ends the subscription and returns when the server has dropped it, so a
// count of subscribers read afterwards no longer includes this one. It is safe to
// call more than once.
func (s *subscription) Close() error {
	s.once.Do(func() {
		close(s.closing)
		if err := s.ps.SUnsubscribe(context.Background(), s.key); err != nil {
			s.err = fmt.Errorf("redis stream: unsubscribe: %w", err)
			_ = s.ps.Close() // the server cannot be asked, so the connection is what ends it
		}
	})
	<-s.done
	return s.err
}

// Subscribe opens id's stream. It returns once the server has confirmed the
// subscription, so anything published afterwards is delivered. ctx bounds the
// subscription as well as the call. A relay passes the authenticated session's
// identity, never one a client supplied (D16).
func (s *Stream) Subscribe(ctx context.Context, id domain.Identity) (ports.Subscription, error) {
	if err := id.Validate(); err != nil {
		return nil, fmt.Errorf("redis stream: %w: %w", domain.ErrPermanent, err)
	}
	key := domain.StreamKey(id)
	ps := s.rdb.SSubscribe(ctx, key)
	if _, err := ps.Receive(ctx); err != nil {
		_ = ps.Close()
		return nil, fmt.Errorf("redis stream: subscribe: %w", classify(err))
	}
	sub := &subscription{
		ps:      ps,
		key:     key,
		ids:     make(chan string),
		closing: make(chan struct{}),
		done:    make(chan struct{}),
	}
	go sub.pump(ctx, ps.ChannelWithSubscriptions())
	return sub, nil
}

// pump moves messages from the connection to IDs until the subscription ends.
func (s *subscription) pump(ctx context.Context, events <-chan interface{}) {
	defer close(s.done)
	defer close(s.ids)
	defer func() { _ = s.ps.Close() }()
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-events:
			if !ok {
				return
			}
			switch e := ev.(type) {
			case *goredis.Message:
				select {
				case s.ids <- e.Payload:
				case <-s.closing: // Close is waiting for the server's confirmation
				case <-ctx.Done():
					return
				}
			case *goredis.Subscription:
				if e.Kind == "sunsubscribe" {
					return
				}
			}
		}
	}
}
