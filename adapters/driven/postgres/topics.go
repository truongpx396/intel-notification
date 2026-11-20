package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// Topics implements ports.TopicRegistry and ports.TopicWriter over notification_topics: the data-backed
// registry of service mode, where a topic is a row a producer registers through the Catalog API and
// not code compiled into the process (NR-017, D31).
type Topics struct {
	pool *pgxpool.Pool
	settings
}

var (
	_ ports.TopicRegistry = (*Topics)(nil)
	_ ports.TopicWriter   = (*Topics)(nil)
)

// NewTopics returns a Topics over pool.
func NewTopics(pool *pgxpool.Pool, opts ...Option) *Topics {
	return &Topics{pool: pool, settings: newSettings(opts)}
}

var errNotImplemented = errors.New("not implemented")

// Lookup implements ports.TopicRegistry.
func (t *Topics) Lookup(context.Context, domain.Realm, domain.Topic) (domain.TopicDef, bool, error) {
	return domain.TopicDef{}, false, errNotImplemented
}

// List implements ports.TopicRegistry.
func (t *Topics) List(context.Context, domain.Realm) ([]domain.Topic, error) {
	return nil, errNotImplemented
}

// UpsertTopic implements ports.TopicWriter.
func (t *Topics) UpsertTopic(context.Context, domain.Realm, domain.Topic, domain.TopicDef) error {
	return errNotImplemented
}
