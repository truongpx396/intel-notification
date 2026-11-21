package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// Topics implements ports.TopicRegistry and ports.TopicWriter over notification_topics: the data-backed
// registry of service mode, where a topic is a row a producer registers through the Catalog API and
// not code compiled into the process (NR-017, D31).
//
// A topic is realm configuration, not a recipient's data, so the table is not row-level scoped: the
// realm is in every predicate, and it is the caller's, never a request's (NR-009).
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

const lookupTopicSQL = `
SELECT default_channels, fallback, default_priority, essential, template_ref
  FROM notification_topics
 WHERE realm = $1 AND topic = $2`

// Lookup implements ports.TopicRegistry.
func (t *Topics) Lookup(ctx context.Context, realm domain.Realm, topic domain.Topic) (domain.TopicDef, bool, error) {
	var (
		def                domain.TopicDef
		channels, fallback []string
		priority           string
	)
	rows, err := t.pool.Query(ctx, lookupTopicSQL, string(realm), string(topic))
	if err != nil {
		return domain.TopicDef{}, false, fmt.Errorf("look up topic %q: %w", topic, err)
	}
	defer rows.Close()
	if !rows.Next() {
		return domain.TopicDef{}, false, rows.Err()
	}
	if err := rows.Scan(&channels, &fallback, &priority, &def.Essential, &def.TemplateRef); err != nil {
		return domain.TopicDef{}, false, fmt.Errorf("look up topic %q: %w", topic, err)
	}
	def.DefaultChannels, def.Fallback, def.DefaultPriority = kinds(channels), kinds(fallback), domain.Priority(priority)
	return def, true, nil
}

// List implements ports.TopicRegistry. The order is bytewise, as the sort of Go strings is, whatever
// the database's collation.
func (t *Topics) List(ctx context.Context, realm domain.Realm) ([]domain.Topic, error) {
	rows, err := t.pool.Query(ctx, `SELECT topic FROM notification_topics WHERE realm = $1 ORDER BY topic COLLATE "C"`, string(realm))
	if err != nil {
		return nil, fmt.Errorf("list topics: %w", err)
	}
	defer rows.Close()
	var out []domain.Topic
	for rows.Next() {
		var topic string
		if err := rows.Scan(&topic); err != nil {
			return nil, fmt.Errorf("list topics: %w", err)
		}
		out = append(out, domain.Topic(topic))
	}
	return out, rows.Err()
}

// upsertTopicSQL replaces a registration whole: a column the new one leaves empty is emptied, so no
// field of the first survives into the second.
const upsertTopicSQL = `
INSERT INTO notification_topics (realm, topic, default_channels, fallback, default_priority, essential, template_ref, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (realm, topic) DO UPDATE
   SET default_channels = EXCLUDED.default_channels,
       fallback         = EXCLUDED.fallback,
       default_priority = EXCLUDED.default_priority,
       essential        = EXCLUDED.essential,
       template_ref     = EXCLUDED.template_ref,
       updated_at       = EXCLUDED.updated_at`

// UpsertTopic implements ports.TopicWriter.
func (t *Topics) UpsertTopic(ctx context.Context, realm domain.Realm, topic domain.Topic, def domain.TopicDef) error {
	if err := domain.ValidateTopicRegistration(realm, topic, def); err != nil {
		return err
	}
	if _, err := t.pool.Exec(ctx, upsertTopicSQL, string(realm), string(topic), strs(def.DefaultChannels), strs(def.Fallback),
		string(def.DefaultPriority), def.Essential, def.TemplateRef, t.clock.Now()); err != nil {
		return fmt.Errorf("register topic %q: %w", topic, err)
	}
	return nil
}

// strs is a channel list as the text[] the table holds: never NULL, because the column is not nullable.
func strs(chs []domain.ChannelKind) []string {
	out := make([]string, len(chs))
	for i, c := range chs {
		out[i] = string(c)
	}
	return out
}

func kinds(ss []string) []domain.ChannelKind {
	out := make([]domain.ChannelKind, len(ss))
	for i, s := range ss {
		out[i] = domain.ChannelKind(s)
	}
	return out
}
