package notifytest

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

type topicKey struct {
	realm domain.Realm
	topic domain.Topic
}

// Topics is an in-memory ports.TopicRegistry, realm-aware as the real one is (one
// service serves several realms). It holds only what a test registers.
type Topics struct {
	mu sync.Mutex
	m  map[topicKey]domain.TopicDef
}

var (
	_ ports.TopicRegistry = (*Topics)(nil)
	_ ports.TopicWriter   = (*Topics)(nil)
)

// NewTopics returns an empty registry.
func NewTopics() *Topics {
	return &Topics{m: map[topicKey]domain.TopicDef{}}
}

// Register sets topic's defaults in realm, replacing any earlier registration. It
// panics for an empty topic or a definition the real registry would refuse
// (TopicDef.Validate), so a test cannot stand on a topic that could not exist.
func (t *Topics) Register(realm domain.Realm, topic domain.Topic, def domain.TopicDef) {
	if topic == "" {
		panic("notifytest: a topic with no name")
	}
	if err := def.Validate(); err != nil {
		panic(fmt.Sprintf("notifytest: topic %q: %v", topic, err))
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.m[topicKey{realm, topic}] = cloneTopicDef(def)
}

// UpsertTopic implements ports.TopicWriter: it refuses what domain.ValidateTopicRegistration refuses, as the
// table does, and replaces an earlier registration whole.
func (t *Topics) UpsertTopic(_ context.Context, realm domain.Realm, topic domain.Topic, def domain.TopicDef) error {
	if err := domain.ValidateTopicRegistration(realm, topic, def); err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.m[topicKey{realm, topic}] = cloneTopicDef(def)
	return nil
}

// Lookup implements ports.TopicRegistry. The definition it returns is a copy.
func (t *Topics) Lookup(_ context.Context, realm domain.Realm, topic domain.Topic) (domain.TopicDef, bool, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	def, ok := t.m[topicKey{realm, topic}]
	if !ok {
		return domain.TopicDef{}, false, nil
	}
	return cloneTopicDef(def), true, nil
}

// List implements ports.TopicRegistry: the topics registered in realm, sorted. Another realm's are not
// there.
func (t *Topics) List(_ context.Context, realm domain.Realm) ([]domain.Topic, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	var out []domain.Topic
	for k := range t.m {
		if k.realm == realm {
			out = append(out, k.topic)
		}
	}
	slices.Sort(out)
	return out, nil
}

func cloneTopicDef(d domain.TopicDef) domain.TopicDef {
	d.DefaultChannels = slices.Clone(d.DefaultChannels)
	d.Fallback = slices.Clone(d.Fallback)
	return d
}
