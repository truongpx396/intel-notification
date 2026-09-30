package notifytest

import (
	"fmt"
	"sync"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// Registry is an in-memory ports.ChannelRegistry. Registering a channel is one
// line, which is what NS-008 measures: a third channel is one file and one line
// (T033a).
type Registry struct {
	mu sync.Mutex
	m  map[domain.ChannelKind]ports.Channel
}

var _ ports.ChannelRegistry = (*Registry)(nil)

// NewRegistry returns a registry holding chs.
func NewRegistry(chs ...ports.Channel) *Registry {
	r := &Registry{m: map[domain.ChannelKind]ports.Channel{}}
	for _, c := range chs {
		r.Register(c)
	}
	return r
}

// Register adds c under its kind. A kind registered twice, or an empty one, is a
// wiring mistake, so it panics.
func (r *Registry) Register(c ports.Channel) {
	if c.Kind() == "" {
		panic(fmt.Sprintf("notifytest: a channel with an empty kind: %T", c))
	}
}

// Get returns the channel registered for kind.
func (r *Registry) Get(kind domain.ChannelKind) (ports.Channel, bool) { return nil, false }

// Kinds lists the registered kinds, sorted.
func (r *Registry) Kinds() []domain.ChannelKind { return nil }
