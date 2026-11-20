package app

import (
	"context"
	"fmt"
	"slices"

	notify "github.com/truongpx396/intel-notification"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// Admin is the operator surface: ports.Admin over the maintenance operations and the preference
// store. It replays a dead letter (T032, NS-006), erases a recipient (T040, NS-012) and sets a tenant's
// default for a (topic, channel) pair (T044, D30).
type Admin struct {
	maintenance ports.Maintenance
	prefs       ports.PreferenceStore
	topics      ports.TopicRegistry
	channels    ports.ChannelRegistry
	waker       ports.Waker    // nil: no dispatcher in this process to wake
	pre         ports.PreCheck // nil: no fast path whose entries an erasure must forget
}

var _ ports.Admin = (*Admin)(nil)

// NewAdmin returns an Admin for cfg over d. It refuses a configuration that does not
// validate, and names every port it needs and d leaves nil. Deps.Waker and Deps.PreCheck
// are optional; pass the PreCheck the Notifier was built with, or an erasure leaves its
// entries for the recipient's keys behind (D51).
func NewAdmin(cfg notify.Config, d Deps) (*Admin, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	var g gaps
	g.need("Channels", d.Channels == nil)
	g.need("Maintenance", d.Maintenance == nil)
	g.need("Prefs", d.Prefs == nil)
	g.need("Topics", d.Topics == nil)
	if err := g.err("Admin"); err != nil {
		return nil, err
	}
	return &Admin{maintenance: d.Maintenance, prefs: d.Prefs, topics: d.Topics, channels: d.Channels, waker: d.Waker, pre: d.PreCheck}, nil
}

// ReplayDeadLetter re-drives one dead letter, once (T032). A dead letter that was
// replayed already is not an error: the call is idempotent, and the operator who clicks
// twice, or the script that retries after a timeout, gets the one delivery. A replay
// that enqueued work wakes the dispatcher of this process, if it has one.
//
// A dead letter that does not exist in the realm is domain.ErrUnknownDeadLetter, and
// one the bus parked, which holds no delivery to re-drive, is domain.ErrNotReplayable.
func (a *Admin) ReplayDeadLetter(ctx context.Context, realm domain.Realm, deadLetterID string) error {
	if err := realm.Validate(); err != nil {
		return fmt.Errorf("replay dead letter: %w", err)
	}
	if deadLetterID == "" {
		return fmt.Errorf("replay dead letter: no id: %w", domain.ErrUnknownDeadLetter)
	}
	replayed, err := a.maintenance.ReplayDeadLetter(ctx, realm, deadLetterID)
	if err != nil {
		return fmt.Errorf("replay dead letter: %w", err)
	}
	if replayed && a.waker != nil {
		a.waker.Wake()
	}
	return nil
}

// SetTenantPreference sets a tenant's default for one (topic, channel) pair in realm, and whether the
// recipient may override it (D30). It refuses, with the reason, a topic or a channel nobody registered,
// and an essential topic's own channel disabled (NR-015): the topic could not be turned off by a
// recipient, and a tenant default must not be a way around that. Enabling it, or locking it on, is
// accepted: it keeps the topic on. The realm is the operator's, an argument and not part of the
// preference (NR-009).
func (a *Admin) SetTenantPreference(ctx context.Context, realm domain.Realm, p domain.TenantPreference) error {
	if err := realm.Validate(); err != nil {
		return fmt.Errorf("set tenant preference: %w", err)
	}
	if err := p.Validate(); err != nil {
		return fmt.Errorf("set tenant preference: %w", err)
	}
	def, found, err := a.topics.Lookup(ctx, realm, p.Topic)
	if err != nil {
		return fmt.Errorf("set tenant preference: look up topic %q: %w", p.Topic, err)
	}
	refuse := func(field, msg string, sentinel error) error {
		return fmt.Errorf("set tenant preference: %w", &domain.ValidationError{Problems: []domain.Problem{{Field: field, Message: msg, Err: sentinel}}})
	}
	if !found {
		return refuse("Topic", fmt.Sprintf("%q is not registered", p.Topic), domain.ErrUnknownTopic)
	}
	if _, known := a.channels.Get(p.Channel); !known {
		return refuse("Channel", fmt.Sprintf("%q is not registered", p.Channel), domain.ErrUnknownChannel)
	}
	if def.Essential && !p.Enabled && (slices.Contains(def.DefaultChannels, p.Channel) || slices.Contains(def.Fallback, p.Channel)) {
		return refuse("Enabled", fmt.Sprintf("topic %q is essential, so its %q channel cannot be disabled for a tenant", p.Topic, p.Channel), domain.ErrPreferenceLocked)
	}
	return a.prefs.SetTenant(ctx, realm, p)
}
