package notifytest

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

type recipientPrefKey struct {
	id      domain.Identity
	topic   domain.Topic
	channel domain.ChannelKind
}

type tenantPrefKey struct {
	realm   domain.Realm
	tenant  domain.Tenant
	topic   domain.Topic
	channel domain.ChannelKind
}

// Preferences is an in-memory ports.PreferenceStore. It resolves with
// domain.ResolvePreferences, the one implementation of D30's precedence, and holds
// the rows: a recipient's choices, a tenant's defaults, and the schedules. It
// refuses what the PostgreSQL store refuses (an incomplete identity or pair, a
// negative digest window), and the shared table in [PreferenceStoreContract] holds
// the two to the same behaviour (T018).
type Preferences struct {
	mu        sync.Mutex
	recipient map[recipientPrefKey]bool
	tenant    map[tenantPrefKey]domain.TenantChoice
	schedules map[domain.Identity]domain.DeliverySchedule
}

var _ ports.PreferenceStore = (*Preferences)(nil)

// NewPreferences returns a store with no rows: everything resolves to the topic's
// defaults.
func NewPreferences() *Preferences {
	return &Preferences{
		recipient: map[recipientPrefKey]bool{},
		tenant:    map[tenantPrefKey]domain.TenantChoice{},
		schedules: map[domain.Identity]domain.DeliverySchedule{},
	}
}

// Resolve implements ports.PreferenceStore.
func (p *Preferences) Resolve(_ context.Context, id domain.Identity, topic domain.Topic, def domain.TopicDef) ([]domain.ResolvedPreference, error) {
	if err := id.Validate(); err != nil {
		return nil, fmt.Errorf("notifytest: resolve: %w", err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	recipient := map[domain.ChannelKind]bool{}
	for k, enabled := range p.recipient {
		if k.id == id && k.topic == topic {
			recipient[k.channel] = enabled
		}
	}
	tenant := map[domain.ChannelKind]domain.TenantChoice{}
	for k, choice := range p.tenant {
		if k.realm == id.Realm && k.tenant == id.Tenant && k.topic == topic {
			tenant[k.channel] = choice
		}
	}
	return domain.ResolvePreferences(topic, def, recipient, tenant), nil
}

// Set upserts the recipient's choices, all or none. A pair not named is left as it
// was, and a pair named twice takes the later.
func (p *Preferences) Set(_ context.Context, id domain.Identity, prefs []domain.Preference) error {
	if err := id.Validate(); err != nil {
		return fmt.Errorf("notifytest: set: %w", err)
	}
	for _, pref := range prefs {
		if err := pref.Validate(); err != nil {
			return fmt.Errorf("notifytest: set: %w", err)
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, pref := range prefs {
		p.recipient[recipientPrefKey{id, pref.Topic, pref.Channel}] = pref.Enabled
	}
	return nil
}

// SetTenant upserts a tenant's default, and whether it is locked.
func (p *Preferences) SetTenant(_ context.Context, realm domain.Realm, tp domain.TenantPreference) error {
	if realm == "" {
		return errors.New("notifytest: set tenant: realm is required")
	}
	if err := tp.Validate(); err != nil {
		return fmt.Errorf("notifytest: set tenant: %w", err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.tenant[tenantPrefKey{realm, tp.Tenant, tp.Topic, tp.Channel}] = domain.TenantChoice{Enabled: tp.Enabled, Locked: tp.Locked}
	return nil
}

// Schedule implements ports.PreferenceStore. A recipient who never set one has the
// zero schedule: immediate delivery, no quiet hours.
func (p *Preferences) Schedule(_ context.Context, id domain.Identity) (domain.DeliverySchedule, error) {
	if err := id.Validate(); err != nil {
		return domain.DeliverySchedule{}, fmt.Errorf("notifytest: schedule: %w", err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.schedules[id], nil
}

// SetSchedule replaces the recipient's schedule.
func (p *Preferences) SetSchedule(_ context.Context, id domain.Identity, s domain.DeliverySchedule) error {
	if err := id.Validate(); err != nil {
		return fmt.Errorf("notifytest: set schedule: %w", err)
	}
	if s.Digest < 0 {
		return fmt.Errorf("notifytest: set schedule: digest window %v is negative", s.Digest)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.schedules[id] = s
	return nil
}
