package notifytest

import (
	"context"
	"slices"
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

type tenantChoice struct {
	enabled, locked bool
}

// Preferences is an in-memory ports.PreferenceStore that resolves as D30 says: a
// recipient's row wins, unless the tenant's row for that (topic, channel) is
// locked, then the tenant's row, then the topic's registered default. Essential
// topics cannot be disabled at any level.
//
// A topic's resolved channels are its default channels and its fallback chain,
// each enabled by the topic's default, and any other channel that a recipient or
// tenant row names. They come back in that order, the extras sorted. Where the
// contract leaves room — the fallback chain being subject to preferences, and what
// an essential topic resolves to — this is the choice made, and the PostgreSQL
// store's table (T018) decides it for both.
type Preferences struct {
	mu        sync.Mutex
	recipient map[recipientPrefKey]bool
	tenant    map[tenantPrefKey]tenantChoice
	schedules map[domain.Identity]domain.DeliverySchedule
}

var _ ports.PreferenceStore = (*Preferences)(nil)

// NewPreferences returns a store with no rows: everything resolves to the topic's
// defaults.
func NewPreferences() *Preferences {
	return &Preferences{
		recipient: map[recipientPrefKey]bool{},
		tenant:    map[tenantPrefKey]tenantChoice{},
		schedules: map[domain.Identity]domain.DeliverySchedule{},
	}
}

// Resolve implements ports.PreferenceStore.
func (p *Preferences) Resolve(_ context.Context, id domain.Identity, topic domain.Topic, def domain.TopicDef) ([]domain.ResolvedPreference, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// The channels: the topic's own, then any other a row names, sorted.
	channels := slices.Concat(def.DefaultChannels, def.Fallback)
	byDefault := map[domain.ChannelKind]bool{}
	for _, ch := range channels {
		byDefault[ch] = true
	}
	var extras []domain.ChannelKind
	note := func(ch domain.ChannelKind) {
		if !byDefault[ch] && !slices.Contains(extras, ch) {
			extras = append(extras, ch)
		}
	}
	for k := range p.recipient {
		if k.id == id && k.topic == topic {
			note(k.channel)
		}
	}
	for k := range p.tenant {
		if k.realm == id.Realm && k.tenant == id.Tenant && k.topic == topic {
			note(k.channel)
		}
	}
	slices.Sort(extras)
	channels = append(channels, extras...)

	out := make([]domain.ResolvedPreference, 0, len(channels))
	for _, ch := range channels {
		r := domain.ResolvedPreference{Topic: topic, Channel: ch, Enabled: byDefault[ch], Source: domain.SourceTopicDefault}
		if tc, ok := p.tenant[tenantPrefKey{id.Realm, id.Tenant, topic, ch}]; ok {
			r.Enabled, r.Source, r.Locked = tc.enabled, domain.SourceTenant, tc.locked
		}
		if choice, ok := p.recipient[recipientPrefKey{id, topic, ch}]; ok && !r.Locked {
			r.Enabled, r.Source = choice, domain.SourceRecipient
		}
		if def.Essential && byDefault[ch] {
			r.Enabled, r.Source, r.Locked = true, domain.SourceTopicDefault, true
		}
		out = append(out, r)
	}
	return out, nil
}

// Set upserts the recipient's choices. A pair not named is left as it was.
func (p *Preferences) Set(_ context.Context, id domain.Identity, prefs []domain.Preference) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, pref := range prefs {
		p.recipient[recipientPrefKey{id, pref.Topic, pref.Channel}] = pref.Enabled
	}
	return nil
}

// SetTenant upserts a tenant's default, and whether it is locked.
func (p *Preferences) SetTenant(_ context.Context, realm domain.Realm, tp domain.TenantPreference) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.tenant[tenantPrefKey{realm, tp.Tenant, tp.Topic, tp.Channel}] = tenantChoice{enabled: tp.Enabled, locked: tp.Locked}
	return nil
}

// Schedule implements ports.PreferenceStore. A recipient who never set one has the
// zero schedule: immediate delivery, no quiet hours.
func (p *Preferences) Schedule(_ context.Context, id domain.Identity) (domain.DeliverySchedule, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.schedules[id], nil
}

// SetSchedule replaces the recipient's schedule.
func (p *Preferences) SetSchedule(_ context.Context, id domain.Identity, s domain.DeliverySchedule) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.schedules[id] = s
	return nil
}
