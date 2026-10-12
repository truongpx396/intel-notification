package domain

import "slices"

// Validate reports every rule p breaks, as a *ValidationError, or nil. A pair names
// a topic and a channel: an empty one would be stored as a row nothing ever matches.
func (p Preference) Validate() error {
	var v ValidationError
	if p.Topic == "" {
		v.add("Topic", "is required", nil)
	}
	if p.Channel == "" {
		v.add("Channel", "is required", nil)
	}
	return v.err()
}

// Validate reports every rule p breaks, as a *ValidationError, or nil. A tenant
// default names a tenant as well as a pair; an empty component of the tenant is
// refused, as an identity's is (D17).
func (p TenantPreference) Validate() error {
	var v ValidationError
	if p.Tenant.Kind == "" || p.Tenant.ID == "" {
		v.add("Tenant", "kind and id must both be set", ErrEmptyIdentity)
	}
	if p.Topic == "" {
		v.add("Topic", "is required", nil)
	}
	if p.Channel == "" {
		v.add("Channel", "is required", nil)
	}
	return v.err()
}

// TenantChoice is a tenant's row for one (topic, channel): its default, and whether
// the recipient may override it (D30).
type TenantChoice struct {
	Enabled bool
	Locked  bool
}

// ResolvePreferences resolves the preference of every channel of one topic, as D30
// and NR-011 say, from the rows that exist: recipient holds the recipient's choice
// per channel and tenant holds the tenant's, each only where a row exists.
//
// A recipient's row wins unless the tenant's row for that channel is locked; then
// the tenant's row; then the topic's registered default, which enables every channel
// the topic names, its own and its fallback chain. An essential topic's own
// channels cannot be disabled at any level: they come back enabled and locked, from
// the topic's default.
//
// The channels are the topic's own, in order, each once, then any other channel a
// row names, sorted. Where the contract is silent (the fallback chain being subject
// to preferences, what an essential topic resolves to) this is the choice, and the
// preference store's shared table holds every implementation to it (T018).
//
// It does not change recipient or tenant.
func ResolvePreferences(topic Topic, def TopicDef, recipient map[ChannelKind]bool, tenant map[ChannelKind]TenantChoice) []ResolvedPreference {
	enabledByDefault := map[ChannelKind]bool{}
	var channels []ChannelKind
	for _, ch := range slices.Concat(def.DefaultChannels, def.Fallback) {
		if !enabledByDefault[ch] {
			enabledByDefault[ch] = true
			channels = append(channels, ch)
		}
	}
	var extras []ChannelKind
	note := func(ch ChannelKind) {
		if !enabledByDefault[ch] && !slices.Contains(extras, ch) {
			extras = append(extras, ch)
		}
	}
	for ch := range recipient {
		note(ch)
	}
	for ch := range tenant {
		note(ch)
	}
	slices.Sort(extras)
	channels = append(channels, extras...)

	out := make([]ResolvedPreference, 0, len(channels))
	for _, ch := range channels {
		r := ResolvedPreference{Topic: topic, Channel: ch, Enabled: enabledByDefault[ch], Source: SourceTopicDefault}
		if tc, ok := tenant[ch]; ok {
			r.Enabled, r.Source, r.Locked = tc.Enabled, SourceTenant, tc.Locked
		}
		if choice, ok := recipient[ch]; ok && !r.Locked {
			r.Enabled, r.Source = choice, SourceRecipient
		}
		if def.Essential && enabledByDefault[ch] {
			r.Enabled, r.Source, r.Locked = true, SourceTopicDefault, true
		}
		out = append(out, r)
	}
	return out
}
