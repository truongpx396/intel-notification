package wire

import (
	"github.com/truongpx396/intel-notification/api/notifyv1"
	"github.com/truongpx396/intel-notification/domain"
)

// The management messages of the Catalog service. Like the producer's, none carries a realm: the
// service derives it from the authenticated producer (NR-009, D31).

// TopicFromProto is the registration a Topic message carries. An empty default priority is "info", as
// the table's column is: a producer that does not care is not made to say so.
func TopicFromProto(p *notifyv1.Topic) (domain.Topic, domain.TopicDef) {
	def := domain.TopicDef{
		DefaultPriority: domain.Priority(p.GetDefaultPriority()),
		Essential:       p.GetEssential(),
		TemplateRef:     p.GetTemplateRef(),
	}
	if def.DefaultPriority == "" {
		def.DefaultPriority = domain.PriorityInfo
	}
	for _, ch := range p.GetDefaultChannels() {
		def.DefaultChannels = append(def.DefaultChannels, domain.ChannelKind(ch))
	}
	for _, ch := range p.GetFallback() {
		def.Fallback = append(def.Fallback, domain.ChannelKind(ch))
	}
	return domain.Topic(p.GetTopic()), def
}

// TopicToProto is the Topic message for a registration.
func TopicToProto(topic domain.Topic, def domain.TopicDef) *notifyv1.Topic {
	p := &notifyv1.Topic{Topic: string(topic), DefaultPriority: string(def.DefaultPriority), Essential: def.Essential, TemplateRef: def.TemplateRef}
	for _, ch := range def.DefaultChannels {
		p.DefaultChannels = append(p.DefaultChannels, string(ch))
	}
	for _, ch := range def.Fallback {
		p.Fallback = append(p.Fallback, string(ch))
	}
	return p
}

// TemplateFromProto is the template a Template message carries. An unset tenant is a realm-wide
// template, and the version is ignored: the service assigns it.
func TemplateFromProto(p *notifyv1.Template) domain.Template {
	t := domain.Template{
		Ref: p.GetTemplateRef(), Channel: domain.ChannelKind(p.GetChannel()), Locale: p.GetLocale(),
		Subject: p.GetSubject(), Body: p.GetBody(), Data: dataFromStruct(p.GetData()),
	}
	if p.GetTenant() != nil {
		t.Tenant = tenantFromProto(p.GetTenant())
	}
	return t
}

// TemplateToProto is the Template message for t. It fails only for structured data that cannot cross
// exactly (see dataToStruct).
func TemplateToProto(t domain.Template) (*notifyv1.Template, error) {
	data, err := dataToStruct(t.Data)
	if err != nil {
		return nil, err
	}
	p := &notifyv1.Template{
		TemplateRef: t.Ref, Channel: string(t.Channel), Locale: t.Locale, Version: int32(t.Version), //nolint:gosec // a version counts saves of one template
		Subject: t.Subject, Body: t.Body, Data: data,
	}
	if t.Tenant != (domain.Tenant{}) {
		p.Tenant = tenantToProto(t.Tenant)
	}
	return p, nil
}

// TemplateVersionFromProto is the version a TemplateVersion message names.
func TemplateVersionFromProto(p *notifyv1.TemplateVersion) domain.TemplateVersion {
	v := domain.TemplateVersion{
		Ref: p.GetTemplateRef(), Channel: domain.ChannelKind(p.GetChannel()), Locale: p.GetLocale(), Version: int(p.GetVersion()),
	}
	if p.GetTenant() != nil {
		v.Tenant = tenantFromProto(p.GetTenant())
	}
	return v
}

// TemplateVersionToProto is the TemplateVersion message for v.
func TemplateVersionToProto(v domain.TemplateVersion) *notifyv1.TemplateVersion {
	p := &notifyv1.TemplateVersion{TemplateRef: v.Ref, Channel: string(v.Channel), Locale: v.Locale, Version: int32(v.Version)} //nolint:gosec // see TemplateToProto
	if v.Tenant != (domain.Tenant{}) {
		p.Tenant = tenantToProto(v.Tenant)
	}
	return p
}

// AddressSet is one recipient's addresses on one channel, as the Catalog's PutAddresses carries them.
type AddressSet struct {
	Tenant    domain.Tenant
	Recipient domain.Recipient
	Channel   domain.ChannelKind
	Addresses []domain.Address
}

// AddressSetFromProto is the set a RecipientAddresses message carries.
func AddressSetFromProto(p *notifyv1.RecipientAddresses) AddressSet {
	set := AddressSet{Tenant: tenantFromProto(p.GetTenant()), Recipient: recipientFromProto(p.GetRecipient()), Channel: domain.ChannelKind(p.GetChannel())}
	for _, a := range p.GetAddresses() {
		set.Addresses = append(set.Addresses, domain.Address{Value: a.GetValue(), Locale: a.GetLocale(), Timezone: a.GetTimezone(), Meta: a.GetMeta()})
	}
	return set
}

// AddressSetToProto is the RecipientAddresses message for set.
func AddressSetToProto(set AddressSet) *notifyv1.RecipientAddresses {
	p := &notifyv1.RecipientAddresses{Tenant: tenantToProto(set.Tenant), Recipient: recipientToProto(set.Recipient), Channel: string(set.Channel)}
	for _, a := range set.Addresses {
		p.Addresses = append(p.Addresses, &notifyv1.Address{Value: a.Value, Locale: a.Locale, Timezone: a.Timezone, Meta: a.Meta})
	}
	return p
}

// TenantPreferenceFromProto is the default a TenantPreference message carries.
func TenantPreferenceFromProto(p *notifyv1.TenantPreference) domain.TenantPreference {
	return domain.TenantPreference{
		Tenant: tenantFromProto(p.GetTenant()), Topic: domain.Topic(p.GetTopic()), Channel: domain.ChannelKind(p.GetChannel()),
		Enabled: p.GetEnabled(), Locked: p.GetLocked(),
	}
}

// TenantPreferenceToProto is the TenantPreference message for pref.
func TenantPreferenceToProto(pref domain.TenantPreference) *notifyv1.TenantPreference {
	return &notifyv1.TenantPreference{Tenant: tenantToProto(pref.Tenant), Topic: string(pref.Topic), Channel: string(pref.Channel), Enabled: pref.Enabled, Locked: pref.Locked}
}

// EraseReceiptToProto is the EraseReceipt message for the rows removed per table.
func EraseReceiptToProto(removed map[string]int) *notifyv1.EraseReceipt {
	p := &notifyv1.EraseReceipt{Removed: make(map[string]int32, len(removed))}
	for table, n := range removed {
		p.Removed[table] = int32(n) //nolint:gosec // rows removed for one recipient
	}
	return p
}

// EraseReceiptFromProto is the rows removed per table an EraseReceipt message carries.
func EraseReceiptFromProto(p *notifyv1.EraseReceipt) map[string]int {
	out := make(map[string]int, len(p.GetRemoved()))
	for table, n := range p.GetRemoved() {
		out[table] = int(n)
	}
	return out
}

// The reasons the Catalog adds to the producer surface's.
const (
	ReasonUnknownChannel   = "UNKNOWN_CHANNEL"
	ReasonInvalidTemplate  = "INVALID_TEMPLATE"
	ReasonUnknownTemplate  = "UNKNOWN_TEMPLATE"
	ReasonPreferenceLocked = "PREFERENCE_LOCKED"
)
