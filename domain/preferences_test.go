package domain

import (
	"errors"
	"reflect"
	"testing"
)

// invite: email and in_app in parallel, sms as the fallback.
var inviteDef = TopicDef{
	DefaultChannels: []ChannelKind{"in_app", "email"},
	Fallback:        []ChannelKind{"sms"},
	DefaultPriority: PriorityInfo,
}

func resolved(ch ChannelKind, enabled bool, source PreferenceSource, locked bool) ResolvedPreference {
	return ResolvedPreference{Topic: "invite", Channel: ch, Enabled: enabled, Source: source, Locked: locked}
}

// D30, NR-011. The precedence: a recipient's row wins unless the tenant's row for
// that pair is locked; then the tenant's row; then the topic's registered default.
// An essential topic's own channels cannot be disabled at any level. The channels
// are the topic's own, in order, then any other a row names, sorted.
func TestResolvePreferences(t *testing.T) {
	t.Parallel()
	essential := inviteDef
	essential.Essential = true

	cases := []struct {
		name      string
		def       TopicDef
		recipient map[ChannelKind]bool
		tenant    map[ChannelKind]TenantChoice
		want      []ResolvedPreference
		why       string
	}{
		{"no rows: the topic's own channels, enabled by its default", inviteDef, nil, nil,
			[]ResolvedPreference{
				resolved("in_app", true, SourceTopicDefault, false),
				resolved("email", true, SourceTopicDefault, false),
				resolved("sms", true, SourceTopicDefault, false)},
			"defaults first, then the fallback chain"},
		{"the recipient disables one channel and no other", inviteDef,
			map[ChannelKind]bool{"email": false}, nil,
			[]ResolvedPreference{
				resolved("in_app", true, SourceTopicDefault, false),
				resolved("email", false, SourceRecipient, false),
				resolved("sms", true, SourceTopicDefault, false)},
			"a pair is resolved on its own (D3)"},
		{"an unlocked tenant default applies when the recipient has no row", inviteDef,
			nil, map[ChannelKind]TenantChoice{"email": {Enabled: false}},
			[]ResolvedPreference{
				resolved("in_app", true, SourceTopicDefault, false),
				resolved("email", false, SourceTenant, false),
				resolved("sms", true, SourceTopicDefault, false)},
			"an absent recipient choice falls back to the tenant"},
		{"the recipient overrides an unlocked tenant default", inviteDef,
			map[ChannelKind]bool{"email": true}, map[ChannelKind]TenantChoice{"email": {Enabled: false}},
			[]ResolvedPreference{
				resolved("in_app", true, SourceTopicDefault, false),
				resolved("email", true, SourceRecipient, false),
				resolved("sms", true, SourceTopicDefault, false)},
			""},
		{"a locked tenant default beats the recipient's opposite choice, off", inviteDef,
			map[ChannelKind]bool{"email": true}, map[ChannelKind]TenantChoice{"email": {Enabled: false, Locked: true}},
			[]ResolvedPreference{
				resolved("in_app", true, SourceTopicDefault, false),
				resolved("email", false, SourceTenant, true),
				resolved("sms", true, SourceTopicDefault, false)},
			"the tenant's lock is the level that decided"},
		{"a locked tenant default beats the recipient's opposite choice, on", inviteDef,
			map[ChannelKind]bool{"sms": false}, map[ChannelKind]TenantChoice{"sms": {Enabled: true, Locked: true}},
			[]ResolvedPreference{
				resolved("in_app", true, SourceTopicDefault, false),
				resolved("email", true, SourceTopicDefault, false),
				resolved("sms", true, SourceTenant, true)},
			""},
		{"a locked tenant default is reported as the source even where the recipient agrees", inviteDef,
			map[ChannelKind]bool{"email": false}, map[ChannelKind]TenantChoice{"email": {Enabled: false, Locked: true}},
			[]ResolvedPreference{
				resolved("in_app", true, SourceTopicDefault, false),
				resolved("email", false, SourceTenant, true),
				resolved("sms", true, SourceTopicDefault, false)},
			""},
		{"an essential topic's channels stay enabled and locked, whatever any row says", essential,
			map[ChannelKind]bool{"email": false, "sms": false}, map[ChannelKind]TenantChoice{"in_app": {Enabled: false, Locked: true}},
			[]ResolvedPreference{
				resolved("in_app", true, SourceTopicDefault, true),
				resolved("email", true, SourceTopicDefault, true),
				resolved("sms", true, SourceTopicDefault, true)},
			"essential topics cannot be disabled at any level (D30)"},
		{"a channel an essential topic does not enable is left to its rows", essential,
			map[ChannelKind]bool{"push": true}, nil,
			[]ResolvedPreference{
				resolved("in_app", true, SourceTopicDefault, true),
				resolved("email", true, SourceTopicDefault, true),
				resolved("sms", true, SourceTopicDefault, true),
				resolved("push", true, SourceRecipient, false)},
			""},
		{"a channel only a row names is disabled until that row enables it", inviteDef,
			map[ChannelKind]bool{"push": false}, map[ChannelKind]TenantChoice{"slack": {Enabled: true}},
			[]ResolvedPreference{
				resolved("in_app", true, SourceTopicDefault, false),
				resolved("email", true, SourceTopicDefault, false),
				resolved("sms", true, SourceTopicDefault, false),
				resolved("push", false, SourceRecipient, false),
				resolved("slack", true, SourceTenant, false)},
			"extras follow the topic's own channels, sorted"},
		{"a channel named by both levels is listed once", inviteDef,
			map[ChannelKind]bool{"push": true}, map[ChannelKind]TenantChoice{"push": {Enabled: false}},
			[]ResolvedPreference{
				resolved("in_app", true, SourceTopicDefault, false),
				resolved("email", true, SourceTopicDefault, false),
				resolved("sms", true, SourceTopicDefault, false),
				resolved("push", true, SourceRecipient, false)},
			""},
		{"a channel the definition lists twice is resolved once", TopicDef{
			DefaultChannels: []ChannelKind{"email", "email"}, Fallback: []ChannelKind{"email"}},
			nil, nil, []ResolvedPreference{resolved("email", true, SourceTopicDefault, false)},
			"TopicDef.Validate refuses it, but a definition that was not validated must not double a channel"},
		{"a topic with no channels resolves to nothing", TopicDef{}, nil, nil, nil, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ResolvePreferences("invite", tc.def, tc.recipient, tc.tenant)
			if len(got) == 0 && len(tc.want) == 0 {
				return
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("ResolvePreferences =\n  %+v\nwant\n  %+v\n%s", got, tc.want, tc.why)
			}
		})
	}

	t.Run("it does not change the maps it is given", func(t *testing.T) {
		t.Parallel()
		recipient := map[ChannelKind]bool{"push": true}
		tenant := map[ChannelKind]TenantChoice{"slack": {Enabled: true}}
		ResolvePreferences("invite", inviteDef, recipient, tenant)
		if len(recipient) != 1 || len(tenant) != 1 {
			t.Fatalf("maps changed: %v, %v", recipient, tenant)
		}
	})
}

// A preference names a topic and a channel; a tenant default also names a tenant.
// An empty component is refused rather than stored: the data layer cannot tell it
// from an unset scope (D17).
func TestPreferenceValidate(t *testing.T) {
	t.Parallel()
	good := Preference{Topic: "invite", Channel: "email", Enabled: true}
	cases := []struct {
		name   string
		mutate func(*Preference)
		fields []string
	}{
		{"a complete pair", func(*Preference) {}, nil},
		{"no topic", func(p *Preference) { p.Topic = "" }, []string{"Topic"}},
		{"no channel", func(p *Preference) { p.Channel = "" }, []string{"Channel"}},
		{"neither, reported together", func(p *Preference) { p.Topic, p.Channel = "", "" }, []string{"Topic", "Channel"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := good
			tc.mutate(&p)
			assertProblems(t, p.Validate(), tc.fields)
		})
	}
}

func TestTenantPreferenceValidate(t *testing.T) {
	t.Parallel()
	good := TenantPreference{Tenant: Tenant{Kind: "workspace", ID: "w1"}, Topic: "invite", Channel: "email", Locked: true}
	cases := []struct {
		name   string
		mutate func(*TenantPreference)
		fields []string
		is     error
	}{
		{"a complete default", func(*TenantPreference) {}, nil, nil},
		{"no tenant kind", func(p *TenantPreference) { p.Tenant.Kind = "" }, []string{"Tenant"}, ErrEmptyIdentity},
		{"no tenant id", func(p *TenantPreference) { p.Tenant.ID = "" }, []string{"Tenant"}, ErrEmptyIdentity},
		{"no topic", func(p *TenantPreference) { p.Topic = "" }, []string{"Topic"}, nil},
		{"no channel", func(p *TenantPreference) { p.Channel = "" }, []string{"Channel"}, nil},
		{"every field, reported together", func(p *TenantPreference) { *p = TenantPreference{} }, []string{"Tenant", "Topic", "Channel"}, ErrEmptyIdentity},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := good
			tc.mutate(&p)
			err := p.Validate()
			assertProblems(t, err, tc.fields)
			if tc.is != nil && !errors.Is(err, tc.is) {
				t.Errorf("err %v does not wrap %v", err, tc.is)
			}
		})
	}
}

// assertProblems checks that err names exactly the fields, in order, or is nil.
func assertProblems(t *testing.T, err error, fields []string) {
	t.Helper()
	if len(fields) == 0 {
		if err != nil {
			t.Fatalf("err = %v, want none", err)
		}
		return
	}
	var v *ValidationError
	if !errors.As(err, &v) {
		t.Fatalf("err = %v, want a *ValidationError naming %v", err, fields)
	}
	var got []string
	for _, p := range v.Problems {
		got = append(got, p.Field)
	}
	if !reflect.DeepEqual(got, fields) {
		t.Fatalf("problems on %v, want %v", got, fields)
	}
}
