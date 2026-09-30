package notifytest

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/truongpx396/intel-notification/domain"
)

var (
	alice = who("aisat", "w1", "alice")
	// inviteDef: email and in_app in parallel, sms as the fallback.
	inviteDef = domain.TopicDef{
		DefaultChannels: []domain.ChannelKind{"in_app", "email"},
		Fallback:        []domain.ChannelKind{"sms"},
		DefaultPriority: domain.PriorityInfo,
	}
)

// resolve returns the resolved preferences of topic "invite" by channel, and the
// channels in the order returned.
func resolve(t *testing.T, p *Preferences, id domain.Identity, def domain.TopicDef) (map[domain.ChannelKind]domain.ResolvedPreference, []domain.ChannelKind) {
	t.Helper()
	got, err := p.Resolve(t.Context(), id, "invite", def)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	by := map[domain.ChannelKind]domain.ResolvedPreference{}
	var order []domain.ChannelKind
	for _, r := range got {
		if r.Topic != "invite" {
			t.Fatalf("Resolve returned a preference for topic %q, want invite", r.Topic)
		}
		if _, dup := by[r.Channel]; dup {
			t.Fatalf("Resolve returned channel %q twice", r.Channel)
		}
		by[r.Channel] = r
		order = append(order, r.Channel)
	}
	return by, order
}

func setRecipient(t *testing.T, p *Preferences, id domain.Identity, ch domain.ChannelKind, enabled bool) {
	t.Helper()
	if err := p.Set(t.Context(), id, []domain.Preference{{Topic: "invite", Channel: ch, Enabled: enabled}}); err != nil {
		t.Fatal(err)
	}
}

func setTenant(t *testing.T, p *Preferences, id domain.Identity, ch domain.ChannelKind, enabled, locked bool) {
	t.Helper()
	tp := domain.TenantPreference{Tenant: id.Tenant, Topic: "invite", Channel: ch, Enabled: enabled, Locked: locked}
	if err := p.SetTenant(t.Context(), id.Realm, tp); err != nil {
		t.Fatal(err)
	}
}

type level int

const (
	unset level = iota
	on
	off
	lockedOn
	lockedOff
)

// The resolution matrix of D30 and NR-011, for a channel the topic enables by
// default: a recipient choice, a tenant choice, and the topic default, asserting
// the winner and where it came from. A locked tenant default beats the recipient's
// opposite choice, and beats their agreement too, because it is the level that
// decided.
func TestPreferencesResolutionMatrix(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name         string
		recipient    level // unset, on, off
		tenant       level // unset, on, off, lockedOn, lockedOff
		wantEnabled  bool
		wantSource   domain.PreferenceSource
		wantLocked   bool
		because      string
		channelIsSMS bool // resolve the fallback channel instead, which the topic also enables
	}{
		{"nothing set: the topic's default", unset, unset, true, domain.SourceTopicDefault, false, "", false},
		{"the recipient enables", on, unset, true, domain.SourceRecipient, false, "", false},
		{"the recipient disables", off, unset, false, domain.SourceRecipient, false, "", false},
		{"the tenant's default, unlocked, enabling", unset, on, true, domain.SourceTenant, false, "an absent recipient choice falls back to the tenant's default (NR-011)", false},
		{"the tenant's default, unlocked, disabling", unset, off, false, domain.SourceTenant, false, "", false},
		{"the recipient overrides an unlocked tenant default to enable", on, off, true, domain.SourceRecipient, false, "a recipient's row wins unless the tenant's is locked (D30)", false},
		{"the recipient overrides an unlocked tenant default to disable", off, on, false, domain.SourceRecipient, false, "", false},
		{"a locked tenant default beats the recipient's opposite choice: off", on, lockedOff, false, domain.SourceTenant, true, "a tenant default marked locked overrides the recipient's choice (NR-011)", false},
		{"a locked tenant default beats the recipient's opposite choice: on", off, lockedOn, true, domain.SourceTenant, true, "", false},
		{"a locked tenant default with no recipient choice", unset, lockedOff, false, domain.SourceTenant, true, "", false},
		{"a locked tenant default the recipient agrees with", on, lockedOn, true, domain.SourceTenant, true, "the level that decided is reported, even when they agree", false},
		{"the fallback channel is enabled by the topic's default", unset, unset, true, domain.SourceTopicDefault, false, "the fallback chain is part of the topic's default set", true},
		{"the fallback channel can be disabled by the recipient", off, unset, false, domain.SourceRecipient, false, "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := NewPreferences()
			ch := domain.ChannelKind("email")
			if tc.channelIsSMS {
				ch = "sms"
			}
			switch tc.recipient {
			case on:
				setRecipient(t, p, alice, ch, true)
			case off:
				setRecipient(t, p, alice, ch, false)
			}
			switch tc.tenant {
			case on:
				setTenant(t, p, alice, ch, true, false)
			case off:
				setTenant(t, p, alice, ch, false, false)
			case lockedOn:
				setTenant(t, p, alice, ch, true, true)
			case lockedOff:
				setTenant(t, p, alice, ch, false, true)
			}
			by, _ := resolve(t, p, alice, inviteDef)
			got, ok := by[ch]
			if !ok {
				t.Fatalf("%s is not among the resolved channels %v", ch, by)
			}
			if got.Enabled != tc.wantEnabled || got.Source != tc.wantSource || got.Locked != tc.wantLocked {
				t.Fatalf("%s resolved to enabled=%v source=%s locked=%v, want enabled=%v source=%s locked=%v: %s",
					ch, got.Enabled, got.Source, got.Locked, tc.wantEnabled, tc.wantSource, tc.wantLocked, tc.because)
			}
		})
	}
}

// The resolved set is the topic's channels — defaults, then the fallback chain —
// and any other channel a row names. A channel nothing mentions is not there.
func TestPreferencesResolvedChannelSet(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("the topic's channels, in order, and nothing else", func(t *testing.T) {
		t.Parallel()
		_, order := resolve(t, NewPreferences(), alice, inviteDef)
		if want := []domain.ChannelKind{"in_app", "email", "sms"}; !slices.Equal(order, want) {
			t.Fatalf("channels = %v, want %v: defaults in order, then the fallback chain", order, want)
		}
	})

	t.Run("a channel only a recipient row names is enabled by that row", func(t *testing.T) {
		t.Parallel()
		p := NewPreferences()
		setRecipient(t, p, alice, "push", true)
		by, order := resolve(t, p, alice, inviteDef)
		if got := by["push"]; !got.Enabled || got.Source != domain.SourceRecipient {
			t.Fatalf("push = %+v, want enabled by the recipient", got)
		}
		if want := []domain.ChannelKind{"in_app", "email", "sms", "push"}; !slices.Equal(order, want) {
			t.Fatalf("channels = %v, want the extra channel after the topic's own: %v", order, want)
		}
	})

	t.Run("a channel only a tenant row names resolves by that row", func(t *testing.T) {
		t.Parallel()
		p := NewPreferences()
		setTenant(t, p, alice, "push", true, false)
		by, _ := resolve(t, p, alice, inviteDef)
		if got := by["push"]; !got.Enabled || got.Source != domain.SourceTenant {
			t.Fatalf("push = %+v, want enabled by the tenant's default", got)
		}
	})

	t.Run("extra channels come back sorted", func(t *testing.T) {
		t.Parallel()
		p := NewPreferences()
		setRecipient(t, p, alice, "webhook", true)
		setRecipient(t, p, alice, "push", true)
		setTenant(t, p, alice, "slack", true, false)
		_, order := resolve(t, p, alice, inviteDef)
		if want := []domain.ChannelKind{"in_app", "email", "sms", "push", "slack", "webhook"}; !slices.Equal(order, want) {
			t.Fatalf("channels = %v, want %v", order, want)
		}
	})

	t.Run("a row for a default channel does not list it twice", func(t *testing.T) {
		t.Parallel()
		p := NewPreferences()
		setRecipient(t, p, alice, "email", false)
		setTenant(t, p, alice, "email", true, false)
		_, order := resolve(t, p, alice, inviteDef) // resolve fails on a duplicate
		if want := []domain.ChannelKind{"in_app", "email", "sms"}; !slices.Equal(order, want) {
			t.Fatalf("channels = %v, want %v", order, want)
		}
	})

	t.Run("a topic with no channels resolves to nothing", func(t *testing.T) {
		t.Parallel()
		got, err := NewPreferences().Resolve(ctx, alice, "invite", domain.TopicDef{DefaultPriority: domain.PriorityInfo})
		if err != nil || len(got) != 0 {
			t.Fatalf("Resolve = %v, %v, want nothing", got, err)
		}
	})
}

// Each (topic, channel) pair is independent (D3): turning email off leaves in_app
// on, and a row for one topic says nothing of another.
func TestPreferencesPairsResolveIndependently(t *testing.T) {
	t.Parallel()
	p := NewPreferences()
	setRecipient(t, p, alice, "email", false)
	by, _ := resolve(t, p, alice, inviteDef)
	if by["email"].Enabled || !by["in_app"].Enabled || !by["sms"].Enabled {
		t.Fatalf("disabling email changed another channel: %+v", by)
	}

	other, err := p.Resolve(t.Context(), alice, "a_different_topic", inviteDef)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range other {
		if r.Channel == "email" && (!r.Enabled || r.Source != domain.SourceTopicDefault) {
			t.Fatalf("a row for one topic changed another: %+v", r)
		}
	}
}

// Essential topics cannot be disabled at any level (D30): a default channel of one
// resolves enabled and locked, whatever a recipient or a locked tenant row says.
func TestPreferencesEssentialTopicsCannotBeDisabled(t *testing.T) {
	t.Parallel()
	essential := inviteDef
	essential.Essential = true

	cases := []struct {
		name  string
		setup func(*testing.T, *Preferences)
	}{
		{"nothing set", func(*testing.T, *Preferences) {}},
		{"the recipient disables", func(t *testing.T, p *Preferences) { t.Helper(); setRecipient(t, p, alice, "email", false) }},
		{"the tenant disables, unlocked", func(t *testing.T, p *Preferences) { t.Helper(); setTenant(t, p, alice, "email", false, false) }},
		{"the tenant locks it off", func(t *testing.T, p *Preferences) { t.Helper(); setTenant(t, p, alice, "email", false, true) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := NewPreferences()
			tc.setup(t, p)
			by, _ := resolve(t, p, alice, essential)
			for _, ch := range []domain.ChannelKind{"in_app", "email", "sms"} {
				got := by[ch]
				if !got.Enabled || !got.Locked || got.Source != domain.SourceTopicDefault {
					t.Fatalf("%s = %+v, want enabled, locked, from the topic default", ch, got)
				}
			}
		})
	}

	t.Run("a channel the topic does not enable is left to its rows", func(t *testing.T) {
		t.Parallel()
		p := NewPreferences()
		setRecipient(t, p, alice, "push", true)
		by, _ := resolve(t, p, alice, essential)
		if got := by["push"]; !got.Enabled || got.Source != domain.SourceRecipient || got.Locked {
			t.Fatalf("push = %+v, want the recipient's own choice, unlocked", got)
		}
	})

	t.Run("the same rows on a non-essential topic do disable it", func(t *testing.T) {
		t.Parallel()
		p := NewPreferences()
		setRecipient(t, p, alice, "email", false)
		by, _ := resolve(t, p, alice, inviteDef)
		if by["email"].Enabled {
			t.Fatal("a non-essential topic's channel could not be disabled")
		}
	})
}

// One recipient's preferences never appear for another (NS-001 shape), and a
// tenant's defaults apply to that tenant's recipients only.
func TestPreferencesAreScoped(t *testing.T) {
	t.Parallel()
	p := NewPreferences()
	setRecipient(t, p, alice, "email", false)
	setTenant(t, p, alice, "in_app", false, true)

	otherKind := alice
	otherKind.Recipient.Kind = "device"
	cases := []struct {
		name string
		id   domain.Identity
	}{
		{"another recipient in the tenant", who("aisat", "w1", "bob")},
		{"the same recipient id in another tenant", who("aisat", "w2", "alice")},
		{"the same recipient id in another realm", who("other", "w1", "alice")},
		{"the same recipient id under another kind", otherKind},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			by, _ := resolve(t, p, tc.id, inviteDef)
			if tc.id.Tenant == alice.Tenant && tc.id.Realm == alice.Realm {
				// Same tenant: the tenant's lock applies, the recipient's row does not.
				if by["in_app"].Enabled || !by["email"].Enabled {
					t.Fatalf("%s: in_app = %+v, email = %+v, want the tenant lock but not alice's own row", tc.name, by["in_app"], by["email"])
				}
				return
			}
			for ch, r := range by {
				if !r.Enabled || r.Source != domain.SourceTopicDefault {
					t.Fatalf("%s: %s = %+v, want the topic's default: nothing of alice's or her tenant's leaks", tc.name, ch, r)
				}
			}
		})
	}
}

func TestPreferencesSetUpserts(t *testing.T) {
	t.Parallel()
	p := NewPreferences()
	setRecipient(t, p, alice, "email", false)
	setRecipient(t, p, alice, "in_app", false)
	setRecipient(t, p, alice, "email", true)
	by, _ := resolve(t, p, alice, inviteDef)
	if !by["email"].Enabled {
		t.Fatal("a later Set did not replace the earlier choice")
	}
	if by["in_app"].Enabled {
		t.Fatal("setting email changed in_app: a pair not named is left as it was")
	}

	// Several pairs in one call.
	if err := p.Set(t.Context(), alice, []domain.Preference{
		{Topic: "invite", Channel: "email", Enabled: false},
		{Topic: "invite", Channel: "sms", Enabled: false},
	}); err != nil {
		t.Fatal(err)
	}
	by, _ = resolve(t, p, alice, inviteDef)
	if by["email"].Enabled || by["sms"].Enabled {
		t.Fatalf("a Set naming two pairs left %+v", by)
	}
}

func TestPreferencesSetTenantUpserts(t *testing.T) {
	t.Parallel()
	p := NewPreferences()
	setTenant(t, p, alice, "email", false, true)
	setTenant(t, p, alice, "email", true, false)
	by, _ := resolve(t, p, alice, inviteDef)
	if got := by["email"]; !got.Enabled || got.Locked || got.Source != domain.SourceTenant {
		t.Fatalf("email = %+v, want the later tenant row: enabled, unlocked", got)
	}
}

func TestPreferencesSchedule(t *testing.T) {
	t.Parallel()
	p := NewPreferences()

	got, err := p.Schedule(t.Context(), alice)
	if err != nil || got != (domain.DeliverySchedule{}) {
		t.Fatalf("Schedule before any Set = %+v, %v, want the zero schedule: immediate, no quiet hours", got, err)
	}

	want := domain.DeliverySchedule{QuietStart: "22:00", QuietEnd: "07:00", Timezone: "Europe/Paris", Digest: 15 * time.Minute}
	if err := p.SetSchedule(t.Context(), alice, want); err != nil {
		t.Fatal(err)
	}
	if got, _ := p.Schedule(t.Context(), alice); got != want {
		t.Fatalf("Schedule = %+v, want the round trip %+v", got, want)
	}

	if other, _ := p.Schedule(t.Context(), who("aisat", "w1", "bob")); other != (domain.DeliverySchedule{}) {
		t.Fatalf("another recipient's Schedule = %+v, want none of alice's", other)
	}
	if other, _ := p.Schedule(t.Context(), who("aisat", "w2", "alice")); other != (domain.DeliverySchedule{}) {
		t.Fatalf("the same recipient id in another tenant has Schedule %+v, want none", other)
	}

	replaced := domain.DeliverySchedule{Digest: time.Minute}
	if err := p.SetSchedule(t.Context(), alice, replaced); err != nil {
		t.Fatal(err)
	}
	if got, _ := p.Schedule(t.Context(), alice); got != replaced {
		t.Fatalf("Schedule = %+v, want the replacement %+v: SetSchedule replaces, it does not merge", got, replaced)
	}
}
