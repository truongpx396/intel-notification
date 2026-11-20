package app_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	notify "github.com/truongpx396/intel-notification"
	"github.com/truongpx396/intel-notification/app"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/notifytest"
)

// T044: an operator's default for a tenant. What it refuses it refuses with the reason, and a refusal
// writes nothing; what it accepts reaches the store under the realm the operator administers, which is
// an argument and never part of the preference (NR-009).

// recPrefs counts the tenant defaults the store was asked to write, so a refusal can be shown to have
// stopped before the store and not merely to have been refused by it too.
type recPrefs struct {
	*notifytest.Preferences
	sets int
}

func (r *recPrefs) SetTenant(ctx context.Context, realm domain.Realm, p domain.TenantPreference) error {
	r.sets++
	return r.Preferences.SetTenant(ctx, realm, p)
}

func tenantFx(t *testing.T) (*app.Admin, *recPrefs) {
	t.Helper()
	topics := notifytest.NewTopics()
	topics.Register(planRealm, topicOpen, domain.TopicDef{DefaultChannels: []domain.ChannelKind{"in_app", "email"}, DefaultPriority: domain.PriorityInfo})
	topics.Register(planRealm, topicEssential, domain.TopicDef{DefaultChannels: []domain.ChannelKind{"in_app", "email"}, DefaultPriority: domain.PriorityCritical, Essential: true})
	probe := notifytest.NewProbe()
	channels := notifytest.NewRegistry(
		notifytest.NewChannel("in_app", domain.ChannelCapabilities{InboxBacked: true, Dedup: domain.DedupLocal}, probe),
		notifytest.NewChannel("email", domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupProvider}, probe))
	prefs := &recPrefs{Preferences: notifytest.NewPreferences()}
	a, err := app.NewAdmin(notify.Config{Realm: planRealm, StoreDSN: "x", RedisURL: "y"},
		app.Deps{Maintenance: &recMaintenance{ev: &events{}}, Prefs: prefs, Topics: topics, Channels: channels})
	if err != nil {
		t.Fatalf("NewAdmin: %v", err)
	}
	return a, prefs
}

func TestSetTenantPreferenceReachesTheStoreForTheOperatorsRealm(t *testing.T) {
	t.Parallel()
	a, prefs := tenantFx(t)
	err := a.SetTenantPreference(t.Context(), planRealm, domain.TenantPreference{Tenant: planID.Tenant, Topic: topicOpen, Channel: "email", Enabled: false, Locked: true})
	if err != nil {
		t.Fatalf("SetTenantPreference: %v", err)
	}
	rs, err := prefs.Resolve(t.Context(), planID, topicOpen, domain.TopicDef{DefaultChannels: []domain.ChannelKind{"in_app", "email"}, DefaultPriority: domain.PriorityInfo})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rs {
		if r.Channel == "email" && (r.Enabled || !r.Locked || r.Source != domain.SourceTenant) {
			t.Errorf("email = %+v, want off, locked, from the tenant", r)
		}
		if r.Channel == "in_app" && r.Source != domain.SourceTopicDefault {
			t.Errorf("in_app = %+v, want untouched", r)
		}
	}
	// The same default under another realm is not this realm's.
	other := domain.Identity{Realm: "rival", Tenant: planID.Tenant, Recipient: planID.Recipient}
	rs, _ = prefs.Resolve(t.Context(), other, topicOpen, domain.TopicDef{DefaultChannels: []domain.ChannelKind{"email"}, DefaultPriority: domain.PriorityInfo})
	if len(rs) != 1 || rs[0].Source == domain.SourceTenant {
		t.Errorf("another realm resolved %+v, want the topic default: a tenant's default is its realm's", rs)
	}
}

func TestSetTenantPreferenceAcceptsWhatKeepsAnEssentialTopicOn(t *testing.T) {
	t.Parallel()
	a, _ := tenantFx(t)
	if err := a.SetTenantPreference(t.Context(), planRealm, domain.TenantPreference{Tenant: planID.Tenant, Topic: topicEssential, Channel: "email", Enabled: true, Locked: true}); err != nil {
		t.Fatalf("SetTenantPreference(enabled, locked, essential) = %v, want it accepted: it keeps the topic on", err)
	}
}

func TestSetTenantPreferenceRefusesWithTheReasonAndWritesNothing(t *testing.T) {
	t.Parallel()
	good := domain.TenantPreference{Tenant: planID.Tenant, Topic: topicOpen, Channel: "email", Enabled: false, Locked: true}
	cases := []struct {
		name   string
		mutate func(*domain.TenantPreference)
		is     error
		names  []string
	}{
		{"an essential topic disabled", func(p *domain.TenantPreference) { p.Topic = topicEssential }, domain.ErrPreferenceLocked, []string{string(topicEssential), "essential"}},
		{"an unregistered topic", func(p *domain.TenantPreference) { p.Topic = "nope" }, domain.ErrUnknownTopic, []string{"nope"}},
		{"an unregistered channel", func(p *domain.TenantPreference) { p.Channel = "pigeon" }, domain.ErrUnknownChannel, []string{"pigeon"}},
		{"no tenant", func(p *domain.TenantPreference) { p.Tenant = domain.Tenant{} }, domain.ErrEmptyIdentity, []string{"Tenant"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			a, prefs := tenantFx(t)
			p := good
			tc.mutate(&p)
			err := a.SetTenantPreference(t.Context(), planRealm, p)
			if !errors.Is(err, tc.is) {
				t.Fatalf("SetTenantPreference = %v, want it to match %v", err, tc.is)
			}
			var ve *domain.ValidationError
			if !errors.As(err, &ve) {
				t.Errorf("SetTenantPreference = %v, want a *domain.ValidationError", err)
			}
			for _, want := range tc.names {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the refusal %q does not name %q", err, want)
				}
			}
			if prefs.sets != 0 {
				t.Errorf("the store was asked to write %d times for a refused default, want none", prefs.sets)
			}
			rs, _ := prefs.Resolve(t.Context(), planID, topicOpen, domain.TopicDef{DefaultChannels: []domain.ChannelKind{"email"}, DefaultPriority: domain.PriorityInfo})
			if len(rs) != 1 || rs[0].Source == domain.SourceTenant {
				t.Errorf("%+v: a refused default was stored", rs)
			}
		})
	}
}

func TestNewAdminNamesEveryPortItNeeds(t *testing.T) {
	t.Parallel()
	_, err := app.NewAdmin(notify.Config{Realm: planRealm, StoreDSN: "x", RedisURL: "y"}, app.Deps{})
	if err == nil {
		t.Fatal("NewAdmin with no ports succeeded")
	}
	for _, port := range []string{"Deps.Maintenance", "Deps.Prefs", "Deps.Topics", "Deps.Channels"} {
		if !strings.Contains(err.Error(), port) {
			t.Errorf("error %q does not name %s", err, port)
		}
	}
}
