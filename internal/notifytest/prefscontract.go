package notifytest

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// PreferenceStoreFactory returns an empty ports.PreferenceStore for t alone: a
// fresh fake, or a store over a database cloned for the test.
type PreferenceStoreFactory func(t *testing.T) ports.PreferenceStore

// PreferenceStoreContract is the table every ports.PreferenceStore runs: the
// in-memory fake in this package and the PostgreSQL store alike, so neither can
// drift from what the other decided where the contract leaves room (T013a, T018).
// It asserts the resolution of D30 and NR-011 through the port, and the plumbing
// under it: that the rows read are the recipient's and the tenant's own and the
// topic's, that writes upsert, and that a schedule round-trips.
//
// Every case builds its own store with newStore.
func PreferenceStoreContract(t *testing.T, newStore PreferenceStoreFactory) {
	t.Helper()

	t.Run("resolution matrix (D30, NR-011)", func(t *testing.T) {
		t.Parallel()
		cases := []struct {
			name         string
			recipient    prefLevel // unset, on, off
			tenant       prefLevel // unset, on, off, lockedOn, lockedOff
			wantEnabled  bool
			wantSource   domain.PreferenceSource
			wantLocked   bool
			because      string
			channelIsSMS bool // resolve the fallback channel instead, which the topic also enables
		}{
			{"nothing set: the topic's default", prefUnset, prefUnset, true, domain.SourceTopicDefault, false, "", false},
			{"the recipient enables", prefOn, prefUnset, true, domain.SourceRecipient, false, "", false},
			{"the recipient disables", prefOff, prefUnset, false, domain.SourceRecipient, false, "", false},
			{"the tenant's default, unlocked, enabling", prefUnset, prefOn, true, domain.SourceTenant, false, "an absent recipient choice falls back to the tenant's default (NR-011)", false},
			{"the tenant's default, unlocked, disabling", prefUnset, prefOff, false, domain.SourceTenant, false, "", false},
			{"the recipient overrides an unlocked tenant default to enable", prefOn, prefOff, true, domain.SourceRecipient, false, "a recipient's row wins unless the tenant's is locked (D30)", false},
			{"the recipient overrides an unlocked tenant default to disable", prefOff, prefOn, false, domain.SourceRecipient, false, "", false},
			{"a locked tenant default beats the recipient's opposite choice: off", prefOn, prefLockedOff, false, domain.SourceTenant, true, "a tenant default marked locked overrides the recipient's choice (NR-011)", false},
			{"a locked tenant default beats the recipient's opposite choice: on", prefOff, prefLockedOn, true, domain.SourceTenant, true, "", false},
			{"a locked tenant default with no recipient choice", prefUnset, prefLockedOff, false, domain.SourceTenant, true, "", false},
			{"a locked tenant default the recipient agrees with", prefOn, prefLockedOn, true, domain.SourceTenant, true, "the level that decided is reported, even when they agree", false},
			{"the fallback channel is enabled by the topic's default", prefUnset, prefUnset, true, domain.SourceTopicDefault, false, "the fallback chain is part of the topic's default set", true},
			{"the fallback channel can be disabled by the recipient", prefOff, prefUnset, false, domain.SourceRecipient, false, "", true},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				p := newStore(t)
				ch := domain.ChannelKind("email")
				if tc.channelIsSMS {
					ch = "sms"
				}
				switch tc.recipient {
				case prefOn:
					prefSetRecipient(t, p, prefAlice, ch, true)
				case prefOff:
					prefSetRecipient(t, p, prefAlice, ch, false)
				}
				switch tc.tenant {
				case prefOn:
					prefSetTenant(t, p, prefAlice, ch, true, false)
				case prefOff:
					prefSetTenant(t, p, prefAlice, ch, false, false)
				case prefLockedOn:
					prefSetTenant(t, p, prefAlice, ch, true, true)
				case prefLockedOff:
					prefSetTenant(t, p, prefAlice, ch, false, true)
				}
				by, _ := prefResolve(t, p, prefAlice, prefInviteDef)
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
	})

	t.Run("resolved channel set", func(t *testing.T) {
		t.Parallel()

		t.Run("the topic's channels, in order, and nothing else", func(t *testing.T) {
			t.Parallel()
			_, order := prefResolve(t, newStore(t), prefAlice, prefInviteDef)
			if want := []domain.ChannelKind{"in_app", "email", "sms"}; !slices.Equal(order, want) {
				t.Fatalf("channels = %v, want %v: defaults in order, then the fallback chain", order, want)
			}
		})

		t.Run("a channel only a recipient row names is enabled by that row", func(t *testing.T) {
			t.Parallel()
			p := newStore(t)
			prefSetRecipient(t, p, prefAlice, "push", true)
			by, order := prefResolve(t, p, prefAlice, prefInviteDef)
			if got := by["push"]; !got.Enabled || got.Source != domain.SourceRecipient {
				t.Fatalf("push = %+v, want enabled by the recipient", got)
			}
			if want := []domain.ChannelKind{"in_app", "email", "sms", "push"}; !slices.Equal(order, want) {
				t.Fatalf("channels = %v, want the extra channel after the topic's own: %v", order, want)
			}
		})

		t.Run("a channel only a tenant row names resolves by that row", func(t *testing.T) {
			t.Parallel()
			p := newStore(t)
			prefSetTenant(t, p, prefAlice, "push", true, false)
			by, _ := prefResolve(t, p, prefAlice, prefInviteDef)
			if got := by["push"]; !got.Enabled || got.Source != domain.SourceTenant {
				t.Fatalf("push = %+v, want enabled by the tenant's default", got)
			}
		})

		t.Run("extra channels come back sorted", func(t *testing.T) {
			t.Parallel()
			p := newStore(t)
			prefSetRecipient(t, p, prefAlice, "webhook", true)
			prefSetRecipient(t, p, prefAlice, "push", true)
			prefSetTenant(t, p, prefAlice, "slack", true, false)
			_, order := prefResolve(t, p, prefAlice, prefInviteDef)
			if want := []domain.ChannelKind{"in_app", "email", "sms", "push", "slack", "webhook"}; !slices.Equal(order, want) {
				t.Fatalf("channels = %v, want %v", order, want)
			}
		})

		t.Run("an extra channel named by two rows is listed once", func(t *testing.T) {
			t.Parallel()
			p := newStore(t)
			prefSetRecipient(t, p, prefAlice, "push", true)
			prefSetTenant(t, p, prefAlice, "push", false, false)
			by, order := prefResolve(t, p, prefAlice, prefInviteDef) // prefResolve fails on a duplicate
			if want := []domain.ChannelKind{"in_app", "email", "sms", "push"}; !slices.Equal(order, want) {
				t.Fatalf("channels = %v, want %v", order, want)
			}
			if got := by["push"]; !got.Enabled || got.Source != domain.SourceRecipient {
				t.Fatalf("push = %+v, want the recipient's choice over the tenant's unlocked default", got)
			}
		})

		t.Run("a row for a default channel does not list it twice", func(t *testing.T) {
			t.Parallel()
			p := newStore(t)
			prefSetRecipient(t, p, prefAlice, "email", false)
			prefSetTenant(t, p, prefAlice, "email", true, false)
			_, order := prefResolve(t, p, prefAlice, prefInviteDef) // prefResolve fails on a duplicate
			if want := []domain.ChannelKind{"in_app", "email", "sms"}; !slices.Equal(order, want) {
				t.Fatalf("channels = %v, want %v", order, want)
			}
		})

		t.Run("a topic with no channels resolves to nothing", func(t *testing.T) {
			t.Parallel()
			got, err := newStore(t).Resolve(t.Context(), prefAlice, "invite", domain.TopicDef{DefaultPriority: domain.PriorityInfo})
			if err != nil || len(got) != 0 {
				t.Fatalf("Resolve = %v, %v, want nothing", got, err)
			}
		})
	})

	t.Run("pairs resolve independently (D3)", func(t *testing.T) {
		t.Parallel()
		p := newStore(t)
		prefSetRecipient(t, p, prefAlice, "email", false)
		by, _ := prefResolve(t, p, prefAlice, prefInviteDef)
		if by["email"].Enabled || !by["in_app"].Enabled || !by["sms"].Enabled {
			t.Fatalf("disabling email changed another channel: %+v", by)
		}

		prefSetRecipient(t, p, prefAlice, "push", true) // a channel only a row for "invite" names
		prefSetTenant(t, p, prefAlice, "slack", true, true)
		prefSetTenant(t, p, prefAlice, "email", false, true) // a lock on invite's email, not on another topic's
		other, err := p.Resolve(t.Context(), prefAlice, "a_different_topic", prefInviteDef)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range other {
			if r.Channel == "push" || r.Channel == "slack" {
				t.Fatalf("a row for one topic added a channel to another: %+v", r)
			}
			if r.Channel == "email" && (!r.Enabled || r.Source != domain.SourceTopicDefault) {
				t.Fatalf("a row for one topic changed another: %+v", r)
			}
		}
	})

	t.Run("essential topics cannot be disabled", func(t *testing.T) {
		t.Parallel()
		essential := prefInviteDef
		essential.Essential = true

		cases := []struct {
			name  string
			setup func(*testing.T, ports.PreferenceStore)
		}{
			{"nothing set", func(*testing.T, ports.PreferenceStore) {}},
			{"the recipient disables", func(t *testing.T, p ports.PreferenceStore) {
				t.Helper()
				prefSetRecipient(t, p, prefAlice, "email", false)
			}},
			{"the tenant disables, unlocked", func(t *testing.T, p ports.PreferenceStore) {
				t.Helper()
				prefSetTenant(t, p, prefAlice, "email", false, false)
			}},
			{"the tenant locks it off", func(t *testing.T, p ports.PreferenceStore) {
				t.Helper()
				prefSetTenant(t, p, prefAlice, "email", false, true)
			}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				p := newStore(t)
				tc.setup(t, p)
				by, _ := prefResolve(t, p, prefAlice, essential)
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
			p := newStore(t)
			prefSetRecipient(t, p, prefAlice, "push", true)
			by, _ := prefResolve(t, p, prefAlice, essential)
			if got := by["push"]; !got.Enabled || got.Source != domain.SourceRecipient || got.Locked {
				t.Fatalf("push = %+v, want the recipient's own choice, unlocked", got)
			}
		})

		t.Run("the same rows on a non-essential topic do disable it", func(t *testing.T) {
			t.Parallel()
			p := newStore(t)
			prefSetRecipient(t, p, prefAlice, "email", false)
			by, _ := prefResolve(t, p, prefAlice, prefInviteDef)
			if by["email"].Enabled {
				t.Fatal("a non-essential topic's channel could not be disabled")
			}
		})
	})

	t.Run("preferences are scoped (NS-001)", func(t *testing.T) {
		t.Parallel()
		p := newStore(t)
		prefSetRecipient(t, p, prefAlice, "email", false)
		prefSetRecipient(t, p, prefAlice, "push", true) // a channel only alice's own row names
		prefSetTenant(t, p, prefAlice, "in_app", false, true)
		prefSetTenant(t, p, prefAlice, "slack", true, false) // a channel only the tenant's row names

		otherKind := prefAlice
		otherKind.Recipient.Kind = "device"
		otherTenantKind := prefAlice
		otherTenantKind.Tenant.Kind = "organization" // the same tenant id, under another kind
		cases := []struct {
			name string
			id   domain.Identity
		}{
			{"another recipient in the tenant", prefWho("aisat", "w1", "bob")},
			{"the same recipient id in another tenant", prefWho("aisat", "w2", "alice")},
			{"the same recipient id in another realm", prefWho("other", "w1", "alice")},
			{"the same recipient id under another kind", otherKind},
			{"the same tenant id under another kind", otherTenantKind},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				by, _ := prefResolve(t, p, tc.id, prefInviteDef)
				if _, leaked := by["push"]; leaked {
					t.Fatalf("%s: alice's own push choice appears: %+v", tc.name, by["push"])
				}
				if tc.id.Tenant == prefAlice.Tenant && tc.id.Realm == prefAlice.Realm {
					// Same tenant: the tenant's lock and its default apply, the recipient's row does not.
					if by["in_app"].Enabled || !by["email"].Enabled {
						t.Fatalf("%s: in_app = %+v, email = %+v, want the tenant lock but not alice's own row", tc.name, by["in_app"], by["email"])
					}
					if got := by["slack"]; !got.Enabled || got.Source != domain.SourceTenant {
						t.Fatalf("%s: slack = %+v, want the tenant's default, which applies to its recipients", tc.name, got)
					}
					return
				}
				if _, leaked := by["slack"]; leaked {
					t.Fatalf("%s: the tenant's slack default appears: %+v", tc.name, by["slack"])
				}
				for ch, r := range by {
					if !r.Enabled || r.Source != domain.SourceTopicDefault {
						t.Fatalf("%s: %s = %+v, want the topic's default: nothing of alice's or her tenant's leaks", tc.name, ch, r)
					}
				}
			})
		}
	})

	t.Run("Set upserts", func(t *testing.T) {
		t.Parallel()
		p := newStore(t)
		prefSetRecipient(t, p, prefAlice, "email", false)
		prefSetRecipient(t, p, prefAlice, "in_app", false)
		prefSetRecipient(t, p, prefAlice, "email", true)
		by, _ := prefResolve(t, p, prefAlice, prefInviteDef)
		if !by["email"].Enabled {
			t.Fatal("a later Set did not replace the earlier choice")
		}
		if by["in_app"].Enabled {
			t.Fatal("setting email changed in_app: a pair not named is left as it was")
		}

		// Several pairs in one call.
		if err := p.Set(t.Context(), prefAlice, []domain.Preference{
			{Topic: "invite", Channel: "email", Enabled: false},
			{Topic: "invite", Channel: "sms", Enabled: false},
		}); err != nil {
			t.Fatal(err)
		}
		by, _ = prefResolve(t, p, prefAlice, prefInviteDef)
		if by["email"].Enabled || by["sms"].Enabled {
			t.Fatalf("a Set naming two pairs left %+v", by)
		}

		// The same pair twice in one call: the later one wins.
		if err := p.Set(t.Context(), prefAlice, []domain.Preference{
			{Topic: "invite", Channel: "sms", Enabled: true},
			{Topic: "invite", Channel: "sms", Enabled: false},
		}); err != nil {
			t.Fatalf("a Set naming a pair twice: %v", err)
		}
		by, _ = prefResolve(t, p, prefAlice, prefInviteDef)
		if by["sms"].Enabled {
			t.Fatal("a Set naming sms on, then off, left it on")
		}

		// Nothing to write is not an error.
		if err := p.Set(t.Context(), prefAlice, nil); err != nil {
			t.Fatalf("an empty Set: %v", err)
		}
	})

	t.Run("SetTenant upserts", func(t *testing.T) {
		t.Parallel()
		p := newStore(t)
		prefSetTenant(t, p, prefAlice, "email", false, true)
		prefSetTenant(t, p, prefAlice, "email", true, false)
		by, _ := prefResolve(t, p, prefAlice, prefInviteDef)
		if got := by["email"]; !got.Enabled || got.Locked || got.Source != domain.SourceTenant {
			t.Fatalf("email = %+v, want the later tenant row: enabled, unlocked", got)
		}
	})

	t.Run("a write is refused whole or not at all", func(t *testing.T) {
		t.Parallel()

		t.Run("a Set with one bad pair writes none of the others", func(t *testing.T) {
			t.Parallel()
			p := newStore(t)
			prefSetRecipient(t, p, prefAlice, "sms", false) // a control: a good write is accepted
			err := p.Set(t.Context(), prefAlice, []domain.Preference{
				{Topic: "invite", Channel: "email", Enabled: false},
				{Topic: "invite", Channel: "", Enabled: false},
			})
			if err == nil {
				t.Fatal("a pair with no channel was accepted")
			}
			by, _ := prefResolve(t, p, prefAlice, prefInviteDef)
			if !by["email"].Enabled {
				t.Fatal("the good pair of a refused Set was written")
			}
			if by["sms"].Enabled {
				t.Fatal("the control write was lost")
			}
		})

		t.Run("a pair with no topic is refused", func(t *testing.T) {
			t.Parallel()
			p := newStore(t)
			prefSetRecipient(t, p, prefAlice, "sms", false) // a control: a good write is accepted
			if err := p.Set(t.Context(), prefAlice, []domain.Preference{{Channel: "email"}}); err == nil {
				t.Fatal("accepted")
			}
		})

		t.Run("an identity with an empty component is refused", func(t *testing.T) {
			t.Parallel()
			p := newStore(t)
			empty := prefAlice
			empty.Recipient.ID = ""
			pref := []domain.Preference{{Topic: "invite", Channel: "email"}}
			// Controls: the same calls for a complete identity are accepted.
			if err := p.Set(t.Context(), prefAlice, pref); err != nil {
				t.Fatalf("Set for a complete identity: %v", err)
			}
			if err := p.SetSchedule(t.Context(), prefAlice, domain.DeliverySchedule{Digest: time.Minute}); err != nil {
				t.Fatalf("SetSchedule for a complete identity: %v", err)
			}
			if err := p.Set(t.Context(), empty, pref); !errors.Is(err, domain.ErrEmptyIdentity) {
				t.Errorf("Set: err = %v, want ErrEmptyIdentity", err)
			}
			if err := p.SetSchedule(t.Context(), empty, domain.DeliverySchedule{Digest: time.Minute}); !errors.Is(err, domain.ErrEmptyIdentity) {
				t.Errorf("SetSchedule: err = %v, want ErrEmptyIdentity", err)
			}
			if _, err := p.Schedule(t.Context(), empty); !errors.Is(err, domain.ErrEmptyIdentity) {
				t.Errorf("Schedule: err = %v, want ErrEmptyIdentity", err)
			}
			if _, err := p.Resolve(t.Context(), empty, "invite", prefInviteDef); !errors.Is(err, domain.ErrEmptyIdentity) {
				t.Errorf("Resolve: err = %v, want ErrEmptyIdentity", err)
			}
		})

		t.Run("a tenant default with an empty tenant, topic or channel is refused", func(t *testing.T) {
			t.Parallel()
			p := newStore(t)
			good := domain.TenantPreference{Tenant: prefAlice.Tenant, Topic: "invite", Channel: "email"}
			if err := p.SetTenant(t.Context(), prefAlice.Realm, good); err != nil { // a control
				t.Fatalf("a complete tenant default: %v", err)
			}
			for name, mutate := range map[string]func(*domain.TenantPreference){
				"no tenant id":   func(tp *domain.TenantPreference) { tp.Tenant.ID = "" },
				"no tenant kind": func(tp *domain.TenantPreference) { tp.Tenant.Kind = "" },
				"no topic":       func(tp *domain.TenantPreference) { tp.Topic = "" },
				"no channel":     func(tp *domain.TenantPreference) { tp.Channel = "" },
			} {
				bad := good
				mutate(&bad)
				if err := p.SetTenant(t.Context(), prefAlice.Realm, bad); err == nil {
					t.Errorf("%s: accepted", name)
				}
			}
			if err := p.SetTenant(t.Context(), "", good); err == nil {
				t.Error("a tenant default in no realm was accepted")
			}
		})
	})

	t.Run("the schedule round-trips", func(t *testing.T) {
		t.Parallel()
		p := newStore(t)

		got, err := p.Schedule(t.Context(), prefAlice)
		if err != nil || got != (domain.DeliverySchedule{}) {
			t.Fatalf("Schedule before any Set = %+v, %v, want the zero schedule: immediate, no quiet hours", got, err)
		}

		want := domain.DeliverySchedule{QuietStart: "22:00", QuietEnd: "07:00", Timezone: "Europe/Paris", Digest: 15 * time.Minute}
		if err := p.SetSchedule(t.Context(), prefAlice, want); err != nil {
			t.Fatal(err)
		}
		if got, _ := p.Schedule(t.Context(), prefAlice); got != want {
			t.Fatalf("Schedule = %+v, want the round trip %+v", got, want)
		}

		if other, _ := p.Schedule(t.Context(), prefWho("aisat", "w1", "bob")); other != (domain.DeliverySchedule{}) {
			t.Fatalf("another recipient's Schedule = %+v, want none of alice's", other)
		}
		if other, _ := p.Schedule(t.Context(), prefWho("aisat", "w2", "alice")); other != (domain.DeliverySchedule{}) {
			t.Fatalf("the same recipient id in another tenant has Schedule %+v, want none", other)
		}
		if other, _ := p.Schedule(t.Context(), prefWho("other", "w1", "alice")); other != (domain.DeliverySchedule{}) {
			t.Fatalf("the same recipient id in another realm has Schedule %+v, want none", other)
		}

		replaced := domain.DeliverySchedule{Digest: time.Minute}
		if err := p.SetSchedule(t.Context(), prefAlice, replaced); err != nil {
			t.Fatal(err)
		}
		if got, _ := p.Schedule(t.Context(), prefAlice); got != replaced {
			t.Fatalf("Schedule = %+v, want the replacement %+v: SetSchedule replaces, it does not merge", got, replaced)
		}

		// A window need not be a whole number of seconds, and a window of nothing is the
		// immediate schedule, not an absent one.
		for _, d := range []time.Duration{90*time.Minute + 30*time.Second + 250*time.Millisecond, 0} {
			s := domain.DeliverySchedule{QuietStart: "23:30", QuietEnd: "06:15", Timezone: "UTC", Digest: d}
			if err := p.SetSchedule(t.Context(), prefAlice, s); err != nil {
				t.Fatal(err)
			}
			if got, _ := p.Schedule(t.Context(), prefAlice); got != s {
				t.Fatalf("Schedule = %+v, want the round trip %+v", got, s)
			}
		}

		if err := p.SetSchedule(t.Context(), prefAlice, domain.DeliverySchedule{Digest: -time.Second}); err == nil {
			t.Fatal("a negative digest window was accepted")
		}
		if got, _ := p.Schedule(t.Context(), prefAlice); got.Digest != 0 {
			t.Fatalf("a refused SetSchedule changed the schedule to %+v", got)
		}
	})

}

var (
	prefAlice = prefWho("aisat", "w1", "alice")
	// prefInviteDef: email and in_app in parallel, sms as the fallback.
	prefInviteDef = domain.TopicDef{
		DefaultChannels: []domain.ChannelKind{"in_app", "email"},
		Fallback:        []domain.ChannelKind{"sms"},
		DefaultPriority: domain.PriorityInfo,
	}
)

func prefWho(realm domain.Realm, tenant, recipient string) domain.Identity {
	return domain.Identity{
		Realm:     realm,
		Tenant:    domain.Tenant{Kind: "workspace", ID: tenant},
		Recipient: domain.Recipient{Kind: "user", ID: recipient},
	}
}

// prefResolve returns the resolved preferences of topic "invite" by channel, and the
// channels in the order returned.
func prefResolve(t *testing.T, p ports.PreferenceStore, id domain.Identity, def domain.TopicDef) (map[domain.ChannelKind]domain.ResolvedPreference, []domain.ChannelKind) {
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

func prefSetRecipient(t *testing.T, p ports.PreferenceStore, id domain.Identity, ch domain.ChannelKind, enabled bool) {
	t.Helper()
	if err := p.Set(t.Context(), id, []domain.Preference{{Topic: "invite", Channel: ch, Enabled: enabled}}); err != nil {
		t.Fatal(err)
	}
}

func prefSetTenant(t *testing.T, p ports.PreferenceStore, id domain.Identity, ch domain.ChannelKind, enabled, locked bool) {
	t.Helper()
	tp := domain.TenantPreference{Tenant: id.Tenant, Topic: "invite", Channel: ch, Enabled: enabled, Locked: locked}
	if err := p.SetTenant(t.Context(), id.Realm, tp); err != nil {
		t.Fatal(err)
	}
}

type prefLevel int

const (
	prefUnset prefLevel = iota
	prefOn
	prefOff
	prefLockedOn
	prefLockedOff
)
