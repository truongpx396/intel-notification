package app_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	notify "github.com/truongpx396/intel-notification"
	"github.com/truongpx396/intel-notification/app"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/notifytest"
	"github.com/truongpx396/intel-notification/ports"
)

// T044: what the REST surface's preference, schedule and unsubscribe routes stand on, with no database
// and no transport. The PreferenceStore is the kit's, held to the real one's table (T018); the
// guarantees asserted here are the use-case's own: what it refuses, with what reason, and that a
// refusal writes nothing.

const (
	topicOpen      domain.Topic = "ingestion_complete"
	topicEssential domain.Topic = "credit_exhausted"
	topicInAppOnly domain.Topic = "weekly_tips"
)

var surfaceKey = []byte("0123456789abcdef0123456789abcdef")

// getStore is an InboxStore that answers Get from a map and remembers the ids it was asked for.
type getStore struct {
	ports.InboxStore
	items map[string]*domain.Notification
	asked []string
	ids   []domain.Identity
}

func (s *getStore) Get(_ context.Context, id domain.Identity, nid string) (*domain.Notification, error) {
	s.asked = append(s.asked, nid)
	s.ids = append(s.ids, id)
	return s.items[nid], nil
}

type surface struct {
	inbox     *app.Inbox
	prefs     *notifytest.Preferences
	templates *notifytest.Templates
	store     *getStore
	signer    *domain.UnsubscribeSigner
	clock     *notifytest.Clock
}

func newSurface(t *testing.T, withSigner bool) *surface {
	t.Helper()
	probe := notifytest.NewProbe()
	channels := notifytest.NewRegistry(
		notifytest.NewChannel("in_app", domain.ChannelCapabilities{InboxBacked: true, Dedup: domain.DedupLocal}, probe),
		notifytest.NewChannel("email", domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupProvider}, probe),
	)
	topics := notifytest.NewTopics()
	topics.Register(planRealm, topicOpen, domain.TopicDef{DefaultChannels: []domain.ChannelKind{"in_app", "email"}, DefaultPriority: domain.PriorityInfo})
	topics.Register(planRealm, topicEssential, domain.TopicDef{DefaultChannels: []domain.ChannelKind{"in_app", "email"}, DefaultPriority: domain.PriorityCritical, Essential: true})
	topics.Register(planRealm, topicInAppOnly, domain.TopicDef{DefaultChannels: []domain.ChannelKind{"in_app"}, DefaultPriority: domain.PriorityInfo})
	topics.Register("rival", "rivals_topic", domain.TopicDef{DefaultChannels: []domain.ChannelKind{"in_app"}, DefaultPriority: domain.PriorityInfo})

	s := &surface{
		prefs: notifytest.NewPreferences(), templates: notifytest.NewTemplates("en"),
		store: &getStore{items: map[string]*domain.Notification{}}, clock: notifytest.NewClock(time.Date(2026, 3, 4, 10, 0, 0, 0, time.UTC)),
	}
	deps := app.Deps{Channels: channels, Topics: topics, Renderer: s.templates, Prefs: s.prefs, Inbox: s.store, Clock: s.clock}
	if withSigner {
		signer, err := domain.NewUnsubscribeSigner("k1", map[string][]byte{"k1": surfaceKey}, domain.MinUnsubscribeTTL)
		if err != nil {
			t.Fatal(err)
		}
		s.signer, deps.UnsubscribeSigner = signer, signer
	}
	inbox, err := app.NewInbox(notify.Config{Realm: planRealm, StoreDSN: "x", RedisURL: "y"}, deps)
	if err != nil {
		t.Fatalf("NewInbox: %v", err)
	}
	s.inbox = inbox
	return s
}

func resolved(t *testing.T, s *surface, topic domain.Topic) map[domain.ChannelKind]domain.ResolvedPreference {
	t.Helper()
	def := map[domain.Topic]domain.TopicDef{
		topicOpen:      {DefaultChannels: []domain.ChannelKind{"in_app", "email"}, DefaultPriority: domain.PriorityInfo},
		topicEssential: {DefaultChannels: []domain.ChannelKind{"in_app", "email"}, DefaultPriority: domain.PriorityCritical, Essential: true},
		topicInAppOnly: {DefaultChannels: []domain.ChannelKind{"in_app"}, DefaultPriority: domain.PriorityInfo},
	}[topic]
	rs, err := s.prefs.Resolve(t.Context(), planID, topic, def)
	if err != nil {
		t.Fatal(err)
	}
	out := map[domain.ChannelKind]domain.ResolvedPreference{}
	for _, r := range rs {
		out[r.Channel] = r
	}
	return out
}

func TestNewInboxNeedsThePreferenceStore(t *testing.T) {
	t.Parallel()
	_, err := app.NewInbox(notify.Config{Realm: planRealm, StoreDSN: "x", RedisURL: "y"}, app.Deps{
		Channels: notifytest.NewRegistry(), Inbox: struct{ ports.InboxStore }{}, Renderer: notifytest.NewTemplates("en"), Topics: notifytest.NewTopics()})
	if err == nil || !strings.Contains(err.Error(), "Deps.Prefs") {
		t.Fatalf("NewInbox with no PreferenceStore = %v, want an error naming Deps.Prefs", err)
	}
}

// A preferences screen is drawn from the realm's own topics, one entry per (topic, channel), each saying
// where its value came from and whether the recipient may change it (D30).
func TestPreferencesListsTheRealmsTopicsWithTheirSourceAndLock(t *testing.T) {
	t.Parallel()
	s := newSurface(t, false)
	ctx := t.Context()
	if err := s.prefs.SetTenant(ctx, planRealm, domain.TenantPreference{Tenant: planID.Tenant, Topic: topicInAppOnly, Channel: "in_app", Enabled: true, Locked: true}); err != nil {
		t.Fatal(err)
	}
	if err := s.prefs.Set(ctx, planID, []domain.Preference{{Topic: topicOpen, Channel: "email", Enabled: false}}); err != nil {
		t.Fatal(err)
	}

	got, err := s.inbox.Preferences(ctx, planID)
	if err != nil {
		t.Fatalf("Preferences: %v", err)
	}
	type pair struct {
		topic   domain.Topic
		channel domain.ChannelKind
	}
	by := map[pair]domain.ResolvedPreference{}
	var order []pair
	for _, r := range got {
		by[pair{r.Topic, r.Channel}] = r
		order = append(order, pair{r.Topic, r.Channel})
	}
	if len(got) != 5 {
		t.Fatalf("%d preferences %v, want 5: two for each two-channel topic and one for the other; rival's topic is not ours", len(got), order)
	}
	if _, leaked := by[pair{"rivals_topic", "in_app"}]; leaked {
		t.Error("another realm's topic is listed")
	}
	if order[0].topic != topicEssential || order[len(order)-1].topic != topicInAppOnly {
		t.Errorf("order %v, want the topics sorted", order)
	}
	checks := []struct {
		p       pair
		enabled bool
		source  domain.PreferenceSource
		locked  bool
	}{
		{pair{topicOpen, "email"}, false, domain.SourceRecipient, false},
		{pair{topicOpen, "in_app"}, true, domain.SourceTopicDefault, false},
		{pair{topicEssential, "email"}, true, domain.SourceTopicDefault, true},
		{pair{topicInAppOnly, "in_app"}, true, domain.SourceTenant, true},
	}
	for _, c := range checks {
		r := by[c.p]
		if r.Enabled != c.enabled || r.Source != c.source || r.Locked != c.locked {
			t.Errorf("%v = %+v, want enabled %v from %s, locked %v", c.p, r, c.enabled, c.source, c.locked)
		}
	}
}

func TestSetPreferencesWritesWhatTheRecipientMayChange(t *testing.T) {
	t.Parallel()
	s := newSurface(t, false)
	err := s.inbox.SetPreferences(t.Context(), planID, []domain.Preference{
		{Topic: topicOpen, Channel: "email", Enabled: false}, {Topic: topicInAppOnly, Channel: "in_app", Enabled: false}})
	if err != nil {
		t.Fatalf("SetPreferences: %v", err)
	}
	if r := resolved(t, s, topicOpen)["email"]; r.Enabled || r.Source != domain.SourceRecipient {
		t.Errorf("email on %s = %+v, want the recipient's own choice, off", topicOpen, r)
	}
	if r := resolved(t, s, topicInAppOnly)["in_app"]; r.Enabled {
		t.Errorf("in_app on %s = %+v, want off", topicInAppOnly, r)
	}
}

// A pair that cannot be changed is refused with its reason, never silently ignored (rest-api.md). A
// request is all or none: a screen that saved half of what was asked would show choices that are not
// the ones in force.
func TestSetPreferencesRefusesWithTheReasonAndWritesNothing(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		prefs []domain.Preference
		is    error
		names []string // what the message must name, so a settings screen can say it
	}{
		{"an unregistered topic", []domain.Preference{{Topic: "nope", Channel: "email"}}, domain.ErrUnknownTopic, []string{"nope"}},
		{"an unregistered channel", []domain.Preference{{Topic: topicOpen, Channel: "carrier_pigeon"}}, domain.ErrUnknownChannel, []string{"carrier_pigeon"}},
		{"another realm's topic", []domain.Preference{{Topic: "rivals_topic", Channel: "in_app"}}, domain.ErrUnknownTopic, []string{"rivals_topic"}},
		{"an essential topic's own channel", []domain.Preference{{Topic: topicEssential, Channel: "email", Enabled: false}}, domain.ErrPreferenceLocked, []string{string(topicEssential), "email", "essential"}},
		{"an essential topic, even to the value it has", []domain.Preference{{Topic: topicEssential, Channel: "email", Enabled: true}}, domain.ErrPreferenceLocked, []string{string(topicEssential), "essential"}},
		{"a pair the tenant locked", []domain.Preference{{Topic: topicInAppOnly, Channel: "in_app", Enabled: false}}, domain.ErrPreferenceLocked, []string{string(topicInAppOnly), "in_app", "tenant"}},
		{"a pair with no topic", []domain.Preference{{Channel: "email"}}, nil, []string{"Topic"}},
		{"a good pair beside a refused one", []domain.Preference{{Topic: topicOpen, Channel: "email", Enabled: false}, {Topic: topicEssential, Channel: "in_app", Enabled: false}}, domain.ErrPreferenceLocked, []string{string(topicEssential)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := newSurface(t, false)
			if err := s.prefs.SetTenant(t.Context(), planRealm, domain.TenantPreference{Tenant: planID.Tenant, Topic: topicInAppOnly, Channel: "in_app", Enabled: true, Locked: true}); err != nil {
				t.Fatal(err)
			}
			err := s.inbox.SetPreferences(t.Context(), planID, tc.prefs)
			var ve *domain.ValidationError
			if !errors.As(err, &ve) {
				t.Fatalf("SetPreferences = %v, want a *domain.ValidationError", err)
			}
			if tc.is != nil && !errors.Is(err, tc.is) {
				t.Errorf("SetPreferences = %v, want it to match %v", err, tc.is)
			}
			for _, want := range tc.names {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the refusal %q does not name %q", err, want)
				}
			}
			if r := resolved(t, s, topicOpen)["email"]; r.Source == domain.SourceRecipient {
				t.Errorf("%+v: a refused request wrote its good half", r)
			}
		})
	}
}

// Every refused pair is named in one answer, so a screen shows them all and not one per attempt.
func TestSetPreferencesNamesEveryRefusedPairAtOnce(t *testing.T) {
	t.Parallel()
	s := newSurface(t, false)
	err := s.inbox.SetPreferences(t.Context(), planID, []domain.Preference{
		{Channel: "email"}, // the pair that has no topic is the first, so a loop that stopped at it would name one
		{Topic: "nope", Channel: "email"}, {Topic: topicEssential, Channel: "email"}, {Topic: topicOpen, Channel: "pigeon"}})
	var ve *domain.ValidationError
	if !errors.As(err, &ve) || len(ve.Problems) != 4 {
		t.Fatalf("SetPreferences = %v, want a ValidationError with 4 problems, one per refused pair", err)
	}
}

func TestSetScheduleValidatesBeforeItStoresAnything(t *testing.T) {
	t.Parallel()
	s := newSurface(t, false)
	ctx := t.Context()
	bad := domain.DeliverySchedule{QuietStart: "22:00", QuietEnd: "07:00", Timezone: "Mars/Olympus_Mons"}
	if err := s.inbox.SetSchedule(ctx, planID, bad); err == nil {
		t.Fatal("SetSchedule accepted a timezone nobody has")
	}
	if got, _ := s.inbox.Schedule(ctx, planID); got != (domain.DeliverySchedule{}) {
		t.Errorf("Schedule = %+v after a refused write, want the default: the planner does not apply a schedule that fails Validate", got)
	}

	good := domain.DeliverySchedule{QuietStart: "22:00", QuietEnd: "07:00", Timezone: "Europe/Paris", Digest: time.Hour}
	if err := s.inbox.SetSchedule(ctx, planID, good); err != nil {
		t.Fatalf("SetSchedule: %v", err)
	}
	if got, err := s.inbox.Schedule(ctx, planID); err != nil || got != good {
		t.Errorf("Schedule = %+v, %v, want %+v", got, err, good)
	}
}

// One-click unsubscribe disables exactly the one pair the token names (D34, NR-015): not the topic's
// other channels, not the recipient's other topics, nobody else.
func TestUnsubscribeDisablesExactlyTheTokensPair(t *testing.T) {
	t.Parallel()
	s := newSurface(t, true)
	token, err := s.signer.Sign(domain.UnsubscribeScope{Identity: planID, Topic: topicOpen, Channel: "email"}, s.clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.inbox.Unsubscribe(t.Context(), token); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	if r := resolved(t, s, topicOpen)["email"]; r.Enabled || r.Source != domain.SourceRecipient {
		t.Errorf("email on %s = %+v, want off by the recipient's choice", topicOpen, r)
	}
	if r := resolved(t, s, topicOpen)["in_app"]; !r.Enabled || r.Source != domain.SourceTopicDefault {
		t.Errorf("in_app on %s = %+v, want untouched: the token named one channel", topicOpen, r)
	}
	if r := resolved(t, s, topicInAppOnly)["in_app"]; !r.Enabled {
		t.Errorf("another topic's switch was turned off: %+v", r)
	}
	other := domain.Identity{Realm: planRealm, Tenant: planID.Tenant, Recipient: domain.Recipient{Kind: "user", ID: "someone-else"}}
	rs, _ := s.prefs.Resolve(t.Context(), other, topicOpen, domain.TopicDef{DefaultChannels: []domain.ChannelKind{"email"}, DefaultPriority: domain.PriorityInfo})
	if len(rs) != 1 || !rs[0].Enabled {
		t.Errorf("another recipient's resolution = %+v, want untouched", rs)
	}
	// A second click is the same click.
	if err := s.inbox.Unsubscribe(t.Context(), token); err != nil {
		t.Errorf("a second Unsubscribe = %v, want none: mail clients retry", err)
	}
}

func TestUnsubscribeRefusesWhatIsNotAGoodTokenAndWritesNothing(t *testing.T) {
	t.Parallel()
	good := func(s *surface) string {
		token, err := s.signer.Sign(domain.UnsubscribeScope{Identity: planID, Topic: topicOpen, Channel: "email"}, s.clock.Now())
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	cases := []struct {
		name  string
		build func(s *surface) string
		is    error
	}{
		{"an altered token", func(s *surface) string { tk := good(s); return tk[:len(tk)-2] + "AA" }, domain.ErrInvalidUnsubscribeToken},
		{"a token that is not one", func(*surface) string { return "not-a-token" }, domain.ErrInvalidUnsubscribeToken},
		{"an empty token", func(*surface) string { return "" }, domain.ErrInvalidUnsubscribeToken},
		{"a token that has lapsed", func(s *surface) string {
			tk := good(s)
			s.clock.Advance(domain.MinUnsubscribeTTL + time.Second)
			return tk
		}, domain.ErrExpiredUnsubscribeToken},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := newSurface(t, true)
			err := s.inbox.Unsubscribe(t.Context(), tc.build(s))
			if !errors.Is(err, tc.is) {
				t.Fatalf("Unsubscribe = %v, want it to match %v", err, tc.is)
			}
			if r := resolved(t, s, topicOpen)["email"]; !r.Enabled {
				t.Errorf("%+v: a refused token turned the switch off", r)
			}
		})
	}
}

// An engine with no signer issued no tokens, so any it is shown is not one of its own.
func TestUnsubscribeWithNoSignerRefusesEveryToken(t *testing.T) {
	t.Parallel()
	s := newSurface(t, false)
	if err := s.inbox.Unsubscribe(t.Context(), "v1.k1.e30.AAAA"); !errors.Is(err, domain.ErrInvalidUnsubscribeToken) {
		t.Fatalf("Unsubscribe = %v, want domain.ErrInvalidUnsubscribeToken", err)
	}
}

// Get is what the relay loads for a nudge. It asks the store under the caller's identity for the id it
// was given, renders what comes back as a list does, and says "none" for what the store does not
// return, which is the same answer for "absent" and "someone else's" (NR-008).
func TestGetRendersOneNotificationUnderTheCallersIdentity(t *testing.T) {
	t.Parallel()
	s := newSurface(t, false)
	const nid = "0195f3a0-0000-7000-8000-000000000001"
	s.store.items[nid] = &domain.Notification{Topic: topicOpen, Priority: domain.PriorityWarning, Title: "stored title", Body: "stored body",
		Data: map[string]any{"name": "Atlas"}, Payload: map[string]string{"url": "/a"}}
	s.templates.Put(planRealm, string(topicOpen), "in_app", "en", domain.RenderedContent{Subject: "Rendered title", Body: "Rendered body"})

	item, err := s.inbox.Get(t.Context(), planID, nid, "")
	if err != nil || item == nil {
		t.Fatalf("Get = %+v, %v, want the item", item, err)
	}
	if item.ID != nid || item.Topic != topicOpen || item.Priority != domain.PriorityWarning || item.Title != "Rendered title" || item.Body != "Rendered body" ||
		item.Data["name"] != "Atlas" || item.Payload["url"] != "/a" {
		t.Errorf("item = %+v, want the notification rendered from its template", item)
	}
	if len(s.store.ids) != 1 || s.store.ids[0] != planID || s.store.asked[0] != nid {
		t.Errorf("the store was asked %v as %v, want %s as the caller", s.store.asked, s.store.ids, nid)
	}

	if none, err := s.inbox.Get(t.Context(), planID, "0195f3a0-0000-7000-8000-00000000ffff", ""); err != nil || none != nil {
		t.Errorf("Get of a notification the store does not return = %+v, %v, want nil and no error", none, err)
	}
}

// The relay's toast reads in the viewer's language, as the list does: the locale is the viewer's, and
// the renderer's ladder (tag, language, default) is the same one.
func TestGetRendersInTheViewersLocale(t *testing.T) {
	t.Parallel()
	s := newSurface(t, false)
	const nid = "0195f3a0-0000-7000-8000-000000000003"
	s.store.items[nid] = &domain.Notification{Topic: topicOpen, Title: "stored", Body: "stored"}
	s.templates.Put(planRealm, string(topicOpen), "in_app", "en", domain.RenderedContent{Subject: "Done", Body: "english"})
	s.templates.Put(planRealm, string(topicOpen), "in_app", "fr", domain.RenderedContent{Subject: "Terminé", Body: "français"})

	if item, _ := s.inbox.Get(t.Context(), planID, nid, "fr"); item == nil || item.Title != "Terminé" {
		t.Errorf("Get(fr) = %+v, want the French template", item)
	}
	if item, _ := s.inbox.Get(t.Context(), planID, nid, ""); item == nil || item.Title != "Done" {
		t.Errorf("Get(\"\") = %+v, want the default locale's template", item)
	}
}

func TestGetKeepsTheStoredCopyWhenNoTemplateMatches(t *testing.T) {
	t.Parallel()
	s := newSurface(t, false)
	const nid = "0195f3a0-0000-7000-8000-000000000002"
	s.store.items[nid] = &domain.Notification{Topic: topicOpen, Title: "stored title", Body: "stored body"}
	item, err := s.inbox.Get(t.Context(), planID, nid, "")
	if err != nil || item == nil || item.Title != "stored title" || item.Body != "stored body" {
		t.Fatalf("Get = %+v, %v, want the stored copy: no template exists", item, err)
	}
}
