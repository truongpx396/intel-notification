package app_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	notify "github.com/truongpx396/intel-notification"
	"github.com/truongpx396/intel-notification/app"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/notifytest"
	"github.com/truongpx396/intel-notification/ports"
)

// The planner (T021) is tested with no infrastructure: a fixed clock and the kit's
// fakes, one row per rule, from a notification to the DeliveryPlan the store
// writes.

const planRealm domain.Realm = "aisat"

var (
	noon       = time.Date(2031, 3, 1, 12, 0, 0, 0, time.UTC)
	planTenant = domain.Tenant{Kind: "workspace", ID: "w1"}
	planUser   = domain.Recipient{Kind: "user", ID: "u1"}
	planID     = domain.Identity{Realm: planRealm, Tenant: planTenant, Recipient: planUser}
)

// planFx is one row's world. A row's setup changes it before the planner is built.
type planFx struct {
	t        *testing.T
	clock    *notifytest.Clock
	topics   *notifytest.Topics
	prefs    *notifytest.Preferences
	quotas   *notifytest.QuotaCounter
	def      domain.TopicDef
	caps     map[domain.ChannelKind]domain.ChannelCapabilities
	cfg      notify.Config
	deps     app.Deps // what Plan is built from; setup may swap a port for a failing one
	noPolicy bool     // build with no QuotaPolicies
}

func newPlanFx(t *testing.T) *planFx {
	t.Helper()
	f := &planFx{
		t:      t,
		clock:  notifytest.NewClock(noon),
		topics: notifytest.NewTopics(),
		prefs:  notifytest.NewPreferences(),
		quotas: notifytest.NewQuotaCounter(),
		def: domain.TopicDef{
			DefaultChannels: []domain.ChannelKind{"in_app", "email"},
			Fallback:        []domain.ChannelKind{"sms"},
			DefaultPriority: domain.PriorityInfo,
		},
		caps: map[domain.ChannelKind]domain.ChannelCapabilities{
			"in_app": {InboxBacked: true},
			"email":  {NeedsAddress: true, NeedsSubject: true},
			"sms":    {NeedsAddress: true},
			"push":   {NeedsAddress: true},
		},
		cfg: notify.Config{Realm: planRealm, StoreDSN: "x", RedisURL: "y", DigestMax: 50, Shards: 7},
	}
	return f
}

func (f *planFx) planner(t *testing.T) *app.Planner {
	t.Helper()
	f.topics.Register(planRealm, "ingestion_complete", f.def)
	reg := notifytest.NewRegistry()
	probe := notifytest.NewProbe()
	for kind, caps := range f.caps {
		reg.Register(notifytest.NewChannel(kind, caps, probe))
	}
	d := f.deps
	if d.Topics == nil {
		d.Topics = f.topics
	}
	if d.Prefs == nil {
		d.Prefs = f.prefs
	}
	if d.Quotas == nil {
		d.Quotas = f.quotas
	}
	if d.QuotaPolicies == nil && !f.noPolicy {
		d.QuotaPolicies = f.quotas
	}
	d.Channels, d.Clock = reg, f.clock
	p, err := app.NewPlanner(f.cfg, d)
	if err != nil {
		t.Fatalf("NewPlanner: %v", err)
	}
	return p
}

func (f *planFx) note() domain.Notification {
	return domain.Notification{Tenant: planTenant, Recipient: planUser, Topic: "ingestion_complete", IdemKey: "k1"}
}

func (f *planFx) quotaKey(ch domain.ChannelKind) domain.QuotaKey {
	return domain.QuotaKey{Realm: planRealm, Tenant: planTenant, Channel: ch}
}

func (f *planFx) setPref(ch domain.ChannelKind, enabled bool) {
	f.t.Helper()
	if err := f.prefs.Set(f.t.Context(), planID, []domain.Preference{{Topic: "ingestion_complete", Channel: ch, Enabled: enabled}}); err != nil {
		f.t.Fatal(err)
	}
}

func (f *planFx) setSchedule(s domain.DeliverySchedule) {
	f.t.Helper()
	if err := f.prefs.SetSchedule(f.t.Context(), planID, s); err != nil {
		f.t.Fatal(err)
	}
}

// wd is one planned delivery as a row expects it.
type wd struct {
	ch     domain.ChannelKind
	mode   domain.PlanMode
	at     time.Time
	reason string
	fb     []domain.ChannelKind
	quiet  bool
}

var sms = []domain.ChannelKind{"sms"}

// imm is an immediate delivery carrying the base topic's fallback chain.
func imm(ch domain.ChannelKind) wd { return wd{ch: ch, mode: domain.PlanImmediate, fb: sms} }

func deferred(ch domain.ChannelKind, at time.Time, reason string) wd {
	return wd{ch: ch, mode: domain.PlanDeferred, at: at, reason: reason, fb: sms}
}

func digest(ch domain.ChannelKind) wd { return wd{ch: ch, mode: domain.PlanDigest} }

// wantPlan is what a row expects.
type wantPlan struct {
	ds          []wd
	visible     bool
	visibleFrom time.Time
	window      time.Duration
	digestMax   int
	priority    domain.Priority
	shard       *domain.Shard
}

func (w wantPlan) check(t *testing.T, got app.Planned) {
	t.Helper()
	p := got.Plan
	if p.InboxVisible != w.visible {
		t.Errorf("InboxVisible = %v, want %v", p.InboxVisible, w.visible)
	}
	if !p.VisibleFrom.Equal(w.visibleFrom) {
		t.Errorf("VisibleFrom = %v, want %v", p.VisibleFrom, w.visibleFrom)
	}
	if p.DigestWindow != w.window || p.DigestMax != w.digestMax {
		t.Errorf("digest window %v and max %d, want %v and %d", p.DigestWindow, p.DigestMax, w.window, w.digestMax)
	}
	wantShard := domain.ShardFor(planID, 7)
	if w.shard != nil {
		wantShard = *w.shard
	}
	if p.Shard != wantShard {
		t.Errorf("Shard = %d, want %d: the identity's shard over the configured count", p.Shard, wantShard)
	}
	if w.priority != "" && got.Notification.Priority != w.priority {
		t.Errorf("effective priority = %q, want %q", got.Notification.Priority, w.priority)
	}
	if len(p.Deliveries) != len(w.ds) {
		t.Fatalf("%d deliveries %+v, want %d %+v", len(p.Deliveries), p.Deliveries, len(w.ds), w.ds)
	}
	for i, want := range w.ds {
		d := p.Deliveries[i]
		if d.Channel != want.ch || d.Mode != want.mode || !d.NotBefore.Equal(want.at) || d.Reason != want.reason ||
			!slices.Equal(d.Fallback, want.fb) || d.QuietHoursOverride != want.quiet {
			t.Errorf("delivery %d = %+v, want channel %s mode %d not before %v reason %q fallback %v quiet-override %v",
				i, d, want.ch, want.mode, want.at, want.reason, want.fb, want.quiet)
		}
	}
}

func at(y int, m time.Month, d, h, mi int) time.Time {
	return time.Date(y, m, d, h, mi, 0, 0, time.UTC)
}

func TestPlan(t *testing.T) {
	t.Parallel()
	quiet1113 := domain.DeliverySchedule{QuietStart: "11:00", QuietEnd: "13:00", Timezone: "UTC"}
	quiet2207 := domain.DeliverySchedule{QuietStart: "22:00", QuietEnd: "07:00", Timezone: "UTC"}
	cadence := domain.DeliverySchedule{Digest: 15 * time.Minute}

	cases := []struct {
		name    string
		why     string
		setup   func(f *planFx)
		mutate  func(n *domain.Notification)
		want    wantPlan
		after   func(f *planFx)
		wantErr error
	}{
		// ------------------------------------------------------ channels, preferences
		{name: "every default channel is immediate, in the topic's order, carrying the fallback chain",
			want: wantPlan{ds: []wd{imm("in_app"), imm("email")}, visible: true, priority: domain.PriorityInfo}},
		{name: "a disabled channel is absent (NR-011)",
			setup: func(f *planFx) { f.setPref("email", false) },
			want:  wantPlan{ds: []wd{imm("in_app")}, visible: true}},
		{name: "every channel disabled gives an empty plan, and the notification is not inbox-visible (NR-012)",
			setup: func(f *planFx) { f.setPref("email", false); f.setPref("in_app", false) },
			want:  wantPlan{visible: false}},
		{name: "inbox visibility follows InboxBacked: disabling the inbox-backed channel hides it",
			setup: func(f *planFx) { f.setPref("in_app", false) },
			want:  wantPlan{ds: []wd{imm("email")}, visible: false}},
		{name: "a channel nothing is registered for is planned, and is not inbox-backed",
			why:   "the registry is the dispatcher's; a producer-only replica may not hold every channel",
			setup: func(f *planFx) { delete(f.caps, "in_app") },
			want:  wantPlan{ds: []wd{imm("in_app"), imm("email")}, visible: false}},
		{name: "inbox visibility is the capability, not the kind: another inbox-backed channel makes it visible",
			setup: func(f *planFx) {
				f.def.DefaultChannels = []domain.ChannelKind{"feed", "email"}
				f.caps["feed"] = domain.ChannelCapabilities{InboxBacked: true}
				delete(f.caps, "in_app")
			},
			want: wantPlan{ds: []wd{imm("feed"), imm("email")}, visible: true}},
		{name: "a locked tenant default beats the recipient (NR-011, D30)",
			setup: func(f *planFx) {
				f.setPref("email", true)
				if err := f.prefs.SetTenant(f.t.Context(), planRealm, domain.TenantPreference{
					Tenant: planTenant, Topic: "ingestion_complete", Channel: "email", Enabled: false, Locked: true}); err != nil {
					f.t.Fatal(err)
				}
			},
			want: wantPlan{ds: []wd{imm("in_app")}, visible: true}},
		{name: "a channel only a tenant default names is fanned out",
			why: "D30: 'everyone gets security email' is a tenant row, whether or not the topic lists the channel",
			setup: func(f *planFx) {
				if err := f.prefs.SetTenant(f.t.Context(), planRealm, domain.TenantPreference{
					Tenant: planTenant, Topic: "ingestion_complete", Channel: "push", Enabled: true}); err != nil {
					f.t.Fatal(err)
				}
			},
			want: wantPlan{ds: []wd{imm("in_app"), imm("email"), imm("push")}, visible: true}},
		{name: "an essential topic's channels cannot be disabled (NR-015)",
			setup: func(f *planFx) { f.def.Essential = true; f.setPref("email", false) },
			want:  wantPlan{ds: []wd{imm("in_app"), imm("email")}, visible: true}},

		// ---------------------------------------------------------------- fallback
		{name: "the fallback chain is kept in order",
			setup: func(f *planFx) { f.def.Fallback = []domain.ChannelKind{"sms", "push"} },
			want: wantPlan{ds: []wd{
				{ch: "in_app", mode: domain.PlanImmediate, fb: []domain.ChannelKind{"sms", "push"}},
				{ch: "email", mode: domain.PlanImmediate, fb: []domain.ChannelKind{"sms", "push"}}}, visible: true}},
		{name: "a fallback channel the recipient disabled is skipped",
			setup: func(f *planFx) {
				f.def.Fallback = []domain.ChannelKind{"sms", "push"}
				f.setPref("sms", false)
			},
			want: wantPlan{ds: []wd{
				{ch: "in_app", mode: domain.PlanImmediate, fb: []domain.ChannelKind{"push"}},
				{ch: "email", mode: domain.PlanImmediate, fb: []domain.ChannelKind{"push"}}}, visible: true}},
		{name: "a fallback channel is never fanned out, even when the recipient enables it explicitly (NR-031)",
			why:   "it is tried only when the one before it ended undelivered",
			setup: func(f *planFx) { f.setPref("sms", true) },
			want:  wantPlan{ds: []wd{imm("in_app"), imm("email")}, visible: true}},

		// ------------------------------------------------------------ priority, shard
		{name: "an unset priority takes the topic's default",
			setup: func(f *planFx) { f.def.DefaultPriority = domain.PriorityWarning },
			want:  wantPlan{ds: []wd{imm("in_app"), imm("email")}, visible: true, priority: domain.PriorityWarning}},
		{name: "a set priority wins over the topic's default",
			setup:  func(f *planFx) { f.def.DefaultPriority = domain.PriorityWarning },
			mutate: func(n *domain.Notification) { n.Priority = domain.PriorityInfo },
			want:   wantPlan{ds: []wd{imm("in_app"), imm("email")}, visible: true, priority: domain.PriorityInfo}},
		{name: "the shard is the identity's over the configured count (D11)",
			setup: func(f *planFx) { f.cfg.Shards = 1 },
			want: wantPlan{ds: []wd{imm("in_app"), imm("email")}, visible: true, shard: func() *domain.Shard {
				s := domain.ShardFor(planID, 1)
				return &s
			}()}},

		// ----------------------------------------------------------------- quiet hours
		{name: "quiet hours defer info to their end (NR-013)",
			setup: func(f *planFx) { f.setSchedule(quiet1113) },
			want: wantPlan{ds: []wd{deferred("in_app", at(2031, 3, 1, 13, 0), "quiet_hours"),
				deferred("email", at(2031, 3, 1, 13, 0), "quiet_hours")}, visible: true}},
		{name: "quiet hours defer warning too",
			setup:  func(f *planFx) { f.setSchedule(quiet1113) },
			mutate: func(n *domain.Notification) { n.Priority = domain.PriorityWarning },
			want: wantPlan{ds: []wd{deferred("in_app", at(2031, 3, 1, 13, 0), "quiet_hours"),
				deferred("email", at(2031, 3, 1, 13, 0), "quiet_hours")}, visible: true}},
		{name: "outside quiet hours a notification is immediate",
			setup: func(f *planFx) { f.setSchedule(quiet2207) },
			want:  wantPlan{ds: []wd{imm("in_app"), imm("email")}, visible: true}},
		{name: "quiet hours that wrap midnight, before it: due tomorrow morning",
			setup: func(f *planFx) { f.setSchedule(quiet2207); f.clock.Set(at(2031, 3, 1, 23, 30)) },
			want: wantPlan{ds: []wd{deferred("in_app", at(2031, 3, 2, 7, 0), "quiet_hours"),
				deferred("email", at(2031, 3, 2, 7, 0), "quiet_hours")}, visible: true}},
		{name: "quiet hours that wrap midnight, after it: due this morning",
			setup: func(f *planFx) { f.setSchedule(quiet2207); f.clock.Set(at(2031, 3, 2, 2, 0)) },
			want: wantPlan{ds: []wd{deferred("in_app", at(2031, 3, 2, 7, 0), "quiet_hours"),
				deferred("email", at(2031, 3, 2, 7, 0), "quiet_hours")}, visible: true}},
		{name: "quiet hours that span the change to summer time end at 07:00 local, not nine hours on",
			why: "Paris moves its clocks forward at 02:00 on 2031-03-30, so that night is an hour shorter: 07:00 there is 05:00 UTC, not 06:00",
			setup: func(f *planFx) {
				f.setSchedule(domain.DeliverySchedule{QuietStart: "22:00", QuietEnd: "07:00", Timezone: "Europe/Paris"})
				f.clock.Set(at(2031, 3, 29, 22, 0))
			},
			want: wantPlan{ds: []wd{deferred("in_app", at(2031, 3, 30, 5, 0), "quiet_hours"),
				deferred("email", at(2031, 3, 30, 5, 0), "quiet_hours")}, visible: true}},
		{name: "critical inside quiet hours is immediate and flagged (D9, NR-013)",
			setup:  func(f *planFx) { f.setSchedule(quiet1113) },
			mutate: func(n *domain.Notification) { n.Priority = domain.PriorityCritical },
			want: wantPlan{ds: []wd{{ch: "in_app", mode: domain.PlanImmediate, fb: sms, quiet: true},
				{ch: "email", mode: domain.PlanImmediate, fb: sms, quiet: true}}, visible: true}},
		{name: "critical outside quiet hours is immediate and not flagged",
			setup:  func(f *planFx) { f.setSchedule(quiet2207) },
			mutate: func(n *domain.Notification) { n.Priority = domain.PriorityCritical },
			want:   wantPlan{ds: []wd{imm("in_app"), imm("email")}, visible: true}},
		{name: "critical only by its topic's default is critical in quiet hours",
			setup: func(f *planFx) {
				f.def.DefaultPriority = domain.PriorityCritical
				f.setSchedule(quiet1113)
			},
			want: wantPlan{ds: []wd{{ch: "in_app", mode: domain.PlanImmediate, fb: sms, quiet: true},
				{ch: "email", mode: domain.PlanImmediate, fb: sms, quiet: true}}, visible: true, priority: domain.PriorityCritical}},
		{name: "an essential topic is still deferred by quiet hours: only critical overrides them",
			setup: func(f *planFx) { f.def.Essential = true; f.setSchedule(quiet1113) },
			want: wantPlan{ds: []wd{deferred("in_app", at(2031, 3, 1, 13, 0), "quiet_hours"),
				deferred("email", at(2031, 3, 1, 13, 0), "quiet_hours")}, visible: true}},
		{name: "a schedule that does not validate is not applied: the notification is sent now",
			why: "a bad stored schedule must not lose a notification, or wake someone on a guess",
			setup: func(f *planFx) {
				f.setSchedule(domain.DeliverySchedule{QuietStart: "11:00", QuietEnd: "13:00", Timezone: "Nowhere/Land", Digest: 15 * time.Minute})
			},
			want: wantPlan{ds: []wd{imm("in_app"), imm("email")}, visible: true}},

		// ---------------------------------------------------------------------- digest
		{name: "a digest cadence folds each enabled channel into its window (NR-014)",
			setup: func(f *planFx) { f.setSchedule(cadence) },
			want:  wantPlan{ds: []wd{digest("in_app"), digest("email")}, visible: true, window: 15 * time.Minute, digestMax: 50}},
		{name: "the configuration's defaults reach the plan: a window holds 100 members and there are 16 shards",
			why: "an unset DigestMax would reach the store as 0, which it refuses",
			setup: func(f *planFx) {
				f.cfg = notify.Config{Realm: planRealm, StoreDSN: "x", RedisURL: "y"}
				f.setSchedule(cadence)
			},
			want: wantPlan{ds: []wd{digest("in_app"), digest("email")}, visible: true, window: 15 * time.Minute, digestMax: 100,
				shard: func() *domain.Shard { s := domain.ShardFor(planID, 16); return &s }()}},
		{name: "no cadence, no digest",
			setup: func(f *planFx) { f.setSchedule(domain.DeliverySchedule{Digest: 0}) },
			want:  wantPlan{ds: []wd{imm("in_app"), imm("email")}, visible: true}},
		{name: "a critical notification is never digested (NR-014)",
			setup:  func(f *planFx) { f.setSchedule(cadence) },
			mutate: func(n *domain.Notification) { n.Priority = domain.PriorityCritical },
			want:   wantPlan{ds: []wd{imm("in_app"), imm("email")}, visible: true}},
		{name: "a topic that is critical by default is never digested",
			setup: func(f *planFx) {
				f.def.DefaultPriority = domain.PriorityCritical
				f.setSchedule(cadence)
			},
			want: wantPlan{ds: []wd{imm("in_app"), imm("email")}, visible: true}},
		{name: "an essential topic is never digested (NR-015)",
			setup: func(f *planFx) { f.def.Essential = true; f.setSchedule(cadence) },
			want:  wantPlan{ds: []wd{imm("in_app"), imm("email")}, visible: true}},
		{name: "a scheduled send is not digested: a window could deliver it before DeliverAfter",
			setup:  func(f *planFx) { f.setSchedule(cadence) },
			mutate: func(n *domain.Notification) { n.DeliverAfter = noon.Add(2 * time.Hour) },
			want: wantPlan{ds: []wd{{ch: "in_app", mode: domain.PlanImmediate, at: noon.Add(2 * time.Hour), reason: "scheduled", fb: sms},
				{ch: "email", mode: domain.PlanImmediate, at: noon.Add(2 * time.Hour), reason: "scheduled", fb: sms}},
				visible: true, visibleFrom: noon.Add(2 * time.Hour)}},
		{name: "a notification with an expiry is not digested: a window could deliver it late",
			setup:  func(f *planFx) { f.setSchedule(cadence) },
			mutate: func(n *domain.Notification) { n.DeliverBefore = noon.Add(time.Hour) },
			want:   wantPlan{ds: []wd{imm("in_app"), imm("email")}, visible: true}},
		{name: "a digested channel is not checked against quota: the digest's delivery is",
			setup: func(f *planFx) {
				f.setSchedule(cadence)
				f.quotas.SetBudget(f.quotaKey("email"), 0, noon.Add(time.Hour))
			},
			want: wantPlan{ds: []wd{digest("in_app"), digest("email")}, visible: true, window: 15 * time.Minute, digestMax: 50}},

		// ----------------------------------------------------------------------- quota
		{name: "an exhausted quota defers that channel to the budget's reset (NR-023)",
			setup: func(f *planFx) { f.quotas.SetBudget(f.quotaKey("email"), 0, noon.Add(30*time.Minute)) },
			want:  wantPlan{ds: []wd{imm("in_app"), deferred("email", noon.Add(30*time.Minute), "quota")}, visible: true}},
		{name: "a budget with units left is immediate, and planning took none of it",
			setup: func(f *planFx) { f.quotas.SetBudget(f.quotaKey("email"), 3, noon.Add(time.Hour)) },
			want:  wantPlan{ds: []wd{imm("in_app"), imm("email")}, visible: true},
			after: func(f *planFx) {
				if n := f.quotas.Taken(f.quotaKey("email")); n != 0 {
					f.t.Errorf("planning took %d units: a peek consumes nothing, the dispatcher takes", n)
				}
			}},
		{name: "a budget belongs to its tenant and channel",
			setup: func(f *planFx) {
				f.quotas.SetBudget(domain.QuotaKey{Realm: planRealm, Tenant: domain.Tenant{Kind: "workspace", ID: "w2"}, Channel: "email"}, 0, noon.Add(time.Hour))
				f.quotas.SetBudget(f.quotaKey("sms"), 0, noon.Add(time.Hour))
			},
			want: wantPlan{ds: []wd{imm("in_app"), imm("email")}, visible: true}},
		{name: "a budget whose reset has passed does not defer",
			why:   "an exhausted counter with a reset in the past is stale, and waiting for it would wait for nothing",
			setup: func(f *planFx) { f.quotas.SetBudget(f.quotaKey("email"), 0, noon.Add(-time.Minute)) },
			want:  wantPlan{ds: []wd{imm("in_app"), imm("email")}, visible: true}},
		{name: "drop_non_essential drops an ordinary info notification, and records why",
			setup: func(f *planFx) {
				f.quotas.SetBudget(f.quotaKey("email"), 0, noon.Add(time.Hour))
				f.quotas.SetPolicy(f.quotaKey("email"), domain.QuotaDropNonEssential)
			},
			want: wantPlan{ds: []wd{imm("in_app"), {ch: "email", mode: domain.PlanDropped, reason: "quota"}}, visible: true}},
		{name: "drop_non_essential never drops a critical notification",
			setup: func(f *planFx) {
				f.quotas.SetBudget(f.quotaKey("email"), 0, noon.Add(time.Hour))
				f.quotas.SetPolicy(f.quotaKey("email"), domain.QuotaDropNonEssential)
			},
			mutate: func(n *domain.Notification) { n.Priority = domain.PriorityCritical },
			want:   wantPlan{ds: []wd{imm("in_app"), deferred("email", noon.Add(time.Hour), "quota")}, visible: true}},
		{name: "drop_non_essential never drops an essential topic",
			setup: func(f *planFx) {
				f.def.Essential = true
				f.quotas.SetBudget(f.quotaKey("email"), 0, noon.Add(time.Hour))
				f.quotas.SetPolicy(f.quotaKey("email"), domain.QuotaDropNonEssential)
			},
			want: wantPlan{ds: []wd{imm("in_app"), deferred("email", noon.Add(time.Hour), "quota")}, visible: true}},
		{name: "with no policy configured an exhausted budget defers",
			setup: func(f *planFx) {
				f.noPolicy = true
				f.quotas.SetBudget(f.quotaKey("email"), 0, noon.Add(time.Hour))
				f.quotas.SetPolicy(f.quotaKey("email"), domain.QuotaDropNonEssential)
			},
			want: wantPlan{ds: []wd{imm("in_app"), deferred("email", noon.Add(time.Hour), "quota")}, visible: true}},
		{name: "a quota counter that fails is a miss: quota fails open (D5)",
			setup: func(f *planFx) { f.deps.Quotas = failingQuota{} },
			want:  wantPlan{ds: []wd{imm("in_app"), imm("email")}, visible: true}},
		{name: "a policy lookup that fails defers: a lookup that fails must not drop a notification",
			setup: func(f *planFx) {
				f.quotas.SetBudget(f.quotaKey("email"), 0, noon.Add(time.Hour))
				f.deps.QuotaPolicies = failingPolicy{}
			},
			want: wantPlan{ds: []wd{imm("in_app"), deferred("email", noon.Add(time.Hour), "quota")}, visible: true}},
		{name: "quota later than quiet hours: the later time and its reason",
			setup: func(f *planFx) {
				f.setSchedule(quiet1113)
				f.quotas.SetBudget(f.quotaKey("email"), 0, at(2031, 3, 1, 14, 0))
			},
			want: wantPlan{ds: []wd{deferred("in_app", at(2031, 3, 1, 13, 0), "quiet_hours"),
				deferred("email", at(2031, 3, 1, 14, 0), "quota")}, visible: true}},
		{name: "quiet hours later than quota: the later time and its reason",
			setup: func(f *planFx) {
				f.setSchedule(quiet1113)
				f.quotas.SetBudget(f.quotaKey("email"), 0, at(2031, 3, 1, 12, 30))
			},
			want: wantPlan{ds: []wd{deferred("in_app", at(2031, 3, 1, 13, 0), "quiet_hours"),
				deferred("email", at(2031, 3, 1, 13, 0), "quiet_hours")}, visible: true}},
		{name: "a critical notification deferred by quota outside quiet hours is not flagged as an override",
			setup: func(f *planFx) {
				f.setSchedule(quiet1113)
				f.quotas.SetBudget(f.quotaKey("email"), 0, at(2031, 3, 1, 14, 0))
			},
			mutate: func(n *domain.Notification) { n.Priority = domain.PriorityCritical },
			want: wantPlan{ds: []wd{{ch: "in_app", mode: domain.PlanImmediate, fb: sms, quiet: true},
				{ch: "email", mode: domain.PlanDeferred, at: at(2031, 3, 1, 14, 0), reason: "quota", fb: sms}}, visible: true}},

		// ---------------------------------------------------------------- scheduled send
		{name: "a scheduled send is due at DeliverAfter, and invisible until then (NR-029, D29)",
			mutate: func(n *domain.Notification) { n.DeliverAfter = noon.Add(2 * time.Hour) },
			want: wantPlan{ds: []wd{{ch: "in_app", mode: domain.PlanImmediate, at: noon.Add(2 * time.Hour), reason: "scheduled", fb: sms},
				{ch: "email", mode: domain.PlanImmediate, at: noon.Add(2 * time.Hour), reason: "scheduled", fb: sms}},
				visible: true, visibleFrom: noon.Add(2 * time.Hour)}},
		{name: "a DeliverAfter already passed is due now",
			mutate: func(n *domain.Notification) { n.DeliverAfter = noon.Add(-time.Hour) },
			want:   wantPlan{ds: []wd{imm("in_app"), imm("email")}, visible: true}},
		{name: "a DeliverAfter that is now is due now",
			mutate: func(n *domain.Notification) { n.DeliverAfter = noon },
			want:   wantPlan{ds: []wd{imm("in_app"), imm("email")}, visible: true}},
		{name: "a scheduled send that falls in quiet hours waits for their end",
			setup:  func(f *planFx) { f.setSchedule(quiet2207) },
			mutate: func(n *domain.Notification) { n.DeliverAfter = at(2031, 3, 1, 22, 30) },
			want: wantPlan{ds: []wd{deferred("in_app", at(2031, 3, 2, 7, 0), "quiet_hours"), deferred("email", at(2031, 3, 2, 7, 0), "quiet_hours")},
				visible: true, visibleFrom: at(2031, 3, 1, 22, 30)}},
		{name: "quiet hours now do not defer a send scheduled after they end",
			why:    "the quiet hours that matter are those at the send time",
			setup:  func(f *planFx) { f.setSchedule(quiet2207); f.clock.Set(at(2031, 3, 1, 23, 0)) },
			mutate: func(n *domain.Notification) { n.DeliverAfter = at(2031, 3, 2, 9, 0) },
			want: wantPlan{ds: []wd{{ch: "in_app", mode: domain.PlanImmediate, at: at(2031, 3, 2, 9, 0), reason: "scheduled", fb: sms},
				{ch: "email", mode: domain.PlanImmediate, at: at(2031, 3, 2, 9, 0), reason: "scheduled", fb: sms}},
				visible: true, visibleFrom: at(2031, 3, 2, 9, 0)}},
		{name: "a scheduled critical send inside quiet hours is flagged",
			setup: func(f *planFx) { f.setSchedule(quiet2207) },
			mutate: func(n *domain.Notification) {
				n.Priority = domain.PriorityCritical
				n.DeliverAfter = at(2031, 3, 1, 23, 0)
			},
			want: wantPlan{ds: []wd{{ch: "in_app", mode: domain.PlanImmediate, at: at(2031, 3, 1, 23, 0), reason: "scheduled", fb: sms, quiet: true},
				{ch: "email", mode: domain.PlanImmediate, at: at(2031, 3, 1, 23, 0), reason: "scheduled", fb: sms, quiet: true}},
				visible: true, visibleFrom: at(2031, 3, 1, 23, 0)}},
		{name: "a scheduled send past an exhausted budget's reset is not deferred by it",
			setup:  func(f *planFx) { f.quotas.SetBudget(f.quotaKey("email"), 0, noon.Add(time.Hour)) },
			mutate: func(n *domain.Notification) { n.DeliverAfter = noon.Add(3 * time.Hour) },
			want: wantPlan{ds: []wd{{ch: "in_app", mode: domain.PlanImmediate, at: noon.Add(3 * time.Hour), reason: "scheduled", fb: sms},
				{ch: "email", mode: domain.PlanImmediate, at: noon.Add(3 * time.Hour), reason: "scheduled", fb: sms}},
				visible: true, visibleFrom: noon.Add(3 * time.Hour)}},

		// ------------------------------------------------------------------------ errors
		{name: "an unregistered topic is ErrUnknownTopic (NR-017)",
			mutate:  func(n *domain.Notification) { n.Topic = "never_registered" },
			wantErr: domain.ErrUnknownTopic},
		{name: "a topic registry that fails fails the plan",
			setup:   func(f *planFx) { f.deps.Topics = failingTopics{} },
			wantErr: errPort},
		{name: "a preference store that fails fails the plan: the producer retries",
			setup:   func(f *planFx) { f.deps.Prefs = failingPrefs{PreferenceStore: f.prefs, resolve: true} },
			wantErr: errPort},
		{name: "a schedule that cannot be read fails the plan",
			setup:   func(f *planFx) { f.deps.Prefs = failingPrefs{PreferenceStore: f.prefs, schedule: true} },
			wantErr: errPort},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newPlanFx(t)
			if tc.setup != nil {
				tc.setup(f)
			}
			p := f.planner(t)
			n := f.note()
			if tc.mutate != nil {
				tc.mutate(&n)
			}
			got, err := p.Plan(t.Context(), planID, n)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("Plan = %+v, %v, want %v", got, err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Plan: %v", err)
			}
			tc.want.check(t, got)
			if tc.after != nil {
				tc.after(f)
			}
		})
	}
}

// NewPlanner names every port it needs and is not given, so a host fixes its wiring
// in one pass.
func TestNewPlannerNamesEveryMissingPort(t *testing.T) {
	t.Parallel()
	cfg := notify.Config{Realm: planRealm, StoreDSN: "x", RedisURL: "y"}
	_, err := app.NewPlanner(cfg, app.Deps{})
	if err == nil {
		t.Fatal("NewPlanner with no ports succeeded")
	}
	for _, port := range []string{"Deps.Channels", "Deps.Prefs", "Deps.Quotas", "Deps.Topics"} {
		if !strings.Contains(err.Error(), port) {
			t.Errorf("error %q does not name %s", err, port)
		}
	}
}

// ---------------------------------------------------------------- failing ports --

var errPort = errors.New("test: the port failed")

type failingQuota struct{}

// Peek fails and, as a misbehaving adapter might, also says the budget is spent
// until an hour from now: the error is the answer, and the rest is not to be read.
func (failingQuota) Peek(context.Context, domain.QuotaKey) (bool, time.Time, error) {
	return true, noon.Add(time.Hour), errPort
}

func (failingQuota) Take(context.Context, domain.QuotaKey) (bool, time.Time, error) {
	return false, time.Time{}, errPort
}

type failingPolicy struct{}

func (failingPolicy) Policy(context.Context, domain.QuotaKey) (domain.QuotaPolicy, error) {
	return "", errPort
}

type failingTopics struct{}

func (failingTopics) Lookup(context.Context, domain.Realm, domain.Topic) (domain.TopicDef, bool, error) {
	return domain.TopicDef{}, false, errPort
}

type failingPrefs struct {
	ports.PreferenceStore
	resolve, schedule bool
}

func (p failingPrefs) Resolve(ctx context.Context, id domain.Identity, t domain.Topic, def domain.TopicDef) ([]domain.ResolvedPreference, error) {
	if p.resolve {
		return nil, errPort
	}
	return p.PreferenceStore.Resolve(ctx, id, t, def)
}

func (p failingPrefs) Schedule(ctx context.Context, id domain.Identity) (domain.DeliverySchedule, error) {
	if p.schedule {
		return domain.DeliverySchedule{}, errPort
	}
	return p.PreferenceStore.Schedule(ctx, id)
}
