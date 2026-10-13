package app

import (
	"context"
	"fmt"
	"slices"
	"time"

	notify "github.com/truongpx396/intel-notification"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// Reasons a planned delivery is not due at once, as the plan records them.
const (
	reasonScheduled  = "scheduled"
	reasonQuietHours = "quiet_hours"
	reasonQuota      = "quota"
)

// Planned is what the planner decided for one notification: the notification with
// its effective priority filled in, which is what the store requires, and the plan.
type Planned struct {
	Notification domain.Notification
	Plan         domain.DeliveryPlan
}

// Planner turns a notification into the plan PersistAndEnqueue writes: which
// channels it goes out on, when each is due, and whether the inbox shows it
// (NR-011 to NR-014, NR-029, NR-031). It decides; it writes nothing.
type Planner struct {
	cfg      notify.Config
	topics   ports.TopicRegistry
	prefs    ports.PreferenceStore
	channels ports.ChannelRegistry
	quotas   ports.QuotaCounter
	policies ports.QuotaPolicies // nil: an exhausted budget defers
	clock    ports.Clock
}

// NewPlanner returns a Planner over d's topics, preferences, channels and quotas,
// or an error naming every one of those that is missing.
func NewPlanner(cfg notify.Config, d Deps) (*Planner, error) {
	var g gaps
	g.need("Channels", d.Channels == nil)
	g.need("Prefs", d.Prefs == nil)
	g.need("Quotas", d.Quotas == nil)
	g.need("Topics", d.Topics == nil)
	if err := g.err("Planner"); err != nil {
		return nil, err
	}
	return &Planner{
		cfg: cfg.WithDefaults(), topics: d.Topics, prefs: d.Prefs, channels: d.Channels,
		quotas: d.Quotas, policies: d.QuotaPolicies, clock: d.clockOf(),
	}, nil
}

// Plan decides, for the notification n of id, which channels it is sent on, when,
// and whether the inbox shows it.
//
// The channels are the topic's own, in order, that the recipient's resolved
// preferences leave enabled, then any other channel a preference row enables (a
// tenant's "everyone gets security email" names a channel the topic may not). A
// fallback channel is never among them: it is tried only when the one before it
// ended undelivered, so each fanned-out delivery carries the enabled part of the
// chain (D29, NR-031).
//
// A delivery is due when the first of these that applies says: a digest window,
// when the recipient set a cadence and the notification may be folded into one;
// otherwise the send time (DeliverAfter, or now), moved to the budget's reset when
// the channel's budget is spent, and then past quiet hours, unless the notification
// is critical. Quiet hours are those at the instant the delivery would otherwise be
// due, so a send scheduled for the morning is not held by tonight's. A budget spent
// under drop_non_essential drops an ordinary notification instead (D5).
//
// The planner reads and never takes: a peek consumes nothing, the dispatcher takes.
// A counter that cannot answer is a budget with units left, because quota fails
// open and correctness does not (D5); a policy that cannot be read defers, because
// a failed lookup must not discard a notification. A schedule that does not validate
// is not applied, with its digest cadence: the notification goes out now rather
// than be lost to, or wake someone on, a guess about a broken window.
//
// An inbox-backed channel is never digested (D39). The inbox row is written when the
// notification is accepted and its nudge interrupts no one, so a window would only
// hold the live update back for as long as the recipient's cadence for their email,
// and could not name what to nudge when it flushed. The channel is told by its
// capability and not by its name (NR-016), and a plan opens a window only when a
// channel uses it. A scheduled send and one with an expiry are never digested,
// because a window could deliver them before DeliverAfter or after DeliverBefore;
// and the inbox shows a scheduled notification from its send time, whatever quiet
// hours do to the deliveries.
func (p *Planner) Plan(ctx context.Context, id domain.Identity, n domain.Notification) (Planned, error) {
	def, ok, err := p.topics.Lookup(ctx, id.Realm, n.Topic)
	if err != nil {
		return Planned{}, fmt.Errorf("plan: look up topic %q: %w", n.Topic, err)
	}
	if !ok {
		return Planned{}, fmt.Errorf("plan: %w: %q", domain.ErrUnknownTopic, n.Topic)
	}
	resolved, err := p.prefs.Resolve(ctx, id, n.Topic, def)
	if err != nil {
		return Planned{}, fmt.Errorf("plan: resolve preferences for %q: %w", n.Topic, err)
	}
	schedule, err := p.prefs.Schedule(ctx, id)
	if err != nil {
		return Planned{}, fmt.Errorf("plan: read the delivery schedule: %w", err)
	}
	if schedule.Validate() != nil {
		schedule = domain.DeliverySchedule{}
	}

	n.Priority = def.EffectivePriority(n.Priority)
	now := p.clock.Now()
	sendAt, scheduled := now, n.DeliverAfter.After(now)
	if scheduled {
		sendAt = n.DeliverAfter
	}

	enabled := make(map[domain.ChannelKind]bool, len(resolved))
	var fanOut []domain.ChannelKind
	for _, r := range resolved {
		enabled[r.Channel] = r.Enabled
		if r.Enabled && !slices.Contains(def.Fallback, r.Channel) {
			fanOut = append(fanOut, r.Channel)
		}
	}
	var chain []domain.ChannelKind
	for _, ch := range def.Fallback {
		if enabled[ch] {
			chain = append(chain, ch)
		}
	}

	plan := domain.DeliveryPlan{Shard: domain.ShardFor(id, p.cfg.Shards)}
	if scheduled {
		plan.VisibleFrom = n.DeliverAfter
	}
	digestible := schedule.Digest > 0 && def.Digestible(n.Priority) && !scheduled && n.DeliverBefore.IsZero()
	for _, ch := range fanOut {
		inboxBacked := false
		if c, registered := p.channels.Get(ch); registered && c.Capabilities().InboxBacked {
			plan.InboxVisible, inboxBacked = true, true
		}
		if digestible && !inboxBacked {
			plan.DigestWindow, plan.DigestMax = schedule.Digest, p.cfg.DigestMax
			plan.Deliveries = append(plan.Deliveries, domain.PlannedDelivery{Channel: ch, Mode: domain.PlanDigest})
			continue
		}
		plan.Deliveries = append(plan.Deliveries, p.planChannel(ctx, id, def, n, schedule, ch, chain, sendAt, scheduled))
	}
	return Planned{Notification: n, Plan: plan}, nil
}

// planChannel plans one fanned-out, undigested delivery.
func (p *Planner) planChannel(ctx context.Context, id domain.Identity, def domain.TopicDef, n domain.Notification,
	schedule domain.DeliverySchedule, ch domain.ChannelKind, chain []domain.ChannelKind, sendAt time.Time, scheduled bool) domain.PlannedDelivery {

	due, reason := sendAt, ""
	if scheduled {
		reason = reasonScheduled
	}

	key := domain.QuotaKey{Realm: id.Realm, Tenant: id.Tenant, Channel: ch}
	if exhausted, resetAt, err := p.quotas.Peek(ctx, key); err == nil && exhausted && resetAt.After(sendAt) {
		if p.dropsWhenExhausted(ctx, key, def, n.Priority) {
			return domain.PlannedDelivery{Channel: ch, Mode: domain.PlanDropped, Reason: reasonQuota}
		}
		due, reason = resetAt, reasonQuota
	}

	d := domain.PlannedDelivery{Channel: ch, Mode: domain.PlanImmediate, Fallback: slices.Clone(chain)}
	if n.Priority == domain.PriorityCritical {
		_, d.QuietHoursOverride = schedule.QuietUntil(due)
	} else if end, quiet := schedule.QuietUntil(due); quiet {
		due, reason = end, reasonQuietHours
	}
	d.Reason = reason
	if reason != "" {
		d.NotBefore = due
	}
	if reason == reasonQuota || reason == reasonQuietHours {
		d.Mode = domain.PlanDeferred
	}
	return d
}

// dropsWhenExhausted reports whether the channel's spent budget drops this
// notification. With no policy store, or one that cannot answer, it does not.
func (p *Planner) dropsWhenExhausted(ctx context.Context, key domain.QuotaKey, def domain.TopicDef, pr domain.Priority) bool {
	if p.policies == nil {
		return false
	}
	policy, err := p.policies.Policy(ctx, key)
	return err == nil && policy.Drops(def, pr)
}
