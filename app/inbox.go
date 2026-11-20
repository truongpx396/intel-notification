package app

import (
	"context"
	"errors"
	"fmt"
	"time"

	notify "github.com/truongpx396/intel-notification"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// Inbox is the recipient-facing use-case: ports.Inbox over the inbox store, the preference store and
// the template renderer. Every method takes the caller's identity from the caller's authenticated
// session (NR-008); the store runs each call under that identity's scope, so nothing here can read
// another recipient's rows.
//
// It renders. The store keeps the producer's fallback title and body, and the copy a
// recipient reads is made from the topic's template, in the viewer's locale, each time
// a page is read, so a template edited after a notification was stored changes what
// the next read shows (NR-019, D27). The rest of the surface is the store's, passed
// through, except for the three things a recipient may be refused:
//
//   - SetPreferences refuses a pair that is not the recipient's to change, with its reason, and is
//     all or none (D30, NR-015);
//   - SetSchedule refuses a schedule the planner would not apply (DeliverySchedule.Validate), so a
//     typo is caught when it is written and not when it silently does nothing;
//   - Unsubscribe refuses a token that is not a good one, and disables the one pair a good one names
//     (D34).
type Inbox struct {
	cfg      notify.Config // the effective configuration
	store    ports.InboxStore
	prefs    ports.PreferenceStore
	renderer ports.TemplateRenderer
	topics   ports.TopicRegistry
	channels ports.ChannelRegistry
	signer   *domain.UnsubscribeSigner // nil: no token is one this engine made
	clock    ports.Clock
}

var _ ports.Inbox = (*Inbox)(nil)

// NewInbox returns an Inbox for cfg over d. It refuses a configuration that does
// not validate, and names every port it needs and d leaves nil. The unsubscribe signer is optional:
// without one, every token is refused.
func NewInbox(cfg notify.Config, d Deps) (*Inbox, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	var g gaps
	g.need("Channels", d.Channels == nil)
	g.need("Inbox", d.Inbox == nil)
	g.need("Prefs", d.Prefs == nil)
	g.need("Renderer", d.Renderer == nil)
	g.need("Topics", d.Topics == nil)
	if err := g.err("Inbox"); err != nil {
		return nil, err
	}
	return &Inbox{cfg: cfg.WithDefaults(), store: d.Inbox, prefs: d.Prefs, renderer: d.Renderer, topics: d.Topics,
		channels: d.Channels, signer: d.UnsubscribeSigner, clock: d.clockOf()}, nil
}

// List returns a page of the caller's notifications, newest first, each rendered.
//
// An item is rendered from its topic's template for the registry's inbox-backed
// channel, found by the capability and not by a kind (NR-016), in q.Locale, or the
// configured default when none is asked. The renderer owns the locale ladder: the tag,
// its language, then its default. A topic with no template for any of them, which the
// renderer reports as domain.ErrNoTemplate, keeps the producer's stored title and body.
// Any other renderer error is returned: a template store that is down, or a template
// that cannot render, is for an operator to fix, and showing the stored copy in its
// place would hide it.
func (i *Inbox) List(ctx context.Context, id domain.Identity, q domain.ListQuery) (domain.Page, error) {
	page, err := i.store.List(ctx, id, q)
	if err != nil {
		return domain.Page{}, err
	}
	if err := i.render(ctx, id, page.Items, q.Locale); err != nil {
		return domain.Page{}, err
	}
	return page, nil
}

// render sets the title and body of each item from its topic's template, in locale or the configured
// default. With no inbox-backed channel registered, or no items, the renderer is not asked.
func (i *Inbox) render(ctx context.Context, id domain.Identity, items []domain.InboxItem, locale string) error {
	channel, ok := i.inboxChannel()
	if !ok || len(items) == 0 {
		return nil
	}
	if locale == "" {
		locale = i.cfg.DefaultLocale
	}

	refs := map[domain.Topic]string{} // a page holds few topics, and each is looked up once
	for n := range items {
		item := &items[n]
		ref, known := refs[item.Topic]
		if !known {
			def, found, err := i.topics.Lookup(ctx, id.Realm, item.Topic)
			if err != nil {
				return fmt.Errorf("inbox: look up topic %q: %w", item.Topic, err)
			}
			if found {
				ref = def.TemplateRef
			}
			refs[item.Topic] = ref
		}
		content, err := i.renderer.Render(ctx, domain.RenderRequest{
			Realm: id.Realm, Tenant: id.Tenant, Topic: item.Topic, TemplateRef: ref,
			Channel: channel, Locale: locale, Data: item.Data,
		})
		switch {
		case err == nil:
			item.Title, item.Body = content.Subject, content.Body
		case errors.Is(err, domain.ErrNoTemplate):
			// The stored copy stands.
		default:
			return fmt.Errorf("inbox: render %q for %q: %w", item.Topic, locale, err)
		}
	}
	return nil
}

// inboxChannel is the kind of the channel whose template the inbox is rendered from:
// the first registered channel, by kind, that declares itself inbox-backed (D26). With
// none registered nothing is rendered and every item shows its stored copy.
func (i *Inbox) inboxChannel() (domain.ChannelKind, bool) {
	for _, kind := range i.channels.Kinds() {
		if c, ok := i.channels.Get(kind); ok && c.Capabilities().InboxBacked {
			return kind, true
		}
	}
	return "", false
}

// Get returns one of the caller's notifications, rendered as List renders it, or nil when the store
// does not return it: absent and someone else's are one answer (NR-008). It is what the relay loads for
// each nudge, under the session's identity, so a nudge that names another recipient's notification
// loads nothing (D16).
func (i *Inbox) Get(ctx context.Context, id domain.Identity, notificationID, locale string) (*domain.InboxItem, error) {
	n, err := i.store.Get(ctx, id, notificationID)
	if err != nil {
		return nil, err
	}
	if n == nil {
		return nil, nil
	}
	items := []domain.InboxItem{{ID: notificationID, Topic: n.Topic, Priority: n.Priority, Title: n.Title, Body: n.Body, Data: n.Data, Payload: n.Payload}}
	if err := i.render(ctx, id, items, locale); err != nil {
		return nil, err
	}
	return &items[0], nil
}

// Unread is the badge: the store's count of unread, visible, unarchived notifications,
// stopping at the configured cap (D15).
func (i *Inbox) Unread(ctx context.Context, id domain.Identity) (domain.UnreadCount, error) {
	return i.store.Unread(ctx, id, i.cfg.UnreadCap)
}

// MarkSeen marks every notification created at or before the instant seen.
func (i *Inbox) MarkSeen(ctx context.Context, id domain.Identity, before time.Time) error {
	return i.store.MarkSeen(ctx, id, before)
}

// MarkRead marks the named notifications read, all or none: an id that is not the
// caller's is domain.ErrNotFound, whatever the reason.
func (i *Inbox) MarkRead(ctx context.Context, id domain.Identity, notificationIDs []string) error {
	return i.store.MarkRead(ctx, id, notificationIDs)
}

// MarkAllRead marks everything the caller can see read, and leaves what is archived.
func (i *Inbox) MarkAllRead(ctx context.Context, id domain.Identity) error {
	return i.store.MarkAllRead(ctx, id)
}

// Archive puts the named notifications away, or brings them back.
func (i *Inbox) Archive(ctx context.Context, id domain.Identity, notificationIDs []string, archived bool) error {
	return i.store.Archive(ctx, id, notificationIDs, archived)
}

// Preferences lists, for every topic the caller's realm has registered, the effective state of each of
// its channels: whether it is on, where that value came from (the recipient, the tenant or the topic's
// default), and whether the recipient may change it (D30). The topics are listed in order, each with its
// own channels in the order the topic declares them.
func (i *Inbox) Preferences(ctx context.Context, id domain.Identity) ([]domain.ResolvedPreference, error) {
	topics, err := i.topics.List(ctx, id.Realm)
	if err != nil {
		return nil, fmt.Errorf("preferences: list topics: %w", err)
	}
	var out []domain.ResolvedPreference
	for _, topic := range topics {
		def, found, err := i.topics.Lookup(ctx, id.Realm, topic)
		if err != nil {
			return nil, fmt.Errorf("preferences: look up topic %q: %w", topic, err)
		}
		if !found {
			continue // unregistered between the list and the lookup
		}
		rs, err := i.prefs.Resolve(ctx, id, topic, def)
		if err != nil {
			return nil, fmt.Errorf("preferences: resolve %q: %w", topic, err)
		}
		out = append(out, rs...)
	}
	return out, nil
}

// SetPreferences stores the caller's choices, all or none. A pair is refused, with the reason in words
// a settings screen can show, when its topic or its channel is not registered, when the topic is
// essential and the channel is one it is delivered on (NR-015), and when the tenant locked the pair
// (D30): refused even when the value asked for is the one in force, because a switch that did nothing
// would look like one that worked. Every refused pair is named in the one answer, a
// *domain.ValidationError whose problems carry domain.ErrUnknownTopic, ErrUnknownChannel and
// ErrPreferenceLocked.
func (i *Inbox) SetPreferences(ctx context.Context, id domain.Identity, prefs []domain.Preference) error {
	var invalid domain.ValidationError
	refuse := func(n int, msg string, sentinel error) {
		invalid.Problems = append(invalid.Problems, domain.Problem{Field: fmt.Sprintf("preferences[%d]", n), Message: msg, Err: sentinel})
	}
	type resolution struct {
		def   domain.TopicDef
		found bool
		rs    []domain.ResolvedPreference
	}
	byTopic := map[domain.Topic]*resolution{} // a request holds few topics, and each is resolved once

	for n, p := range prefs {
		if err := p.Validate(); err != nil {
			var ve *domain.ValidationError
			if errors.As(err, &ve) {
				for _, problem := range ve.Problems {
					refuse(n, problem.Field+" "+problem.Message, problem.Err)
				}
			}
			continue
		}
		r, ok := byTopic[p.Topic]
		if !ok {
			def, found, err := i.topics.Lookup(ctx, id.Realm, p.Topic)
			if err != nil {
				return fmt.Errorf("set preferences: look up topic %q: %w", p.Topic, err)
			}
			r = &resolution{def: def, found: found}
			if found {
				if r.rs, err = i.prefs.Resolve(ctx, id, p.Topic, def); err != nil {
					return fmt.Errorf("set preferences: resolve %q: %w", p.Topic, err)
				}
			}
			byTopic[p.Topic] = r
		}
		if !r.found {
			refuse(n, fmt.Sprintf("topic %q is not registered", p.Topic), domain.ErrUnknownTopic)
			continue
		}
		if _, known := i.channels.Get(p.Channel); !known {
			refuse(n, fmt.Sprintf("channel %q is not registered", p.Channel), domain.ErrUnknownChannel)
			continue
		}
		for _, cur := range r.rs {
			if cur.Channel != p.Channel || !cur.Locked {
				continue
			}
			if r.def.Essential {
				refuse(n, fmt.Sprintf("topic %q is essential: its %q channel cannot be changed, because the notification is one you must receive", p.Topic, p.Channel), domain.ErrPreferenceLocked)
			} else {
				refuse(n, fmt.Sprintf("topic %q on channel %q is locked by the tenant, and cannot be changed here", p.Topic, p.Channel), domain.ErrPreferenceLocked)
			}
		}
	}
	if len(invalid.Problems) > 0 {
		return fmt.Errorf("set preferences: %w", &invalid)
	}
	return i.prefs.Set(ctx, id, prefs)
}

// Schedule is the caller's quiet hours and digest cadence.
func (i *Inbox) Schedule(ctx context.Context, id domain.Identity) (domain.DeliverySchedule, error) {
	return i.prefs.Schedule(ctx, id)
}

// SetSchedule stores the caller's quiet hours and digest cadence, after DeliverySchedule.Validate: the
// planner does not apply a schedule that fails it, so a recipient who saved one would believe they had
// quiet hours they did not have.
func (i *Inbox) SetSchedule(ctx context.Context, id domain.Identity, s domain.DeliverySchedule) error {
	if err := s.Validate(); err != nil {
		return fmt.Errorf("set schedule: %w", err)
	}
	return i.prefs.SetSchedule(ctx, id, s)
}

// Unsubscribe is RFC 8058's one-click: it verifies the token and disables exactly the one pair it names
// for the one recipient it names, and nothing else (D34, NR-015). The identity comes from the token and
// not from a session, because the click comes from a mail client and has none; the token's signature is
// what stands in for one, and it is checked before anything in it is read. A token that is not a good
// one is domain.ErrInvalidUnsubscribeToken, and a good one that has lapsed is
// domain.ErrExpiredUnsubscribeToken, and neither writes anything. Doing it twice is doing it once.
func (i *Inbox) Unsubscribe(ctx context.Context, token string) error {
	if i.signer == nil {
		return fmt.Errorf("unsubscribe: %w: this engine signs no tokens", domain.ErrInvalidUnsubscribeToken)
	}
	scope, err := i.signer.Verify(token, i.clock.Now())
	if err != nil {
		return fmt.Errorf("unsubscribe: %w", err)
	}
	return i.prefs.Set(ctx, scope.Identity, []domain.Preference{{Topic: scope.Topic, Channel: scope.Channel, Enabled: false}})
}
