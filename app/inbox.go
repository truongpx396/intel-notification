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

// Inbox is the recipient-facing use-case: ports.Inbox over the inbox store and the
// template renderer. Every method takes the caller's identity from the caller's
// authenticated session (NR-008); the store runs each call under that identity's
// scope, so nothing here can read another recipient's rows.
//
// It renders. The store keeps the producer's fallback title and body, and the copy a
// recipient reads is made from the topic's template, in the viewer's locale, each time
// a page is read, so a template edited after a notification was stored changes what
// the next read shows (NR-019, D27). The rest of the surface is the store's, passed
// through.
//
// The preference, schedule and unsubscribe methods belong to the REST surface's
// tasks (T026, T044): until then they return an error that says so, because a nil
// would tell a recipient their choice was saved. SetSchedule, when it is built, must
// call domain.DeliverySchedule.Validate before it stores anything: the planner does
// not apply a schedule that fails it, and a typo should be caught when it is written.
type Inbox struct {
	cfg      notify.Config // the effective configuration
	store    ports.InboxStore
	renderer ports.TemplateRenderer
	topics   ports.TopicRegistry
	channels ports.ChannelRegistry
}

var _ ports.Inbox = (*Inbox)(nil)

// NewInbox returns an Inbox for cfg over d. It refuses a configuration that does
// not validate, and names every port it needs and d leaves nil.
func NewInbox(cfg notify.Config, d Deps) (*Inbox, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	var g gaps
	g.need("Channels", d.Channels == nil)
	g.need("Inbox", d.Inbox == nil)
	g.need("Renderer", d.Renderer == nil)
	g.need("Topics", d.Topics == nil)
	if err := g.err("Inbox"); err != nil {
		return nil, err
	}
	return &Inbox{cfg: cfg.WithDefaults(), store: d.Inbox, renderer: d.Renderer, topics: d.Topics, channels: d.Channels}, nil
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
	channel, ok := i.inboxChannel()
	if !ok || len(page.Items) == 0 {
		return page, nil
	}
	locale := q.Locale
	if locale == "" {
		locale = i.cfg.DefaultLocale
	}

	refs := map[domain.Topic]string{} // a page holds few topics, and each is looked up once
	for n := range page.Items {
		item := &page.Items[n]
		ref, known := refs[item.Topic]
		if !known {
			def, found, err := i.topics.Lookup(ctx, id.Realm, item.Topic)
			if err != nil {
				return domain.Page{}, fmt.Errorf("inbox: look up topic %q: %w", item.Topic, err)
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
			return domain.Page{}, fmt.Errorf("inbox: render %q for %q: %w", item.Topic, locale, err)
		}
	}
	return page, nil
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

func errNotBuilt(what, task string) error {
	return fmt.Errorf("app: %s is not implemented yet (%s)", what, task)
}

// Preferences is built with the REST surface (T044).
func (i *Inbox) Preferences(context.Context, domain.Identity) ([]domain.ResolvedPreference, error) {
	return nil, errNotBuilt("Inbox.Preferences", "T044")
}

// SetPreferences is built with the REST surface (T044).
func (i *Inbox) SetPreferences(context.Context, domain.Identity, []domain.Preference) error {
	return errNotBuilt("Inbox.SetPreferences", "T044")
}

// Schedule is built with the REST surface (T044).
func (i *Inbox) Schedule(context.Context, domain.Identity) (domain.DeliverySchedule, error) {
	return domain.DeliverySchedule{}, errNotBuilt("Inbox.Schedule", "T044")
}

// SetSchedule is built with the REST surface (T044), and validates first.
func (i *Inbox) SetSchedule(context.Context, domain.Identity, domain.DeliverySchedule) error {
	return errNotBuilt("Inbox.SetSchedule", "T044")
}

// Unsubscribe is built with the email channel's tokens (T026, D34).
func (i *Inbox) Unsubscribe(context.Context, string) error {
	return errNotBuilt("Inbox.Unsubscribe", "T026")
}
