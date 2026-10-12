//go:build integration

package app_test

import (
	"context"
	"errors"
	"maps"
	"slices"
	"sync"
	"testing"
	"time"

	notify "github.com/truongpx396/intel-notification"
	"github.com/truongpx396/intel-notification/app"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/notifytest"
	"github.com/truongpx396/intel-notification/ports"
)

// T017a: the driving Inbox over the real InboxStore and the Templates fake. An item
// is rendered at read time, in the viewer's locale, from the topic's inbox-backed
// channel's template, and shows the stored copy when there is no template (NR-019,
// D27).

const inboxTopic domain.Topic = "ingestion_complete"

// recRenderer records what the Inbox asked the renderer for, and can be made to fail.
type recRenderer struct {
	inner ports.TemplateRenderer
	err   error

	mu   sync.Mutex
	reqs []domain.RenderRequest
}

func (r *recRenderer) Render(ctx context.Context, req domain.RenderRequest) (domain.RenderedContent, error) {
	r.mu.Lock()
	r.reqs = append(r.reqs, req)
	r.mu.Unlock()
	if r.err != nil {
		return domain.RenderedContent{}, r.err
	}
	return r.inner.Render(ctx, req)
}

func (r *recRenderer) requests() []domain.RenderRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.reqs)
}

type inboxFx struct {
	t     *testing.T
	e     *notifytest.Env
	inbox *app.Inbox
	rend  *recRenderer
	id    domain.Identity
}

// newInboxFx is an Env with an inbox-backed in_app channel and an email channel, a
// topic on both, and an Inbox over them. cfg and def may be changed first.
func newInboxFx(t *testing.T, change ...func(*notify.Config, *domain.TopicDef)) *inboxFx {
	t.Helper()
	e := notifytest.NewEnv(t)
	e.AddChannel("in_app", domain.ChannelCapabilities{InboxBacked: true, Dedup: domain.DedupLocal})
	e.AddChannel("email", domain.ChannelCapabilities{NeedsAddress: true, NeedsSubject: true, Dedup: domain.DedupProvider})
	def := domain.TopicDef{DefaultChannels: []domain.ChannelKind{"in_app", "email"}, DefaultPriority: domain.PriorityInfo}
	cfg := e.Config
	for _, c := range change {
		c(&cfg, &def)
	}
	e.Topics.Register(notifytest.Realm, inboxTopic, def)
	rend := &recRenderer{inner: e.Templates}
	inbox, err := app.NewInbox(cfg, app.Deps{Inbox: e.Inbox, Renderer: rend, Topics: e.Topics, Channels: e.Channels})
	if err != nil {
		t.Fatalf("NewInbox: %v", err)
	}
	return &inboxFx{t: t, e: e, inbox: inbox, rend: rend, id: domain.Identity{Realm: notifytest.Realm,
		Tenant: domain.Tenant{Kind: "workspace", ID: "w1"}, Recipient: domain.Recipient{Kind: "user", ID: "u1"}}}
}

// accept sends a notification to id one minute after the last, with fallback copy
// and template data, and returns its id.
func (f *inboxFx) accept(id domain.Identity, key string) string {
	f.t.Helper()
	f.e.Clock.Advance(time.Minute)
	r, err := f.e.Notifier.Notify(f.t.Context(), domain.Notification{
		Tenant: id.Tenant, Recipient: id.Recipient, Topic: inboxTopic, IdemKey: key,
		Title: "Done " + key, Body: "Your upload finished.", Data: map[string]any{"doc": "d-" + key},
	})
	if err != nil || !r.Applied || !r.InboxVisible {
		f.t.Fatalf("Notify(%s) = %+v, %v, want applied and inbox-visible", key, r, err)
	}
	return r.NotificationID
}

func (f *inboxFx) put(ref string, ch domain.ChannelKind, locale, subject, body string) {
	f.e.Templates.Put(notifytest.Realm, ref, ch, locale, domain.RenderedContent{Subject: subject, Body: body})
}

func (f *inboxFx) list(q domain.ListQuery) domain.Page {
	f.t.Helper()
	p, err := f.inbox.List(f.t.Context(), f.id, q)
	if err != nil {
		f.t.Fatalf("List(%+v): %v", q, err)
	}
	return p
}

func (f *inboxFx) only(q domain.ListQuery) domain.InboxItem {
	f.t.Helper()
	p := f.list(q)
	if len(p.Items) != 1 {
		f.t.Fatalf("List(%+v) returned %d items, want 1", q, len(p.Items))
	}
	return p.Items[0]
}

// The copy comes from the topic's template in the requested locale, and falls back
// along the ladder the renderer fixes: the tag, its language, the default locale; and
// when nothing in it matches, the stored title and body. The fake renderer is the
// one T048's data-backed renderer will run the same table as.
func TestInboxRendersInTheViewersLocale(t *testing.T) {
	t.Parallel()
	type tpl struct{ locale, subject string }
	cases := []struct {
		name      string
		templates []tpl
		locale    string
		cfgLocale string // the configured default; "" leaves "en"
		want      string
	}{
		{"the requested locale", []tpl{{"fr", "Terminé"}, {"en", "Done"}}, "fr", "", "Terminé"},
		{"a regional tag with its own template", []tpl{{"fr-CA", "Fini"}, {"fr", "Terminé"}, {"en", "Done"}}, "fr-CA", "", "Fini"},
		{"a regional tag falls back to its language", []tpl{{"fr", "Terminé"}, {"en", "Done"}}, "fr-CA", "", "Terminé"},
		{"a locale with no template falls back to the default", []tpl{{"fr", "Terminé"}, {"en", "Done"}}, "de", "", "Done"},
		{"no locale asked: the configured default", []tpl{{"fr", "Terminé"}, {"en", "Done"}}, "", "fr", "Terminé"},
		{"no locale asked, and the default is en", []tpl{{"fr", "Terminé"}, {"en", "Done"}}, "", "", "Done"},
		{"nothing in the ladder: the stored copy", []tpl{{"fr", "Terminé"}}, "de", "", "Done k1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newInboxFx(t, func(c *notify.Config, _ *domain.TopicDef) {
				if tc.cfgLocale != "" {
					c.DefaultLocale = tc.cfgLocale
				}
			})
			if tc.cfgLocale != "" {
				// The renderer's own ladder ends at its default locale, which an
				// operator sets to the same value.
				f.e.Templates = notifytest.NewTemplates(tc.cfgLocale)
				f.rend.inner = f.e.Templates
			}
			for _, tp := range tc.templates {
				f.put(string(inboxTopic), "in_app", tp.locale, tp.subject, "body in "+tp.locale)
			}
			f.accept(f.id, "k1")
			item := f.only(domain.ListQuery{Locale: tc.locale})
			if item.Title != tc.want {
				t.Fatalf("Title = %q, want %q", item.Title, tc.want)
			}
			if tc.want == "Done k1" && item.Body != "Your upload finished." {
				t.Fatalf("Body = %q, want the stored body beside the stored title", item.Body)
			}
		})
	}
}

// A topic with no template shows what the producer stored (D27).
func TestInboxShowsTheStoredCopyWhenTheTopicHasNoTemplate(t *testing.T) {
	t.Parallel()
	f := newInboxFx(t)
	f.accept(f.id, "k1")
	item := f.only(domain.ListQuery{})
	if item.Title != "Done k1" || item.Body != "Your upload finished." {
		t.Fatalf("item = %q / %q, want the stored title and body", item.Title, item.Body)
	}
}

// A template edited after the notification was stored changes what the next read
// shows: the failure D27 names, copy fixed at the moment of sending.
func TestInboxShowsAnEditedTemplateOnTheNextRead(t *testing.T) {
	t.Parallel()
	f := newInboxFx(t)
	f.accept(f.id, "k1")

	f.put(string(inboxTopic), "in_app", "en", "Upload finished", "Version one.")
	if item := f.only(domain.ListQuery{}); item.Title != "Upload finished" || item.Body != "Version one." {
		t.Fatalf("first read = %q / %q", item.Title, item.Body)
	}
	f.put(string(inboxTopic), "in_app", "en", "Your upload is ready", "Version two.")
	if item := f.only(domain.ListQuery{}); item.Title != "Your upload is ready" || item.Body != "Version two." {
		t.Fatalf("read after the template changed = %q / %q, want the new copy: the old one was fixed at send time", item.Title, item.Body)
	}
}

// The template read is the inbox-backed channel's, found by its capability and not
// its kind (NR-016), and not the email channel's for the same topic.
func TestInboxReadsTheInboxBackedChannelsTemplate(t *testing.T) {
	t.Parallel()
	t.Run("in_app, beside email", func(t *testing.T) {
		t.Parallel()
		f := newInboxFx(t)
		f.put(string(inboxTopic), "email", "en", "Email subject", "Email body")
		f.put(string(inboxTopic), "in_app", "en", "In-app title", "In-app body")
		f.accept(f.id, "k1")
		if item := f.only(domain.ListQuery{}); item.Title != "In-app title" {
			t.Fatalf("Title = %q, want the inbox-backed channel's template", item.Title)
		}
	})
	t.Run("a feed channel that is inbox-backed", func(t *testing.T) {
		t.Parallel()
		e := notifytest.NewEnv(t)
		e.AddChannel("feed", domain.ChannelCapabilities{InboxBacked: true})
		e.AddChannel("email", domain.ChannelCapabilities{NeedsAddress: true})
		e.Topics.Register(notifytest.Realm, inboxTopic, domain.TopicDef{
			DefaultChannels: []domain.ChannelKind{"feed", "email"}, DefaultPriority: domain.PriorityInfo})
		e.Templates.Put(notifytest.Realm, string(inboxTopic), "feed", "en", domain.RenderedContent{Subject: "From the feed"})
		e.Templates.Put(notifytest.Realm, string(inboxTopic), "in_app", "en", domain.RenderedContent{Subject: "Wrong: in_app is not special"})
		inbox, err := app.NewInbox(e.Config, app.Deps{Inbox: e.Inbox, Renderer: e.Templates, Topics: e.Topics, Channels: e.Channels})
		if err != nil {
			t.Fatal(err)
		}
		id := domain.Identity{Realm: notifytest.Realm, Tenant: domain.Tenant{Kind: "workspace", ID: "w1"}, Recipient: domain.Recipient{Kind: "user", ID: "u1"}}
		if r, err := e.Notifier.Notify(t.Context(), domain.Notification{Tenant: id.Tenant, Recipient: id.Recipient, Topic: inboxTopic, IdemKey: "k1"}); err != nil || !r.InboxVisible {
			t.Fatalf("Notify = %+v, %v", r, err)
		}
		page, err := inbox.List(t.Context(), id, domain.ListQuery{})
		if err != nil || len(page.Items) != 1 || page.Items[0].Title != "From the feed" {
			t.Fatalf("List = %+v, %v, want the feed channel's template", page, err)
		}
	})
}

// A topic that names its own template ref is rendered from it, not from a template
// under its name.
func TestInboxReadsTheTopicsTemplateRef(t *testing.T) {
	t.Parallel()
	f := newInboxFx(t, func(_ *notify.Config, d *domain.TopicDef) { d.TemplateRef = "ingest_shared" })
	f.put("ingest_shared", "in_app", "en", "From the shared template", "b")
	f.put(string(inboxTopic), "in_app", "en", "From the topic's name", "b")
	f.accept(f.id, "k1")
	if item := f.only(domain.ListQuery{}); item.Title != "From the shared template" {
		t.Fatalf("Title = %q, want the topic's TemplateRef used", item.Title)
	}
}

// The renderer is asked in the viewer's scope: their realm and tenant, the topic, the
// inbox-backed channel, the locale, and the notification's own data, which is what a
// template renders from (D27).
func TestInboxAsksTheRendererInTheViewersScope(t *testing.T) {
	t.Parallel()
	f := newInboxFx(t)
	f.accept(f.id, "k1")
	f.only(domain.ListQuery{Locale: "fr"})
	reqs := f.rend.requests()
	if len(reqs) != 1 {
		t.Fatalf("%d render requests, want 1", len(reqs))
	}
	r := reqs[0]
	if r.Realm != f.id.Realm || r.Tenant != f.id.Tenant || r.Topic != inboxTopic || r.Channel != "in_app" || r.Locale != "fr" {
		t.Errorf("render request %+v, want the viewer's realm and tenant, the topic, in_app and fr", r)
	}
	if !maps.Equal(r.Data, map[string]any{"doc": "d-k1"}) {
		t.Errorf("render data %v, want the notification's own data", r.Data)
	}
	if len(r.Digest) != 0 {
		t.Errorf("render request carries a digest %v for a single notification", r.Digest)
	}
}

// A renderer that fails is an error the caller sees: a template store that is down
// or a template that cannot render is an operator's to fix, not a reason to show
// copy the template was meant to replace. "No template" is not a fault.
func TestInboxReturnsARenderFault(t *testing.T) {
	t.Parallel()
	f := newInboxFx(t)
	f.accept(f.id, "k1")
	boom := errors.New("template store down")
	f.rend.err = boom
	if _, err := f.inbox.List(t.Context(), f.id, domain.ListQuery{}); !errors.Is(err, boom) {
		t.Fatalf("List = %v, want the renderer's error", err)
	}
}

// A page keeps the store's order and its cursor, and every item on it is rendered.
func TestInboxPagesAreRenderedAndKeepTheirCursor(t *testing.T) {
	t.Parallel()
	f := newInboxFx(t)
	f.put(string(inboxTopic), "in_app", "en", "Upload finished", "b")
	ids := []string{f.accept(f.id, "k1"), f.accept(f.id, "k2"), f.accept(f.id, "k3")}

	first := f.list(domain.ListQuery{Limit: 2})
	if len(first.Items) != 2 || first.NextCursor == "" {
		t.Fatalf("first page = %d items, cursor %q, want 2 and a cursor", len(first.Items), first.NextCursor)
	}
	if first.Items[0].ID != ids[2] || first.Items[1].ID != ids[1] {
		t.Errorf("first page ids %s, %s, want newest first %s, %s", first.Items[0].ID, first.Items[1].ID, ids[2], ids[1])
	}
	second := f.list(domain.ListQuery{Limit: 2, Cursor: first.NextCursor})
	if len(second.Items) != 1 || second.Items[0].ID != ids[0] || second.NextCursor != "" {
		t.Fatalf("second page = %+v, want the oldest alone and no cursor", second)
	}
	for _, it := range append(first.Items, second.Items...) {
		if it.Title != "Upload finished" {
			t.Errorf("item %s title %q, want rendered", it.ID, it.Title)
		}
	}
	if _, err := f.inbox.List(t.Context(), f.id, domain.ListQuery{Cursor: "not-a-cursor"}); !errors.Is(err, domain.ErrInvalidCursor) {
		t.Fatalf("List with a forged cursor = %v, want ErrInvalidCursor from the store", err)
	}
}

// What is not copy is the store's, unchanged: the data and payload, and the three
// independent states.
func TestInboxLeavesEverythingButTheCopyAlone(t *testing.T) {
	t.Parallel()
	f := newInboxFx(t)
	f.put(string(inboxTopic), "in_app", "en", "Upload finished", "b")
	nid := f.accept(f.id, "k1")
	if err := f.inbox.MarkRead(t.Context(), f.id, []string{nid}); err != nil {
		t.Fatal(err)
	}
	item := f.only(domain.ListQuery{})
	if item.ID != nid || item.Topic != inboxTopic || item.Priority != domain.PriorityInfo || !maps.Equal(item.Data, map[string]any{"doc": "d-k1"}) {
		t.Errorf("item %+v lost something besides the copy", item)
	}
	if item.ReadAt.IsZero() || !item.SeenAt.IsZero() || !item.ArchivedAt.IsZero() {
		t.Errorf("states read %v seen %v archived %v, want only read set", item.ReadAt, item.SeenAt, item.ArchivedAt)
	}
}

// The badge is the store's count, bounded at the configured cap (D15).
func TestInboxUnreadStopsAtTheConfiguredCap(t *testing.T) {
	t.Parallel()
	f := newInboxFx(t, func(c *notify.Config, _ *domain.TopicDef) { c.UnreadCap = 2 })
	for _, k := range []string{"k1", "k2", "k3"} {
		f.accept(f.id, k)
	}
	got, err := f.inbox.Unread(t.Context(), f.id)
	if err != nil || got.Count != 2 || !got.Capped {
		t.Fatalf("Unread = %+v, %v, want a count of 2, capped", got, err)
	}

	g := newInboxFx(t)
	g.accept(g.id, "k1")
	if got, err := g.inbox.Unread(t.Context(), g.id); err != nil || got.Count != 1 || got.Capped {
		t.Fatalf("Unread with one under the default cap = %+v, %v, want 1, not capped", got, err)
	}
}

// The marks are the store's, for the caller's identity only, and its errors are
// passed on: another tenant's notification is not found, whatever its id (NR-008).
func TestInboxMarksActOnTheCallersOwnNotifications(t *testing.T) {
	t.Parallel()
	f := newInboxFx(t)
	a, b := f.accept(f.id, "k1"), f.accept(f.id, "k2")
	other := domain.Identity{Realm: f.id.Realm, Tenant: domain.Tenant{Kind: "workspace", ID: "w2"}, Recipient: f.id.Recipient}
	foreign := f.accept(other, "k1")

	if err := f.inbox.MarkRead(t.Context(), f.id, []string{a}); err != nil {
		t.Fatal(err)
	}
	if got, _ := f.inbox.Unread(t.Context(), f.id); got.Count != 1 {
		t.Fatalf("Unread after MarkRead = %d, want 1", got.Count)
	}
	if err := f.inbox.MarkRead(t.Context(), f.id, []string{foreign}); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("MarkRead of another tenant's notification = %v, want ErrNotFound", err)
	}

	if err := f.inbox.Archive(t.Context(), f.id, []string{b}, true); err != nil {
		t.Fatal(err)
	}
	if got := f.list(domain.ListQuery{}); len(got.Items) != 1 || got.Items[0].ID != a {
		t.Fatalf("default list after archiving %s = %+v, want only %s", b, got.Items, a)
	}
	if got := f.list(domain.ListQuery{State: domain.InboxArchived}); len(got.Items) != 1 || got.Items[0].ID != b {
		t.Fatalf("archived list = %+v, want %s", got.Items, b)
	}

	if err := f.inbox.MarkSeen(t.Context(), f.id, f.e.Clock.Now()); err != nil {
		t.Fatal(err)
	}
	if item := f.only(domain.ListQuery{}); item.SeenAt.IsZero() {
		t.Fatal("MarkSeen did not mark the item seen")
	}
	if err := f.inbox.Archive(t.Context(), f.id, []string{b}, false); err != nil {
		t.Fatal(err)
	}
	if err := f.inbox.MarkAllRead(t.Context(), f.id); err != nil {
		t.Fatal(err)
	}
	if got, _ := f.inbox.Unread(t.Context(), f.id); got.Count != 0 {
		t.Fatalf("Unread after MarkAllRead = %d, want 0", got.Count)
	}
	if got, _ := f.inbox.Unread(t.Context(), other); got.Count != 1 {
		t.Fatalf("the other tenant's unread = %d, want its own 1: nothing leaked across", got.Count)
	}
}
