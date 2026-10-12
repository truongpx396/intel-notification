//go:build integration

package postgres

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/truongpx396/intel-notification/domain"
)

// T017: the recipient's read and write path over the inbox, as the table owner
// under the recipient's scope (NR-024, NR-033, D15, D26).

// accept persists an inbox-visible notification for id one minute after the last
// thing the clock was moved for, so every notification has its own created_at, and
// returns its id. mutate, if given, changes the notification and its plan first.
func (e *env) accept(id domain.Identity, key string, mutate ...func(*domain.Notification, *domain.DeliveryPlan)) string {
	e.t.Helper()
	e.clock.Advance(time.Minute)
	n, p := notif(id, key), planOf(1, immediate("in_app"))
	n.Title = "title " + key
	for _, m := range mutate {
		m(&n, &p)
	}
	return e.persist(id, n, p).NotificationID
}

type flags struct{ Seen, Read, Archived *time.Time }

func (e *env) flags(nid string) flags {
	e.t.Helper()
	var f flags
	if err := e.db.Admin.QueryRow(e.t.Context(),
		`SELECT seen_at, read_at, archived_at FROM notifications WHERE id = $1::uuid`, nid).
		Scan(&f.Seen, &f.Read, &f.Archived); err != nil {
		e.t.Fatal(err)
	}
	return f
}

// list returns the ids of one page, and the cursor to the next.
func (e *env) list(id domain.Identity, q domain.ListQuery) ([]string, string) {
	e.t.Helper()
	page, err := e.inbox.List(e.t.Context(), id, q)
	if err != nil {
		e.t.Fatalf("List(%+v): %v", q, err)
	}
	var ids []string
	for _, it := range page.Items {
		ids = append(ids, it.ID)
	}
	return ids, page.NextCursor
}

func (e *env) unread(id domain.Identity, limit int) domain.UnreadCount {
	e.t.Helper()
	c, err := e.inbox.Unread(e.t.Context(), id, limit)
	if err != nil {
		e.t.Fatalf("Unread(%d): %v", limit, err)
	}
	return c
}

func reversed(ids ...string) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[len(ids)-1-i] = id
	}
	return out
}

func hidden(n *domain.Notification, p *domain.DeliveryPlan) { p.InboxVisible = false }

// NS-001, NR-008. The same recipient id in two tenants, in two realms, and a
// neighbour in the same tenant: each list, count and read sees only its own.
func TestInboxIsScoped(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	mine, otherTenant, neighbour := ident("w1", "u1"), ident("w2", "u1"), ident("w1", "u2")
	otherRealm := domain.Identity{Realm: "other", Tenant: mine.Tenant, Recipient: mine.Recipient}

	a := e.accept(mine, "k1")
	b := e.accept(mine, "k2")
	c := e.accept(otherTenant, "k1")
	d := e.accept(neighbour, "k1")
	f := e.accept(otherRealm, "k1")

	for _, tc := range []struct {
		name string
		id   domain.Identity
		want []string
	}{
		{"the recipient", mine, []string{b, a}},
		{"the same recipient id in another tenant", otherTenant, []string{c}},
		{"another recipient of the tenant", neighbour, []string{d}},
		{"the same tenant and recipient in another realm", otherRealm, []string{f}},
		{"a tenant with nothing", ident("w3", "u1"), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := e.with(t)
			if got, _ := e.list(tc.id, domain.ListQuery{}); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("List = %v, want %v", got, tc.want)
			}
			if got := e.unread(tc.id, 99); got != (domain.UnreadCount{Count: len(tc.want)}) {
				t.Errorf("Unread = %+v, want %d", got, len(tc.want))
			}
		})
	}

	t.Run("a notification of another tenant is not readable by id", func(t *testing.T) {
		t.Parallel()
		e := e.with(t)
		if n, err := e.inbox.Get(t.Context(), mine, c); err != nil || n != nil {
			t.Errorf("Get(another tenant's id) = %+v, %v, want nil", n, err)
		}
	})
}

// NR-033, D26. Seen, read and archived are three states, none inferred from
// another: each transition sets its own timestamp and leaves the other two as they
// were, and doing it again keeps the first time.
func TestInboxStatesAreIndependent(t *testing.T) {
	t.Parallel()
	id := ident("w1", "u1")
	type act func(e *env, nid string)
	seen := func(e *env, nid string) {
		if err := e.inbox.MarkSeen(e.t.Context(), id, e.clock.Now()); err != nil {
			e.t.Fatal(err)
		}
	}
	read := func(e *env, nid string) {
		if err := e.inbox.MarkRead(e.t.Context(), id, []string{nid}); err != nil {
			e.t.Fatal(err)
		}
	}
	readAll := func(e *env, nid string) {
		if err := e.inbox.MarkAllRead(e.t.Context(), id); err != nil {
			e.t.Fatal(err)
		}
	}
	archive := func(e *env, nid string) {
		if err := e.inbox.Archive(e.t.Context(), id, []string{nid}, true); err != nil {
			e.t.Fatal(err)
		}
	}
	unarchive := func(e *env, nid string) {
		if err := e.inbox.Archive(e.t.Context(), id, []string{nid}, false); err != nil {
			e.t.Fatal(err)
		}
	}

	// before is applied first, to give the other two states a value to be left alone.
	cases := []struct {
		name                 string
		before               []act
		do                   act
		seen, read, archived bool // after, which are set
		why                  string
	}{
		{"marking seen", nil, seen, true, false, false, "seen is the bell cleared, not the item read"},
		{"marking read", nil, read, false, true, false, "read does not imply seen"},
		{"marking all read", nil, readAll, false, true, false, ""},
		{"archiving", nil, archive, false, false, true, "archived does not imply read"},
		{"marking seen leaves read and archived as they were", []act{read, archive}, seen, true, true, true, ""},
		{"marking read leaves seen and archived as they were", []act{seen, archive}, read, true, true, true, ""},
		{"archiving leaves seen and read as they were", []act{seen, read}, archive, true, true, true, ""},
		{"unarchiving clears only archived", []act{seen, read, archive}, unarchive, true, true, false, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			nid := e.accept(id, "k")
			for _, b := range tc.before {
				e.clock.Advance(time.Minute)
				b(e, nid)
			}
			before := e.flags(nid)
			e.clock.Advance(time.Minute)
			tc.do(e, nid)
			got := e.flags(nid)
			for name, c := range map[string]struct {
				got  *time.Time
				want bool
			}{"seen": {got.Seen, tc.seen}, "read": {got.Read, tc.read}, "archived": {got.Archived, tc.archived}} {
				if (c.got != nil) != c.want {
					t.Errorf("%s set = %v, want %v: %s", name, c.got != nil, c.want, tc.why)
				}
			}
			// What was already set kept its time.
			if before.Seen != nil && !before.Seen.Equal(*got.Seen) || before.Read != nil && !before.Read.Equal(*got.Read) {
				t.Errorf("a state that was already set changed its time: before %+v, after %+v", before, got)
			}
		})
	}

	t.Run("doing it again keeps the first time", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t)
		nid := e.accept(id, "k")
		for name, do := range map[string]act{"seen": seen, "read": read, "archived": archive} {
			do(e, nid)
			first := e.flags(nid)
			e.clock.Advance(time.Hour)
			do(e, nid)
			if again := e.flags(nid); !reflect.DeepEqual(first, again) {
				t.Errorf("%s twice: %+v then %+v, want the first time kept", name, first, again)
			}
		}
	})
}

// NR-008, REST: "marking another recipient's notification read returns 404 and
// changes nothing". A missing id, a malformed one and another tenant's are the same
// answer, and a request that names any one of them changes none of the others.
func TestInboxRefusesNotificationsThatAreNotTheCallers(t *testing.T) {
	t.Parallel()
	mine, theirs := ident("w1", "u1"), ident("w2", "u1")
	acts := map[string]func(e *env, ids []string) error{
		"MarkRead": func(e *env, ids []string) error { return e.inbox.MarkRead(e.t.Context(), mine, ids) },
		"Archive":  func(e *env, ids []string) error { return e.inbox.Archive(e.t.Context(), mine, ids, true) },
	}
	for name, act := range acts {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			own := e.accept(mine, "k1")
			foreign := e.accept(theirs, "k1")
			unknown := "01a0a25d-0400-7000-8000-000000000000"

			var messages []string
			for _, ids := range [][]string{{foreign}, {unknown}, {"not-a-uuid"}, {own, foreign}, {foreign, own}, {own, unknown, own}} {
				err := act(e, ids)
				if !errors.Is(err, domain.ErrNotFound) {
					t.Fatalf("%s(%v) = %v, want ErrNotFound", name, ids, err)
				}
				messages = append(messages, err.Error())
			}
			for _, m := range messages {
				if m != messages[0] {
					t.Errorf("errors differ (%q and %q): the answer must not say whether an id exists", messages[0], m)
				}
			}
			if got := e.flags(own); got != (flags{}) {
				t.Errorf("a refused request changed the caller's own notification: %+v", got)
			}
			if got := e.flags(foreign); got != (flags{}) {
				t.Errorf("a refused request changed another tenant's notification: %+v", got)
			}
			if err := act(e, nil); err != nil {
				t.Errorf("%s with no ids = %v, want a no-op", name, err)
			}
			if err := act(e, []string{own, own}); err != nil {
				t.Errorf("%s with a repeated id = %v, want it counted once", name, err)
			}
		})
	}

	t.Run("MarkAllRead and MarkSeen never reach another recipient", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t)
		e.accept(mine, "k1")
		foreign := e.accept(theirs, "k1")
		if err := e.inbox.MarkAllRead(t.Context(), mine); err != nil {
			t.Fatal(err)
		}
		if err := e.inbox.MarkSeen(t.Context(), mine, e.clock.Now()); err != nil {
			t.Fatal(err)
		}
		if got := e.flags(foreign); got != (flags{}) {
			t.Errorf("another tenant's notification was marked: %+v", got)
		}
	})
}

// NR-024, D15. The badge is bounded: it reads at most limit+1 rows, reports limit,
// and says it stopped.
func TestInboxUnreadIsBounded(t *testing.T) {
	t.Parallel()
	const cap = 5
	id := ident("w1", "u1")
	for _, n := range []int{0, 1, cap - 1, cap, cap + 1, 2 * cap} {
		t.Run(fmt.Sprintf("%d unread", n), func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			for i := range n {
				e.accept(id, fmt.Sprintf("k%d", i))
			}
			want := domain.UnreadCount{Count: min(n, cap), Capped: n > cap}
			if got := e.unread(id, cap); got != want {
				t.Errorf("Unread(%d) with %d unread = %+v, want %+v", cap, n, got, want)
			}
		})
	}

	t.Run("read and archived notifications do not count", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t)
		a := e.accept(id, "a")
		b := e.accept(id, "b")
		e.accept(id, "c")
		if err := e.inbox.MarkRead(t.Context(), id, []string{a}); err != nil {
			t.Fatal(err)
		}
		if err := e.inbox.Archive(t.Context(), id, []string{b}, true); err != nil {
			t.Fatal(err)
		}
		if got := e.unread(id, cap); got != (domain.UnreadCount{Count: 1}) {
			t.Errorf("Unread = %+v, want only the one not read and not archived", got)
		}
		if err := e.inbox.Archive(t.Context(), id, []string{b}, false); err != nil {
			t.Fatal(err)
		}
		if got := e.unread(id, cap); got.Count != 2 {
			t.Errorf("Unread after unarchiving = %+v, want the notification back", got)
		}
	})

	t.Run("a limit below one is refused", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t)
		for _, limit := range []int{0, -1} {
			if _, err := e.inbox.Unread(t.Context(), id, limit); err == nil {
				t.Errorf("Unread(%d) was accepted", limit)
			}
		}
	})
}

// NR-012, D26, D29. What the inbox does not show is, to every method, absent: a
// row with no inbox-backed channel enabled, a canceled one, and a scheduled one
// before its send time. It is neither listed nor counted, cannot be read by id, and
// cannot be marked.
func TestInboxHidesWhatIsNotInTheInbox(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	id := ident("w1", "u1")
	shown := e.accept(id, "shown")
	noChannel := e.accept(id, "no-inbox-backed-channel", hidden)
	sendAt := e.clock.Now().Add(2 * time.Hour)
	scheduled := e.accept(id, "scheduled", func(n *domain.Notification, p *domain.DeliveryPlan) {
		n.DeliverAfter, p.VisibleFrom = sendAt, sendAt
	})
	canceled := e.accept(id, "canceled")
	if r, err := e.s.Cancel(t.Context(), id, "canceled"); err != nil || !r.Matched {
		t.Fatalf("cancel: %+v %v", r, err)
	}

	t.Run("before the send time", func(t *testing.T) {
		t.Parallel()
		e := e.with(t)
		if got, _ := e.list(id, domain.ListQuery{}); !reflect.DeepEqual(got, []string{shown}) {
			t.Errorf("List = %v, want only %v", got, shown)
		}
		if got := e.unread(id, 99); got.Count != 1 {
			t.Errorf("Unread = %+v, want 1", got)
		}
		for name, nid := range map[string]string{"no inbox-backed channel": noChannel, "scheduled": scheduled, "canceled": canceled} {
			if n, err := e.inbox.Get(t.Context(), id, nid); err != nil || n != nil {
				t.Errorf("Get(%s) = %+v, %v, want nil", name, n, err)
			}
			if err := e.inbox.MarkRead(t.Context(), id, []string{nid}); !errors.Is(err, domain.ErrNotFound) {
				t.Errorf("MarkRead(%s) = %v, want ErrNotFound", name, err)
			}
		}
		if err := e.inbox.MarkAllRead(t.Context(), id); err != nil {
			t.Fatal(err)
		}
		for name, nid := range map[string]string{"no inbox-backed channel": noChannel, "scheduled": scheduled, "canceled": canceled} {
			if got := e.flags(nid); got != (flags{}) {
				t.Errorf("MarkAllRead marked the %s notification: %+v", name, got)
			}
		}
	})

	t.Run("a scheduled notification appears at its send time, not before", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t)
		send := e.clock.Now().Add(2 * time.Hour)
		nid := e.accept(id, "later", func(n *domain.Notification, p *domain.DeliveryPlan) { n.DeliverAfter, p.VisibleFrom = send, send })
		e.clock.Set(send.Add(-time.Nanosecond))
		if got, _ := e.list(id, domain.ListQuery{}); len(got) != 0 {
			t.Errorf("List one nanosecond before the send time = %v", got)
		}
		e.clock.Set(send)
		if got, _ := e.list(id, domain.ListQuery{}); !reflect.DeepEqual(got, []string{nid}) || e.unread(id, 9).Count != 1 {
			t.Errorf("at the send time, List = %v and Unread = %+v, want it listed and counted", got, e.unread(id, 9))
		}
	})
}

// NR-033. A page of the inbox: newest first, filtered by state and topic, walked by
// a cursor that survives new arrivals.
func TestInboxList(t *testing.T) {
	t.Parallel()
	id := ident("w1", "u1")

	t.Run("newest first, with every field the inbox shows", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t)
		first := e.accept(id, "k1", func(n *domain.Notification, _ *domain.DeliveryPlan) { n.Priority = domain.PriorityCritical })
		second := e.accept(id, "k2")
		if err := e.inbox.MarkRead(t.Context(), id, []string{first}); err != nil {
			t.Fatal(err)
		}
		page, err := e.inbox.List(t.Context(), id, domain.ListQuery{})
		if err != nil || page.NextCursor != "" || len(page.Items) != 2 {
			t.Fatalf("page = %+v, %v", page, err)
		}
		if page.Items[0].ID != second || page.Items[1].ID != first {
			t.Fatalf("order = %s, %s, want newest first", page.Items[0].ID, page.Items[1].ID)
		}
		it := page.Items[1]
		if it.Topic != "ingestion_complete" || it.Priority != domain.PriorityCritical || it.Title != "title k1" ||
			it.Body != "Your upload finished." || !reflect.DeepEqual(it.Data, notif(id, "k1").Data) ||
			!reflect.DeepEqual(it.Payload, map[string]string{"doc_id": "d1"}) {
			t.Errorf("item = %+v", it)
		}
		if !it.CreatedAt.Equal(fixtureDay.Add(time.Minute)) || it.ReadAt.IsZero() || !it.SeenAt.IsZero() || !it.ArchivedAt.IsZero() {
			t.Errorf("item times = created %v, seen %v, read %v, archived %v", it.CreatedAt, it.SeenAt, it.ReadAt, it.ArchivedAt)
		}
		if page.Items[0].ReadAt != (time.Time{}) {
			t.Errorf("an unread item has ReadAt %v", page.Items[0].ReadAt)
		}
	})

	t.Run("filtered by state and topic", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t)
		other := func(n *domain.Notification, _ *domain.DeliveryPlan) { n.Topic = "invite_received" }
		unreadOne := e.accept(id, "unread")
		readOne := e.accept(id, "read")
		archivedOne := e.accept(id, "archived")
		inviteOne := e.accept(id, "invite", other)
		if err := e.inbox.MarkRead(t.Context(), id, []string{readOne}); err != nil {
			t.Fatal(err)
		}
		if err := e.inbox.Archive(t.Context(), id, []string{archivedOne}, true); err != nil {
			t.Fatal(err)
		}
		for _, tc := range []struct {
			name string
			q    domain.ListQuery
			want []string
		}{
			{"the default list is everything not archived", domain.ListQuery{}, []string{inviteOne, readOne, unreadOne}},
			{"unread", domain.ListQuery{State: domain.InboxUnread}, []string{inviteOne, unreadOne}},
			{"read", domain.ListQuery{State: domain.InboxRead}, []string{readOne}},
			{"archived", domain.ListQuery{State: domain.InboxArchived}, []string{archivedOne}},
			{"a topic", domain.ListQuery{Topic: "invite_received"}, []string{inviteOne}},
			{"a topic and a state", domain.ListQuery{Topic: "ingestion_complete", State: domain.InboxUnread}, []string{unreadOne}},
			{"a topic nothing has", domain.ListQuery{Topic: "no_such_topic"}, nil},
		} {
			if got, _ := e.list(id, tc.q); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("%s: List = %v, want %v", tc.name, got, tc.want)
			}
		}
		if _, err := e.inbox.List(t.Context(), id, domain.ListQuery{State: "bogus"}); err == nil {
			t.Error("an unknown state was accepted")
		}
	})

	t.Run("a cursor walks every page once, even as new notifications arrive", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t)
		var oldestFirst []string
		for i := range 7 {
			oldestFirst = append(oldestFirst, e.accept(id, fmt.Sprintf("k%d", i)))
		}
		want := reversed(oldestFirst...)

		page1, cursor := e.list(id, domain.ListQuery{Limit: 3})
		e.accept(id, "arrives-between-pages")
		page2, cursor2 := e.list(id, domain.ListQuery{Limit: 3, Cursor: cursor})
		page3, cursor3 := e.list(id, domain.ListQuery{Limit: 3, Cursor: cursor2})

		if got := append(append(page1, page2...), page3...); !reflect.DeepEqual(got, want) {
			t.Errorf("pages %v, %v, %v joined = %v, want %v: no gap and no repeat", page1, page2, page3, got, want)
		}
		if len(page1) != 3 || len(page2) != 3 || len(page3) != 1 || cursor == "" || cursor2 == "" || cursor3 != "" {
			t.Errorf("page sizes %d/%d/%d, cursors %q/%q/%q, want 3/3/1 and the last empty",
				len(page1), len(page2), len(page3), cursor, cursor2, cursor3)
		}
	})

	t.Run("a notification created in the same instant as another is not skipped at a page boundary", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t)
		var all []string
		for i := range 5 {
			n, p := notif(id, fmt.Sprintf("same-%d", i)), planOf(1, immediate("in_app")) // the clock stays put
			all = append(all, e.persist(id, n, p).NotificationID)
		}
		var got []string
		cursor := ""
		for range 5 {
			page, next := e.list(id, domain.ListQuery{Limit: 2, Cursor: cursor})
			got = append(got, page...)
			if cursor = next; cursor == "" {
				break
			}
		}
		if len(got) != 5 || len(map[string]bool{got[0]: true, got[1]: true, got[2]: true, got[3]: true, got[4]: true}) != 5 {
			t.Errorf("walked %v, want the five notifications once each (%v)", got, all)
		}
	})

	t.Run("a limit that is missing or too large is replaced", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t)
		if _, err := e.db.Admin.Exec(t.Context(), `
INSERT INTO notifications (id, realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
                           topic, idem_key, created_at, visible_from)
SELECT gen_random_uuid(), 'aisat', 'workspace', 'w1', 'user', 'u1', 't', 'k' || g,
       $1::timestamptz + g * interval '1 second', $1::timestamptz
  FROM generate_series(1, $2::int) AS g`, fixtureDay, MaxPageSize+50); err != nil {
			t.Fatal(err)
		}
		for _, tc := range []struct {
			name        string
			limit, want int
		}{
			{"zero takes the default", 0, DefaultPageSize},
			{"a negative takes the default", -4, DefaultPageSize},
			{"a modest limit is honoured", 7, 7},
			{"a huge limit is capped", 100000, MaxPageSize},
		} {
			got, cursor := e.list(id, domain.ListQuery{Limit: tc.limit})
			if len(got) != tc.want || cursor == "" {
				t.Errorf("%s: %d items, cursor %q, want %d and a next page", tc.name, len(got), cursor, tc.want)
			}
		}
	})

	t.Run("a cursor the store did not issue is refused", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t)
		e.accept(id, "k")
		for _, cursor := range []string{"garbage", "!!!", "MTIzNDU2Nzg5", "Zm9vOmJhcg"} {
			if _, err := e.inbox.List(t.Context(), id, domain.ListQuery{Cursor: cursor}); !errors.Is(err, domain.ErrInvalidCursor) {
				t.Errorf("cursor %q: err = %v, want ErrInvalidCursor", cursor, err)
			}
		}
	})
}

// Get is the inbox's view of one notification.
func TestInboxGet(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	id := ident("w1", "u1")
	nid := e.accept(id, "k")

	n, err := e.inbox.Get(t.Context(), id, nid)
	if err != nil || n == nil || n.IdemKey != "k" || n.Title != "title k" || n.Topic != "ingestion_complete" {
		t.Fatalf("Get = %+v, %v", n, err)
	}
	if err := e.inbox.Archive(t.Context(), id, []string{nid}, true); err != nil {
		t.Fatal(err)
	}
	if n, err := e.inbox.Get(t.Context(), id, nid); err != nil || n == nil {
		t.Errorf("an archived notification is still the caller's: Get = %+v, %v", n, err)
	}
	for _, other := range []string{"01a0a25d-0400-7000-8000-000000000000", "not-a-uuid", ""} {
		if n, err := e.inbox.Get(t.Context(), id, other); err != nil || n != nil {
			t.Errorf("Get(%q) = %+v, %v, want nil and no error", other, n, err)
		}
	}
}

// MarkSeen clears the bell up to an instant: everything created at or before it,
// and nothing after, nothing hidden, and nothing archived or read changes.
func TestInboxMarkSeenUpToAnInstant(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	id := ident("w1", "u1")
	old := e.accept(id, "old")
	boundary := e.accept(id, "boundary")
	boundaryAt := e.clock.Now()
	newer := e.accept(id, "newer")
	hiddenOne := e.accept(id, "hidden", hidden)

	if err := e.inbox.MarkSeen(t.Context(), id, boundaryAt); err != nil {
		t.Fatal(err)
	}
	for name, c := range map[string]struct {
		nid  string
		seen bool
	}{"before the instant": {old, true}, "at the instant": {boundary, true}, "after it": {newer, false}, "hidden": {hiddenOne, false}} {
		if got := e.flags(c.nid).Seen != nil; got != c.seen {
			t.Errorf("%s: seen = %v, want %v", name, got, c.seen)
		}
	}
}

// MarkAllRead reads what the bell shows: not what was put away, and not what is
// hidden.
func TestInboxMarkAllReadMarksOnlyWhatIsShown(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	id := ident("w1", "u1")
	shown := e.accept(id, "shown")
	archived := e.accept(id, "archived")
	hiddenOne := e.accept(id, "hidden", hidden)
	if err := e.inbox.Archive(t.Context(), id, []string{archived}, true); err != nil {
		t.Fatal(err)
	}
	if err := e.inbox.MarkAllRead(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	for name, c := range map[string]struct {
		nid  string
		read bool
	}{"shown": {shown, true}, "archived": {archived, false}, "hidden": {hiddenOne, false}} {
		if got := e.flags(c.nid).Read != nil; got != c.read {
			t.Errorf("%s: read = %v, want %v", name, got, c.read)
		}
	}
	if got := e.unread(id, 9); got.Count != 0 {
		t.Errorf("Unread after MarkAllRead = %+v", got)
	}
}
