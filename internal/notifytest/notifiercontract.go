//go:build integration

package notifytest

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/truongpx396/intel-notification/domain"
)

// contractTopic is the topic every NotifierContract case sends on: in_app and
// email in parallel, at info priority.
const contractTopic domain.Topic = "ingestion_complete"

var (
	contractTenant = domain.Tenant{Kind: "workspace", ID: "w1"}
	contractUser   = domain.Recipient{Kind: "user", ID: "u1"}
)

// notifierFixture is one case's world: an Env with an inbox-backed in_app channel
// and an email channel registered, and the topic above registered on both.
type notifierFixture struct {
	t  *testing.T
	e  *Env
	id domain.Identity
}

func newNotifierFixture(t *testing.T, newEnv func(*testing.T) *Env) *notifierFixture {
	t.Helper()
	e := newEnv(t)
	e.AddChannel("in_app", domain.ChannelCapabilities{InboxBacked: true, Dedup: domain.DedupLocal})
	e.AddChannel("email", domain.ChannelCapabilities{NeedsSubject: true, NeedsAddress: true, Dedup: domain.DedupProvider})
	e.Topics.Register(Realm, contractTopic, domain.TopicDef{
		DefaultChannels: []domain.ChannelKind{"in_app", "email"},
		DefaultPriority: domain.PriorityInfo,
	})
	return &notifierFixture{t: t, e: e, id: domain.Identity{Realm: Realm, Tenant: contractTenant, Recipient: contractUser}}
}

// note is a valid notification for the fixture's recipient.
func (f *notifierFixture) note(idemKey string) domain.Notification {
	return domain.Notification{
		Tenant: f.id.Tenant, Recipient: f.id.Recipient, Topic: contractTopic, IdemKey: idemKey,
		Title: "Done", Body: "Your upload finished.", OccurredAt: Epoch,
	}
}

// notify sends n and fails the case unless it was accepted.
func (f *notifierFixture) notify(n domain.Notification) domain.Receipt {
	f.t.Helper()
	r, err := f.e.Notifier.Notify(f.t.Context(), n)
	if err != nil {
		f.t.Fatalf("Notify(%s): %v", n.IdemKey, err)
	}
	return r
}

// rows counts the rows id has in table, as the superuser: the engine's own
// connection would see only its scope, and a case asks what is really there.
func (f *notifierFixture) rows(table string, id domain.Identity) int {
	f.t.Helper()
	var n int
	err := f.e.DB.Admin.QueryRow(f.t.Context(),
		`SELECT count(*) FROM `+table+` WHERE realm = $1 AND tenant_kind = $2 AND tenant_id = $3
		   AND recipient_kind = $4 AND recipient_id = $5`,
		string(id.Realm), id.Tenant.Kind, id.Tenant.ID, id.Recipient.Kind, id.Recipient.ID).Scan(&n)
	if err != nil {
		f.t.Fatalf("count %s: %v", table, err)
	}
	return n
}

// nothingPersisted fails unless id has no row in any table Notify writes.
func (f *notifierFixture) nothingPersisted(id domain.Identity, because string) {
	f.t.Helper()
	for _, table := range []string{"notifications", "notify_idem", "notification_outbox", "digest_buffer", "notification_deliveries"} {
		if n := f.rows(table, id); n != 0 {
			f.t.Errorf("%d rows in %s, want none: %s", n, table, because)
		}
	}
}

// queuedChannels lists the channels notification nid has a queue row for, sorted.
func (f *notifierFixture) queuedChannels(nid string) []string {
	f.t.Helper()
	rows, err := f.e.DB.Admin.Query(f.t.Context(),
		`SELECT channel FROM notification_outbox WHERE notification_id = $1::uuid ORDER BY channel`, nid)
	if err != nil {
		f.t.Fatalf("queued channels: %v", err)
	}
	defer rows.Close()
	var chs []string
	for rows.Next() {
		var ch string
		if err := rows.Scan(&ch); err != nil {
			f.t.Fatal(err)
		}
		chs = append(chs, ch)
	}
	if err := rows.Err(); err != nil {
		f.t.Fatal(err)
	}
	return chs
}

// queueRow is one queue row's schedule, as stored.
type queueRow struct {
	NextAttemptAt time.Time
	DeliverBefore *time.Time
	Quiet         bool
	DueNow        bool // next_attempt_at has passed on the database's clock
}

func (f *notifierFixture) queueRow(nid string, ch domain.ChannelKind) queueRow {
	f.t.Helper()
	var q queueRow
	err := f.e.DB.Admin.QueryRow(f.t.Context(),
		`SELECT next_attempt_at, deliver_before, quiet_hours_override, next_attempt_at <= now()
		   FROM notification_outbox WHERE notification_id = $1::uuid AND channel = $2`, nid, string(ch)).
		Scan(&q.NextAttemptAt, &q.DeliverBefore, &q.Quiet, &q.DueNow)
	if err != nil {
		f.t.Fatalf("queue row of %s on %s: %v", nid, ch, err)
	}
	return q
}

func (f *notifierFixture) inboxVisible(nid string) (visible bool, from time.Time) {
	f.t.Helper()
	err := f.e.DB.Admin.QueryRow(f.t.Context(),
		`SELECT inbox_visible, visible_from FROM notifications WHERE id = $1::uuid`, nid).Scan(&visible, &from)
	if err != nil {
		f.t.Fatalf("inbox row of %s: %v", nid, err)
	}
	return visible, from
}

func (f *notifierFixture) shard() domain.Shard {
	return domain.ShardFor(f.id, f.e.Config.WithDefaults().Shards)
}

func channelsOf(r domain.Receipt) []string {
	var chs []string
	for _, d := range r.Deliveries {
		chs = append(chs, string(d.Channel))
	}
	slices.Sort(chs)
	return chs
}

// NotifierContract runs against a real Notifier, built by newEnv over the Env's
// PostgreSQL and its pre-check (T024). Each case builds its own Env. It asserts
// NS-001 and NS-002 through the producer's port: that a replay is one row and one
// enqueue per channel, that two tenants never share a key, that a pre-check is
// written after the commit it reports and never for NotifyTx, that preferences,
// quiet hours and a schedule decide what is enqueued and when, and that Cancel
// and Status answer from the queue.
//
// The cases that join a caller's transaction (NotifyTx) hold for the in-process
// Notifier; a remote one returns domain.ErrTxUnsupported, and the transport suite
// that reuses this one says which cases it leaves out.
func NotifierContract(t *testing.T, newEnv func(t *testing.T) *Env) {
	t.Helper()

	t.Run("a replay is one row and one enqueue per channel (NR-002)", func(t *testing.T) {
		t.Parallel()
		f := newNotifierFixture(t, newEnv)
		first := f.notify(f.note("k1"))
		if !first.Applied || first.NotificationID == "" || !first.InboxVisible {
			t.Fatalf("first Notify = %+v, want applied, with an id, inbox-visible", first)
		}
		if got := channelsOf(first); !slices.Equal(got, []string{"email", "in_app"}) {
			t.Fatalf("first receipt plans %v, want email and in_app", got)
		}

		second := f.notify(f.note("k1"))
		if second.Applied {
			t.Fatal("a replay reported Applied = true")
		}
		if second.NotificationID != first.NotificationID || second.IdemKey != "k1" {
			t.Fatalf("replay = %+v, want the first notification %s named with its key", second, first.NotificationID)
		}
		for table, want := range map[string]int{"notifications": 1, "notify_idem": 1, "notification_outbox": 2} {
			if got := f.rows(table, f.id); got != want {
				t.Errorf("%d rows in %s after a replay, want %d", got, table, want)
			}
		}
	})

	t.Run("the same user in two tenants gets both (D18)", func(t *testing.T) {
		t.Parallel()
		f := newNotifierFixture(t, newEnv)
		other := domain.Tenant{Kind: "workspace", ID: "w2"}
		a := f.notify(f.note("weekly_digest:W39"))
		nb := f.note("weekly_digest:W39")
		nb.Tenant = other
		b := f.notify(nb)
		if !a.Applied || !b.Applied {
			t.Fatalf("applied = %v, %v, want both: the tenant is part of the key", a.Applied, b.Applied)
		}
		if a.NotificationID == b.NotificationID {
			t.Fatal("two tenants share one notification")
		}
		otherID := domain.Identity{Realm: Realm, Tenant: other, Recipient: contractUser}
		for _, id := range []domain.Identity{f.id, otherID} {
			if n, want := f.rows("notifications", id), 1; n != want {
				t.Errorf("%d notifications for %v, want %d", n, id.Tenant, want)
			}
			if n, want := f.rows("notification_outbox", id), 2; n != want {
				t.Errorf("%d queue rows for %v, want %d", n, id.Tenant, want)
			}
		}
	})

	t.Run("a failed transaction leaves no pre-check entry (D18)", func(t *testing.T) {
		t.Parallel()
		f := newNotifierFixture(t, newEnv)
		key := domain.PreCheckKey(f.id, "k1")

		f.e.Store.FailNextCommit()
		if _, err := f.e.Notifier.Notify(t.Context(), f.note("k1")); !errors.Is(err, ErrInjectedCommit) {
			t.Fatalf("Notify with the commit failed = %v, want ErrInjectedCommit", err)
		}
		if id, ok, _ := f.e.PreCheck.Seen(t.Context(), key); ok {
			t.Fatalf("the pre-check remembers %s after the commit failed: the retry would be swallowed", id)
		}
		f.nothingPersisted(f.id, "the commit failed")

		r := f.notify(f.note("k1"))
		if !r.Applied {
			t.Fatal("the retry was swallowed by a pre-check written before the commit")
		}
		if id, ok, _ := f.e.PreCheck.Seen(t.Context(), key); !ok || id != r.NotificationID {
			t.Fatalf("after the commit the pre-check holds %q, %v, want %s: it is written once the commit has happened", id, ok, r.NotificationID)
		}
	})

	t.Run("NotifyTx rolled back by the host leaves nothing (D28)", func(t *testing.T) {
		t.Parallel()
		f := newNotifierFixture(t, newEnv)
		tx, err := f.e.DB.Owner.Begin(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		r, err := f.e.Notifier.NotifyTx(t.Context(), tx, f.note("k1"))
		if err != nil || !r.Applied {
			_ = tx.Rollback(t.Context())
			t.Fatalf("NotifyTx = %+v, %v, want applied inside the caller's transaction", r, err)
		}
		if err := tx.Rollback(t.Context()); err != nil {
			t.Fatal(err)
		}
		f.nothingPersisted(f.id, "the host rolled back")
		if claims, err := f.e.Queue.Claim(t.Context(), f.shard(), 10, time.Minute); err != nil || len(claims) != 0 {
			t.Fatalf("Claim = %d rows, %v, want nothing: a rolled-back NotifyTx left work", len(claims), err)
		}
		if id, ok, _ := f.e.PreCheck.Seen(t.Context(), domain.PreCheckKey(f.id, "k1")); ok {
			t.Fatalf("the pre-check remembers %s for a transaction the host rolled back", id)
		}
		if again := f.notify(f.note("k1")); !again.Applied {
			t.Fatal("the key was spent by a transaction that rolled back")
		}
	})

	t.Run("NotifyTx committed by the host is delivered (D28)", func(t *testing.T) {
		t.Parallel()
		f := newNotifierFixture(t, newEnv)
		tx, err := f.e.DB.Owner.Begin(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		r, err := f.e.Notifier.NotifyTx(t.Context(), tx, f.note("k1"))
		if err != nil || !r.Applied {
			_ = tx.Rollback(t.Context())
			t.Fatalf("NotifyTx = %+v, %v, want applied", r, err)
		}
		// Not committed yet: nothing is visible to another session, and a replay of
		// the key inside the same transaction is a replay.
		if n := f.rows("notifications", f.id); n != 0 {
			_ = tx.Rollback(t.Context())
			t.Fatalf("%d notifications visible before the host committed", n)
		}
		if err := tx.Commit(t.Context()); err != nil {
			t.Fatal(err)
		}

		claims, err := f.e.Queue.Claim(t.Context(), f.shard(), 10, time.Minute)
		if err != nil {
			t.Fatalf("Claim: %v", err)
		}
		var claimed []string
		for _, c := range claims {
			if c.Entry.NotificationID != r.NotificationID {
				t.Errorf("claimed a delivery of %s, want %s", c.Entry.NotificationID, r.NotificationID)
			}
			claimed = append(claimed, string(c.Entry.Channel))
		}
		slices.Sort(claimed)
		if !slices.Equal(claimed, []string{"email", "in_app"}) {
			t.Fatalf("claimed channels %v, want email and in_app: committed work must be claimable", claimed)
		}
		if id, ok, _ := f.e.PreCheck.Seen(t.Context(), domain.PreCheckKey(f.id, "k1")); ok {
			t.Fatalf("the pre-check remembers %s: NotifyTx must never write it, the host may yet roll back", id)
		}
	})

	t.Run("NotifyTx refuses to run without the host's transaction", func(t *testing.T) {
		t.Parallel()
		f := newNotifierFixture(t, newEnv)
		// A control first, so a Notifier that accepts nothing cannot pass.
		f.notify(f.note("control"))
		if r, err := f.e.Notifier.NotifyTx(t.Context(), nil, f.note("k1")); err == nil {
			t.Fatalf("NotifyTx with no transaction = %+v, want an error: a nil tx would make the engine open and commit its own (D28)", r)
		}
		if got := f.rows("notifications", f.id); got != 1 {
			t.Errorf("%d notifications, want only the control's: the refused call wrote", got)
		}
	})

	t.Run("a disabled channel is not enqueued; the row still exists (NR-011, NR-012)", func(t *testing.T) {
		t.Parallel()
		f := newNotifierFixture(t, newEnv)
		if err := f.e.Prefs.Set(t.Context(), f.id, []domain.Preference{{Topic: contractTopic, Channel: "email", Enabled: false}}); err != nil {
			t.Fatal(err)
		}
		r := f.notify(f.note("k1"))
		if got := channelsOf(r); !slices.Equal(got, []string{"in_app"}) {
			t.Fatalf("receipt plans %v, want in_app only", got)
		}
		if got := f.queuedChannels(r.NotificationID); !slices.Equal(got, []string{"in_app"}) {
			t.Fatalf("queued %v, want in_app only: the disabled channel was enqueued", got)
		}
		if n := f.rows("notifications", f.id); n != 1 {
			t.Fatalf("%d notifications, want the one row", n)
		}
	})

	t.Run("no inbox-backed channel enabled: the row exists, not inbox-visible (D26, NR-012)", func(t *testing.T) {
		t.Parallel()
		f := newNotifierFixture(t, newEnv)
		if err := f.e.Prefs.Set(t.Context(), f.id, []domain.Preference{{Topic: contractTopic, Channel: "in_app", Enabled: false}}); err != nil {
			t.Fatal(err)
		}
		r := f.notify(f.note("k1"))
		if r.InboxVisible {
			t.Fatal("the receipt says inbox-visible with the only inbox-backed channel disabled")
		}
		if visible, _ := f.inboxVisible(r.NotificationID); visible {
			t.Fatal("inbox_visible is true with the only inbox-backed channel disabled: the badge would count it")
		}
		if got := f.queuedChannels(r.NotificationID); !slices.Equal(got, []string{"email"}) {
			t.Fatalf("queued %v, want email only", got)
		}
	})

	t.Run("every channel disabled stops all delivery and still persists (NR-001, NR-012)", func(t *testing.T) {
		t.Parallel()
		f := newNotifierFixture(t, newEnv)
		if err := f.e.Prefs.Set(t.Context(), f.id, []domain.Preference{
			{Topic: contractTopic, Channel: "in_app", Enabled: false},
			{Topic: contractTopic, Channel: "email", Enabled: false},
		}); err != nil {
			t.Fatal(err)
		}
		r := f.notify(f.note("k1"))
		if !r.Applied || len(r.Deliveries) != 0 {
			t.Fatalf("receipt = %+v, want applied with no deliveries", r)
		}
		for table, want := range map[string]int{"notifications": 1, "notify_idem": 1, "notification_outbox": 0} {
			if got := f.rows(table, f.id); got != want {
				t.Errorf("%d rows in %s, want %d", got, table, want)
			}
		}
		if again := f.notify(f.note("k1")); again.Applied {
			t.Fatal("the dedup record is missing: a replay of a notification with every channel off was applied")
		}
	})

	t.Run("a locked tenant preference overrides the recipient (NR-011, D30)", func(t *testing.T) {
		t.Parallel()
		cases := []struct {
			name      string
			tenantOn  bool
			recipient bool
			want      []string
		}{
			{"locked off beats a recipient who wants email", false, true, []string{"in_app"}},
			{"locked on beats a recipient who turned email off", true, false, []string{"email", "in_app"}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				f := newNotifierFixture(t, newEnv)
				if err := f.e.Prefs.SetTenant(t.Context(), Realm, domain.TenantPreference{
					Tenant: contractTenant, Topic: contractTopic, Channel: "email", Enabled: tc.tenantOn, Locked: true}); err != nil {
					t.Fatal(err)
				}
				if err := f.e.Prefs.Set(t.Context(), f.id, []domain.Preference{{Topic: contractTopic, Channel: "email", Enabled: tc.recipient}}); err != nil {
					t.Fatal(err)
				}
				r := f.notify(f.note("k1"))
				if got := f.queuedChannels(r.NotificationID); !slices.Equal(got, tc.want) {
					t.Fatalf("queued %v, want %v", got, tc.want)
				}
			})
		}
	})

	t.Run("critical inside quiet hours is immediate and marked overridden (D9, NR-013)", func(t *testing.T) {
		t.Parallel()
		f := newNotifierFixture(t, newEnv)
		// 11:00 to 13:00 UTC, and the Env's clock reads 12:00.
		if err := f.e.Prefs.SetSchedule(t.Context(), f.id, domain.DeliverySchedule{QuietStart: "11:00", QuietEnd: "13:00", Timezone: "UTC"}); err != nil {
			t.Fatal(err)
		}

		crit := f.note("critical-1")
		crit.Priority = domain.PriorityCritical
		cr := f.notify(crit)
		for _, ch := range []domain.ChannelKind{"in_app", "email"} {
			q := f.queueRow(cr.NotificationID, ch)
			if !q.DueNow || !q.Quiet {
				t.Errorf("critical on %s: due now = %v, quiet_hours_override = %v, want both true", ch, q.DueNow, q.Quiet)
			}
		}

		info := f.note("info-1")
		ir := f.notify(info)
		for _, ch := range []domain.ChannelKind{"in_app", "email"} {
			q := f.queueRow(ir.NotificationID, ch)
			if q.DueNow || q.Quiet || !q.NextAttemptAt.Equal(Epoch.Add(time.Hour)) {
				t.Errorf("info on %s: due at %v (due now %v), override %v, want deferred to 13:00 with no override", ch, q.NextAttemptAt, q.DueNow, q.Quiet)
			}
		}
	})

	t.Run("the realm cannot be set by a producer (NR-009)", func(t *testing.T) {
		t.Parallel()
		f := newNotifierFixture(t, newEnv)
		n := f.note("k1")
		// There is no realm field on a Notification. Every free-form place a producer
		// controls says another one, and none of them may be read as a routing input.
		n.Attributes = map[string]string{"realm": "rival", "x-realm": "rival"}
		n.Payload = map[string]string{"realm": "rival"}
		n.Data = map[string]any{"realm": "rival"}
		r := f.notify(n)

		for _, table := range []string{"notifications", "notify_idem", "notification_outbox"} {
			var elsewhere int
			if err := f.e.DB.Admin.QueryRow(t.Context(), `SELECT count(*) FROM `+table+` WHERE realm <> $1`, string(Realm)).Scan(&elsewhere); err != nil {
				t.Fatal(err)
			}
			if elsewhere != 0 {
				t.Errorf("%d rows of %s are in a realm the producer named, not the engine's %q", elsewhere, table, Realm)
			}
			if f.rows(table, f.id) == 0 {
				t.Errorf("no row of %s in the engine's realm %q", table, Realm)
			}
		}
		if !r.Applied {
			t.Fatal("not applied")
		}
	})

	t.Run("DeliverAfter defers the queue row; DeliverBefore is carried to it (NR-029)", func(t *testing.T) {
		t.Parallel()
		f := newNotifierFixture(t, newEnv)
		n := f.note("k1")
		n.DeliverAfter = Epoch.Add(2 * time.Hour)
		n.DeliverBefore = Epoch.Add(3 * time.Hour)
		r := f.notify(n)
		for _, ch := range []domain.ChannelKind{"in_app", "email"} {
			q := f.queueRow(r.NotificationID, ch)
			if !q.NextAttemptAt.Equal(n.DeliverAfter) || q.DueNow {
				t.Errorf("%s is due at %v (due now %v), want the scheduled %v", ch, q.NextAttemptAt, q.DueNow, n.DeliverAfter)
			}
			if q.DeliverBefore == nil || !q.DeliverBefore.Equal(n.DeliverBefore) {
				t.Errorf("%s carries expiry %v, want %v", ch, q.DeliverBefore, n.DeliverBefore)
			}
		}
		if _, from := f.inboxVisible(r.NotificationID); !from.Equal(n.DeliverAfter) {
			t.Errorf("visible from %v, want the send time %v: a scheduled notification is not in the inbox early (D29)", from, n.DeliverAfter)
		}
	})

	t.Run("Cancel stops pending deliveries and reports an in-flight one (NR-030, D29)", func(t *testing.T) {
		t.Parallel()
		f := newNotifierFixture(t, newEnv)
		r := f.notify(f.note("k1"))

		claims, err := f.e.Queue.Claim(t.Context(), f.shard(), 1, time.Minute)
		if err != nil || len(claims) != 1 {
			t.Fatalf("Claim = %d rows, %v, want the one in flight", len(claims), err)
		}
		inFlight := string(claims[0].Entry.Channel)

		got, err := f.e.Notifier.Cancel(t.Context(), domain.CancelRequest{Tenant: f.id.Tenant, Recipient: f.id.Recipient, IdemKey: "k1"})
		if err != nil {
			t.Fatalf("Cancel: %v", err)
		}
		if want := (domain.CancelReceipt{Matched: true, Canceled: 1, InFlight: 1}); got != want {
			t.Fatalf("Cancel = %+v, want %+v", got, want)
		}

		rows, err := f.e.DB.Admin.Query(t.Context(),
			`SELECT channel, outcome FROM notification_deliveries WHERE notification_id = $1::uuid`, r.NotificationID)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		recorded := map[string]string{}
		for rows.Next() {
			var ch, outcome string
			if err := rows.Scan(&ch, &outcome); err != nil {
				t.Fatal(err)
			}
			recorded[ch] = outcome
		}
		if _, falsely := recorded[inFlight]; falsely {
			t.Errorf("the in-flight %s delivery was recorded as %q: it may already be at the provider", inFlight, recorded[inFlight])
		}
		if len(recorded) != 1 {
			t.Errorf("history %v, want the one pending delivery recorded canceled", recorded)
		}
		for ch, outcome := range recorded {
			if outcome != string(domain.OutcomeCanceled) {
				t.Errorf("%s recorded as %q, want canceled", ch, outcome)
			}
		}
		if rest := f.queuedChannels(r.NotificationID); !slices.Equal(rest, []string{inFlight}) {
			t.Errorf("queue holds %v, want only the in-flight %s", rest, inFlight)
		}

		// Another tenant's Cancel of the same recipient id and key reaches nothing.
		other, err := f.e.Notifier.Cancel(t.Context(), domain.CancelRequest{
			Tenant: domain.Tenant{Kind: "workspace", ID: "w2"}, Recipient: f.id.Recipient, IdemKey: "k1"})
		if err != nil || other.Matched {
			t.Fatalf("Cancel from another tenant = %+v, %v, want no match", other, err)
		}
	})

	t.Run("Cancel of a key nobody sent matches nothing", func(t *testing.T) {
		t.Parallel()
		f := newNotifierFixture(t, newEnv)
		got, err := f.e.Notifier.Cancel(t.Context(), domain.CancelRequest{Tenant: f.id.Tenant, Recipient: f.id.Recipient, IdemKey: "never-sent"})
		if err != nil || got != (domain.CancelReceipt{}) {
			t.Fatalf("Cancel = %+v, %v, want an empty receipt and no error", got, err)
		}
	})

	t.Run("Status answers per channel and address (NR-032)", func(t *testing.T) {
		t.Parallel()
		f := newNotifierFixture(t, newEnv)
		req := domain.StatusRequest{Tenant: f.id.Tenant, Recipient: f.id.Recipient, IdemKey: "k1"}

		if st, err := f.e.Notifier.Status(t.Context(), req); err != nil || st.Found {
			t.Fatalf("Status before anything was sent = %+v, %v, want not found", st, err)
		}

		r := f.notify(f.note("k1"))
		st, err := f.e.Notifier.Status(t.Context(), req)
		if err != nil || !st.Found || st.NotificationID != r.NotificationID || st.Canceled {
			t.Fatalf("Status = %+v, %v, want found, for %s, not canceled", st, err, r.NotificationID)
		}
		if len(st.Deliveries) != 2 || st.Deliveries[0].Channel != "email" || st.Deliveries[1].Channel != "in_app" {
			t.Fatalf("deliveries %+v, want email then in_app", st.Deliveries)
		}
		for _, d := range st.Deliveries {
			if d.State != "pending" {
				t.Errorf("%s is %q, want pending before any claim finishes", d.Channel, d.State)
			}
		}

		claims, err := f.e.Queue.Claim(t.Context(), f.shard(), 10, time.Minute)
		if err != nil || len(claims) != 2 {
			t.Fatalf("Claim = %d, %v", len(claims), err)
		}
		fin, _ := domain.NewFinish(domain.OutcomeDelivered, "pm-1", "")
		var finished domain.ChannelKind
		for _, c := range claims {
			if c.Entry.Channel == "email" {
				finished = c.Entry.Channel
				if ok, err := f.e.Queue.Finish(t.Context(), c, fin); err != nil || !ok {
					t.Fatalf("Finish = %v, %v", ok, err)
				}
			}
		}
		st, err = f.e.Notifier.Status(t.Context(), req)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range st.Deliveries {
			switch d.Channel {
			case finished:
				if d.State != string(domain.OutcomeDelivered) || d.ProviderMessageID != "pm-1" || d.CompletedAt.IsZero() {
					t.Errorf("email = %+v, want delivered with its provider message id", d)
				}
			default:
				if d.State != "pending" {
					t.Errorf("%s = %q, want still pending", d.Channel, d.State)
				}
			}
		}

		// Status is the asking tenant's own.
		foreign := domain.StatusRequest{Tenant: domain.Tenant{Kind: "workspace", ID: "w2"}, Recipient: f.id.Recipient, IdemKey: "k1"}
		if st, err := f.e.Notifier.Status(t.Context(), foreign); err != nil || st.Found {
			t.Fatalf("Status for another tenant = %+v, %v, want not found", st, err)
		}
	})

	t.Run("a notification that cannot be accepted writes nothing", func(t *testing.T) {
		t.Parallel()
		cases := []struct {
			name   string
			mutate func(*domain.Notification)
			is     error
		}{
			{"no idempotency key", func(n *domain.Notification) { n.IdemKey = "" }, domain.ErrMissingIdem},
			{"an unregistered topic", func(n *domain.Notification) { n.Topic = "never_registered" }, domain.ErrUnknownTopic},
			{"no tenant", func(n *domain.Notification) { n.Tenant = domain.Tenant{} }, domain.ErrEmptyIdentity},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				f := newNotifierFixture(t, newEnv)
				// A control first, so a Notifier that accepts nothing cannot pass.
				f.notify(f.note("control"))
				n := f.note("k1")
				tc.mutate(&n)
				if _, err := f.e.Notifier.Notify(t.Context(), n); !errors.Is(err, tc.is) {
					t.Fatalf("Notify = %v, want %v", err, tc.is)
				}
				if got := f.rows("notifications", f.id); got != 1 {
					t.Errorf("%d notifications, want only the control's", got)
				}
			})
		}
	})
}
