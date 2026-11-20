//go:build integration

package postgres_test

import (
	"errors"
	"slices"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/truongpx396/intel-notification/adapters/driven/postgres"
	"github.com/truongpx396/intel-notification/app"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/notifytest"
	"github.com/truongpx396/intel-notification/internal/pgtest"
)

// T048. The data-backed ports of service mode, against a real PostgreSQL as the owner role: the
// contract each of them shares with its in-memory fake, and what only a database can hold.

func TestTopicsRunTheRegistryContract(t *testing.T) {
	t.Parallel()
	notifytest.TopicRegistryContract(t, func(t *testing.T) notifytest.TopicsWorld {
		t.Helper()
		topics := postgres.NewTopics(pgtest.New(t).Owner)
		return notifytest.TopicsWorld{Registry: topics, Writer: topics}
	})
}

func TestTemplatesRunTheRendererContract(t *testing.T) {
	t.Parallel()
	notifytest.TemplateRendererContract(t, func(t *testing.T) notifytest.TemplatesWorld {
		t.Helper()
		templates := postgres.NewTemplates(pgtest.New(t).Owner, postgres.WithDefaultLocale("en"))
		return notifytest.TemplatesWorld{Renderer: templates, Writer: templates, DefaultLocale: "en"}
	})
}

func TestAddressesRunTheBookContract(t *testing.T) {
	t.Parallel()
	notifytest.AddressBookContract(t, func(t *testing.T) notifytest.AddressesWorld {
		t.Helper()
		addresses := postgres.NewAddresses(pgtest.New(t).Owner)
		return notifytest.AddressesWorld{Book: addresses, Writer: addresses}
	})
}

// NR-017. A topic is a registration: the Notifier refuses it before the call and accepts it after, in
// the same process, with no migration and no restart.
func TestATopicRegisteredAtRuntimeIsUsableWithoutAMigration(t *testing.T) {
	t.Parallel()
	e := notifytest.NewEnv(t)
	e.AddChannel("in_app", domain.ChannelCapabilities{InboxBacked: true, Dedup: domain.DedupLocal})
	topics := postgres.NewTopics(e.DB.Owner)
	notifier, err := app.NewNotifier(e.Config, app.Deps{
		Channels: e.Channels, Topics: topics, Prefs: e.Prefs, Quotas: e.Quotas, QuotaPolicies: e.Quotas,
		Store: e.Store, Queue: e.Queue, Clock: e.Clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	note := domain.Notification{
		Tenant: domain.Tenant{Kind: "workspace", ID: "w1"}, Recipient: domain.Recipient{Kind: "user", ID: "u1"},
		Topic: "just_registered", IdemKey: "k1", Title: "Hello", OccurredAt: notifytest.Epoch,
	}
	if _, err := notifier.Notify(t.Context(), note); !errors.Is(err, domain.ErrUnknownTopic) {
		t.Fatalf("Notify before the registration = %v, want ErrUnknownTopic", err)
	}
	err = topics.UpsertTopic(t.Context(), notifytest.Realm, "just_registered", domain.TopicDef{
		DefaultChannels: []domain.ChannelKind{"in_app"}, DefaultPriority: domain.PriorityInfo})
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := notifier.Notify(t.Context(), note)
	if err != nil || !receipt.Applied || len(receipt.Deliveries) != 1 || receipt.Deliveries[0].Channel != "in_app" {
		t.Fatalf("Notify after the registration = %+v, %v, want it accepted and planned on in_app", receipt, err)
	}
}

// NR-017, NR-019, NR-028 together: the topic, the template and the address are all rows, and a
// notification is rendered from the one and sent to the other.
func TestADeliveryIsRenderedAndAddressedFromTheDataBackedPorts(t *testing.T) {
	t.Parallel()
	e := notifytest.NewEnv(t)
	email := notifytest.NewScripted("email", domain.ChannelCapabilities{NeedsSubject: true, NeedsAddress: true, Dedup: domain.DedupProvider})
	e.Channels.Register(email)

	owner := e.DB.Owner
	topics, templates, addresses := postgres.NewTopics(owner), postgres.NewTemplates(owner), postgres.NewAddresses(owner)
	id := domain.Identity{Realm: notifytest.Realm, Tenant: domain.Tenant{Kind: "workspace", ID: "w1"}, Recipient: domain.Recipient{Kind: "user", ID: "u1"}}

	if err := topics.UpsertTopic(t.Context(), id.Realm, "invoice_overdue", domain.TopicDef{
		DefaultChannels: []domain.ChannelKind{"email"}, DefaultPriority: domain.PriorityWarning, TemplateRef: "invoice"}); err != nil {
		t.Fatal(err)
	}
	stored, err := templates.PutTemplate(t.Context(), id.Realm, domain.Template{
		Ref: "invoice", Channel: "email", Locale: "en", Subject: "Invoice {{ .number }} is overdue", Body: "Hello {{ .name }}, please pay."})
	if err != nil {
		t.Fatal(err)
	}
	if err := templates.ActivateTemplate(t.Context(), id.Realm, domain.TemplateVersion{
		Ref: "invoice", Channel: "email", Locale: "en", Version: stored.Version}); err != nil {
		t.Fatal(err)
	}
	two := []domain.Address{{Value: "ada@example.test", Locale: "en"}, {Value: "ada@work.example.test", Locale: "en"}}
	if err := addresses.PutAddresses(t.Context(), id, "email", two, []string{
		domain.AddressKey("email", two[0].Value), domain.AddressKey("email", two[1].Value)}); err != nil {
		t.Fatal(err)
	}

	notifier, err := app.NewNotifier(e.Config, app.Deps{
		Channels: e.Channels, Topics: topics, Prefs: e.Prefs, Quotas: e.Quotas, QuotaPolicies: e.Quotas,
		Store: e.Store, Queue: e.Queue, Clock: e.Clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := notifier.Notify(t.Context(), domain.Notification{
		Tenant: id.Tenant, Recipient: id.Recipient, Topic: "invoice_overdue", IdemKey: "inv-1", OccurredAt: notifytest.Epoch,
		Data: map[string]any{"number": "INV-42", "name": "Ada"},
	}); err != nil {
		t.Fatal(err)
	}

	deps := e.DispatcherDeps()
	deps.Topics, deps.Renderer, deps.Addresses = topics, templates, addresses
	dispatcher, err := app.NewDispatcher(e.Config, deps, app.WithRand(func() float64 { return notifytest.Jitter }))
	if err != nil {
		t.Fatal(err)
	}
	for s := range e.Config.WithDefaults().Shards {
		if _, err := dispatcher.Drain(t.Context(), domain.Shard(s), 10); err != nil {
			t.Fatal(err)
		}
	}
	// Two addresses make two deliveries of one channel, each rendered from the template (NR-028).
	var sent []string
	for _, d := range email.Deliveries() {
		if d.Content.Subject != "Invoice INV-42 is overdue" || d.Content.Body != "Hello Ada, please pay." || d.Content.TemplateVersion != 1 {
			t.Errorf("delivery to %q carries %+v, want the active template rendered over the notification's data", d.Address.Value, d.Content)
		}
		sent = append(sent, d.Address.Value)
	}
	slices.Sort(sent)
	if !slices.Equal(sent, []string{"ada@example.test", "ada@work.example.test"}) {
		t.Fatalf("email went to %v, want both of the recipient's addresses", sent)
	}
}

// NS-001, D17. An address is personal data: the table is scoped, so the owner role sees a row only
// under that recipient's scope, whatever the adapter's own predicates say.
func TestAddressesAreStoredUnderTheRecipientsScope(t *testing.T) {
	t.Parallel()
	db := pgtest.New(t)
	addresses := postgres.NewAddresses(db.Owner)
	alice := domain.Identity{Realm: "aisat", Tenant: domain.Tenant{Kind: "workspace", ID: "w1"}, Recipient: domain.Recipient{Kind: "user", ID: "alice"}}
	bob := alice
	bob.Recipient.ID = "bob"
	a := []domain.Address{{Value: "alice@example.test"}}
	if err := addresses.PutAddresses(t.Context(), alice, "email", a, []string{domain.AddressKey("email", a[0].Value)}); err != nil {
		t.Fatal(err)
	}

	var forced bool
	if err := db.Owner.QueryRow(t.Context(), `SELECT relrowsecurity AND relforcerowsecurity FROM pg_class WHERE relname = 'recipient_addresses'`).Scan(&forced); err != nil || !forced {
		t.Fatalf("recipient_addresses has forced row-level security = %v, %v, want true", forced, err)
	}
	seen := func(scope *domain.Identity) int {
		t.Helper()
		var n int
		err := pgx.BeginFunc(t.Context(), db.Owner, func(tx pgx.Tx) error {
			if scope != nil {
				if _, err := tx.Exec(t.Context(), `SELECT set_config('notify.realm', $1, true), set_config('notify.tenant_kind', $2, true),
					set_config('notify.tenant_id', $3, true), set_config('notify.recipient_kind', $4, true), set_config('notify.recipient_id', $5, true)`,
					string(scope.Realm), scope.Tenant.Kind, scope.Tenant.ID, scope.Recipient.Kind, scope.Recipient.ID); err != nil {
					return err
				}
			}
			return tx.QueryRow(t.Context(), `SELECT count(*) FROM recipient_addresses`).Scan(&n)
		})
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	if got := seen(nil); got != 0 {
		t.Errorf("the owner with no scope sees %d address rows, want 0", got)
	}
	if got := seen(&bob); got != 0 {
		t.Errorf("under another recipient's scope the owner sees %d address rows, want 0", got)
	}
	if got := seen(&alice); got != 1 {
		t.Errorf("under the recipient's own scope the owner sees %d address rows, want 1", got)
	}
}

// D55. A template that reaches outside the restricted map is refused when it is saved: no row exists
// for it, so no delivery can meet it later.
func TestATemplateOutsideTheRestrictedMapLeavesNoRow(t *testing.T) {
	t.Parallel()
	db := pgtest.New(t)
	templates := postgres.NewTemplates(db.Owner)
	for _, body := range []string{`{{ env "HOME" }}`, `{{ readFile "/etc/passwd" }}`, `{{ call .x }}`} {
		_, err := templates.PutTemplate(t.Context(), "aisat", domain.Template{Ref: "r", Channel: "email", Locale: "en", Body: body})
		if !errors.Is(err, domain.ErrInvalidTemplate) {
			t.Fatalf("PutTemplate(%q) = %v, want ErrInvalidTemplate", body, err)
		}
	}
	var n int
	if err := db.Admin.QueryRow(t.Context(), `SELECT count(*) FROM notification_templates`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("notification_templates holds %d rows, %v, want none: a refused template is not stored", n, err)
	}
}

// Two producers saving one template at once each get a version of their own, and none is lost or
// numbered twice: the primary key is the arbiter, and the loser of a race takes the next number.
func TestConcurrentPutsNumberEveryVersionOnce(t *testing.T) {
	t.Parallel()
	templates := postgres.NewTemplates(pgtest.New(t).Owner)
	const writers = 8
	versions := make([]int, writers)
	var wg sync.WaitGroup
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := templates.PutTemplate(t.Context(), "aisat", domain.Template{Ref: "r", Channel: "email", Locale: "en", Body: "v"})
			if err != nil {
				t.Errorf("PutTemplate: %v", err)
				return
			}
			versions[i] = got.Version
		}()
	}
	wg.Wait()
	slices.Sort(versions)
	want := make([]int, writers)
	for i := range want {
		want[i] = i + 1
	}
	if !slices.Equal(versions, want) {
		t.Fatalf("versions = %v, want %v: each save is a version of its own", versions, want)
	}
}

// D27. Activating a version is one step: a render that runs while an edit goes live finds the old
// version or the new one, never neither, so a delivery is never rejected for an edit in progress.
func TestActivationNeverLeavesAWindowWithNoActiveVersion(t *testing.T) {
	t.Parallel()
	templates := postgres.NewTemplates(pgtest.New(t).Owner)
	for range 2 {
		if _, err := templates.PutTemplate(t.Context(), "aisat", domain.Template{Ref: "r", Channel: "email", Locale: "en", Body: "v"}); err != nil {
			t.Fatal(err)
		}
	}
	ver := func(v int) domain.TemplateVersion {
		return domain.TemplateVersion{Ref: "r", Channel: "email", Locale: "en", Version: v}
	}
	if err := templates.ActivateTemplate(t.Context(), "aisat", ver(1)); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := range 150 {
			if err := templates.ActivateTemplate(t.Context(), "aisat", ver(i%2+1)); err != nil {
				t.Errorf("ActivateTemplate: %v", err)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for range 600 {
			_, err := templates.Render(t.Context(), domain.RenderRequest{Realm: "aisat", TemplateRef: "r", Channel: "email", Locale: "en"})
			if err != nil {
				t.Errorf("Render while a version was being activated = %v, want the old version or the new", err)
				return
			}
		}
	}()
	wg.Wait()
}
