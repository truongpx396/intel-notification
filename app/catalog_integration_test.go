//go:build integration

package app_test

import (
	"errors"
	"slices"
	"testing"

	"github.com/truongpx396/intel-notification/adapters/driven/postgres"
	"github.com/truongpx396/intel-notification/app"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/notifytest"
)

// T048, over the use-cases. The data-backed ports are the ones a service has instead of a host's code, so
// the proof that they work is the Notifier and the Dispatcher running on them.

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
	if _, err := templates.ActivateTemplate(t.Context(), id.Realm, domain.TemplateVersion{
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
	// Two addresses fan the claim out into one row each (D25), so the children are claimed by a
	// second pass: drain until a pass finds nothing.
	for pass := 0; ; pass++ {
		handled := 0
		for s := range e.Config.WithDefaults().Shards {
			n, err := dispatcher.Drain(t.Context(), domain.Shard(s), 10)
			if err != nil {
				t.Fatal(err)
			}
			handled += n
		}
		if handled == 0 {
			break
		}
		if pass > 5 {
			t.Fatal("the dispatcher is still finding work after six passes")
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
