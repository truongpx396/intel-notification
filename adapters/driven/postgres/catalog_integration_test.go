//go:build integration

package postgres_test

import (
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/truongpx396/intel-notification/adapters/driven/postgres"
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
	if _, err := templates.ActivateTemplate(t.Context(), "aisat", ver(1)); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := range 150 {
			if _, err := templates.ActivateTemplate(t.Context(), "aisat", ver(i%2+1)); err != nil {
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

// Two producers activating different versions of one template at once are taken in turn: both calls
// succeed, one after the other, and the template ends with exactly one active version. Without the
// lock on the template's versions the unique index still keeps two from being active, but it does so by
// failing one of the calls with a constraint error the producer cannot act on.
func TestConcurrentActivationsAreTakenInTurn(t *testing.T) {
	t.Parallel()
	db := pgtest.New(t)
	templates := postgres.NewTemplates(db.Owner)
	for range 2 {
		if _, err := templates.PutTemplate(t.Context(), "aisat", domain.Template{Ref: "r", Channel: "email", Locale: "en", Body: "v"}); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	for version := 1; version <= 2; version++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 120 {
				_, err := templates.ActivateTemplate(t.Context(), "aisat", domain.TemplateVersion{Ref: "r", Channel: "email", Locale: "en", Version: version})
				if err != nil {
					t.Errorf("ActivateTemplate(version %d): %v", version, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	var active int
	if err := db.Admin.QueryRow(t.Context(), `SELECT count(*) FROM notification_templates WHERE active`).Scan(&active); err != nil || active != 1 {
		t.Fatalf("%d versions are active, %v, want exactly 1", active, err)
	}
}

// D29, D31. The providers a deployment has configured, in the order a channel tries them, and never a
// credential: a row holds a reference to one.
func TestProvidersAreTheEnabledRowsInTheOrderTheyAreTried(t *testing.T) {
	t.Parallel()
	db := pgtest.New(t)
	insert := func(realm, tk, tid, channel, provider string, priority int, config, secretRef string, enabled bool) {
		t.Helper()
		if _, err := db.Admin.Exec(t.Context(), `
INSERT INTO channel_providers (realm, tenant_kind, tenant_id, channel, provider, priority, config, secret_ref, enabled)
VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9)`, realm, tk, tid, channel, provider, priority, config, secretRef, enabled); err != nil {
			t.Fatal(err)
		}
	}
	// smtp is tried first although "resend" sorts first: priority orders them, not the name.
	insert("alpha", "", "", "email", "smtp", 10, `{"addr":"relay.internal:587","from":"n@x.test"}`, "", true)
	insert("alpha", "", "", "email", "resend", 20, `{"from":"n@x.test"}`, "env:RESEND_KEY", true)
	insert("alpha", "workspace", "w1", "email", "resend", 0, `{}`, "file:/run/secrets/w1", true)
	insert("alpha", "", "", "email", "ses", 5, `{}`, "env:SES", false) // disabled: nobody uses it
	insert("beta", "", "", "webhook", "webhook", 0, `{}`, "env:HOOK", true)

	got, err := postgres.NewProviders(db.Owner).Providers(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	type row struct {
		realm, tenant, channel, provider, ref string
		priority                              int
	}
	var seen []row
	for _, p := range got {
		seen = append(seen, row{string(p.Realm), p.Tenant.ID, string(p.Channel), p.Provider, p.SecretRef, p.Priority})
	}
	want := []row{
		{"alpha", "", "email", "smtp", "", 10},
		{"alpha", "", "email", "resend", "env:RESEND_KEY", 20},
		{"alpha", "w1", "email", "resend", "file:/run/secrets/w1", 0},
		{"beta", "", "webhook", "webhook", "env:HOOK", 0},
	}
	if !slices.Equal(seen, want) {
		t.Fatalf("Providers = %+v, want %+v: enabled rows only, each scope's in priority order, a reference and never a credential", seen, want)
	}
	if got[0].Config["addr"] != "relay.internal:587" || got[1].Config["from"] != "n@x.test" {
		t.Fatalf("config = %v and %v, want each row's own settings", got[0].Config, got[1].Config)
	}
	if got[2].Tenant != (domain.Tenant{Kind: "workspace", ID: "w1"}) || got[0].Tenant != (domain.Tenant{}) {
		t.Fatalf("tenants = %+v and %+v, want the realm-wide row's empty and the tenant's own", got[0].Tenant, got[2].Tenant)
	}
}

// The schema is what keeps a credential out of the table: a bare value is refused, so it cannot be read
// back by anything.
func TestAProviderRowCannotHoldABareCredential(t *testing.T) {
	t.Parallel()
	db := pgtest.New(t)
	_, err := db.Owner.Exec(t.Context(), `INSERT INTO channel_providers (realm, channel, provider, secret_ref) VALUES ('alpha', 'email', 'resend', 're_live_AbCdEf123456')`)
	if err == nil {
		t.Fatal("a row whose secret_ref is a bare key was stored, want the schema to refuse it")
	}
}

// T052, NR-035. A message the bus ingest gave up on is parked as a dead letter, once however many times it is
// recorded, with what arrived, and is not a delivery to replay.
func TestPoisonIsParkedOnceAsABusDeadLetter(t *testing.T) {
	t.Parallel()
	db := pgtest.New(t)
	s := postgres.New(db.Owner, postgres.WithClock(fixedClock{time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)}))
	msg := domain.PoisonMessage{Realm: "alpha", Key: "NOTIFY:7", Subject: "notify.ingest.alpha.e5a69683f60c0c5f", Body: []byte(`{"topic":"t"}`), Attempts: 1, Reason: "idem_key is required"}
	for range 3 {
		if err := s.RecordPoison(t.Context(), msg); err != nil {
			t.Fatal(err)
		}
	}
	var n int
	var source, reason, lastErr, body string
	var tenant *string
	if err := db.Admin.QueryRow(t.Context(), `SELECT count(*) FROM dead_letters`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("%d dead letters after recording one message three times, %v, want 1", n, err)
	}
	if err := db.Admin.QueryRow(t.Context(), `SELECT source, reason, last_error, payload->>'body', tenant_id FROM dead_letters`).Scan(&source, &reason, &lastErr, &body, &tenant); err != nil {
		t.Fatal(err)
	}
	if source != "bus" || reason != "poison" || lastErr != "idem_key is required" || body != `{"topic":"t"}` || tenant != nil {
		t.Fatalf("row = %q %q %q %q tenant=%v, want a bus poison holding what arrived and no tenant", source, reason, lastErr, body, tenant)
	}
	// Another message is another dead letter, and a body that is not text is kept as base64.
	other := msg
	other.Key, other.Body = "NOTIFY:8", []byte{0xff, 0xfe, 0x00, 0x01}
	if err := s.RecordPoison(t.Context(), other); err != nil {
		t.Fatal(err)
	}
	var enc string
	if err := db.Admin.QueryRow(t.Context(), `SELECT payload->>'body_encoding' FROM dead_letters WHERE last_error = $1 AND payload->>'key' = 'NOTIFY:8'`, msg.Reason).Scan(&enc); err != nil || enc != "base64" {
		t.Fatalf("body encoding = %q, %v, want base64 for bytes that are not text", enc, err)
	}
	if err := s.RecordPoison(t.Context(), domain.PoisonMessage{Realm: "alpha", Subject: "s"}); err == nil {
		t.Fatal("RecordPoison with no key = nil, want an error: it could not be told from its own redelivery")
	}
	// A bus message holds no queue row, so replaying it is not a re-drive.
	var id string
	if err := db.Admin.QueryRow(t.Context(), `SELECT id::text FROM dead_letters WHERE payload->>'key' = 'NOTIFY:7'`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReplayDeadLetter(t.Context(), "alpha", id); !errors.Is(err, domain.ErrNotReplayable) {
		t.Fatalf("ReplayDeadLetter of a bus poison = %v, want ErrNotReplayable: the producer must resend it", err)
	}
	// An oversized body is cut.
	big := msg
	big.Key, big.Body = "NOTIFY:9", make([]byte, 200<<10)
	for i := range big.Body {
		big.Body[i] = 'x'
	}
	if err := s.RecordPoison(t.Context(), big); err != nil {
		t.Fatal(err)
	}
	var kept int
	var truncated bool
	if err := db.Admin.QueryRow(t.Context(), `SELECT length(payload->>'body'), (payload->>'truncated')::boolean FROM dead_letters WHERE payload->>'key' = 'NOTIFY:9'`).Scan(&kept, &truncated); err != nil || kept != 64<<10 || !truncated {
		t.Fatalf("kept %d bytes, truncated=%v, %v, want 64 KiB and flagged", kept, truncated, err)
	}
}

type fixedClock struct{ at time.Time }

func (c fixedClock) Now() time.Time { return c.at }
