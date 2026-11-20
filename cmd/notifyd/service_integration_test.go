//go:build integration

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/truongpx396/intel-notification/adapters/driving/grpcclient"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/notifytest"
	"github.com/truongpx396/intel-notification/internal/pgtest"
	"github.com/truongpx396/intel-notification/internal/redistest"
)

// T049a, T051, NS-011. The service as a process: Build against a real PostgreSQL as its table owner and a
// real Redis, real listeners, and a producer that knows nothing of this repository but the service's API,
// authenticated by a token its host signed, doing what NS-011 says: register a topic and a template, store
// a recipient's addresses, send one notification, and see it delivered on two channels.

func TestMain(m *testing.M) { os.Exit(notifytest.Main(m, notifytest.WithRedis())) }

// mailbox is the email provider: a fake Resend that keeps what it was asked to send.
type mailbox struct {
	srv  *httptest.Server
	mu   sync.Mutex
	sent []map[string]any
	hdr  []http.Header
}

func newMailbox(t *testing.T) *mailbox {
	t.Helper()
	m := &mailbox{}
	m.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var msg map[string]any
		_ = json.Unmarshal(body, &msg)
		m.mu.Lock()
		m.sent, m.hdr = append(m.sent, msg), append(m.hdr, r.Header.Clone())
		n := len(m.sent)
		m.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]string{"id": fmt.Sprintf("re_%d", n)})
	}))
	t.Cleanup(m.srv.Close)
	return m
}

func (m *mailbox) messages() []map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]map[string]any(nil), m.sent...)
}

// dsnOf is a connection string for the role and database a pool actually uses. Config().ConnString() is
// the string the pool was parsed from, which for a pool the kit copied from the superuser's is not it.
func dsnOf(pool *pgxpool.Pool) string {
	cc := pool.Config().ConnConfig
	u := url.URL{Scheme: "postgres", User: url.UserPassword(cc.User, cc.Password), Host: net.JoinHostPort(cc.Host, strconv.Itoa(int(cc.Port))), Path: cc.Database, RawQuery: "sslmode=disable"}
	return u.String()
}

type bearer string

func (b bearer) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + string(b)}, nil
}
func (bearer) RequireTransportSecurity() bool { return false }

// world is a running service and the things around it: the host that signs tokens, the mailbox, and the
// producer's connection.
type world struct {
	t       *testing.T
	svc     *Service
	done    chan error
	stop    context.CancelFunc
	db      *pgtest.DB
	mail    *mailbox
	signer  *notifytest.TokenSigner
	conn    *grpc.ClientConn
	catalog *grpcclient.Catalog
	sender  *grpcclient.Client
}

const emailKey = "re_live_the-providers-api-key"

// startService starts a service for one test. withEmail seeds the provider row that makes the email channel
// exist; without it the service has the in-app channel alone.
func startService(t *testing.T, withEmail bool) *world {
	t.Helper()
	w := &world{t: t, db: pgtest.New(t), mail: newMailbox(t), signer: notifytest.NewTokenSigner("host-key-1")}
	dir := t.TempDir()
	write := func(name, content string) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return "file:" + path
	}
	jwks := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		_, _ = rw.Write(notifytest.JWKS(w.signer))
	}))
	t.Cleanup(jwks.Close)

	if withEmail {
		if _, err := w.db.Admin.Exec(t.Context(), `
INSERT INTO channel_providers (realm, channel, provider, priority, config, secret_ref)
VALUES ('local', 'email', 'resend', 0, $1::jsonb, $2)`,
			fmt.Sprintf(`{"from":"Notify <notify@example.test>","base_url":%q}`, w.mail.srv.URL), write("resend-key", emailKey)); err != nil {
			t.Fatal(err)
		}
	}
	redisOpt := redistest.New(t).Client.Options()
	cfg, err := LoadConfig(func(k string) string {
		return map[string]string{
			"NOTIFY_PG_DSN":              dsnOf(w.db.Owner),
			"NOTIFY_REDIS_URL":           fmt.Sprintf("redis://%s/%d", redisOpt.Addr, redisOpt.DB),
			"NOTIFY_REALM_BINDINGS":      "local-producer=local",
			"NOTIFY_RECIPIENT_JWKS":      "local=" + jwks.URL,
			"NOTIFY_ALLOW_INSECURE_HTTP": "true",
			"NOTIFY_HTTP_ADDR":           "127.0.0.1:0",
			"NOTIFY_GRPC_ADDR":           "127.0.0.1:0",
			"NOTIFY_PUBLIC_URL":          "http://notify.example.test",
			"NOTIFY_UNSUBSCRIBE_KEYS":    "k1=" + write("unsub-key", strings.Repeat("u", 40)),
			"NOTIFY_OPERATOR_TOKENS":     write("operator", "operator-secret"),
			"NOTIFY_POLL_INTERVAL":       "50ms",
		}[k]
	})
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	w.svc, err = Build(t.Context(), cfg, log)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	ctx, stop := context.WithCancel(context.Background())
	w.stop, w.done = stop, make(chan error, 1)
	go func() { w.done <- w.svc.Run(ctx) }()
	t.Cleanup(func() {
		stop()
		select {
		case err := <-w.done:
			if err != nil {
				t.Errorf("the service stopped with %v, want a clean stop", err)
			}
		case <-time.After(30 * time.Second):
			t.Error("the service did not stop")
		}
		w.svc.Close()
	})

	token := w.signer.Producer("local", "local-producer", time.Now(), 30*time.Minute)
	w.conn, err = grpc.NewClient(w.svc.GRPCAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithPerRPCCredentials(bearer(token)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.conn.Close() })
	w.catalog, w.sender = grpcclient.NewCatalog(w.conn), grpcclient.New(w.conn)
	return w
}

var (
	wsTenant = domain.Tenant{Kind: "workspace", ID: "w1"}
	ada      = domain.Recipient{Kind: "user", ID: "ada"}
)

// final is an error that ends a poll at once: what it reports will not change by waiting.
type final struct{ error }

// eventually polls until check holds or the deadline passes: delivery is asynchronous.
func eventually(deadline time.Duration, check func() error) error {
	end := time.Now().Add(deadline)
	for {
		err := check()
		var f final
		if err == nil || errors.As(err, &f) || time.Now().After(end) {
			return err
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// scenario is NS-011, one step after another, through the service's API only. It returns the name of the
// step that failed and why, or "" when the notification reached both channels.
func (w *world) scenario(skipTemplates bool) (failedAt string, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	step := func(name string, f func() error) bool {
		if err = f(); err != nil {
			failedAt = name
			return false
		}
		return true
	}
	topicDef := domain.TopicDef{DefaultChannels: []domain.ChannelKind{"in_app", "email"}, DefaultPriority: domain.PriorityInfo}
	putTemplate := func(ch domain.ChannelKind, subject, body string) error {
		stored, err := w.catalog.PutTemplate(ctx, domain.Template{Ref: "invoice_overdue", Channel: ch, Locale: "en", Subject: subject, Body: body})
		if err != nil {
			return err
		}
		_, err = w.catalog.ActivateTemplate(ctx, domain.TemplateVersion{Ref: "invoice_overdue", Channel: ch, Locale: "en", Version: stored.Version})
		return err
	}
	var receipt domain.Receipt
	ok := step("register the topic", func() error {
		_, err := w.catalog.UpsertTopic(ctx, "invoice_overdue", topicDef)
		return err
	}) && (skipTemplates || step("register the email template", func() error {
		return putTemplate("email", "Invoice {{ .number }} is overdue", "Hello {{ .name }}, invoice {{ .number }} is overdue.")
	})) && (skipTemplates || step("register the in-app template", func() error {
		return putTemplate("in_app", "Invoice {{ .number }} is overdue", "{{ .name }}, please pay.")
	})) && step("store the recipient's addresses", func() error {
		got, err := w.catalog.PutAddresses(ctx, wsTenant, ada, "email", []domain.Address{{Value: "ada@example.test", Locale: "en"}})
		if err == nil && len(got) != 1 {
			err = fmt.Errorf("the service stored %d addresses, want 1", len(got))
		}
		return err
	}) && step("send one notification", func() error {
		var err error
		receipt, err = w.sender.Notify(ctx, domain.Notification{
			Tenant: wsTenant, Recipient: ada, Topic: "invoice_overdue", IdemKey: "invoice:42:overdue",
			Data: map[string]any{"number": "INV-42", "name": "Ada"},
		})
		if err == nil && !receipt.Applied {
			err = errors.New("the notification was not applied")
		}
		return err
	}) && step("see it delivered on two channels", func() error {
		return eventually(20*time.Second, func() error {
			st, err := w.sender.Status(ctx, domain.StatusRequest{Tenant: wsTenant, Recipient: ada, IdemKey: "invoice:42:overdue"})
			if err != nil {
				return err
			}
			states := map[domain.ChannelKind]string{}
			for _, d := range st.Deliveries {
				states[d.Channel] = d.State
			}
			if states["in_app"] != "delivered" || states["email"] != "delivered" {
				err := fmt.Errorf("deliveries are %v, want in_app and email delivered", states)
				for _, state := range states {
					// A delivery that ended in anything but delivered has ended: waiting will not change it.
					if state != "delivered" && state != "pending" && state != "" {
						return final{err}
					}
				}
				return err
			}
			return nil
		})
	})
	_ = ok
	return failedAt, err
}

func TestNS011AProducerThatKnowsOnlyTheAPIGetsOneNotificationDeliveredOnTwoChannels(t *testing.T) {
	w := startService(t, true)
	if failedAt, err := w.scenario(false); err != nil {
		t.Fatalf("the scenario failed at %q: %v", failedAt, err)
	}

	// What was delivered, read from where it landed and not from what the service says of itself.
	msgs := w.mail.messages()
	if len(msgs) != 1 {
		t.Fatalf("the provider was asked to send %d messages, want 1", len(msgs))
	}
	if msgs[0]["subject"] != "Invoice INV-42 is overdue" || fmt.Sprint(msgs[0]["to"]) != "[ada@example.test]" {
		t.Fatalf("the message = %v, want the template rendered from the notification's data, to the address stored through the API", msgs[0])
	}
	if html, _ := msgs[0]["html"].(string); !strings.Contains(html, "Hello Ada, invoice INV-42 is overdue.") || !strings.Contains(html, "Unsubscribe") {
		t.Fatalf("the body = %q, want the rendered copy and the unsubscribe footer a non-essential topic carries", html)
	}
	if h, _ := msgs[0]["headers"].(map[string]any); h["List-Unsubscribe-Post"] != "List-Unsubscribe=One-Click" {
		t.Fatalf("headers = %v, want RFC 8058's one-click headers", h)
	}

	// And in the recipient's inbox, over the REST surface, with a token their host signed.
	req, _ := http.NewRequest(http.MethodGet, "http://"+w.svc.HTTPAddr()+"/notifications", nil)
	req.Header.Set("Authorization", "Bearer "+w.signer.Recipient("local", wsTenant, ada, time.Now(), 10*time.Minute))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var list struct {
		Items []struct {
			Topic, Title string
		} `json:"items"`
	}
	body, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(body, &list); err != nil || resp.StatusCode != 200 || len(list.Items) != 1 || list.Items[0].Topic != "invoice_overdue" || list.Items[0].Title != "Invoice INV-42 is overdue" {
		t.Fatalf("the inbox = %d %s, want the one notification, its title rendered from the in-app template", resp.StatusCode, body)
	}

	// Another recipient's token opens another inbox, and no token opens none.
	for name, header := range map[string]string{
		"another recipient": "Bearer " + w.signer.Recipient("local", wsTenant, domain.Recipient{Kind: "user", ID: "grace"}, time.Now(), time.Minute),
		"no token":          "",
	} {
		req, _ := http.NewRequest(http.MethodGet, "http://"+w.svc.HTTPAddr()+"/notifications", nil)
		if header != "" {
			req.Header.Set("Authorization", header)
		}
		r, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(r.Body)
		r.Body.Close()
		if name == "no token" && r.StatusCode != http.StatusUnauthorized {
			t.Errorf("%s: status %d, want 401", name, r.StatusCode)
		}
		if name == "another recipient" && (r.StatusCode != 200 || strings.Contains(string(b), "invoice_overdue")) {
			t.Errorf("%s: %d %s, want an empty inbox that is not Ada's", name, r.StatusCode, b)
		}
	}
	if r, err := http.Get("http://" + w.svc.HTTPAddr() + "/readyz"); err != nil || r.StatusCode != 200 {
		t.Errorf("readyz = %v, %v, want 200", r, err)
	}
}

// T051's "show it can fail", the first half: a service with one channel only. The scenario fails at the step
// that needs the second channel, and says what is missing.
func TestTheScenarioFailsWhenTheServiceHasOneChannelOnly(t *testing.T) {
	w := startService(t, false)
	failedAt, err := w.scenario(false)
	if failedAt != "register the topic" || !errors.Is(err, domain.ErrUnknownChannel) || !strings.Contains(err.Error(), "email") {
		t.Fatalf("the scenario failed at %q with %v, want the topic naming email refused as an unregistered channel", failedAt, err)
	}
}

// ...and the second: the template registration is skipped. Everything is accepted, and the notification
// reaches the provider as nothing, because there is no copy to send: it is rejected, and the scenario fails
// at the delivery it was written to see.
func TestTheScenarioFailsWhenTheTemplateRegistrationIsSkipped(t *testing.T) {
	w := startService(t, true)
	failedAt, err := w.scenario(true)
	if failedAt != "see it delivered on two channels" || err == nil || !strings.Contains(err.Error(), "rejected") {
		t.Fatalf("the scenario failed at %q with %v, want it to fail at the delivery, the email rejected for want of copy", failedAt, err)
	}
	if n := len(w.mail.messages()); n != 0 {
		t.Fatalf("the provider was asked to send %d messages, want none: there was nothing to say", n)
	}
}

// Isolation is the schema's, so the service refuses a database role that would skip it.
func TestTheServiceRefusesADatabaseRoleThatBypassesRowLevelSecurity(t *testing.T) {
	db := pgtest.New(t)
	redisOpt := redistest.New(t).Client.Options()
	env := func(dsn string, allow string) func(string) string {
		return func(k string) string {
			return map[string]string{
				"NOTIFY_PG_DSN": dsn, "NOTIFY_REDIS_URL": fmt.Sprintf("redis://%s/%d", redisOpt.Addr, redisOpt.DB),
				"NOTIFY_REALM_BINDINGS": "p=local", "NOTIFY_RECIPIENT_JWKS": "local=https://host.example.test/jwks.json",
				"NOTIFY_HTTP_ADDR": "127.0.0.1:0", "NOTIFY_GRPC_ADDR": "127.0.0.1:0", "NOTIFY_ALLOW_PRIVILEGED_DB_ROLE": allow,
			}[k]
		}
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg, err := LoadConfig(env(dsnOf(db.Admin), ""))
	if err != nil {
		t.Fatal(err)
	}
	if svc, err := Build(t.Context(), cfg, log); err == nil || !strings.Contains(err.Error(), "row-level security") {
		if svc != nil {
			svc.Close()
		}
		t.Fatalf("Build as a superuser = %v, want a refusal naming row-level security", err)
	}
	cfg, err = LoadConfig(env(dsnOf(db.Admin), "true"))
	if err != nil {
		t.Fatal(err)
	}
	svc, err := Build(t.Context(), cfg, log)
	if err != nil {
		t.Fatalf("Build with the override = %v, want it to start (a throwaway development database)", err)
	}
	svc.Close()
}
