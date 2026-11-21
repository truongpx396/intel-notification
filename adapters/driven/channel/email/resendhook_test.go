package email_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/truongpx396/intel-notification/adapters/driven/channel/email"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// T044a, NR-032, D14. Resend signs its webhooks with Svix: an HMAC-SHA256 over "<id>.<timestamp>.<body>" with
// the endpoint's secret, sent as `v1,<base64>` in svix-signature (several, space-separated, while a secret is
// being rotated). The decoder verifies that over the raw bytes before it reads one of them, and then applies
// what the event says: a hard bounce or a complaint suppresses the address for ever, a soft bounce for a
// while, and every delivery event records the provider's status against the delivery it names.

// The vector is Svix's own documentation example, and its signature was recomputed with Python's hmac and
// hashlib, not with this code.
const (
	svixSecret    = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"
	svixID        = "msg_p5jXN8AQM9LWM0D4loKWxJek"
	svixTimestamp = "1614265330"
	svixBody      = `{"test": 2432232314}`
	svixSignature = "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE="
)

var svixNow = time.Unix(1614265330, 0).UTC()

func hdr(kv ...string) map[string][]string {
	h := http.Header{}
	for i := 0; i+1 < len(kv); i += 2 {
		h.Add(kv[i], kv[i+1])
	}
	return h
}

type statusCall struct {
	ch     domain.ChannelKind
	id     string
	status string
}

type suppressCall struct {
	realm  domain.Realm
	ch     domain.ChannelKind
	hash   string
	reason domain.SuppressionReason
	until  time.Time
}

// hookWorld is the decoder over a recorder of what it wrote. Calls are kept in the order they happened, so a
// test can say which came first.
type hookWorld struct {
	mu          sync.Mutex
	order       []string
	statuses    []statusCall
	suppressed  []suppressCall
	statusErr   error
	suppressErr error
	hook        *email.ResendWebhook
}

func (w *hookWorld) RecordProviderStatus(_ context.Context, ch domain.ChannelKind, id, status string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.order = append(w.order, "status")
	w.statuses = append(w.statuses, statusCall{ch, id, status})
	return w.statusErr
}

func (w *hookWorld) Suppressed(context.Context, domain.Realm, domain.ChannelKind, []byte) (bool, error) {
	return false, nil
}

func (w *hookWorld) Suppress(_ context.Context, realm domain.Realm, ch domain.ChannelKind, hash []byte, reason domain.SuppressionReason, until time.Time) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.order = append(w.order, "suppress")
	w.suppressed = append(w.suppressed, suppressCall{realm, ch, string(hash), reason, until})
	return w.suppressErr
}

var _ ports.SuppressionStore = (*hookWorld)(nil)

func newHookWorld(t *testing.T, opts ...email.ResendWebhookOption) *hookWorld {
	t.Helper()
	w := &hookWorld{}
	opts = append([]email.ResendWebhookOption{email.WithWebhookClock(func() time.Time { return svixNow })}, opts...)
	hook, err := email.NewResendWebhook("alpha", []string{svixSecret}, w, w, opts...)
	if err != nil {
		t.Fatal(err)
	}
	w.hook = hook
	return w
}

func TestResendWebhookVerifiesTheSignatureOverTheRawBody(t *testing.T) {
	t.Parallel()
	good := hdr("svix-id", svixID, "svix-timestamp", svixTimestamp, "svix-signature", svixSignature)
	other := "whsec_" + base64.StdEncoding.EncodeToString([]byte("another endpoint's secret"))
	cases := []struct {
		name    string
		secrets []string
		header  map[string][]string
		body    string
		at      time.Time
		ok      bool
		why     string
	}{
		{"the documented vector", []string{svixSecret}, good, svixBody, svixNow, true, "a signature computed by another tool verifies"},
		{"header names in any case", []string{svixSecret}, hdr("SVIX-ID", svixID, "Svix-Timestamp", svixTimestamp, "svix-SIGNATURE", svixSignature), svixBody, svixNow, true, "net/http canonicalizes, a test double may not"},
		{"a body altered by one byte", []string{svixSecret}, good, `{"test": 2432232315}`, svixNow, false, "the signature is over the raw bytes"},
		{"a body with a space added", []string{svixSecret}, good, `{"test":  2432232314}`, svixNow, false, "re-serialising a body is not the body"},
		{"no svix-id", []string{svixSecret}, hdr("svix-timestamp", svixTimestamp, "svix-signature", svixSignature), svixBody, svixNow, false, "each header is part of what is signed"},
		{"no svix-timestamp", []string{svixSecret}, hdr("svix-id", svixID, "svix-signature", svixSignature), svixBody, svixNow, false, ""},
		{"no svix-signature", []string{svixSecret}, hdr("svix-id", svixID, "svix-timestamp", svixTimestamp), svixBody, svixNow, false, ""},
		{"another id", []string{svixSecret}, hdr("svix-id", "msg_other", "svix-timestamp", svixTimestamp, "svix-signature", svixSignature), svixBody, svixNow, false, "the id is signed"},
		{"another timestamp", []string{svixSecret}, hdr("svix-id", svixID, "svix-timestamp", "1614265331", "svix-signature", svixSignature), svixBody, svixNow, false, "the timestamp is signed"},
		{"a timestamp that is not a number", []string{svixSecret}, hdr("svix-id", svixID, "svix-timestamp", "yesterday", "svix-signature", svixSignature), svixBody, svixNow, false, ""},
		{"stale", []string{svixSecret}, good, svixBody, svixNow.Add(6 * time.Minute), false, "a captured callback is not replayable for ever"},
		{"from the future", []string{svixSecret}, good, svixBody, svixNow.Add(-6 * time.Minute), false, "the tolerance is both ways"},
		{"just inside the tolerance", []string{svixSecret}, good, svixBody, svixNow.Add(4 * time.Minute), true, "clocks differ a little"},
		{"another endpoint's secret", []string{other}, good, svixBody, svixNow, false, ""},
		{"the second of two secrets, while one is rotated", []string{other, svixSecret}, good, svixBody, svixNow, true, "a rotation overlaps"},
		{"the second signature of two", []string{svixSecret}, hdr("svix-id", svixID, "svix-timestamp", svixTimestamp, "svix-signature", "v1,AAAA "+svixSignature), svixBody, svixNow, true, "Svix sends several while a secret is being rotated"},
		{"a signature of another version only", []string{svixSecret}, hdr("svix-id", svixID, "svix-timestamp", svixTimestamp, "svix-signature", strings.Replace(svixSignature, "v1,", "v2,", 1)), svixBody, svixNow, false, "only v1 is a signature this decoder knows"},
		{"a signature that is not base64", []string{svixSecret}, hdr("svix-id", svixID, "svix-timestamp", svixTimestamp, "svix-signature", "v1,!!!"), svixBody, svixNow, false, ""},
		{"an empty signature", []string{svixSecret}, hdr("svix-id", svixID, "svix-timestamp", svixTimestamp, "svix-signature", "v1,"), svixBody, svixNow, false, "an empty value is not a match for an empty one"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			w := &hookWorld{}
			at := c.at
			hook, err := email.NewResendWebhook("alpha", c.secrets, w, w, email.WithWebhookClock(func() time.Time { return at }))
			if err != nil {
				t.Fatal(err)
			}
			err = hook.Verify(c.header, []byte(c.body))
			if c.ok && err != nil {
				t.Fatalf("Verify = %v, want nil: %s", err, c.why)
			}
			if !c.ok && err == nil {
				t.Fatalf("Verify = nil, want an error: %s", c.why)
			}
		})
	}
}

func TestResendWebhookRefusesASecretItCannotUse(t *testing.T) {
	t.Parallel()
	w := &hookWorld{}
	for name, secrets := range map[string][]string{
		"none":                 nil,
		"empty":                {""},
		"not base64":           {"whsec_!!!"},
		"nothing after prefix": {"whsec_"},
		"a good one and a bad": {svixSecret, "whsec_!!!"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := email.NewResendWebhook("alpha", secrets, w, w); err == nil {
				t.Fatalf("NewResendWebhook(%q) = nil error, want one: a decoder that cannot verify would accept or refuse everything", secrets)
			}
		})
	}
	t.Run("the error does not repeat the secret", func(t *testing.T) {
		t.Parallel()
		_, err := email.NewResendWebhook("alpha", []string{"whsec_!!!secret-material"}, w, w)
		if err == nil || strings.Contains(err.Error(), "secret-material") {
			t.Fatalf("error = %v, want one that does not carry the secret", err)
		}
	})
	t.Run("a realm is required", func(t *testing.T) {
		t.Parallel()
		if _, err := email.NewResendWebhook("", []string{svixSecret}, w, w); err == nil {
			t.Fatal("NewResendWebhook with no realm = nil error: a suppression would be written under no realm")
		}
	})
}

// emailIDOf is the provider's message id in a body: the key the delivery is found by.
func emailIDOf(t *testing.T, body string) string {
	t.Helper()
	var e struct {
		Data struct {
			EmailID string `json:"email_id"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(body), &e); err != nil {
		t.Fatal(err)
	}
	return e.Data.EmailID
}

func event(typ, id string, to ...string) string {
	quoted := make([]string, len(to))
	for i, a := range to {
		quoted[i] = strconv.Quote(a)
	}
	return fmt.Sprintf(`{"type":%q,"created_at":"2026-11-21T00:00:00Z","data":{"email_id":%q,"to":[%s]}}`, typ, id, strings.Join(quoted, ","))
}

func bounce(kind, id string, to ...string) string {
	quoted := make([]string, len(to))
	for i, a := range to {
		quoted[i] = strconv.Quote(a)
	}
	return fmt.Sprintf(`{"type":"email.bounced","data":{"email_id":%q,"to":[%s],"bounce":{"type":%q,"subType":"General","message":"x"}}}`, id, strings.Join(quoted, ","), kind)
}

func TestResendWebhookAppliesWhatTheEventSays(t *testing.T) {
	t.Parallel()
	hash := func(addr string) string { return string(domain.SuppressionHash("email", addr)) }
	cases := []struct {
		name       string
		body       string
		status     string
		suppressed []suppressCall
		why        string
	}{
		{"a hard bounce suppresses for ever", bounce("Permanent", "re_1", "Ada@Example.COM"), "bounced",
			[]suppressCall{{"alpha", "email", hash("Ada@example.com"), domain.SuppressHardBounce, time.Time{}}},
			"the zero time is permanent; the address is hashed in the channel's own normal form (D25)"},
		{"a soft bounce suppresses for a while", bounce("Transient", "re_2", "a@example.com"), "bounced",
			[]suppressCall{{"alpha", "email", hash("a@example.com"), domain.SuppressSoftBounce, svixNow.Add(time.Hour)}},
			"a full mailbox is a cooldown (D14)"},
		{"an undetermined bounce is not permanent", bounce("Undetermined", "re_3", "a@example.com"), "bounced",
			[]suppressCall{{"alpha", "email", hash("a@example.com"), domain.SuppressSoftBounce, svixNow.Add(time.Hour)}},
			"only a provider that says Permanent gets a permanent answer"},
		{"a bounce of no type is not permanent", `{"type":"email.bounced","data":{"email_id":"re_4","to":["a@example.com"]}}`, "bounced",
			[]suppressCall{{"alpha", "email", hash("a@example.com"), domain.SuppressSoftBounce, svixNow.Add(time.Hour)}},
			"a missing field must not read as the strongest value"},
		{"a complaint suppresses for ever", event("email.complained", "re_5", "a@example.com"), "complained",
			[]suppressCall{{"alpha", "email", hash("a@example.com"), domain.SuppressComplaint, time.Time{}}}, ""},
		{"every address of a bounce", bounce("Permanent", "re_6", "a@example.com", "b@example.com"), "bounced",
			[]suppressCall{
				{"alpha", "email", hash("a@example.com"), domain.SuppressHardBounce, time.Time{}},
				{"alpha", "email", hash("b@example.com"), domain.SuppressHardBounce, time.Time{}},
			}, ""},
		{"delivered is a status and no suppression", event("email.delivered", "re_7", "a@example.com"), "delivered", nil, ""},
		{"delayed is a status and no suppression", event("email.delivery_delayed", "re_8", "a@example.com"), "delayed", nil, "the provider is still trying"},
		{"sent is a status", event("email.sent", "re_9", "a@example.com"), "sent", nil, ""},
		{"failed is a status and no suppression", event("email.failed", "re_10", "a@example.com"), "failed", nil, "a failure to send says nothing of the address"},
		{"a bounce naming no address still records its status", `{"type":"email.bounced","data":{"email_id":"re_11","bounce":{"type":"Permanent"}}}`, "bounced", nil, ""},
		{"the realm in a body is not read", `{"type":"email.bounced","realm":"beta","data":{"email_id":"re_12","realm":"beta","to":["a@example.com"],"bounce":{"type":"Permanent"}}}`, "bounced",
			[]suppressCall{{"alpha", "email", hash("a@example.com"), domain.SuppressHardBounce, time.Time{}}},
			"the callback names no realm: it comes from the provider's own configuration (NR-009)"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			w := newHookWorld(t)
			if err := w.hook.Handle(t.Context(), []byte(c.body)); err != nil {
				t.Fatalf("Handle = %v, want nil", err)
			}
			if len(w.statuses) != 1 || w.statuses[0].ch != "email" || w.statuses[0].status != c.status {
				t.Fatalf("statuses = %+v, want one on email with %q: %s", w.statuses, c.status, c.why)
			}
			if got, want := w.statuses[0].id, emailIDOf(t, c.body); got != want {
				t.Fatalf("status recorded against %q, want the event's email_id %q", got, want)
			}
			if fmt.Sprint(w.suppressed) != fmt.Sprint(c.suppressed) {
				t.Fatalf("suppressions = %+v, want %+v: %s", w.suppressed, c.suppressed, c.why)
			}
		})
	}
}

func TestResendWebhookIgnoresAnEventItDoesNotKnow(t *testing.T) {
	t.Parallel()
	for _, typ := range []string{"email.opened", "email.clicked", "email.received", "domain.created", "contact.updated", ""} {
		t.Run(typ, func(t *testing.T) {
			t.Parallel()
			w := newHookWorld(t)
			if err := w.hook.Handle(t.Context(), []byte(event(typ, "re_1", "a@example.com"))); err != nil {
				t.Fatalf("Handle = %v, want nil: an event nobody asked for is not a failure the provider should retry", err)
			}
			if len(w.statuses)+len(w.suppressed) != 0 {
				t.Fatalf("wrote %+v and %+v for an event it does not apply", w.statuses, w.suppressed)
			}
		})
	}
}

func TestResendWebhookAnUnknownDeliveryStillSuppressesAndAsksForARetry(t *testing.T) {
	t.Parallel()
	w := newHookWorld(t)
	w.statusErr = domain.ErrUnknownDelivery
	err := w.hook.Handle(t.Context(), []byte(bounce("Permanent", "re_not_yet", "a@example.com")))
	if !errors.Is(err, domain.ErrUnknownDelivery) {
		t.Fatalf("Handle = %v, want ErrUnknownDelivery, so the provider is asked to try again", err)
	}
	if len(w.suppressed) != 1 {
		t.Fatalf("suppressions = %+v, want the address suppressed all the same: the address is dead whether or not its delivery is recorded yet", w.suppressed)
	}
	if fmt.Sprint(w.order) != "[suppress status]" {
		t.Fatalf("order = %v, want the suppression before the status, so a retry of a half-applied callback finishes it", w.order)
	}
}

func TestResendWebhookReportsAFaultOfItsStores(t *testing.T) {
	t.Parallel()
	boom := errors.New("database is down")
	t.Run("the suppression", func(t *testing.T) {
		t.Parallel()
		w := newHookWorld(t)
		w.suppressErr = boom
		err := w.hook.Handle(t.Context(), []byte(bounce("Permanent", "re_1", "a@example.com")))
		if !errors.Is(err, boom) || errors.Is(err, domain.ErrUnknownDelivery) {
			t.Fatalf("Handle = %v, want the store's fault and not a request to retry as if the delivery were unknown", err)
		}
		if len(w.statuses) != 0 {
			t.Fatalf("recorded %+v after the suppression failed: the callback is retried whole", w.statuses)
		}
	})
	t.Run("the status", func(t *testing.T) {
		t.Parallel()
		w := newHookWorld(t)
		w.statusErr = boom
		if err := w.hook.Handle(t.Context(), []byte(event("email.delivered", "re_1", "a@example.com"))); !errors.Is(err, boom) {
			t.Fatalf("Handle = %v, want the store's fault", err)
		}
	})
}

func TestResendWebhookRefusesABodyItCannotRead(t *testing.T) {
	t.Parallel()
	for name, body := range map[string]string{
		"not JSON":                          `{not json`,
		"not an object":                     `[1,2]`,
		"data is not an object":             `{"type":"email.bounced","data":"x"}`,
		"a delivery event with no email id": `{"type":"email.delivered","data":{"to":["a@example.com"]}}`,
		"a bounce with no email id":         `{"type":"email.bounced","data":{"to":["a@example.com"],"bounce":{"type":"Permanent"}}}`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			w := newHookWorld(t)
			err := w.hook.Handle(t.Context(), []byte(body))
			var bad *domain.ValidationError
			if !errors.As(err, &bad) {
				t.Fatalf("Handle = %v, want a *domain.ValidationError: a verified body that makes no sense is the provider's to fix, not a fault to retry", err)
			}
			if len(w.statuses)+len(w.suppressed) != 0 {
				t.Fatalf("wrote %+v and %+v for a body it could not read", w.statuses, w.suppressed)
			}
		})
	}
}

func TestResendWebhookSoftBounceCooldownIsConfigurable(t *testing.T) {
	t.Parallel()
	w := newHookWorld(t, email.WithSoftBounceCooldown(6*time.Hour))
	if err := w.hook.Handle(t.Context(), []byte(bounce("Transient", "re_1", "a@example.com"))); err != nil {
		t.Fatal(err)
	}
	if len(w.suppressed) != 1 || !w.suppressed[0].until.Equal(svixNow.Add(6*time.Hour)) {
		t.Fatalf("suppressions = %+v, want one until %v", w.suppressed, svixNow.Add(6*time.Hour))
	}
}

// A realm the decoder was not built for never appears: suppressions are written for the realm it was given.
func TestResendWebhookSuppressesInItsOwnRealm(t *testing.T) {
	t.Parallel()
	w := &hookWorld{}
	hook, err := email.NewResendWebhook("beta", []string{svixSecret}, w, w)
	if err != nil {
		t.Fatal(err)
	}
	if err := hook.Handle(t.Context(), []byte(bounce("Permanent", "re_1", "a@example.com"))); err != nil {
		t.Fatal(err)
	}
	if len(w.suppressed) != 1 || w.suppressed[0].realm != "beta" {
		t.Fatalf("suppressions = %+v, want one in realm beta", w.suppressed)
	}
}
