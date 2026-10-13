package webhook_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/truongpx396/intel-notification/adapters/driven/channel/webhook"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/notifytest"
)

const secret = "whsec_test_secret"

// request is one POST as the receiver saw it.
type request struct {
	Method, Path, RawQuery string
	Header                 http.Header
	Body                   []byte
}

// receiver stands where a customer's endpoint would. A reply scripted for the next
// request replaces the answer; otherwise a probe's armed failure becomes the status a
// real endpoint would give, and a success is counted against the Idempotency-Key.
type receiver struct {
	*httptest.Server
	probe *notifytest.Probe

	mu     sync.Mutex
	reqs   []request
	script []reply
}

type reply struct {
	status int
	header map[string]string
	body   string
}

func newReceiver(t *testing.T, probe *notifytest.Probe) *receiver {
	t.Helper()
	r := &receiver{probe: probe}
	r.Server = httptest.NewServer(http.HandlerFunc(r.handle))
	t.Cleanup(r.Close)
	return r
}

func (r *receiver) answer(rep ...reply) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.script = append(r.script, rep...)
}

func (r *receiver) got() []request {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]request(nil), r.reqs...)
}

func (r *receiver) handle(w http.ResponseWriter, req *http.Request) {
	body, _ := io.ReadAll(req.Body)
	r.mu.Lock()
	r.reqs = append(r.reqs, request{Method: req.Method, Path: req.URL.Path, RawQuery: req.URL.RawQuery, Header: req.Header.Clone(), Body: body})
	var rep *reply
	if len(r.script) > 0 {
		rep, r.script = &r.script[0], r.script[1:]
	}
	r.mu.Unlock()

	if rep != nil {
		for k, v := range rep.header {
			w.Header().Set(k, v)
		}
		w.WriteHeader(rep.status)
		_, _ = io.WriteString(w, rep.body)
		return
	}
	key := req.Header.Get("Idempotency-Key")
	if r.probe != nil {
		if f, failed := r.probe.Attempt(key); failed {
			switch f {
			case notifytest.Transient:
				w.WriteHeader(http.StatusServiceUnavailable)
			case notifytest.DeadAddress:
				w.WriteHeader(http.StatusGone)
			case notifytest.Permanent:
				w.WriteHeader(http.StatusUnprocessableEntity)
				_, _ = io.WriteString(w, `{"error":"payload does not match the schema"}`)
			default: // the endpoint falls over mid-request
				conn, _, err := w.(http.Hijacker).Hijack()
				if err == nil {
					_ = conn.Close()
				}
			}
			return
		}
		r.probe.Record(key, false)
	}
	w.Header().Set("X-Request-Id", "req-"+key[:min(8, len(key))])
	w.WriteHeader(http.StatusOK)
}

func newChannel(t *testing.T, opts ...webhook.Option) *webhook.Channel {
	t.Helper()
	ch, err := webhook.New(secret, append([]webhook.Option{webhook.WithAllowHTTP(), webhook.WithAllowPrivateNetworks()}, opts...)...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return ch
}

type fixedClock time.Time

func (c fixedClock) Now() time.Time { return time.Time(c) }

func delivery(url string) domain.Delivery {
	return domain.Delivery{
		ID: "d1", IdemKey: "idem-1",
		Identity:       domain.Identity{Realm: "aisat", Tenant: domain.Tenant{Kind: "workspace", ID: "w1"}, Recipient: domain.Recipient{Kind: "user", ID: "u1"}},
		NotificationID: "n1", Topic: "ingestion_complete", Channel: "webhook", Attempt: 2,
		Notification: domain.Notification{Priority: domain.PriorityWarning, Payload: map[string]string{"url": "/uploads/9"}, OccurredAt: time.Date(2031, 3, 1, 11, 0, 0, 0, time.UTC)},
		Address:      domain.Address{Channel: "webhook", Value: url},
		Content:      domain.RenderedContent{Subject: "Upload done", Body: "Your upload finished.", Data: map[string]any{"files": 3}},
	}
}

func TestWebhookRunsTheChannelContract(t *testing.T) {
	t.Parallel()
	notifytest.ChannelContract(t, func(t *testing.T) notifytest.ChannelUnderTest {
		t.Helper()
		probe := notifytest.NewProbe()
		srv := newReceiver(t, probe)
		return notifytest.ChannelUnderTest{
			Channel: newChannel(t), Probe: probe,
			Address: func(n int) string { return fmt.Sprintf("%s/hook/%d", srv.URL, n) },
		}
	})
}

// What it says about itself decides what the engine does with it: it is addressed by
// a URL and consumes structured data, it shows nothing in the inbox, and it is
// at-least-once, since a receiver can dedupe on the key it is sent but this channel
// cannot know that it does (D32).
func TestWebhookDeclaresWhatItIs(t *testing.T) {
	t.Parallel()
	ch := newChannel(t)
	want := domain.ChannelCapabilities{NeedsAddress: true, RichContent: true, Dedup: domain.DedupNone}
	if got := ch.Capabilities(); got != want {
		t.Fatalf("Capabilities = %+v, want %+v", got, want)
	}
	if ch.Kind() != "webhook" {
		t.Fatalf("Kind = %q, want webhook", ch.Kind())
	}
	if other := newChannel(t, webhook.WithKind("pager")); other.Kind() != "pager" {
		t.Fatalf("Kind = %q, want pager", other.Kind())
	}
}

func TestNewRefusesWhatCannotWork(t *testing.T) {
	t.Parallel()
	if _, err := webhook.New(""); err == nil {
		t.Error("New with no secret succeeded: an unsigned webhook cannot be told from a forged one")
	}
	if _, err := webhook.New(secret, webhook.WithTimeout(0)); err == nil {
		t.Error("New with a zero timeout succeeded")
	}
	if _, err := webhook.New(secret, webhook.WithKind("")); err == nil {
		t.Error("New with an empty kind succeeded")
	}
}

// The signature scheme, against a vector computed with Python's hmac and hashlib: the
// HMAC-SHA256 of "<timestamp>.<body>" under the secret, in hex, as "v1=<hex>". A
// receiver in any language recomputes it, so it is a contract and not an
// implementation detail.
func TestSignAgainstAnIndependentVector(t *testing.T) {
	t.Parallel()
	body := []byte(`{"topic":"ingestion_complete","body":"Your upload finished."}`)
	const want = "v1=76db1b4e75526bece369e1bd5db58783f6741666b48f55ee27d9846a84ea9e31"
	if got := webhook.Sign(secret, 1900000000, body); got != want {
		t.Fatalf("Sign = %s, want %s", got, want)
	}
	if webhook.Sign(secret, 1900000001, body) == want || webhook.Sign("another", 1900000000, body) == want ||
		webhook.Sign(secret, 1900000000, append(body, ' ')) == want {
		t.Fatal("the signature did not change with the timestamp, the secret or the body")
	}
}

// The request as a receiver sees it: a POST of JSON, the delivery key as the
// Idempotency-Key a receiver dedupes on, the time the request was made, and a
// signature that verifies against the bytes actually sent.
func TestWebhookRequestShape(t *testing.T) {
	t.Parallel()
	srv := newReceiver(t, nil)
	now := time.Date(2031, 3, 1, 12, 0, 0, 0, time.UTC)
	ch := newChannel(t, webhook.WithClock(fixedClock(now)))
	d := delivery(srv.URL + "/hooks/in?token=s3cret")

	res, err := ch.Deliver(t.Context(), d)
	if err != nil || res.Outcome != domain.Delivered || res.ProviderMessageID != "req-idem-1" {
		t.Fatalf("Deliver = %+v, %v, want Delivered with the receiver's request id", res, err)
	}
	reqs := srv.got()
	if len(reqs) != 1 {
		t.Fatalf("%d requests, want 1", len(reqs))
	}
	r := reqs[0]
	if r.Method != "POST" || r.Path != "/hooks/in" || r.RawQuery != "token=s3cret" {
		t.Errorf("request = %s %s?%s, want POST to the address as given", r.Method, r.Path, r.RawQuery)
	}
	if ct := r.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q", ct)
	}
	if k := r.Header.Get("Idempotency-Key"); k != "idem-1" {
		t.Errorf("Idempotency-Key = %q, want the delivery key", k)
	}
	if ts := r.Header.Get(webhook.TimestampHeader); ts != fmt.Sprint(now.Unix()) {
		t.Errorf("%s = %q, want %d", webhook.TimestampHeader, ts, now.Unix())
	}
	if sig, want := r.Header.Get(webhook.SignatureHeader), webhook.Sign(secret, now.Unix(), r.Body); sig != want {
		t.Errorf("%s = %q, want %q: the signature must verify against the bytes sent", webhook.SignatureHeader, sig, want)
	}
	if ua := r.Header.Get("User-Agent"); !strings.HasPrefix(ua, "intel-notification") {
		t.Errorf("User-Agent = %q", ua)
	}

	var got map[string]any
	if err := json.Unmarshal(r.Body, &got); err != nil {
		t.Fatalf("body is not JSON: %v\n%s", err, r.Body)
	}
	want := map[string]any{
		"topic": "ingestion_complete", "priority": "warning", "subject": "Upload done", "body": "Your upload finished.",
		"data": map[string]any{"files": float64(3)}, "payload": map[string]any{"url": "/uploads/9"},
		"notification_id": "n1", "attempt": float64(2), "occurred_at": "2031-03-01T11:00:00Z",
		"tenant": map[string]any{"kind": "workspace", "id": "w1"}, "recipient": map[string]any{"kind": "user", "id": "u1"},
	}
	wantJSON, _ := json.Marshal(want)
	gotJSON, _ := json.Marshal(got)
	if string(gotJSON) != string(wantJSON) {
		t.Errorf("body = %s\nwant   %s", gotJSON, wantJSON)
	}
}

// A digest is one request that carries its members, and names the window and no
// notification (D4).
func TestWebhookBodyForADigest(t *testing.T) {
	t.Parallel()
	srv := newReceiver(t, nil)
	d := delivery(srv.URL + "/hook")
	d.NotificationID, d.Notification = "", domain.Notification{}
	d.DigestID = "w-1"
	d.Digest = []domain.Notification{{Title: "Upload a", Body: "a done", Data: map[string]any{"n": 1}}, {Title: "Upload b", Body: "b done"}}

	if res, err := newChannel(t).Deliver(t.Context(), d); err != nil || res.Outcome != domain.Delivered {
		t.Fatalf("Deliver = %+v, %v", res, err)
	}
	var got struct {
		NotificationID string `json:"notification_id"`
		DigestID       string `json:"digest_id"`
		Members        []struct {
			Title string `json:"title"`
			Body  string `json:"body"`
		} `json:"members"`
	}
	if err := json.Unmarshal(srv.got()[0].Body, &got); err != nil {
		t.Fatal(err)
	}
	if got.NotificationID != "" || got.DigestID != "w-1" || len(got.Members) != 2 || got.Members[0].Title != "Upload a" || got.Members[1].Body != "b done" {
		t.Fatalf("body = %+v, want the window and its two members", got)
	}
}

// What an endpoint answers decides what the engine does next (D10, D12, D41). A
// success is delivery. A refusal of the message, any 4xx that is not about the
// endpoint, is rejected for good. The endpoint's own trouble is a retry, so an
// expired credential on the customer's side does not dead-letter a message that
// would send tomorrow. An endpoint that is gone is a dead address, and one that
// merely does not answer right now is a cooldown.
func TestWebhookClassifiesTheAnswers(t *testing.T) {
	t.Parallel()
	cases := []struct {
		status  int
		outcome domain.Outcome
		reason  domain.SuppressionReason
		forever bool // a suppression with no expiry
		why     string
	}{
		{200, domain.Delivered, "", false, "success"},
		{201, domain.Delivered, "", false, "any 2xx is success"},
		{204, domain.Delivered, "", false, "no content is still success"},
		{500, domain.Retry, "", false, "the endpoint's own fault"},
		{502, domain.Retry, "", false, "a gateway in front of it"},
		{503, domain.Retry, "", false, "unavailable"},
		{504, domain.Retry, "", false, "a gateway timeout"},
		{408, domain.Retry, "", false, "it timed out waiting for us"},
		{425, domain.Retry, "", false, "too early"},
		{429, domain.Retry, "", false, "rate limited: wait, do not drop"},
		{401, domain.Retry, "", false, "a credential trouble on the customer's side is theirs to fix, not a verdict on the message (D41)"},
		{403, domain.Retry, "", false, "the same"},
		{410, domain.Suppressed, domain.SuppressInvalidAddress, true, "gone for good"},
		{404, domain.Suppressed, domain.SuppressSoftBounce, false, "no such route now: a cooldown, since a customer may yet fix it"},
		{301, domain.Suppressed, domain.SuppressSoftBounce, false, "a redirect is not followed, and the address as given does not serve"},
		{308, domain.Suppressed, domain.SuppressSoftBounce, false, "the same"},
		{400, domain.Rejected, "", false, "the endpoint refuses this message"},
		{413, domain.Rejected, "", false, "too large for the endpoint"},
		{415, domain.Rejected, "", false, "it will not take JSON"},
		{422, domain.Rejected, "", false, "it does not accept the content"},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprint(tc.status), func(t *testing.T) {
			t.Parallel()
			srv := newReceiver(t, nil)
			srv.answer(reply{status: tc.status})
			res, err := newChannel(t).Deliver(t.Context(), delivery(srv.URL+"/hook"))
			if err != nil {
				t.Fatalf("Deliver returned the error %v, want an outcome: %s", err, tc.why)
			}
			if res.Outcome != tc.outcome {
				t.Fatalf("HTTP %d gave %v, want %v: %s", tc.status, res.Outcome, tc.outcome, tc.why)
			}
			if tc.outcome == domain.Suppressed {
				if res.SuppressReason != tc.reason || (res.SuppressFor == 0) != tc.forever {
					t.Fatalf("suppression = %q for %v, want %q and forever = %v", res.SuppressReason, res.SuppressFor, tc.reason, tc.forever)
				}
			}
			if tc.outcome != domain.Delivered && !strings.Contains(res.Detail, fmt.Sprint(tc.status)) {
				t.Fatalf("Detail = %q, want it to name the status", res.Detail)
			}
		})
	}
}

// A provider's Retry-After is a hint the dispatcher takes as a floor (D10): seconds or
// a date, never negative, and bounded, so that a header nobody can believe cannot
// stall a delivery for a year.
func TestWebhookReadsRetryAfter(t *testing.T) {
	t.Parallel()
	now := time.Date(2031, 3, 1, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name   string
		header string
		want   time.Duration
	}{
		{"seconds", "120", 120 * time.Second},
		{"a date", now.Add(90 * time.Second).UTC().Format(http.TimeFormat), 90 * time.Second},
		{"a date already past", now.Add(-time.Hour).UTC().Format(http.TimeFormat), 0},
		{"zero", "0", 0},
		{"negative", "-5", 0},
		{"nonsense", "soon", 0},
		{"absent", "", 0},
		{"a year", "31536000", 24 * time.Hour},
		{"a number that overflows a duration", "9223372036854775807", 24 * time.Hour},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := newReceiver(t, nil)
			h := map[string]string{}
			if tc.header != "" {
				h["Retry-After"] = tc.header
			}
			srv.answer(reply{status: 429, header: h})
			res, err := newChannel(t, webhook.WithClock(fixedClock(now))).Deliver(t.Context(), delivery(srv.URL+"/hook"))
			if err != nil || res.Outcome != domain.Retry || res.RetryAfter != tc.want {
				t.Fatalf("Deliver = %+v, %v, want Retry after %v", res, err, tc.want)
			}
		})
	}
}

// A redirect is not followed. Following one would let an endpoint send the engine
// anywhere, including to an address the destination check never saw.
func TestWebhookDoesNotFollowRedirects(t *testing.T) {
	t.Parallel()
	hit := make(chan struct{}, 1)
	elsewhere := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hit <- struct{}{} }))
	t.Cleanup(elsewhere.Close)
	srv := newReceiver(t, nil)
	srv.answer(reply{status: 302, header: map[string]string{"Location": elsewhere.URL + "/inner"}})

	res, err := newChannel(t).Deliver(t.Context(), delivery(srv.URL+"/hook"))
	if err != nil || res.Outcome != domain.Suppressed {
		t.Fatalf("Deliver = %+v, %v, want a suppression of the address as given", res, err)
	}
	select {
	case <-hit:
		t.Fatal("the redirect was followed")
	default:
	}
}

// An address the channel must not call is a dead address, and nothing is sent: the
// destination is an input from a tenant, and the engine's network is not theirs to
// probe. Without WithAllowPrivateNetworks a loopback receiver is refused, and it is
// refused at the connection, after name resolution, so a name that resolves to an
// internal address is no way round it.
func TestWebhookRefusesAddressesItMustNotCall(t *testing.T) {
	t.Parallel()
	srv := newReceiver(t, nil)
	strict, err := webhook.New(secret) // no opt-ins: https to public addresses only
	if err != nil {
		t.Fatal(err)
	}
	loopbackOnly, err := webhook.New(secret, webhook.WithAllowHTTP()) // http is allowed, private networks are not
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name    string
		ch      *webhook.Channel
		address string
	}{
		{"empty", strict, ""},
		{"not a URL", strict, "not a url"},
		{"no scheme", strict, "hooks.example.com/in"},
		{"no host", strict, "https:///path"},
		{"an unsupported scheme", strict, "ftp://hooks.example.com/in"},
		{"plain http", strict, "http://hooks.example.com/in"},
		{"credentials in the URL", strict, "https://user:pw@hooks.example.com/in"},
		{"a loopback address", loopbackOnly, srv.URL + "/hook"},
		{"localhost by name", loopbackOnly, strings.Replace(srv.URL, "127.0.0.1", "localhost", 1) + "/hook"},
		{"a private address", loopbackOnly, "http://10.1.2.3/hook"},
		{"the cloud metadata address", loopbackOnly, "http://169.254.169.254/latest/meta-data"},
		{"an IPv6 loopback", loopbackOnly, "http://[::1]:9/hook"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			res, err := tc.ch.Deliver(t.Context(), delivery(tc.address))
			if err != nil {
				t.Fatalf("Deliver returned the error %v, want an outcome", err)
			}
			if res.Outcome != domain.Suppressed || res.SuppressReason != domain.SuppressInvalidAddress || res.SuppressFor != 0 {
				t.Fatalf("Deliver = %+v, want the address suppressed for good as invalid", res)
			}
			if res.Detail == "" {
				t.Fatal("no Detail")
			}
		})
	}
	if n := len(srv.got()); n != 0 {
		t.Fatalf("the receiver got %d requests, want none: a refused address is never called", n)
	}
}

// What the channel reports is read by an operator and stored in the dead letter, so
// it must not carry what the address does: a URL often holds a token. A transport fault
// names the host and the reason and not the URL, and a refusal's body is quoted short
// and without control characters.
func TestWebhookDetailNeverCarriesTheURLsSecrets(t *testing.T) {
	t.Parallel()
	closed := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := closed.URL + "/hooks/in?token=s3cret-token"
	closed.Close() // nothing listens any more

	res, err := newChannel(t).Deliver(t.Context(), delivery(url))
	if err != nil || res.Outcome != domain.Retry {
		t.Fatalf("Deliver = %+v, %v, want Retry for a refused connection", res, err)
	}
	if strings.Contains(res.Detail, "s3cret-token") || strings.Contains(res.Detail, "/hooks/in") {
		t.Fatalf("Detail = %q carries the URL", res.Detail)
	}

	srv := newReceiver(t, nil)
	srv.answer(reply{status: 400, body: "bad\x00request\r\n" + strings.Repeat("x", 1000)})
	res, err = newChannel(t).Deliver(t.Context(), delivery(srv.URL+"/hook?token=s3cret-token"))
	if err != nil || res.Outcome != domain.Rejected {
		t.Fatalf("Deliver = %+v, %v", res, err)
	}
	if strings.Contains(res.Detail, "s3cret-token") {
		t.Fatalf("Detail = %q carries the URL", res.Detail)
	}
	if len(res.Detail) > 400 || strings.ContainsAny(res.Detail, "\x00\r\n") {
		t.Fatalf("Detail has %d bytes and control characters: %q", len(res.Detail), res.Detail)
	}
	if !strings.Contains(res.Detail, "bad") {
		t.Fatalf("Detail = %q, want the start of the body quoted", res.Detail)
	}
}

// What is quoted is text: bytes that are not valid UTF-8 are dropped, because the report
// is stored in a text column and a stray byte would fail the write that records the
// outcome.
func TestWebhookQuotesOnlyValidText(t *testing.T) {
	t.Parallel()
	srv := newReceiver(t, nil)
	srv.answer(reply{status: 400, body: "caf\xe9 \xff\xfe broke"})
	res, err := newChannel(t).Deliver(t.Context(), delivery(srv.URL+"/hook"))
	if err != nil || res.Outcome != domain.Rejected {
		t.Fatalf("Deliver = %+v, %v", res, err)
	}
	if !utf8.ValidString(res.Detail) || !strings.Contains(res.Detail, "broke") {
		t.Fatalf("Detail = %q, want valid text that still says what the endpoint said", res.Detail)
	}
}

// An endpoint that does not answer in time is a retry, whatever it is doing. The
// request is bounded by the channel's own timeout, which ClaimLease must exceed.
func TestWebhookTimesOutAsARetry(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	slow := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-release }))
	t.Cleanup(func() { close(release); slow.Close() })

	res, err := newChannel(t, webhook.WithTimeout(100*time.Millisecond)).Deliver(t.Context(), delivery(slow.URL+"/hook"))
	if err != nil || res.Outcome != domain.Retry {
		t.Fatalf("Deliver = %+v, %v, want Retry", res, err)
	}
}

// A context that ends is the engine shutting down and not the endpoint's fault, but
// it is still not a verdict: the delivery is tried again, by whoever claims it next.
func TestWebhookReportsACancelledContextAsARetry(t *testing.T) {
	t.Parallel()
	srv := newReceiver(t, nil)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	res, err := newChannel(t).Deliver(ctx, delivery(srv.URL+"/hook"))
	if err != nil || res.Outcome != domain.Retry {
		t.Fatalf("Deliver = %+v, %v, want Retry", res, err)
	}
	if n := len(srv.got()); n != 0 {
		t.Fatalf("%d requests were made on a context that had ended", n)
	}
}

// Address equality is the channel's: the same URL spelled with another case in the
// scheme or host is one address, so it is one suppression and one delivery key. The
// path is case-sensitive and stays as it is.
func TestWebhookNormalizesAddresses(t *testing.T) {
	t.Parallel()
	ch := newChannel(t)
	cases := []struct{ in, want string }{
		{"https://Hooks.Example.COM/In?x=1", "https://hooks.example.com/In?x=1"},
		{"HTTPS://hooks.example.com/in", "https://hooks.example.com/in"},
		{"  https://hooks.example.com/in  ", "https://hooks.example.com/in"},
		{"https://hooks.example.com/in#frag", "https://hooks.example.com/in"},
		{"https://hooks.example.com:8443/in", "https://hooks.example.com:8443/in"},
		{"not a url", "not a url"},
		{"", ""},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			if got := ch.NormalizeAddress(tc.in); got != tc.want {
				t.Fatalf("NormalizeAddress(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// Many deliveries at once do not interfere: each request carries its own key and a
// signature that verifies.
func TestWebhookIsSafeForConcurrentUse(t *testing.T) {
	t.Parallel()
	srv := newReceiver(t, nil)
	ch := newChannel(t)
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Go(func() {
			d := delivery(srv.URL + "/hook")
			d.IdemKey = fmt.Sprintf("key-%d", i)
			if res, err := ch.Deliver(t.Context(), d); err != nil || res.Outcome != domain.Delivered {
				t.Errorf("delivery %d = %+v, %v", i, res, err)
			}
		})
	}
	wg.Wait()
	seen := map[string]bool{}
	for _, r := range srv.got() {
		k := r.Header.Get("Idempotency-Key")
		ts := r.Header.Get(webhook.TimestampHeader)
		var unix int64
		_, _ = fmt.Sscan(ts, &unix)
		if seen[k] || r.Header.Get(webhook.SignatureHeader) != webhook.Sign(secret, unix, r.Body) {
			t.Fatalf("request %s repeated or badly signed", k)
		}
		seen[k] = true
	}
	if len(seen) != 16 {
		t.Fatalf("%d distinct keys, want 16", len(seen))
	}
}
