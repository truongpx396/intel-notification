package email_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/truongpx396/intel-notification/adapters/driven/channel/email"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/notifytest"
)

const resendKey = "re_test_key"

// resendRequest is one POST /emails as the fake saw it.
type resendRequest struct {
	Method, Path   string
	Authorization  string
	ContentType    string
	IdempotencyKey string
	Body           map[string]any
	RawBody        string
}

// fakeResend implements what Resend documents of POST /emails: a message id for a
// send, the same id for a repeat of the same key and body within the window, 409
// invalid_idempotent_request for the same key with another body, 409
// concurrent_idempotent_requests while the first is still in flight. A scripted
// reply, or a probe's armed failure, replaces the send.
type fakeResend struct {
	*httptest.Server
	probe *notifytest.Probe

	mu       sync.Mutex
	requests []resendRequest
	seen     map[string]sentMail // idempotency key -> what was sent
	next     int
	script   *scripted // answers the next request
}

type sentMail struct{ id, body string }

type scripted struct {
	status  int
	name    string
	message string
	header  map[string]string
}

func newFakeResend(t *testing.T, probe *notifytest.Probe) *fakeResend {
	t.Helper()
	f := &fakeResend{probe: probe, seen: map[string]sentMail{}}
	f.Server = httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.Close)
	return f
}

func (f *fakeResend) answerNext(s scripted) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.script = &s
}

func (f *fakeResend) got() []resendRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]resendRequest(nil), f.requests...)
}

func (f *fakeResend) handle(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(r.Body)
	req := resendRequest{
		Method: r.Method, Path: r.URL.Path, Authorization: r.Header.Get("Authorization"),
		ContentType: r.Header.Get("Content-Type"), IdempotencyKey: r.Header.Get("Idempotency-Key"), RawBody: string(raw),
	}
	_ = json.Unmarshal(raw, &req.Body)

	f.mu.Lock()
	f.requests = append(f.requests, req)
	script := f.script
	f.script = nil
	f.mu.Unlock()

	fail := func(status int, name, message string, header map[string]string) {
		for k, v := range header {
			w.Header().Set(k, v)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]any{"statusCode": status, "name": name, "message": message})
	}
	if script != nil {
		fail(script.status, script.name, script.message, script.header)
		return
	}
	if req.Authorization != "Bearer "+resendKey {
		fail(401, "missing_api_key", "Missing API key.", nil)
		return
	}
	if f.probe != nil {
		if fl, failed := f.probe.Attempt(req.IdempotencyKey); failed {
			switch fl {
			case notifytest.Transient:
				fail(503, "service_unavailable", "The API is temporarily unavailable.", nil)
			case notifytest.DeadAddress:
				// Resend judges a mailbox asynchronously, by webhook. What it does report
				// at once is a recipient that is not an address at all.
				fail(422, "validation_error", "Invalid `to` field. The email address needs to follow the `email@example.com` or `Name <email@example.com>` format.", nil)
			case notifytest.Permanent:
				fail(422, "missing_required_field", "Missing `subject` field.", nil)
			default:
				panic(http.ErrAbortHandler) // the connection drops
			}
			return
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	prior, repeated := f.seen[req.IdempotencyKey]
	switch {
	case repeated && prior.body != req.RawBody:
		fail(409, "invalid_idempotent_request", "This idempotency key has already been used with a different request payload.", nil)
		return
	case repeated:
		_ = json.NewEncoder(w).Encode(map[string]any{"id": prior.id})
		return
	}
	f.next++
	id := fmt.Sprintf("resend-%d", f.next)
	f.seen[req.IdempotencyKey] = sentMail{id: id, body: req.RawBody}
	if f.probe != nil {
		f.probe.Record(req.IdempotencyKey, true)
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"id": id})
}

func newResend(t *testing.T, f *fakeResend, opts ...email.ResendOption) *email.Resend {
	t.Helper()
	opts = append([]email.ResendOption{email.WithResendBaseURL(f.URL)}, opts...)
	m, err := email.NewResend(resendKey, "Notifications <notify@example.test>", opts...)
	if err != nil {
		t.Fatalf("NewResend: %v", err)
	}
	return m
}

// Resend honours an idempotency key for 24 hours (its documentation, and the
// sandbox under -tags provider): the mailer says so, and the contract, run against
// a double that behaves so, holds the claim to what the provider side shows (D32).
func TestResendDeclaresADayOfProviderDedup(t *testing.T) {
	t.Parallel()
	m := newResend(t, newFakeResend(t, nil))
	if level, window := m.Dedup(); level != domain.DedupProvider || window != 24*time.Hour {
		t.Fatalf("Dedup = %v, %v, want DedupProvider for 24h", level, window)
	}
}

func TestResendRunsTheChannelContractThroughTheEmailChannel(t *testing.T) {
	t.Parallel()
	notifytest.ChannelContract(t, func(t *testing.T) notifytest.ChannelUnderTest {
		t.Helper()
		probe := notifytest.NewProbe()
		ch, _ := newChannel(t, newResend(t, newFakeResend(t, probe)))
		return notifytest.ChannelUnderTest{Channel: ch, Probe: probe}
	})
}

// The request as Resend documents it: a POST to /emails with the key as a bearer
// token, the delivery key as the Idempotency-Key, and a JSON body naming exactly
// the sender, the one recipient, the subject, the HTML and the headers.
func TestResendRequestShape(t *testing.T) {
	t.Parallel()
	f := newFakeResend(t, nil)
	id, err := newResend(t, f).Send(t.Context(), sampleMail())
	if err != nil || id != "resend-1" {
		t.Fatalf("Send = %q, %v, want the id Resend returned", id, err)
	}
	reqs := f.got()
	if len(reqs) != 1 {
		t.Fatalf("%d requests, want 1", len(reqs))
	}
	r := reqs[0]
	if r.Method != "POST" || r.Path != "/emails" || r.Authorization != "Bearer "+resendKey || r.ContentType != "application/json" {
		t.Fatalf("request = %s %s auth %q content-type %q", r.Method, r.Path, r.Authorization, r.ContentType)
	}
	if r.IdempotencyKey != "abc123" {
		t.Fatalf("Idempotency-Key = %q, want the delivery key abc123", r.IdempotencyKey)
	}
	want := map[string]any{
		"from":    "\"Notifications\" <notify@example.test>",
		"to":      []any{"ada@example.com"},
		"subject": "Upload finished",
		"html":    "<p>Hello, Ada = welcome</p>",
		"headers": map[string]any{
			"List-Unsubscribe":      "<https://notify.example.test/u?t=1>",
			"List-Unsubscribe-Post": "List-Unsubscribe=One-Click",
		},
	}
	gotJSON, _ := json.Marshal(r.Body)
	wantJSON, _ := json.Marshal(want)
	if string(gotJSON) != string(wantJSON) {
		t.Fatalf("body = %s, want %s", gotJSON, wantJSON)
	}
}

// A sender with no display name is sent as the bare address, which Resend
// documents; one with a name is sent as "Name <address>".
func TestResendSendsTheSenderAsConfigured(t *testing.T) {
	t.Parallel()
	for name, c := range map[string]struct{ from, want string }{
		"a bare address": {"notify@example.test", "notify@example.test"},
		"with a name":    {"Notifications <notify@example.test>", "\"Notifications\" <notify@example.test>"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newFakeResend(t, nil)
			m, err := email.NewResend(resendKey, c.from, email.WithResendBaseURL(f.URL))
			if err != nil {
				t.Fatalf("NewResend: %v", err)
			}
			if _, err := m.Send(t.Context(), sampleMail()); err != nil {
				t.Fatalf("Send: %v", err)
			}
			if got := f.got()[0].Body["from"]; got != c.want {
				t.Fatalf("from = %q, want %q", got, c.want)
			}
		})
	}
}

// A repeat of the same delivery is the same email: Resend returns the first id and
// sends nothing again.
func TestResendCollapsesARepeatOfTheSameKey(t *testing.T) {
	t.Parallel()
	f := newFakeResend(t, nil)
	m := newResend(t, f)
	first, err1 := m.Send(t.Context(), sampleMail())
	second, err2 := m.Send(t.Context(), sampleMail())
	if err1 != nil || err2 != nil || first != second {
		t.Fatalf("Send twice = %q, %v then %q, %v, want one id", first, err1, second, err2)
	}
}

// The key Resend accepts is 1 to 256 characters. A delivery key is a 64-character
// hash, so it passes as it is; anything longer is replaced by its hash, which is
// the same for the same key, so a re-drive still collapses.
func TestResendKeepsTheKeyWithinWhatResendAccepts(t *testing.T) {
	t.Parallel()
	long := strings.Repeat("k", 300)
	sum := sha256.Sum256([]byte(long))
	cases := []struct{ name, key, want string }{
		{"a delivery key", strings.Repeat("a", 64), strings.Repeat("a", 64)},
		{"256 characters, the longest accepted", strings.Repeat("b", 256), strings.Repeat("b", 256)},
		{"257 characters", strings.Repeat("c", 257), func() string { s := sha256.Sum256([]byte(strings.Repeat("c", 257))); return hex.EncodeToString(s[:]) }()},
		{"far too long", long, hex.EncodeToString(sum[:])},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newFakeResend(t, nil)
			m := sampleMail()
			m.IdemKey = tc.key
			if _, err := newResend(t, f).Send(t.Context(), m); err != nil {
				t.Fatalf("Send: %v", err)
			}
			if got := f.got()[0].IdempotencyKey; got != tc.want {
				t.Fatalf("Idempotency-Key = %q, want %q", got, tc.want)
			}
		})
	}
}

// Without a key there is no exactly-once at the provider, and a send that looked
// like it had one would be a lie: it is refused before any request is made.
func TestResendRefusesToSendWithoutAKey(t *testing.T) {
	t.Parallel()
	f := newFakeResend(t, nil)
	m := sampleMail()
	m.IdemKey = ""
	_, err := newResend(t, f).Send(t.Context(), m)
	var se *email.SendError
	if !errors.As(err, &se) || se.Kind != email.Permanent {
		t.Fatalf("Send with no key = %v, want a permanent *SendError", err)
	}
	if n := len(f.got()); n != 0 {
		t.Fatalf("%d requests were made without a key", n)
	}
}

// What Resend answers decides the kind of failure (its error reference). A refusal
// about this message is permanent. Anything about the account, the key, the domain
// or the plan is the provider's to fix and not the message's, so it is transient
// and a failover chain moves on; and a key reused for another body is permanent,
// since the first body was sent and a retry cannot change that.
func TestResendClassifiesTheAnswers(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		reply      scripted
		want       email.ErrorKind
		retryAfter time.Duration
	}{
		{"a field is invalid", scripted{status: 400, name: "validation_error", message: "Invalid `subject` field."}, email.Permanent, 0},
		{"a required field is missing", scripted{status: 422, name: "missing_required_field", message: "Missing `html`."}, email.Permanent, 0},
		{"the recipient is not an address", scripted{status: 422, name: "validation_error", message: "Invalid `to` field. The email address needs to follow the format."}, email.InvalidAddress, 0},
		{"the key was used for another body", scripted{status: 409, name: "invalid_idempotent_request", message: "different payload"}, email.Permanent, 0},
		{"the first request is still in flight", scripted{status: 409, name: "concurrent_idempotent_requests", message: "in progress"}, email.Transient, 0},
		{"a resource is locked", scripted{status: 409, name: "resource_locked", message: "locked"}, email.Transient, 0},
		{"the key is missing", scripted{status: 401, name: "missing_api_key", message: "Missing API key."}, email.Transient, 0},
		{"the key is suspended", scripted{status: 403, name: "suspended_api_key", message: "suspended"}, email.Transient, 0},
		{"the domain is not verified", scripted{status: 403, name: "validation_error", message: "The example.test domain is not verified."}, email.Transient, 0},
		{"too many requests", scripted{status: 429, name: "rate_limit_exceeded", message: "slow down"}, email.Transient, 0},
		{"too many requests with a hint in seconds", scripted{status: 429, name: "rate_limit_exceeded", header: map[string]string{"Retry-After": "45"}}, email.Transient, 45 * time.Second},
		{"the daily quota is spent", scripted{status: 429, name: "daily_quota_exceeded", message: "quota"}, email.Transient, 0},
		{"a server error", scripted{status: 500, name: "application_error", message: "oops"}, email.Transient, 0},
		{"unavailable", scripted{status: 503, name: "service_unavailable", message: "later"}, email.Transient, 0},
		{"an endpoint that does not exist", scripted{status: 404, name: "not_found", message: "no"}, email.Transient, 0},
		{"an answer that is not JSON", scripted{status: 502}, email.Transient, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newFakeResend(t, nil)
			f.answerNext(tc.reply)
			_, err := newResend(t, f).Send(t.Context(), sampleMail())
			var se *email.SendError
			if !errors.As(err, &se) || se.Kind != tc.want || se.RetryAfter != tc.retryAfter {
				t.Fatalf("Send = %v, want a *SendError of kind %v retrying after %v", err, tc.want, tc.retryAfter)
			}
			if !strings.Contains(err.Error(), strconv.Itoa(tc.reply.status)) {
				t.Fatalf("error %q does not carry the HTTP status, which is the last error a dead letter keeps", err)
			}
			if strings.Contains(err.Error(), resendKey) {
				t.Fatalf("error %q contains the API key", err)
			}
		})
	}
}

// A provider's hint is a floor on the backoff and not a veto: one a client cannot
// believe, a date in the past or a number a day long, does not stall a delivery.
func TestResendRetryAfterIsBounded(t *testing.T) {
	t.Parallel()
	future := time.Now().Add(2 * time.Minute).UTC().Format(http.TimeFormat)
	cases := []struct {
		name, header string
		min, max     time.Duration
	}{
		{"seconds", "30", 30 * time.Second, 30 * time.Second},
		{"zero", "0", 0, 0},
		{"a negative number", "-5", 0, 0},
		{"not a number", "soon", 0, 0},
		{"an HTTP date in the future", future, 100 * time.Second, 2 * time.Minute},
		{"an HTTP date in the past", "Mon, 02 Jan 2006 15:04:05 GMT", 0, 0},
		{"a hint longer than a day is capped", "999999999", 24 * time.Hour, 24 * time.Hour},
		{"a number too large for a duration is capped, not wrapped negative", "10000000000", 24 * time.Hour, 24 * time.Hour},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newFakeResend(t, nil)
			f.answerNext(scripted{status: 429, name: "rate_limit_exceeded", header: map[string]string{"Retry-After": tc.header}})
			_, err := newResend(t, f).Send(t.Context(), sampleMail())
			var se *email.SendError
			if !errors.As(err, &se) {
				t.Fatalf("Send = %v, want a *SendError", err)
			}
			if se.RetryAfter < tc.min || se.RetryAfter > tc.max {
				t.Fatalf("RetryAfter = %v, want between %v and %v", se.RetryAfter, tc.min, tc.max)
			}
		})
	}
}

// A connection that fails, or an answer cut short, has given no verdict.
func TestResendCannotReachTheAPI(t *testing.T) {
	t.Parallel()
	t.Run("the server is gone", func(t *testing.T) {
		t.Parallel()
		f := newFakeResend(t, nil)
		m := newResend(t, f)
		f.Close()
		_, err := m.Send(t.Context(), sampleMail())
		var se *email.SendError
		if err == nil || errors.As(err, &se) {
			t.Fatalf("Send = %v, want a plain error", err)
		}
		if strings.Contains(err.Error(), resendKey) {
			t.Fatalf("error %q contains the API key", err)
		}
	})
	t.Run("a success with no id", func(t *testing.T) {
		t.Parallel()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, `{}`) }))
		defer srv.Close()
		m, _ := email.NewResend(resendKey, "notify@example.test", email.WithResendBaseURL(srv.URL))
		_, err := m.Send(t.Context(), sampleMail())
		var se *email.SendError
		if err == nil || errors.As(err, &se) {
			t.Fatalf("Send = %v, want a plain error: the send cannot be traced without an id", err)
		}
	})
}

func TestNewResendRefusesWhatCannotWork(t *testing.T) {
	t.Parallel()
	for name, c := range map[string]struct{ key, from string }{
		"no API key":                     {"", "notify@example.test"},
		"no sender":                      {resendKey, ""},
		"a sender that is not a mailbox": {resendKey, "not an address"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := email.NewResend(c.key, c.from); err == nil {
				t.Fatal("NewResend succeeded")
			}
		})
	}
}
