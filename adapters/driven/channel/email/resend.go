package email

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/mail"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/truongpx396/intel-notification/domain"
)

// Resend is a Mailer over the Resend HTTP API (POST /emails).
//
// It declares DedupProvider for 24 hours, which is what Resend documents of its
// Idempotency-Key: a repeat of a key, within a day, returns the first message's id
// and sends nothing. A replay of a dead letter a day later sends again, so the
// window is stated and not rounded up (D32). The claim is held to the provider's
// sandbox by the suite run under -tags provider.
//
// A hard bounce is not something POST /emails reports: Resend learns of it after
// the send and says so by webhook (T044), which suppresses the address then. What
// the call can report at once is a recipient that is not an address.
type Resend struct {
	apiKey  string
	from    string
	baseURL string
	client  *http.Client
}

var _ Mailer = (*Resend)(nil)

// ResendWindow is how long Resend remembers an Idempotency-Key.
const ResendWindow = 24 * time.Hour

// maxRetryAfter bounds a provider's hint. It is a floor on the backoff and not a
// veto (D10): a header a client cannot believe must not stall a delivery.
const maxRetryAfter = 24 * time.Hour

// ResendOption configures a Resend mailer.
type ResendOption func(*Resend)

// WithResendBaseURL points the mailer at another API host: a test double.
func WithResendBaseURL(u string) ResendOption {
	return func(r *Resend) { r.baseURL = strings.TrimRight(u, "/") }
}

// WithResendHTTPClient sets the HTTP client. The default times out after 30 seconds.
func WithResendHTTPClient(c *http.Client) ResendOption {
	return func(r *Resend) {
		if c != nil {
			r.client = c
		}
	}
}

// NewResend returns a mailer that sends as from, a mailbox with or without a
// display name, with the given API key. Credentials reach it by the host's wiring
// (a channel_providers row's secret_ref), never from this package's configuration.
func NewResend(apiKey, from string, opts ...ResendOption) (*Resend, error) {
	if apiKey == "" {
		return nil, errors.New("email: Resend needs an API key")
	}
	sender, err := mail.ParseAddress(from)
	if err != nil {
		return nil, fmt.Errorf("email: Resend sender %q is not a mailbox: %w", from, err)
	}
	// A bare address is sent bare: Resend documents "Name <address>" and the plain form.
	fromField := sender.Address
	if sender.Name != "" {
		fromField = sender.String()
	}
	r := &Resend{apiKey: apiKey, from: fromField, baseURL: "https://api.resend.com", client: &http.Client{Timeout: 30 * time.Second}}
	for _, opt := range opts {
		opt(r)
	}
	return r, nil
}

// Dedup implements Mailer.
func (*Resend) Dedup() (domain.DedupLevel, time.Duration) { return domain.DedupProvider, ResendWindow }

type resendBody struct {
	From    string            `json:"from"`
	To      []string          `json:"to"`
	Subject string            `json:"subject"`
	HTML    string            `json:"html"`
	Headers map[string]string `json:"headers,omitempty"`
}

// printableKey is what an HTTP header value may hold without escaping, and Resend's
// length bound.
var printableKey = regexp.MustCompile(`^[\x21-\x7e]{1,256}$`)

// idempotencyKey is the key as Resend accepts it: 1 to 256 printable characters. A
// delivery key (a 64-character hash) passes as it is; one that does not is replaced
// by its hash, which is the same for the same key, so a re-drive still collapses.
func idempotencyKey(k string) string {
	if printableKey.MatchString(k) {
		return k
	}
	sum := sha256.Sum256([]byte(k))
	return hex.EncodeToString(sum[:])
}

// Send implements Mailer.
func (r *Resend) Send(ctx context.Context, m Mail) (string, error) {
	if m.IdemKey == "" {
		return "", &SendError{Kind: Permanent, Err: errors.New("resend: the message has no idempotency key, so the provider could not collapse a re-drive")}
	}
	payload, err := json.Marshal(resendBody{From: r.from, To: []string{m.To}, Subject: m.Subject, HTML: m.HTML, Headers: m.Headers})
	if err != nil {
		return "", &SendError{Kind: Permanent, Err: fmt.Errorf("resend: encode the message: %w", err)}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.baseURL+"/emails", bytes.NewReader(payload))
	if err != nil {
		return "", fmt.Errorf("email: resend: build the request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+r.apiKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", idempotencyKey(m.IdemKey))

	resp, err := r.client.Do(req)
	if err != nil {
		// The URL error can carry the request URL but never a header, so the key is not in it.
		return "", fmt.Errorf("email: resend: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("email: resend: read the answer: %w", err)
	}

	if resp.StatusCode/100 == 2 {
		var ok struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(body, &ok); err != nil || ok.ID == "" {
			return "", fmt.Errorf("email: resend: HTTP %d with no message id", resp.StatusCode)
		}
		return ok.ID, nil
	}
	return "", classifyResend(resp.StatusCode, body, resp.Header.Get("Retry-After"), time.Now())
}

// classifyResend maps Resend's answer (its error reference) to a *SendError.
//
// Only a refusal about this message is permanent: 400 and 422, and 409
// invalid_idempotent_request, a key reused for another body, where the first body
// was sent and a retry cannot change that. Anything about the account, the key, the
// domain or the plan, 401 and 403 and the rest, is the provider's to fix and not the
// message's, so it is transient: the delivery waits for the operator, and a failover
// chain moves to its next provider.
func classifyResend(status int, body []byte, retryAfter string, now time.Time) error {
	var e struct {
		Name    string `json:"name"`
		Message string `json:"message"`
	}
	_ = json.Unmarshal(body, &e)
	if e.Message == "" && e.Name == "" {
		e.Message = strings.TrimSpace(string(body))
		if len(e.Message) > 200 {
			e.Message = e.Message[:200]
		}
	}
	detail := fmt.Errorf("resend: HTTP %d %s: %s", status, e.Name, e.Message)
	switch {
	case status == http.StatusTooManyRequests:
		return &SendError{Kind: Transient, RetryAfter: parseRetryAfter(retryAfter, now), Err: detail}
	case status == http.StatusConflict && e.Name == "invalid_idempotent_request":
		return &SendError{Kind: Permanent, Err: detail}
	case (status == http.StatusBadRequest || status == http.StatusUnprocessableEntity) && namesTheRecipient(e.Message):
		return &SendError{Kind: InvalidAddress, Err: detail}
	case status == http.StatusBadRequest || status == http.StatusUnprocessableEntity:
		return &SendError{Kind: Permanent, Err: detail}
	}
	return &SendError{Kind: Transient, Err: detail}
}

// namesTheRecipient reports whether a validation message is about the `to` field.
func namesTheRecipient(message string) bool {
	m := strings.ToLower(message)
	return strings.Contains(m, "`to`") && strings.Contains(m, "invalid")
}

// parseRetryAfter reads a Retry-After header, in seconds or as an HTTP date, and
// bounds it. A value it cannot read, or one already past, is no hint.
func parseRetryAfter(h string, now time.Time) time.Duration {
	h = strings.TrimSpace(h)
	var d time.Duration
	if secs, err := strconv.ParseInt(h, 10, 64); err == nil {
		if secs <= 0 {
			return 0
		}
		if secs > int64(maxRetryAfter/time.Second) {
			return maxRetryAfter
		}
		d = time.Duration(secs) * time.Second
	} else if t, err := http.ParseTime(h); err == nil {
		d = t.Sub(now)
	}
	if d < 0 {
		return 0
	}
	return min(d, maxRetryAfter)
}
