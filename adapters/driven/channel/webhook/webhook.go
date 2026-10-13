// Package webhook is a channel that delivers a notification as a signed HTTP POST to the
// recipient's URL (T033a). It is the third channel, the one that shows NS-008: it is
// this package and a registration line, and nothing in fan-out, persistence or
// preferences knows its name (NR-016).
//
// The address is a URL a tenant supplied, so the channel treats it as hostile input.
// It calls https only, never an address on a private network (checked at the
// connection, after the name is resolved, so a name that points inward is no way
// round), never follows a redirect, takes no proxy from the environment, and reports a
// URL it must not call as a dead address and not as a fault. What it says about a
// failure never carries the URL, which often holds a token.
//
// It is at-least-once by declaration (D32): a receiver can dedupe on the
// Idempotency-Key it is sent, and this channel has no way to know that one does.
package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// The headers a receiver verifies. The signature is "v1=" and the hex HMAC-SHA256, under
// the shared secret, of the timestamp, a dot, and the exact bytes of the body; a receiver
// that also rejects a timestamp far from its own clock is safe against replay.
const (
	SignatureHeader = "X-Notify-Signature"
	TimestampHeader = "X-Notify-Timestamp"
)

const (
	defaultTimeout = 10 * time.Second

	// maxSnippet is how much of a refusal's body is quoted in the report: enough to
	// tell an operator what the endpoint said, not enough to carry a page of it into a
	// dead letter.
	maxSnippet = 256

	// maxRetryAfter bounds a hint: a floor on the backoff and not a veto (D10).
	maxRetryAfter = 24 * time.Hour

	// coolDown is how long an address that answered 404, or redirected, is left alone:
	// a customer may yet fix the route, and until then every notification would only
	// fail the same way.
	coolDown = 24 * time.Hour
)

// Channel is the webhook channel.
type Channel struct {
	kind         domain.ChannelKind
	secret       string
	timeout      time.Duration
	clock        ports.Clock
	allowHTTP    bool
	allowPrivate bool
	client       *http.Client
}

var (
	_ ports.Channel           = (*Channel)(nil)
	_ ports.AddressNormalizer = (*Channel)(nil)
)

// Option configures a Channel.
type Option func(*Channel)

// WithKind registers the channel under another kind than "webhook": a second endpoint
// family with its own secret, say.
func WithKind(kind domain.ChannelKind) Option { return func(c *Channel) { c.kind = kind } }

// WithTimeout bounds one request, connection to last byte of the answer. The default is
// ten seconds. Config.ClaimLease must exceed it.
func WithTimeout(d time.Duration) Option { return func(c *Channel) { c.timeout = d } }

// WithClock sets the clock the signature's timestamp and a Retry-After date are read
// against. The default is the system clock.
func WithClock(clock ports.Clock) Option {
	return func(c *Channel) {
		if clock != nil {
			c.clock = clock
		}
	}
}

// WithAllowHTTP permits http:// addresses. It is for a receiver inside the same trust
// boundary, such as a service in the same cluster, and for tests. Nothing on the open
// network should be called in the clear.
func WithAllowHTTP() Option { return func(c *Channel) { c.allowHTTP = true } }

// WithAllowPrivateNetworks permits addresses that resolve to loopback, private,
// link-local and other internal ranges. It is for a receiver inside the same trust
// boundary, and for tests. Turned on for a channel whose addresses tenants supply, it
// lets a tenant make the engine call its internal network, including a cloud's metadata
// service.
func WithAllowPrivateNetworks() Option { return func(c *Channel) { c.allowPrivate = true } }

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

// New returns a webhook channel that signs every request with secret. The secret reaches
// it by the host's wiring (a channel_providers row's secret_ref), never from this
// package's configuration. An unsigned webhook cannot be told from a forged one, so a
// channel with no secret is refused.
func New(secret string, opts ...Option) (*Channel, error) {
	if secret == "" {
		return nil, errors.New("webhook: a signing secret is required: an unsigned webhook cannot be told from a forged one")
	}
	c := &Channel{kind: "webhook", secret: secret, timeout: defaultTimeout, clock: systemClock{}}
	for _, opt := range opts {
		opt(c)
	}
	if c.kind == "" {
		return nil, errors.New("webhook: the channel kind is empty")
	}
	if c.timeout <= 0 {
		return nil, fmt.Errorf("webhook: the timeout must be positive, got %v", c.timeout)
	}
	c.client = c.newClient()
	return c, nil
}

// newClient builds the HTTP client. It takes no proxy from the environment, because a
// proxy would be what the connection is made to and the destination check would judge
// the proxy; it follows no redirect, which could lead anywhere, including to an address
// the check never saw.
func (c *Channel) newClient() *http.Client {
	dialer := &net.Dialer{Timeout: c.timeout}
	if !c.allowPrivate {
		dialer.Control = func(_, address string, _ syscall.RawConn) error {
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return err
			}
			if ip, err := netip.ParseAddr(host); err != nil || blockedDestination(ip) {
				return errBlocked{}
			}
			return nil
		}
	}
	return &http.Client{
		Timeout: c.timeout,
		Transport: &http.Transport{
			Proxy:               nil,
			DialContext:         dialer.DialContext,
			ForceAttemptHTTP2:   true,
			TLSHandshakeTimeout: c.timeout,
			MaxIdleConnsPerHost: 4,
			IdleConnTimeout:     90 * time.Second,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

// errBlocked is what the dialer's check reports for a destination on a private network.
type errBlocked struct{}

func (errBlocked) Error() string { return "the address resolves to a private network" }

var (
	// carrierGradeNAT is shared address space, RFC 6598: not private by RFC 1918, and not
	// the open internet either.
	carrierGradeNAT = netip.MustParsePrefix("100.64.0.0/10")
	// thisNetwork, protocolAssignments, benchmarking and reserved are ranges no public
	// endpoint lives in (RFC 1122, 6890, 2544); the last also holds the broadcast address.
	thisNetwork         = netip.MustParsePrefix("0.0.0.0/8")
	protocolAssignments = netip.MustParsePrefix("192.0.0.0/24")
	benchmarking        = netip.MustParsePrefix("198.18.0.0/15")
	reserved            = netip.MustParsePrefix("240.0.0.0/4")
)

// blockedDestination reports whether a connection may not be made to ip. An IPv4 address
// written as IPv6 is judged as the IPv4 it is.
func blockedDestination(ip netip.Addr) bool {
	ip = ip.Unmap()
	switch {
	case !ip.IsValid(), ip.IsLoopback(), ip.IsPrivate(), ip.IsLinkLocalUnicast(), ip.IsLinkLocalMulticast(),
		ip.IsInterfaceLocalMulticast(), ip.IsMulticast(), ip.IsUnspecified():
		return true
	}
	for _, p := range []netip.Prefix{carrierGradeNAT, thisNetwork, protocolAssignments, benchmarking, reserved} {
		if p.Contains(ip) {
			return true
		}
	}
	return false
}

// Kind implements ports.Channel.
func (c *Channel) Kind() domain.ChannelKind { return c.kind }

// Capabilities implements ports.Channel: addressed by a URL, consuming structured data,
// not shown in the inbox, and at-least-once.
func (*Channel) Capabilities() domain.ChannelCapabilities {
	return domain.ChannelCapabilities{NeedsAddress: true, RichContent: true, Dedup: domain.DedupNone}
}

// NormalizeAddress implements ports.AddressNormalizer: the same URL with another case in
// the scheme or the host, or a fragment, is one address, so one suppression and one
// delivery key. The path and the query are case-sensitive and stay as they are.
func (*Channel) NormalizeAddress(value string) string {
	v := strings.TrimSpace(value)
	u, err := url.Parse(v)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return v
	}
	u.Scheme, u.Host = strings.ToLower(u.Scheme), strings.ToLower(u.Host)
	u.Fragment, u.RawFragment = "", ""
	return u.String()
}

// Sign is the signature header's value for a body sent at timestamp (Unix seconds): what a
// receiver recomputes.
func Sign(secret string, timestamp int64, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(strconv.FormatInt(timestamp, 10)))
	mac.Write([]byte("."))
	mac.Write(body)
	return "v1=" + hex.EncodeToString(mac.Sum(nil))
}

type identity struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

type member struct {
	Title string         `json:"title,omitempty"`
	Body  string         `json:"body,omitempty"`
	Data  map[string]any `json:"data,omitempty"`
}

// body is what a receiver is sent. It names the delivery's subject and content, and who
// it is for in the host's own terms; it carries no address, and no key but the header's.
type body struct {
	Topic          string            `json:"topic"`
	Priority       string            `json:"priority,omitempty"`
	Subject        string            `json:"subject,omitempty"`
	Body           string            `json:"body,omitempty"`
	Data           map[string]any    `json:"data,omitempty"`
	Payload        map[string]string `json:"payload,omitempty"`
	NotificationID string            `json:"notification_id,omitempty"`
	DigestID       string            `json:"digest_id,omitempty"`
	Members        []member          `json:"members,omitempty"`
	OccurredAt     string            `json:"occurred_at,omitempty"`
	Tenant         identity          `json:"tenant"`
	Recipient      identity          `json:"recipient"`
	Attempt        int               `json:"attempt"`
}

// Deliver implements ports.Channel. It makes one request and reports what the endpoint
// answered; it checks no suppression and does not retry (D10, D13).
func (c *Channel) Deliver(ctx context.Context, d domain.Delivery) (domain.DeliveryResult, error) {
	target, problem := c.parse(d.Address.Value)
	if problem != "" {
		return deadAddress(domain.SuppressInvalidAddress, 0, "the address is not a usable webhook URL: "+problem), nil
	}

	payload := body{
		Topic: string(d.Topic), Priority: string(d.Notification.Priority), Subject: d.Content.Subject, Body: d.Content.Body,
		Data: d.Content.Data, Payload: d.Notification.Payload, NotificationID: d.NotificationID, DigestID: d.DigestID,
		Tenant:    identity{d.Identity.Tenant.Kind, d.Identity.Tenant.ID},
		Recipient: identity{d.Identity.Recipient.Kind, d.Identity.Recipient.ID},
		Attempt:   d.Attempt,
	}
	if !d.Notification.OccurredAt.IsZero() {
		payload.OccurredAt = d.Notification.OccurredAt.UTC().Format(time.RFC3339)
	}
	for _, m := range d.Digest {
		payload.Members = append(payload.Members, member{Title: m.Title, Body: m.Body, Data: m.Data})
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		// Content the channel cannot encode will not encode on the next try either.
		return domain.DeliveryResult{Outcome: domain.Rejected, Detail: "the content cannot be encoded as JSON: " + err.Error()}, nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target.String(), bytes.NewReader(raw))
	if err != nil {
		return deadAddress(domain.SuppressInvalidAddress, 0, "the address is not a usable webhook URL"), nil
	}
	now := c.clock.Now()
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", d.IdemKey)
	req.Header.Set("User-Agent", "intel-notification/webhook")
	req.Header.Set(TimestampHeader, strconv.FormatInt(now.Unix(), 10))
	req.Header.Set(SignatureHeader, Sign(c.secret, now.Unix(), raw))

	resp, err := c.client.Do(req)
	if err != nil {
		var blocked errBlocked
		if errors.As(err, &blocked) {
			return deadAddress(domain.SuppressInvalidAddress, 0, "the address resolves to a private network, which the channel does not call"), nil
		}
		return domain.DeliveryResult{Outcome: domain.Retry, Detail: transportDetail(err, target.Host)}, nil
	}
	defer func() { _ = resp.Body.Close() }()
	snippet, _ := io.ReadAll(io.LimitReader(resp.Body, maxSnippet))
	return c.classify(resp, target.Host, snippet, now), nil
}

// parse checks an address and returns the URL to call, or why it may not be called. The
// reason never repeats the address.
func (c *Channel) parse(value string) (*url.URL, string) {
	u, err := url.Parse(strings.TrimSpace(value))
	switch {
	case err != nil:
		return nil, "it does not parse"
	case u.Host == "" || u.Hostname() == "":
		return nil, "it names no host"
	case u.User != nil:
		return nil, "it carries credentials; the secret is the channel's"
	case u.Scheme == "https", u.Scheme == "http" && c.allowHTTP:
		return u, ""
	case u.Scheme == "http":
		return nil, "it is plain http"
	default:
		return nil, "its scheme is not https"
	}
}

// classify turns an answer into the outcome the dispatcher acts on (D10, D12, D41).
func (c *Channel) classify(resp *http.Response, host string, snippet []byte, now time.Time) domain.DeliveryResult {
	status := resp.StatusCode
	detail := fmt.Sprintf("HTTP %d from %s", status, host)
	if q := quote(snippet); q != "" {
		detail += ": " + q
	}
	switch {
	case status >= 200 && status < 300:
		return domain.DeliveryResult{Outcome: domain.Delivered, ProviderMessageID: requestID(resp)}
	case status == http.StatusGone:
		return deadAddress(domain.SuppressInvalidAddress, 0, detail)
	case status == http.StatusNotFound, status >= 300 && status < 400:
		if status >= 300 && status < 400 {
			detail += " (a redirect is not followed)"
		}
		return deadAddress(domain.SuppressSoftBounce, coolDown, detail)
	case status == http.StatusUnauthorized, status == http.StatusForbidden,
		status == http.StatusRequestTimeout, status == http.StatusTooEarly, status == http.StatusTooManyRequests,
		status >= 500:
		// The endpoint's own trouble, or a credential on the customer's side: theirs to
		// fix, and not a verdict on the message (D41).
		return domain.DeliveryResult{Outcome: domain.Retry, RetryAfter: retryAfter(resp.Header.Get("Retry-After"), now), Detail: detail}
	default:
		return domain.DeliveryResult{Outcome: domain.Rejected, Detail: detail}
	}
}

func deadAddress(reason domain.SuppressionReason, forAWhile time.Duration, detail string) domain.DeliveryResult {
	return domain.DeliveryResult{Outcome: domain.Suppressed, SuppressReason: reason, SuppressFor: forAWhile, Detail: detail}
}

// requestID is the receiver's own id for the request, when it sends one, so a callback or
// a support request can name it.
func requestID(resp *http.Response) string {
	for _, h := range []string{"X-Request-Id", "Request-Id"} {
		if v := strings.TrimSpace(resp.Header.Get(h)); v != "" && len(v) <= 128 {
			return v
		}
	}
	return ""
}

// retryAfter reads a Retry-After header: seconds, or an HTTP date. Never negative, and
// never more than a day.
func retryAfter(v string, now time.Time) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	var d time.Duration
	if secs, err := strconv.ParseInt(v, 10, 64); err == nil {
		if secs > int64(maxRetryAfter/time.Second) {
			return maxRetryAfter
		}
		d = time.Duration(secs) * time.Second
	} else if t, err := http.ParseTime(v); err == nil {
		d = t.Sub(now)
	}
	return min(max(d, 0), maxRetryAfter)
}

// quote makes a response's first bytes fit for a report: control characters become
// spaces, bytes that are not valid UTF-8 become U+FFFD (strings.Map does that to every
// one), and it is short. The report is stored in a text column, where a stray byte would
// fail the write that records the outcome.
func quote(b []byte) string {
	s := strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return ' '
		}
		return r
	}, string(b))
	return strings.TrimSpace(s)
}

// transportDetail describes a failure to make the request without the URL net/http puts
// in its error: the host, and the reason.
func transportDetail(err error, host string) string {
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		err = urlErr.Err
	}
	return fmt.Sprintf("POST to %s failed: %v", host, err)
}
