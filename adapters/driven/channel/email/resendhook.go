package email

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// StatusRecorder records a provider's delivery status against the delivery it names (ports.Store).
type StatusRecorder interface {
	RecordProviderStatus(ctx context.Context, ch domain.ChannelKind, providerMessageID, status string) error
}

// Defaults of a ResendWebhook.
const (
	// DefaultSoftBounceCooldown is how long a soft bounce keeps an address out of use: a full mailbox is a
	// cooldown and not a verdict (D14).
	DefaultSoftBounceCooldown = time.Hour
	// WebhookTolerance is how far a callback's timestamp may be from the clock, either way. Svix's own
	// libraries use five minutes.
	WebhookTolerance = 5 * time.Minute
)

// errNotVerified is all a caller is told of a callback that does not verify: what was wrong with it is not
// the sender's to learn.
var errNotVerified = errors.New("email: the callback's signature is not verified")

// ResendWebhook is the ports.Webhook for /webhooks/email/resend (T044a, NR-032, D14).
//
// Resend signs a webhook with Svix: an HMAC-SHA256 over "<svix-id>.<svix-timestamp>.<raw body>" with the
// endpoint's secret, sent as "v1,<base64>" in svix-signature, several of them while a secret is rotated.
// Verify checks that over the bytes as they arrived, before Handle reads one of them. Handle then applies what
// the event says: a hard bounce or a complaint suppresses the address for ever, a soft bounce for a cooldown,
// and every delivery event records the provider's status against the delivery it names.
//
// A callback names no realm. The decoder is built for one, from the provider's own configuration, and it is
// the realm every suppression it writes is under (NR-009).
type ResendWebhook struct {
	realm     domain.Realm
	keys      [][]byte
	status    StatusRecorder
	suppress  ports.SuppressionStore
	now       func() time.Time
	cooldown  time.Duration
	tolerance time.Duration
}

var _ ports.Webhook = (*ResendWebhook)(nil)

// ResendWebhookOption configures a ResendWebhook.
type ResendWebhookOption func(*ResendWebhook)

// WithWebhookClock sets the clock a callback's timestamp and a cooldown are measured against.
func WithWebhookClock(now func() time.Time) ResendWebhookOption {
	return func(h *ResendWebhook) {
		if now != nil {
			h.now = now
		}
	}
}

// WithSoftBounceCooldown sets how long a soft bounce suppresses an address (DefaultSoftBounceCooldown).
func WithSoftBounceCooldown(d time.Duration) ResendWebhookOption {
	return func(h *ResendWebhook) {
		if d > 0 {
			h.cooldown = d
		}
	}
}

// NewResendWebhook returns the decoder for realm's Resend endpoint. secrets are its signing secrets, as Resend
// shows them ("whsec_" and base64): more than one while a secret is being rotated, and at least one. An error
// never repeats a secret.
func NewResendWebhook(realm domain.Realm, secrets []string, rec StatusRecorder, sup ports.SuppressionStore, opts ...ResendWebhookOption) (*ResendWebhook, error) {
	if err := realm.Validate(); err != nil {
		return nil, fmt.Errorf("email: a Resend webhook is for one realm: %w", err)
	}
	if len(secrets) == 0 {
		return nil, errors.New("email: a Resend webhook needs a signing secret: with none it could verify nothing")
	}
	h := &ResendWebhook{realm: realm, status: rec, suppress: sup, now: time.Now, cooldown: DefaultSoftBounceCooldown, tolerance: WebhookTolerance}
	for i, s := range secrets {
		key, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(s, "whsec_"))
		if err != nil || len(key) == 0 {
			return nil, fmt.Errorf("email: Resend webhook secret %d is not a signing secret (whsec_ and base64)", i+1)
		}
		h.keys = append(h.keys, key)
	}
	for _, o := range opts {
		o(h)
	}
	return h, nil
}

// headerValues is every value of the named header, whatever case the map spells it in.
func headerValues(header map[string][]string, name string) []string {
	var out []string
	for k, v := range header {
		if strings.EqualFold(k, name) {
			out = append(out, v...)
		}
	}
	return out
}

// Verify implements ports.Webhook.
func (h *ResendWebhook) Verify(header map[string][]string, body []byte) error {
	ids, stamps, sigs := headerValues(header, "svix-id"), headerValues(header, "svix-timestamp"), headerValues(header, "svix-signature")
	if len(ids) != 1 || len(stamps) != 1 || len(sigs) == 0 || ids[0] == "" {
		return errNotVerified
	}
	id, stamp := ids[0], stamps[0]
	seconds, err := strconv.ParseInt(stamp, 10, 64)
	if err != nil {
		return errNotVerified
	}
	if skew := h.now().Sub(time.Unix(seconds, 0)); skew > h.tolerance || skew < -h.tolerance {
		return errNotVerified
	}
	for _, key := range h.keys {
		mac := hmac.New(sha256.New, key)
		mac.Write([]byte(id + "." + stamp + "."))
		mac.Write(body)
		want := mac.Sum(nil)
		for _, header := range sigs {
			for _, field := range strings.Fields(header) {
				version, encoded, ok := strings.Cut(field, ",")
				if !ok || version != "v1" {
					continue
				}
				got, err := base64.StdEncoding.DecodeString(encoded)
				if err == nil && hmac.Equal(got, want) {
					return nil
				}
			}
		}
	}
	return errNotVerified
}

// resendStatus is the provider status recorded for each event the decoder applies. Any other event (opened,
// clicked, a domain's, a contact's) is not one a delivery's state depends on, and is ignored and
// acknowledged: a provider told to retry what will never be applied retries it for ever.
var resendStatus = map[string]string{
	"email.sent":             "sent",
	"email.delivered":        "delivered",
	"email.delivery_delayed": "delayed",
	"email.bounced":          "bounced",
	"email.complained":       "complained",
	"email.failed":           "failed",
}

type resendEvent struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

type resendData struct {
	EmailID string   `json:"email_id"`
	To      []string `json:"to"`
	Bounce  *struct {
		Type string `json:"type"`
	} `json:"bounce"`
}

func unreadable(why string) error {
	return &domain.ValidationError{Problems: []domain.Problem{{Field: "body", Message: why}}}
}

// Handle implements ports.Webhook.
func (h *ResendWebhook) Handle(ctx context.Context, body []byte) error {
	var ev resendEvent
	if err := json.Unmarshal(body, &ev); err != nil {
		return unreadable("not a Resend event")
	}
	status, ok := resendStatus[ev.Type]
	if !ok {
		return nil
	}
	var data resendData
	if err := json.Unmarshal(ev.Data, &data); err != nil {
		return unreadable("an event with no data object")
	}
	if data.EmailID == "" {
		return unreadable("an event that names no message")
	}

	reason, until, suppress := domain.SuppressionReason(""), time.Time{}, false
	switch ev.Type {
	case "email.complained":
		reason, suppress = domain.SuppressComplaint, true
	case "email.bounced":
		// Only a provider that says the bounce is permanent gets a permanent answer: a bounce that says
		// nothing, or that it cannot tell, is a cooldown (D14).
		if data.Bounce != nil && strings.EqualFold(data.Bounce.Type, "Permanent") {
			reason, suppress = domain.SuppressHardBounce, true
		} else {
			reason, until, suppress = domain.SuppressSoftBounce, h.now().Add(h.cooldown), true
		}
	}
	// The suppression first: the address is dead whether or not its delivery is recorded yet, and a callback
	// that is retried because the delivery was not there repeats an idempotent write.
	if suppress {
		for _, to := range data.To {
			if address := normalizeEmail(to); address != "" {
				if err := h.suppress.Suppress(ctx, h.realm, Kind, domain.SuppressionHash(Kind, address), reason, until); err != nil {
					return fmt.Errorf("email: suppress an address after a %s: %w", ev.Type, err)
				}
			}
		}
	}
	return h.status.RecordProviderStatus(ctx, Kind, data.EmailID, status)
}
