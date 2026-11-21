package email

import (
	"context"
	"errors"
	"time"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

var errNotImplemented = errors.New("email: not implemented")

// StatusRecorder records a provider's delivery status against the delivery it names (ports.Store).
type StatusRecorder interface {
	RecordProviderStatus(ctx context.Context, ch domain.ChannelKind, providerMessageID, status string) error
}

// ResendWebhook is a stub.
type ResendWebhook struct{}

var _ ports.Webhook = (*ResendWebhook)(nil)

// ResendWebhookOption configures a ResendWebhook.
type ResendWebhookOption func(*ResendWebhook)

// WithWebhookClock sets the clock.
func WithWebhookClock(func() time.Time) ResendWebhookOption { return func(*ResendWebhook) {} }

// WithSoftBounceCooldown sets the cooldown.
func WithSoftBounceCooldown(time.Duration) ResendWebhookOption { return func(*ResendWebhook) {} }

// NewResendWebhook is a stub.
func NewResendWebhook(realm domain.Realm, secrets []string, rec StatusRecorder, sup ports.SuppressionStore, opts ...ResendWebhookOption) (*ResendWebhook, error) {
	return &ResendWebhook{}, nil
}

// Verify is a stub.
func (*ResendWebhook) Verify(map[string][]string, []byte) error { return errNotImplemented }

// Handle is a stub.
func (*ResendWebhook) Handle(context.Context, []byte) error { return nil }
