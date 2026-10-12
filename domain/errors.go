package domain

import "errors"

// Sentinel errors a caller can match with errors.Is. They are the contract's:
// the ports document which method returns which.
var (
	// ErrTxUnsupported is returned by a Notifier that cannot join a caller's
	// transaction, such as the remote one (D28).
	ErrTxUnsupported = errors.New("notify: this Notifier cannot join a caller's transaction")
	// ErrNoTemplate is returned by a TemplateRenderer when nothing matches after
	// locale fallback; the dispatcher then uses the notification's fallback copy.
	ErrNoTemplate = errors.New("notify: no template for topic, channel and locale")
	// ErrUnknownTopic is returned when a topic is not registered (NR-017).
	ErrUnknownTopic = errors.New("notify: topic is not registered")
	// ErrMissingIdem is returned for a notification or broadcast with no IdemKey:
	// without one there is no exactly-once identity (NR-002).
	ErrMissingIdem = errors.New("notify: IdemKey is required")
	// ErrUnknownDelivery is returned by RecordProviderStatus when no delivery on
	// the channel carries the provider message id: a callback for a message that
	// was never recorded, or one that arrived before the delivery's outcome did.
	// The caller decides whether to ask the provider to try again (NR-032).
	ErrUnknownDelivery = errors.New("notify: no delivery with that provider message id")
)
