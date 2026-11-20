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
	// ErrNotFound is returned by the inbox when a notification is not one of the
	// caller's: it does not exist, or belongs to someone else, or is not in their
	// inbox. The two are never told apart, so an id cannot be probed for existence
	// (NR-008); the REST surface answers it with a 404.
	ErrNotFound = errors.New("notify: no such notification for this recipient")
	// ErrInvalidCursor is returned for a list cursor the store did not issue. A
	// cursor is client input, so a bad one is a request error, not a fault.
	ErrInvalidCursor = errors.New("notify: invalid list cursor")
	// ErrUnknownDeadLetter is returned when a dead letter is replayed by an id that names
	// none in the realm: it does not exist, or it is another realm's, and the two are
	// not told apart. A malformed id is the same (NR-007).
	ErrUnknownDeadLetter = errors.New("notify: no such dead letter")
	// ErrNotReplayable is returned for a dead letter that holds no queue row to re-drive: one
	// parked by the bus ingest (reason poison), which the producer must resend (NR-007).
	ErrNotReplayable = errors.New("notify: this dead letter has no delivery to replay")
	// ErrTooManyRecipients is returned for a broadcast whose inline recipient list is
	// longer than Config.MaxInlineRecipients: a bigger audience is a selector the host
	// resolves in pages, not a list the request carries (NR-020, D31).
	ErrTooManyRecipients = errors.New("notify: an inline recipient list is over the limit; use an audience selector")
	// ErrAudienceUnsupported is returned for a broadcast that names an audience selector
	// when the engine was built with no AudienceResolver, so it can only expand an inline
	// recipient list (D7, D31).
	ErrAudienceUnsupported = errors.New("notify: no AudienceResolver is configured, so a broadcast takes an inline recipient list")
	// ErrBroadcastLeaseLost is returned by CommitBroadcastPage when the lease the page was
	// expanded under is no longer the broadcast's: it lapsed and another worker took the
	// page, or the broadcast is done. Nothing was written. The worker that meets it does
	// not retry the page, because the one that holds the lease will (NR-020, D7).
	ErrBroadcastLeaseLost = errors.New("notify: the broadcast's lease was lost; the page was not committed")
	// ErrUnknownChannel is returned when a preference names a channel nobody registered: a pair that
	// nothing would ever deliver on, which a recipient would take for a choice that was saved (NR-016).
	ErrUnknownChannel = errors.New("notify: channel is not registered")
	// ErrPreferenceLocked is returned for a write to a (topic, channel) pair that is not the writer's to
	// change: an essential topic's own channel, which can never be disabled (NR-015), or a pair a
	// tenant locked (D30). It is refused with its reason and never ignored, so a settings screen cannot
	// show a switch that does nothing.
	ErrPreferenceLocked = errors.New("notify: this preference cannot be changed")
	// ErrPermanent is wrapped by a driven port to say a fault is one no retry can fix:
	// a Redis that refuses the command for this user, or does not have it. A channel
	// that meets it reports Rejected rather than Retry, so the delivery is dead-lettered
	// with its reason at once instead of burning every attempt first (D10, D12). An
	// error that does not wrap it is an infrastructure fault, which is Retry.
	ErrPermanent = errors.New("notify: the fault is permanent; retrying cannot fix it")
)
