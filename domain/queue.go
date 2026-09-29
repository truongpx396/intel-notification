package domain

import (
	"fmt"
	"time"
)

// Priority orders and styles a notification: "info" | "warning" | "critical".
type Priority string

// Topic is a registered notification type — a string, never a database enum.
type Topic string

// Address is where one channel delivers. A recipient may have several per
// channel — every device — and each becomes its own delivery (D25).
type Address struct {
	Channel  ChannelKind       `json:"channel"`
	Value    string            `json:"value"`
	Locale   string            `json:"locale,omitempty"`
	Timezone string            `json:"timezone,omitempty"`
	Meta     map[string]string `json:"meta,omitempty"`
}

// OutboxEntry is one pending delivery as a claimer sees it.
type OutboxEntry struct {
	ID                    string
	NotificationID        string // "" for a digest
	NotificationCreatedAt time.Time
	DigestID              string // "" for a notification
	Identity              Identity
	Topic                 Topic
	Priority              Priority
	Channel               ChannelKind
	AddressKey            string   // "" until bound
	Address               *Address // nil until bound
	Fallback              []ChannelKind
	Shard                 Shard
	Attempts              int // counted at claim (D19)
	Deferrals             int
	DeliverBefore         *time.Time
	QuietHoursOverride    bool
}

// Claim is an entry held under a lease. Every state change presents LeaseToken;
// a change carrying a stale token is discarded (D19).
type Claim struct {
	Entry      OutboxEntry
	LeaseToken string
}

// TerminalOutcome is why a delivery finished: the vocabulary of
// notification_deliveries.outcome (D12).
type TerminalOutcome string

const (
	OutcomeDelivered    TerminalOutcome = "delivered"
	OutcomeSuppressed   TerminalOutcome = "suppressed"
	OutcomeNoAddress    TerminalOutcome = "no_address"
	OutcomeMaxAttempts  TerminalOutcome = "max_attempts"
	OutcomeRejected     TerminalOutcome = "rejected"
	OutcomeExpired      TerminalOutcome = "expired"
	OutcomeCanceled     TerminalOutcome = "canceled"
	OutcomeDroppedQuota TerminalOutcome = "dropped_quota"
	// OutcomeFannedOut records a delivery that split into one row per address.
	// Only address binding records it; a claimer cannot finish with it.
	OutcomeFannedOut TerminalOutcome = "fanned_out"
)

// Disposition is what a terminal outcome means beyond itself.
type Disposition struct {
	// DeadLetter: a genuine failure an operator must see. A correct outcome —
	// a suppressed or missing address, an expiry, a cancel — is not, or the
	// dead-letter alarm pages someone for the engine doing its job (D8).
	DeadLetter bool
	// Fallback: the channel ended undelivered, so the topic's fallback chain
	// continues with its next channel (D29).
	Fallback bool
}

// DispositionOf is the policy for each terminal outcome. It lives here, in pure
// code, and the store only executes it (D37): which failures an operator is paged
// for, and when a fallback channel is tried, are domain rules, not storage ones.
func DispositionOf(o TerminalOutcome) Disposition {
	switch o {
	case OutcomeMaxAttempts, OutcomeRejected:
		return Disposition{DeadLetter: true, Fallback: true}
	case OutcomeSuppressed, OutcomeNoAddress:
		return Disposition{Fallback: true}
	default:
		return Disposition{}
	}
}

// Finish is a terminal state change for one claim: the outcome, and what the
// store must do besides recording it. Build one with [NewFinish] so the
// disposition always comes from [DispositionOf].
type Finish struct {
	Outcome           TerminalOutcome
	ProviderMessageID string
	Detail            string
	Disposition       Disposition
}

// NewFinish returns the finish for outcome o with its policy applied. It rejects
// OutcomeFannedOut and any value outside the vocabulary.
func NewFinish(o TerminalOutcome, providerMessageID, detail string) (Finish, error) {
	switch o {
	case OutcomeDelivered, OutcomeSuppressed, OutcomeNoAddress, OutcomeMaxAttempts,
		OutcomeRejected, OutcomeExpired, OutcomeCanceled, OutcomeDroppedQuota:
		return Finish{Outcome: o, ProviderMessageID: providerMessageID, Detail: detail,
			Disposition: DispositionOf(o)}, nil
	case OutcomeFannedOut:
		return Finish{}, fmt.Errorf("%s is recorded by address binding, not by a finish", o)
	default:
		return Finish{}, fmt.Errorf("unknown terminal outcome %q", o)
	}
}

// Binding reports how a claim was bound to its addresses (D25).
type Binding struct {
	// Addresses is how many addresses were bound.
	Addresses int
	// StillHeld: one address was bound in place and the caller still holds the
	// claim, so it goes on to deliver. False after a fan-out: the claim is spent
	// and each address is a new, unleased delivery.
	StillHeld bool
}

// CancelReceipt is the outcome of canceling one notification (D29).
type CancelReceipt struct {
	Matched  bool
	Canceled int // pending deliveries stopped
	InFlight int // under a live lease: may already be at the provider
}

// DigestMember is one notification folded into a digest window (D4).
type DigestMember struct {
	Identity       Identity
	Topic          Topic
	Channel        ChannelKind
	Shard          Shard
	NotificationID string
}
