// Package notify is the module's root. It holds the one configuration surface of
// the engine; the use-cases, ports and adapters live in the packages beneath it.
package notify

import (
	"fmt"
	"strings"
	"time"

	"github.com/truongpx396/intel-notification/domain"
)

// Config is the entire configuration surface of the engine (D1, D21). The core
// reads no environment variable and no global setting; adapters dial what they
// are given. A zero value means "use the default", so a field is overridden by
// setting it, and Validate judges the effective configuration — the one the
// engine will run with — not the literal struct.
//
// Realm, StoreDSN and RedisURL have no default: a wrong-but-plausible one would
// silently merge two products' key spaces or point at someone else's database.
type Config struct {
	// Realm is required in library mode and must match ^[a-z0-9][a-z0-9-]{0,62}$.
	// Service mode takes realms from its producer bindings instead (D31).
	Realm domain.Realm

	StoreDSN string // PostgreSQL
	RedisURL string // pre-check, live stream, quota counters

	Shards       int           // 16. Changeable online (D11)
	ClaimLease   time.Duration // 5m. Must exceed the slowest channel's send timeout
	ClaimBatch   int           // 100
	PollInterval time.Duration // 500ms: the idle ceiling on claim latency (D22)
	ListenNotify bool          // false. Transactional NOTIFY wakeups; see D22 before enabling

	MaxAttempts    int           // 5
	BackoffBase    time.Duration // 10s
	BackoffCeiling time.Duration // 1h

	IdempotencyWindow time.Duration // 7d; at least 24h (D21)
	PreCheckTTL       time.Duration // 24h; at most IdempotencyWindow (D18)

	RetentionWindow      time.Duration // 90d: inbox partitions, read and unread (D33)
	DeliveryLogRetention time.Duration // 30d
	DeadLetterRetention  time.Duration // 30d

	DigestMax           int    // 100
	UnreadCap           int    // 99
	DefaultLocale       string // "en"
	MaxInlineRecipients int    // 1000
}

// ConfigProblem is one rule a configuration breaks, named by the field at fault.
type ConfigProblem struct {
	Field   string
	Message string
}

// ConfigError is every problem Validate found, so a misconfigured deployment
// fails at startup with the complete list rather than one rule per restart.
type ConfigError struct {
	Problems []ConfigProblem
}

// Error implements error.
func (e *ConfigError) Error() string {
	var b strings.Builder
	b.WriteString("invalid configuration:")
	for _, p := range e.Problems {
		b.WriteString("\n  ")
		b.WriteString(p.Field)
		b.WriteString(": ")
		b.WriteString(p.Message)
	}
	return b.String()
}

// minIdempotencyWindow is the shortest window a key can guard replays for. A
// producer's retries and a bus's redeliveries outlive anything shorter, and the
// industry norm is the same (D21).
const minIdempotencyWindow = 24 * time.Hour

// WithDefaults returns a copy of c with every unset field given its default.
// The receiver is not changed: Config holds no slice, map or pointer, so the
// copy shares nothing with it.
func (c Config) WithDefaults() Config {
	c.Shards = orDefault(c.Shards, 16)
	c.ClaimLease = orDefault(c.ClaimLease, 5*time.Minute)
	c.ClaimBatch = orDefault(c.ClaimBatch, 100)
	c.PollInterval = orDefault(c.PollInterval, 500*time.Millisecond)
	c.MaxAttempts = orDefault(c.MaxAttempts, 5)
	c.BackoffBase = orDefault(c.BackoffBase, 10*time.Second)
	c.BackoffCeiling = orDefault(c.BackoffCeiling, time.Hour)
	c.IdempotencyWindow = orDefault(c.IdempotencyWindow, 7*24*time.Hour)
	c.PreCheckTTL = orDefault(c.PreCheckTTL, 24*time.Hour)
	c.RetentionWindow = orDefault(c.RetentionWindow, 90*24*time.Hour)
	c.DeliveryLogRetention = orDefault(c.DeliveryLogRetention, 30*24*time.Hour)
	c.DeadLetterRetention = orDefault(c.DeadLetterRetention, 30*24*time.Hour)
	c.DigestMax = orDefault(c.DigestMax, 100)
	c.UnreadCap = orDefault(c.UnreadCap, 99)
	c.DefaultLocale = orDefault(c.DefaultLocale, "en")
	c.MaxInlineRecipients = orDefault(c.MaxInlineRecipients, 1000)
	return c
}

// orDefault returns v, or def when v is the zero value.
func orDefault[T comparable](v, def T) T {
	var zero T
	if v == zero {
		return def
	}
	return v
}

// Validate checks the effective configuration — c with its defaults applied to a
// copy — and reports every problem at once, as a *ConfigError. Judging the
// effective config matters for the rules that compare two fields: a BackoffBase
// of two hours is wrong against the default one-hour ceiling although the
// ceiling was never set.
func (c Config) Validate() error {
	e := c.WithDefaults()
	var ce ConfigError
	add := func(field, format string, args ...any) {
		ce.Problems = append(ce.Problems, ConfigProblem{Field: field, Message: fmt.Sprintf(format, args...)})
	}

	if err := e.Realm.Validate(); err != nil {
		add("Realm", "%v", err)
	}
	if e.StoreDSN == "" {
		add("StoreDSN", "is required")
	}
	if e.RedisURL == "" {
		add("RedisURL", "is required")
	}

	for _, n := range []struct {
		field string
		value int
	}{
		{"Shards", e.Shards},
		{"ClaimBatch", e.ClaimBatch},
		{"MaxAttempts", e.MaxAttempts},
		{"DigestMax", e.DigestMax},
		{"UnreadCap", e.UnreadCap},
		{"MaxInlineRecipients", e.MaxInlineRecipients},
	} {
		if n.value < 1 {
			add(n.field, "must be at least 1, got %d", n.value)
		}
	}

	for _, d := range []struct {
		field string
		value time.Duration
	}{
		{"ClaimLease", e.ClaimLease},
		{"PollInterval", e.PollInterval},
	} {
		if d.value <= 0 {
			add(d.field, "must be positive, got %s", d.value)
		}
	}

	if e.BackoffCeiling < e.BackoffBase {
		add("BackoffCeiling", "%s is below BackoffBase %s, so the first retry interval would exceed its own cap",
			e.BackoffCeiling, e.BackoffBase)
	}
	if e.IdempotencyWindow < minIdempotencyWindow {
		add("IdempotencyWindow", "must be at least %s, got %s", minIdempotencyWindow, e.IdempotencyWindow)
	}
	if e.PreCheckTTL > e.IdempotencyWindow {
		add("PreCheckTTL", "%s outlives IdempotencyWindow %s, so the pre-check could drop a notification the guard would accept",
			e.PreCheckTTL, e.IdempotencyWindow)
	}

	if len(ce.Problems) > 0 {
		return &ce
	}
	return nil
}
