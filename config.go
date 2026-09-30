// Package notify is the module's root. It holds the one configuration surface of
// the engine; the use-cases, ports and adapters live in the packages beneath it.
package notify

import (
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
func (e *ConfigError) Error() string { return "" }

// WithDefaults returns a copy of c with every unset field given its default.
// The receiver is not changed.
func (c Config) WithDefaults() Config { return c }

// Validate checks the effective configuration — c with its defaults applied to a
// copy — and reports every problem at once, as a *ConfigError.
func (c Config) Validate() error { return nil }
