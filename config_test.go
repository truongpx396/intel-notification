package notify

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/truongpx396/intel-notification/domain"
)

// validConfig sets every field to a legal value that is not its default, so a
// row that changes one field shows that field's rule and nothing else.
func validConfig() Config {
	return Config{
		Realm:    "aisat",
		StoreDSN: "postgres://owner@db/notify",
		RedisURL: "redis://cache:6379",

		Shards:       8,
		ClaimLease:   2 * time.Minute,
		ClaimBatch:   50,
		PollInterval: time.Second,
		ListenNotify: true,

		MaxAttempts:    3,
		BackoffBase:    5 * time.Second,
		BackoffCeiling: 30 * time.Minute,

		IdempotencyWindow: 48 * time.Hour,
		PreCheckTTL:       12 * time.Hour,

		RetentionWindow:      24 * time.Hour,
		DeliveryLogRetention: 12 * time.Hour,
		DeadLetterRetention:  6 * time.Hour,

		DigestMax:           20,
		UnreadCap:           9,
		DefaultLocale:       "fr",
		MaxInlineRecipients: 10,
	}
}

// minimalConfig is what a host must set: the three fields with no default.
func minimalConfig() Config {
	return Config{Realm: "aisat", StoreDSN: "postgres://owner@db/notify", RedisURL: "redis://cache:6379"}
}

// fieldsOf returns the fields a Validate error names, sorted, or nil for no error.
func fieldsOf(t *testing.T, err error) []string {
	t.Helper()
	if err == nil {
		return nil
	}
	var ce *ConfigError
	if !errors.As(err, &ce) {
		t.Fatalf("Validate returned %T (%v), want a *ConfigError", err, err)
	}
	if len(ce.Problems) == 0 {
		t.Fatal("a *ConfigError with no problems: a clean config must be a nil error")
	}
	fields := make([]string, 0, len(ce.Problems))
	for _, p := range ce.Problems {
		if p.Message == "" {
			t.Fatalf("a problem with no message for field %s", p.Field)
		}
		fields = append(fields, p.Field)
	}
	slices.Sort(fields)
	return fields
}

// Every rule in the contract's list (notification-ports.md § config.go), one row
// each. A rejection row changes exactly one field of a valid config and names the
// field it expects to be blamed; a boundary row sits on each side of a limit.
func TestConfigValidate(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		config func() Config
		want   []string // the fields blamed, sorted; nil for a valid config
		why    string
	}{
		// The starting points.
		{"a fully specified config", validConfig, nil, ""},
		{"only the three required fields", minimalConfig, nil, "every other field has a default"},

		// Realm: ^[a-z0-9][a-z0-9-]{0,62}$, and no default (D1).
		{"empty realm", func() Config { c := validConfig(); c.Realm = ""; return c }, []string{"Realm"},
			"a default realm would silently merge two products' key spaces"},
		{"realm at 63 characters", func() Config { c := validConfig(); c.Realm = realmOf(63); return c }, nil, "the longest allowed"},
		{"realm at 64 characters", func() Config { c := validConfig(); c.Realm = realmOf(64); return c }, []string{"Realm"}, "one past the longest"},
		{"realm of one character", func() Config { c := validConfig(); c.Realm = "a"; return c }, nil, ""},
		{"realm starting with a digit", func() Config { c := validConfig(); c.Realm = "9lives"; return c }, nil, ""},
		{"realm with a hyphen inside", func() Config { c := validConfig(); c.Realm = "my-product"; return c }, nil, ""},
		{"realm with a leading hyphen", func() Config { c := validConfig(); c.Realm = "-aisat"; return c }, []string{"Realm"}, ""},
		{"realm with an uppercase letter", func() Config { c := validConfig(); c.Realm = "Aisat"; return c }, []string{"Realm"},
			"a realm is a bus subject token and a Redis key segment, compared byte for byte"},
		{"realm with an underscore", func() Config { c := validConfig(); c.Realm = "ai_sat"; return c }, []string{"Realm"}, ""},
		{"realm with a dot", func() Config { c := validConfig(); c.Realm = "ai.sat"; return c }, []string{"Realm"}, "a dot would split a bus subject"},

		// The two connection strings have no default either.
		{"empty StoreDSN", func() Config { c := validConfig(); c.StoreDSN = ""; return c }, []string{"StoreDSN"}, ""},
		{"empty RedisURL", func() Config { c := validConfig(); c.RedisURL = ""; return c }, []string{"RedisURL"}, ""},

		// A count below 1. Zero is not one of them: it means "the default", which the
		// effective config has already applied.
		{"Shards below 1", func() Config { c := validConfig(); c.Shards = -1; return c }, []string{"Shards"}, ""},
		{"Shards of exactly 1", func() Config { c := validConfig(); c.Shards = 1; return c }, nil, "one shard is a valid, if unparallel, setting"},
		{"ClaimBatch below 1", func() Config { c := validConfig(); c.ClaimBatch = -1; return c }, []string{"ClaimBatch"}, ""},
		{"ClaimBatch of exactly 1", func() Config { c := validConfig(); c.ClaimBatch = 1; return c }, nil, ""},
		{"MaxAttempts below 1", func() Config { c := validConfig(); c.MaxAttempts = -1; return c }, []string{"MaxAttempts"}, ""},
		{"MaxAttempts of exactly 1", func() Config { c := validConfig(); c.MaxAttempts = 1; return c }, nil, "no retries, but a delivery is still attempted"},
		{"DigestMax below 1", func() Config { c := validConfig(); c.DigestMax = -1; return c }, []string{"DigestMax"}, ""},
		{"DigestMax of exactly 1", func() Config { c := validConfig(); c.DigestMax = 1; return c }, nil, ""},
		{"UnreadCap below 1", func() Config { c := validConfig(); c.UnreadCap = -1; return c }, []string{"UnreadCap"}, ""},
		{"UnreadCap of exactly 1", func() Config { c := validConfig(); c.UnreadCap = 1; return c }, nil, ""},
		{"MaxInlineRecipients below 1", func() Config { c := validConfig(); c.MaxInlineRecipients = -1; return c }, []string{"MaxInlineRecipients"}, ""},
		{"MaxInlineRecipients of exactly 1", func() Config { c := validConfig(); c.MaxInlineRecipients = 1; return c }, nil, ""},

		// A duration that must be positive.
		{"ClaimLease below zero", func() Config { c := validConfig(); c.ClaimLease = -time.Nanosecond; return c }, []string{"ClaimLease"}, ""},
		{"ClaimLease of one nanosecond", func() Config { c := validConfig(); c.ClaimLease = time.Nanosecond; return c }, nil, "positive is the whole rule"},
		{"PollInterval below zero", func() Config { c := validConfig(); c.PollInterval = -time.Nanosecond; return c }, []string{"PollInterval"}, ""},
		{"PollInterval of one nanosecond", func() Config { c := validConfig(); c.PollInterval = time.Nanosecond; return c }, nil, ""},

		// BackoffCeiling < BackoffBase.
		{"ceiling one nanosecond under the base",
			func() Config {
				c := validConfig()
				c.BackoffBase = 10 * time.Second
				c.BackoffCeiling = 10*time.Second - 1
				return c
			},
			[]string{"BackoffCeiling"}, "the computed interval would be capped below its own first step"},
		{"ceiling equal to the base",
			func() Config {
				c := validConfig()
				c.BackoffBase = 10 * time.Second
				c.BackoffCeiling = 10 * time.Second
				return c
			},
			nil, "a flat backoff is legal"},

		// IdempotencyWindow >= 24h (D21).
		{"window one nanosecond under 24h",
			func() Config { c := validConfig(); c.IdempotencyWindow = 24*time.Hour - 1; return c },
			[]string{"IdempotencyWindow"}, "a producer's retries and a bus's redeliveries outlive anything shorter"},
		{"window of exactly 24h",
			func() Config { c := validConfig(); c.IdempotencyWindow = 24 * time.Hour; return c }, nil, "the floor"},
		{"a negative window", func() Config { c := validConfig(); c.IdempotencyWindow = -time.Hour; return c },
			[]string{"IdempotencyWindow", "PreCheckTTL"}, "also shorter than the pre-check's TTL"},

		// PreCheckTTL <= IdempotencyWindow (D18).
		{"pre-check TTL equal to the window",
			func() Config {
				c := validConfig()
				c.IdempotencyWindow = 48 * time.Hour
				c.PreCheckTTL = 48 * time.Hour
				return c
			},
			nil, "the pre-check may live as long as the guard"},
		{"pre-check TTL one nanosecond over the window",
			func() Config {
				c := validConfig()
				c.IdempotencyWindow = 48 * time.Hour
				c.PreCheckTTL = 48*time.Hour + 1
				return c
			},
			[]string{"PreCheckTTL"}, "a pre-check that outlives the guard would drop a notification the guard would accept"},
		{"pre-check TTL above the window",
			func() Config {
				c := validConfig()
				c.IdempotencyWindow = 25 * time.Hour
				c.PreCheckTTL = 72 * time.Hour
				return c
			},
			[]string{"PreCheckTTL"}, ""},

		// The effective config is what is judged: a field left unset takes its default,
		// and the rule is applied to that.
		{"a pre-check TTL above the default window but under an explicit one",
			func() Config { c := minimalConfig(); c.PreCheckTTL = 30 * time.Hour; return c }, nil, "the window defaults to 7d"},
		{"a window at 24h against the default pre-check TTL of 24h",
			func() Config { c := minimalConfig(); c.IdempotencyWindow = 24 * time.Hour; return c }, nil, "24h <= 24h"},
		{"a window under 24h against the default pre-check TTL",
			func() Config { c := minimalConfig(); c.IdempotencyWindow = 12 * time.Hour; return c },
			[]string{"IdempotencyWindow", "PreCheckTTL"}, "the pre-check TTL defaults to 24h, over a 12h window"},
		{"a base above the default ceiling",
			func() Config { c := minimalConfig(); c.BackoffBase = 2 * time.Hour; return c },
			[]string{"BackoffCeiling"}, "the ceiling defaults to 1h"},
		{"a ceiling under the default base",
			func() Config { c := minimalConfig(); c.BackoffCeiling = 5 * time.Second; return c },
			[]string{"BackoffCeiling"}, "the base defaults to 10s"},

		// Every problem is reported at once.
		{"several rules broken together",
			func() Config {
				c := validConfig()
				c.Realm = "Bad Realm"
				c.StoreDSN = ""
				c.RedisURL = ""
				c.Shards = -1
				c.ClaimLease = -time.Second
				c.BackoffCeiling = time.Second
				c.IdempotencyWindow = time.Hour
				return c
			},
			[]string{"BackoffCeiling", "ClaimLease", "IdempotencyWindow", "PreCheckTTL", "Realm", "RedisURL", "Shards", "StoreDSN"},
			"a deployment fixes its configuration in one pass, not one restart per rule"},
		{"every count and duration rule broken together",
			func() Config {
				c := validConfig()
				c.Shards, c.ClaimBatch, c.MaxAttempts, c.DigestMax, c.UnreadCap, c.MaxInlineRecipients = -1, -1, -1, -1, -1, -1
				c.ClaimLease, c.PollInterval = -1, -1
				return c
			},
			[]string{"ClaimBatch", "ClaimLease", "DigestMax", "MaxAttempts", "MaxInlineRecipients", "PollInterval", "Shards", "UnreadCap"},
			""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := tc.config()
			got := fieldsOf(t, c.Validate())
			if !slices.Equal(got, tc.want) {
				t.Fatalf("Validate blamed %v, want %v: %s\n config: %+v", got, tc.want, tc.why, c)
			}
		})
	}
}

// A ConfigError names every problem in its message, so the operator reading a
// startup failure sees them all.
func TestConfigErrorMessage(t *testing.T) {
	t.Parallel()
	c := validConfig()
	c.StoreDSN = ""
	c.Shards = -1
	err := c.Validate()
	if err == nil {
		t.Fatal("Validate accepted a config with an empty StoreDSN and negative Shards")
	}
	for _, field := range []string{"StoreDSN", "Shards"} {
		if !strings.Contains(err.Error(), field) {
			t.Errorf("the error message %q does not name %s", err.Error(), field)
		}
	}
}

// Defaults are the values docs/configuration.md publishes. They are written out as
// literals here rather than read from the code, so changing one is a visible
// change to a published default.
func TestConfigWithDefaults(t *testing.T) {
	t.Parallel()
	got := Config{}.WithDefaults()
	want := Config{
		Shards:               16,
		ClaimLease:           5 * time.Minute,
		ClaimBatch:           100,
		PollInterval:         500 * time.Millisecond,
		ListenNotify:         false,
		MaxAttempts:          5,
		BackoffBase:          10 * time.Second,
		BackoffCeiling:       time.Hour,
		IdempotencyWindow:    168 * time.Hour,
		PreCheckTTL:          24 * time.Hour,
		RetentionWindow:      2160 * time.Hour,
		DeliveryLogRetention: 720 * time.Hour,
		DeadLetterRetention:  720 * time.Hour,
		DigestMax:            100,
		UnreadCap:            99,
		DefaultLocale:        "en",
		MaxInlineRecipients:  1000,
	}
	if got != want {
		t.Fatalf("Config{}.WithDefaults() =\n%+v\nwant\n%+v", got, want)
	}
}

func TestConfigWithDefaultsKeepsWhatIsSet(t *testing.T) {
	t.Parallel()
	set := validConfig()
	if got := set.WithDefaults(); got != set {
		t.Fatalf("a fully specified config changed:\n got %+v\nwant %+v", got, set)
	}

	// One field at a time: the others take their defaults, this one keeps its value.
	c := Config{Shards: 4}.WithDefaults()
	if c.Shards != 4 || c.ClaimBatch != 100 {
		t.Fatalf("Shards = %d, ClaimBatch = %d, want 4 and 100", c.Shards, c.ClaimBatch)
	}
	if !(Config{ListenNotify: true}.WithDefaults().ListenNotify) {
		t.Fatal("ListenNotify: true was lost to the default")
	}
	if d := (Config{}).WithDefaults(); d.Realm != "" || d.StoreDSN != "" || d.RedisURL != "" {
		t.Fatalf("a default was invented for a field that must be set: %+v", d)
	}
}

// Defaults are applied to a copy. Validate and WithDefaults must leave the
// caller's value as it was, so a config can be validated, logged and passed on
// without the engine's defaults having leaked into it.
func TestConfigDefaultsAreAppliedToACopy(t *testing.T) {
	t.Parallel()
	c := minimalConfig()
	before := c

	if err := c.Validate(); err != nil {
		t.Fatalf("a minimal config is valid, got %v", err)
	}
	if c != before {
		t.Fatalf("Validate changed the caller's config:\n got %+v\nwant %+v", c, before)
	}

	eff := c.WithDefaults()
	if c != before {
		t.Fatalf("WithDefaults changed the caller's config:\n got %+v\nwant %+v", c, before)
	}
	if eff.Shards == 0 {
		t.Fatal("WithDefaults returned a config with no defaults applied")
	}
	eff.Shards = 99
	if c.Shards != 0 {
		t.Fatal("the returned config is not a copy: writing to it changed the original")
	}
}

func realmOf(n int) domain.Realm {
	return domain.Realm(strings.Repeat("a", n))
}
