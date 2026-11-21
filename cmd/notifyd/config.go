package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	notify "github.com/truongpx396/intel-notification"
	"github.com/truongpx396/intel-notification/domain"
)

// Config is everything the service reads from its environment (docs/configuration.md). The engine's own
// settings are notify.Config; the rest is what only a service has: who may call it and as which realm,
// where its tokens are verified, which providers' credentials it may read, and where it listens.
// Provider credentials are never here: channel_providers rows reference them (D31).
type Config struct {
	Engine notify.Config

	// HTTPAddr serves the recipient REST surface, the SSE relay, the operator routes and health.
	HTTPAddr string
	// GRPCAddr serves the producer and management surface. It is a capability to send as a product, so it
	// belongs on an internal network.
	GRPCAddr string
	// PublicURL is the address the REST surface has outside, where an unsubscribe link in an email points.
	PublicURL string

	Bindings      domain.RealmBindings
	RecipientJWKS map[domain.Realm]string
	// AllowInsecureHTTP lets the JWKS URLs and the public URL be plain HTTP: development and the e2e stack.
	// Keys read in the clear can be replaced by anyone on the path, who could then mint tokens for any recipient.
	AllowInsecureHTTP bool
	RecipientAud      string
	ProducerAud       string
	OperatorTokens    []OperatorToken
	UnsubscribeKeys   []KeyRef // the first signs; all verify
	DirectoryAddrs    map[domain.Realm]string
	DirectoryTLS      bool
	GRPCTLS           TLSFiles
	NATSURL           string
	// JWKSTTL and JWKSMinRefetch are how long a realm's keys are used, and the shortest interval between two
	// fetches of them (an unknown key id, a signature that stops verifying, a retry after a failure).
	JWKSTTL, JWKSMinRefetch time.Duration
	// NATSReplicas is the replication of the ingest stream. JetStream with a replicated stream (R3) is what
	// makes an acknowledged publish survive a node (bus-subjects.md); one replica is for development.
	NATSReplicas int

	// Role is which loops this process runs: "all", "api" (the surfaces only) or "worker" (the dispatcher
	// and the jobs only). Any number of each is safe: claims are leased and fenced.
	Role            string
	ProviderRefresh time.Duration
	// AllowPrivilegedRole lets the service run as a role that bypasses row-level security. It exists for a
	// throwaway development database and nothing else: with it, tenant isolation is not held.
	AllowPrivilegedRole bool
}

// OperatorToken is a bearer token accepted on /admin routes for one realm, as a reference to the secret.
type OperatorToken struct {
	Realm domain.Realm
	Ref   string
}

// KeyRef is a key id and the reference to its secret.
type KeyRef struct{ ID, Ref string }

// TLSFiles are the paths of the gRPC server's certificate and key, and of the CA that signs its clients.
type TLSFiles struct{ Cert, Key, ClientCA string }

// ConfigError is every problem LoadConfig found, so a deployment fails at start with the whole list.
type ConfigError struct{ Problems []string }

func (e *ConfigError) Error() string {
	return "invalid configuration:\n  " + strings.Join(e.Problems, "\n  ")
}

// LoadConfig reads the service's configuration from getenv, reporting every problem at once.
func LoadConfig(getenv func(string) string) (Config, error) {
	var (
		c        Config
		problems []string
	)
	bad := func(name, format string, args ...any) {
		problems = append(problems, name+": "+fmt.Sprintf(format, args...))
	}
	str := func(name, def string) string {
		if v := strings.TrimSpace(getenv(name)); v != "" {
			return v
		}
		return def
	}
	dur := func(name string, into *time.Duration) {
		if v := str(name, ""); v != "" {
			d, err := time.ParseDuration(v)
			if err != nil {
				bad(name, "%q is not a duration such as 30s or 168h", v)
				return
			}
			*into = d
		}
	}
	num := func(name string, into *int) {
		if v := str(name, ""); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil {
				bad(name, "%q is not a whole number", v)
				return
			}
			*into = n
		}
	}
	flag := func(name string) bool {
		switch strings.ToLower(str(name, "")) {
		case "":
			return false
		case "1", "true", "yes":
			return true
		case "0", "false", "no":
			return false
		}
		bad(name, "%q is not true or false", getenv(name))
		return false
	}

	e := &c.Engine
	e.StoreDSN = str("NOTIFY_PG_DSN", "")
	e.RedisURL = str("NOTIFY_REDIS_URL", "")
	num("NOTIFY_SHARDS", &e.Shards)
	num("NOTIFY_CLAIM_BATCH", &e.ClaimBatch)
	num("NOTIFY_MAX_ATTEMPTS", &e.MaxAttempts)
	num("NOTIFY_DIGEST_MAX", &e.DigestMax)
	num("NOTIFY_UNREAD_CAP", &e.UnreadCap)
	num("NOTIFY_MAX_INLINE_RECIPIENTS", &e.MaxInlineRecipients)
	dur("NOTIFY_CLAIM_LEASE", &e.ClaimLease)
	dur("NOTIFY_POLL_INTERVAL", &e.PollInterval)
	dur("NOTIFY_BACKOFF_BASE", &e.BackoffBase)
	dur("NOTIFY_BACKOFF_CEILING", &e.BackoffCeiling)
	dur("NOTIFY_IDEMPOTENCY_WINDOW", &e.IdempotencyWindow)
	dur("NOTIFY_PRECHECK_TTL", &e.PreCheckTTL)
	dur("NOTIFY_RETENTION_WINDOW", &e.RetentionWindow)
	dur("NOTIFY_DELIVERY_LOG_RETENTION", &e.DeliveryLogRetention)
	dur("NOTIFY_DEAD_LETTER_RETENTION", &e.DeadLetterRetention)
	e.DefaultLocale = str("NOTIFY_DEFAULT_LOCALE", "")
	e.ListenNotify = flag("NOTIFY_LISTEN_NOTIFY")

	c.HTTPAddr = str("NOTIFY_HTTP_ADDR", ":8080")
	c.GRPCAddr = str("NOTIFY_GRPC_ADDR", ":9090")
	c.PublicURL = strings.TrimRight(str("NOTIFY_PUBLIC_URL", ""), "/")
	c.Role = strings.ToLower(str("NOTIFY_ROLE", "all"))
	if c.Role != "all" && c.Role != "api" && c.Role != "worker" {
		bad("NOTIFY_ROLE", "%q is not all, api or worker", c.Role)
	}
	c.ProviderRefresh = time.Minute
	dur("NOTIFY_PROVIDER_REFRESH", &c.ProviderRefresh)
	c.AllowInsecureHTTP = flag("NOTIFY_ALLOW_INSECURE_HTTP")
	c.DirectoryTLS = !flag("NOTIFY_DIRECTORY_INSECURE")
	c.AllowPrivilegedRole = flag("NOTIFY_ALLOW_PRIVILEGED_DB_ROLE")
	c.RecipientAud = str("NOTIFY_RECIPIENT_AUDIENCE", "")
	c.ProducerAud = str("NOTIFY_PRODUCER_AUDIENCE", "")
	c.NATSURL = str("NOTIFY_INGEST_NATS_URL", "")
	c.JWKSTTL, c.JWKSMinRefetch = 10*time.Minute, time.Minute
	dur("NOTIFY_JWKS_TTL", &c.JWKSTTL)
	dur("NOTIFY_JWKS_MIN_REFETCH", &c.JWKSMinRefetch)
	if c.JWKSTTL <= 0 || c.JWKSMinRefetch <= 0 {
		bad("NOTIFY_JWKS_TTL", "the keys' ttl and the refetch interval must both be positive")
	}
	c.NATSReplicas = 1
	num("NOTIFY_INGEST_REPLICAS", &c.NATSReplicas)
	if c.NATSReplicas < 1 || c.NATSReplicas > 5 {
		bad("NOTIFY_INGEST_REPLICAS", "%d is not between 1 and 5", c.NATSReplicas)
	}
	c.GRPCTLS = TLSFiles{Cert: str("NOTIFY_GRPC_TLS_CERT", ""), Key: str("NOTIFY_GRPC_TLS_KEY", ""), ClientCA: str("NOTIFY_GRPC_TLS_CLIENT_CA", "")}
	if (c.GRPCTLS.Cert == "") != (c.GRPCTLS.Key == "") {
		bad("NOTIFY_GRPC_TLS_CERT", "the certificate and the key are set together or not at all")
	}
	if c.GRPCTLS.ClientCA != "" && c.GRPCTLS.Cert == "" {
		bad("NOTIFY_GRPC_TLS_CLIENT_CA", "client certificates are verified over TLS, which needs NOTIFY_GRPC_TLS_CERT and NOTIFY_GRPC_TLS_KEY")
	}

	var err error
	if c.Bindings, err = domain.ParseRealmBindings(str("NOTIFY_REALM_BINDINGS", "")); err != nil {
		bad("NOTIFY_REALM_BINDINGS", "%v", err)
	}
	if len(c.Bindings.Realms()) == 0 {
		bad("NOTIFY_REALM_BINDINGS", "is required: a service serves the realms its producers are bound to, and with none no one may call it")
	}
	if c.RecipientJWKS, err = domain.ParseRealmPairs(str("NOTIFY_RECIPIENT_JWKS", "")); err != nil {
		bad("NOTIFY_RECIPIENT_JWKS", "%v", err)
	}
	if c.DirectoryAddrs, err = domain.ParseRealmPairs(str("NOTIFY_DIRECTORY_ADDRS", "")); err != nil {
		bad("NOTIFY_DIRECTORY_ADDRS", "%v", err)
	}
	for _, realm := range c.Bindings.Realms() {
		if _, ok := c.RecipientJWKS[realm]; !ok && c.GRPCTLS.ClientCA == "" {
			bad("NOTIFY_RECIPIENT_JWKS", "realm %q has producers and no way to authenticate them: set its JWKS (producer tokens) or NOTIFY_GRPC_TLS_CLIENT_CA (mTLS)", realm)
		}
	}

	c.OperatorTokens = parseOperatorTokens(str("NOTIFY_OPERATOR_TOKENS", ""), c.Bindings.Realms(), func(msg string) { bad("NOTIFY_OPERATOR_TOKENS", "%s", msg) })
	for _, pair := range splitList(str("NOTIFY_UNSUBSCRIBE_KEYS", "")) {
		id, ref, ok := strings.Cut(pair, "=")
		if !ok || strings.TrimSpace(id) == "" || strings.TrimSpace(ref) == "" {
			bad("NOTIFY_UNSUBSCRIBE_KEYS", "%q is not kid=secret-ref", pair)
			continue
		}
		c.UnsubscribeKeys = append(c.UnsubscribeKeys, KeyRef{ID: strings.TrimSpace(id), Ref: strings.TrimSpace(ref)})
	}

	if realms := c.Bindings.Realms(); len(realms) > 0 {
		// The shared components hold no realm of their own; the configuration only wants a valid one.
		e.Realm = realms[0]
	}
	if verr := e.Validate(); verr != nil {
		var ce *notify.ConfigError
		if asConfigError(verr, &ce) {
			for _, p := range ce.Problems {
				if p.Field == "Realm" {
					continue // the service has none of its own; a missing binding is reported above
				}
				name := engineEnv[p.Field]
				if name == "" {
					name = "engine"
				}
				bad(name, "%s: %s", p.Field, p.Message)
			}
		} else {
			bad("engine", "%v", verr)
		}
	}
	if len(problems) > 0 {
		return Config{}, &ConfigError{Problems: problems}
	}
	return c, nil
}

// engineEnv names the variable each engine setting is read from, so a problem the engine reports is
// reported against the name an operator set.
var engineEnv = map[string]string{
	"StoreDSN": "NOTIFY_PG_DSN", "RedisURL": "NOTIFY_REDIS_URL", "Shards": "NOTIFY_SHARDS", "ClaimBatch": "NOTIFY_CLAIM_BATCH",
	"ClaimLease": "NOTIFY_CLAIM_LEASE", "PollInterval": "NOTIFY_POLL_INTERVAL", "MaxAttempts": "NOTIFY_MAX_ATTEMPTS",
	"BackoffBase": "NOTIFY_BACKOFF_BASE", "BackoffCeiling": "NOTIFY_BACKOFF_CEILING", "IdempotencyWindow": "NOTIFY_IDEMPOTENCY_WINDOW",
	"PreCheckTTL": "NOTIFY_PRECHECK_TTL", "DigestMax": "NOTIFY_DIGEST_MAX", "UnreadCap": "NOTIFY_UNREAD_CAP",
	"MaxInlineRecipients": "NOTIFY_MAX_INLINE_RECIPIENTS",
}

func asConfigError(err error, target **notify.ConfigError) bool { return errors.As(err, target) }

func splitList(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

// parseOperatorTokens reads "realm=ref" entries, or a bare "ref" when exactly one realm is served: a bare
// token administers the one realm there is, and with several a token must say which, so an operator of one
// product never holds the other's.
func parseOperatorTokens(s string, realms []domain.Realm, bad func(string)) []OperatorToken {
	var out []OperatorToken
	for _, entry := range splitList(s) {
		if name, ref, ok := strings.Cut(entry, "="); ok && domain.Realm(name).Validate() == nil {
			if _, _, err := domain.ParseSecretRef(ref); err != nil {
				bad(fmt.Sprintf("the token of realm %q: %v", name, err))
				continue
			}
			out = append(out, OperatorToken{Realm: domain.Realm(name), Ref: ref})
			continue
		}
		if _, _, err := domain.ParseSecretRef(entry); err != nil {
			bad(err.Error())
			continue
		}
		if len(realms) != 1 {
			bad(fmt.Sprintf("%d realms are served, so each operator token says which: realm=ref", len(realms)))
			continue
		}
		out = append(out, OperatorToken{Realm: realms[0], Ref: entry})
	}
	return out
}
