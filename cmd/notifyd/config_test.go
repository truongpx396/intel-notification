package main

import (
	"strings"
	"testing"
	"time"
)

// T049a. The service refuses to start on a configuration that cannot work, with the whole list of what is
// wrong, and reads the one the compose stack and docs/configuration.md describe.

func env(kv map[string]string) func(string) string { return func(k string) string { return kv[k] } }

func good() map[string]string {
	return map[string]string{
		"NOTIFY_PG_DSN":         "postgres://notify_owner@db/notify",
		"NOTIFY_REDIS_URL":      "redis://redis:6379/0",
		"NOTIFY_REALM_BINDINGS": "local-producer=local",
		"NOTIFY_RECIPIENT_JWKS": "local=http://host:8099/jwks.json",
	}
}

func TestLoadConfigReadsTheServicesEnvironment(t *testing.T) {
	t.Parallel()
	kv := good()
	kv["NOTIFY_SHARDS"] = "8"
	kv["NOTIFY_CLAIM_LEASE"] = "2m"
	kv["NOTIFY_IDEMPOTENCY_WINDOW"] = "48h"
	kv["NOTIFY_OPERATOR_TOKENS"] = "env:NOTIFY_OPERATOR_TOKEN"
	kv["NOTIFY_UNSUBSCRIBE_KEYS"] = "k1=env:KEY1, k0=file:/run/secrets/old"
	kv["NOTIFY_ROLE"] = "Worker"
	kv["NOTIFY_PROVIDER_REFRESH"] = "30s"
	kv["NOTIFY_PUBLIC_URL"] = "https://notify.example.test/"
	c, err := LoadConfig(env(kv))
	if err != nil {
		t.Fatal(err)
	}
	if c.Engine.Shards != 8 || c.Engine.ClaimLease != 2*time.Minute || c.Engine.IdempotencyWindow != 48*time.Hour || c.Engine.StoreDSN == "" || c.Engine.RedisURL == "" {
		t.Errorf("engine = %+v, want the environment's settings", c.Engine)
	}
	if c.Role != "worker" || c.ProviderRefresh != 30*time.Second || c.HTTPAddr != ":8080" || c.GRPCAddr != ":9090" {
		t.Errorf("role %q, refresh %v, http %q, grpc %q", c.Role, c.ProviderRefresh, c.HTTPAddr, c.GRPCAddr)
	}
	if c.PublicURL != "https://notify.example.test" {
		t.Errorf("PublicURL = %q, want the trailing slash trimmed", c.PublicURL)
	}
	if realm, ok := c.Bindings.RealmOf("local-producer"); !ok || realm != "local" {
		t.Errorf("binding of local-producer = %q, %v", realm, ok)
	}
	if len(c.OperatorTokens) != 1 || c.OperatorTokens[0].Realm != "local" || c.OperatorTokens[0].Ref != "env:NOTIFY_OPERATOR_TOKEN" {
		t.Errorf("operator tokens = %+v: a bare reference administers the one realm there is", c.OperatorTokens)
	}
	if len(c.UnsubscribeKeys) != 2 || c.UnsubscribeKeys[0].ID != "k1" || c.UnsubscribeKeys[1].ID != "k0" {
		t.Errorf("unsubscribe keys = %+v, want them in order: the first signs", c.UnsubscribeKeys)
	}
	if c.Engine.Realm != "local" {
		t.Errorf("engine realm = %q: the shared components want a valid realm, and there is one", c.Engine.Realm)
	}
}

func TestLoadConfigNamesEveryProblemAtOnce(t *testing.T) {
	t.Parallel()
	_, err := LoadConfig(env(map[string]string{
		"NOTIFY_SHARDS": "many", "NOTIFY_CLAIM_LEASE": "soon", "NOTIFY_ROLE": "boss",
		"NOTIFY_REALM_BINDINGS": "p=Not_A_Realm", "NOTIFY_OPERATOR_TOKENS": "re_live_pasted_key",
		"NOTIFY_UNSUBSCRIBE_KEYS": "nonsense", "NOTIFY_GRPC_TLS_CERT": "/c.pem",
	}))
	ce, ok := err.(*ConfigError)
	if !ok {
		t.Fatalf("err = %v, want a *ConfigError", err)
	}
	all := strings.Join(ce.Problems, "\n")
	for _, want := range []string{"NOTIFY_SHARDS", "NOTIFY_CLAIM_LEASE", "NOTIFY_ROLE", "NOTIFY_REALM_BINDINGS", "NOTIFY_OPERATOR_TOKENS", "NOTIFY_UNSUBSCRIBE_KEYS", "NOTIFY_GRPC_TLS_CERT", "NOTIFY_PG_DSN", "NOTIFY_REDIS_URL"} {
		if !strings.Contains(all, want) {
			t.Errorf("no problem names %s:\n%s", want, all)
		}
	}
	if strings.Contains(all, "re_live_pasted_key") {
		t.Errorf("a problem repeats the value pasted where a reference belongs:\n%s", all)
	}
}

func TestLoadConfigRefusesWhatCouldNotAuthenticateAProducer(t *testing.T) {
	t.Parallel()
	kv := good()
	kv["NOTIFY_REALM_BINDINGS"] = "a=alpha,b=beta"
	kv["NOTIFY_RECIPIENT_JWKS"] = "alpha=https://h/jwks.json"
	if _, err := LoadConfig(env(kv)); err == nil || !strings.Contains(err.Error(), `"beta"`) {
		t.Fatalf("err = %v, want it to name beta: its producers have neither a JWKS nor mTLS", err)
	}
	kv["NOTIFY_GRPC_TLS_CERT"], kv["NOTIFY_GRPC_TLS_KEY"], kv["NOTIFY_GRPC_TLS_CLIENT_CA"] = "/c", "/k", "/ca"
	if _, err := LoadConfig(env(kv)); err != nil {
		t.Fatalf("with mTLS configured: %v, want it accepted: client certificates authenticate beta's producers", err)
	}
}

func TestLoadConfigNeedsAtLeastOneBinding(t *testing.T) {
	t.Parallel()
	kv := good()
	delete(kv, "NOTIFY_REALM_BINDINGS")
	if _, err := LoadConfig(env(kv)); err == nil || !strings.Contains(err.Error(), "NOTIFY_REALM_BINDINGS") {
		t.Fatalf("err = %v, want it to say a service with no bindings can be called by no one", err)
	}
}

func TestOperatorTokensSaySWhichRealmWhenSeveralAreServed(t *testing.T) {
	t.Parallel()
	kv := good()
	kv["NOTIFY_REALM_BINDINGS"] = "a=alpha,b=beta"
	kv["NOTIFY_RECIPIENT_JWKS"] = "alpha=https://h/a,beta=https://h/b"
	kv["NOTIFY_OPERATOR_TOKENS"] = "env:BARE"
	if _, err := LoadConfig(env(kv)); err == nil || !strings.Contains(err.Error(), "NOTIFY_OPERATOR_TOKENS") {
		t.Fatalf("err = %v, want a bare token refused when two realms are served", err)
	}
	kv["NOTIFY_OPERATOR_TOKENS"] = "alpha=env:A_TOKEN,beta=file:/run/secrets/b"
	c, err := LoadConfig(env(kv))
	if err != nil || len(c.OperatorTokens) != 2 || c.OperatorTokens[1].Realm != "beta" {
		t.Fatalf("operator tokens = %+v, %v, want one per realm, so an operator of one product never holds the other's", c.OperatorTokens, err)
	}
}

func TestEngineProblemsAreNamedByTheEnginesOwnRules(t *testing.T) {
	t.Parallel()
	kv := good()
	kv["NOTIFY_IDEMPOTENCY_WINDOW"] = "1h"
	_, err := LoadConfig(env(kv))
	if err == nil || !strings.Contains(err.Error(), "IdempotencyWindow") {
		t.Fatalf("err = %v, want the engine's own rule: the window is at least 24h", err)
	}
}
