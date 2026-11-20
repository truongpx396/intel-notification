package domain_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/truongpx396/intel-notification/domain"
)

// T049, NR-009, D31.
func TestParseRealmBindings(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		in      string
		want    map[string]domain.Realm
		wantErr string // a substring of the message; "" means it parses
	}{
		{"one pair", "local-producer=local", map[string]domain.Realm{"local-producer": "local"}, ""},
		{"several, with space around them", " a=alpha , b=beta ,c=alpha", map[string]domain.Realm{"a": "alpha", "b": "beta", "c": "alpha"}, ""},
		{"a principal that is a URI with an equals sign", "spiffe://x/y?v=1=alpha", map[string]domain.Realm{"spiffe://x/y?v=1": "alpha"}, "" /* the realm is what follows the last = */},
		{"nothing is no bindings, and not an error", "", map[string]domain.Realm{}, ""},
		{"a trailing comma is no pair", "a=alpha,", map[string]domain.Realm{"a": "alpha"}, ""},
		{"a pair with no equals sign", "a", nil, "a"},
		{"an empty principal", "=alpha", nil, "principal"},
		{"an empty realm", "a=", nil, "realm"},
		{"a realm that is not a realm", "a=Not_A_Realm", nil, "realm"},
		{"a principal bound twice, to different realms", "a=alpha,a=beta", nil, `"a"`},
		{"a principal bound twice, to the same realm", "a=alpha,a=alpha", nil, `"a"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b, err := domain.ParseRealmBindings(tc.in)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("ParseRealmBindings(%q) = %v, want an error naming %q", tc.in, err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseRealmBindings(%q) = %v", tc.in, err)
			}
			for principal, realm := range tc.want {
				if got, ok := b.RealmOf(principal); !ok || got != realm {
					t.Errorf("RealmOf(%q) = %q, %v, want %q", principal, got, ok, realm)
				}
			}
			if _, ok := b.RealmOf("nobody"); ok {
				t.Error("RealmOf(nobody) is bound: an unbound principal must be refused")
			}
		})
	}
}

func TestRealmBindingsDoNotMatchByPrefixOrCase(t *testing.T) {
	t.Parallel()
	b, err := domain.ParseRealmBindings("billing-service=alpha")
	if err != nil {
		t.Fatal(err)
	}
	for _, principal := range []string{"billing", "billing-service-2", "Billing-Service", "billing-service ", ""} {
		if realm, ok := b.RealmOf(principal); ok {
			t.Errorf("RealmOf(%q) = %q, want unbound: a principal is matched whole (constitution VIII)", principal, realm)
		}
	}
}

func TestRealmBindingsListTheirRealms(t *testing.T) {
	t.Parallel()
	b, err := domain.ParseRealmBindings("a=beta,b=alpha,c=beta")
	if err != nil {
		t.Fatal(err)
	}
	if got := b.Realms(); !slices.Equal(got, []domain.Realm{"alpha", "beta"}) {
		t.Fatalf("Realms() = %v, want [alpha beta]: sorted, without repeats", got)
	}
	if got := (domain.RealmBindings{}).Realms(); len(got) != 0 {
		t.Fatalf("Realms() of no bindings = %v, want none", got)
	}
}

func TestParseRealmPairs(t *testing.T) {
	t.Parallel()
	got, err := domain.ParseRealmPairs("alpha=https://h.example/jwks.json?x=1, beta=host:9090")
	if err != nil {
		t.Fatal(err)
	}
	if got["alpha"] != "https://h.example/jwks.json?x=1" || got["beta"] != "host:9090" || len(got) != 2 {
		t.Fatalf("ParseRealmPairs = %v, want the value to be everything after the first equals sign", got)
	}
	for name, in := range map[string]string{
		"no equals sign":      "alpha",
		"a realm that is not": "Not_A_Realm=x",
		"no value":            "alpha=",
		"a realm twice":       "alpha=x,alpha=y",
	} {
		if _, err := domain.ParseRealmPairs(in); err == nil {
			t.Errorf("ParseRealmPairs(%q) (%s) = nil, want an error", in, name)
		}
	}
	if empty, err := domain.ParseRealmPairs(""); err != nil || len(empty) != 0 {
		t.Errorf("ParseRealmPairs(\"\") = %v, %v, want none and no error", empty, err)
	}
}
