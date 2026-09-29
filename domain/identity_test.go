package domain

import (
	"errors"
	"testing"
)

var u1 = Identity{
	Realm:     "aisat",
	Tenant:    Tenant{Kind: "workspace", ID: "w1"},
	Recipient: Recipient{Kind: "user", ID: "u1"},
}

func TestCanonical(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		parts []string
		want  string
	}{
		// The frozen vector. It is the published definition of the encoding
		// (data-model.md): changing it changes every stored key and hash.
		{"identity", []string{"aisat", "workspace", "w1", "user", "u1"}, "5:aisat,9:workspace,2:w1,4:user,2:u1"},
		{"lengths are octets, not runes", []string{"é", "b"}, "2:é,1:b"},
		{"empty parts are encoded, not dropped", []string{"", "a"}, "0:,1:a"},
		{"no parts", nil, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := Canonical(tc.parts...); got != tc.want {
				t.Fatalf("Canonical(%q) = %q, want %q", tc.parts, got, tc.want)
			}
		})
	}
	t.Run("Identity.Canonical matches", func(t *testing.T) {
		t.Parallel()
		if got := u1.Canonical(); got != "5:aisat,9:workspace,2:w1,4:user,2:u1" {
			t.Fatalf("got %q", got)
		}
	})
}

func TestCanonicalIsUnambiguous(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		a, b []string
	}{
		{"the separator inside an id", []string{"a:b", "c"}, []string{"a", "b:c"}},
		{"an empty component", []string{"ab", ""}, []string{"a", "b"}},
		{"the joiner inside an id", []string{"a,b"}, []string{"a", "b"}},
		{"a part that looks encoded", []string{"1:a"}, []string{"1", "a"}},
		{"the inherited Tenant.Tag() collision", []string{"workspace:a", "b"}, []string{"workspace", "a:b"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if Canonical(tc.a...) == Canonical(tc.b...) {
				t.Fatalf("%q and %q encode alike: %q", tc.a, tc.b, Canonical(tc.a...))
			}
		})
	}
}

func TestIdentityValidate(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		mutate func(*Identity)
		want   error
	}{
		{"complete", func(*Identity) {}, nil},
		{"empty realm", func(i *Identity) { i.Realm = "" }, ErrEmptyIdentity},
		{"empty tenant kind", func(i *Identity) { i.Tenant.Kind = "" }, ErrEmptyIdentity},
		{"empty tenant id", func(i *Identity) { i.Tenant.ID = "" }, ErrEmptyIdentity},
		{"empty recipient kind", func(i *Identity) { i.Recipient.Kind = "" }, ErrEmptyIdentity},
		{"empty recipient id", func(i *Identity) { i.Recipient.ID = "" }, ErrEmptyIdentity},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			id := u1
			tc.mutate(&id)
			if err := id.Validate(); !errors.Is(err, tc.want) {
				t.Fatalf("Validate() = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestRealmValidate(t *testing.T) {
	t.Parallel()
	cases := []struct {
		realm Realm
		valid bool
	}{
		{"aisat", true},
		{"my-product", true},
		{"a", true},
		{"p2", true},
		{"", false},
		{"My-Product", false},
		{"-leading", false},
		{"has.dot", false}, // would split a bus subject token
		{"has space", false},
		{"has:colon", false}, // would split a Redis key segment
		{"x123456789012345678901234567890123456789012345678901234567890123", false},
	}
	for _, tc := range cases {
		t.Run(string(tc.realm), func(t *testing.T) {
			t.Parallel()
			if err := tc.realm.Validate(); (err == nil) != tc.valid {
				t.Fatalf("Validate(%q) = %v, want valid = %v", tc.realm, err, tc.valid)
			}
		})
	}
}
