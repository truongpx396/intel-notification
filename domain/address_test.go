package domain_test

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/truongpx396/intel-notification/domain"
)

// T048, T049, NR-028. What a producer may store as a recipient's addresses.
func TestPrepareAddresses(t *testing.T) {
	t.Parallel()
	lowerDomain := func(v string) string {
		at := strings.LastIndexByte(v, '@')
		if at < 0 {
			return v
		}
		return v[:at+1] + strings.ToLower(v[at+1:])
	}
	t.Run("a set is stored with its channel and the key of each", func(t *testing.T) {
		t.Parallel()
		in := []domain.Address{
			{Value: "a@X.test", Locale: "fr", Timezone: "Europe/Paris", Meta: map[string]string{"k": "v"}},
			{Value: "b@x.test"},
		}
		got, keys, err := domain.PrepareAddresses("email", in, lowerDomain)
		if err != nil || len(got) != 2 {
			t.Fatalf("PrepareAddresses = %+v, %v, want both", got, err)
		}
		if got[0].Channel != "email" || got[0].Value != "a@X.test" || got[0].Locale != "fr" || got[0].Meta["k"] != "v" {
			t.Errorf("address 0 = %+v, want its value as given, the channel set, and what it carried", got[0])
		}
		if want := []string{domain.AddressKey("email", "a@x.test"), domain.AddressKey("email", "b@x.test")}; !slices.Equal(keys, want) {
			t.Errorf("keys = %v, want the keys of the normalized values %v", keys, want)
		}
		if in[0].Channel != "" {
			t.Error("the caller's addresses were changed")
		}
		in[0].Meta["k"] = "changed after the call"
		if got[0].Meta["k"] != "v" {
			t.Errorf("meta = %v, want a copy: a caller reusing its map must not change what is stored", got[0].Meta)
		}
	})

	t.Run("two spellings of one address are one address, and the first is kept", func(t *testing.T) {
		t.Parallel()
		got, keys, err := domain.PrepareAddresses("email", []domain.Address{
			{Value: "a@X.test", Locale: "fr"}, {Value: "b@x.test"}, {Value: "a@x.TEST", Locale: "de"},
		}, lowerDomain)
		if err != nil || len(got) != 2 || len(keys) != 2 {
			t.Fatalf("PrepareAddresses = %+v, %v, want two", got, err)
		}
		if got[0].Value != "a@X.test" || got[0].Locale != "fr" || got[1].Value != "b@x.test" {
			t.Fatalf("addresses = %+v, want the first spelling kept, in order", got)
		}
	})

	t.Run("with no normalization the value is the key", func(t *testing.T) {
		t.Parallel()
		_, keys, err := domain.PrepareAddresses("push", []domain.Address{{Value: "Tok"}, {Value: "tok"}}, nil)
		if err != nil || len(keys) != 2 || keys[0] == keys[1] {
			t.Fatalf("keys = %v, %v, want two distinct keys: without a normalizer a token is compared exactly", keys, err)
		}
	})

	t.Run("an empty set is a set", func(t *testing.T) {
		t.Parallel()
		got, keys, err := domain.PrepareAddresses("email", nil, nil)
		if err != nil || len(got) != 0 || len(keys) != 0 {
			t.Fatalf("PrepareAddresses(nil) = %v, %v, %v, want an empty set: it clears the channel", got, keys, err)
		}
	})

	cases := []struct {
		name   string
		addrs  func() []domain.Address
		fields []string
	}{
		{"a value that is empty", func() []domain.Address { return []domain.Address{{Value: ""}} }, []string{"Addresses[0].Value"}},
		{"a value that is only space", func() []domain.Address { return []domain.Address{{Value: "  \t"}} }, []string{"Addresses[0].Value"}},
		{"a value too long", func() []domain.Address {
			return []domain.Address{{Value: strings.Repeat("a", domain.MaxAddressValueBytes+1)}}
		}, []string{"Addresses[0].Value"}},
		{"too many addresses", func() []domain.Address {
			out := make([]domain.Address, domain.MaxAddressesPerChannel+1)
			for i := range out {
				out[i].Value = "t" + strings.Repeat("x", i)
			}
			return out
		}, []string{"Addresses"}},
		{"a locale that is no tag", func() []domain.Address { return []domain.Address{{Value: "a", Locale: "en US"}} }, []string{"Addresses[0].Locale"}},
		{"a time zone nobody knows", func() []domain.Address { return []domain.Address{{Value: "a", Timezone: "Mars/Olympus"}} }, []string{"Addresses[0].Timezone"}},
		{"the host's own zone", func() []domain.Address { return []domain.Address{{Value: "a", Timezone: "Local"}} }, []string{"Addresses[0].Timezone"}},
		{"too much meta", func() []domain.Address {
			m := map[string]string{}
			for i := range domain.MaxAddressMetaEntries + 1 {
				m[strings.Repeat("k", i+1)] = "v"
			}
			return []domain.Address{{Value: "a", Meta: m}}
		}, []string{"Addresses[0].Meta"}},
		{"a meta value too long", func() []domain.Address {
			return []domain.Address{{Value: "a", Meta: map[string]string{"k": strings.Repeat("v", domain.MaxAddressMetaBytes+1)}}}
		}, []string{"Addresses[0].Meta"}},
		{"an address that names another channel", func() []domain.Address { return []domain.Address{{Value: "a", Channel: "sms"}} }, []string{"Addresses[0].Channel"}},
		{"every problem at once", func() []domain.Address {
			return []domain.Address{{Value: ""}, {Value: "b", Locale: "x y"}}
		}, []string{"Addresses[0].Value", "Addresses[1].Locale"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, keys, err := domain.PrepareAddresses("email", tc.addrs(), nil)
			var v *domain.ValidationError
			if !errors.As(err, &v) {
				t.Fatalf("PrepareAddresses = %+v, %v, want a *ValidationError", got, err)
			}
			if got != nil || keys != nil {
				t.Errorf("PrepareAddresses returned %v and %v with an error, want nothing to store", got, keys)
			}
			named := map[string]bool{}
			for _, p := range v.Problems {
				named[p.Field] = true
			}
			for _, f := range tc.fields {
				if !named[f] {
					t.Errorf("no problem names %s: %v", f, v.Problems)
				}
			}
		})
	}
}
