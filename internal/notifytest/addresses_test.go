package notifytest

import (
	"slices"
	"testing"

	"github.com/truongpx396/intel-notification/domain"
)

func who(realm domain.Realm, tenant, recipient string) domain.Identity {
	return domain.Identity{
		Realm:     realm,
		Tenant:    domain.Tenant{Kind: "workspace", ID: tenant},
		Recipient: domain.Recipient{Kind: "user", ID: recipient},
	}
}

func values(addrs []domain.Address) []string {
	out := make([]string, 0, len(addrs))
	for _, a := range addrs {
		out = append(out, a.Value)
	}
	return out
}

func TestAddressBookResolve(t *testing.T) {
	t.Parallel()
	u1 := who("aisat", "w1", "u1")
	book := NewAddressBook()
	book.Put(u1, "push",
		domain.Address{Value: "token-phone", Locale: "fr", Meta: map[string]string{"app": "ios"}},
		domain.Address{Value: "token-tablet"},
		domain.Address{Value: "token-laptop"})
	book.Put(u1, "email", domain.Address{Value: "u1@example.com", Timezone: "Europe/Paris"})

	push, err := book.Resolve(t.Context(), u1, "push")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := values(push), []string{"token-phone", "token-tablet", "token-laptop"}; !slices.Equal(got, want) {
		t.Fatalf("Resolve(push) = %v, want every device, in the order put: %v (NR-028)", got, want)
	}
	for _, a := range push {
		if a.Channel != "push" {
			t.Errorf("address %q has Channel %q, want push: Put fills it in", a.Value, a.Channel)
		}
	}
	if push[0].Locale != "fr" || push[0].Meta["app"] != "ios" {
		t.Fatalf("the address lost its locale or meta: %+v", push[0])
	}

	email, _ := book.Resolve(t.Context(), u1, "email")
	if len(email) != 1 || email[0].Timezone != "Europe/Paris" {
		t.Fatalf("Resolve(email) = %+v, want the one address with its timezone", email)
	}

	none, err := book.Resolve(t.Context(), u1, "sms")
	if err != nil || len(none) != 0 {
		t.Fatalf("Resolve(sms) = %v, %v, want no addresses and no error: a miss is terminal, not a fault", none, err)
	}
}

// The same recipient id under another tenant, realm or recipient kind is another
// recipient (NS-001): a book that keyed by recipient id alone would hand one
// tenant's addresses to another.
func TestAddressBookIsScopedByTheFullIdentity(t *testing.T) {
	t.Parallel()
	u1 := who("aisat", "w1", "u1")
	book := NewAddressBook()
	book.Put(u1, "email", domain.Address{Value: "u1@w1.example"})

	otherKind := u1
	otherKind.Recipient.Kind = "device"
	cases := []struct {
		name string
		id   domain.Identity
	}{
		{"another tenant", who("aisat", "w2", "u1")},
		{"another realm", who("other", "w1", "u1")},
		{"another recipient", who("aisat", "w1", "u2")},
		{"another recipient kind", otherKind},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := book.Resolve(t.Context(), tc.id, "email")
			if err != nil || len(got) != 0 {
				t.Fatalf("Resolve for %s = %v, %v, want nothing", tc.name, got, err)
			}
		})
	}
	if got, _ := book.Resolve(t.Context(), u1, "email"); len(got) != 1 {
		t.Fatalf("the owner's own address was not found: %v", got)
	}
}

func TestAddressBookPutReplacesTheChannelsSet(t *testing.T) {
	t.Parallel()
	u1 := who("aisat", "w1", "u1")
	book := NewAddressBook()
	book.Put(u1, "push", domain.Address{Value: "old-1"}, domain.Address{Value: "old-2"})
	book.Put(u1, "email", domain.Address{Value: "u1@example.com"})
	book.Put(u1, "push", domain.Address{Value: "new-1"})

	got, _ := book.Resolve(t.Context(), u1, "push")
	if want := []string{"new-1"}; !slices.Equal(values(got), want) {
		t.Fatalf("Resolve(push) = %v, want %v: Put replaces the channel's set", values(got), want)
	}
	if email, _ := book.Resolve(t.Context(), u1, "email"); len(email) != 1 {
		t.Fatalf("replacing push changed email: %v", email)
	}
	book.Put(u1, "push")
	if got, _ := book.Resolve(t.Context(), u1, "push"); len(got) != 0 {
		t.Fatalf("Put with no addresses left %v: it must clear the set", got)
	}
}

func TestAddressBookDoesNotShareMemoryWithCallers(t *testing.T) {
	t.Parallel()
	u1 := who("aisat", "w1", "u1")
	book := NewAddressBook()
	in := domain.Address{Value: "a@example.com", Meta: map[string]string{"k": "v"}}
	book.Put(u1, "email", in)
	in.Meta["k"] = "tampered-after-put"

	out, _ := book.Resolve(t.Context(), u1, "email")
	if len(out) != 1 {
		t.Fatalf("Resolve returned %d addresses, want the one put", len(out))
	}
	if out[0].Meta["k"] != "v" {
		t.Fatalf("a map the caller kept changed the stored address: %v", out[0].Meta)
	}
	out[0].Meta["k"] = "tampered-after-resolve"
	out[0].Value = "tampered"
	again, _ := book.Resolve(t.Context(), u1, "email")
	if len(again) != 1 || again[0].Meta["k"] != "v" || again[0].Value != "a@example.com" {
		t.Fatalf("a value returned by Resolve changed the stored address: %+v", again[0])
	}
}

func TestAddressBookRefusesAnAddressForAnotherChannel(t *testing.T) {
	t.Parallel()
	defer func() {
		if recover() == nil {
			t.Fatal("Put did not panic")
		}
	}()
	NewAddressBook().Put(who("aisat", "w1", "u1"), "email", domain.Address{Channel: "sms", Value: "+15551234567"})
}
