package domain

import (
	"encoding/hex"
	"testing"
)

// Frozen vectors, computed independently from the definitions in data-model.md
// rather than from this code. Changing one changes a key that is already stored
// or already at a provider.
func TestDerivedKeys(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		got  func() string
		want string
	}{
		{"AddressKey", func() string { return AddressKey("email", "u1@example.com") },
			"5995cea917d46aed86d60f16238dd0cb"},
		{"DeliveryIdemKey", func() string { return DeliveryIdemKey(u1, "invoice:42:overdue", "", "email", "addrkey1") },
			"9602e45b0e75892a423e8c52157ced818019629335020fca1d8590a91864fc46"},
		{"DeliveryIdemKey for a digest", func() string { return DeliveryIdemKey(u1, "", "win-1", "email", "addrkey1") },
			"1ba58824a778e5eb81372751cb497ff07d9e77b4acaf180e3d89fea2a93d1a57"},
		{"BroadcastMemberKey", func() string { return BroadcastMemberKey("launch:2026", u1.Recipient) },
			"957e4d4674597867ed8e37f1ced461b568f08ebca8939e4f9b6e8a435c48a1e9"},
		{"SuppressionHash", func() string { return hex.EncodeToString(SuppressionHash("email", "u1@example.com")) },
			"5995cea917d46aed86d60f16238dd0cb4c27f35aad6f99d50526365471251157"},
		{"SubjectHash", func() string { return hex.EncodeToString(SubjectHash(u1)) },
			"f0072fa3f1ee37a2e7c239330a27dc874f5d80b52732d40e27fcc3eae00dae7c"},
		{"PreCheckKey", func() string { return PreCheckKey(u1, "invoice:42:overdue") },
			"notify:applied:aisat:2006f991ca0e66c48051540129023c7fc5c3f7f77455b4f8a3b70bade98c8df1"},
		{"StreamKey", func() string { return StreamKey(u1) },
			"notify:inbox:{f0072fa3f1ee37a2e7c239330a27dc874f5d80b52732d40e27fcc3eae00dae7c}"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.got(); got != tc.want {
				t.Fatalf("got %s, want %s", got, tc.want)
			}
		})
	}
}

// Each derived key must separate what the guarantee behind it separates. The
// stream key isolates the live path (D16) — the inherited "notify:user:" + id
// was shared by every tenant and realm. The pre-check shares the durable
// guard's key space (D18), or it drops what the guard would accept. The
// delivery key is per address (D25), or the provider drops the second device.
func TestDerivedKeysSeparate(t *testing.T) {
	t.Parallel()
	with := func(mutate func(*Identity)) Identity {
		id := u1
		mutate(&id)
		return id
	}
	otherTenant := with(func(i *Identity) { i.Tenant.ID = "w2" })
	otherRealm := with(func(i *Identity) { i.Realm = "other" })
	otherKind := with(func(i *Identity) { i.Recipient.Kind = "device" })
	otherRecipient := with(func(i *Identity) { i.Recipient.ID = "u2" })

	cases := []struct {
		name string
		a, b string
	}{
		{"stream: another tenant", StreamKey(u1), StreamKey(otherTenant)},
		{"stream: another realm", StreamKey(u1), StreamKey(otherRealm)},
		{"stream: another recipient kind", StreamKey(u1), StreamKey(otherKind)},
		{"pre-check: another recipient", PreCheckKey(u1, "k"), PreCheckKey(otherRecipient, "k")},
		{"pre-check: another tenant", PreCheckKey(u1, "k"), PreCheckKey(otherTenant, "k")},
		{"delivery: another address", DeliveryIdemKey(u1, "k", "", "push", "dev-a"),
			DeliveryIdemKey(u1, "k", "", "push", "dev-b")},
		{"delivery: another realm", DeliveryIdemKey(u1, "k", "", "push", "dev-a"),
			DeliveryIdemKey(otherRealm, "k", "", "push", "dev-a")},
		{"delivery: a digest named like a key", DeliveryIdemKey(u1, "digest:w", "", "email", "a"),
			DeliveryIdemKey(u1, "", "w2", "email", "a")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.a == tc.b {
				t.Fatalf("keys collide: %s", tc.a)
			}
		})
	}
}
