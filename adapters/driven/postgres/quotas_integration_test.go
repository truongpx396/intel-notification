//go:build integration

package postgres

import (
	"testing"

	"github.com/truongpx396/intel-notification/domain"
)

func quotaKeyOf(realm domain.Realm, kind, tenant string, ch domain.ChannelKind) domain.QuotaKey {
	return domain.QuotaKey{Realm: realm, Tenant: domain.Tenant{Kind: kind, ID: tenant}, Channel: ch}
}

func (e *env) quotaRow(realm, kind, tenant, channel string, hour, day int, onExhausted string) {
	e.t.Helper()
	if _, err := e.db.Admin.Exec(e.t.Context(), `
INSERT INTO channel_quotas (realm, tenant_kind, tenant_id, channel, max_per_hour, max_per_day, on_exhausted)
VALUES ($1, $2, $3, $4, $5, $6, $7)`, realm, kind, tenant, channel, hour, day, onExhausted); err != nil {
		e.t.Fatalf("fixture quota: %v", err)
	}
}

// The realm-wide row is the default for a tenant with none of its own, and a tenant's
// own row overrides it (D5, NR-023). Both the ceilings and what to do when they are
// spent come from the same row, so a tenant is never judged by its own ceiling and the
// realm's policy.
func TestQuotasReadTheRealmDefaultUntilATenantOverridesIt(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.quotaRow("aisat", "", "", "email", 1000, 10000, "defer")
	e.quotaRow("aisat", "", "", "sms", 5, 50, "drop_non_essential")
	e.quotaRow("aisat", "workspace", "w1", "email", 10, 100, "drop_non_essential")
	e.quotaRow("aisat", "workspace", "silenced", "email", 0, 0, "defer")
	e.quotaRow("other", "", "", "email", 7, 70, "defer")
	q := NewQuotas(e.db.Owner)

	cases := []struct {
		name   string
		key    domain.QuotaKey
		limits domain.QuotaLimits
		ok     bool
		policy domain.QuotaPolicy
		why    string
	}{
		{"a tenant with its own row", quotaKeyOf("aisat", "workspace", "w1", "email"),
			domain.QuotaLimits{PerHour: 10, PerDay: 100}, true, domain.QuotaDropNonEssential, "the tenant's row overrides the realm's"},
		{"a tenant with none takes the realm default", quotaKeyOf("aisat", "workspace", "w2", "email"),
			domain.QuotaLimits{PerHour: 1000, PerDay: 10000}, true, domain.QuotaDefer, "so a tenant nobody configured still cannot monopolise the drainers (D24)"},
		{"an override on one channel leaves the others the default", quotaKeyOf("aisat", "workspace", "w1", "sms"),
			domain.QuotaLimits{PerHour: 5, PerDay: 50}, true, domain.QuotaDropNonEssential, "the realm's sms row, not w1's email row"},
		{"a row of zero is a row", quotaKeyOf("aisat", "workspace", "silenced", "email"),
			domain.QuotaLimits{PerHour: 0, PerDay: 0}, true, domain.QuotaDefer, "zero allows none; it is not the absence of a limit"},
		{"a channel nothing configures is unlimited", quotaKeyOf("aisat", "workspace", "w1", "push"),
			domain.QuotaLimits{}, false, domain.QuotaDefer, "no row at all: deferring discards nothing"},
		{"another realm has its own default", quotaKeyOf("other", "workspace", "w1", "email"),
			domain.QuotaLimits{PerHour: 7, PerDay: 70}, true, domain.QuotaDefer, "a realm's row is never another's"},
		{"a realm nothing configures is unlimited", quotaKeyOf("third", "workspace", "w1", "email"),
			domain.QuotaLimits{}, false, domain.QuotaDefer, ""},
		{"a tenant of the same id and another kind is not that tenant", quotaKeyOf("aisat", "org", "w1", "email"),
			domain.QuotaLimits{PerHour: 1000, PerDay: 10000}, true, domain.QuotaDefer, "kind and id are one identity (constitution VIII)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			limits, ok, err := q.Limits(t.Context(), tc.key)
			if err != nil || ok != tc.ok || limits != tc.limits {
				t.Fatalf("Limits(%+v) = %+v, %v, %v, want %+v, %v: %s", tc.key, limits, ok, err, tc.limits, tc.ok, tc.why)
			}
			policy, err := q.Policy(t.Context(), tc.key)
			if err != nil || policy != tc.policy {
				t.Fatalf("Policy(%+v) = %q, %v, want %q: %s", tc.key, policy, err, tc.policy, tc.why)
			}
		})
	}
}

// A tenant's override takes effect on the next read: the reader keeps no copy, because
// the counter in front of it does (T036).
func TestQuotasSeeAnOverrideOnTheNextRead(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.quotaRow("aisat", "", "", "email", 1000, 10000, "defer")
	q := NewQuotas(e.db.Owner)
	k := quotaKeyOf("aisat", "workspace", "w1", "email")

	// A sequence: the second read depends on the write between.
	if limits, _, _ := q.Limits(t.Context(), k); limits.PerHour != 1000 {
		t.Fatalf("before the override Limits = %+v, want the realm default", limits)
	}
	e.quotaRow("aisat", "workspace", "w1", "email", 3, 30, "drop_non_essential")
	if limits, _, _ := q.Limits(t.Context(), k); limits.PerHour != 3 || limits.PerDay != 30 {
		t.Fatalf("after the override Limits = %+v, want the tenant's own 3 and 30", limits)
	}
	if policy, _ := q.Policy(t.Context(), k); policy != domain.QuotaDropNonEssential {
		t.Fatalf("after the override Policy = %q, want drop_non_essential", policy)
	}
}
