package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// Quotas reads channel_quotas: a budget's ceilings and what to do when it is spent.
// The table is configuration an operator writes, so the reader keeps no copy of it; the
// counter in front of it does (adapters/driven/redis).
type Quotas struct{ pool *pgxpool.Pool }

var (
	_ ports.QuotaPolicies = (*Quotas)(nil)
	_ ports.QuotaLimits   = (*Quotas)(nil)
)

// NewQuotas returns the reader over pool.
func NewQuotas(pool *pgxpool.Pool) *Quotas { return &Quotas{pool: pool} }

// quotaRowSQL finds the row that governs a (realm, tenant, channel): the tenant's own,
// else the realm's, whose tenant pair is empty (D5). One row supplies the ceilings and
// the policy, so a tenant is never judged by its own ceiling and the realm's policy.
// ORDER BY puts the tenant's row first: its tenant_id is not empty, so the comparison is
// false, and false sorts before true.
const quotaRowSQL = `
SELECT max_per_hour, max_per_day, on_exhausted
  FROM channel_quotas
 WHERE realm = $1 AND channel = $2
   AND ((tenant_kind = $3 AND tenant_id = $4) OR (tenant_kind = '' AND tenant_id = ''))
 ORDER BY tenant_id = ''
 LIMIT 1`

// row reads the governing row; ok is false when no row configures the key.
func (q *Quotas) row(ctx context.Context, k domain.QuotaKey) (limits domain.QuotaLimits, policy domain.QuotaPolicy, ok bool, err error) {
	var onExhausted string
	err = q.pool.QueryRow(ctx, quotaRowSQL, string(k.Realm), string(k.Channel), k.Tenant.Kind, k.Tenant.ID).
		Scan(&limits.PerHour, &limits.PerDay, &onExhausted)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return domain.QuotaLimits{}, domain.QuotaDefer, false, nil
	case err != nil:
		return domain.QuotaLimits{}, "", false, fmt.Errorf("read channel_quotas for %s on %s: %w", k.Tenant.ID, k.Channel, err)
	}
	return limits, domain.QuotaPolicy(onExhausted), true, nil
}

// Policy implements ports.QuotaPolicies. A key nothing configures defers, which
// discards nothing.
func (q *Quotas) Policy(ctx context.Context, k domain.QuotaKey) (domain.QuotaPolicy, error) {
	_, policy, _, err := q.row(ctx, k)
	return policy, err
}

// Limits implements ports.QuotaLimits.
func (q *Quotas) Limits(ctx context.Context, k domain.QuotaKey) (domain.QuotaLimits, bool, error) {
	limits, _, ok, err := q.row(ctx, k)
	return limits, ok, err
}
