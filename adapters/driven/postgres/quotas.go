package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// Quotas reads channel_quotas.
type Quotas struct{ pool *pgxpool.Pool }

var (
	_ ports.QuotaPolicies = (*Quotas)(nil)
	_ ports.QuotaLimits   = (*Quotas)(nil)
)

// NewQuotas returns the reader over pool.
func NewQuotas(pool *pgxpool.Pool) *Quotas { return &Quotas{pool: pool} }

// Policy implements ports.QuotaPolicies.
func (*Quotas) Policy(context.Context, domain.QuotaKey) (domain.QuotaPolicy, error) {
	return "", errors.New("not implemented")
}

// Limits implements ports.QuotaLimits.
func (*Quotas) Limits(context.Context, domain.QuotaKey) (domain.QuotaLimits, bool, error) {
	return domain.QuotaLimits{}, false, errors.New("not implemented")
}
