package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// Suppressions implements ports.SuppressionStore over one pool. A suppression is
// keyed by a hash of the address, never the address (D35).
type Suppressions struct {
	pool *pgxpool.Pool
	settings
}

var _ ports.SuppressionStore = (*Suppressions)(nil)

// NewSuppressions returns a Suppressions over pool.
func NewSuppressions(pool *pgxpool.Pool, opts ...Option) *Suppressions {
	return &Suppressions{pool: pool, settings: newSettings(opts)}
}

var errSuppressionsNotBuilt = errors.New("not implemented")

// Suppressed implements ports.SuppressionStore.
func (s *Suppressions) Suppressed(ctx context.Context, realm domain.Realm, ch domain.ChannelKind, addressHash []byte) (bool, error) {
	return false, errSuppressionsNotBuilt
}

// Suppress implements ports.SuppressionStore.
func (s *Suppressions) Suppress(ctx context.Context, realm domain.Realm, ch domain.ChannelKind, addressHash []byte,
	reason domain.SuppressionReason, until time.Time) error {
	return errSuppressionsNotBuilt
}
