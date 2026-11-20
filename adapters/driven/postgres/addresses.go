package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// Addresses implements ports.AddressBook and ports.AddressWriter over recipient_addresses (NR-028,
// D25, D31).
type Addresses struct {
	pool *pgxpool.Pool
	settings
}

var (
	_ ports.AddressBook   = (*Addresses)(nil)
	_ ports.AddressWriter = (*Addresses)(nil)
)

// NewAddresses returns an Addresses over pool.
func NewAddresses(pool *pgxpool.Pool, opts ...Option) *Addresses {
	return &Addresses{pool: pool, settings: newSettings(opts)}
}

// Resolve implements ports.AddressBook.
func (a *Addresses) Resolve(context.Context, domain.Identity, domain.ChannelKind) ([]domain.Address, error) {
	return nil, errNotImplemented
}

// PutAddresses implements ports.AddressWriter.
func (a *Addresses) PutAddresses(context.Context, domain.Identity, domain.ChannelKind, []domain.Address, []string) error {
	return errNotImplemented
}
