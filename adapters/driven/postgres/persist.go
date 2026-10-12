package postgres

import (
	"context"
	"errors"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// PersistAndEnqueue implements ports.Store.
func (s *Store) PersistAndEnqueue(ctx context.Context, tx ports.Tx, id domain.Identity, n domain.Notification, plan domain.DeliveryPlan) (domain.Receipt, error) {
	return domain.Receipt{}, errors.New("not implemented")
}
