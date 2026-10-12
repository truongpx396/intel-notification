package postgres

import (
	"context"
	"errors"

	"github.com/truongpx396/intel-notification/domain"
)

// Load implements ports.Store.
func (s *Store) Load(ctx context.Context, c domain.Claim) (*domain.Notification, []domain.Notification, bool, error) {
	return nil, nil, false, errors.New("not implemented")
}
