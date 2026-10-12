package postgres

import (
	"context"
	"errors"

	"github.com/truongpx396/intel-notification/domain"
)

// Status implements ports.Store.
func (s *Store) Status(ctx context.Context, id domain.Identity, idemKey string) (domain.NotificationStatus, error) {
	return domain.NotificationStatus{}, errors.New("not implemented")
}

// RecordProviderStatus implements ports.Store.
func (s *Store) RecordProviderStatus(ctx context.Context, ch domain.ChannelKind, providerMessageID, status string) error {
	return errors.New("not implemented")
}
