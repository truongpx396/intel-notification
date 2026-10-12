package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/truongpx396/intel-notification/domain"
)

// The broadcast methods of ports.Store belong to Stage 9 (T041, T042). They are
// declared so the Store satisfies the port, and fail loudly until then.
var errBroadcastNotBuilt = errors.New("postgres: broadcast is not implemented yet (T041)")

// CreateBroadcast implements ports.Store.
func (s *Store) CreateBroadcast(ctx context.Context, realm domain.Realm, b domain.BroadcastRequest) (domain.BroadcastReceipt, error) {
	return domain.BroadcastReceipt{}, errBroadcastNotBuilt
}

// LeaseBroadcast implements ports.Store.
func (s *Store) LeaseBroadcast(ctx context.Context, lease time.Duration) (*domain.BroadcastJob, error) {
	return nil, errBroadcastNotBuilt
}

// CommitBroadcastPage implements ports.Store.
func (s *Store) CommitBroadcastPage(ctx context.Context, job domain.BroadcastJob, page []domain.PageItem, nextCursor string, done bool) error {
	return errBroadcastNotBuilt
}
