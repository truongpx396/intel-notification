package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// Preferences implements ports.PreferenceStore over one pool, as the owner role,
// under the recipient's scope (D17).
type Preferences struct {
	pool *pgxpool.Pool
	settings
}

var _ ports.PreferenceStore = (*Preferences)(nil)

// NewPreferences returns a Preferences over pool.
func NewPreferences(pool *pgxpool.Pool, opts ...Option) *Preferences {
	return &Preferences{pool: pool, settings: newSettings(opts)}
}

var errPrefsNotBuilt = errors.New("not implemented")

// Resolve implements ports.PreferenceStore.
func (p *Preferences) Resolve(ctx context.Context, id domain.Identity, topic domain.Topic, def domain.TopicDef) ([]domain.ResolvedPreference, error) {
	return nil, errPrefsNotBuilt
}

// Schedule implements ports.PreferenceStore.
func (p *Preferences) Schedule(ctx context.Context, id domain.Identity) (domain.DeliverySchedule, error) {
	return domain.DeliverySchedule{}, errPrefsNotBuilt
}

// Set implements ports.PreferenceStore.
func (p *Preferences) Set(ctx context.Context, id domain.Identity, prefs []domain.Preference) error {
	return errPrefsNotBuilt
}

// SetSchedule implements ports.PreferenceStore.
func (p *Preferences) SetSchedule(ctx context.Context, id domain.Identity, s domain.DeliverySchedule) error {
	return errPrefsNotBuilt
}

// SetTenant implements ports.PreferenceStore.
func (p *Preferences) SetTenant(ctx context.Context, realm domain.Realm, tp domain.TenantPreference) error {
	return errPrefsNotBuilt
}
