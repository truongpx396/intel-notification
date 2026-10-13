package app

import (
	"context"
	"fmt"

	notify "github.com/truongpx396/intel-notification"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// Admin is the operator surface: ports.Admin over the maintenance operations. What it
// builds today is the dead-letter replay (T032, NS-006); erasure is T040's and the
// tenant preference is the REST surface's (T044), and until then they say so, because a
// nil would tell an operator that a recipient was erased.
type Admin struct {
	maintenance ports.Maintenance
	waker       ports.Waker // nil: no dispatcher in this process to wake
}

var _ ports.Admin = (*Admin)(nil)

// NewAdmin returns an Admin for cfg over d. It refuses a configuration that does not
// validate, and names the port it needs when d leaves it nil. Deps.Waker is optional.
func NewAdmin(cfg notify.Config, d Deps) (*Admin, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	var g gaps
	g.need("Maintenance", d.Maintenance == nil)
	if err := g.err("Admin"); err != nil {
		return nil, err
	}
	return &Admin{maintenance: d.Maintenance, waker: d.Waker}, nil
}

// ReplayDeadLetter re-drives one dead letter, once (T032). A dead letter that was
// replayed already is not an error: the call is idempotent, and the operator who clicks
// twice, or the script that retries after a timeout, gets the one delivery. A replay
// that enqueued work wakes the dispatcher of this process, if it has one.
//
// A dead letter that does not exist in the realm is domain.ErrUnknownDeadLetter, and
// one the bus parked, which holds no delivery to re-drive, is domain.ErrNotReplayable.
func (a *Admin) ReplayDeadLetter(ctx context.Context, realm domain.Realm, deadLetterID string) error {
	if err := realm.Validate(); err != nil {
		return fmt.Errorf("replay dead letter: %w", err)
	}
	if deadLetterID == "" {
		return fmt.Errorf("replay dead letter: no id: %w", domain.ErrUnknownDeadLetter)
	}
	replayed, err := a.maintenance.ReplayDeadLetter(ctx, realm, deadLetterID)
	if err != nil {
		return fmt.Errorf("replay dead letter: %w", err)
	}
	if replayed && a.waker != nil {
		a.waker.Wake()
	}
	return nil
}

// Erase is built with the erasure use-case (T040).
func (a *Admin) Erase(context.Context, domain.Identity) (map[string]int, error) {
	return nil, errNotBuilt("Admin.Erase", "T040")
}

// SetTenantPreference is built with the REST surface (T044).
func (a *Admin) SetTenantPreference(context.Context, domain.Realm, domain.TenantPreference) error {
	return errNotBuilt("Admin.SetTenantPreference", "T044")
}
