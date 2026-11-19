package app

import (
	"context"
	"fmt"

	"github.com/truongpx396/intel-notification/domain"
)

// Erase removes one recipient's personal data from every table that holds it, in one
// transaction under the recipient's own scope, and records that it happened by the
// identity's hash and not the identity (NR-034, D35). It returns the rows removed per
// table, so an operator can see what was there.
//
// What it keeps is as much of the contract as what it removes. The address suppressions
// stay: they hold hashes of addresses and never the address, and deleting one would resume
// mailing someone who complained, which is worse than not erasing at all. So an address
// that was suppressed before the erasure is still refused after it, whoever it is
// mapped to next (NS-012).
//
// An identity with an empty component is refused before the store is asked. An empty
// component is the one that equals an unset scope setting, and an erasure that began and
// failed in the middle of a table list is worse than one that never began. A failure
// returns no counts: an erasure that failed has erased nothing, because the store runs it
// in one transaction, and a count beside a fault would say otherwise.
func (a *Admin) Erase(ctx context.Context, id domain.Identity) (map[string]int, error) {
	if err := id.Validate(); err != nil {
		return nil, fmt.Errorf("erase: %w", err)
	}
	counts, err := a.maintenance.Erase(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("erase: %w", err)
	}
	return counts, nil
}
