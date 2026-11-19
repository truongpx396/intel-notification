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
// The pre-check, if the Admin was given one, forgets its entry for each of the recipient's
// idempotency keys first, and it is first so that a failure leaves the safe state: an entry
// forgotten for a row that survives costs a miss, and the retry finds the keys still there,
// while an entry left for a row that is gone answers "replay" for a notification that no
// longer exists, and a retry would find no key left to forget (D51). The keys are read from the
// store, a page at a time, before the erasure removes them. A notification the producer sends
// for this recipient while the erasure runs is a new one, and its entry is the pre-check's own,
// bounded by its ttl.
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
	if err := a.forgetPreCheck(ctx, id); err != nil {
		return nil, fmt.Errorf("erase: %w", err)
	}
	counts, err := a.maintenance.Erase(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("erase: %w", err)
	}
	return counts, nil
}

// eraseKeyPage is how many of a recipient's idempotency keys are read, and forgotten, at a
// time, so that a recipient with a great many of them is not held in memory whole.
const eraseKeyPage = 500

// forgetPreCheck forgets the pre-check's entry for every idempotency key the store holds for
// id. A short page is the last; a full one is followed by another ask, which may come back
// empty. A store whose page does not move past the last key it was given would have the loop
// forget the same page for ever, so that is an error and not a loop.
func (a *Admin) forgetPreCheck(ctx context.Context, id domain.Identity) error {
	if a.pre == nil {
		return nil
	}
	after := ""
	for {
		keys, err := a.maintenance.IdemKeys(ctx, id, after, eraseKeyPage)
		if err != nil {
			return fmt.Errorf("list idempotency keys: %w", err)
		}
		if len(keys) == 0 {
			return nil
		}
		last := keys[len(keys)-1]
		if last == after {
			return fmt.Errorf("list idempotency keys: the store's page does not move past %q", after)
		}
		entries := make([]string, len(keys))
		for i, k := range keys {
			entries[i] = domain.PreCheckKey(id, k)
		}
		if err := a.pre.Forget(ctx, entries); err != nil {
			return fmt.Errorf("forget pre-check entries: %w", err)
		}
		if len(keys) < eraseKeyPage {
			return nil
		}
		after = last
	}
}
