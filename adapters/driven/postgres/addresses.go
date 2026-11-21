package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// Addresses implements ports.AddressBook and ports.AddressWriter over recipient_addresses (NR-028,
// D25, D31). An address is a recipient's personal data, so every call runs in a transaction under the
// recipient's scope, as the inbox does: the policy is the guarantee, and the predicate lets the planner
// use the recipient's index (D17).
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

// resolveAddressesSQL lists a recipient's addresses on one channel in the order they were put.
const resolveAddressesSQL = `
SELECT value, locale, timezone, meta
  FROM recipient_addresses
 WHERE ` + identityPredicate + ` AND channel = $6
 ORDER BY updated_at, address_key`

// Resolve implements ports.AddressBook. An identity with none gets none and no error: a miss is
// terminal for the delivery, which the dispatcher records, and not a fault.
func (a *Addresses) Resolve(ctx context.Context, id domain.Identity, ch domain.ChannelKind) ([]domain.Address, error) {
	var out []domain.Address
	err := readScoped(ctx, a.pool, id, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, resolveAddressesSQL, append(identityArgs(id), string(ch))...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			addr := domain.Address{Channel: ch}
			var meta []byte
			if err := rows.Scan(&addr.Value, &addr.Locale, &addr.Timezone, &meta); err != nil {
				return err
			}
			if err := json.Unmarshal(meta, &addr.Meta); err != nil {
				return fmt.Errorf("the meta of an address is not a JSON object: %w", err)
			}
			if len(addr.Meta) == 0 {
				addr.Meta = nil
			}
			out = append(out, addr)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, fmt.Errorf("resolve %s addresses: %w", ch, err)
	}
	return out, nil
}

const (
	removeAddressesSQL = `
DELETE FROM recipient_addresses
 WHERE ` + identityPredicate + ` AND channel = $6 AND address_key <> ALL($7::text[])`

	// upsertAddressSQL keeps the row of an address already stored and gives it what this put carries.
	upsertAddressSQL = `
INSERT INTO recipient_addresses (realm, tenant_kind, tenant_id, recipient_kind, recipient_id, channel,
                                 address_key, value, locale, timezone, meta, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12)
ON CONFLICT (realm, tenant_kind, tenant_id, recipient_kind, recipient_id, channel, address_key) DO UPDATE
   SET value = EXCLUDED.value, locale = EXCLUDED.locale, timezone = EXCLUDED.timezone,
       meta = EXCLUDED.meta, updated_at = EXCLUDED.updated_at`
)

// PutAddresses implements ports.AddressWriter. What the table's own constraints would refuse, and the
// store would refuse later and less clearly, is refused first: an empty component of the identity, keys
// that do not pair with the addresses, a key twice, an address with no value.
func (a *Addresses) PutAddresses(ctx context.Context, id domain.Identity, ch domain.ChannelKind, addrs []domain.Address, keys []string) error {
	if err := id.Validate(); err != nil {
		return err
	}
	if ch == "" {
		return errors.New("put addresses: a channel is required")
	}
	if len(keys) != len(addrs) {
		return fmt.Errorf("put addresses: %d addresses with %d keys", len(addrs), len(keys))
	}
	seen := make(map[string]bool, len(keys))
	metas := make([][]byte, len(addrs))
	for i, addr := range addrs {
		switch {
		case addr.Value == "":
			return fmt.Errorf("put addresses: address %d has no value", i)
		case addr.Channel != "" && addr.Channel != ch:
			return fmt.Errorf("put addresses: address %d is for channel %q, put on %q", i, addr.Channel, ch)
		case seen[keys[i]]:
			return fmt.Errorf("put addresses: address %d has the same key as an earlier one", i)
		}
		seen[keys[i]] = true
		meta := addr.Meta
		if meta == nil {
			meta = map[string]string{}
		}
		raw, err := json.Marshal(meta)
		if err != nil {
			return fmt.Errorf("put addresses: address %d: %w", i, err)
		}
		metas[i] = raw
	}
	now := a.clock.Now()
	err := pgx.BeginFunc(ctx, a.pool, func(tx pgx.Tx) error {
		if err := setScope(ctx, tx, id); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, removeAddressesSQL, append(identityArgs(id), string(ch), nonNil(keys))...); err != nil {
			return err
		}
		for i, addr := range addrs {
			// A microsecond apart, so resolving them returns them in the order they were put.
			at := now.Add(time.Duration(i) * time.Microsecond)
			args := append(identityArgs(id), string(ch), keys[i], addr.Value, addr.Locale, addr.Timezone, metas[i], at)
			if _, err := tx.Exec(ctx, upsertAddressSQL, args...); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("put %s addresses: %w", ch, err)
	}
	return nil
}

func nonNil(ss []string) []string {
	if ss == nil {
		return []string{}
	}
	return ss
}
