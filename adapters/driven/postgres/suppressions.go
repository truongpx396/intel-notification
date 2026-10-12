package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// Suppressions implements ports.SuppressionStore over one pool. A suppression is
// keyed by a hash of the address, never the address (D35): the table has no column
// that could hold one, which is what lets it outlive the recipient's erasure.
type Suppressions struct {
	pool *pgxpool.Pool
	settings
}

var _ ports.SuppressionStore = (*Suppressions)(nil)

// NewSuppressions returns a Suppressions over pool.
func NewSuppressions(pool *pgxpool.Pool, opts ...Option) *Suppressions {
	return &Suppressions{pool: pool, settings: newSettings(opts)}
}

// addressHashLen is the length of a domain.SuppressionHash: a SHA-256.
const addressHashLen = 32

func checkSuppressionKey(realm domain.Realm, ch domain.ChannelKind, addressHash []byte) error {
	switch {
	case realm == "":
		return errors.New("suppression: realm is required")
	case ch == "":
		return errors.New("suppression: channel is required")
	case len(addressHash) != addressHashLen:
		return fmt.Errorf("suppression: address hash must be %d bytes, got %d", addressHashLen, len(addressHash))
	}
	return nil
}

// suppressedSQL asks whether the key has a suppression that has not lapsed at $4. A
// suppression ends AT its expiry, not after it.
const suppressedSQL = `
SELECT EXISTS (
    SELECT 1 FROM channel_suppressions
     WHERE realm = $1 AND channel = $2 AND address_hash = $3
       AND (expires_at IS NULL OR expires_at > $4))`

// Suppressed implements ports.SuppressionStore.
func (s *Suppressions) Suppressed(ctx context.Context, realm domain.Realm, ch domain.ChannelKind, addressHash []byte) (bool, error) {
	if err := checkSuppressionKey(realm, ch, addressHash); err != nil {
		return false, err
	}
	var live bool
	if err := s.pool.QueryRow(ctx, suppressedSQL, string(realm), string(ch), addressHash, s.clock.Now()).Scan(&live); err != nil {
		return false, fmt.Errorf("suppressed: %w", err)
	}
	return live, nil
}

// suppressSQL records a suppression, or strengthens the one that is there. A
// later record can only make a suppression last longer: a permanent one (no
// expiry) stays permanent whatever arrives, an expiring one becomes permanent when
// a permanent record does, and of two expiries the later wins. Otherwise a stale
// soft-bounce callback arriving after a complaint would quietly undo it, and the
// address would be mailed again.
//
// The reason follows the record that decided the expiry: a permanent suppression
// keeps its reason when only an expiring record follows it, and otherwise the
// latest reason is kept.
//
//	$1 realm  $2 channel  $3 address hash  $4 reason  $5 recorded at  $6 expires at, NULL for never
const suppressSQL = `
INSERT INTO channel_suppressions AS s (realm, channel, address_hash, reason, created_at, expires_at)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (realm, channel, address_hash) DO UPDATE
   SET reason     = CASE WHEN s.expires_at IS NULL AND EXCLUDED.expires_at IS NOT NULL
                         THEN s.reason ELSE EXCLUDED.reason END,
       expires_at = CASE WHEN s.expires_at IS NULL OR EXCLUDED.expires_at IS NULL
                         THEN NULL ELSE greatest(s.expires_at, EXCLUDED.expires_at) END`

// Suppress implements ports.SuppressionStore. The zero until is permanent (D14).
func (s *Suppressions) Suppress(ctx context.Context, realm domain.Realm, ch domain.ChannelKind, addressHash []byte,
	reason domain.SuppressionReason, until time.Time) error {
	if err := checkSuppressionKey(realm, ch, addressHash); err != nil {
		return err
	}
	if !reason.Valid() {
		return fmt.Errorf("suppression: %q is not a suppression reason", reason)
	}
	var expires *time.Time
	if !until.IsZero() {
		expires = &until
	}
	if _, err := s.pool.Exec(ctx, suppressSQL, string(realm), string(ch), addressHash, string(reason),
		s.clock.Now(), expires); err != nil {
		return fmt.Errorf("suppress: %w", err)
	}
	return nil
}
