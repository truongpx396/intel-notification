package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// Preferences implements ports.PreferenceStore over one pool, as the owner role,
// under the recipient's scope (D17). It reads the rows and writes them; which one
// wins is domain.ResolvePreferences's, the one place D30's precedence is written.
type Preferences struct {
	pool *pgxpool.Pool
	settings
}

var _ ports.PreferenceStore = (*Preferences)(nil)

// NewPreferences returns a Preferences over pool.
func NewPreferences(pool *pgxpool.Pool, opts ...Option) *Preferences {
	return &Preferences{pool: pool, settings: newSettings(opts)}
}

// The rows a topic's resolution needs: the recipient's own, under the recipient's
// scope, and the tenant's, which are not recipient data and carry the tenant in the
// predicate alone.
const (
	recipientPrefsSQL = `
SELECT channel, enabled FROM notification_preferences
 WHERE ` + identityPredicate + ` AND topic = $6`

	tenantPrefsSQL = `
SELECT channel, enabled, locked FROM notification_tenant_preferences
 WHERE realm = $1 AND tenant_kind = $2 AND tenant_id = $3 AND topic = $4`
)

// Resolve implements ports.PreferenceStore.
func (p *Preferences) Resolve(ctx context.Context, id domain.Identity, topic domain.Topic, def domain.TopicDef) ([]domain.ResolvedPreference, error) {
	recipient := map[domain.ChannelKind]bool{}
	tenant := map[domain.ChannelKind]domain.TenantChoice{}
	err := readScoped(ctx, p.pool, id, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, recipientPrefsSQL, append(identityArgs(id), string(topic))...)
		if err != nil {
			return err
		}
		for rows.Next() {
			var ch string
			var enabled bool
			if err := rows.Scan(&ch, &enabled); err != nil {
				rows.Close()
				return err
			}
			recipient[domain.ChannelKind(ch)] = enabled
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}

		rows, err = tx.Query(ctx, tenantPrefsSQL, string(id.Realm), id.Tenant.Kind, id.Tenant.ID, string(topic))
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var ch string
			var choice domain.TenantChoice
			if err := rows.Scan(&ch, &choice.Enabled, &choice.Locked); err != nil {
				return err
			}
			tenant[domain.ChannelKind(ch)] = choice
		}
		return rows.Err()
	})
	if err != nil {
		return nil, fmt.Errorf("resolve preferences: %w", err)
	}
	return domain.ResolvePreferences(topic, def, recipient, tenant), nil
}

// setPrefsSQL upserts a call's pairs in one statement: $7-$9 are parallel arrays,
// one entry per pair, with no pair twice.
const setPrefsSQL = `
INSERT INTO notification_preferences (realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
                                      topic, channel, enabled, updated_at)
SELECT $1::text, $2::text, $3::text, $4::text, $5::text, p.topic, p.channel, p.enabled, $6::timestamptz
  FROM unnest($7::text[], $8::text[], $9::boolean[]) AS p(topic, channel, enabled)
ON CONFLICT (realm, tenant_kind, tenant_id, recipient_kind, recipient_id, topic, channel)
DO UPDATE SET enabled = EXCLUDED.enabled, updated_at = EXCLUDED.updated_at`

// Set implements ports.PreferenceStore. Every pair is checked before any is written,
// and the write is one statement, so a call is stored whole or not at all. A pair
// named twice takes the later.
func (p *Preferences) Set(ctx context.Context, id domain.Identity, prefs []domain.Preference) error {
	if err := id.Validate(); err != nil {
		return fmt.Errorf("set preferences: %w", err)
	}
	type pair struct{ topic, channel string }
	enabled := map[pair]bool{}
	var order []pair
	for _, pref := range prefs {
		if err := pref.Validate(); err != nil {
			return fmt.Errorf("set preferences: %w", err)
		}
		k := pair{string(pref.Topic), string(pref.Channel)}
		if _, seen := enabled[k]; !seen {
			order = append(order, k)
		}
		enabled[k] = pref.Enabled
	}
	if len(order) == 0 {
		return nil
	}
	topics, channels, flags := make([]string, len(order)), make([]string, len(order)), make([]bool, len(order))
	for i, k := range order {
		topics[i], channels[i], flags[i] = k.topic, k.channel, enabled[k]
	}
	err := pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		if err := setScope(ctx, tx, id); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, setPrefsSQL, append(identityArgs(id), p.clock.Now(), topics, channels, flags)...)
		return err
	})
	if err != nil {
		return fmt.Errorf("set preferences: %w", err)
	}
	return nil
}

const setTenantPrefSQL = `
INSERT INTO notification_tenant_preferences (realm, tenant_kind, tenant_id, topic, channel, enabled, locked, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (realm, tenant_kind, tenant_id, topic, channel)
DO UPDATE SET enabled = EXCLUDED.enabled, locked = EXCLUDED.locked, updated_at = EXCLUDED.updated_at`

// SetTenant implements ports.PreferenceStore.
func (p *Preferences) SetTenant(ctx context.Context, realm domain.Realm, tp domain.TenantPreference) error {
	if realm == "" {
		return errors.New("set tenant preference: realm is required")
	}
	if err := tp.Validate(); err != nil {
		return fmt.Errorf("set tenant preference: %w", err)
	}
	if _, err := p.pool.Exec(ctx, setTenantPrefSQL, string(realm), tp.Tenant.Kind, tp.Tenant.ID,
		string(tp.Topic), string(tp.Channel), tp.Enabled, tp.Locked, p.clock.Now()); err != nil {
		return fmt.Errorf("set tenant preference: %w", err)
	}
	return nil
}

// scheduleSQL reads a recipient's schedule. A clock time is read as HH24:MI:SS, and
// the digest window as whole microseconds.
const scheduleSQL = `
SELECT coalesce(to_char(quiet_start, 'HH24:MI:SS'), ''), coalesce(to_char(quiet_end, 'HH24:MI:SS'), ''),
       timezone, (extract(epoch FROM digest_window) * 1000000)::bigint
  FROM notification_schedules
 WHERE ` + identityPredicate

// Schedule implements ports.PreferenceStore. A recipient with no row has the zero
// schedule: immediate delivery, no quiet hours.
func (p *Preferences) Schedule(ctx context.Context, id domain.Identity) (domain.DeliverySchedule, error) {
	var s domain.DeliverySchedule
	err := readScoped(ctx, p.pool, id, func(tx pgx.Tx) error {
		var start, end string
		var micros int64
		err := tx.QueryRow(ctx, scheduleSQL, identityArgs(id)...).Scan(&start, &end, &s.Timezone, &micros)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		s.QuietStart, s.QuietEnd = clockTime(start), clockTime(end)
		s.Digest = time.Duration(micros) * time.Microsecond
		return nil
	})
	if err != nil {
		return domain.DeliverySchedule{}, fmt.Errorf("schedule: %w", err)
	}
	return s, nil
}

// clockTime is a stored time as the schedule writes it: "22:00", or with seconds
// when they are not zero.
func clockTime(hms string) string {
	return strings.TrimSuffix(hms, ":00")
}

const setScheduleSQL = `
INSERT INTO notification_schedules (realm, tenant_kind, tenant_id, recipient_kind, recipient_id,
                                    quiet_start, quiet_end, timezone, digest_window, updated_at)
VALUES ($1, $2, $3, $4, $5, NULLIF($6::text, '')::time, NULLIF($7::text, '')::time, $8,
        $9::bigint * interval '1 microsecond', $10)
ON CONFLICT (realm, tenant_kind, tenant_id, recipient_kind, recipient_id)
DO UPDATE SET quiet_start = EXCLUDED.quiet_start, quiet_end = EXCLUDED.quiet_end,
              timezone = EXCLUDED.timezone, digest_window = EXCLUDED.digest_window,
              updated_at = EXCLUDED.updated_at`

// SetSchedule implements ports.PreferenceStore. It replaces the whole schedule; it
// does not merge. Whether a quiet-hours time or a zone is well formed is the
// planner's to judge (T021); the database refuses a time that is not a time.
func (p *Preferences) SetSchedule(ctx context.Context, id domain.Identity, s domain.DeliverySchedule) error {
	if err := id.Validate(); err != nil {
		return fmt.Errorf("set schedule: %w", err)
	}
	if s.Digest < 0 {
		return fmt.Errorf("set schedule: digest window %v is negative", s.Digest)
	}
	err := pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		if err := setScope(ctx, tx, id); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, setScheduleSQL, append(identityArgs(id), s.QuietStart, s.QuietEnd, s.Timezone,
			s.Digest.Microseconds(), p.clock.Now())...)
		return err
	})
	if err != nil {
		return fmt.Errorf("set schedule: %w", err)
	}
	return nil
}
