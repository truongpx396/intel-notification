package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// Providers implements ports.ProviderStore over channel_providers: which provider backs each channel, per
// realm or per tenant, and the reference to its credential (D29, D31). The table holds configuration an
// operator writes, and no recipient's data, so it is not row-level scoped. It never holds a credential:
// the schema refuses a secret_ref that is not a reference.
type Providers struct {
	pool *pgxpool.Pool
	settings
}

var _ ports.ProviderStore = (*Providers)(nil)

// NewProviders returns a Providers over pool.
func NewProviders(pool *pgxpool.Pool, opts ...Option) *Providers {
	return &Providers{pool: pool, settings: newSettings(opts)}
}

// providersSQL lists the enabled rows in the order a channel's providers are tried: the lowest priority
// first, and the provider's name to break a tie the same way every time.
const providersSQL = `
SELECT realm, tenant_kind, tenant_id, channel, provider, priority, config, secret_ref
  FROM channel_providers
 WHERE enabled
 ORDER BY realm, tenant_kind, tenant_id, channel, priority, provider`

// Providers implements ports.ProviderStore.
func (p *Providers) Providers(ctx context.Context) ([]domain.ChannelProvider, error) {
	rows, err := p.pool.Query(ctx, providersSQL)
	if err != nil {
		return nil, fmt.Errorf("read channel_providers: %w", err)
	}
	defer rows.Close()
	var out []domain.ChannelProvider
	for rows.Next() {
		var (
			cp                     domain.ChannelProvider
			realm, channel, config = "", "", []byte(nil)
			priority               int16
		)
		if err := rows.Scan(&realm, &cp.Tenant.Kind, &cp.Tenant.ID, &channel, &cp.Provider, &priority, &config, &cp.SecretRef); err != nil {
			return nil, fmt.Errorf("read channel_providers: %w", err)
		}
		cp.Realm, cp.Channel, cp.Priority = domain.Realm(realm), domain.ChannelKind(channel), int(priority)
		if err := json.Unmarshal(config, &cp.Config); err != nil {
			return nil, fmt.Errorf("channel_providers: the config of %s on %s in realm %s is not a JSON object: %w", cp.Provider, channel, realm, err)
		}
		out = append(out, cp)
	}
	return out, rows.Err()
}
