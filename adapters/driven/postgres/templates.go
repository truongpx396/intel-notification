package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// Templates implements ports.TemplateRenderer and ports.TemplateWriter over notification_templates
// (D27, D55). What a template says and how it is checked is the domain's (domain.Template); this is
// the table: versions numbered and stored, one of them active, and the lookup down the locale ladder.
//
// Like topics, templates are realm configuration and not row-level scoped: the realm is in every
// predicate and is never a request's.
type Templates struct {
	pool *pgxpool.Pool
	settings
}

var (
	_ ports.TemplateRenderer = (*Templates)(nil)
	_ ports.TemplateWriter   = (*Templates)(nil)
)

// NewTemplates returns a Templates over pool.
func NewTemplates(pool *pgxpool.Pool, opts ...Option) *Templates {
	return &Templates{pool: pool, settings: newSettings(opts)}
}

// renderSQL reads every active candidate of the ladder in one query: the realm-wide template and the
// tenant's own at each locale. Which of them wins is the ladder's order and not the database's, so the
// rule lives in one place, in Go, beside the fake's.
const renderSQL = `
SELECT tenant_kind, tenant_id, locale, version, subject, body, data
  FROM notification_templates
 WHERE realm = $1 AND template_ref = $2 AND channel = $3 AND active AND locale = ANY($4)
   AND ((tenant_kind = '' AND tenant_id = '') OR (tenant_kind = $5 AND tenant_id = $6))`

// Render implements ports.TemplateRenderer: the request's locale, then its language, then the default
// locale, and at each the tenant's own template before the realm-wide one (D55).
func (t *Templates) Render(ctx context.Context, r domain.RenderRequest) (domain.RenderedContent, error) {
	ref := r.TemplateRef
	if ref == "" {
		ref = string(r.Topic)
	}
	ladder := domain.LocaleLadder(r.Locale, t.defaultLocale)
	rows, err := t.pool.Query(ctx, renderSQL, string(r.Realm), ref, string(r.Channel), ladder, r.Tenant.Kind, r.Tenant.ID)
	if err != nil {
		return domain.RenderedContent{}, fmt.Errorf("find template %q: %w", ref, err)
	}
	defer rows.Close()
	type key struct {
		tenant domain.Tenant
		locale string
	}
	found := map[key]domain.Template{}
	for rows.Next() {
		var tpl domain.Template
		var data []byte
		if err := rows.Scan(&tpl.Tenant.Kind, &tpl.Tenant.ID, &tpl.Locale, &tpl.Version, &tpl.Subject, &tpl.Body, &data); err != nil {
			return domain.RenderedContent{}, fmt.Errorf("find template %q: %w", ref, err)
		}
		if err := json.Unmarshal(data, &tpl.Data); err != nil {
			return domain.RenderedContent{}, fmt.Errorf("template %q version %d holds data that is not a JSON object: %w", ref, tpl.Version, err)
		}
		tpl.Ref, tpl.Channel = ref, r.Channel
		found[key{tpl.Tenant, tpl.Locale}] = tpl
	}
	if err := rows.Err(); err != nil {
		return domain.RenderedContent{}, fmt.Errorf("find template %q: %w", ref, err)
	}
	tenants := []domain.Tenant{{}}
	if r.Tenant != (domain.Tenant{}) {
		tenants = []domain.Tenant{r.Tenant, {}}
	}
	for _, locale := range ladder {
		for _, tenant := range tenants {
			if tpl, ok := found[key{tenant, locale}]; ok {
				return tpl.Render(r)
			}
		}
	}
	return domain.RenderedContent{}, fmt.Errorf("%w: realm %q, template %q, channel %q, locale %q",
		domain.ErrNoTemplate, r.Realm, ref, r.Channel, r.Locale)
}

// putTemplateSQL numbers the new version as one more than the highest stored, in the statement that
// stores it. Two saves racing for one number collide on the primary key, and the loser is told so and
// takes the next (putAttempts).
const putTemplateSQL = `
INSERT INTO notification_templates (realm, tenant_kind, tenant_id, template_ref, channel, locale, version, subject, body, data, created_at)
SELECT $1, $2, $3, $4, $5, $6, coalesce(max(version), 0) + 1, $7, $8, $9::jsonb, $10
  FROM notification_templates
 WHERE realm = $1 AND tenant_kind = $2 AND tenant_id = $3 AND template_ref = $4 AND channel = $5 AND locale = $6
RETURNING version`

// putAttempts bounds the retries of a save that lost a race for its version number. A save loses at
// most once to each save that was in flight with it, so this is the number of producers saving one
// template at the same instant that the call tolerates.
const putAttempts = 25

const uniqueViolation = "23505"

// PutTemplate implements ports.TemplateWriter.
func (t *Templates) PutTemplate(ctx context.Context, realm domain.Realm, tpl domain.Template) (domain.Template, error) {
	if err := realm.Validate(); err != nil {
		return domain.Template{}, fmt.Errorf("put template: %w", err)
	}
	if err := tpl.Validate(); err != nil {
		return domain.Template{}, err
	}
	tpl.Locale = domain.NormalizeLocale(tpl.Locale)
	data := tpl.Data
	if data == nil {
		data = map[string]any{}
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return domain.Template{}, fmt.Errorf("put template: the structured data is not JSON: %w", err)
	}
	for range putAttempts {
		err = t.pool.QueryRow(ctx, putTemplateSQL, string(realm), tpl.Tenant.Kind, tpl.Tenant.ID, tpl.Ref, string(tpl.Channel),
			tpl.Locale, tpl.Subject, tpl.Body, raw, t.clock.Now()).Scan(&tpl.Version)
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
			continue
		}
		if err != nil {
			return domain.Template{}, fmt.Errorf("put template %q: %w", tpl.Ref, err)
		}
		return tpl, nil
	}
	return domain.Template{}, fmt.Errorf("put template %q: %d attempts to take a version number each lost to another save", tpl.Ref, putAttempts)
}

// templateKeySQL names one template, all its versions: $1 realm, $2 tenant kind, $3 tenant id, $4 ref,
// $5 channel, $6 locale.
const templateKeySQL = `realm = $1 AND tenant_kind = $2 AND tenant_id = $3 AND template_ref = $4 AND channel = $5 AND locale = $6`

// ActivateTemplate implements ports.TemplateWriter. It runs in one transaction that first locks the
// template's versions, so two activations of one template are taken in turn, then turns the old one
// off and the new one on. A render sees the state before the commit or after it, so it finds the old
// version or the new one and never neither.
func (t *Templates) ActivateTemplate(ctx context.Context, realm domain.Realm, v domain.TemplateVersion) error {
	locale := domain.NormalizeLocale(v.Locale)
	args := []any{string(realm), v.Tenant.Kind, v.Tenant.ID, v.Ref, string(v.Channel), locale}
	notFound := fmt.Errorf("%w: realm %q, template %q, channel %q, locale %q, version %d",
		domain.ErrUnknownTemplate, realm, v.Ref, v.Channel, v.Locale, v.Version)
	err := pgx.BeginFunc(ctx, t.pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT version FROM notification_templates WHERE `+templateKeySQL+` ORDER BY version FOR UPDATE`, args...)
		if err != nil {
			return err
		}
		stored := false
		for rows.Next() {
			var version int
			if err := rows.Scan(&version); err != nil {
				rows.Close()
				return err
			}
			stored = stored || version == v.Version
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		if !stored {
			return notFound
		}
		withVersion := append(args[:6:6], v.Version)
		if _, err := tx.Exec(ctx, `UPDATE notification_templates SET active = false WHERE `+templateKeySQL+` AND active AND version <> $7`, withVersion...); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE notification_templates SET active = true WHERE `+templateKeySQL+` AND version = $7`, withVersion...)
		return err
	})
	if errors.Is(err, domain.ErrUnknownTemplate) {
		return err
	}
	if err != nil {
		return fmt.Errorf("activate template %q version %d: %w", v.Ref, v.Version, err)
	}
	return nil
}
