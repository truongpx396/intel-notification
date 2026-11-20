package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// Templates implements ports.TemplateRenderer and ports.TemplateWriter over notification_templates
// (D27, D55).
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

// Render implements ports.TemplateRenderer.
func (t *Templates) Render(context.Context, domain.RenderRequest) (domain.RenderedContent, error) {
	return domain.RenderedContent{}, errNotImplemented
}

// PutTemplate implements ports.TemplateWriter.
func (t *Templates) PutTemplate(context.Context, domain.Realm, domain.Template) (domain.Template, error) {
	return domain.Template{}, errNotImplemented
}

// ActivateTemplate implements ports.TemplateWriter.
func (t *Templates) ActivateTemplate(context.Context, domain.Realm, domain.TemplateVersion) error {
	return errNotImplemented
}
