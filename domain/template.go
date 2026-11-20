package domain

import "errors"

// ErrInvalidTemplate is wrapped by every refusal of a template's text or shape: a function outside the
// restricted map, a syntax error, an output that would be too large (D55). It is returned when a
// template is saved, so a producer learns of a bad template from the call that stored it and not from
// a delivery that failed hours later.
var ErrInvalidTemplate = errors.New("notify: invalid template")

// Template is one stored version of the copy for (ref, channel, locale), realm-wide or for one tenant
// (NR-019, D27). A version is immutable once written: a change is a new version, activated by an
// explicit call, so an in-flight retry never renders half of an edit.
type Template struct {
	// Tenant is the zero value for a realm-wide template, and a tenant for its override: that tenant's
	// branding (D27).
	Tenant  Tenant
	Ref     string
	Channel ChannelKind
	// Locale is a BCP 47 tag. It is compared without regard to case.
	Locale string
	// Version is assigned by the store on Put and is 0 before then.
	Version int
	Subject string
	// Body is Go template text over the restricted function map (D55).
	Body string
	// Data is structured content for rich channels. Every string in it is a template too.
	Data map[string]any
}

// TemplateVersion names one stored version of a template, as an activation does.
type TemplateVersion struct {
	Tenant  Tenant
	Ref     string
	Channel ChannelKind
	Locale  string
	Version int
}

// Limits on a template's size and what it may produce. They bound a mistake and are not a sandbox
// against a hostile author (D55).
const (
	MaxTemplateBytes = 64 << 10
	MaxRenderedBytes = 256 << 10
)

// AllowedTemplateFunctions names every function a template may call, builtins included, sorted.
func AllowedTemplateFunctions() []string { return nil }

// CheckTemplate reports whether text is a template the engine will run: it parses, and calls only
// functions of the restricted map (D55). The error wraps ErrInvalidTemplate.
func CheckTemplate(text string) error { return errors.New("not implemented") }

// ExecuteTemplate runs text over data and returns what it wrote. A key the data does not hold is an
// error and not "<no value>", and the output is bounded by MaxRenderedBytes.
func ExecuteTemplate(name, text string, data any) (string, error) {
	return "", errors.New("not implemented")
}

// Validate reports every rule t breaks, as a *ValidationError wrapping ErrInvalidTemplate, or nil.
func (t Template) Validate() error { return errors.New("not implemented") }

// Render returns the content of t for req: its subject and body, and its structured data, each string
// run as a template over the request's data (or, for a digest, over its members).
func (t Template) Render(req RenderRequest) (RenderedContent, error) {
	return RenderedContent{}, errors.New("not implemented")
}

// NormalizeLocale returns locale lowercased, or "" when it is not a language tag.
func NormalizeLocale(locale string) string { return "?" }

// LocaleLadder is the locales to try for a request, most specific first: the tag, its language, then
// the default, without repeats.
func LocaleLadder(locale, def string) []string { return nil }
