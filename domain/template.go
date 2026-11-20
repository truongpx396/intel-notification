package domain

import (
	"errors"
	"fmt"
	"io"
	"maps"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"text/template"
	"text/template/parse"
	"unicode"
	"unicode/utf8"
)

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

// templateFuncs is the whole of what a template may call that text/template does not already hold. It
// is a closed list: adding to it is a decision about what a producer's text may do (D55). None reads
// the environment, a file, the clock or the network, and all are pure functions of their arguments.
var templateFuncs = template.FuncMap{
	"upper":    func(v any) string { return strings.ToUpper(text(v)) },
	"lower":    func(v any) string { return strings.ToLower(text(v)) },
	"title":    func(v any) string { return titleCase(text(v)) },
	"trim":     func(v any) string { return strings.TrimSpace(text(v)) },
	"join":     joinList,
	"default":  func(def, v any) any { return orDefaultValue(def, v) },
	"truncate": truncate,
}

// allowedBuiltins are the functions text/template defines that a template may call. `call` is not one:
// it runs a function the data carries, which is code the map never named.
var allowedBuiltins = []string{
	"and", "or", "not", "eq", "ne", "lt", "le", "gt", "ge", "len", "index", "slice",
	"print", "printf", "println", "html", "urlquery", "js",
}

// AllowedTemplateFunctions names every function a template may call, builtins included, sorted.
func AllowedTemplateFunctions() []string {
	names := slices.Clone(allowedBuiltins)
	for name := range templateFuncs {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

func text(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	}
	return fmt.Sprint(v)
}

// titleCase upper-cases the first letter of each word and leaves the rest alone.
func titleCase(s string) string {
	var b strings.Builder
	start := true
	for _, r := range s {
		if start && unicode.IsLetter(r) {
			r = unicode.ToTitle(r)
		}
		start = !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '\''
		b.WriteRune(r)
	}
	return b.String()
}

func joinList(sep string, list any) (string, error) {
	switch l := list.(type) {
	case nil:
		return "", nil
	case []string:
		return strings.Join(l, sep), nil
	case []any:
		parts := make([]string, len(l))
		for i, e := range l {
			parts[i] = text(e)
		}
		return strings.Join(parts, sep), nil
	}
	return "", fmt.Errorf("join: %T is not a list", list)
}

// truncate keeps the first n characters of s, never a part of one.
func truncate(n int, v any) string {
	s := text(v)
	if n < 0 {
		n = 0
	}
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	return string([]rune(s)[:n])
}

// orDefaultValue is v unless it is empty: absent, "", false, zero, or a list or map with nothing in it.
func orDefaultValue(def, v any) any {
	if v == nil {
		return def
	}
	rv := reflect.ValueOf(v)
	if rv.IsZero() {
		return def
	}
	switch rv.Kind() {
	case reflect.Slice, reflect.Map, reflect.Array:
		if rv.Len() == 0 {
			return def
		}
	}
	return v
}

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidTemplate, fmt.Sprintf(format, args...))
}

// parseTemplate parses text under the restricted map and checks what the parser lets through.
func parseTemplate(name, body string) (*template.Template, error) {
	if len(body) > MaxTemplateBytes {
		return nil, invalid("%s is %d bytes, the limit is %d", name, len(body), MaxTemplateBytes)
	}
	t, err := template.New(name).Option("missingkey=error").Funcs(templateFuncs).Parse(body)
	if err != nil {
		return nil, invalid("%s: %v", name, err)
	}
	for _, each := range t.Templates() {
		if each.Tree == nil {
			continue
		}
		if err := checkNode(each.Tree.Root); err != nil {
			return nil, invalid("%s: %v", name, err)
		}
	}
	return t, nil
}

// printfDirective is the part of a printf directive that can carry a width or a precision: everything
// from a percent sign to its verb.
var (
	printfDirective = regexp.MustCompile(`%[^a-zA-Z%]*`)
	longNumber      = regexp.MustCompile(`\d{4,}`)
)

// checkNode walks a parsed template for what the function map alone does not stop. The parser accepts
// every builtin, so `call` and an unbounded printf are refused here.
func checkNode(n parse.Node) error {
	switch n := n.(type) {
	case nil:
		return nil
	case *parse.ListNode:
		if n == nil {
			return nil
		}
		for _, c := range n.Nodes {
			if err := checkNode(c); err != nil {
				return err
			}
		}
	case *parse.ActionNode:
		return checkNode(n.Pipe)
	case *parse.PipeNode:
		if n == nil {
			return nil
		}
		for _, c := range n.Cmds {
			if err := checkNode(c); err != nil {
				return err
			}
		}
	case *parse.IfNode:
		return checkBranch(&n.BranchNode)
	case *parse.RangeNode:
		return checkBranch(&n.BranchNode)
	case *parse.WithNode:
		return checkBranch(&n.BranchNode)
	case *parse.TemplateNode:
		return checkNode(n.Pipe)
	case *parse.ChainNode:
		return checkNode(n.Node)
	case *parse.CommandNode:
		for i, arg := range n.Args {
			if id, ok := arg.(*parse.IdentifierNode); ok && id.Ident == "printf" {
				if i != 0 {
					return errors.New("printf is called with no format")
				}
				if err := checkPrintf(n.Args[1:]); err != nil {
					return err
				}
			}
			if err := checkNode(arg); err != nil {
				return err
			}
		}
	case *parse.IdentifierNode:
		if n.Ident == "call" {
			return errors.New(`"call" is not allowed: it runs a function the data carries`)
		}
	}
	return nil
}

func checkBranch(b *parse.BranchNode) error {
	for _, c := range []parse.Node{b.Pipe, b.List, b.ElseList} {
		if err := checkNode(c); err != nil {
			return err
		}
	}
	return nil
}

// checkPrintf allows printf only with a format written in the template whose widths and precisions are
// written in it too and are short: a width taken from an argument or from the data is a way to ask for
// a gigabyte.
func checkPrintf(args []parse.Node) error {
	if len(args) == 0 {
		return errors.New("printf is called with no format")
	}
	format, ok := args[0].(*parse.StringNode)
	if !ok {
		return errors.New("printf takes a format written in the template, not one computed")
	}
	for _, d := range printfDirective.FindAllString(format.Text, -1) {
		if strings.Contains(d, "*") || longNumber.MatchString(d) {
			return fmt.Errorf("printf format %q has a width or precision that is not a short number", format.Text)
		}
	}
	return nil
}

// CheckTemplate reports whether text is a template the engine will run: it parses, and calls only
// functions of the restricted map (D55). The error wraps ErrInvalidTemplate.
func CheckTemplate(text string) error {
	_, err := parseTemplate("template", text)
	return err
}

// errTooLarge is what the bounded writer refuses with.
var errTooLarge = errors.New("the output is too large")

// boundedWriter accepts at most limit bytes.
type boundedWriter struct {
	b     strings.Builder
	limit int
}

func (w *boundedWriter) Write(p []byte) (int, error) {
	if w.b.Len()+len(p) > w.limit {
		return 0, errTooLarge
	}
	return w.b.Write(p)
}

var _ io.Writer = (*boundedWriter)(nil)

// ExecuteTemplate runs text over data and returns what it wrote. A key the data does not hold is an
// error and not "<no value>", and the output is bounded by MaxRenderedBytes.
func ExecuteTemplate(name, body string, data any) (string, error) {
	t, err := parseTemplate(name, body)
	if err != nil {
		return "", err
	}
	w := &boundedWriter{limit: MaxRenderedBytes}
	if err := t.Execute(w, data); err != nil {
		if errors.Is(err, errTooLarge) {
			return "", invalid("%s writes more than %d bytes", name, MaxRenderedBytes)
		}
		return "", fmt.Errorf("render %s: %w", name, err)
	}
	return w.b.String(), nil
}

// Validate reports every rule t breaks, as a *ValidationError wrapping ErrInvalidTemplate, or nil.
func (t Template) Validate() error {
	var v ValidationError
	if t.Ref == "" {
		v.add("Ref", "is required: nothing could name the template", ErrInvalidTemplate)
	}
	if t.Channel == "" {
		v.add("Channel", "is required: nothing could name the template", ErrInvalidTemplate)
	}
	if NormalizeLocale(t.Locale) == "" {
		v.add("Locale", fmt.Sprintf("%q is not a language tag such as en or fr-CA", t.Locale), ErrInvalidTemplate)
	}
	if (t.Tenant.Kind == "") != (t.Tenant.ID == "") {
		v.add("Tenant", "kind and id must both be set, or both empty for a realm-wide template", ErrInvalidTemplate)
	}
	if t.Subject == "" && t.Body == "" {
		v.add("Body", "a template with neither a subject nor a body would deliver nothing", ErrInvalidTemplate)
	}
	if err := CheckTemplate(t.Subject); err != nil {
		v.add("Subject", err.Error(), ErrInvalidTemplate)
	}
	if err := CheckTemplate(t.Body); err != nil {
		v.add("Body", err.Error(), ErrInvalidTemplate)
	}
	if err := checkData(t.Data); err != nil {
		v.add("Data", err.Error(), ErrInvalidTemplate)
	}
	return v.err()
}

// checkData checks every string of the structured content, which is a template.
func checkData(v any) error {
	switch x := v.(type) {
	case string:
		return CheckTemplate(x)
	case map[string]any:
		for _, k := range slices.Sorted(maps.Keys(x)) {
			if err := checkData(x[k]); err != nil {
				return fmt.Errorf("%q: %w", k, err)
			}
		}
	case []any:
		for i, e := range x {
			if err := checkData(e); err != nil {
				return fmt.Errorf("[%d]: %w", i, err)
			}
		}
	}
	return nil
}

// Render returns the content of t for req: its subject and body, and its structured data, each string
// run as a template over the request's data (or, for a digest, over its members).
func (t Template) Render(req RenderRequest) (RenderedContent, error) {
	root := renderRoot(req)
	subject, err := ExecuteTemplate("subject", t.Subject, root)
	if err != nil {
		return RenderedContent{}, err
	}
	body, err := ExecuteTemplate("body", t.Body, root)
	if err != nil {
		return RenderedContent{}, err
	}
	var data map[string]any
	if len(t.Data) > 0 {
		rendered, err := renderData(t.Data, root)
		if err != nil {
			return RenderedContent{}, err
		}
		data = rendered.(map[string]any)
	}
	return RenderedContent{Subject: subject, Body: body, Data: data, TemplateVersion: t.Version}, nil
}

// renderRoot is what a template's dot is: the notification's data, or for a digest a list of its
// members under "digest", each with its own data. A digest has no single Data.
func renderRoot(req RenderRequest) map[string]any {
	if len(req.Digest) == 0 {
		if req.Data == nil {
			return map[string]any{}
		}
		return req.Data
	}
	members := make([]any, len(req.Digest))
	for i, n := range req.Digest {
		data := n.Data
		if data == nil {
			data = map[string]any{}
		}
		members[i] = map[string]any{"topic": string(n.Topic), "title": n.Title, "body": n.Body, "data": data, "payload": n.Payload}
	}
	return map[string]any{"digest": members}
}

// renderData returns a copy of v with every string run as a template over root.
func renderData(v any, root map[string]any) (any, error) {
	switch x := v.(type) {
	case string:
		return ExecuteTemplate("data", x, root)
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			r, err := renderData(e, root)
			if err != nil {
				return nil, err
			}
			out[k] = r
		}
		return out, nil
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			r, err := renderData(e, root)
			if err != nil {
				return nil, err
			}
			out[i] = r
		}
		return out, nil
	}
	return v, nil
}

// localePattern is a BCP 47 language tag as far as the engine needs it: a language of two or three
// letters and subtags of up to eight letters or digits.
var localePattern = regexp.MustCompile(`^[A-Za-z]{2,3}(-[A-Za-z0-9]{1,8})*$`)

// NormalizeLocale returns locale lowercased, or "" when it is not a language tag. Tags compare without
// regard to case, so the lowercase form is the one that is stored and matched.
func NormalizeLocale(locale string) string {
	if !localePattern.MatchString(locale) {
		return ""
	}
	return strings.ToLower(locale)
}

// LocaleLadder is the locales to try for a request, most specific first: the tag, its language, then
// the default, without repeats. A tag that is not a language tag is skipped.
func LocaleLadder(locale, def string) []string {
	var ladder []string
	add := func(l string) {
		if l != "" && !slices.Contains(ladder, l) {
			ladder = append(ladder, l)
		}
	}
	tag := NormalizeLocale(locale)
	add(tag)
	if language, _, hasRegion := strings.Cut(tag, "-"); hasRegion {
		add(language)
	}
	add(NormalizeLocale(def))
	return ladder
}
