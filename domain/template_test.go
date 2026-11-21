package domain_test

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/truongpx396/intel-notification/domain"
)

// T048, NR-019, D55. A template is code a producer wrote and the engine runs: the function map it may
// call is closed, and a template that reaches outside it is refused when it is saved.

// A function outside the restricted map is refused at save time, whatever the function would have done.
// The names are the ones a template author would reach for: the environment, the file system, a
// process, the clock, the network.
func TestCheckTemplateRefusesAFunctionOutsideTheMap(t *testing.T) {
	t.Parallel()
	for _, fn := range []string{"env", "getenv", "readFile", "exec", "include", "now", "httpGet", "expandenv"} {
		t.Run(fn, func(t *testing.T) {
			t.Parallel()
			err := domain.CheckTemplate("Hello {{ " + fn + " \"X\" }}")
			if !errors.Is(err, domain.ErrInvalidTemplate) {
				t.Fatalf("CheckTemplate(%q) = %v, want ErrInvalidTemplate: %s is not in the restricted map", fn, err, fn)
			}
		})
	}
}

func TestCheckTemplateAcceptsTheRestrictedMapAndBuiltins(t *testing.T) {
	t.Parallel()
	for _, text := range []string{
		"plain text, no actions",
		"Hello {{ .name }}",
		"{{ .name | upper | trim }}",
		`{{ default "friend" .name }}`,
		`{{ join ", " .items }}`,
		`{{ truncate 10 .title }}`,
		`{{ if eq .plan "pro" }}Pro{{ else }}Free{{ end }}`,
		`{{ range .items }}{{ . }};{{ end }}`,
		`{{ printf "%.2f" .amount }}`,
		`{{ printf "%-10s|" .name }}`,
		`{{ len .items }} item(s)`,
		`{{ define "x" }}inner{{ end }}{{ template "x" }}`,
	} {
		t.Run(text, func(t *testing.T) {
			t.Parallel()
			if err := domain.CheckTemplate(text); err != nil {
				t.Fatalf("CheckTemplate(%q) = %v, want nil", text, err)
			}
		})
	}
}

// What the map does not name is refused even where text/template would allow it: `call` runs a value a
// host's data may carry, and a printf width the author did not write down is a way to ask for a
// gigabyte.
func TestCheckTemplateRefusesWhatTheBuiltinsWouldAllow(t *testing.T) {
	t.Parallel()
	cases := []struct{ name, text, why string }{
		{"call", `{{ call .fn }}`, "call runs a function the data carries, which is code the map never named"},
		{"a syntax error", `{{ .name `, "a template that does not parse could never render"},
		{"an unknown action", `{{ nonsense }}`, "an identifier that is no function"},
		{"printf with a dynamic format", `{{ printf .format .x }}`, "the format is the width's source"},
		{"printf with a huge width", `{{ printf "%999999999d" .x }}`, "one call would allocate a gigabyte"},
		{"printf with a star width", `{{ printf "%*d" 5 .x }}`, "a width taken from an argument is not written down"},
		{"printf with a huge precision", `{{ printf "%.99999999f" .x }}`, "precision is a width too"},
		{"too large", strings.Repeat("a", domain.MaxTemplateBytes+1), "a template is text a person wrote"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if err := domain.CheckTemplate(tc.text); !errors.Is(err, domain.ErrInvalidTemplate) {
				t.Fatalf("CheckTemplate = %v, want ErrInvalidTemplate: %s", err, tc.why)
			}
		})
	}
}

func TestAllowedTemplateFunctionsNamesTheMap(t *testing.T) {
	t.Parallel()
	got := domain.AllowedTemplateFunctions()
	for _, want := range []string{"upper", "lower", "title", "trim", "join", "default", "truncate", "eq", "len", "printf", "index"} {
		if !slices.Contains(got, want) {
			t.Errorf("AllowedTemplateFunctions() does not name %q: %v", want, got)
		}
	}
	for _, banned := range []string{"call", "env", "readFile", "exec", "now"} {
		if slices.Contains(got, banned) {
			t.Errorf("AllowedTemplateFunctions() names %q, which a template must not call", banned)
		}
	}
	if !slices.IsSorted(got) {
		t.Errorf("AllowedTemplateFunctions() = %v, want it sorted so a document built from it is stable", got)
	}
}

func TestExecuteTemplate(t *testing.T) {
	t.Parallel()
	data := map[string]any{
		"name": "  Ada ", "plan": "pro", "amount": 1250.5,
		"items": []any{"a", "b", "c"}, "title": "A very long subject line",
	}
	cases := []struct {
		name, text, want string
	}{
		{"a variable", "Hello {{ .name }}!", "Hello   Ada !"},
		{"a pipeline over the map", "{{ .name | trim | upper }}", "ADA"},
		{"lower", "{{ lower \"MiXeD\" }}", "mixed"},
		{"title", "{{ title \"invoice is overdue\" }}", "Invoice Is Overdue"},
		{"default, taken", `{{ default "friend" "" }}`, "friend"},
		{"default, not taken", `{{ default "friend" .plan }}`, "pro"},
		{"join over the data's own list", `{{ join ", " .items }}`, "a, b, c"},
		{"truncate counts characters", `{{ truncate 6 .title }}`, "A very"},
		{"truncate leaves a short string", `{{ truncate 60 .plan }}`, "pro"},
		{"truncate does not cut a multibyte character", `{{ truncate 2 "héllo" }}`, "hé"},
		{"a number formatted by the author", `{{ printf "%.2f" .amount }}`, "1250.50"},
		{"a conditional", `{{ if eq .plan "pro" }}Pro{{ else }}Free{{ end }}`, "Pro"},
		{"a range", `{{ range .items }}[{{ . }}]{{ end }}`, "[a][b][c]"},
		{"a defined template", `{{ define "sig" }}-- {{ .plan }}{{ end }}Hi {{ template "sig" . }}`, "Hi -- pro"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := domain.ExecuteTemplate("t", tc.text, data)
			if err != nil || got != tc.want {
				t.Fatalf("ExecuteTemplate(%q) = %q, %v, want %q", tc.text, got, err, tc.want)
			}
		})
	}
}

// A variable the producer did not send is the producer's bug, and the copy says so by failing: an email
// that reads "Dear <no value>" is a worse outcome than a delivery that is retried and then dead-lettered
// with the reason.
func TestExecuteTemplateRefusesAMissingKey(t *testing.T) {
	t.Parallel()
	got, err := domain.ExecuteTemplate("t", "Dear {{ .name }}", map[string]any{"other": 1})
	if err == nil {
		t.Fatalf("ExecuteTemplate = %q, nil, want an error naming the missing key", got)
	}
	if !strings.Contains(err.Error(), "name") {
		t.Fatalf("error = %v, want it to name the key %q", err, "name")
	}
}

func TestExecuteTemplateBoundsItsOutput(t *testing.T) {
	t.Parallel()
	text := `{{ range .items }}` + strings.Repeat("x", 1024) + `{{ end }}`
	items := make([]any, 1024) // 1 MiB of output, four times the bound
	got, err := domain.ExecuteTemplate("t", text, map[string]any{"items": items})
	if err == nil {
		t.Fatalf("ExecuteTemplate wrote %d bytes, want an error: the bound is %d", len(got), domain.MaxRenderedBytes)
	}
	if !errors.Is(err, domain.ErrInvalidTemplate) {
		t.Fatalf("err = %v, want it to wrap ErrInvalidTemplate: the template is what is too large", err)
	}
}

// Nothing a producer's data holds is read as template text: only what the author wrote is run.
func TestExecuteTemplateDoesNotRunTheData(t *testing.T) {
	t.Parallel()
	got, err := domain.ExecuteTemplate("t", "{{ .name }}", map[string]any{"name": "{{ env \"HOME\" }}"})
	if err != nil || got != `{{ env "HOME" }}` {
		t.Fatalf("ExecuteTemplate = %q, %v, want the data's text unchanged", got, err)
	}
}

func TestTemplateValidate(t *testing.T) {
	t.Parallel()
	valid := func() domain.Template {
		return domain.Template{Ref: "invoice", Channel: "email", Locale: "en-US", Subject: "Invoice {{ .number }}", Body: "Pay {{ .amount }}"}
	}
	cases := []struct {
		name   string
		mutate func(*domain.Template)
		bad    bool
		why    string
	}{
		{"a valid realm-wide template", func(*domain.Template) {}, false, ""},
		{"a valid tenant override", func(t *domain.Template) { t.Tenant = domain.Tenant{Kind: "workspace", ID: "w1"} }, false, ""},
		{"a template with a subject only", func(t *domain.Template) { t.Body = "" }, false, "a push notification is a title and no body"},
		{"no ref", func(t *domain.Template) { t.Ref = "" }, true, "nothing could name it"},
		{"no channel", func(t *domain.Template) { t.Channel = "" }, true, "nothing could name it"},
		{"no locale", func(t *domain.Template) { t.Locale = "" }, true, "the ladder needs a rung to find it on"},
		{"a locale that is no tag", func(t *domain.Template) { t.Locale = "en US" }, true, ""},
		{"neither subject nor body", func(t *domain.Template) { t.Subject, t.Body = "", "" }, true, "it would deliver nothing"},
		{"half a tenant", func(t *domain.Template) { t.Tenant = domain.Tenant{Kind: "workspace"} }, true, "a tenant is a kind and an id, as the schema's check says"},
		{"a function outside the map in the subject", func(t *domain.Template) { t.Subject = `{{ env "HOME" }}` }, true, "the subject is a template"},
		{"a function outside the map in the body", func(t *domain.Template) { t.Body = `{{ readFile "/etc/passwd" }}` }, true, "the body is a template"},
		{"a function outside the map in the data", func(t *domain.Template) {
			t.Data = map[string]any{"blocks": []any{map[string]any{"text": `{{ env "X" }}`}}}
		}, true, "every string of the structured content is a template"},
		{"a version already set is not a rule", func(t *domain.Template) { t.Version = 7 }, false, "the store assigns it"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tpl := valid()
			tc.mutate(&tpl)
			err := tpl.Validate()
			if tc.bad && !errors.Is(err, domain.ErrInvalidTemplate) {
				t.Fatalf("Validate = %v, want ErrInvalidTemplate: %s", err, tc.why)
			}
			if !tc.bad && err != nil {
				t.Fatalf("Validate = %v, want nil", err)
			}
		})
	}
}

func TestTemplateValidateNamesEveryProblem(t *testing.T) {
	t.Parallel()
	err := (domain.Template{Subject: `{{ env "X" }}`}).Validate()
	var v *domain.ValidationError
	if !errors.As(err, &v) {
		t.Fatalf("Validate = %v, want a *ValidationError", err)
	}
	fields := map[string]bool{}
	for _, p := range v.Problems {
		fields[p.Field] = true
	}
	for _, want := range []string{"Ref", "Channel", "Locale", "Subject"} {
		if !fields[want] {
			t.Errorf("no problem names %s: %v", want, v.Problems)
		}
	}
}

func TestTemplateRender(t *testing.T) {
	t.Parallel()
	tpl := domain.Template{
		Ref: "invoice", Channel: "slack", Locale: "en", Version: 3,
		Subject: "Invoice {{ .number }}",
		Body:    "Pay {{ printf \"%.2f\" .amount }} {{ upper .currency }}",
		Data: map[string]any{
			"blocks": []any{map[string]any{"text": "Due: {{ .number }}", "n": 2.0}},
			"static": "kept",
		},
	}
	got, err := tpl.Render(domain.RenderRequest{Data: map[string]any{"number": "INV-42", "amount": 1250.0, "currency": "eur"}})
	if err != nil {
		t.Fatal(err)
	}
	if got.Subject != "Invoice INV-42" || got.Body != "Pay 1250.00 EUR" || got.TemplateVersion != 3 {
		t.Fatalf("Render = %+v, want subject, body and the version rendered", got)
	}
	blocks, _ := got.Data["blocks"].([]any)
	if len(blocks) != 1 {
		t.Fatalf("Data = %v, want the structured content kept", got.Data)
	}
	block, _ := blocks[0].(map[string]any)
	if block["text"] != "Due: INV-42" || block["n"] != 2.0 || got.Data["static"] != "kept" {
		t.Fatalf("Data = %v, want its strings rendered and its other values kept", got.Data)
	}
	if tpl.Data["blocks"].([]any)[0].(map[string]any)["text"] != "Due: {{ .number }}" {
		t.Fatalf("the stored template was changed by rendering it: %v", tpl.Data)
	}
}

// A digest has no single Data: its template ranges over the members, each with its own data.
func TestTemplateRenderADigest(t *testing.T) {
	t.Parallel()
	tpl := domain.Template{
		Ref: "invoice", Channel: "email", Locale: "en", Version: 1,
		Subject: "{{ len .digest }} invoices",
		Body:    `{{ range .digest }}{{ .data.number }} {{ .title }};{{ end }}`,
	}
	got, err := tpl.Render(domain.RenderRequest{Digest: []domain.Notification{
		{Topic: "invoice", Title: "first", Data: map[string]any{"number": "A"}},
		{Topic: "invoice", Title: "second", Data: map[string]any{"number": "B"}},
	}})
	if err != nil || got.Subject != "2 invoices" || got.Body != "A first;B second;" {
		t.Fatalf("Render = %+v, %v", got, err)
	}
}

func TestTemplateRenderFailsOnAMissingKey(t *testing.T) {
	t.Parallel()
	_, err := domain.Template{Ref: "r", Channel: "email", Locale: "en", Body: "{{ .missing }}"}.Render(domain.RenderRequest{Data: map[string]any{}})
	if err == nil {
		t.Fatal("Render = nil, want an error for a variable the producer did not send")
	}
}

func TestNormalizeLocale(t *testing.T) {
	t.Parallel()
	cases := []struct{ in, want string }{
		{"en", "en"}, {"EN", "en"}, {"en-US", "en-us"}, {"zh-Hant-TW", "zh-hant-tw"}, {"fil", "fil"},
		{"", ""}, {"e", ""}, {"en US", ""}, {"en_US", ""}, {"en-", ""}, {"-en", ""}, {"english1", ""}, {"en-" + strings.Repeat("a", 9), ""},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			if got := domain.NormalizeLocale(tc.in); got != tc.want {
				t.Fatalf("NormalizeLocale(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestLocaleLadder(t *testing.T) {
	t.Parallel()
	cases := []struct {
		locale, def string
		want        []string
	}{
		{"fr-CA", "en", []string{"fr-ca", "fr", "en"}},
		{"fr", "en", []string{"fr", "en"}},
		{"en", "en", []string{"en"}},
		{"EN-gb", "EN", []string{"en-gb", "en"}},
		{"FR-ca", "EN", []string{"fr-ca", "fr", "en"}},
		{"zh-Hant-TW", "en", []string{"zh-hant-tw", "zh", "en"}},
		{"", "en", []string{"en"}},
		{"fr-CA", "", []string{"fr-ca", "fr"}},
	}
	for _, tc := range cases {
		t.Run(tc.locale+"/"+tc.def, func(t *testing.T) {
			t.Parallel()
			if got := domain.LocaleLadder(tc.locale, tc.def); !slices.Equal(got, tc.want) {
				t.Fatalf("LocaleLadder(%q, %q) = %v, want %v", tc.locale, tc.def, got, tc.want)
			}
		})
	}
}
