package domain

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"
)

// fieldsOf returns the fields a *ValidationError names, sorted, or nil for no
// error. It fails the test if the error is some other type.
func fieldsOf(t *testing.T, err error) []string {
	t.Helper()
	if err == nil {
		return nil
	}
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("got %T (%v), want a *ValidationError", err, err)
	}
	if len(ve.Problems) == 0 {
		t.Fatal("a *ValidationError with no problems: a clean value must be a nil error")
	}
	fields := make([]string, 0, len(ve.Problems))
	for _, p := range ve.Problems {
		if p.Message == "" {
			t.Fatalf("a problem with no message for field %s", p.Field)
		}
		fields = append(fields, p.Field)
	}
	slices.Sort(fields)
	return fields
}

func TestPriorityValid(t *testing.T) {
	t.Parallel()
	cases := []struct {
		p    Priority
		want bool
	}{
		{PriorityInfo, true},
		{PriorityWarning, true},
		{PriorityCritical, true},
		{"", false},
		{"INFO", false},
		{"urgent", false},
	}
	for _, tc := range cases {
		t.Run(string(tc.p), func(t *testing.T) {
			t.Parallel()
			if got := tc.p.Valid(); got != tc.want {
				t.Fatalf("Priority(%q).Valid() = %v, want %v", tc.p, got, tc.want)
			}
		})
	}
}

func validTopicDef() TopicDef {
	return TopicDef{
		DefaultChannels: []ChannelKind{"in_app", "email"},
		Fallback:        []ChannelKind{"push", "sms"},
		DefaultPriority: PriorityInfo,
	}
}

// A topic registration's rules (NR-017, NR-031). The fallback rules are the
// schema's CHECK on notification_topics written as code, so a topic registered in
// code fails the way a row would.
func TestTopicDefValidate(t *testing.T) {
	t.Parallel()
	with := func(f func(*TopicDef)) func() TopicDef {
		return func() TopicDef { d := validTopicDef(); f(&d); return d }
	}
	cases := []struct {
		name string
		def  func() TopicDef
		want []string
		why  string
	}{
		{"a valid topic", validTopicDef, nil, ""},
		{"no fallback chain", with(func(d *TopicDef) { d.Fallback = nil }), nil, "a fallback is optional"},
		{"an essential topic", with(func(d *TopicDef) { d.Essential = true; d.DefaultPriority = PriorityCritical }), nil, ""},
		{"a warning default", with(func(d *TopicDef) { d.DefaultPriority = PriorityWarning }), nil, ""},
		{"a critical default", with(func(d *TopicDef) { d.DefaultPriority = PriorityCritical }), nil, ""},

		{"a fallback chain that repeats a channel",
			with(func(d *TopicDef) { d.Fallback = []ChannelKind{"push", "push"} }),
			[]string{"Fallback"}, "an ordered chain tries each channel once; the second try could never differ (NR-031)"},
		{"a fallback chain that repeats a channel after another",
			with(func(d *TopicDef) { d.Fallback = []ChannelKind{"push", "sms", "push"} }),
			[]string{"Fallback"}, "not only adjacent repeats"},
		{"default channels that repeat a channel",
			with(func(d *TopicDef) { d.DefaultChannels = []ChannelKind{"email", "in_app", "email"} }),
			[]string{"DefaultChannels"}, "a channel fanned out twice would enqueue one delivery twice"},
		{"a channel both fanned out and in the fallback chain",
			with(func(d *TopicDef) { d.Fallback = []ChannelKind{"push", "email"} }),
			[]string{"Fallback"}, "a channel is either delivered in parallel or tried as a fallback, not both"},
		{"an empty channel kind among the defaults",
			with(func(d *TopicDef) { d.DefaultChannels = []ChannelKind{"in_app", ""} }),
			[]string{"DefaultChannels"}, ""},
		{"an empty channel kind in the fallback chain",
			with(func(d *TopicDef) { d.Fallback = []ChannelKind{""} }),
			[]string{"Fallback"}, ""},
		{"no default priority", with(func(d *TopicDef) { d.DefaultPriority = "" }),
			[]string{"DefaultPriority"}, "a registration carries its priority (NR-017); the schema column is NOT NULL"},
		{"an unknown default priority", with(func(d *TopicDef) { d.DefaultPriority = "urgent" }),
			[]string{"DefaultPriority"}, ""},
		{"several problems together",
			with(func(d *TopicDef) {
				d.DefaultChannels = []ChannelKind{"email", "email"}
				d.Fallback = []ChannelKind{"sms", "sms"}
				d.DefaultPriority = "urgent"
			}),
			[]string{"DefaultChannels", "DefaultPriority", "Fallback"}, "every problem at once"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			def := tc.def()
			if got := fieldsOf(t, def.Validate()); !slices.Equal(got, tc.want) {
				t.Fatalf("Validate blamed %v, want %v: %s\n def: %+v", got, tc.want, tc.why, def)
			}
		})
	}
}

// What a notification's priority is, once the topic's default fills a gap.
func TestTopicDefEffectivePriority(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		def     Priority
		p       Priority
		want    Priority
		because string
	}{
		{"an explicit priority is kept", PriorityInfo, PriorityWarning, PriorityWarning, ""},
		{"an explicit priority overrides a critical default", PriorityCritical, PriorityInfo, PriorityInfo,
			"the producer decides this notification is not urgent"},
		{"an explicit critical overrides an info default", PriorityInfo, PriorityCritical, PriorityCritical, ""},
		{"empty takes the default", PriorityWarning, "", PriorityWarning, `"" means the topic's default`},
		{"empty takes a critical default", PriorityCritical, "", PriorityCritical, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			d := TopicDef{DefaultPriority: tc.def}
			if got := d.EffectivePriority(tc.p); got != tc.want {
				t.Fatalf("EffectivePriority(%q) on a %q default = %q, want %q: %s", tc.p, tc.def, got, tc.want, tc.because)
			}
		})
	}
}

// Digest eligibility (NR-014, NR-015): never critical, never essential. The
// priority that counts is the effective one, so an unset priority on a topic that
// defaults to critical is critical.
func TestTopicDefDigestible(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		essential bool
		def       Priority
		p         Priority
		want      bool
		why       string
	}{
		{"info", false, PriorityInfo, PriorityInfo, true, "the ordinary case"},
		{"warning", false, PriorityInfo, PriorityWarning, true, ""},
		{"critical", false, PriorityInfo, PriorityCritical, false, "critical is never digested (NR-014)"},
		{"info on an essential topic", true, PriorityInfo, PriorityInfo, false, "an essential topic is never digested (NR-015)"},
		{"warning on an essential topic", true, PriorityInfo, PriorityWarning, false, ""},
		{"critical on an essential topic", true, PriorityCritical, PriorityCritical, false, ""},
		{"unset priority on an info topic", false, PriorityInfo, "", true, "the default applies"},
		{"unset priority on a critical topic", false, PriorityCritical, "", false,
			"critical by default is critical: digesting it would hold back what must not wait"},
		{"info overriding a critical default", false, PriorityCritical, PriorityInfo, true,
			"the notification's own priority is what counts"},
		{"unset priority on an essential topic", true, PriorityInfo, "", false, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			d := TopicDef{Essential: tc.essential, DefaultPriority: tc.def}
			if got := d.Digestible(tc.p); got != tc.want {
				t.Fatalf("Digestible(%q) on essential=%v default=%q is %v, want %v: %s",
					tc.p, tc.essential, tc.def, got, tc.want, tc.why)
			}
		})
	}
}

func validNotification() Notification {
	return Notification{
		Tenant:    Tenant{Kind: "workspace", ID: "w1"},
		Recipient: Recipient{Kind: "user", ID: "u1"},
		Topic:     "invoice_overdue",
		IdemKey:   "invoice:42:overdue",
	}
}

// What Notify refuses before it touches a store. Each row changes one field of a
// valid notification. The identity and key rules are the schema's CHECKs on
// notifications and notify_idem written as code: an empty component would equal
// an unset scope and defeat the row-level security predicate (D17).
func TestNotificationValidate(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2031, 3, 1, 12, 0, 0, 0, time.UTC)
	with := func(f func(*Notification)) func() Notification {
		return func() Notification { n := validNotification(); f(&n); return n }
	}
	cases := []struct {
		name    string
		n       func() Notification
		want    []string
		wantIs  error // a sentinel the error must match, if any
		because string
	}{
		{"a valid notification", validNotification, nil, nil, ""},
		{"an explicit priority", with(func(n *Notification) { n.Priority = PriorityCritical }), nil, nil, ""},

		{"an empty tenant kind", with(func(n *Notification) { n.Tenant.Kind = "" }), []string{"Tenant"}, ErrEmptyIdentity, ""},
		{"an empty tenant id", with(func(n *Notification) { n.Tenant.ID = "" }), []string{"Tenant"}, ErrEmptyIdentity, ""},
		{"an empty recipient kind", with(func(n *Notification) { n.Recipient.Kind = "" }), []string{"Recipient"}, ErrEmptyIdentity, ""},
		{"an empty recipient id", with(func(n *Notification) { n.Recipient.ID = "" }), []string{"Recipient"}, ErrEmptyIdentity, ""},
		{"an empty topic", with(func(n *Notification) { n.Topic = "" }), []string{"Topic"}, nil, ""},
		{"an unknown priority", with(func(n *Notification) { n.Priority = "urgent" }), []string{"Priority"}, nil, ""},
		{"an uppercase priority", with(func(n *Notification) { n.Priority = "INFO" }), []string{"Priority"}, nil, ""},

		{"no idempotency key", with(func(n *Notification) { n.IdemKey = "" }), []string{"IdemKey"}, ErrMissingIdem,
			"without a key there is no exactly-once identity (NR-002)"},
		{"a key of one byte", with(func(n *Notification) { n.IdemKey = "k" }), nil, nil, ""},
		{"a key of 255 bytes", with(func(n *Notification) { n.IdemKey = strings.Repeat("k", 255) }), nil, nil, "the longest allowed"},
		{"a key of 256 bytes", with(func(n *Notification) { n.IdemKey = strings.Repeat("k", 256) }), []string{"IdemKey"}, nil,
			"one past the longest, and not reported as missing"},
		{"a key of 128 two-byte characters", with(func(n *Notification) { n.IdemKey = strings.Repeat("é", 128) }),
			[]string{"IdemKey"}, nil, "the limit counts bytes, as the contract says, not characters"},

		{"an expiry equal to the send time",
			with(func(n *Notification) { n.DeliverAfter, n.DeliverBefore = t0, t0 }),
			[]string{"DeliverBefore"}, nil, "it would expire the instant it became due"},
		{"an expiry before the send time",
			with(func(n *Notification) { n.DeliverAfter, n.DeliverBefore = t0, t0.Add(-time.Hour) }),
			[]string{"DeliverBefore"}, nil, "it could never be delivered (NR-029)"},
		{"an expiry one nanosecond after the send time",
			with(func(n *Notification) { n.DeliverAfter, n.DeliverBefore = t0, t0.Add(1) }), nil, nil, ""},
		{"an expiry with no send time",
			with(func(n *Notification) { n.DeliverBefore = t0 }), nil, nil,
			"the send time is now, which only the notifier's clock knows"},
		{"a send time with no expiry", with(func(n *Notification) { n.DeliverAfter = t0 }), nil, nil, ""},

		{"several problems together",
			with(func(n *Notification) {
				n.Tenant.ID = ""
				n.Topic = ""
				n.Priority = "urgent"
				n.IdemKey = ""
			}),
			[]string{"IdemKey", "Priority", "Tenant", "Topic"}, ErrMissingIdem, "every problem at once"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			n := tc.n()
			err := n.Validate()
			if got := fieldsOf(t, err); !slices.Equal(got, tc.want) {
				t.Fatalf("Validate blamed %v, want %v: %s\n notification: %+v", got, tc.want, tc.because, n)
			}
			if tc.wantIs != nil && !errors.Is(err, tc.wantIs) {
				t.Fatalf("Validate = %v, want it to match %v", err, tc.wantIs)
			}
			// A missing key is the only problem that may match ErrMissingIdem.
			if !errors.Is(tc.wantIs, ErrMissingIdem) && errors.Is(err, ErrMissingIdem) {
				t.Fatalf("Validate = %v matched ErrMissingIdem, but a key was given", err)
			}
		})
	}
}

// A ValidationError names every problem in its message, so whoever reads a
// refusal sees all of them.
func TestValidationErrorMessage(t *testing.T) {
	t.Parallel()
	n := validNotification()
	n.Topic = ""
	n.IdemKey = ""
	err := n.Validate()
	if err == nil {
		t.Fatal("Validate accepted a notification with no topic and no key")
	}
	for _, field := range []string{"Topic", "IdemKey"} {
		if !strings.Contains(err.Error(), field) {
			t.Errorf("the message %q does not name %s", err.Error(), field)
		}
	}
}

// The realm is an argument, never a field of the notification: a producer cannot
// set it (NR-009). Tenant and recipient are the same shape, so the test uses
// values that differ everywhere: a swap or a dropped component shows.
func TestNotificationIdentity(t *testing.T) {
	t.Parallel()
	n := Notification{
		Tenant:    Tenant{Kind: "workspace", ID: "w1"},
		Recipient: Recipient{Kind: "user", ID: "u1"},
	}
	got := n.Identity("aisat")
	want := Identity{
		Realm:     "aisat",
		Tenant:    Tenant{Kind: "workspace", ID: "w1"},
		Recipient: Recipient{Kind: "user", ID: "u1"},
	}
	if got != want {
		t.Fatalf("Identity(aisat) = %+v, want %+v", got, want)
	}
	if other := n.Identity("other"); other.Realm != "other" || other == got {
		t.Fatalf("Identity(other) = %+v: the realm must come from the argument", other)
	}
}
