package domain

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func inlineBroadcast(n int) BroadcastRequest {
	rs := make([]Recipient, n)
	for i := range rs {
		rs[i] = Recipient{Kind: "user", ID: fmt.Sprintf("u%d", i+1)}
	}
	return BroadcastRequest{
		Tenant:     Tenant{Kind: "workspace", ID: "w1"},
		Recipients: rs,
		Topic:      "launch",
		IdemKey:    "launch:2026",
	}
}

func audienceBroadcast() BroadcastRequest {
	b := inlineBroadcast(0)
	b.Recipients, b.Audience = nil, "everyone"
	return b
}

// T042: what a broadcast must be before it is recorded (NR-020). A request that cannot be
// expanded is refused at the door, where the producer can fix it, and not recorded as a
// durable row a worker would retry forever.
func TestBroadcastRequestValidate(t *testing.T) {
	t.Parallel()
	const max = 3
	cases := []struct {
		name  string
		build func() BroadcastRequest
		field string // the problem's field, "" when the request is valid
		is    error  // a sentinel the error must match
		why   string
	}{
		{"an inline list", func() BroadcastRequest { return inlineBroadcast(2) }, "", nil, ""},
		{"an audience selector", audienceBroadcast, "", nil, ""},
		{"an inline list of exactly the limit", func() BroadcastRequest { return inlineBroadcast(max) }, "", nil,
			"the limit is inclusive: MaxInlineRecipients recipients are allowed"},
		{"an inline list over the limit", func() BroadcastRequest { return inlineBroadcast(max + 1) }, "Recipients", ErrTooManyRecipients,
			"a bigger audience is a selector the host resolves in pages (NR-020)"},
		{"no idempotency key", func() BroadcastRequest { b := inlineBroadcast(1); b.IdemKey = ""; return b }, "IdemKey", ErrMissingIdem,
			"without a key a retried broadcast would notify everyone twice"},
		{"an idempotency key past the limit", func() BroadcastRequest {
			b := inlineBroadcast(1)
			b.IdemKey = strings.Repeat("k", MaxIdemKeyBytes+1)
			return b
		}, "IdemKey", nil, "the schema's own bound on the key"},
		{"neither an audience nor recipients", func() BroadcastRequest { b := inlineBroadcast(0); b.Recipients = nil; return b }, "Audience", nil,
			"exactly one source of recipients, as the schema's CHECK says"},
		{"an empty inline list", func() BroadcastRequest { return inlineBroadcast(0) }, "Audience", nil,
			"an empty list is no source: it would complete having notified no one"},
		{"both an audience and recipients", func() BroadcastRequest { b := inlineBroadcast(1); b.Audience = "everyone"; return b }, "Audience", nil, ""},
		{"a recipient with no id", func() BroadcastRequest { b := inlineBroadcast(2); b.Recipients[1].ID = ""; return b }, "Recipients", ErrEmptyIdentity,
			"an empty component would equal an unset scope (D17)"},
		{"a recipient with no kind", func() BroadcastRequest { b := inlineBroadcast(2); b.Recipients[0].Kind = ""; return b }, "Recipients", ErrEmptyIdentity, ""},
		{"no tenant id", func() BroadcastRequest { b := inlineBroadcast(1); b.Tenant.ID = ""; return b }, "Tenant", ErrEmptyIdentity, ""},
		{"no tenant kind", func() BroadcastRequest { b := inlineBroadcast(1); b.Tenant.Kind = ""; return b }, "Tenant", ErrEmptyIdentity, ""},
		{"no topic", func() BroadcastRequest { b := inlineBroadcast(1); b.Topic = ""; return b }, "Topic", nil, ""},
		{"a priority that is not one", func() BroadcastRequest { b := inlineBroadcast(1); b.Priority = "urgent"; return b }, "Priority", nil, ""},
		{"the topic's default priority", func() BroadcastRequest { b := inlineBroadcast(1); b.Priority = ""; return b }, "", nil,
			"an unset priority means the topic's default, as for a notification"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.build().Validate(max)
			if tc.field == "" {
				if err != nil {
					t.Fatalf("Validate = %v, want nil", err)
				}
				return
			}
			var ve *ValidationError
			if !errors.As(err, &ve) {
				t.Fatalf("Validate = %v (%T), want a *ValidationError naming %s: %s", err, err, tc.field, tc.why)
			}
			named := false
			for _, p := range ve.Problems {
				named = named || p.Field == tc.field
			}
			if !named {
				t.Errorf("Validate = %v, want a problem on %s: %s", err, tc.field, tc.why)
			}
			if tc.is != nil && !errors.Is(err, tc.is) {
				t.Errorf("Validate = %v, want it to match %v", err, tc.is)
			}
		})
	}
}

// Every rule a request breaks is reported at once, so a producer fixes it in one pass.
func TestBroadcastRequestValidateReportsEveryProblem(t *testing.T) {
	t.Parallel()
	b := inlineBroadcast(5)
	b.IdemKey, b.Topic = "", ""
	var ve *ValidationError
	if err := b.Validate(3); !errors.As(err, &ve) || len(ve.Problems) != 3 {
		t.Fatalf("Validate = %v, want three problems: the key, the topic and the list over the limit", err)
	}
}
