package domain_test

import (
	"strings"
	"testing"

	"github.com/truongpx396/intel-notification/domain"
)

// T049, D31. A credential is referenced and never stored; a value that is not a reference is refused,
// and the refusal never repeats it.
func TestParseSecretRef(t *testing.T) {
	t.Parallel()
	good := []struct{ ref, scheme, rest string }{
		{"env:RESEND_KEY", "env", "RESEND_KEY"},
		{"file:/run/secrets/resend", "file", "/run/secrets/resend"},
		{"vault://kv/notify#resend", "vault", "//kv/notify#resend"},
		{"aws-sm:arn:aws:secretsmanager:eu-west-1:1:secret:x", "aws-sm", "arn:aws:secretsmanager:eu-west-1:1:secret:x"},
	}
	for _, tc := range good {
		scheme, rest, err := domain.ParseSecretRef(tc.ref)
		if err != nil || scheme != tc.scheme || rest != tc.rest {
			t.Errorf("ParseSecretRef(%q) = %q, %q, %v, want %q, %q", tc.ref, scheme, rest, err, tc.scheme, tc.rest)
		}
	}
	for _, bad := range []string{"", "re_live_AbCdEf123456", "ENV:NAME", "env:", ":x", "1env:x", "no scheme here"} {
		_, _, err := domain.ParseSecretRef(bad)
		if err == nil {
			t.Errorf("ParseSecretRef(%q) = nil, want a refusal", bad)
			continue
		}
	}
	// The likeliest mistake is the key itself pasted where its reference belongs, so the refusal must not
	// carry it into a log.
	const pasted = "re_live_AbCdEf123456"
	if _, _, err := domain.ParseSecretRef(pasted); err == nil || strings.Contains(err.Error(), pasted) || strings.Contains(err.Error(), "AbCdEf") {
		t.Errorf("ParseSecretRef(%q) = %v, want a refusal that does not repeat the value", pasted, err)
	}
}
