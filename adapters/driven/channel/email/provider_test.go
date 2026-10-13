//go:build provider

package email_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/truongpx396/intel-notification/adapters/driven/channel/email"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/notifytest"
)

// These tests run against Resend itself, in its sandbox addresses, and cost sending
// quota. They are the only way to show that a provider dedupes, and so that the
// level the Resend mailer declares is no stronger than the provider's behaviour
// (D32, NR-005):
//
//	NOTIFY_PROVIDER_RESEND_API_KEY=re_... go test -tags provider -run Sandbox ./adapters/driven/channel/email
//
// A key that is absent fails the run. The tag is a request to talk to the provider,
// and a skip would let a green run mean nothing.

func sandboxResend(t *testing.T) *email.Resend {
	t.Helper()
	key := os.Getenv("NOTIFY_PROVIDER_RESEND_API_KEY")
	if key == "" {
		t.Fatal("NOTIFY_PROVIDER_RESEND_API_KEY is not set: -tags provider runs against Resend's sandbox and needs a key")
	}
	from := os.Getenv("NOTIFY_PROVIDER_RESEND_FROM")
	if from == "" {
		from = "Notifications <onboarding@resend.dev>"
	}
	m, err := email.NewResend(key, from)
	if err != nil {
		t.Fatalf("NewResend: %v", err)
	}
	return m
}

// sandboxMailer sends to Resend's test inbox and reports to a probe what the
// provider shows of it: one send for each distinct message id a key was given. Two
// requests that Resend collapsed return one id, and so are one send.
//
// Keys are salted per run, because Resend remembers a key for a day and a second run
// would otherwise be answered from the first.
type sandboxMailer struct {
	*email.Resend
	probe *notifytest.Probe
	salt  string

	mu  sync.Mutex
	ids map[string]map[string]bool
}

func (s *sandboxMailer) Send(ctx context.Context, m email.Mail) (string, error) {
	key := m.IdemKey
	m.IdemKey = s.salt + key
	m.To = "delivered+" + key + "@resend.dev"
	id, err := s.Resend.Send(ctx, m)
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ids[key] == nil {
		s.ids[key] = map[string]bool{}
	}
	if !s.ids[key][id] {
		s.ids[key][id] = true
		s.probe.Record(key, false)
	}
	return id, nil
}

func TestResendSandboxHoldsTheDedupItDeclares(t *testing.T) {
	t.Parallel()
	real := sandboxResend(t)
	if level, window := real.Dedup(); level != domain.DedupProvider || window != 24*time.Hour {
		t.Fatalf("the mailer declares %v for %v, and this test checks DedupProvider for 24h", level, window)
	}
	var run [4]byte
	_, _ = rand.Read(run[:])
	salt := "run" + hex.EncodeToString(run[:]) + "-"

	// The cases that need no failure injected: a sandbox cannot be made to time out,
	// but it can be sent the same key twice.
	notifytest.ChannelDedupContract(t, func(t *testing.T) notifytest.ChannelUnderTest {
		t.Helper()
		probe := notifytest.NewProbe()
		mailer := &sandboxMailer{Resend: real, probe: probe, salt: salt + t.Name() + "-", ids: map[string]map[string]bool{}}
		ch, _ := newChannel(t, mailer)
		return notifytest.ChannelUnderTest{Channel: ch, Probe: probe}
	})
}
