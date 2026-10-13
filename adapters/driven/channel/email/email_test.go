package email_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/truongpx396/intel-notification/adapters/driven/channel/email"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/notifytest"
	"github.com/truongpx396/intel-notification/ports"
)

var user = domain.Identity{
	Realm:     "aisat",
	Tenant:    domain.Tenant{Kind: "workspace", ID: "w1"},
	Recipient: domain.Recipient{Kind: "user", ID: "u1"},
}

const (
	idemKey   = "9602e45b0e75892a423e8c52157ced818019629335020fca1d8590a91864fc46"
	optional  = domain.Topic("ingestion_complete")
	essential = domain.Topic("password_changed")
)

// fakeMailer is a Mailer standing in for a provider: it keeps what it was given,
// answers with the error it is armed with.
type fakeMailer struct {
	mu    sync.Mutex
	sent  []email.Mail
	err   error
	level domain.DedupLevel
	win   time.Duration
}

func (m *fakeMailer) Dedup() (domain.DedupLevel, time.Duration) { return m.level, m.win }

func (m *fakeMailer) Send(_ context.Context, mail email.Mail) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return "", m.err
	}
	m.sent = append(m.sent, mail)
	return "msg-" + mail.IdemKey, nil
}

func (m *fakeMailer) mails() []email.Mail {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.sent)
}

// probeMailer is a fakeMailer that behaves as a provider does under the contract:
// it answers an armed failure with the kind of error a real one reports, and
// counts a send once per key when it honours one.
type probeMailer struct {
	level domain.DedupLevel
	win   time.Duration
	probe *notifytest.Probe
}

func (m *probeMailer) Dedup() (domain.DedupLevel, time.Duration) { return m.level, m.win }

func (m *probeMailer) Send(_ context.Context, mail email.Mail) (string, error) {
	if f, failed := m.probe.Attempt(mail.IdemKey); failed {
		switch f {
		case notifytest.Transient:
			return "", &email.SendError{Kind: email.Transient, Err: errors.New("451 try again later")}
		case notifytest.DeadAddress:
			return "", &email.SendError{Kind: email.HardBounce, Err: errors.New("550 5.1.1 no such user")}
		case notifytest.Permanent:
			return "", &email.SendError{Kind: email.Permanent, Err: errors.New("554 message refused")}
		default:
			return "", notifytest.ErrInfrastructure
		}
	}
	m.probe.Record(mail.IdemKey, m.level == domain.DedupProvider)
	return "msg-" + mail.IdemKey, nil
}

// fakeLinks signs nothing: it names the pair the link is for, so a test sees what
// the channel asked a link for. The ampersand is there to be escaped in a footer.
type fakeLinks struct {
	mu   sync.Mutex
	asks []domain.Delivery
	err  error
}

func (l *fakeLinks) For(_ context.Context, d domain.Delivery) (string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.asks = append(l.asks, d)
	if l.err != nil {
		return "", l.err
	}
	return fmt.Sprintf("https://notify.example.test/notifications/unsubscribe?token=%s.%s.%s.%s.%s&v=1",
		d.Identity.Realm, d.Identity.Tenant.ID, d.Identity.Recipient.ID, d.Topic, d.Channel), nil
}

// topics registers one optional and one essential topic.
func topics() *notifytest.Topics {
	t := notifytest.NewTopics()
	t.Register("aisat", optional, domain.TopicDef{DefaultChannels: []domain.ChannelKind{"email"}, DefaultPriority: domain.PriorityInfo})
	t.Register("aisat", essential, domain.TopicDef{DefaultChannels: []domain.ChannelKind{"email"}, DefaultPriority: domain.PriorityInfo, Essential: true})
	return t
}

func delivery(topic domain.Topic, subject, body string) domain.Delivery {
	return domain.Delivery{
		ID:             "d1",
		IdemKey:        idemKey,
		Identity:       user,
		NotificationID: "n1",
		Topic:          topic,
		Notification:   domain.Notification{IdemKey: "invoice:42:overdue"},
		Channel:        email.Kind,
		Address:        domain.Address{Channel: email.Kind, Value: "ada@example.com"},
		Content:        domain.RenderedContent{Subject: subject, Body: body},
		Attempt:        1,
	}
}

// render is the message as a provider is handed it, in the form the golden files
// hold: the envelope, the headers in order, a blank line, the HTML as it is.
func render(m email.Mail) string {
	var b strings.Builder
	fmt.Fprintf(&b, "To: %s\nSubject: %s\nIdempotency-Key: %s\n", m.To, m.Subject, m.IdemKey)
	for _, k := range slices.Sorted(mapsKeys(m.Headers)) {
		fmt.Fprintf(&b, "%s: %s\n", k, m.Headers[k])
	}
	b.WriteString("\n")
	b.WriteString(m.HTML)
	return b.String()
}

func mapsKeys(m map[string]string) func(func(string) bool) {
	return func(yield func(string) bool) {
		for k := range m {
			if !yield(k) {
				return
			}
		}
	}
}

func golden(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read golden %s: %v", name, err)
	}
	return string(b)
}

func newChannel(t *testing.T, mailer email.Mailer, opts ...email.Option) (*email.Channel, *fakeLinks) {
	t.Helper()
	links := &fakeLinks{}
	return email.New(mailer, topics(), links, opts...), links
}

// The message a provider is handed, held to files: a non-essential topic carries
// the RFC 8058 headers and the footer, an essential one carries neither (NR-015,
// D34). The files are written out by hand, so a change to the message is a change
// to a file in the review.
func TestEmailMessageMatchesTheGoldenFiles(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		golden  string
		subject string
		body    string
		topic   domain.Topic
	}{
		{"a non-essential topic carries the unsubscribe headers and footer", "non_essential.golden",
			"Upload finished", "<p>Your upload finished.</p>", optional},
		{"an essential topic carries neither", "essential.golden",
			"Your password was changed", "<p>Your password was changed. If this was not you, contact support.</p>", essential},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mailer := &fakeMailer{}
			ch, _ := newChannel(t, mailer)
			r, err := ch.Deliver(t.Context(), delivery(tc.topic, tc.subject, tc.body))
			if err != nil || r.Outcome != domain.Delivered {
				t.Fatalf("Deliver = %+v, %v, want Delivered", r, err)
			}
			sent := mailer.mails()
			if len(sent) != 1 {
				t.Fatalf("the provider was handed %d messages, want 1", len(sent))
			}
			if got, want := render(sent[0]), golden(t, tc.golden); got != want {
				t.Fatalf("the message differs from %s.\n--- got\n%s\n--- want\n%s", tc.golden, got, want)
			}
		})
	}
}

// The compliance a golden file shows by example, stated as the rule (NR-015): the
// one-click headers and the footer go together, on a topic that is not essential
// and only there, and a topic the registry does not know is not essential.
func TestEmailUnsubscribeAffordanceIsForNonEssentialTopicsOnly(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		topic domain.Topic
		want  bool
		why   string
	}{
		{"a non-essential topic", optional, true, "Gmail and Yahoo require one-click unsubscribe from bulk senders"},
		{"an essential topic", essential, false, "an essential topic cannot be opted out of, so a link that offers to would mislead"},
		{"a topic the registry does not know", "never_registered", true, "uncertainty falls on the side of the law, not of the copy"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mailer := &fakeMailer{}
			ch, links := newChannel(t, mailer)
			if _, err := ch.Deliver(t.Context(), delivery(tc.topic, "S", "<p>B</p>")); err != nil {
				t.Fatalf("Deliver: %v", err)
			}
			m := mailer.mails()[0]
			_, hasList := m.Headers["List-Unsubscribe"]
			_, hasPost := m.Headers["List-Unsubscribe-Post"]
			hasFooter := strings.Contains(m.HTML, "Unsubscribe")
			if hasList != tc.want || hasPost != tc.want || hasFooter != tc.want {
				t.Fatalf("List-Unsubscribe %v, List-Unsubscribe-Post %v, footer %v, want all %v: %s", hasList, hasPost, hasFooter, tc.want, tc.why)
			}
			if got := len(links.asks) == 1; got != tc.want {
				t.Fatalf("a link was asked for %d times, want it asked for only when it is used", len(links.asks))
			}
		})
	}
}

// RFC 8058: the header names the URI in angle brackets, and the POST header says
// the URI accepts a one-click POST. The same link is in the footer, HTML-escaped,
// and the body the template produced is untouched ahead of it.
func TestEmailUnsubscribeHeadersAndFooterShareOneLink(t *testing.T) {
	t.Parallel()
	mailer := &fakeMailer{}
	ch, _ := newChannel(t, mailer)
	if _, err := ch.Deliver(t.Context(), delivery(optional, "S", "<p>B</p>")); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	m := mailer.mails()[0]
	const link = "https://notify.example.test/notifications/unsubscribe?token=aisat.w1.u1.ingestion_complete.email&v=1"
	if got := m.Headers["List-Unsubscribe"]; got != "<"+link+">" {
		t.Fatalf("List-Unsubscribe = %q, want the link in angle brackets", got)
	}
	if got := m.Headers["List-Unsubscribe-Post"]; got != "List-Unsubscribe=One-Click" {
		t.Fatalf("List-Unsubscribe-Post = %q, want List-Unsubscribe=One-Click", got)
	}
	if !strings.HasPrefix(m.HTML, "<p>B</p>") || !strings.Contains(m.HTML, `href="`+strings.ReplaceAll(link, "&", "&amp;")+`"`) {
		t.Fatalf("HTML = %q, want the body first and then a footer linking the same URL, escaped", m.HTML)
	}
	if len(m.Headers) != 2 {
		t.Fatalf("headers = %v, want exactly the two unsubscribe headers", m.Headers)
	}
}

// The link is made for the pair the delivery is for, so a leaked link disables one
// preference and not everything (D34).
func TestEmailAsksForALinkForTheDeliverysOwnPair(t *testing.T) {
	t.Parallel()
	ch, links := newChannel(t, &fakeMailer{})
	d := delivery(optional, "S", "B")
	if _, err := ch.Deliver(t.Context(), d); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if len(links.asks) != 1 || links.asks[0].Identity != d.Identity || links.asks[0].Topic != d.Topic || links.asks[0].Channel != email.Kind {
		t.Fatalf("link asked for %+v, want the delivery's identity, topic and channel", links.asks)
	}
}

// The provider is given Delivery.IdemKey, the per-address key, and not the
// notification's own: that one would collapse a second address and a digest (D25).
func TestEmailGivesTheProviderTheDeliveryKey(t *testing.T) {
	t.Parallel()
	mailer := &fakeMailer{}
	ch, _ := newChannel(t, mailer)
	d := delivery(essential, "S", "B")
	if _, err := ch.Deliver(t.Context(), d); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if got := mailer.mails()[0].IdemKey; got != d.IdemKey || got == d.Notification.IdemKey {
		t.Fatalf("Mail.IdemKey = %q, want Delivery.IdemKey %q and not the notification's %q", got, d.IdemKey, d.Notification.IdemKey)
	}
}

func TestEmailReturnsTheProvidersMessageID(t *testing.T) {
	t.Parallel()
	ch, _ := newChannel(t, &fakeMailer{})
	r, err := ch.Deliver(t.Context(), delivery(essential, "S", "B"))
	if err != nil || r.ProviderMessageID != "msg-"+idemKey {
		t.Fatalf("Deliver = %+v, %v, want the provider's message id, which a bounce callback is traced by (D20)", r, err)
	}
}

// What a provider says decides the outcome. A kind it does not name, and an error
// it does not classify, are the transient kind: only a verdict ends a delivery.
func TestEmailMapsTheProvidersAnswerToAnOutcome(t *testing.T) {
	t.Parallel()
	cause := errors.New("provider said so")
	cases := []struct {
		name       string
		err        error
		want       domain.Outcome
		reason     domain.SuppressionReason
		retryAfter time.Duration
	}{
		{"a hard bounce", &email.SendError{Kind: email.HardBounce, Err: cause}, domain.Suppressed, domain.SuppressHardBounce, 0},
		{"an invalid address", &email.SendError{Kind: email.InvalidAddress, Err: cause}, domain.Suppressed, domain.SuppressInvalidAddress, 0},
		{"a permanent refusal", &email.SendError{Kind: email.Permanent, Err: cause}, domain.Rejected, "", 0},
		{"a transient failure", &email.SendError{Kind: email.Transient, Err: cause}, domain.Retry, "", 0},
		{"a transient failure with a hint", &email.SendError{Kind: email.Transient, RetryAfter: 90 * time.Second, Err: cause}, domain.Retry, "", 90 * time.Second},
		{"a classified error that was wrapped", fmt.Errorf("send: %w", &email.SendError{Kind: email.Permanent, Err: cause}), domain.Rejected, "", 0},
		{"an error the provider did not classify", cause, domain.Retry, "", 0},
		{"a deadline", context.DeadlineExceeded, domain.Retry, "", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ch, _ := newChannel(t, &fakeMailer{err: tc.err})
			r, err := ch.Deliver(t.Context(), delivery(essential, "S", "B"))
			if err != nil {
				t.Fatalf("Deliver returned the error %v, want an outcome", err)
			}
			if r.Outcome != tc.want || r.SuppressReason != tc.reason || r.RetryAfter != tc.retryAfter {
				t.Fatalf("Deliver = %+v, want %v / %q / retry after %v", r, tc.want, tc.reason, tc.retryAfter)
			}
			if r.Detail == "" {
				t.Fatal("no Detail: it is the last error a dead letter keeps")
			}
			if tc.want == domain.Suppressed && r.SuppressFor != 0 {
				t.Fatalf("SuppressFor = %v, want 0: a dead mailbox is dead for good (D14)", r.SuppressFor)
			}
		})
	}
}

// An address that cannot be mailed is the address's problem, found before a
// provider is asked, and is suppressed so it is not asked again. An empty one is a
// data fault, not a verdict on any address, and suppressing it would suppress the
// hash of nothing (D13).
func TestEmailRefusesAnAddressItCannotMail(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		value string
		want  domain.Outcome
	}{
		{"no @", "ada.example.com", domain.Suppressed},
		{"no domain", "ada@", domain.Suppressed},
		{"two addresses", "ada@example.com, eve@example.com", domain.Suppressed},
		{"a display name, which is not a mailbox the suppression hash can name", "Ada <ada@example.com>", domain.Suppressed},
		{"a header injected through the address", "ada@example.com\r\nBcc: eve@example.com", domain.Suppressed},
		{"empty", "", domain.Rejected},
		{"blank", "   ", domain.Rejected},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mailer := &fakeMailer{}
			ch, _ := newChannel(t, mailer)
			d := delivery(essential, "S", "B")
			d.Address.Value = tc.value
			r, err := ch.Deliver(t.Context(), d)
			if err != nil || r.Outcome != tc.want {
				t.Fatalf("Deliver = %+v, %v, want %v", r, err, tc.want)
			}
			if tc.want == domain.Suppressed && r.SuppressReason != domain.SuppressInvalidAddress {
				t.Fatalf("SuppressReason = %q, want invalid_address", r.SuppressReason)
			}
			if len(mailer.mails()) != 0 {
				t.Fatalf("the provider was handed %v for an address that cannot be mailed", mailer.mails())
			}
		})
	}
}

// A topic registry or a link signer that cannot answer is a fault in our own
// infrastructure. The message waits for it: sending without the headers would be
// the compliance failure, and sending to a guess would be worse.
func TestEmailRetriesWhenItCannotLearnWhetherToAddAnUnsubscribeLink(t *testing.T) {
	t.Parallel()
	t.Run("the topic lookup fails", func(t *testing.T) {
		t.Parallel()
		mailer := &fakeMailer{}
		ch := email.New(mailer, failingTopics{}, &fakeLinks{})
		r, err := ch.Deliver(t.Context(), delivery(optional, "S", "B"))
		if err != nil || r.Outcome != domain.Retry || !strings.Contains(r.Detail, "topic store down") {
			t.Fatalf("Deliver = %+v, %v, want Retry whose detail is the registry's own error", r, err)
		}
		if len(mailer.mails()) != 0 {
			t.Fatal("a message went out although the channel could not tell whether it needed an unsubscribe link")
		}
	})
	t.Run("the link cannot be made", func(t *testing.T) {
		t.Parallel()
		mailer := &fakeMailer{}
		ch, links := newChannel(t, mailer)
		links.err = errors.New("signing key unavailable")
		r, err := ch.Deliver(t.Context(), delivery(optional, "S", "B"))
		if err != nil || r.Outcome != domain.Retry || !strings.Contains(r.Detail, "signing key unavailable") {
			t.Fatalf("Deliver = %+v, %v, want Retry whose detail is the signer's own error", r, err)
		}
		if len(mailer.mails()) != 0 {
			t.Fatal("a non-essential message went out with no unsubscribe link")
		}
	})
}

// A header is one line. A link that is not a single URI would split the header or
// the footer's href, so it is a fault of the signer and never sent.
func TestEmailNeverSendsALinkThatIsNotOneURI(t *testing.T) {
	t.Parallel()
	for name, link := range map[string]string{
		"a new line":       "https://notify.example.test/u?t=1\r\nBcc: eve@example.com",
		"a space":          "https://notify.example.test/u?t=1 2",
		"an angle bracket": "https://notify.example.test/u?t=1>",
		"nothing":          "",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			mailer := &fakeMailer{}
			ch := email.New(mailer, topics(), constLink(link))
			r, err := ch.Deliver(t.Context(), delivery(optional, "S", "B"))
			if err != nil || r.Outcome != domain.Retry || r.Detail == "" {
				t.Fatalf("Deliver = %+v, %v, want Retry with a detail", r, err)
			}
			if len(mailer.mails()) != 0 {
				t.Fatalf("a message went out with the link %q", link)
			}
		})
	}
}

type constLink string

func (l constLink) For(context.Context, domain.Delivery) (string, error) { return string(l), nil }

type failingTopics struct{}

func (failingTopics) Lookup(context.Context, domain.Realm, domain.Topic) (domain.TopicDef, bool, error) {
	return domain.TopicDef{}, false, errors.New("topic store down")
}

// The footer is copy, and copy is the template's (NR-019): the default is English
// and plain, and a host that wants its own words passes them.
func TestEmailFooterCanBeReplaced(t *testing.T) {
	t.Parallel()
	mailer := &fakeMailer{}
	ch, _ := newChannel(t, mailer, email.WithFooter(func(link string) string { return "<small>" + link + "</small>" }))
	if _, err := ch.Deliver(t.Context(), delivery(optional, "S", "<p>B</p>")); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if got := mailer.mails()[0].HTML; !strings.HasPrefix(got, "<p>B</p><small>https://notify.example.test/") {
		t.Fatalf("HTML = %q, want the body followed by the host's footer", got)
	}
}

// What the engine reads of the channel (NR-016), and the provider's guarantee as
// the channel's own, never stronger (D32): a window means something only with
// DedupProvider.
func TestEmailDeclaresWhatItIsAndTakesItsDedupFromTheProvider(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		level     domain.DedupLevel
		window    time.Duration
		wantLevel domain.DedupLevel
		wantWin   time.Duration
	}{
		{"a provider that dedupes for a day", domain.DedupProvider, 24 * time.Hour, domain.DedupProvider, 24 * time.Hour},
		{"a provider that does not", domain.DedupNone, 0, domain.DedupNone, 0},
		{"a window with no provider dedup is dropped", domain.DedupNone, 24 * time.Hour, domain.DedupNone, 0},
		{"a provider that records sends itself", domain.DedupLocal, 0, domain.DedupLocal, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ch, _ := newChannel(t, &fakeMailer{level: tc.level, win: tc.window})
			if ch.Kind() != "email" {
				t.Fatalf("Kind = %q, want email", ch.Kind())
			}
			want := domain.ChannelCapabilities{NeedsSubject: true, NeedsAddress: true, Dedup: tc.wantLevel, DedupWindow: tc.wantWin}
			if got := ch.Capabilities(); got != want {
				t.Fatalf("Capabilities = %+v, want %+v", got, want)
			}
		})
	}
}

// Address equality is the channel's to define (ports.AddressNormalizer): the domain
// of an address is case-insensitive, the local part is not assumed to be, and the
// key and the suppression hash are made over this form.
func TestEmailNormalizesAddresses(t *testing.T) {
	t.Parallel()
	cases := []struct{ in, want string }{
		{"ada@example.com", "ada@example.com"},
		{"ada@Example.COM", "ada@example.com"},
		{"  ada@example.com\t", "ada@example.com"},
		{"Ada.Lovelace@Example.com", "Ada.Lovelace@example.com"},
		{"a@b@Example.com", "a@b@example.com"},
		{"no-at-sign", "no-at-sign"},
		{"", ""},
	}
	ch, _ := newChannel(t, &fakeMailer{})
	for _, tc := range cases {
		if got := ch.NormalizeAddress(tc.in); got != tc.want {
			t.Errorf("NormalizeAddress(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// A channel missing a port is a wiring mistake, found when the host starts.
func TestNewNamesWhatIsMissing(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		call   func()
		wanted []string
	}{
		{"nothing", func() { email.New(nil, nil, nil) }, []string{"Mailer", "TopicRegistry", "UnsubscribeLinks"}},
		{"only a mailer", func() { email.New(&fakeMailer{}, nil, nil) }, []string{"TopicRegistry", "UnsubscribeLinks"}},
		{"no mailer", func() { email.New(nil, topics(), &fakeLinks{}) }, []string{"Mailer"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			defer func() {
				p := recover()
				if p == nil {
					t.Fatal("New did not panic")
				}
				for _, w := range tc.wanted {
					if !strings.Contains(fmt.Sprint(p), w) {
						t.Fatalf("panic %q does not name %s", p, w)
					}
				}
			}()
			tc.call()
		})
	}
}

// The email channel is held to the contract that every channel is (NR-005, D32),
// at each dedup level a provider can honestly declare.
func TestEmailRunsTheChannelContract(t *testing.T) {
	t.Parallel()
	levels := []struct {
		name  string
		level domain.DedupLevel
		win   time.Duration
	}{
		{"a provider that does not dedupe", domain.DedupNone, 0},
		{"a provider that dedupes for a day", domain.DedupProvider, 24 * time.Hour},
	}
	for _, l := range levels {
		t.Run(l.name, func(t *testing.T) {
			t.Parallel()
			notifytest.ChannelContract(t, func(t *testing.T) notifytest.ChannelUnderTest {
				t.Helper()
				probe := notifytest.NewProbe()
				ch, _ := newChannel(t, &probeMailer{level: l.level, win: l.win, probe: probe})
				return notifytest.ChannelUnderTest{Channel: ch, Probe: probe}
			})
		})
	}
}

var _ ports.TopicRegistry = failingTopics{}
