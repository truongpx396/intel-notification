package email_test

import (
	"strings"
	"testing"
	"time"

	"github.com/truongpx396/intel-notification/adapters/driven/channel/email"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/notifytest"
)

var (
	linkNow = time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC)
	linkKey = []byte("0123456789abcdef0123456789abcdef")
)

// The token for aisat/w1/u1, ingestion_complete, email, signed at linkNow with key k1 and a 60-day
// life: the vector frozen in domain's tests, computed there with Python.
const frozenLinkToken = "v1.k1.eyJyIjoiYWlzYXQiLCJ0ayI6IndvcmtzcGFjZSIsInRpIjoidzEiLCJyayI6InVzZXIiLCJyaSI6InUxIiwidCI6ImluZ2VzdGlvbl9jb21wbGV0ZSIsImMiOiJlbWFpbCIsIngiOjE3OTcwMzM2MDB9.RWd57YKRHBK2RY66RFzc5Wydeo8BWtIVlQCODyoJ-Lg"

func linkSigner(t *testing.T) *domain.UnsubscribeSigner {
	t.Helper()
	s, err := domain.NewUnsubscribeSigner("k1", map[string][]byte{"k1": linkKey}, domain.MinUnsubscribeTTL)
	if err != nil {
		t.Fatalf("NewUnsubscribeSigner: %v", err)
	}
	return s
}

func newLinks(t *testing.T, base string, opts ...email.LinksOption) (*email.SignedLinks, *notifytest.Clock) {
	t.Helper()
	clock := notifytest.NewClock(linkNow)
	l, err := email.NewSignedLinks(linkSigner(t), base, clock, opts...)
	if err != nil {
		t.Fatalf("NewSignedLinks(%q): %v", base, err)
	}
	return l, clock
}

// The link is the unsubscribe route (rest-api.md) with the token as the query. The route's path is
// joined to the base however the base is written, so a deployment behind a prefix works.
func TestSignedLinkShape(t *testing.T) {
	t.Parallel()
	cases := []struct{ name, base, want string }{
		{"a host", "https://notify.example.test", "https://notify.example.test/notifications/unsubscribe?token="},
		{"a trailing slash", "https://notify.example.test/", "https://notify.example.test/notifications/unsubscribe?token="},
		{"a path prefix", "https://example.test/api", "https://example.test/api/notifications/unsubscribe?token="},
		{"a path prefix and a trailing slash", "https://example.test/api/", "https://example.test/api/notifications/unsubscribe?token="},
		{"a port", "https://notify.example.test:8443", "https://notify.example.test:8443/notifications/unsubscribe?token="},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			l, _ := newLinks(t, tc.base)
			got, err := l.For(t.Context(), delivery(optional, "S", "B"))
			if err != nil || got != tc.want+frozenLinkToken {
				t.Fatalf("For = %q, %v, want %q", got, err, tc.want+frozenLinkToken)
			}
		})
	}
}

// RFC 8058 wants one HTTPS URI in the header, and a mail scanner follows whatever it is given, so a
// base that is not HTTPS is refused unless the host says it knows (the e2e stack runs on localhost).
func TestSignedLinksRefuseABaseThatCannotWork(t *testing.T) {
	t.Parallel()
	refused := map[string]string{
		"nothing":                        "",
		"no scheme":                      "notify.example.test",
		"a relative path":                "/notifications",
		"plain http":                     "http://notify.example.test",
		"another scheme":                 "ftp://notify.example.test",
		"no host":                        "https://",
		"credentials in it":              "https://user:pw@notify.example.test",
		"a query":                        "https://notify.example.test?x=1",
		"a fragment":                     "https://notify.example.test#top",
		"a bad escape":                   "https://notify.example.test/%zz",
		"a space":                        "https://notify .example.test",
		"plain http spelled in capitals": "HTTP://notify.example.test",
	}
	for name, base := range refused {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := email.NewSignedLinks(linkSigner(t), base, notifytest.NewClock(linkNow)); err == nil {
				t.Fatalf("NewSignedLinks(%q) succeeded", base)
			}
		})
	}
	t.Run("plain http when the host says so", func(t *testing.T) {
		t.Parallel()
		l, _ := newLinks(t, "http://localhost:8080", email.AllowInsecureHTTP())
		got, err := l.For(t.Context(), delivery(optional, "S", "B"))
		if err != nil || !strings.HasPrefix(got, "http://localhost:8080/notifications/unsubscribe?token=") {
			t.Fatalf("For = %q, %v", got, err)
		}
	})
	t.Run("but never another scheme", func(t *testing.T) {
		t.Parallel()
		if _, err := email.NewSignedLinks(linkSigner(t), "ftp://localhost", notifytest.NewClock(linkNow), email.AllowInsecureHTTP()); err == nil {
			t.Fatal("NewSignedLinks accepted ftp")
		}
	})
	t.Run("no signer", func(t *testing.T) {
		t.Parallel()
		if _, err := email.NewSignedLinks(nil, "https://notify.example.test", notifytest.NewClock(linkNow)); err == nil {
			t.Fatal("NewSignedLinks accepted no signer")
		}
	})
	t.Run("no clock", func(t *testing.T) {
		t.Parallel()
		if _, err := email.NewSignedLinks(linkSigner(t), "https://notify.example.test", nil); err == nil {
			t.Fatal("NewSignedLinks accepted no clock")
		}
	})
}

// The token is for the delivery's own pair, and its life runs from the moment the link is made, so
// a message sent later is good for 60 days from then.
func TestSignedLinkIsForTheDeliverysPairAndDatedWhenMade(t *testing.T) {
	t.Parallel()
	l, clock := newLinks(t, "https://notify.example.test")
	signer := linkSigner(t)
	token := func(link string) string { return link[strings.Index(link, "token=")+len("token="):] }

	d := delivery(optional, "S", "B")
	d.Identity.Recipient.ID, d.Identity.Tenant.ID, d.Topic = "u9", "w9", "invoice_overdue"
	first, err := l.For(t.Context(), d)
	if err != nil {
		t.Fatalf("For: %v", err)
	}
	clock.Advance(40 * 24 * time.Hour)
	second, err := l.For(t.Context(), d)
	if err != nil {
		t.Fatalf("For: %v", err)
	}

	want := domain.UnsubscribeScope{Identity: d.Identity, Topic: "invoice_overdue", Channel: email.Kind}
	if got, err := signer.Verify(token(first), linkNow); err != nil || got != want {
		t.Fatalf("the first link verifies to %+v, %v, want the delivery's own pair %+v", got, err, want)
	}
	atDay61 := linkNow.Add(61 * 24 * time.Hour)
	if _, err := signer.Verify(token(first), atDay61); err == nil {
		t.Fatal("the link made at day 0 still works at day 61")
	}
	if _, err := signer.Verify(token(second), atDay61); err != nil {
		t.Fatalf("the link made at day 40 = %v at day 61, want it to hold: its life runs from when it was made", err)
	}
}

// A delivery that names no topic cannot have a link, and the channel waits on that as a fault.
func TestSignedLinkForADeliveryThatNamesNoPair(t *testing.T) {
	t.Parallel()
	l, _ := newLinks(t, "https://notify.example.test")
	d := delivery(optional, "S", "B")
	d.Topic = ""
	if link, err := l.For(t.Context(), d); err == nil {
		t.Fatalf("For = %q, want an error", link)
	}
}

// The channel and the links together, as a host wires them: the header a recipient's mail client
// POSTs to carries a token that verifies to exactly the pair the mail was about, and the footer
// carries the same.
func TestEmailChannelWithSignedLinksCarriesAVerifiableToken(t *testing.T) {
	t.Parallel()
	links, _ := newLinks(t, "https://notify.example.test")
	mailer := &fakeMailer{}
	ch := email.New(mailer, topics(), links)
	d := delivery(optional, "S", "<p>B</p>")
	if r, err := ch.Deliver(t.Context(), d); err != nil || r.Outcome != domain.Delivered {
		t.Fatalf("Deliver = %+v, %v", r, err)
	}
	m := mailer.mails()[0]
	header := m.Headers["List-Unsubscribe"]
	uri := strings.TrimSuffix(strings.TrimPrefix(header, "<"), ">")
	if !strings.HasPrefix(uri, "https://notify.example.test/notifications/unsubscribe?token=") {
		t.Fatalf("List-Unsubscribe = %q, want an HTTPS URI on the unsubscribe route", header)
	}
	got, err := linkSigner(t).Verify(strings.TrimPrefix(uri, "https://notify.example.test/notifications/unsubscribe?token="), linkNow)
	if err != nil || got != (domain.UnsubscribeScope{Identity: d.Identity, Topic: d.Topic, Channel: email.Kind}) {
		t.Fatalf("the header's token verifies to %+v, %v, want the pair the mail was about", got, err)
	}
	if !strings.Contains(m.HTML, strings.ReplaceAll(uri, "&", "&amp;")) {
		t.Fatalf("the footer does not carry the header's link: %q", m.HTML)
	}
}
