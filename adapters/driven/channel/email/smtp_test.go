package email_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"math/big"
	"mime"
	"mime/quotedprintable"
	"net"
	"net/mail"
	"net/smtp"
	"strings"
	"testing"
	"time"

	"github.com/truongpx396/intel-notification/adapters/driven/channel/email"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/notifytest"
)

var fixedNow = time.Date(2026, 10, 12, 9, 30, 0, 0, time.UTC)

func newSMTP(t *testing.T, srv *smtpServer, opts ...email.SMTPOption) *email.SMTP {
	t.Helper()
	opts = append([]email.SMTPOption{email.WithSMTPClock(func() time.Time { return fixedNow })}, opts...)
	m, err := email.NewSMTP(srv.addr(), "Notifications <notify@example.test>", opts...)
	if err != nil {
		t.Fatalf("NewSMTP: %v", err)
	}
	return m
}

func sampleMail() email.Mail {
	return email.Mail{
		To: "ada@example.com", Subject: "Upload finished", HTML: "<p>Hello, Ada = welcome</p>", IdemKey: "abc123",
		Headers: map[string]string{
			"List-Unsubscribe":      "<https://notify.example.test/u?t=1>",
			"List-Unsubscribe-Post": "List-Unsubscribe=One-Click",
		},
	}
}

// A plain relay accepts the same message twice and sends it twice: the mailer says
// so (D32), and the contract, run against a relay that counts, holds it to that.
func TestSMTPDeclaresThatItDoesNotDedupe(t *testing.T) {
	t.Parallel()
	m := newSMTP(t, newSMTPServer(t, nil, nil))
	if level, window := m.Dedup(); level != domain.DedupNone || window != 0 {
		t.Fatalf("Dedup = %v, %v, want DedupNone and no window: SMTP has no idempotency key", level, window)
	}
}

func TestSMTPRunsTheChannelContractThroughTheEmailChannel(t *testing.T) {
	t.Parallel()
	notifytest.ChannelContract(t, func(t *testing.T) notifytest.ChannelUnderTest {
		t.Helper()
		probe := notifytest.NewProbe()
		ch, _ := newChannel(t, newSMTP(t, newSMTPServer(t, probe, nil)))
		return notifytest.ChannelUnderTest{Channel: ch, Probe: probe}
	})
}

// A mailer that claims to dedupe over a relay that does not is caught by the same
// suite: the claim is held to what the provider side shows (D32).
func TestAnSMTPMailerThatClaimsProviderDedupFailsTheContract(t *testing.T) {
	t.Parallel()
	liar := func(t *testing.T) notifytest.ChannelUnderTest {
		t.Helper()
		probe := notifytest.NewProbe()
		ch, _ := newChannel(t, claimsDedup{newSMTP(t, newSMTPServer(t, probe, nil))})
		return notifytest.ChannelUnderTest{Channel: ch, Probe: probe}
	}
	u := liar(t)
	d := delivery(essential, "S", "B")
	for range 2 {
		if r, err := u.Channel.Deliver(t.Context(), d); err != nil || r.Outcome != domain.Delivered {
			t.Fatalf("Deliver = %+v, %v", r, err)
		}
	}
	if level := u.Channel.Capabilities().Dedup; level != domain.DedupProvider {
		t.Fatalf("the liar declares %v, the fixture is wrong", level)
	}
	if got := u.Probe.Sends(d.IdemKey); got != 2 {
		t.Fatalf("a relay that does not dedupe saw %d sends of one key, want 2: the contract's re-drive case would catch a DedupProvider claim here", got)
	}
}

type claimsDedup struct{ *email.SMTP }

func (claimsDedup) Dedup() (domain.DedupLevel, time.Duration) {
	return domain.DedupProvider, 24 * time.Hour
}

// The message on the wire, held to a file: the headers in a fixed order, the date
// from the mailer's clock, a Message-ID the key can be read back from, and the HTML
// quoted-printable. The file is written by hand.
func TestSMTPMessageMatchesTheGoldenFile(t *testing.T) {
	t.Parallel()
	srv := newSMTPServer(t, nil, nil)
	if _, err := newSMTP(t, srv).Send(t.Context(), sampleMail()); err != nil {
		t.Fatalf("Send: %v", err)
	}
	got := srv.accepted()
	if len(got) != 1 {
		t.Fatalf("the relay accepted %d messages, want 1", len(got))
	}
	if got[0].From != "notify@example.test" || len(got[0].To) != 1 || got[0].To[0] != "ada@example.com" {
		t.Fatalf("envelope = %q -> %q, want notify@example.test -> [ada@example.com]", got[0].From, got[0].To)
	}
	if want := golden(t, "smtp_message.golden"); got[0].Raw != want {
		t.Fatalf("the message differs from smtp_message.golden.\n--- got\n%s\n--- want\n%s", got[0].Raw, want)
	}
}

// Whatever a template or a producer puts in a header field cannot start a new one
// (CWE-93). A subject is encoded; a header that would break a line is refused, and
// nothing is sent.
func TestSMTPCannotBeUsedToInjectAHeader(t *testing.T) {
	t.Parallel()
	t.Run("a subject with a line break is encoded", func(t *testing.T) {
		t.Parallel()
		srv := newSMTPServer(t, nil, nil)
		m := sampleMail()
		m.Subject = "Hello\r\nBcc: eve@example.com"
		if _, err := newSMTP(t, srv).Send(t.Context(), m); err != nil {
			t.Fatalf("Send: %v", err)
		}
		msg, err := mail.ReadMessage(strings.NewReader(srv.accepted()[0].Raw))
		if err != nil {
			t.Fatalf("the relay's message does not parse: %v", err)
		}
		if bcc := msg.Header.Get("Bcc"); bcc != "" {
			t.Fatalf("the message has a Bcc: %q", bcc)
		}
		decoded, err := new(mime.WordDecoder).DecodeHeader(msg.Header.Get("Subject"))
		if err != nil || decoded != m.Subject {
			t.Fatalf("Subject decodes to %q, %v, want the original text back, line break and all, as data", decoded, err)
		}
	})
	t.Run("a header value that breaks a line is refused", func(t *testing.T) {
		t.Parallel()
		srv := newSMTPServer(t, nil, nil)
		m := sampleMail()
		m.Headers["List-Unsubscribe"] = "<https://x.test/u>\r\nBcc: eve@example.com"
		_, err := newSMTP(t, srv).Send(t.Context(), m)
		var se *email.SendError
		if !errors.As(err, &se) || se.Kind != email.Permanent {
			t.Fatalf("Send = %v, want a permanent *SendError: no retry makes a header one line", err)
		}
		if n := len(srv.accepted()); n != 0 {
			t.Fatalf("the relay accepted %d messages for a refused header", n)
		}
	})
	t.Run("a recipient that carries a header is refused", func(t *testing.T) {
		t.Parallel()
		srv := newSMTPServer(t, nil, nil)
		m := sampleMail()
		m.To = "ada@example.com\r\nBcc: eve@example.com"
		_, err := newSMTP(t, srv).Send(t.Context(), m)
		var se *email.SendError
		if !errors.As(err, &se) || se.Kind != email.InvalidAddress {
			t.Fatalf("Send = %v, want an *SendError of kind invalid address: the To line is written from this field", err)
		}
		if n := len(srv.accepted()); n != 0 {
			t.Fatalf("the relay accepted %d messages for a recipient with a header in it", n)
		}
	})
	t.Run("a header the mailer writes itself is refused", func(t *testing.T) {
		t.Parallel()
		for _, name := range []string{"From", "from", "TO", "Bcc", "Cc", "Subject", "Date", "Message-ID", "Content-Type", "MIME-Version", "Return-Path"} {
			srv := newSMTPServer(t, nil, nil)
			m := sampleMail()
			m.Headers = map[string]string{name: "eve@example.com"}
			_, err := newSMTP(t, srv).Send(t.Context(), m)
			var se *email.SendError
			if !errors.As(err, &se) || se.Kind != email.Permanent {
				t.Errorf("Send with a %q header = %v, want a permanent *SendError: a second one would spoof or duplicate the mailer's own", name, err)
			}
			if n := len(srv.accepted()); n != 0 {
				t.Errorf("the relay accepted %d messages carrying a %q header", n, name)
			}
		}
	})
	t.Run("a header name that is not a token is refused", func(t *testing.T) {
		t.Parallel()
		srv := newSMTPServer(t, nil, nil)
		m := sampleMail()
		m.Headers = map[string]string{"X Bad: name": "v"}
		_, err := newSMTP(t, srv).Send(t.Context(), m)
		var se *email.SendError
		if !errors.As(err, &se) || se.Kind != email.Permanent {
			t.Fatalf("Send = %v, want a permanent *SendError", err)
		}
	})
}

// A non-ASCII subject and body survive the round trip, and no line of the encoded
// body is longer than the 76 characters the format allows.
func TestSMTPEncodesNonASCIIAndLongLines(t *testing.T) {
	t.Parallel()
	srv := newSMTPServer(t, nil, nil)
	m := sampleMail()
	m.Subject = "Tải lên hoàn tất"
	m.HTML = "<p>" + strings.Repeat("Xin chào, đây là một dòng rất dài. ", 12) + "</p>\n<p>Dòng hai = ok</p>"
	if _, err := newSMTP(t, srv).Send(t.Context(), m); err != nil {
		t.Fatalf("Send: %v", err)
	}
	msg, err := mail.ReadMessage(strings.NewReader(srv.accepted()[0].Raw))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got, _ := new(mime.WordDecoder).DecodeHeader(msg.Header.Get("Subject")); got != m.Subject {
		t.Fatalf("Subject = %q, want %q", got, m.Subject)
	}
	raw, _ := io.ReadAll(msg.Body)
	for _, line := range strings.Split(string(raw), "\n") {
		if len(strings.TrimRight(line, "\r")) > 76 {
			t.Fatalf("an encoded body line is %d characters, want at most 76: %q", len(line), line)
		}
	}
	decoded, err := io.ReadAll(quotedprintable.NewReader(bytes.NewReader(raw)))
	if err != nil || strings.TrimRight(strings.ReplaceAll(string(decoded), "\r\n", "\n"), "\n") != m.HTML {
		t.Fatalf("the body decodes to %q, %v, want the original", decoded, err)
	}
}

// What the relay says decides the kind of failure, by the status it sends. The
// enhanced code (RFC 3463) is read first, wherever in the conversation it comes,
// because some relays judge a recipient only after DATA; 4xx is always worth a
// retry; and a refusal of our own credentials or sender is the provider's fault
// and not the message's, so it is transient and a failover chain moves on.
func TestSMTPClassifiesTheRelaysAnswers(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		stage string
		reply string
		want  email.ErrorKind
	}{
		{"user unknown at RCPT", "RCPT", "550 5.1.1 <ada@example.com>: Recipient address rejected: User unknown", email.HardBounce},
		{"user unknown after DATA", "DATA", "550 5.1.1 no such user", email.HardBounce},
		{"bad destination system", "RCPT", "550 5.1.2 Bad destination system address", email.HardBounce},
		{"bad mailbox syntax", "RCPT", "553 5.1.3 mailbox syntax incorrect", email.InvalidAddress},
		{"550 with no enhanced code at RCPT", "RCPT", "550 mailbox unavailable", email.HardBounce},
		{"551 user not local at RCPT", "RCPT", "551 user not local", email.HardBounce},
		{"553 mailbox name not allowed at RCPT", "RCPT", "553 mailbox name not allowed", email.InvalidAddress},
		{"a policy refusal at RCPT", "RCPT", "554 5.7.1 message blocked", email.Permanent},
		{"greylisting at RCPT", "RCPT", "450 4.2.0 greylisted, try later", email.Transient},
		{"mailbox full", "RCPT", "552 5.2.2 mailbox full", email.Transient},
		{"content refused at DATA", "DATA", "554 5.6.0 message content rejected", email.Permanent},
		{"busy at DATA", "DATA", "451 4.3.0 local error in processing", email.Transient},
		{"sender refused at MAIL", "MAIL", "553 5.1.8 sender address rejected", email.Transient},
		{"authentication refused", "AUTH", "535 5.7.8 authentication failed", email.Transient},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := newSMTPServer(t, nil, nil)
			srv.reply(tc.stage, tc.reply)
			opts := []email.SMTPOption{}
			if tc.stage == "AUTH" {
				opts = append(opts, email.WithSMTPAuth(smtp.PlainAuth("", "user", "pw", "127.0.0.1")))
			}
			_, err := newSMTP(t, srv, opts...).Send(t.Context(), sampleMail())
			var se *email.SendError
			if !errors.As(err, &se) || se.Kind != tc.want {
				t.Fatalf("Send = %v, want a *SendError of kind %v", err, tc.want)
			}
			if !strings.Contains(err.Error(), strings.Fields(tc.reply)[0]) {
				t.Fatalf("error %q does not carry the relay's status, which is the last error a dead letter keeps", err)
			}
			if len(srv.accepted()) != 0 {
				t.Fatal("a message was accepted although the relay refused it")
			}
		})
	}
}

// A relay that cannot be reached, or drops the connection, has given no verdict:
// the error is not a *SendError, so the channel treats it as transient.
func TestSMTPCannotReachTheRelay(t *testing.T) {
	t.Parallel()
	t.Run("nothing is listening", func(t *testing.T) {
		t.Parallel()
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := ln.Addr().String()
		_ = ln.Close()
		m, _ := email.NewSMTP(addr, "notify@example.test")
		_, err = m.Send(t.Context(), sampleMail())
		var se *email.SendError
		if err == nil || errors.As(err, &se) {
			t.Fatalf("Send = %v, want an error that is not a classified refusal", err)
		}
	})
	t.Run("the connection drops mid-message", func(t *testing.T) {
		t.Parallel()
		probe := notifytest.NewProbe()
		probe.FailNext(notifytest.Infrastructure)
		srv := newSMTPServer(t, probe, nil)
		m := sampleMail()
		_, err := newSMTP(t, srv).Send(t.Context(), m)
		var se *email.SendError
		if err == nil || errors.As(err, &se) {
			t.Fatalf("Send = %v, want an error that is not a classified refusal", err)
		}
	})
	// silentRelay accepts a connection and says nothing, as a hung relay does. It
	// signals the moment it has accepted one.
	silentRelay := func(t *testing.T) (addr string, accepted <-chan struct{}) {
		t.Helper()
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = ln.Close() })
		got := make(chan struct{})
		go func() {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			defer c.Close()
			close(got)
			_, _ = io.Copy(io.Discard, c)
		}()
		return ln.Addr().String(), got
	}
	returns := func(t *testing.T, send func() error) error {
		t.Helper()
		done := make(chan error, 1)
		go func() { done <- send() }()
		select {
		case err := <-done:
			return err
		case <-time.After(10 * time.Second):
			t.Fatal("Send did not return")
			return nil
		}
	}
	t.Run("the context is cancelled", func(t *testing.T) {
		t.Parallel()
		addr, accepted := silentRelay(t)
		m, _ := email.NewSMTP(addr, "notify@example.test")
		ctx, cancel := context.WithCancel(t.Context())
		go func() { <-accepted; cancel() }() // no deadline: only the cancellation can end it
		err := returns(t, func() error { _, err := m.Send(ctx, sampleMail()); return err })
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Send = %v, want an error that is context.Canceled", err)
		}
	})
	t.Run("the context has a deadline", func(t *testing.T) {
		t.Parallel()
		addr, _ := silentRelay(t)
		m, _ := email.NewSMTP(addr, "notify@example.test")
		ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
		defer cancel()
		err := returns(t, func() error { _, err := m.Send(ctx, sampleMail()); return err })
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Send = %v, want an error that is context.DeadlineExceeded", err)
		}
	})
	t.Run("the relay is silent and the context never ends", func(t *testing.T) {
		t.Parallel()
		addr, _ := silentRelay(t)
		m, _ := email.NewSMTP(addr, "notify@example.test", email.WithSMTPTimeout(200*time.Millisecond))
		if err := returns(t, func() error { _, err := m.Send(t.Context(), sampleMail()); return err }); err == nil {
			t.Fatal("Send to a silent relay succeeded")
		}
	})
}

// A relay that offers STARTTLS is spoken to over TLS, and the certificate is
// checked: a mailer that skipped either would send credentials and mail in clear.
func TestSMTPUsesStartTLSWhenOfferedAndChecksTheCertificate(t *testing.T) {
	t.Parallel()
	cert, pool := selfSigned(t)
	serverTLS := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}

	t.Run("a trusted certificate", func(t *testing.T) {
		t.Parallel()
		srv := newSMTPServer(t, nil, serverTLS)
		if _, err := newSMTP(t, srv, email.WithSMTPTLS(&tls.Config{RootCAs: pool, ServerName: "127.0.0.1", MinVersion: tls.VersionTLS12})).Send(t.Context(), sampleMail()); err != nil {
			t.Fatalf("Send: %v", err)
		}
		if got := srv.accepted(); len(got) != 1 || !got[0].TLS {
			t.Fatalf("accepted %+v, want one message that arrived over TLS", got)
		}
	})
	t.Run("a certificate nobody trusts", func(t *testing.T) {
		t.Parallel()
		srv := newSMTPServer(t, nil, serverTLS)
		_, err := newSMTP(t, srv).Send(t.Context(), sampleMail()) // default roots do not include the test CA
		if err == nil {
			t.Fatal("Send over a certificate the roots do not vouch for succeeded")
		}
		if !strings.Contains(err.Error(), "STARTTLS") {
			t.Fatalf("Send = %v, want the failure to be the STARTTLS handshake's: a mailer that went on in clear after it would fail a later command instead", err)
		}
		if len(srv.accepted()) != 0 {
			t.Fatal("a message was sent over an unverified connection")
		}
	})
}

func selfSigned(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "fake relay"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, BasicConstraintsValid: true, IsCA: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, _ := x509.ParseCertificate(der)
	pool := x509.NewCertPool()
	pool.AddCert(leaf)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, pool
}

// A relay beyond localhost must offer STARTTLS, or nothing is sent: a network that
// strips the offer would otherwise read the mail and the credentials.
func TestSMTPRequireTLSRefusesARelayThatDoesNotOfferIt(t *testing.T) {
	t.Parallel()
	srv := newSMTPServer(t, nil, nil) // offers no STARTTLS
	_, err := newSMTP(t, srv, email.WithSMTPRequireTLS()).Send(t.Context(), sampleMail())
	var se *email.SendError
	if err == nil || errors.As(err, &se) {
		t.Fatalf("Send = %v, want a plain error: no relay has given a verdict, and the mail waits for one that offers TLS", err)
	}
	if len(srv.accepted()) != 0 {
		t.Fatal("a message was sent in clear although TLS was required")
	}
}

// A sender that is not a mailbox, or a relay with no address, is found at wiring.
func TestNewSMTPRefusesWhatCannotWork(t *testing.T) {
	t.Parallel()
	for name, c := range map[string]struct{ addr, from string }{
		"no relay address":               {"", "notify@example.test"},
		"no port":                        {"localhost", "notify@example.test"},
		"no sender":                      {"localhost:25", ""},
		"a sender that is not a mailbox": {"localhost:25", "not an address"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := email.NewSMTP(c.addr, c.from); err == nil {
				t.Fatal("NewSMTP succeeded")
			}
		})
	}
}
