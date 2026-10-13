package email

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"mime"
	"mime/quotedprintable"
	"net"
	"net/mail"
	"net/smtp"
	"net/textproto"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/truongpx396/intel-notification/domain"
)

// SMTP is a Mailer over an SMTP relay: Mailpit in the e2e stack, a company relay,
// a provider's SMTP endpoint.
//
// It declares DedupNone. SMTP has no idempotency key, so a delivery re-driven after
// a crash between the send and its record is sent again (D32). The Message-ID is
// derived from the delivery key, which a receiving server may use to drop a
// repeat, but that is a courtesy and not a guarantee, so it is not claimed.
//
// STARTTLS is used whenever the relay offers it, with the certificate checked, and
// a handshake that fails is an error and never a fall back to clear text.
type SMTP struct {
	addr       string
	host       string
	from       *mail.Address
	auth       smtp.Auth
	tlsConfig  *tls.Config
	requireTLS bool
	timeout    time.Duration
	now        func() time.Time
}

var _ Mailer = (*SMTP)(nil)

// SMTPOption configures an SMTP mailer.
type SMTPOption func(*SMTP)

// WithSMTPAuth authenticates to the relay. net/smtp sends PLAIN credentials only
// over TLS, or to localhost.
func WithSMTPAuth(a smtp.Auth) SMTPOption { return func(s *SMTP) { s.auth = a } }

// WithSMTPClock sets the clock the Date header is read from.
func WithSMTPClock(now func() time.Time) SMTPOption {
	return func(s *SMTP) {
		if now != nil {
			s.now = now
		}
	}
}

// WithSMTPTLS sets the TLS configuration used when the relay offers STARTTLS. The
// default verifies the certificate against the system roots for the relay's host.
func WithSMTPTLS(c *tls.Config) SMTPOption { return func(s *SMTP) { s.tlsConfig = c } }

// WithSMTPRequireTLS refuses to send to a relay that does not offer STARTTLS, so a
// network that strips the offer cannot downgrade the connection. Use it for any
// relay beyond localhost.
func WithSMTPRequireTLS() SMTPOption { return func(s *SMTP) { s.requireTLS = true } }

// WithSMTPTimeout bounds one Send from connecting to the relay's final answer. The
// default is two minutes. A context that ends sooner ends the Send sooner.
func WithSMTPTimeout(d time.Duration) SMTPOption {
	return func(s *SMTP) {
		if d > 0 {
			s.timeout = d
		}
	}
}

// NewSMTP returns a mailer that sends through the relay at addr (host:port), as
// from, a mailbox with or without a display name.
func NewSMTP(addr, from string, opts ...SMTPOption) (*SMTP, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil || host == "" || port == "" {
		return nil, fmt.Errorf("email: SMTP relay address %q is not host:port", addr)
	}
	sender, err := mail.ParseAddress(from)
	if err != nil {
		return nil, fmt.Errorf("email: SMTP sender %q is not a mailbox: %w", from, err)
	}
	s := &SMTP{addr: addr, host: host, from: sender, timeout: defaultTimeout, now: time.Now}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

// Dedup implements Mailer: a relay does not honour an idempotency key.
func (*SMTP) Dedup() (domain.DedupLevel, time.Duration) { return domain.DedupNone, 0 }

// defaultTimeout bounds one Send.
const defaultTimeout = 2 * time.Minute

// Send implements Mailer. The provider message id is the Message-ID, which a
// delivery status notification names as the message it concerns.
func (s *SMTP) Send(ctx context.Context, m Mail) (string, error) {
	msg, msgID, err := s.build(m)
	if err != nil {
		return "", err
	}
	if err := s.deliver(ctx, m.To, msg); err != nil {
		if ctx.Err() != nil { // the connection was closed under it: the context is the cause
			return "", fmt.Errorf("email: smtp: %w", ctx.Err())
		}
		return "", err
	}
	return msgID, nil
}

func (s *SMTP) deliver(ctx context.Context, to string, msg []byte) error {
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", s.addr)
	if err != nil {
		return fmt.Errorf("email: smtp connect: %w", err)
	}
	defer func() { _ = conn.Close() }()
	// net/smtp has no context. The Send is bounded in real time, since the clock
	// option only dates the message, and cancelling the context closes the
	// connection, which fails whatever the client is waiting on. A context's own
	// deadline cancels it, so it needs no handling here.
	_ = conn.SetDeadline(time.Now().Add(s.timeout))
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	c, err := smtp.NewClient(conn, s.host)
	if err != nil {
		return classifySMTP("greeting", err)
	}
	defer func() { _ = c.Close() }()
	if offered, _ := c.Extension("STARTTLS"); offered {
		cfg := &tls.Config{MinVersion: tls.VersionTLS12}
		if s.tlsConfig != nil {
			cfg = s.tlsConfig.Clone()
		}
		if cfg.ServerName == "" {
			cfg.ServerName = s.host
		}
		if err := c.StartTLS(cfg); err != nil {
			return classifySMTP("STARTTLS", err)
		}
	} else if s.requireTLS {
		return errors.New("email: smtp: the relay does not offer STARTTLS, and TLS is required")
	}
	if s.auth != nil {
		if err := c.Auth(s.auth); err != nil {
			return classifySMTP("AUTH", err)
		}
	}
	if err := c.Mail(s.from.Address); err != nil {
		return classifySMTP("MAIL", err)
	}
	if err := c.Rcpt(to); err != nil {
		return classifySMTP("RCPT", err)
	}
	w, err := c.Data()
	if err != nil {
		return classifySMTP("DATA", err)
	}
	if _, err := w.Write(msg); err != nil {
		return classifySMTP("DATA", err)
	}
	if err := w.Close(); err != nil { // the relay's verdict on the message arrives here
		return classifySMTP("DATA", err)
	}
	_ = c.Quit()
	return nil
}

var enhancedStatus = regexp.MustCompile(`^([245])\.\d{1,3}\.\d{1,3}(?:\s|$)`)

// classifySMTP turns a relay's refusal into a *SendError, and leaves any other
// error, a dropped connection or a failed handshake, as it is: no verdict was
// given, so the channel treats it as transient.
//
// The status is read, not the stage, wherever a recipient is judged: the enhanced
// code (RFC 3463) when the relay sends one, since some relays judge a recipient
// only after DATA, and the basic code otherwise. 4xx is always worth a retry. A
// refusal of our credentials or our sender is the provider's fault and not the
// message's, so it is transient, and a failover chain moves to the next provider.
func classifySMTP(stage string, err error) error {
	var te *textproto.Error
	if !errors.As(err, &te) {
		return fmt.Errorf("email: smtp %s: %w", stage, err)
	}
	return &SendError{Kind: smtpKind(stage, te), Err: fmt.Errorf("smtp %s: %w", stage, err)}
}

func smtpKind(stage string, te *textproto.Error) ErrorKind {
	if te.Code >= 400 && te.Code < 500 {
		return Transient
	}
	if stage == "AUTH" || stage == "MAIL" {
		return Transient
	}
	enhanced := ""
	if m := enhancedStatus.FindString(te.Msg); m != "" {
		enhanced = strings.TrimSpace(m)
	}
	atRcpt := stage == "RCPT"
	switch {
	case enhanced == "5.1.3", enhanced == "" && atRcpt && te.Code == 553:
		return InvalidAddress
	case strings.HasPrefix(enhanced, "5.1."), enhanced == "" && atRcpt && (te.Code == 550 || te.Code == 551):
		return HardBounce
	case enhanced == "5.2.2", enhanced == "" && atRcpt && te.Code == 552:
		return Transient // a full mailbox empties
	}
	return Permanent
}

// reservedHeaders are written by the mailer. A caller's header of the same name
// would duplicate or override them: a second From is a spoofing vector.
var reservedHeaders = []string{"from", "to", "cc", "bcc", "subject", "date", "message-id", "mime-version", "content-type", "content-transfer-encoding", "return-path"}

// headerName is an RFC 5322 field name: printable ASCII but for the colon.
var headerName = regexp.MustCompile(`^[\x21-\x39\x3b-\x7e]+$`)

// build writes the message. Nothing a caller supplies reaches a header unchecked: the
// recipient must be one bare mailbox, the subject is encoded, and a custom header
// that is not a single line, or is one the mailer writes itself, is refused as
// permanent, since no retry changes it.
func (s *SMTP) build(m Mail) ([]byte, string, error) {
	if to, err := mail.ParseAddress(m.To); err != nil || to.Address != m.To {
		return nil, "", &SendError{Kind: InvalidAddress, Err: fmt.Errorf("smtp: %q is not a single mailbox", m.To)}
	}
	names := make([]string, 0, len(m.Headers))
	for k, v := range m.Headers {
		if !headerName.MatchString(k) || strings.ContainsAny(v, "\r\n\x00") {
			return nil, "", &SendError{Kind: Permanent, Err: fmt.Errorf("smtp: header %q is not a single line", k)}
		}
		if slices.Contains(reservedHeaders, strings.ToLower(k)) {
			return nil, "", &SendError{Kind: Permanent, Err: fmt.Errorf("smtp: header %q is written by the mailer", k)}
		}
		names = append(names, k)
	}
	slices.Sort(names)

	id := messageIDLocal(m.IdemKey)
	_, fromDomain, _ := strings.Cut(s.from.Address, "@")
	msgID := id + "@" + fromDomain

	var b bytes.Buffer
	line := func(k, v string) { b.WriteString(k + ": " + v + "\r\n") }
	line("From", s.from.String())
	line("To", m.To)
	line("Subject", mime.QEncoding.Encode("utf-8", m.Subject))
	line("Date", s.now().Format(time.RFC1123Z))
	line("Message-ID", "<"+msgID+">")
	line("MIME-Version", "1.0")
	line("Content-Type", "text/html; charset=UTF-8")
	line("Content-Transfer-Encoding", "quoted-printable")
	for _, k := range names {
		line(k, m.Headers[k])
	}
	b.WriteString("\r\n")
	body := strings.ReplaceAll(strings.ReplaceAll(m.HTML, "\r\n", "\n"), "\n", "\r\n")
	qp := quotedprintable.NewWriter(&b)
	if _, err := qp.Write([]byte(body)); err != nil {
		return nil, "", fmt.Errorf("email: smtp: encode the body: %w", err)
	}
	if err := qp.Close(); err != nil {
		return nil, "", fmt.Errorf("email: smtp: encode the body: %w", err)
	}
	return b.Bytes(), msgID, nil
}

var safeIDChars = regexp.MustCompile(`^[A-Za-z0-9._-]{1,200}$`)

// messageIDLocal is the delivery key when it can stand as the local part of a
// Message-ID, and its hash when it cannot.
func messageIDLocal(key string) string {
	if safeIDChars.MatchString(key) {
		return key
	}
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}
