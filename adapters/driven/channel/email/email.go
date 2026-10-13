// Package email is the reference email channel. It sends what the template
// renderer produced, through a Mailer, and carries the compliance that is specific
// to email: the unsubscribe footer and the RFC 8058 headers on a topic that is not
// essential (NR-015, D34). Whether an address may be mailed at all is the
// dispatcher's to decide, for every channel (D13).
package email

import (
	"context"
	"errors"
	"fmt"
	"html"
	"net/mail"
	"strings"
	"time"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// Kind is the channel kind a topic's DefaultChannels and a preference name.
const Kind domain.ChannelKind = "email"

// Mail is one message as a Mailer sends it.
type Mail struct {
	To      string
	Subject string
	HTML    string
	Headers map[string]string
	// IdemKey is Delivery.IdemKey: stable across re-drives and distinct per address
	// (D25), so a provider that honours it collapses a re-drive.
	IdemKey string
}

// Mailer is one email provider: SMTP, Resend, SES.
type Mailer interface {
	// Send hands one message to the provider and returns the provider's message id.
	// A failure the provider reported is a *[SendError], which says what kind it
	// was; any other error is a fault the channel treats as transient.
	Send(ctx context.Context, m Mail) (messageID string, err error)
	// Dedup is what the provider guarantees on Mail.IdemKey, and for how long, as
	// its sandbox shows (D32). A provider that does not honour a key says DedupNone.
	Dedup() (level domain.DedupLevel, window time.Duration)
}

// UnsubscribeLinks makes the link a recipient follows to unsubscribe from one
// (realm, tenant, recipient, topic, channel) pair (D34). The URI must be HTTPS and
// valid for at least 60 days; the signer that makes it owns both.
type UnsubscribeLinks interface {
	For(ctx context.Context, d domain.Delivery) (string, error)
}

// ErrorKind says what a provider's refusal means for the delivery.
type ErrorKind int

const (
	Transient      ErrorKind = iota // try again: the provider is busy, down or rate limiting
	HardBounce                      // the mailbox does not exist
	InvalidAddress                  // the address is malformed or refused outright
	Permanent                       // a refusal unrelated to the address: a malformed message, a rejected body
)

func (k ErrorKind) String() string {
	switch k {
	case Transient:
		return "transient"
	case HardBounce:
		return "hard bounce"
	case InvalidAddress:
		return "invalid address"
	case Permanent:
		return "permanent"
	}
	return fmt.Sprintf("error kind %d", int(k))
}

// SendError is a failure a provider reported, classified.
type SendError struct {
	Kind       ErrorKind
	RetryAfter time.Duration // with Transient: the provider's hint, a floor on backoff (D10)
	Err        error
}

func (e *SendError) Error() string {
	if e.Err == nil {
		return "email: " + e.Kind.String()
	}
	return "email: " + e.Kind.String() + ": " + e.Err.Error()
}

func (e *SendError) Unwrap() error { return e.Err }

// Channel sends email through a Mailer.
type Channel struct {
	mailer Mailer
	topics ports.TopicRegistry
	links  UnsubscribeLinks
	footer func(link string) string
}

var (
	_ ports.Channel           = (*Channel)(nil)
	_ ports.AddressNormalizer = (*Channel)(nil)
)

// Option configures a Channel.
type Option func(*Channel)

// WithFooter replaces the default unsubscribe footer: f is given the link and
// returns the HTML appended to a non-essential message. Copy is the template's to
// own (NR-019), so a host that wants its own words, in its own language, passes
// them here. f must escape the link for HTML itself.
func WithFooter(f func(link string) string) Option {
	return func(c *Channel) {
		if f != nil {
			c.footer = f
		}
	}
}

// defaultFooter is plain English, with the link escaped for an href.
func defaultFooter(link string) string {
	return "\n<hr>\n<p style=\"color:#666;font-size:12px\">You are receiving this email because of your " +
		"notification settings. <a href=\"" + html.EscapeString(link) + "\">Unsubscribe</a> from this kind of notification.</p>\n"
}

// New returns the email channel. A missing port is a wiring mistake and panics,
// naming every one, so it is found when the host starts and not on the first
// delivery.
func New(mailer Mailer, topics ports.TopicRegistry, links UnsubscribeLinks, opts ...Option) *Channel {
	var missing []string
	if mailer == nil {
		missing = append(missing, "a Mailer")
	}
	if topics == nil {
		missing = append(missing, "a ports.TopicRegistry")
	}
	if links == nil {
		missing = append(missing, "an UnsubscribeLinks")
	}
	if len(missing) > 0 {
		panic("email: New needs " + strings.Join(missing, ", "))
	}
	c := &Channel{mailer: mailer, topics: topics, links: links, footer: defaultFooter}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Kind implements ports.Channel.
func (*Channel) Kind() domain.ChannelKind { return Kind }

// Capabilities implements ports.Channel. The channel's guarantee on a re-drive is
// its provider's, and no stronger: it keeps no record of its own (D32). A window
// means something only with DedupProvider, so it is reported only then.
func (c *Channel) Capabilities() domain.ChannelCapabilities {
	level, window := c.mailer.Dedup()
	if level != domain.DedupProvider {
		window = 0
	}
	return domain.ChannelCapabilities{NeedsSubject: true, NeedsAddress: true, Dedup: level, DedupWindow: window}
}

// NormalizeAddress implements ports.AddressNormalizer: the domain of an address is
// case-insensitive and its local part is not assumed to be, so only the domain is
// lowercased, after the surrounding space is trimmed. The address key and the
// suppression hash are made over this form.
func (*Channel) NormalizeAddress(v string) string {
	v = strings.TrimSpace(v)
	at := strings.LastIndexByte(v, '@')
	if at < 0 {
		return v
	}
	return v[:at+1] + strings.ToLower(v[at+1:])
}

// Deliver implements ports.Channel.
//
// A non-essential topic carries the RFC 8058 one-click headers and a footer with
// the same link. If the channel cannot learn whether the topic is essential, or
// cannot make the link, the delivery waits for it as a fault: sending without the
// headers would be the compliance failure. A topic the registry does not know is
// treated as non-essential, so uncertainty falls on the side of the unsubscribe
// link.
//
// An address that is not exactly one mailbox is the address's problem and is
// suppressed before a provider is asked; an empty one is a data fault and is
// rejected, because suppressing it would suppress the hash of nothing (D13).
func (c *Channel) Deliver(ctx context.Context, d domain.Delivery) (domain.DeliveryResult, error) {
	to := strings.TrimSpace(d.Address.Value)
	if to == "" {
		return domain.DeliveryResult{Outcome: domain.Rejected, Detail: "email: the delivery has no address"}, nil
	}
	if parsed, err := mail.ParseAddress(to); err != nil || parsed.Address != to {
		return domain.DeliveryResult{
			Outcome:        domain.Suppressed,
			SuppressReason: domain.SuppressInvalidAddress,
			Detail:         "email: the address is not a single mailbox",
		}, nil
	}

	def, found, err := c.topics.Lookup(ctx, d.Identity.Realm, d.Topic)
	if err != nil {
		return domain.DeliveryResult{Outcome: domain.Retry, Detail: "email: look up the topic: " + err.Error()}, nil
	}
	m := Mail{To: to, Subject: d.Content.Subject, HTML: d.Content.Body, IdemKey: d.IdemKey}
	if !found || !def.Essential {
		link, err := c.links.For(ctx, d)
		if err != nil {
			return domain.DeliveryResult{Outcome: domain.Retry, Detail: "email: make the unsubscribe link: " + err.Error()}, nil
		}
		if link == "" || strings.ContainsAny(link, "\r\n\t <>") {
			return domain.DeliveryResult{Outcome: domain.Retry, Detail: "email: the unsubscribe link is not a single URI"}, nil
		}
		m.HTML += c.footer(link)
		m.Headers = map[string]string{
			"List-Unsubscribe":      "<" + link + ">",
			"List-Unsubscribe-Post": "List-Unsubscribe=One-Click",
		}
	}

	id, err := c.mailer.Send(ctx, m)
	if err == nil {
		return domain.DeliveryResult{Outcome: domain.Delivered, ProviderMessageID: id}, nil
	}
	var sendErr *SendError
	if errors.As(err, &sendErr) {
		switch sendErr.Kind {
		case HardBounce:
			return domain.DeliveryResult{Outcome: domain.Suppressed, SuppressReason: domain.SuppressHardBounce, Detail: err.Error()}, nil
		case InvalidAddress:
			return domain.DeliveryResult{Outcome: domain.Suppressed, SuppressReason: domain.SuppressInvalidAddress, Detail: err.Error()}, nil
		case Permanent:
			return domain.DeliveryResult{Outcome: domain.Rejected, Detail: err.Error()}, nil
		case Transient:
			return domain.DeliveryResult{Outcome: domain.Retry, RetryAfter: sendErr.RetryAfter, Detail: err.Error()}, nil
		}
	}
	return domain.DeliveryResult{Outcome: domain.Retry, Detail: err.Error()}, nil
}
