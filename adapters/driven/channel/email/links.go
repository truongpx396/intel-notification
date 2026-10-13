package email

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// unsubscribePath is the REST surface's unsubscribe route (rest-api.md). GET on it renders a
// confirmation and changes nothing; the one-click POST is what unsubscribes (D34).
const unsubscribePath = "/notifications/unsubscribe"

// SignedLinks makes the unsubscribe link in an email: the unsubscribe route with a signed token for
// one (realm, tenant, recipient, topic, channel) pair (D34). A link is made for each message, and its
// life of at least 60 days runs from the moment it is made.
//
// RFC 8058 wants one HTTPS URI in List-Unsubscribe, and a mail scanner follows whatever it is given,
// so a base URL that is not HTTPS is refused unless the host asks for [AllowInsecureHTTP]: the e2e
// stack, which runs on localhost, does.
type SignedLinks struct {
	signer    *domain.UnsubscribeSigner
	base      *url.URL
	clock     ports.Clock
	allowHTTP bool
}

var _ UnsubscribeLinks = (*SignedLinks)(nil)

// LinksOption configures SignedLinks.
type LinksOption func(*SignedLinks)

// AllowInsecureHTTP permits a base URL that is plain HTTP, for development and the e2e stack. A link
// to a public host over HTTP breaks RFC 8058 and lets anyone on the path read and replay the token.
func AllowInsecureHTTP() LinksOption { return func(l *SignedLinks) { l.allowHTTP = true } }

// NewSignedLinks returns links under baseURL, the public address of the REST surface, signed by
// signer and dated by clock. A base that cannot work is refused here, when the host starts, and not
// on the first send: it must be an absolute HTTPS URL with a host, and no credentials, query or
// fragment of its own.
func NewSignedLinks(signer *domain.UnsubscribeSigner, baseURL string, clock ports.Clock, opts ...LinksOption) (*SignedLinks, error) {
	if signer == nil {
		return nil, errors.New("email: SignedLinks needs an UnsubscribeSigner")
	}
	if clock == nil {
		return nil, errors.New("email: SignedLinks needs a ports.Clock")
	}
	l := &SignedLinks{signer: signer, clock: clock}
	for _, opt := range opts {
		opt(l)
	}
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("email: unsubscribe base URL %q: %w", baseURL, err)
	}
	switch {
	case u.Scheme == "https", u.Scheme == "http" && l.allowHTTP:
	case u.Scheme == "http":
		return nil, fmt.Errorf("email: unsubscribe base URL %q is plain HTTP: RFC 8058 wants HTTPS (AllowInsecureHTTP is for development)", baseURL)
	default:
		return nil, fmt.Errorf("email: unsubscribe base URL %q is not an absolute HTTPS URL", baseURL)
	}
	if u.Host == "" || u.Hostname() == "" {
		return nil, fmt.Errorf("email: unsubscribe base URL %q has no host", baseURL)
	}
	if u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return nil, fmt.Errorf("email: unsubscribe base URL %q must not carry credentials, a query or a fragment", baseURL)
	}
	u.Path = strings.TrimRight(u.Path, "/")
	u.RawPath = ""
	l.base = u
	return l, nil
}

// For implements UnsubscribeLinks. The token is for the delivery's own pair, so a leaked link turns
// off one preference and nothing else. A delivery that names no topic or channel has no pair, and the
// channel waits on that as the fault it is.
func (l *SignedLinks) For(_ context.Context, d domain.Delivery) (string, error) {
	token, err := l.signer.Sign(domain.UnsubscribeScope{Identity: d.Identity, Topic: d.Topic, Channel: d.Channel}, l.clock.Now())
	if err != nil {
		return "", fmt.Errorf("email: sign the unsubscribe link: %w", err)
	}
	link := *l.base
	link.Path += unsubscribePath
	link.RawQuery = "token=" + url.QueryEscape(token)
	return link.String(), nil
}
