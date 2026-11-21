package grpcclient

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"

	"github.com/truongpx396/intel-notification/adapters/driving/wire"
	"github.com/truongpx396/intel-notification/api/notifyv1"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// MaxDirectoryPage is the most recipients one answer of a host's Directory may carry. A host that pages
// its audience in larger pages than this is asked for smaller ones: one page is one transaction of the
// broadcast (D7, D53).
const MaxDirectoryPage = 5000

// defaultDirectoryTimeout bounds one call to a host's Directory.
const defaultDirectoryTimeout = 10 * time.Second

// Directory is the engine's side of the host's Directory callback (D31): a ports.AudienceResolver that
// asks the host, over gRPC, who an audience selector names, a page at a time. One host serves one realm,
// so a call goes to the realm's own Directory and to no other.
type Directory struct {
	clients map[domain.Realm]notifyv1.DirectoryClient
	timeout time.Duration
}

var _ ports.AudienceResolver = (*Directory)(nil)

// DirectoryOption configures a Directory.
type DirectoryOption func(*Directory)

// WithDirectoryTimeout bounds one call to a host's Directory. A host that does not answer in time fails the
// page, which the broadcast resumes at (T042); it does not hold a worker for ever. The default is ten
// seconds.
func WithDirectoryTimeout(d time.Duration) DirectoryOption {
	return func(x *Directory) { x.timeout = d }
}

// NewDirectory returns a Directory over one connection per realm.
func NewDirectory(conns map[domain.Realm]grpc.ClientConnInterface, opts ...DirectoryOption) *Directory {
	d := &Directory{clients: make(map[domain.Realm]notifyv1.DirectoryClient, len(conns)), timeout: defaultDirectoryTimeout}
	for realm, conn := range conns {
		d.clients[realm] = notifyv1.NewDirectoryClient(conn)
	}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

// Resolve implements ports.AudienceResolver. It returns a page whole or an error and nothing else: a
// host that fails or is too slow fails the page, and the broadcast resumes at it, where a page committed
// short would notify some of an audience and never the rest. What the host returns is the host's, so an
// identity with an empty component, which would equal an unset scope (D17), refuses the whole page, as
// does one over MaxDirectoryPage.
func (d *Directory) Resolve(ctx context.Context, realm domain.Realm, t domain.Tenant, audience, cursor string) ([]domain.Recipient, string, error) {
	client, ok := d.clients[realm]
	if !ok {
		return nil, "", fmt.Errorf("%w: no Directory serves realm %q", domain.ErrAudienceUnsupported, realm)
	}
	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()
	resp, err := client.ResolveAudience(ctx, &notifyv1.ResolveAudienceRequest{
		Tenant: &notifyv1.Tenant{Kind: t.Kind, Id: t.ID}, Audience: audience, Cursor: cursor,
	})
	if err != nil {
		return nil, "", fmt.Errorf("the Directory of realm %q: %w", realm, wire.FromStatus(err))
	}
	if n := len(resp.GetRecipients()); n > MaxDirectoryPage {
		return nil, "", fmt.Errorf("the Directory of realm %q answered a page of %d recipients, the most taken is %d", realm, n, MaxDirectoryPage)
	}
	out := make([]domain.Recipient, 0, len(resp.GetRecipients()))
	for i, r := range resp.GetRecipients() {
		if r.GetKind() == "" || r.GetId() == "" {
			return nil, "", fmt.Errorf("the Directory of realm %q answered a recipient (%d) with no kind or no id: %w", realm, i, domain.ErrEmptyIdentity)
		}
		out = append(out, domain.Recipient{Kind: r.GetKind(), ID: r.GetId()})
	}
	return out, resp.GetNextCursor(), nil
}
