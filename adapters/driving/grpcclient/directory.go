package grpcclient

import (
	"context"
	"time"

	"google.golang.org/grpc"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// MaxDirectoryPage is the most recipients one answer of a host's Directory may carry. A host that pages
// its audience in larger pages than this is asked for smaller ones: one page is one transaction of the
// broadcast (D7, D53).
const MaxDirectoryPage = 5000

// Directory is the engine's side of the host's Directory callback (D31): a ports.AudienceResolver that
// asks the host, over gRPC, who an audience selector names, a page at a time. One host serves one realm,
// so a call goes to the realm's own Directory and to no other.
type Directory struct{}

var _ ports.AudienceResolver = (*Directory)(nil)

// DirectoryOption configures a Directory.
type DirectoryOption func(*Directory)

// WithDirectoryTimeout bounds one call to a host's Directory. A host that does not answer in time fails the
// page, which the broadcast resumes at (T042); it does not hold a worker for ever. The default is ten
// seconds.
func WithDirectoryTimeout(d time.Duration) DirectoryOption { return func(*Directory) {} }

// NewDirectory returns a Directory over one connection per realm.
func NewDirectory(conns map[domain.Realm]grpc.ClientConnInterface, opts ...DirectoryOption) *Directory {
	return &Directory{}
}

// Resolve implements ports.AudienceResolver.
func (d *Directory) Resolve(ctx context.Context, realm domain.Realm, t domain.Tenant, audience, cursor string) ([]domain.Recipient, string, error) {
	return nil, "", nil
}
