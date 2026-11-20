package notifytest

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"sync"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

type audienceKey struct {
	realm    domain.Realm
	tenant   domain.Tenant
	audience string
}

// AudienceCall is one question put to [Audiences.Resolve].
type AudienceCall struct {
	Realm    domain.Realm
	Tenant   domain.Tenant
	Audience string
	Cursor   string
}

// Audiences is an in-memory ports.AudienceResolver (D7): host-defined selectors, each
// naming an ordered list of recipients in one realm and tenant, served in pages of a size
// the test chooses. A cursor is the offset of the page's first member, and the last page
// returns the empty cursor, which is how a resolver says it is done.
//
// It remembers every question it was asked, so a test can show a worker resumed at a
// cursor and did not ask for the pages before it again.
type Audiences struct {
	mu       sync.Mutex
	pageSize int
	sets     map[audienceKey][]domain.Recipient
	calls    []AudienceCall
	fail     []error
}

var _ ports.AudienceResolver = (*Audiences)(nil)

// NewAudiences returns a resolver with no audiences, serving pageSize members a page. It
// panics for a page size below one, which no resolver could serve.
func NewAudiences(pageSize int) *Audiences {
	if pageSize < 1 {
		panic(fmt.Sprintf("notifytest: audience page size %d is not at least one", pageSize))
	}
	return &Audiences{pageSize: pageSize, sets: map[audienceKey][]domain.Recipient{}}
}

// Set defines audience in realm and tenant as members, in order, replacing an earlier
// definition. The members are copied.
func (a *Audiences) Set(realm domain.Realm, t domain.Tenant, audience string, members ...domain.Recipient) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.sets[audienceKey{realm, t, audience}] = slices.Clone(members)
}

// FailNext makes the next question fail with err, once. Calling it again queues another.
func (a *Audiences) FailNext(err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.fail = append(a.fail, err)
}

// Calls returns every question asked so far, in order.
func (a *Audiences) Calls() []AudienceCall {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.calls)
}

// Resolve implements ports.AudienceResolver. An audience that was never set is an error,
// not an empty one, as a host's resolver refuses a selector it does not know; so is a
// cursor the resolver did not issue.
func (a *Audiences) Resolve(_ context.Context, realm domain.Realm, t domain.Tenant, audience, cursor string) ([]domain.Recipient, string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls = append(a.calls, AudienceCall{Realm: realm, Tenant: t, Audience: audience, Cursor: cursor})
	if len(a.fail) > 0 {
		err := a.fail[0]
		a.fail = a.fail[1:]
		return nil, "", err
	}
	members, ok := a.sets[audienceKey{realm, t, audience}]
	if !ok {
		return nil, "", fmt.Errorf("notifytest: audience %q is not set for %s/%s in realm %q", audience, t.Kind, t.ID, realm)
	}
	offset := 0
	if cursor != "" {
		n, err := strconv.Atoi(cursor)
		if err != nil || n < 0 || n > len(members) {
			return nil, "", fmt.Errorf("notifytest: cursor %q was not issued by this resolver", cursor)
		}
		offset = n
	}
	end := min(offset+a.pageSize, len(members))
	next := ""
	if end < len(members) {
		next = strconv.Itoa(end)
	}
	return slices.Clone(members[offset:end]), next, nil
}
