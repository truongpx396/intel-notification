package notifytest

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/truongpx396/intel-notification/domain"
)

func members(n int) []domain.Recipient {
	out := make([]domain.Recipient, n)
	for i := range out {
		out[i] = domain.Recipient{Kind: "user", ID: fmt.Sprintf("u%d", i+1)}
	}
	return out
}

var (
	audRealm  domain.Realm = "aisat"
	audTenant              = domain.Tenant{Kind: "workspace", ID: "w1"}
)

// A resolver serves its members in pages, in order, and the cursor it hands back leads to
// the next page and ends with the empty cursor: the whole of D7's contract.
func TestAudiencesServeEveryMemberOnceInPagesAndEndWithTheEmptyCursor(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		total int
		page  int
		pages int
	}{
		{"fewer than a page", 2, 3, 1},
		{"exactly one page", 3, 3, 1},
		{"a page and one more", 4, 3, 2},
		{"several pages", 10, 3, 4},
		{"no members", 0, 3, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			a := NewAudiences(tc.page)
			want := members(tc.total)
			a.Set(audRealm, audTenant, "everyone", want...)

			var got []domain.Recipient
			cursor, pages := "", 0
			for {
				page, next, err := a.Resolve(t.Context(), audRealm, audTenant, "everyone", cursor)
				if err != nil {
					t.Fatalf("Resolve(%q) = %v", cursor, err)
				}
				got = append(got, page...)
				pages++
				if next == "" {
					break
				}
				cursor = next
				if pages > tc.total+1 {
					t.Fatal("the cursor never ended")
				}
			}
			if !slices.Equal(got, want) {
				t.Errorf("served %v, want %v", got, want)
			}
			if pages != tc.pages {
				t.Errorf("%d pages, want %d", pages, tc.pages)
			}
		})
	}
}

// An audience belongs to one realm and tenant; another tenant naming the same selector
// gets nothing of it. A host's selector means different people in different tenants.
func TestAnAudienceIsScopedToItsRealmAndTenant(t *testing.T) {
	t.Parallel()
	a := NewAudiences(10)
	a.Set(audRealm, audTenant, "everyone", members(2)...)

	for name, ask := range map[string]func() error{
		"another tenant": func() error {
			_, _, err := a.Resolve(t.Context(), audRealm, domain.Tenant{Kind: "workspace", ID: "w2"}, "everyone", "")
			return err
		},
		"another realm": func() error {
			_, _, err := a.Resolve(t.Context(), "other", audTenant, "everyone", "")
			return err
		},
		"an unknown selector": func() error {
			_, _, err := a.Resolve(t.Context(), audRealm, audTenant, "admins", "")
			return err
		},
	} {
		if err := ask(); err == nil {
			t.Errorf("%s: Resolve succeeded, want an error", name)
		}
	}
}

func TestAudiencesRefuseACursorTheyDidNotIssue(t *testing.T) {
	t.Parallel()
	a := NewAudiences(2)
	a.Set(audRealm, audTenant, "everyone", members(3)...)
	for _, cursor := range []string{"x", "-1", "4"} {
		if _, _, err := a.Resolve(t.Context(), audRealm, audTenant, "everyone", cursor); err == nil {
			t.Errorf("Resolve(%q) succeeded, want an error", cursor)
		}
	}
}

// The calls are the evidence that a worker resumed at a cursor, so they are kept in the
// order they were made, with everything the question named.
func TestAudiencesRememberEveryQuestionInOrder(t *testing.T) {
	t.Parallel()
	a := NewAudiences(2)
	a.Set(audRealm, audTenant, "everyone", members(3)...)
	_, next, _ := a.Resolve(t.Context(), audRealm, audTenant, "everyone", "")
	_, _, _ = a.Resolve(t.Context(), audRealm, audTenant, "everyone", next)

	want := []AudienceCall{
		{Realm: audRealm, Tenant: audTenant, Audience: "everyone", Cursor: ""},
		{Realm: audRealm, Tenant: audTenant, Audience: "everyone", Cursor: "2"},
	}
	if got := a.Calls(); !slices.Equal(got, want) {
		t.Fatalf("Calls = %+v, want %+v", got, want)
	}
}

func TestAudiencesFailOnRequestOnceEach(t *testing.T) {
	t.Parallel()
	a := NewAudiences(2)
	a.Set(audRealm, audTenant, "everyone", members(1)...)
	boom := errors.New("the directory timed out")
	a.FailNext(boom)

	if _, _, err := a.Resolve(t.Context(), audRealm, audTenant, "everyone", ""); !errors.Is(err, boom) {
		t.Fatalf("Resolve = %v, want the failure that was armed", err)
	}
	if _, _, err := a.Resolve(t.Context(), audRealm, audTenant, "everyone", ""); err != nil {
		t.Fatalf("the second Resolve = %v, want it to work: a failure is armed once", err)
	}
}

// The resolver and its caller share slices. Neither may change what the other sees.
func TestAudiencesDoNotShareSlicesWithCallers(t *testing.T) {
	t.Parallel()
	a := NewAudiences(5)
	in := members(2)
	a.Set(audRealm, audTenant, "everyone", in...)
	in[0] = domain.Recipient{Kind: "user", ID: "tampered-after-set"}

	page, _, _ := a.Resolve(t.Context(), audRealm, audTenant, "everyone", "")
	if page[0].ID != "u1" {
		t.Fatalf("served %v, want the members as they were set", page)
	}
	page[1] = domain.Recipient{Kind: "user", ID: "tampered-after-resolve"}
	again, _, _ := a.Resolve(t.Context(), audRealm, audTenant, "everyone", "")
	if again[1].ID != "u2" {
		t.Fatalf("served %v, want a page the caller cannot change", again)
	}
}

func TestNewAudiencesRefusesAPageSizeNoResolverCouldServe(t *testing.T) {
	t.Parallel()
	defer func() {
		if recover() == nil {
			t.Fatal("NewAudiences(0) did not panic")
		}
	}()
	NewAudiences(0)
}
