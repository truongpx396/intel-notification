package notifytest

import (
	"slices"
	"testing"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

func chanOf(kind domain.ChannelKind) ports.Channel {
	return NewChannel(kind, domain.ChannelCapabilities{}, NewProbe())
}

func TestRegistry(t *testing.T) {
	t.Parallel()
	email, sms, inapp := chanOf("email"), chanOf("sms"), chanOf("in_app")
	r := NewRegistry(email)
	r.Register(sms)
	r.Register(inapp)

	if got, ok := r.Get("sms"); !ok || got != sms {
		t.Fatalf("Get(sms) = %v, %v, want the registered channel", got, ok)
	}
	if got, ok := r.Get("email"); !ok || got != email {
		t.Fatalf("Get(email) = %v, %v, want the channel passed to NewRegistry", got, ok)
	}
	if got, ok := r.Get("push"); ok || got != nil {
		t.Fatalf("Get(push) = %v, %v, want nothing for an unregistered kind", got, ok)
	}
	if got, want := r.Kinds(), []domain.ChannelKind{"email", "in_app", "sms"}; !slices.Equal(got, want) {
		t.Fatalf("Kinds() = %v, want %v, sorted", got, want)
	}
	if got := NewRegistry().Kinds(); len(got) != 0 {
		t.Fatalf("an empty registry lists %v", got)
	}
}

// Registration is wiring. Two channels of one kind would make one of them
// unreachable, and a channel with no kind could never be asked for, so both fail
// loudly at startup rather than quietly at the first delivery.
func TestRegistryRefusesWhatCouldNeverWork(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		fn   func()
	}{
		{"the same kind twice", func() { NewRegistry(chanOf("email"), chanOf("email")) }},
		{"a channel with no kind", func() { NewRegistry(chanOf("")) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			defer func() {
				if recover() == nil {
					t.Fatal("Register did not panic")
				}
			}()
			tc.fn()
		})
	}
}
