package notifytest

import (
	"errors"
	"slices"
	"sync"
	"testing"

	"github.com/truongpx396/intel-notification/domain"
)

func send(key, address string) domain.Delivery {
	return domain.Delivery{
		ID:      "d-" + key,
		IdemKey: key,
		Channel: "email",
		Address: domain.Address{Channel: "email", Value: address},
		Content: domain.RenderedContent{Subject: "Done", Body: "Your upload finished."},
	}
}

// What a provider sees of a re-driven delivery depends on the dedup level the
// channel declares (D32): a channel at DedupNone may send twice, and one that
// declares more must not. The probe counts both the calls and the sends.
func TestChannelHonoursItsDedupLevel(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		dedup     domain.DedupLevel
		wantSends int
	}{
		{"none: every call is a send", domain.DedupNone, 2},
		{"local: a re-drive collapses", domain.DedupLocal, 1},
		{"provider: a re-drive collapses", domain.DedupProvider, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			probe := NewProbe()
			ch := NewChannel("email", domain.ChannelCapabilities{Dedup: tc.dedup}, probe)
			for i := range 2 {
				r, err := ch.Deliver(t.Context(), send("k1", "a@example.com"))
				if err != nil || r.Outcome != domain.Delivered {
					t.Fatalf("call %d = %+v, %v, want Delivered: a collapsed re-drive still succeeds", i+1, r, err)
				}
			}
			if got := probe.Sends("k1"); got != tc.wantSends {
				t.Fatalf("Sends(k1) = %d, want %d", got, tc.wantSends)
			}
			if got := probe.Calls("k1"); got != 2 {
				t.Fatalf("Calls(k1) = %d, want 2: the probe counts the calls a provider collapsed", got)
			}
		})
	}
}

// The delivery key is per address (D25), so two addresses of one recipient are
// two sends even at the strongest dedup level.
func TestChannelCountsDistinctKeysSeparately(t *testing.T) {
	t.Parallel()
	probe := NewProbe()
	ch := NewChannel("email", domain.ChannelCapabilities{Dedup: domain.DedupProvider}, probe)
	for _, d := range []domain.Delivery{send("k1", "a@example.com"), send("k2", "b@example.com"), send("k1", "a@example.com")} {
		if _, err := ch.Deliver(t.Context(), d); err != nil {
			t.Fatal(err)
		}
	}
	if probe.Sends("k1") != 1 || probe.Sends("k2") != 1 {
		t.Fatalf("Sends(k1) = %d, Sends(k2) = %d, want 1 and 1", probe.Sends("k1"), probe.Sends("k2"))
	}
	if got := probe.Total(); got != 2 {
		t.Fatalf("Total() = %d, want 2", got)
	}
	if got := probe.Sends("never-sent"); got != 0 {
		t.Fatalf("Sends of an unseen key = %d, want 0", got)
	}
}

func TestProbeKeysAreSorted(t *testing.T) {
	t.Parallel()
	probe := NewProbe()
	ch := NewChannel("email", domain.ChannelCapabilities{}, probe)
	for _, k := range []string{"k3", "k1", "k2", "k1"} {
		if _, err := ch.Deliver(t.Context(), send(k, "a@example.com")); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := probe.Keys(), []string{"k1", "k2", "k3"}; !slices.Equal(got, want) {
		t.Fatalf("Keys() = %v, want %v", got, want)
	}
}

// Each failure becomes the outcome the contract prescribes for it, never a drop;
// and a send that failed never reached the provider, so it is a call and not a
// send.
func TestChannelTurnsAFailureIntoItsOutcome(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		failure Failure
		want    domain.Outcome
		reason  domain.SuppressionReason
		wantErr error
	}{
		{"transient is Retry", Transient, domain.Retry, "", nil},
		{"a dead address is Suppressed, with a reason", DeadAddress, domain.Suppressed, domain.SuppressHardBounce, nil},
		{"a permanent refusal is Rejected", Permanent, domain.Rejected, "", nil},
		{"an infrastructure fault is an error", Infrastructure, 0, "", ErrInfrastructure},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			probe := NewProbe()
			ch := NewChannel("email", domain.ChannelCapabilities{Dedup: domain.DedupProvider}, probe)
			probe.FailNext(tc.failure)
			r, err := ch.Deliver(t.Context(), send("k1", "a@example.com"))
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			if r.Outcome != tc.want || r.SuppressReason != tc.reason {
				t.Fatalf("result = %+v, want outcome %d with reason %q", r, tc.want, tc.reason)
			}
			if probe.Sends("k1") != 0 || probe.Calls("k1") != 1 {
				t.Fatalf("Sends = %d, Calls = %d, want 0 and 1: a failed send did not reach the provider",
					probe.Sends("k1"), probe.Calls("k1"))
			}
		})
	}
}

// A failure is armed for one send. They queue in the order armed, and a send after
// the last is clean.
func TestProbeFailuresAreConsumedOnceInOrder(t *testing.T) {
	t.Parallel()
	probe := NewProbe()
	ch := NewChannel("email", domain.ChannelCapabilities{}, probe)
	probe.FailNext(Transient)
	probe.FailNext(Permanent)

	want := []domain.Outcome{domain.Retry, domain.Rejected, domain.Delivered, domain.Delivered}
	for i, w := range want {
		r, err := ch.Deliver(t.Context(), send("k1", "a@example.com"))
		if err != nil || r.Outcome != w {
			t.Fatalf("send %d = %+v, %v, want outcome %d", i+1, r, err, w)
		}
	}
}

// A delivered send carries the id a provider would, so a dispatcher suite can
// check it is recorded, and so a callback could be correlated to it (D20).
func TestChannelDeliveredCarriesAStableProviderMessageID(t *testing.T) {
	t.Parallel()
	ch := NewChannel("email", domain.ChannelCapabilities{Dedup: domain.DedupProvider}, NewProbe())
	a1, _ := ch.Deliver(t.Context(), send("k1", "a@example.com"))
	a2, _ := ch.Deliver(t.Context(), send("k1", "a@example.com"))
	b, _ := ch.Deliver(t.Context(), send("k2", "b@example.com"))
	if a1.ProviderMessageID == "" {
		t.Fatal("a delivered send has no provider message id")
	}
	if a1.ProviderMessageID != a2.ProviderMessageID {
		t.Fatalf("a re-drive got %q, want the first send's %q", a2.ProviderMessageID, a1.ProviderMessageID)
	}
	if a1.ProviderMessageID == b.ProviderMessageID {
		t.Fatalf("two keys share the provider message id %q", b.ProviderMessageID)
	}
}

func TestChannelReportsItsKindAndCapabilities(t *testing.T) {
	t.Parallel()
	caps := domain.ChannelCapabilities{NeedsSubject: true, NeedsAddress: true, InboxBacked: false,
		Dedup: domain.DedupProvider}
	ch := NewChannel("sms", caps, NewProbe())
	if ch.Kind() != "sms" {
		t.Fatalf("Kind() = %q, want sms", ch.Kind())
	}
	if ch.Capabilities() != caps {
		t.Fatalf("Capabilities() = %+v, want %+v", ch.Capabilities(), caps)
	}
}

// Workers send concurrently. Under -race this fails on an unsynchronized probe,
// and the counts show a provider that dedupes under contention sends once.
func TestChannelIsSafeForConcurrentUse(t *testing.T) {
	t.Parallel()
	const workers = 50
	for _, tc := range []struct {
		dedup domain.DedupLevel
		sends int
	}{{domain.DedupNone, workers}, {domain.DedupProvider, 1}} {
		probe := NewProbe()
		ch := NewChannel("email", domain.ChannelCapabilities{Dedup: tc.dedup}, probe)
		var wg sync.WaitGroup
		for range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, _ = ch.Deliver(t.Context(), send("k1", "a@example.com"))
			}()
		}
		wg.Wait()
		if probe.Sends("k1") != tc.sends || probe.Calls("k1") != workers {
			t.Fatalf("dedup %d: Sends = %d, Calls = %d, want %d and %d",
				tc.dedup, probe.Sends("k1"), probe.Calls("k1"), tc.sends, workers)
		}
	}
}

// Arming a failure that does not exist would leave a test passing without
// failing anything, so it is a programmer error.
func TestProbeRefusesAFailureThatDoesNotExist(t *testing.T) {
	t.Parallel()
	for _, f := range []Failure{0, Infrastructure + 1, -1} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("FailNext(%d) did not panic", int(f))
				}
			}()
			NewProbe().FailNext(f)
		}()
	}
}
