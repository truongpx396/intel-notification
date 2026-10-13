package inapp_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/truongpx396/intel-notification/adapters/driven/channel/inapp"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/notifytest"
)

var user = domain.Identity{
	Realm:     "aisat",
	Tenant:    domain.Tenant{Kind: "workspace", ID: "w1"},
	Recipient: domain.Recipient{Kind: "user", ID: "u1"},
}

// delivery is an in-app delivery: no address, and the copy a nudge must not carry.
func delivery() domain.Delivery {
	return domain.Delivery{
		ID:             "d1",
		IdemKey:        "k1",
		Identity:       user,
		NotificationID: "n1",
		Topic:          "ingestion_complete",
		Notification: domain.Notification{
			Title: "Upload finished", Body: "Your upload finished.",
			Data: map[string]any{"files": 3}, Payload: map[string]string{"href": "/uploads/9"},
		},
		Channel: inapp.Kind,
		Content: domain.RenderedContent{Subject: "Upload finished", Body: "Your upload finished.", Data: map[string]any{"unread": 7}},
		Attempt: 1,
	}
}

func newChannel(t *testing.T) (*inapp.Channel, *notifytest.Stream, *notifytest.Probe) {
	t.Helper()
	probe := notifytest.NewProbe()
	stream := notifytest.NewStream(probe)
	return inapp.New(stream), stream, probe
}

// The channel is held to the contract that every channel is (NR-005, D32). Its
// effect is a nudge for a notification, so that is what the provider side counts.
func TestInAppRunsTheChannelContract(t *testing.T) {
	t.Parallel()
	notifytest.ChannelContract(t, func(t *testing.T) notifytest.ChannelUnderTest {
		t.Helper()
		ch, _, probe := newChannel(t)
		return notifytest.ChannelUnderTest{
			Channel: ch,
			Probe:   probe,
			Key:     func(d domain.Delivery) string { return d.NotificationID },
		}
	})
}

// What the engine reads of this channel instead of branching on its kind
// (NR-016): it is addressed by the identity alone, makes a notification visible in
// the inbox, and is idempotent by construction (D26, D32).
func TestInAppDeclaresWhatItIs(t *testing.T) {
	t.Parallel()
	ch, _, _ := newChannel(t)
	if got := ch.Kind(); got != "in_app" {
		t.Fatalf("Kind = %q, want in_app", got)
	}
	want := domain.ChannelCapabilities{
		NeedsAddress: false, NeedsSubject: false, RichContent: true, InboxBacked: true,
		Dedup: domain.DedupLocal, DedupWindow: 0,
	}
	if got := ch.Capabilities(); got != want {
		t.Fatalf("Capabilities = %+v, want %+v", got, want)
	}
}

// The nudge is the notification id and nothing else: no title, no body, no count
// (NR-024, D16). The relay loads the item under the session's identity, so what a
// nudge could say is only ever a pointer.
func TestInAppPublishesAnIdAndNothingElse(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		mutate func(*domain.Delivery)
	}{
		{"a delivery with its copy and data", func(*domain.Delivery) {}},
		{"a delivery whose content names a count", func(d *domain.Delivery) {
			d.Content.Subject, d.Content.Body = "You have 7 unread", "7"
		}},
		{"a delivery with a long title", func(d *domain.Delivery) { d.Notification.Title = strings.Repeat("t", 10_000) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ch, stream, _ := newChannel(t)
			d := delivery()
			tc.mutate(&d)
			r, err := ch.Deliver(t.Context(), d)
			if err != nil || r.Outcome != domain.Delivered {
				t.Fatalf("Deliver = %+v, %v, want Delivered", r, err)
			}
			want := []notifytest.Nudge{{Identity: user, NotificationID: "n1"}}
			got := stream.Nudges()
			if len(got) != 1 || got[0] != want[0] {
				t.Fatalf("published %+v, want exactly the id n1 for the delivery's identity", got)
			}
		})
	}
}

// A nudge goes to the identity the delivery is for, in full: the same recipient id
// in another tenant, realm or kind is another stream (D16, NS-001).
func TestInAppPublishesToTheDeliverysIdentityAndNoOther(t *testing.T) {
	t.Parallel()
	variants := map[string]func(*domain.Identity){
		"tenant id":      func(i *domain.Identity) { i.Tenant.ID = "w2" },
		"tenant kind":    func(i *domain.Identity) { i.Tenant.Kind = "organization" },
		"recipient id":   func(i *domain.Identity) { i.Recipient.ID = "u2" },
		"recipient kind": func(i *domain.Identity) { i.Recipient.Kind = "device" },
		"realm":          func(i *domain.Identity) { i.Realm = "other" },
	}
	for name, vary := range variants {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ch, stream, _ := newChannel(t)
			d := delivery()
			vary(&d.Identity)
			if _, err := ch.Deliver(t.Context(), d); err != nil {
				t.Fatalf("Deliver: %v", err)
			}
			got := stream.Nudges()
			if len(got) != 1 || got[0].Identity != d.Identity || got[0].Identity == user {
				t.Fatalf("published for %+v, want the delivery's own identity %+v and not %+v", got, d.Identity, user)
			}
		})
	}
}

// A re-drive repeats the nudge for the same row, which shows one item: that is
// what DedupLocal means for this channel, and why it needs no record of its own.
func TestInAppReDriveRepeatsTheSameNudge(t *testing.T) {
	t.Parallel()
	ch, stream, probe := newChannel(t)
	for range 2 {
		if r, err := ch.Deliver(t.Context(), delivery()); err != nil || r.Outcome != domain.Delivered {
			t.Fatalf("Deliver = %+v, %v, want Delivered", r, err)
		}
	}
	got := stream.Nudges()
	if len(got) != 2 || got[0] != got[1] {
		t.Fatalf("a re-drive published %+v, want the same nudge twice", got)
	}
	if probe.Sends("n1") != 1 {
		t.Fatalf("the relay would show %d items for n1, want 1", probe.Sends("n1"))
	}
}

// A digest names no notification: its members carry no ids, and their inbox rows
// are already visible. There is nothing to point a nudge at, and that is not a
// failure, or every digested in-app delivery would be dead-lettered.
func TestInAppDigestDeliveryPublishesNothingAndSucceeds(t *testing.T) {
	t.Parallel()
	ch, stream, _ := newChannel(t)
	d := delivery()
	d.NotificationID, d.DigestID = "", "win-1"
	d.Digest = []domain.Notification{{Title: "a"}, {Title: "b"}}
	r, err := ch.Deliver(t.Context(), d)
	if err != nil || r.Outcome != domain.Delivered {
		t.Fatalf("Deliver of a digest = %+v, %v, want Delivered", r, err)
	}
	if got := stream.Nudges(); len(got) != 0 {
		t.Fatalf("a digest published %+v, want nothing: it names no notification", got)
	}
}

// A delivery that names neither a notification nor a digest is malformed for good.
func TestInAppRejectsADeliveryThatNamesNothing(t *testing.T) {
	t.Parallel()
	ch, stream, _ := newChannel(t)
	d := delivery()
	d.NotificationID = ""
	r, err := ch.Deliver(t.Context(), d)
	if err != nil || r.Outcome != domain.Rejected || r.Detail == "" {
		t.Fatalf("Deliver = %+v, %v, want Rejected with a detail", r, err)
	}
	if got := stream.Nudges(); len(got) != 0 {
		t.Fatalf("published %+v for a delivery that names nothing", got)
	}
}

// What the stream says about its own failure decides the outcome: a refusal no
// retry fixes is Rejected so it is dead-lettered at once with its reason, and any
// other fault is Retry for the dispatcher's backoff (D10, D12).
func TestInAppReadsTheStreamsFault(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		arm  notifytest.Failure
		want domain.Outcome
	}{
		{"a refusal no retry fixes", notifytest.Permanent, domain.Rejected},
		{"a transient fault", notifytest.Transient, domain.Retry},
		{"an infrastructure fault", notifytest.Infrastructure, domain.Retry},
		{"a dead address, which a stream cannot report", notifytest.DeadAddress, domain.Retry},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ch, _, probe := newChannel(t)
			probe.FailNext(tc.arm)
			r, err := ch.Deliver(t.Context(), delivery())
			if err != nil || r.Outcome != tc.want {
				t.Fatalf("Deliver = %+v, %v, want %v and no error", r, err, tc.want)
			}
			if r.Detail == "" {
				t.Fatal("no Detail: the dead letter keeps the last error, and the dispatcher logs it")
			}
		})
	}
}

// A channel with no stream is a wiring mistake, found when the host starts and not
// on the first delivery.
func TestNewRefusesNoStream(t *testing.T) {
	t.Parallel()
	defer func() {
		p := recover()
		if p == nil {
			t.Fatal("New(nil) did not panic")
		}
		if msg := fmt.Sprint(p); !strings.Contains(msg, "StreamPublisher") {
			t.Fatalf("New(nil) panicked with %q, want it to name the missing port", msg)
		}
	}()
	inapp.New(nil)
}
