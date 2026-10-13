package failover_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/truongpx396/intel-notification/adapters/driven/channel/failover"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/notifytest"
	"github.com/truongpx396/intel-notification/ports"
)

// member is a provider whose answers a test scripts, and which remembers what it
// was asked and in what order across the whole chain.
type member struct {
	name  string
	caps  domain.ChannelCapabilities
	norm  func(string) string
	log   *callLog
	reply func(n int) (domain.DeliveryResult, error) // n counts this member's calls from 1

	mu    sync.Mutex
	calls []domain.Delivery
}

type callLog struct {
	mu    sync.Mutex
	order []string
}

func (l *callLog) add(name string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.order = append(l.order, name)
}

func (l *callLog) got() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.order...)
}

func (m *member) Kind() domain.ChannelKind                 { return "email" }
func (m *member) Capabilities() domain.ChannelCapabilities { return m.caps }

func (m *member) Deliver(_ context.Context, d domain.Delivery) (domain.DeliveryResult, error) {
	m.mu.Lock()
	m.calls = append(m.calls, d)
	n := len(m.calls)
	m.mu.Unlock()
	m.log.add(m.name)
	return m.reply(n)
}

func (m *member) asked() []domain.Delivery {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]domain.Delivery(nil), m.calls...)
}

// normalizing is a member that defines address equality.
type normalizing struct{ *member }

func (n normalizing) NormalizeAddress(v string) string { return n.norm(v) }

func answers(r domain.DeliveryResult) func(int) (domain.DeliveryResult, error) {
	return func(int) (domain.DeliveryResult, error) { return r, nil }
}

func newMember(name string, log *callLog, r domain.DeliveryResult) *member {
	return &member{name: name, log: log, caps: domain.ChannelCapabilities{NeedsAddress: true}, reply: answers(r)}
}

var (
	delivered = domain.DeliveryResult{Outcome: domain.Delivered}
	retry     = domain.DeliveryResult{Outcome: domain.Retry, Detail: "busy"}
)

func theDelivery() domain.Delivery {
	return domain.Delivery{
		ID: "d1", IdemKey: "k1", NotificationID: "n1", Channel: "email", Attempt: 2, Topic: "invoice",
		Identity: domain.Identity{Realm: "aisat", Tenant: domain.Tenant{Kind: "workspace", ID: "w1"}, Recipient: domain.Recipient{Kind: "user", ID: "u1"}},
		Address:  domain.Address{Channel: "email", Value: "ada@example.com"},
		Content:  domain.RenderedContent{Subject: "S", Body: "B"},
	}
}

// The composite promises no more than its weakest member (D29, D32). A re-drive
// after a crash may reach any provider, and the guarantee that holds is the one
// every provider gives, so it is the weakest level, over every pairing, and a
// window only where the weakest is a provider's, and then the shortest.
func TestCompositeDeclaresTheWeakestDedupOfItsMembers(t *testing.T) {
	t.Parallel()
	levels := []domain.DedupLevel{domain.DedupNone, domain.DedupLocal, domain.DedupProvider}
	for _, a := range levels {
		for _, b := range levels {
			t.Run(fmt.Sprintf("%d with %d", a, b), func(t *testing.T) {
				t.Parallel()
				want := min(a, b)
				log := &callLog{}
				ma, mb := newMember("a", log, delivered), newMember("b", log, delivered)
				ma.caps.Dedup, mb.caps.Dedup = a, b
				if a == domain.DedupProvider {
					ma.caps.DedupWindow = 24 * time.Hour
				}
				if b == domain.DedupProvider {
					mb.caps.DedupWindow = 6 * time.Hour
				}
				got := failover.New(ma, mb).Capabilities()
				if got.Dedup != want {
					t.Fatalf("members %d and %d: composite declares %d, want the weakest, %d", a, b, got.Dedup, want)
				}
				wantWindow := time.Duration(0)
				switch {
				case want != domain.DedupProvider:
				case a == domain.DedupProvider && b == domain.DedupProvider:
					wantWindow = 6 * time.Hour
				}
				if got.DedupWindow != wantWindow {
					t.Fatalf("members %d and %d: window = %v, want %v", a, b, got.DedupWindow, wantWindow)
				}
			})
		}
	}
	t.Run("a window means something only for a provider's dedup, whatever a member declares", func(t *testing.T) {
		t.Parallel()
		log := &callLog{}
		a, b := newMember("a", log, delivered), newMember("b", log, delivered)
		a.caps.Dedup, a.caps.DedupWindow = domain.DedupLocal, 24*time.Hour
		b.caps.Dedup, b.caps.DedupWindow = domain.DedupLocal, 24*time.Hour
		if got := failover.New(a, b).Capabilities(); got.Dedup != domain.DedupLocal || got.DedupWindow != 0 {
			t.Fatalf("Capabilities = %+v, want DedupLocal and no window", got)
		}
	})
	t.Run("three members: the weakest of any", func(t *testing.T) {
		t.Parallel()
		log := &callLog{}
		ms := []ports.Channel{newMember("a", log, delivered), newMember("b", log, delivered), newMember("c", log, delivered)}
		for i, level := range []domain.DedupLevel{domain.DedupProvider, domain.DedupProvider, domain.DedupLocal} {
			ms[i].(*member).caps.Dedup = level
			if level == domain.DedupProvider {
				ms[i].(*member).caps.DedupWindow = time.Hour
			}
		}
		if got := failover.New(ms...).Capabilities(); got.Dedup != domain.DedupLocal || got.DedupWindow != 0 {
			t.Fatalf("Capabilities = %+v, want DedupLocal and no window", got)
		}
	})
}

// What the engine reads of the kind has to be what any member needs, so the
// renderer produces it, and what every member provides, so a notification is not
// shown as inbox-visible on the strength of one.
func TestCompositeCombinesTheOtherCapabilities(t *testing.T) {
	t.Parallel()
	log := &callLog{}
	a, b := newMember("a", log, delivered), newMember("b", log, delivered)
	a.caps = domain.ChannelCapabilities{NeedsSubject: true, NeedsAddress: true, RichContent: false, InboxBacked: true}
	b.caps = domain.ChannelCapabilities{NeedsSubject: false, NeedsAddress: true, RichContent: true, InboxBacked: false}
	got := failover.New(a, b).Capabilities()
	want := domain.ChannelCapabilities{NeedsSubject: true, NeedsAddress: true, RichContent: true, InboxBacked: false}
	if got != want {
		t.Fatalf("Capabilities = %+v, want %+v", got, want)
	}
	// And the other way about: the address is needed if either member needs it.
	a.caps.NeedsAddress, b.caps.NeedsAddress = false, true
	if got := failover.New(a, b).Capabilities(); !got.NeedsAddress {
		t.Fatalf("Capabilities = %+v, want NeedsAddress: one member needs the address the renderer and the address book supply", got)
	}
}

// The chain: a transient failure moves on inside the same attempt, a delivery or a
// verdict about the address or the message ends it. A provider that fails with an
// error is read as the transient kind, as the port says.
func TestCompositeWalksTheChain(t *testing.T) {
	t.Parallel()
	suppressed := domain.DeliveryResult{Outcome: domain.Suppressed, SuppressReason: domain.SuppressHardBounce, Detail: "no such user"}
	rejected := domain.DeliveryResult{Outcome: domain.Rejected, Detail: "malformed"}
	cases := []struct {
		name       string
		replies    []func(int) (domain.DeliveryResult, error)
		wantCalls  []string
		wantResult domain.Outcome
		wantID     string
		why        string
	}{
		{"the first delivers", []func(int) (domain.DeliveryResult, error){answers(domain.DeliveryResult{Outcome: domain.Delivered, ProviderMessageID: "p1-msg"}), answers(delivered)},
			[]string{"p1"}, domain.Delivered, "p1-msg", "a delivery stops the chain: trying the next would send twice"},
		{"the first fails, the second delivers", []func(int) (domain.DeliveryResult, error){answers(retry), answers(domain.DeliveryResult{Outcome: domain.Delivered, ProviderMessageID: "p2-msg"})},
			[]string{"p1", "p2"}, domain.Delivered, "p2-msg", "the point of the chain, and the id is the provider that sent's, so a bounce callback finds it (D20)"},
		{"the first errors, the second delivers", []func(int) (domain.DeliveryResult, error){
			func(int) (domain.DeliveryResult, error) {
				return domain.DeliveryResult{}, errors.New("connection reset")
			}, answers(delivered)},
			[]string{"p1", "p2"}, domain.Delivered, "", "an error is an infrastructure fault, which is transient"},
		{"the first fails, the second fails, the third delivers", []func(int) (domain.DeliveryResult, error){answers(retry), answers(retry), answers(delivered)},
			[]string{"p1", "p2", "p3"}, domain.Delivered, "", ""},
		{"the address is dead for the first", []func(int) (domain.DeliveryResult, error){answers(suppressed), answers(delivered)},
			[]string{"p1"}, domain.Suppressed, "", "no other provider can reach a mailbox that does not exist"},
		{"the first refuses the message", []func(int) (domain.DeliveryResult, error){answers(rejected), answers(delivered)},
			[]string{"p1"}, domain.Rejected, "", "a permanent refusal is a verdict on the message, and ends the delivery"},
		{"the first returns an outcome that does not exist", []func(int) (domain.DeliveryResult, error){answers(domain.DeliveryResult{}), answers(delivered)},
			[]string{"p1", "p2"}, domain.Delivered, "", "a buggy provider cannot end a delivery by accident"},
		{"every provider fails", []func(int) (domain.DeliveryResult, error){answers(retry), answers(retry)},
			[]string{"p1", "p2"}, domain.Retry, "", "the dispatcher backs off and the next attempt starts again from the first"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			log := &callLog{}
			var chain []ports.Channel
			for i, reply := range tc.replies {
				m := newMember(fmt.Sprintf("p%d", i+1), log, domain.DeliveryResult{})
				m.reply = reply
				chain = append(chain, m)
			}
			r, err := failover.New(chain...).Deliver(t.Context(), theDelivery())
			if err != nil {
				t.Fatalf("Deliver returned the error %v, want an outcome", err)
			}
			if r.Outcome != tc.wantResult || r.ProviderMessageID != tc.wantID {
				t.Fatalf("Deliver = %+v, want %v with id %q: %s", r, tc.wantResult, tc.wantID, tc.why)
			}
			if got := log.got(); strings.Join(got, ",") != strings.Join(tc.wantCalls, ",") {
				t.Fatalf("providers called %v, want %v: %s", got, tc.wantCalls, tc.why)
			}
		})
	}
}

// Every provider is handed the same delivery, key included: the key is stable
// across providers as it is across re-drives (D25), and a provider that honours it
// must see the one the others saw.
func TestCompositeHandsEveryProviderTheSameDelivery(t *testing.T) {
	t.Parallel()
	log := &callLog{}
	a, b := newMember("a", log, retry), newMember("b", log, delivered)
	d := theDelivery()
	if _, err := failover.New(a, b).Deliver(t.Context(), d); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	for _, m := range []*member{a, b} {
		got := m.asked()
		if len(got) != 1 || got[0].IdemKey != d.IdemKey || got[0].Address.Value != d.Address.Value || got[0].Identity != d.Identity ||
			got[0].Attempt != d.Attempt || got[0].Content.Subject != d.Content.Subject {
			t.Fatalf("provider %s was handed %+v, want the delivery unchanged", m.name, got)
		}
	}
}

// Every attempt starts at the first provider. The composite keeps no memory of
// which one answered, so the order is the operator's configured preference at every
// attempt, and nothing about a delivery lives in the channel.
func TestCompositeStartsEveryAttemptAtTheFirstProvider(t *testing.T) {
	t.Parallel()
	log := &callLog{}
	a, b := newMember("a", log, retry), newMember("b", log, retry)
	c := failover.New(a, b)
	for range 2 {
		if _, err := c.Deliver(t.Context(), theDelivery()); err != nil {
			t.Fatalf("Deliver: %v", err)
		}
	}
	if got := strings.Join(log.got(), ","); got != "a,b,a,b" {
		t.Fatalf("providers called %s, want a,b,a,b", got)
	}
}

// When no provider can send, the dispatcher is told when it might be worth trying
// again. A hint is a floor on the backoff, and one provider's hint does not bind a
// delivery another may carry, so the floor is the earliest any provider offered:
// none at all if any gave none.
func TestCompositeReportsWhenToTryAgain(t *testing.T) {
	t.Parallel()
	hint := func(d time.Duration, detail string) domain.DeliveryResult {
		return domain.DeliveryResult{Outcome: domain.Retry, RetryAfter: d, Detail: detail}
	}
	cases := []struct {
		name    string
		replies []func(int) (domain.DeliveryResult, error)
		want    time.Duration
	}{
		{"every provider gives a hint: the shortest", []func(int) (domain.DeliveryResult, error){answers(hint(10*time.Minute, "a")), answers(hint(2*time.Minute, "b"))}, 2 * time.Minute},
		{"the order of the hints does not matter", []func(int) (domain.DeliveryResult, error){answers(hint(2*time.Minute, "a")), answers(hint(10*time.Minute, "b"))}, 2 * time.Minute},
		{"one gives none", []func(int) (domain.DeliveryResult, error){answers(hint(10*time.Minute, "a")), answers(retry)}, 0},
		{"one is an error", []func(int) (domain.DeliveryResult, error){answers(hint(10*time.Minute, "a")),
			func(int) (domain.DeliveryResult, error) { return domain.DeliveryResult{}, errors.New("reset") }}, 0},
		{"one provider with a hint", []func(int) (domain.DeliveryResult, error){answers(hint(90*time.Second, "a"))}, 90 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			log := &callLog{}
			var chain []ports.Channel
			for i, reply := range tc.replies {
				m := newMember(fmt.Sprintf("p%d", i+1), log, domain.DeliveryResult{})
				m.reply = reply
				chain = append(chain, m)
			}
			r, err := failover.New(chain...).Deliver(t.Context(), theDelivery())
			if err != nil || r.Outcome != domain.Retry || r.RetryAfter != tc.want {
				t.Fatalf("Deliver = %+v, %v, want Retry after %v", r, err, tc.want)
			}
		})
	}
}

// The last error a dead letter keeps has to say what every provider said, or an
// operator learns about one outage of two.
func TestCompositeDetailNamesEveryProvidersAnswer(t *testing.T) {
	t.Parallel()
	log := &callLog{}
	a := newMember("a", log, domain.DeliveryResult{Outcome: domain.Retry, Detail: "HTTP 503 from the first"})
	b := newMember("b", log, domain.DeliveryResult{})
	b.reply = func(int) (domain.DeliveryResult, error) {
		return domain.DeliveryResult{}, errors.New("connection refused by the second")
	}
	r, _ := failover.New(a, b).Deliver(t.Context(), theDelivery())
	for _, want := range []string{"HTTP 503 from the first", "connection refused by the second"} {
		if !strings.Contains(r.Detail, want) {
			t.Fatalf("Detail = %q, want it to carry %q", r.Detail, want)
		}
	}
}

// A worker that is stopping does not go on to try the rest of the chain.
func TestCompositeStopsWhenTheContextEnds(t *testing.T) {
	t.Parallel()
	log := &callLog{}
	ctx, cancel := context.WithCancel(t.Context())
	a := newMember("a", log, domain.DeliveryResult{})
	a.reply = func(int) (domain.DeliveryResult, error) { cancel(); return retry, nil }
	b := newMember("b", log, delivered)
	_, err := failover.New(a, b).Deliver(ctx, theDelivery())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Deliver = %v, want context.Canceled", err)
	}
	if got := strings.Join(log.got(), ","); got != "a" {
		t.Fatalf("providers called %s, want only a", got)
	}
}

// Address equality is the kind's, so the composite answers with the first member
// that defines it, and leaves the value alone when none does.
func TestCompositeNormalizesAddressesAsItsMembersDo(t *testing.T) {
	t.Parallel()
	log := &callLog{}
	plain := newMember("plain", log, delivered)
	lower := normalizing{&member{name: "lower", log: log, norm: strings.ToLower, reply: answers(delivered)}}
	upper := normalizing{&member{name: "upper", log: log, norm: strings.ToUpper, reply: answers(delivered)}}
	cases := []struct {
		name    string
		members []ports.Channel
		want    string
	}{
		{"no member normalizes", []ports.Channel{plain, plain}, "Ada@Example.COM"},
		{"the first that does", []ports.Channel{plain, lower, upper}, "ada@example.com"},
		{"the first member does", []ports.Channel{upper, lower}, "ADA@EXAMPLE.COM"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := failover.New(tc.members...).NormalizeAddress("Ada@Example.COM"); got != tc.want {
				t.Fatalf("NormalizeAddress = %q, want %q", got, tc.want)
			}
		})
	}
}

// One kind, at least one provider, and no gaps: found when the host starts.
func TestNewRefusesWhatCannotWork(t *testing.T) {
	t.Parallel()
	log := &callLog{}
	sms := &member{name: "sms", log: log, reply: answers(delivered)}
	other := kindOf{sms, "sms"}
	cases := []struct {
		name    string
		members []ports.Channel
		wanted  string
	}{
		{"no providers", nil, "at least one"},
		{"a nil provider", []ports.Channel{newMember("a", log, delivered), nil}, "provider 2 is nil"},
		{"providers of two kinds", []ports.Channel{newMember("a", log, delivered), other}, "kind"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			defer func() {
				p := recover()
				if p == nil {
					t.Fatal("New did not panic")
				}
				if !strings.Contains(fmt.Sprint(p), tc.wanted) {
					t.Fatalf("panic %q does not say %q", p, tc.wanted)
				}
			}()
			failover.New(tc.members...)
		})
	}
}

type kindOf struct {
	*member
	kind domain.ChannelKind
}

func (k kindOf) Kind() domain.ChannelKind { return k.kind }

// The order is fixed when the channel is made: a host that reuses the slice it
// passed cannot reorder, or empty, a chain that is already delivering.
func TestCompositeKeepsTheOrderItWasGiven(t *testing.T) {
	t.Parallel()
	log := &callLog{}
	a, b := newMember("a", log, retry), newMember("b", log, retry)
	members := []ports.Channel{a, b}
	c := failover.New(members...)
	members[0], members[1] = b, a
	if _, err := c.Deliver(t.Context(), theDelivery()); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if got := strings.Join(log.got(), ","); got != "a,b" {
		t.Fatalf("providers called %s after the caller reordered its slice, want a,b", got)
	}
}

// The kind is the providers'.
func TestCompositeIsOfTheMembersKind(t *testing.T) {
	t.Parallel()
	log := &callLog{}
	if got := failover.New(newMember("a", log, delivered), newMember("b", log, delivered)).Kind(); got != "email" {
		t.Fatalf("Kind = %q, want email", got)
	}
}

// The composite is a channel like any other, held to the contract (T029). A failure
// is armed on every member, because one is consumed by the first; the dead address
// and the refusal are verdicts and end the chain at the first provider, and the
// transient one is tried at each, which is why the provider may be called once per
// member and no more.
func TestCompositeRunsTheChannelContract(t *testing.T) {
	t.Parallel()
	for _, level := range []struct {
		name   string
		level  domain.DedupLevel
		window time.Duration
	}{
		{"members that record sends themselves", domain.DedupLocal, 0},
		{"members whose provider dedupes", domain.DedupProvider, 24 * time.Hour},
		{"members that do not dedupe", domain.DedupNone, 0},
	} {
		t.Run(level.name, func(t *testing.T) {
			t.Parallel()
			notifytest.ChannelContract(t, func(t *testing.T) notifytest.ChannelUnderTest {
				t.Helper()
				probe := notifytest.NewProbe()
				caps := domain.ChannelCapabilities{NeedsAddress: true, NeedsSubject: true, Dedup: level.level, DedupWindow: level.window}
				members := []ports.Channel{
					withCaps{notifytest.NewChannel("email", caps, probe), caps},
					withCaps{notifytest.NewChannel("email", caps, probe), caps},
				}
				return notifytest.ChannelUnderTest{
					Channel:       failover.New(members...),
					Probe:         probe,
					ProviderCalls: len(members),
					Fail: func(f notifytest.Failure) {
						for range members {
							probe.FailNext(f)
						}
					},
				}
			})
		})
	}
}

// withCaps gives a kit channel the window it was built to declare; the kit's own
// channel declares a level only.
type withCaps struct {
	ports.Channel
	caps domain.ChannelCapabilities
}

func (w withCaps) Capabilities() domain.ChannelCapabilities { return w.caps }
