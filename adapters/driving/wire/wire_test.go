package wire_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/truongpx396/intel-notification/adapters/driving/wire"
	"github.com/truongpx396/intel-notification/domain"
)

var (
	t0 = time.Date(2026, time.March, 4, 10, 30, 0, 0, time.UTC)

	fullNotification = domain.Notification{
		Tenant:        domain.Tenant{Kind: "workspace", ID: "w1"},
		Recipient:     domain.Recipient{Kind: "user", ID: "u1"},
		Topic:         "ingestion_complete",
		Priority:      domain.PriorityWarning,
		Data:          map[string]any{"doc_name": "document.pdf", "pages": float64(12), "tags": []any{"a", "b"}, "nested": map[string]any{"ok": true}, "none": nil},
		Title:         "Done",
		Body:          "Your upload finished.",
		Payload:       map[string]string{"doc_id": "d1"},
		Attributes:    map[string]string{"trace": "abc"},
		IdemKey:       "ingest:d1:complete",
		OccurredAt:    t0,
		DeliverAfter:  t0.Add(time.Hour),
		DeliverBefore: t0.Add(2 * time.Hour),
	}
)

// Every field a producer can set crosses the wire and comes back the same (NR-025): the library and
// the service accept the same notification. A field dropped on either leg is a notification that
// means something else to the service than to the library.
func TestANotificationCrossesTheWireWithEveryField(t *testing.T) {
	t.Parallel()
	p, err := wire.NotificationToProto(fullNotification)
	if err != nil {
		t.Fatalf("NotificationToProto: %v", err)
	}
	if got := wire.NotificationFromProto(p); !reflect.DeepEqual(got, fullNotification) {
		t.Fatalf("round trip\n got %+v\nwant %+v", got, fullNotification)
	}
	// The ends of the wire are not the Go types: check the message itself, field by field, against the
	// numbers in the contract's proto.
	if p.GetTenant().GetKind() != "workspace" || p.GetTenant().GetId() != "w1" ||
		p.GetRecipient().GetKind() != "user" || p.GetRecipient().GetId() != "u1" ||
		p.GetTopic() != "ingestion_complete" || p.GetPriority() != "warning" ||
		p.GetTitle() != "Done" || p.GetBody() != "Your upload finished." ||
		p.GetIdemKey() != "ingest:d1:complete" || p.GetPayload()["doc_id"] != "d1" || p.GetAttributes()["trace"] != "abc" {
		t.Errorf("message = %v, a field is missing or in the wrong place", p)
	}
	if !p.GetOccurredAt().AsTime().Equal(t0) || !p.GetDeliverAfter().AsTime().Equal(t0.Add(time.Hour)) || !p.GetDeliverBefore().AsTime().Equal(t0.Add(2*time.Hour)) {
		t.Errorf("times = %v %v %v, want %v and an hour and two after it", p.GetOccurredAt(), p.GetDeliverAfter(), p.GetDeliverBefore(), t0)
	}
}

// An unset time is the zero time on both sides, not 1970: DeliverAfter's zero means "now" and
// DeliverBefore's means "never expires" (D29).
func TestAnUnsetTimeStaysUnset(t *testing.T) {
	t.Parallel()
	n := fullNotification
	n.OccurredAt, n.DeliverAfter, n.DeliverBefore = time.Time{}, time.Time{}, time.Time{}
	p, err := wire.NotificationToProto(n)
	if err != nil {
		t.Fatal(err)
	}
	if p.GetOccurredAt() != nil || p.GetDeliverAfter() != nil || p.GetDeliverBefore() != nil {
		t.Errorf("an unset time was sent as %v %v %v, want none", p.GetOccurredAt(), p.GetDeliverAfter(), p.GetDeliverBefore())
	}
	got := wire.NotificationFromProto(p)
	if !got.OccurredAt.IsZero() || !got.DeliverAfter.IsZero() || !got.DeliverBefore.IsZero() {
		t.Errorf("times came back as %v %v %v, want zero", got.OccurredAt, got.DeliverAfter, got.DeliverBefore)
	}
}

// Data is a JSON document on both sides (the store keeps it as JSONB), so what must survive is the
// document, not the Go types: an int and a []string become a number and an array.
func TestDataSurvivesAsTheSameJSONDocument(t *testing.T) {
	t.Parallel()
	type invoice struct {
		ID    int       `json:"id"`
		Due   time.Time `json:"due"`
		Notes []string  `json:"notes"`
	}
	n := fullNotification
	n.Data = map[string]any{
		"count":   3,
		"names":   []string{"a", "b"},
		"invoice": invoice{ID: 42, Due: t0, Notes: []string{"x"}},
		"exact":   int64(1) << 53, // the largest integer a double holds exactly
		"price":   12.5,
	}
	p, err := wire.NotificationToProto(n)
	if err != nil {
		t.Fatalf("NotificationToProto: %v", err)
	}
	got, _ := json.Marshal(wire.NotificationFromProto(p).Data)
	want, _ := json.Marshal(n.Data)
	if string(got) != string(want) {
		t.Fatalf("data after the round trip\n got %s\nwant %s", got, want)
	}
}

// A number a Struct cannot hold exactly is refused, never rounded: a 19-digit id would arrive as a
// different id, and the template would render someone else's invoice number.
func TestDataThatCannotCrossExactlyIsRefused(t *testing.T) {
	t.Parallel()
	cases := map[string]map[string]any{
		"an integer beyond 2^53":      {"id": int64(1)<<53 + 1},
		"a nested one":                {"a": map[string]any{"b": []any{uint64(1)<<63 + 5}}},
		"a value JSON cannot encode":  {"f": func() {}},
		"a float JSON cannot encode":  {"f": nanFloat()},
		"a negative one beyond -2^53": {"id": -(int64(1)<<53 + 1)},
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			n := fullNotification
			n.Data = data
			if p, err := wire.NotificationToProto(n); err == nil {
				t.Fatalf("NotificationToProto = %v, nil, want an error: %v would not arrive as it was sent", p, data)
			}
		})
	}
}

func nanFloat() float64 { var z float64; return z / z }

func TestAReceiptCrossesTheWire(t *testing.T) {
	t.Parallel()
	r := domain.Receipt{
		NotificationID: "0195f3a0-0000-7000-8000-000000000001", IdemKey: "k1", Applied: true, InboxVisible: true,
		Deliveries: []domain.PlannedDelivery{{Channel: "in_app", Mode: domain.PlanImmediate}, {Channel: "email", Mode: domain.PlanDeferred, NotBefore: t0}},
	}
	p := wire.ReceiptToProto(r)
	if p.GetNotificationId() != r.NotificationID || !p.GetApplied() || !p.GetInboxVisible() ||
		!reflect.DeepEqual(p.GetChannels(), []string{"in_app", "email"}) {
		t.Fatalf("message = %v, want the id, applied, inbox-visible, and the channels in plan order", p)
	}
	got := wire.ReceiptFromProto(p, "k1")
	// The wire carries the channels planned and not each plan's timing, so a remote receipt names the
	// channels and leaves the rest of a plan to Status.
	want := domain.Receipt{NotificationID: r.NotificationID, IdemKey: "k1", Applied: true, InboxVisible: true,
		Deliveries: []domain.PlannedDelivery{{Channel: "in_app"}, {Channel: "email"}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip\n got %+v\nwant %+v", got, want)
	}
	if replay := wire.ReceiptFromProto(wire.ReceiptToProto(domain.Receipt{NotificationID: "n1", IdemKey: "k1"}), "k1"); replay.Applied || len(replay.Deliveries) != 0 {
		t.Errorf("a replay came back as %+v, want not applied and no deliveries", replay)
	}
}

func TestABroadcastCrossesTheWireWithEitherSource(t *testing.T) {
	t.Parallel()
	base := domain.BroadcastRequest{
		Tenant: domain.Tenant{Kind: "workspace", ID: "w1"}, Topic: "release_notes", Priority: domain.PriorityInfo,
		Data: map[string]any{"version": "1.2"}, Title: "1.2", Body: "Notes", Payload: map[string]string{"url": "/r"}, IdemKey: "release:1.2",
	}
	byAudience := base
	byAudience.Audience = "all-members"
	byList := base
	byList.Recipients = []domain.Recipient{{Kind: "user", ID: "u1"}, {Kind: "user", ID: "u2"}}

	for name, b := range map[string]domain.BroadcastRequest{"an audience selector": byAudience, "an inline list": byList} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			p, err := wire.BroadcastToProto(b)
			if err != nil {
				t.Fatal(err)
			}
			if got := wire.BroadcastFromProto(p); !reflect.DeepEqual(got, b) {
				t.Fatalf("round trip\n got %+v\nwant %+v", got, b)
			}
			if (p.GetAudience() != "") == (p.GetRecipients() != nil) {
				t.Errorf("source = audience %q and recipients %v, want exactly one of them", p.GetAudience(), p.GetRecipients())
			}
		})
	}
	// A request that names no source stays one that names none: the Notifier, not the wire, says it is
	// invalid, so the library and the service refuse it in the same words.
	none, err := wire.BroadcastToProto(base)
	if err != nil {
		t.Fatal(err)
	}
	if got := wire.BroadcastFromProto(none); got.Audience != "" || len(got.Recipients) != 0 {
		t.Errorf("a broadcast with no source came back with %q and %v", got.Audience, got.Recipients)
	}
}

func TestCancelStatusAndTheirRequestCrossTheWire(t *testing.T) {
	t.Parallel()
	ref := domain.CancelRequest{Tenant: domain.Tenant{Kind: "workspace", ID: "w1"}, Recipient: domain.Recipient{Kind: "user", ID: "u1"}, IdemKey: "k1"}
	if got := wire.RefFromProto(wire.RefToProto(ref)); got != ref {
		t.Errorf("ref = %+v, want %+v", got, ref)
	}
	cr := domain.CancelReceipt{Matched: true, Canceled: 2, InFlight: 1}
	if got := wire.CancelReceiptFromProto(wire.CancelReceiptToProto(cr)); got != cr {
		t.Errorf("cancel receipt = %+v, want %+v", got, cr)
	}
	st := domain.NotificationStatus{Found: true, NotificationID: "n1", Canceled: true, Deliveries: []domain.DeliveryStatus{
		{Channel: "email", AddressKey: "ak1", State: "delivered", Attempts: 2, ProviderMessageID: "pm-1", ProviderStatus: "delivered", CompletedAt: t0},
		{Channel: "in_app", State: "pending"},
	}}
	if got := wire.StatusFromProto(wire.StatusToProto(st)); !reflect.DeepEqual(got, st) {
		t.Errorf("status\n got %+v\nwant %+v", got, st)
	}
}

// What a producer's code branches on survives the wire as a code and a reason. The pairs are written
// out, not derived: a producer in another language has these literals in its own code, so changing one
// is a breaking change however the Go reads.
func TestAnErrorCrossesTheWireAsItsCodeAndReason(t *testing.T) {
	t.Parallel()
	invalid := &domain.ValidationError{Problems: []domain.Problem{{Field: "Priority", Message: `"urgent" is not info, warning or critical`}}}
	cases := []struct {
		name   string
		err    error
		code   codes.Code
		reason string
		is     error // what the client's error must match
	}{
		{"no idempotency key", fmt.Errorf("notify: %w", domain.ErrMissingIdem), codes.InvalidArgument, "MISSING_IDEM_KEY", domain.ErrMissingIdem},
		{"an empty identity", fmt.Errorf("notify: %w", domain.ErrEmptyIdentity), codes.InvalidArgument, "EMPTY_IDENTITY", domain.ErrEmptyIdentity},
		{"an unregistered topic", fmt.Errorf("broadcast: %w: %q", domain.ErrUnknownTopic, "x"), codes.InvalidArgument, "UNKNOWN_TOPIC", domain.ErrUnknownTopic},
		{"too many recipients", fmt.Errorf("broadcast: %w", domain.ErrTooManyRecipients), codes.InvalidArgument, "TOO_MANY_RECIPIENTS", domain.ErrTooManyRecipients},
		{"no audience resolver", fmt.Errorf("broadcast: %w", domain.ErrAudienceUnsupported), codes.FailedPrecondition, "AUDIENCE_UNSUPPORTED", domain.ErrAudienceUnsupported},
		{"a request that breaks a rule with no sentinel", fmt.Errorf("notify: %w", invalid), codes.InvalidArgument, "INVALID_REQUEST", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sent := wire.ToStatus(tc.err, nil)
			st, ok := status.FromError(sent)
			if !ok || st.Code() != tc.code {
				t.Fatalf("status = %v, %v, want code %v", st, ok, tc.code)
			}
			if got := reasonOf(st); got != tc.reason {
				t.Errorf("reason = %q, want %q", got, tc.reason)
			}
			if !strings.Contains(st.Message(), strings.TrimPrefix(tc.err.Error(), "notify: ")) {
				t.Errorf("message %q does not carry the error's own text %q", st.Message(), tc.err)
			}

			got := wire.FromStatus(sent)
			if got == nil {
				t.Fatal("FromStatus = nil")
			}
			if tc.is != nil && !errors.Is(got, tc.is) {
				t.Errorf("FromStatus(%v) does not match %v", got, tc.is)
			}
			var ve *domain.ValidationError
			if !errors.As(got, &ve) {
				t.Errorf("FromStatus(%v) is not a *domain.ValidationError, which is what the library returns for a request that breaks a rule", got)
			}
			if !strings.Contains(got.Error(), strings.TrimPrefix(tc.err.Error(), "notify: ")) {
				t.Errorf("the client's error %q lost the server's message %q", got, tc.err)
			}
		})
	}
}

// A fault of the engine's own (a SQL error, a host name, a DSN) is for the operator and never for the
// producer: the producer is told there was a fault, and the handler is given the real one.
func TestAnInternalFaultIsNotShownToTheProducer(t *testing.T) {
	t.Parallel()
	fault := errors.New(`notify: persist: ERROR: relation "notifications_2026_03" does not exist at 10.0.0.5:5432 (SQLSTATE 42P01)`)
	var reported error
	sent := wire.ToStatus(fault, func(err error) { reported = err })
	st, _ := status.FromError(sent)
	if st.Code() != codes.Internal {
		t.Fatalf("code = %v, want Internal", st.Code())
	}
	for _, leak := range []string{"10.0.0.5", "notifications_2026_03", "SQLSTATE", "persist"} {
		if strings.Contains(st.Message(), leak) {
			t.Errorf("the producer was told %q, which names the engine's internals (%q)", st.Message(), leak)
		}
	}
	if !errors.Is(reported, fault) {
		t.Errorf("reported %v, want the fault itself so an operator can read it", reported)
	}
}

func TestAContextEndingCrossesTheWireAsItself(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		err  error
		code codes.Code
	}{{context.Canceled, codes.Canceled}, {fmt.Errorf("notify: %w", context.DeadlineExceeded), codes.DeadlineExceeded}} {
		var reported error
		st, _ := status.FromError(wire.ToStatus(tc.err, func(err error) { reported = err }))
		if st.Code() != tc.code {
			t.Errorf("%v became %v, want %v", tc.err, st.Code(), tc.code)
		}
		if reported != nil {
			t.Errorf("%v was reported as a fault: a caller who gave up is not one", tc.err)
		}
		if got := wire.FromStatus(wire.ToStatus(tc.err, nil)); !errors.Is(got, tc.err) {
			t.Errorf("FromStatus gave %v, which is not %v", got, tc.err)
		}
	}
}

func TestNilAndForeignErrorsPassThroughUntouched(t *testing.T) {
	t.Parallel()
	if wire.ToStatus(nil, nil) != nil || wire.FromStatus(nil) != nil {
		t.Error("a nil error became one")
	}
	// A transport failure is not the engine's: the client keeps it as the gRPC status it is, so the
	// producer's retry policy can read the code.
	down := status.Error(codes.Unavailable, "connection refused")
	got := wire.FromStatus(down)
	if st, ok := status.FromError(got); !ok || st.Code() != codes.Unavailable {
		t.Errorf("FromStatus(Unavailable) = %v, want the status kept", got)
	}
	// A status from some other service with an ErrorInfo of its own is not read as ours.
	foreign, err := status.New(codes.InvalidArgument, "bad").WithDetails(&errdetails.ErrorInfo{Domain: "other.example", Reason: "MISSING_IDEM_KEY"})
	if err != nil {
		t.Fatal(err)
	}
	if got := wire.FromStatus(foreign.Err()); errors.Is(got, domain.ErrMissingIdem) {
		t.Errorf("a reason from domain %q was read as ours", "other.example")
	}
}

func reasonOf(st *status.Status) string {
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok && info.GetDomain() == wire.ErrorDomain {
			return info.GetReason()
		}
	}
	return ""
}
