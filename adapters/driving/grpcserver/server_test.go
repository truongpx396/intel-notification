package grpcserver_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/truongpx396/intel-notification/adapters/driving/grpcserver"
	"github.com/truongpx396/intel-notification/api/notifyv1"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/notifytest"
	"github.com/truongpx396/intel-notification/ports"
)

// serve starts a server over n and returns a generated client of it: the whole gRPC stack in memory,
// so the tests assert on what a producer in any language would see.
func serve(t *testing.T, n ports.Notifier, opts ...grpcserver.Option) notifyv1.NotifierClient {
	t.Helper()
	return notifyv1.NewNotifierClient(notifytest.NewBufconn(t, grpcserver.New(n, opts...).Register))
}

func note(key string) *notifyv1.Notification {
	return &notifyv1.Notification{
		Tenant: &notifyv1.Tenant{Kind: "workspace", Id: "w1"}, Recipient: &notifyv1.Recipient{Kind: "user", Id: "u1"},
		Topic: "ingestion_complete", Title: "Done", IdemKey: key,
	}
}

func reasonOf(err error) string {
	for _, d := range status.Convert(err).Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok && info.GetDomain() == "notify.v1" {
			return info.GetReason()
		}
	}
	return ""
}

// The server hands the engine the notification the producer sent, and the producer the receipt the
// engine gave.
func TestNotifyPassesTheNotificationOnAndTheReceiptBack(t *testing.T) {
	t.Parallel()
	n := &notifytest.ScriptedNotifier{OnNotify: func(_ context.Context, nt domain.Notification) (domain.Receipt, error) {
		return domain.Receipt{NotificationID: "n1", IdemKey: nt.IdemKey, Applied: true, InboxVisible: true,
			Deliveries: []domain.PlannedDelivery{{Channel: "in_app"}, {Channel: "email"}}}, nil
	}}
	req := note("k1")
	req.Priority, req.Payload = "critical", map[string]string{"doc_id": "d1"}

	got, err := serve(t, n).Notify(t.Context(), req)
	if err != nil {
		t.Fatalf("Notify: %v", err)
	}
	if got.GetNotificationId() != "n1" || !got.GetApplied() || !got.GetInboxVisible() || strings.Join(got.GetChannels(), ",") != "in_app,email" {
		t.Errorf("receipt = %v, want n1, applied, inbox-visible, in_app and email", got)
	}
	asked := n.Notified()
	if len(asked) != 1 || asked[0].IdemKey != "k1" || asked[0].Tenant.ID != "w1" || asked[0].Recipient.ID != "u1" ||
		asked[0].Topic != "ingestion_complete" || asked[0].Priority != domain.PriorityCritical || asked[0].Payload["doc_id"] != "d1" {
		t.Errorf("the engine was asked %+v, want what the producer sent", asked)
	}
}

// idem_key is required on every mutating RPC: a missing one is INVALID_ARGUMENT, never a notification
// that nothing guards against a retry (contract rule). The server says so itself, so it does not
// depend on the engine behind it remembering to.
func TestAMissingIdemKeyIsRefusedBeforeTheEngineIsAsked(t *testing.T) {
	t.Parallel()
	n := &notifytest.ScriptedNotifier{}
	c := serve(t, n)
	ctx := t.Context()

	cases := map[string]func() error{
		"Notify": func() error { _, err := c.Notify(ctx, note("")); return err },
		"Broadcast": func() error {
			_, err := c.Broadcast(ctx, &notifyv1.BroadcastRequest{Tenant: note("").Tenant, Topic: "t", Source: &notifyv1.BroadcastRequest_Audience{Audience: "all"}})
			return err
		},
		"Cancel": func() error {
			_, err := c.Cancel(ctx, &notifyv1.NotificationRef{Tenant: note("").Tenant, Recipient: note("").Recipient})
			return err
		},
	}
	for name, call := range cases {
		err := call()
		if status.Code(err) != codes.InvalidArgument || reasonOf(err) != "MISSING_IDEM_KEY" {
			t.Errorf("%s with no idem_key = %v (reason %q), want InvalidArgument MISSING_IDEM_KEY", name, err, reasonOf(err))
		}
	}
	if got := len(n.Notified()) + len(n.Broadcasted()) + len(n.Canceled()); got != 0 {
		t.Errorf("the engine was asked %d times for requests with no key, want none", got)
	}
}

func TestTheEnginesRefusalsKeepTheirCodeAndReason(t *testing.T) {
	t.Parallel()
	cases := []struct {
		err    error
		code   codes.Code
		reason string
	}{
		{fmt.Errorf("notify: %w: %q", domain.ErrUnknownTopic, "x"), codes.InvalidArgument, "UNKNOWN_TOPIC"},
		{fmt.Errorf("notify: %w", domain.ErrEmptyIdentity), codes.InvalidArgument, "EMPTY_IDENTITY"},
	}
	for _, tc := range cases {
		n := &notifytest.ScriptedNotifier{OnNotify: func(context.Context, domain.Notification) (domain.Receipt, error) { return domain.Receipt{}, tc.err }}
		_, err := serve(t, n).Notify(t.Context(), note("k1"))
		if status.Code(err) != tc.code || reasonOf(err) != tc.reason {
			t.Errorf("%v became %v (reason %q), want %v %s", tc.err, err, reasonOf(err), tc.code, tc.reason)
		}
	}
}

func TestAFaultOfTheEnginesOwnIsReportedAndNotShown(t *testing.T) {
	t.Parallel()
	fault := errors.New("notify: persist: connection to 10.0.0.5:5432 refused")
	var reported atomic.Value
	n := &notifytest.ScriptedNotifier{OnNotify: func(context.Context, domain.Notification) (domain.Receipt, error) { return domain.Receipt{}, fault }}
	_, err := serve(t, n, grpcserver.WithErrorHandler(func(e error) { reported.Store(e) })).Notify(t.Context(), note("k1"))
	if status.Code(err) != codes.Internal || strings.Contains(err.Error(), "10.0.0.5") {
		t.Fatalf("Notify = %v, want Internal with the fault's text withheld", err)
	}
	if got, _ := reported.Load().(error); !errors.Is(got, fault) {
		t.Errorf("reported %v, want the fault", got)
	}
}

// The deadline a producer sets and the cancellation it sends reach the engine: a producer that gives
// up does not leave a transaction running for it.
func TestTheCallersDeadlineAndCancellationReachTheEngine(t *testing.T) {
	t.Parallel()
	var sawDeadline atomic.Bool
	held, released := make(chan struct{}), make(chan struct{})
	n := &notifytest.ScriptedNotifier{OnNotify: func(ctx context.Context, nt domain.Notification) (domain.Receipt, error) {
		if _, ok := ctx.Deadline(); ok {
			sawDeadline.Store(true)
		}
		if nt.IdemKey == "hang" {
			close(held)
			<-ctx.Done()
			close(released)
			return domain.Receipt{}, ctx.Err()
		}
		return domain.Receipt{NotificationID: "n", Applied: true}, nil
	}}
	c := serve(t, n)

	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	if _, err := c.Notify(ctx, note("k1")); err != nil || !sawDeadline.Load() {
		t.Fatalf("Notify = %v, deadline seen by the engine = %v, want the producer's deadline to arrive", err, sawDeadline.Load())
	}

	hung, cancelHung := context.WithCancel(t.Context())
	go func() { _, _ = c.Notify(hung, note("hang")) }()
	wait := func(ch <-chan struct{}, what string) {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(10 * time.Second):
			t.Fatal(what)
		}
	}
	wait(held, "the engine never received the call")
	cancelHung()
	wait(released, "the engine's context did not end when the producer cancelled")
}

func TestNotifyBatchIsRefusedWholeOverTheLimit(t *testing.T) {
	t.Parallel()
	n := &notifytest.ScriptedNotifier{}
	c := serve(t, n)
	batch := func(size int) *notifyv1.NotifyBatchRequest {
		r := &notifyv1.NotifyBatchRequest{}
		for i := range size {
			r.Notifications = append(r.Notifications, note(fmt.Sprintf("k%d", i)))
		}
		return r
	}
	_, err := c.NotifyBatch(t.Context(), batch(grpcserver.MaxBatch+1))
	if status.Code(err) != codes.InvalidArgument || reasonOf(err) != "BATCH_TOO_LARGE" {
		t.Fatalf("a batch of %d = %v (reason %q), want InvalidArgument BATCH_TOO_LARGE", grpcserver.MaxBatch+1, err, reasonOf(err))
	}
	if got := len(n.Notified()); got != 0 {
		t.Errorf("%d notifications of a refused batch were accepted, want none: a half-applied batch cannot be retried safely", got)
	}
	resp, err := c.NotifyBatch(t.Context(), batch(grpcserver.MaxBatch))
	if err != nil || len(resp.GetReceipts()) != grpcserver.MaxBatch {
		t.Fatalf("a batch of exactly %d = %d receipts, %v, want all of them", grpcserver.MaxBatch, len(resp.GetReceipts()), err)
	}
}

// Each notification of a batch is its own: one the engine refuses does not stop the next, and the
// answers are in the order of the questions, so a producer retries exactly the ones that failed.
func TestEachNotificationOfABatchIsIndependent(t *testing.T) {
	t.Parallel()
	n := &notifytest.ScriptedNotifier{OnNotify: func(_ context.Context, nt domain.Notification) (domain.Receipt, error) {
		switch nt.IdemKey {
		case "bad-topic":
			return domain.Receipt{}, fmt.Errorf("notify: %w: %q", domain.ErrUnknownTopic, "nope")
		case "fault":
			return domain.Receipt{}, errors.New("persist: connection to 10.0.0.5 refused")
		}
		return domain.Receipt{NotificationID: "n-" + nt.IdemKey, IdemKey: nt.IdemKey, Applied: true}, nil
	}}
	resp, err := serve(t, n).NotifyBatch(t.Context(), &notifyv1.NotifyBatchRequest{Notifications: []*notifyv1.Notification{
		note("k1"), note("bad-topic"), note(""), note("fault"), note("k5"),
	}})
	if err != nil {
		t.Fatalf("NotifyBatch: %v", err)
	}
	if len(resp.GetReceipts()) != 5 || len(resp.GetErrors()) != 5 {
		t.Fatalf("%d receipts and %d errors for 5 notifications, want one of each per notification", len(resp.GetReceipts()), len(resp.GetErrors()))
	}
	for i, wantID := range []string{"n-k1", "", "", "", "n-k5"} {
		if got := resp.GetReceipts()[i].GetNotificationId(); got != wantID {
			t.Errorf("receipt %d = %q, want %q", i, got, wantID)
		}
	}
	errs := resp.GetErrors()
	if errs[0] != "" || errs[4] != "" {
		t.Errorf("errors of the accepted ones = %q and %q, want empty", errs[0], errs[4])
	}
	if !strings.Contains(errs[1], "not registered") || !strings.Contains(errs[2], "IdemKey") {
		t.Errorf("errors = %q and %q, want the engine's words for a bad topic and a missing key", errs[1], errs[2])
	}
	if strings.Contains(errs[3], "10.0.0.5") || errs[3] == "" {
		t.Errorf("error of the faulted one = %q, want a fault named and not explained", errs[3])
	}
	if got := n.Notified(); len(got) != 4 {
		t.Errorf("the engine was asked %d times, want 4: the item with no key never reaches it", len(got))
	}
}

func TestBroadcastCancelAndStatusGoThroughTheEngine(t *testing.T) {
	t.Parallel()
	n := &notifytest.ScriptedNotifier{
		OnBroadcast: func(context.Context, domain.BroadcastRequest) (domain.BroadcastReceipt, error) {
			return domain.BroadcastReceipt{BroadcastID: "b1", Applied: true}, nil
		},
		OnCancel: func(context.Context, domain.CancelRequest) (domain.CancelReceipt, error) {
			return domain.CancelReceipt{Matched: true, Canceled: 2, InFlight: 1}, nil
		},
		OnStatus: func(context.Context, domain.StatusRequest) (domain.NotificationStatus, error) {
			return domain.NotificationStatus{Found: true, NotificationID: "n1", Deliveries: []domain.DeliveryStatus{{Channel: "email", State: "delivered", Attempts: 1}}}, nil
		},
	}
	c := serve(t, n)
	ctx := t.Context()

	b, err := c.Broadcast(ctx, &notifyv1.BroadcastRequest{
		Tenant: note("").Tenant, Topic: "release_notes", IdemKey: "release:1",
		Source: &notifyv1.BroadcastRequest_Recipients{Recipients: &notifyv1.RecipientList{Recipients: []*notifyv1.Recipient{{Kind: "user", Id: "u1"}}}},
	})
	if err != nil || b.GetBroadcastId() != "b1" || !b.GetApplied() {
		t.Fatalf("Broadcast = %v, %v", b, err)
	}
	if got := n.Broadcasted(); len(got) != 1 || got[0].Topic != "release_notes" || len(got[0].Recipients) != 1 || got[0].Audience != "" {
		t.Errorf("the engine was asked %+v, want the inline list and no audience", got)
	}

	ref := &notifyv1.NotificationRef{Tenant: note("").Tenant, Recipient: note("").Recipient, IdemKey: "k1"}
	cr, err := c.Cancel(ctx, ref)
	if err != nil || !cr.GetMatched() || cr.GetCanceled() != 2 || cr.GetInFlight() != 1 {
		t.Fatalf("Cancel = %v, %v", cr, err)
	}
	st, err := c.Status(ctx, ref)
	if err != nil || !st.GetFound() || st.GetNotificationId() != "n1" || len(st.GetDeliveries()) != 1 || st.GetDeliveries()[0].GetState() != "delivered" {
		t.Fatalf("Status = %v, %v", st, err)
	}
	if got := n.StatusAsked(); len(got) != 1 || got[0].IdemKey != "k1" || got[0].Tenant.ID != "w1" {
		t.Errorf("Status asked the engine %+v, want the producer's reference", got)
	}
}
