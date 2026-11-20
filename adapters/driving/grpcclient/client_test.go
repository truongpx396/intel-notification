package grpcclient_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/truongpx396/intel-notification/adapters/driving/grpcclient"
	"github.com/truongpx396/intel-notification/adapters/driving/grpcserver"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/notifytest"
)

// countingConn is a connection that counts the calls made on it and fails each one, so a test can say
// that a method never reached the wire.
type countingConn struct{ calls atomic.Int64 }

func (c *countingConn) Invoke(context.Context, string, any, any, ...grpc.CallOption) error {
	c.calls.Add(1)
	return status.Error(codes.Unavailable, "no server")
}

func (c *countingConn) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	c.calls.Add(1)
	return nil, status.Error(codes.Unavailable, "no server")
}

// A remote Notifier cannot join a caller's transaction: the producer's database is not the service's,
// so the service cannot commit with it (D28). NotifyTx says so and does nothing else: it opens no
// transaction, makes no call, and touches nothing it was handed.
func TestNotifyTxIsUnsupportedAndNeverOpensATransaction(t *testing.T) {
	t.Parallel()
	tx := &struct{ touched bool }{}
	for name, arg := range map[string]any{"a caller's transaction": tx, "no transaction": nil} {
		conn := &countingConn{}
		r, err := grpcclient.New(conn).NotifyTx(t.Context(), arg, domain.Notification{IdemKey: "k1"})
		if !errors.Is(err, domain.ErrTxUnsupported) {
			t.Errorf("%s: NotifyTx = %v, want domain.ErrTxUnsupported", name, err)
		}
		if !reflect.DeepEqual(r, domain.Receipt{}) {
			t.Errorf("%s: NotifyTx returned a receipt %+v for work it did not do", name, r)
		}
		if got := conn.calls.Load(); got != 0 {
			t.Errorf("%s: NotifyTx made %d calls on the connection, want none", name, got)
		}
	}
	if tx.touched {
		t.Error("NotifyTx touched the caller's transaction")
	}
}

// What the engine says reaches the producer as the same sentinel, so `errors.Is` reads the same in
// library mode and in service mode.
func TestARefusalArrivesAsTheSentinelTheLibraryReturns(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  error
		is   error
	}{
		{"an unregistered topic", fmt.Errorf("notify: %w: %q", domain.ErrUnknownTopic, "x"), domain.ErrUnknownTopic},
		{"an empty identity", fmt.Errorf("notify: %w", domain.ErrEmptyIdentity), domain.ErrEmptyIdentity},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			n := &notifytest.ScriptedNotifier{OnNotify: func(context.Context, domain.Notification) (domain.Receipt, error) { return domain.Receipt{}, tc.err }}
			_, err := grpcclient.New(connTo(t, n)).Notify(t.Context(), domain.Notification{
				Tenant: domain.Tenant{Kind: "workspace", ID: "w1"}, Recipient: domain.Recipient{Kind: "user", ID: "u1"}, Topic: "x", IdemKey: "k1",
			})
			if !errors.Is(err, tc.is) {
				t.Fatalf("Notify = %v, want it to match %v", err, tc.is)
			}
			var ve *domain.ValidationError
			if !errors.As(err, &ve) {
				t.Errorf("Notify = %v, want a *domain.ValidationError as the library returns", err)
			}
		})
	}
}

func TestNotifySendsTheNotificationAndReadsTheReceipt(t *testing.T) {
	t.Parallel()
	n := &notifytest.ScriptedNotifier{OnNotify: func(_ context.Context, nt domain.Notification) (domain.Receipt, error) {
		return domain.Receipt{NotificationID: "n1", IdemKey: nt.IdemKey, Applied: true, InboxVisible: true,
			Deliveries: []domain.PlannedDelivery{{Channel: "in_app"}, {Channel: "email"}}}, nil
	}}
	want := domain.Notification{
		Tenant: domain.Tenant{Kind: "workspace", ID: "w1"}, Recipient: domain.Recipient{Kind: "user", ID: "u1"},
		Topic: "ingestion_complete", Priority: domain.PriorityCritical, Data: map[string]any{"doc": "a.pdf", "pages": float64(3)},
		Title: "Done", Body: "b", Payload: map[string]string{"p": "1"}, IdemKey: "k1",
		OccurredAt: time.Date(2026, 3, 4, 10, 0, 0, 0, time.UTC),
	}
	r, err := grpcclient.New(connTo(t, n)).Notify(t.Context(), want)
	if err != nil {
		t.Fatalf("Notify: %v", err)
	}
	wantReceipt := domain.Receipt{NotificationID: "n1", IdemKey: "k1", Applied: true, InboxVisible: true,
		Deliveries: []domain.PlannedDelivery{{Channel: "in_app"}, {Channel: "email"}}}
	if !reflect.DeepEqual(r, wantReceipt) {
		t.Errorf("receipt = %+v, want %+v", r, wantReceipt)
	}
	if got := n.Notified(); len(got) != 1 || !reflect.DeepEqual(got[0], want) {
		t.Errorf("the engine received\n %+v\nwant\n %+v", got, want)
	}
}

// Data a Struct cannot hold exactly is refused by the client, before any call: sending it would
// deliver a different number than the producer wrote.
func TestNotifyRefusesDataThatCannotCrossBeforeCalling(t *testing.T) {
	t.Parallel()
	conn := &countingConn{}
	_, err := grpcclient.New(conn).Notify(t.Context(), domain.Notification{IdemKey: "k1", Data: map[string]any{"id": int64(1)<<53 + 1}})
	if err == nil {
		t.Fatal("Notify accepted an integer a double cannot hold")
	}
	if conn.calls.Load() != 0 {
		t.Errorf("%d calls made for a notification that could not be sent", conn.calls.Load())
	}
}

// A producer's deadline is its own and ends the call as the library's context would: the error
// matches context.DeadlineExceeded, which is what a retry policy keys on.
func TestADeadlineEndsTheCallAsAContextError(t *testing.T) {
	t.Parallel()
	n := &notifytest.ScriptedNotifier{OnNotify: func(ctx context.Context, _ domain.Notification) (domain.Receipt, error) {
		<-ctx.Done()
		return domain.Receipt{}, ctx.Err()
	}}
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	_, err := grpcclient.New(connTo(t, n)).Notify(ctx, domain.Notification{IdemKey: "k1"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Notify = %v, want it to match context.DeadlineExceeded", err)
	}
}

// A transport failure stays a gRPC status, so the producer's retry policy can read Unavailable.
func TestATransportFailureKeepsItsStatus(t *testing.T) {
	t.Parallel()
	_, err := grpcclient.New(&countingConn{}).Notify(t.Context(), domain.Notification{IdemKey: "k1"})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("Notify = %v, want the Unavailable status kept", err)
	}
}

func TestBroadcastCancelAndStatusMapBothWays(t *testing.T) {
	t.Parallel()
	n := &notifytest.ScriptedNotifier{
		OnBroadcast: func(context.Context, domain.BroadcastRequest) (domain.BroadcastReceipt, error) {
			return domain.BroadcastReceipt{BroadcastID: "b1", Applied: true}, nil
		},
		OnCancel: func(context.Context, domain.CancelRequest) (domain.CancelReceipt, error) {
			return domain.CancelReceipt{Matched: true, Canceled: 2, InFlight: 1}, nil
		},
		OnStatus: func(context.Context, domain.StatusRequest) (domain.NotificationStatus, error) {
			return domain.NotificationStatus{Found: true, NotificationID: "n1", Canceled: true,
				Deliveries: []domain.DeliveryStatus{{Channel: "email", State: "delivered", Attempts: 2, ProviderMessageID: "pm"}}}, nil
		},
	}
	c := grpcclient.New(connTo(t, n))
	tenant, rcpt := domain.Tenant{Kind: "workspace", ID: "w1"}, domain.Recipient{Kind: "user", ID: "u1"}

	b := domain.BroadcastRequest{Tenant: tenant, Audience: "all-members", Topic: "release", IdemKey: "r1"}
	br, err := c.Broadcast(t.Context(), b)
	if err != nil || br != (domain.BroadcastReceipt{BroadcastID: "b1", Applied: true}) {
		t.Fatalf("Broadcast = %+v, %v", br, err)
	}
	if got := n.Broadcasted(); len(got) != 1 || !reflect.DeepEqual(got[0], b) {
		t.Errorf("the engine received %+v, want %+v", got, b)
	}
	ref := domain.CancelRequest{Tenant: tenant, Recipient: rcpt, IdemKey: "k1"}
	if cr, err := c.Cancel(t.Context(), ref); err != nil || cr != (domain.CancelReceipt{Matched: true, Canceled: 2, InFlight: 1}) {
		t.Fatalf("Cancel = %+v, %v", cr, err)
	}
	st, err := c.Status(t.Context(), ref)
	if err != nil || !st.Found || !st.Canceled || len(st.Deliveries) != 1 || st.Deliveries[0].ProviderMessageID != "pm" {
		t.Fatalf("Status = %+v, %v", st, err)
	}
	if got := n.StatusAsked(); len(got) != 1 || got[0] != ref {
		t.Errorf("Status asked the engine %+v, want %+v", got, ref)
	}
}

func TestNotifyBatchReturnsOneAnswerPerNotification(t *testing.T) {
	t.Parallel()
	n := &notifytest.ScriptedNotifier{OnNotify: func(_ context.Context, nt domain.Notification) (domain.Receipt, error) {
		if nt.IdemKey == "bad" {
			return domain.Receipt{}, fmt.Errorf("notify: %w: %q", domain.ErrUnknownTopic, "x")
		}
		return domain.Receipt{NotificationID: "n-" + nt.IdemKey, IdemKey: nt.IdemKey, Applied: true}, nil
	}}
	mk := func(key string) domain.Notification {
		return domain.Notification{Tenant: domain.Tenant{Kind: "workspace", ID: "w1"}, Recipient: domain.Recipient{Kind: "user", ID: "u1"}, Topic: "x", IdemKey: key}
	}
	got, err := grpcclient.New(connTo(t, n)).NotifyBatch(t.Context(), []domain.Notification{mk("k1"), mk("bad"), mk("k3")})
	if err != nil || len(got) != 3 {
		t.Fatalf("NotifyBatch = %+v, %v, want three answers", got, err)
	}
	if got[0].Err != nil || got[0].Receipt.NotificationID != "n-k1" || got[0].Receipt.IdemKey != "k1" {
		t.Errorf("answer 0 = %+v, want the accepted receipt with its key", got[0])
	}
	if got[1].Err == nil || got[2].Err != nil || got[2].Receipt.NotificationID != "n-k3" {
		t.Errorf("answers 1 and 2 = %+v and %+v, want a refusal and then an accepted one: an item's refusal does not stop the next", got[1], got[2])
	}
}

func connTo(t *testing.T, n *notifytest.ScriptedNotifier) grpc.ClientConnInterface {
	t.Helper()
	return notifytest.NewBufconn(t, grpcserver.New(n).Register)
}

func TestNotifyBatchOverTheLimitIsRefusedWithoutACall(t *testing.T) {
	t.Parallel()
	conn := &countingConn{}
	_, err := grpcclient.New(conn).NotifyBatch(t.Context(), make([]domain.Notification, grpcclient.MaxBatch+1))
	if !errors.Is(err, grpcclient.ErrBatchTooLarge) || conn.calls.Load() != 0 {
		t.Fatalf("NotifyBatch(501) = %v after %d calls, want ErrBatchTooLarge and no call", err, conn.calls.Load())
	}
}
