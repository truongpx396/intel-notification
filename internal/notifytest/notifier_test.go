package notifytest_test

import (
	"context"
	"errors"
	"testing"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/notifytest"
)

// The fake is a fake of a port, so it is held to what it says: it remembers every call in order,
// answers from the function it was given, and accepts everything when it was given none.
func TestScriptedNotifierRemembersAndAnswers(t *testing.T) {
	t.Parallel()
	boom := errors.New("boom")
	n := &notifytest.ScriptedNotifier{}

	r, err := n.Notify(t.Context(), domain.Notification{IdemKey: "k1"})
	if err != nil || !r.Applied || r.NotificationID != "n-k1" || r.IdemKey != "k1" {
		t.Fatalf("default Notify = %+v, %v, want applied, named after the key", r, err)
	}
	n.OnNotify = func(context.Context, domain.Notification) (domain.Receipt, error) { return domain.Receipt{}, boom }
	if _, err := n.Notify(t.Context(), domain.Notification{IdemKey: "k2"}); !errors.Is(err, boom) {
		t.Fatalf("scripted Notify = %v, want the scripted error", err)
	}
	if got := n.Notified(); len(got) != 2 || got[0].IdemKey != "k1" || got[1].IdemKey != "k2" {
		t.Errorf("Notified = %+v, want k1 then k2", got)
	}

	if b, _ := n.Broadcast(t.Context(), domain.BroadcastRequest{IdemKey: "b1"}); !b.Applied || b.BroadcastID != "b-b1" {
		t.Errorf("default Broadcast = %+v", b)
	}
	_, _ = n.Cancel(t.Context(), domain.CancelRequest{IdemKey: "c1"})
	_, _ = n.Status(t.Context(), domain.StatusRequest{IdemKey: "s1"})
	_, _ = n.NotifyTx(t.Context(), struct{}{}, domain.Notification{IdemKey: "t1"})
	if len(n.Broadcasted()) != 1 || len(n.Canceled()) != 1 || len(n.StatusAsked()) != 1 || n.TxCalls() != 1 {
		t.Errorf("calls = %d broadcasts, %d cancels, %d statuses, %d tx, want one of each",
			len(n.Broadcasted()), len(n.Canceled()), len(n.StatusAsked()), n.TxCalls())
	}
}
