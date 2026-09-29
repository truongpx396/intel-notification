//go:build integration

package postgres

import (
	"crypto/rand"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/truongpx396/intel-notification/domain"
)

func member(recipient string) domain.DigestMember {
	return domain.DigestMember{
		Identity: ident("w1", recipient), Topic: "ingestion_complete", Channel: "email",
		Shard: 3, NotificationID: newUUID(),
	}
}

// newUUID returns a random version 4 UUID.
func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

// D4 / NS-005. Members share a window until DigestMax; a full window seals, the
// next member opens another; a flush enqueues one delivery, once.
func TestDigestWindows(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()
	var windows []string
	for range 3 {
		w, err := e.s.AppendDigest(ctx, member("u1"), 15*time.Minute, 2)
		if err != nil {
			t.Fatal(err)
		}
		windows = append(windows, w)
	}
	if windows[0] != windows[1] || windows[2] == windows[0] {
		t.Fatalf("windows %v: want the first two to share one and the third to open another", windows)
	}

	due, err := e.s.DueDigests(ctx, 3, 10)
	if err != nil || len(due) != 1 || due[0] != windows[0] {
		t.Fatalf("due windows %v (%v): only the sealed, full one should be due now", due, err)
	}

	cases := []struct {
		name string
		want bool
	}{
		{"the first flush enqueues the digest", true},
		{"a duplicate tick is a no-op", false},
	}
	for _, tc := range cases { // sequential on purpose: order is the point
		if queued, err := e.s.FlushDigest(ctx, windows[0]); err != nil || queued != tc.want {
			t.Fatalf("%s: queued=%v err=%v", tc.name, queued, err)
		}
	}
	if n := e.count(`SELECT count(*) FROM notification_outbox WHERE digest_id = $1::uuid`, windows[0]); n != 1 {
		t.Fatalf("%d deliveries for one window, want exactly 1", n)
	}
}

// A burst from many writers at once: every member lands exactly once, no window
// exceeds DigestMax, at most one is ever open, and N members make ceil(N/max)
// windows.
func TestDigestAppendUnderConcurrency(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	const writers, perWindow = 32, 10
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for range writers {
		wg.Go(func() {
			if _, err := e.s.AppendDigest(t.Context(), member("u-burst"), time.Hour, perWindow); err != nil {
				errs <- err
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	var windows, members, largest, open int
	if err := e.db.Admin.QueryRow(t.Context(), `
SELECT count(*), coalesce(sum(member_count), 0), coalesce(max(member_count), 0),
       count(*) FILTER (WHERE sealed_at IS NULL)
  FROM digest_buffer WHERE recipient_id = 'u-burst'`).Scan(&windows, &members, &largest, &open); err != nil {
		t.Fatal(err)
	}
	if members != writers || largest > perWindow || open > 1 || windows != 4 {
		t.Fatalf("windows=%d members=%d largest=%d open=%d; want 4 windows holding all %d, none over %d, at most one open",
			windows, members, largest, open, writers, perWindow)
	}
}
