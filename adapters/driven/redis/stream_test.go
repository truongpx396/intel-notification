package redis

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/truongpx396/intel-notification/domain"
)

// serverError is an error a Redis server reported, as go-redis types them: only
// its text tells one from another.
type serverError string

func (e serverError) Error() string { return string(e) }
func (serverError) RedisError()     {}

// Which Redis faults no retry fixes. The server's error text is the only signal a
// client gets, so it is held to the texts Redis sends.
func TestClassifyPublishFaults(t *testing.T) {
	t.Parallel()
	cases := []struct {
		fault     string
		err       error
		permanent bool
		why       string
	}{
		{"an ACL denies the command", serverError("NOPERM User app has no permissions to run the 'spublish' command"), true,
			"a rotation of the ACL is an operator's act, not a retry's"},
		{"the command is unknown (the text Redis 7 sends)", serverError("ERR unknown command 'spublish', with args beginning with: 'k' 'v'"), true,
			"sharded pub/sub arrived in Redis 7.0, so a server that sends this predates it"},
		{"the command is unknown (the text Redis 6 sends)", serverError("ERR unknown command `SPUBLISH`, with args beginning with: `k` `v`"), true,
			"the older error text carries backticks"},
		{"the dataset is loading", serverError("LOADING Redis is loading the dataset in memory"), false, "it will finish"},
		{"a replica took over", serverError("READONLY You can't write against a read only replica."), false, "failover completes"},
		{"the cluster is down", serverError("CLUSTERDOWN The cluster is down"), false, "it comes back"},
		{"the slot moved", serverError("MOVED 3999 127.0.0.1:6381"), false, "the client follows it"},
		{"too many clients", serverError("ERR max number of clients reached"), false, "load, not configuration"},
		{"a transport error that quotes a server text", errors.New("NOPERM raised by a proxy's own error page"), false,
			"only a server's own verdict is permanent, and a guess dead-letters a deliverable nudge"},
		{"the connection dropped", io.EOF, false, ""},
		{"the deadline passed", context.DeadlineExceeded, false, ""},
		{"the worker is stopping", context.Canceled, false, ""},
	}
	for _, tc := range cases {
		t.Run(tc.fault, func(t *testing.T) {
			t.Parallel()
			got := classify(tc.err)
			if is := errors.Is(got, domain.ErrPermanent); is != tc.permanent {
				t.Fatalf("classify(%v) permanent = %v, want %v: %s", tc.err, is, tc.permanent, tc.why)
			}
			if !errors.Is(got, tc.err) {
				t.Fatalf("classify(%v) = %v, which lost the error it classified", tc.err, got)
			}
		})
	}
	if classify(nil) != nil {
		t.Fatal("classify(nil) is not nil")
	}
}
