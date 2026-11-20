package notifytest

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/truongpx396/intel-notification/domain"
)

// Event is one server-sent event as the relay sent it. A comment line (the relay's heartbeat) is an
// Event whose Name is ":" and whose Data is the comment.
type Event struct {
	Name string
	Data string
}

// Relay is a client of the live stream at a base URL: it opens /notifications/stream as an identity
// and reads what comes back, the way a browser's EventSource does, so a suite can ask of the relay
// what a recipient would see (D16, NS-001).
type Relay struct {
	// BaseURL is the relay's, without a trailing slash.
	BaseURL string
	// Token returns the credential that makes the relay see id as the caller. An empty one sends no
	// Authorization header, for a test that carries the token another way.
	Token func(domain.Identity) string
	// Wait is how long MustReceive and Next wait for an event before the test fails. Never zero.
	Wait time.Duration
}

// NewRelay returns a client of the relay at baseURL.
func NewRelay(baseURL string, token func(domain.Identity) string) *Relay {
	return &Relay{BaseURL: baseURL, Token: token, Wait: 10 * time.Second}
}

// RelayConn is one open stream.
type RelayConn struct {
	relay  *Relay
	events chan Event
	cancel context.CancelFunc
	done   chan struct{}
}

// Connect opens the stream as id and returns once the relay has answered 200, which it does only after
// its subscription is established: a nudge published after Connect returns is heard. Any other status
// fails the test.
func (r *Relay) Connect(tb testing.TB, id domain.Identity, headers ...string) *RelayConn {
	tb.Helper()
	return r.ConnectAt(tb, id, "/notifications/stream", headers...)
}

// ConnectAt is Connect to a path and query of the caller's choosing, for a test that asks whether the
// relay reads anything from them.
func (r *Relay) ConnectAt(tb testing.TB, id domain.Identity, pathAndQuery string, headers ...string) *RelayConn {
	tb.Helper()
	resp, conn := r.open(tb, id, pathAndQuery, headers...)
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		conn.cancel()
		tb.Fatalf("notifytest: the relay answered %d to the stream for %+v, want 200", resp.StatusCode, id)
	}
	go conn.read(resp)
	tb.Cleanup(conn.Close)
	return conn
}

// Status opens the stream as id and returns the status the relay answered, closing it at once: for a
// test of what the relay refuses.
func (r *Relay) Status(tb testing.TB, id domain.Identity, pathAndQuery string, headers ...string) int {
	tb.Helper()
	resp, conn := r.open(tb, id, pathAndQuery, headers...)
	_ = resp.Body.Close()
	conn.cancel()
	return resp.StatusCode
}

func (r *Relay) open(tb testing.TB, id domain.Identity, pathAndQuery string, headers ...string) (*http.Response, *RelayConn) {
	tb.Helper()
	ctx, cancel := context.WithCancel(tb.Context())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.BaseURL+pathAndQuery, nil)
	if err != nil {
		cancel()
		tb.Fatalf("notifytest: build the stream request: %v", err)
	}
	req.Header.Set("Accept", "text/event-stream")
	if token := r.Token(id); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		cancel()
		tb.Fatalf("notifytest: open the stream: %v", err)
	}
	return resp, &RelayConn{relay: r, events: make(chan Event, 256), cancel: cancel, done: make(chan struct{})}
}

// read parses the body into events until it ends.
func (c *RelayConn) read(resp *http.Response) {
	defer close(c.done)
	defer close(c.events)
	defer func() { _ = resp.Body.Close() }()
	sc := bufio.NewScanner(resp.Body)
	var ev Event
	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "":
			if ev.Name != "" {
				c.events <- ev
			}
			ev = Event{}
		case strings.HasPrefix(line, ":"):
			c.events <- Event{Name: ":", Data: strings.TrimSpace(line[1:])}
		case strings.HasPrefix(line, "event:"):
			ev.Name = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		case strings.HasPrefix(line, "data:"):
			ev.Data = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		}
	}
}

// Close ends the stream. It is safe to call more than once, and the test's cleanup calls it.
func (c *RelayConn) Close() {
	c.cancel()
	<-c.done
}

// Next returns the next event of any name, or fails the test if none comes in Wait.
func (c *RelayConn) Next(tb testing.TB) Event {
	tb.Helper()
	select {
	case ev, ok := <-c.events:
		if !ok {
			tb.Fatal("notifytest: the stream ended while an event was expected")
		}
		return ev
	case <-time.After(c.relay.Wait):
		tb.Fatalf("notifytest: no event within %v", c.relay.Wait)
		return Event{}
	}
}

// Count is the badge a "count" event carried.
type Count struct {
	Count  int  `json:"count"`
	Capped bool `json:"capped"`
}

// NextCount returns the next event, which must be a count.
func (c *RelayConn) NextCount(tb testing.TB) Count {
	tb.Helper()
	ev := c.Next(tb)
	if ev.Name != "count" {
		tb.Fatalf("notifytest: the next event is %q %s, want a count", ev.Name, ev.Data)
	}
	var n Count
	if err := json.Unmarshal([]byte(ev.Data), &n); err != nil {
		tb.Fatalf("notifytest: a count event %q: %v", ev.Data, err)
	}
	return n
}

// Notification is what a "notification" event carried: the item, and the badge after it.
type Notification struct {
	Item struct {
		ID    string `json:"id"`
		Topic string `json:"topic"`
		Title string `json:"title"`
		Body  string `json:"body"`
	} `json:"item"`
	Unread Count `json:"unread"`
}

// MustReceive waits for the notification event for notificationID, skipping counts and the events of
// other notifications, and fails the test if it does not come in Wait.
func (c *RelayConn) MustReceive(tb testing.TB, notificationID string) Notification {
	tb.Helper()
	deadline := time.After(c.relay.Wait)
	for {
		select {
		case ev, ok := <-c.events:
			if !ok {
				tb.Fatalf("notifytest: the stream ended before the notification %s arrived", notificationID)
			}
			if ev.Name != "notification" {
				continue
			}
			var n Notification
			if err := json.Unmarshal([]byte(ev.Data), &n); err != nil {
				tb.Fatalf("notifytest: a notification event %q: %v", ev.Data, err)
			}
			if n.Item.ID == notificationID {
				return n
			}
		case <-deadline:
			tb.Fatalf("notifytest: notification %s did not arrive within %v", notificationID, c.relay.Wait)
		}
	}
}

// MustReceiveNothing fails the test if a notification event arrives within d. It is the one wait a
// suite spends in real time, because "nothing is shown" can be seen only by letting time pass; every
// positive assertion waits on the event and not on the clock.
func (c *RelayConn) MustReceiveNothing(tb testing.TB, d time.Duration) {
	tb.Helper()
	deadline := time.After(d)
	for {
		select {
		case ev, ok := <-c.events:
			if !ok {
				return
			}
			if ev.Name == "notification" {
				tb.Fatalf("notifytest: a notification arrived on a stream that must stay silent: %s", ev.Data)
			}
		case <-deadline:
			return
		}
	}
}
