package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
)

// seenCap bounds the notification ids one connection remembers, so a long stream does not grow with
// every notification it has carried.
const seenCap = 4096

// stream is the live push (D16): server-sent events.
//
//   - It subscribes from the session's identity, which is what the host's session or token says, and
//     from nothing else: not a query parameter, not a header, not a path.
//   - On every (re)connect it sends the authoritative bounded count first (D15), so a push that was
//     missed is healed by the next connect.
//   - A nudge carries an id and nothing else. For each one the relay loads the notification under the
//     caller's scope and sends it with the new count; a nudge that loads nothing sends nothing, so a
//     forged or misrouted nudge shows nobody anything. The push path inherits the data layer's
//     isolation instead of needing its own.
//
// EventSource cannot set a header, so on this route, and no other, a token in the query is moved to
// Authorization before the host's IdentityFunc sees the request. URLs are logged, which is why no other
// route reads one.
func (s *server) stream(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") == "" {
		if token := r.URL.Query().Get("token"); token != "" {
			r = r.Clone(r.Context())
			r.Header.Set("Authorization", "Bearer "+token)
		}
	}
	id, ok := s.identify(w, r)
	if !ok {
		return
	}
	flusher, canFlush := w.(http.Flusher)
	if !canFlush {
		s.onError(errors.New("httpapi: the response writer cannot flush, so the stream cannot be served"))
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	loc, ok := locale(r)
	if !ok {
		writeError(w, http.StatusBadRequest, "locale is a language tag, such as en or fr-CA")
		return
	}

	// Subscribe before anything is read or sent, so a nudge published while the count is being read is
	// held for the loop and not lost.
	sub, err := s.d.Stream.Subscribe(r.Context(), id)
	if err != nil {
		s.onError(fmt.Errorf("httpapi: subscribe the stream: %w", err))
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	defer func() { _ = sub.Close() }()

	// A stream outlives any write timeout the server was given for ordinary responses.
	_ = http.NewResponseController(w).SetWriteDeadline(time.Time{})
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush() // headers out: the subscription is established, and the client may rely on it

	send := func(event string, v any) bool {
		body, err := json.Marshal(v)
		if err != nil {
			return true
		}
		if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, body); err != nil {
			return false
		}
		flusher.Flush()
		return true
	}

	count, err := s.d.Inbox.Unread(r.Context(), id)
	if err != nil {
		s.lateFault(err)
		return // the client reconnects, and asks again
	}
	if !send("count", toCount(count)) {
		return
	}

	beat := time.NewTicker(s.heartbeat)
	defer beat.Stop()
	seen := map[string]struct{}{}
	for {
		select {
		case <-r.Context().Done():
			return
		case <-beat.C:
			if _, err := fmt.Fprint(w, ": keepalive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case nid, open := <-sub.IDs():
			if !open {
				return
			}
			if _, dup := seen[nid]; dup {
				continue // a re-nudge is only a re-nudge: one id loads one row, however often it is named
			}
			item, err := s.d.Inbox.Get(r.Context(), id, nid, loc)
			if err != nil {
				s.lateFault(err)
				continue
			}
			if item == nil {
				continue // not this caller's, or not there: nothing is shown
			}
			if len(seen) >= seenCap {
				clear(seen)
			}
			seen[nid] = struct{}{}
			unread, err := s.d.Inbox.Unread(r.Context(), id)
			if err != nil {
				s.lateFault(err)
				continue
			}
			if !send("notification", struct {
				Item   itemJSON  `json:"item"`
				Unread countJSON `json:"unread"`
			}{toItem(*item), toCount(unread)}) {
				return
			}
		}
	}
}

// lateFault reports a fault met after the stream began: the status line is gone, so there is nothing to
// tell the caller and the fault is the operator's. A caller that left is no fault.
func (s *server) lateFault(err error) {
	if err != nil && !errors.Is(err, context.Canceled) {
		s.onError(err)
	}
}
