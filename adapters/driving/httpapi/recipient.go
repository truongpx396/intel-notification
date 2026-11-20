package httpapi

import (
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/truongpx396/intel-notification/domain"
)

// localePattern is what a language tag looks like. A value that is not one never reaches the renderer,
// whose template keys it would otherwise become.
var localePattern = regexp.MustCompile(`^[A-Za-z]{1,8}(-[A-Za-z0-9]{1,8})*$`)

// preferredLocale is the highest-weighted language tag of an Accept-Language header, or "" when it names
// none a template could be keyed by: a wildcard, a tag that is refused (q=0) and a value that is not a
// tag are skipped.
func preferredLocale(header string) string {
	best, bestQ := "", 0.0
	for part := range strings.SplitSeq(header, ",") {
		tag, q := part, 1.0
		if i := strings.IndexByte(part, ';'); i >= 0 {
			tag = part[:i]
			if params := strings.TrimSpace(part[i+1:]); strings.HasPrefix(params, "q=") {
				if v, err := strconv.ParseFloat(strings.TrimPrefix(params, "q="), 64); err == nil {
					q = v
				}
			}
		}
		tag = strings.TrimSpace(tag)
		if q <= 0 || !localePattern.MatchString(tag) {
			continue
		}
		if q > bestQ {
			best, bestQ = tag, q
		}
	}
	return best
}

// locale is the viewer's: the query's, which must be a language tag, or the best of Accept-Language.
func locale(r *http.Request) (loc string, ok bool) {
	if q := r.URL.Query().Get("locale"); q != "" {
		return q, localePattern.MatchString(q)
	}
	return preferredLocale(r.Header.Get("Accept-Language")), true
}

// ---------------------------------------------------------------------------------------- items

type itemJSON struct {
	ID         string            `json:"id"`
	CreatedAt  *time.Time        `json:"created_at,omitempty"`
	Topic      string            `json:"topic"`
	Priority   string            `json:"priority,omitempty"`
	Title      string            `json:"title"`
	Body       string            `json:"body"`
	Data       map[string]any    `json:"data,omitempty"`
	Payload    map[string]string `json:"payload,omitempty"`
	SeenAt     *time.Time        `json:"seen_at,omitempty"`
	ReadAt     *time.Time        `json:"read_at,omitempty"`
	ArchivedAt *time.Time        `json:"archived_at,omitempty"`
}

// instant is an unset time left out, and a set one in UTC.
func instant(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	u := t.UTC()
	return &u
}

func toItem(i domain.InboxItem) itemJSON {
	return itemJSON{
		ID: i.ID, CreatedAt: instant(i.CreatedAt), Topic: string(i.Topic), Priority: string(i.Priority), Title: i.Title, Body: i.Body,
		Data: i.Data, Payload: i.Payload, SeenAt: instant(i.SeenAt), ReadAt: instant(i.ReadAt), ArchivedAt: instant(i.ArchivedAt),
	}
}

type countJSON struct {
	Count  int  `json:"count"`
	Capped bool `json:"capped"`
}

func toCount(c domain.UnreadCount) countJSON { return countJSON{Count: c.Count, Capped: c.Capped} }

// ---------------------------------------------------------------------------------------- routes

func (s *server) list(w http.ResponseWriter, r *http.Request, id domain.Identity) {
	q := domain.ListQuery{Cursor: r.URL.Query().Get("cursor"), Topic: domain.Topic(r.URL.Query().Get("topic"))}
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			writeError(w, http.StatusBadRequest, "limit is a number of items, zero or more")
			return
		}
		q.Limit = n
	}
	switch state := domain.InboxState(r.URL.Query().Get("state")); state {
	case "", domain.InboxUnread, domain.InboxRead, domain.InboxArchived:
		q.State = state
	default:
		writeError(w, http.StatusBadRequest, "state is unread, read or archived")
		return
	}
	loc, ok := locale(r)
	if !ok {
		writeError(w, http.StatusBadRequest, "locale is a language tag, such as en or fr-CA")
		return
	}
	q.Locale = loc

	page, err := s.d.Inbox.List(r.Context(), id, q)
	if err != nil {
		s.fail(w, err)
		return
	}
	out := struct {
		Items      []itemJSON `json:"items"`
		NextCursor string     `json:"next_cursor,omitempty"`
	}{Items: make([]itemJSON, 0, len(page.Items)), NextCursor: page.NextCursor}
	for _, it := range page.Items {
		out.Items = append(out.Items, toItem(it))
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *server) unread(w http.ResponseWriter, r *http.Request, id domain.Identity) {
	c, err := s.d.Inbox.Unread(r.Context(), id)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toCount(c))
}

func (s *server) seen(w http.ResponseWriter, r *http.Request, id domain.Identity) {
	var body struct {
		Before *time.Time `json:"before"`
	}
	if !decode(w, r, &body) {
		return
	}
	if body.Before == nil {
		writeError(w, http.StatusBadRequest, "before is required: the instant the bell was cleared at")
		return
	}
	if err := s.d.Inbox.MarkSeen(r.Context(), id, *body.Before); err != nil {
		s.fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) read(w http.ResponseWriter, r *http.Request, id domain.Identity) {
	if err := s.d.Inbox.MarkRead(r.Context(), id, []string{r.PathValue("id")}); err != nil {
		s.fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) readAll(w http.ResponseWriter, r *http.Request, id domain.Identity) {
	if err := s.d.Inbox.MarkAllRead(r.Context(), id); err != nil {
		s.fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) archive(w http.ResponseWriter, r *http.Request, id domain.Identity) {
	var body struct {
		Archived *bool `json:"archived"`
	}
	if !decode(w, r, &body) {
		return
	}
	if body.Archived == nil {
		writeError(w, http.StatusBadRequest, "archived is required: true to put it away, false to bring it back")
		return
	}
	if err := s.d.Inbox.Archive(r.Context(), id, []string{r.PathValue("id")}, *body.Archived); err != nil {
		s.fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ------------------------------------------------------------------------- preferences, schedule

func (s *server) preferences(w http.ResponseWriter, r *http.Request, id domain.Identity) {
	prefs, err := s.d.Inbox.Preferences(r.Context(), id)
	if err != nil {
		s.fail(w, err)
		return
	}
	type pref struct {
		Topic   string `json:"topic"`
		Channel string `json:"channel"`
		Enabled bool   `json:"enabled"`
		Source  string `json:"source"`
		Locked  bool   `json:"locked"`
	}
	out := make([]pref, 0, len(prefs))
	for _, p := range prefs {
		out = append(out, pref{Topic: string(p.Topic), Channel: string(p.Channel), Enabled: p.Enabled, Source: string(p.Source), Locked: p.Locked})
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *server) setPreferences(w http.ResponseWriter, r *http.Request, id domain.Identity) {
	var body []struct {
		Topic   string `json:"topic"`
		Channel string `json:"channel"`
		Enabled *bool  `json:"enabled"`
	}
	if !decode(w, r, &body) {
		return
	}
	prefs := make([]domain.Preference, 0, len(body))
	for i, p := range body {
		if p.Enabled == nil {
			writeError(w, http.StatusBadRequest, "row "+strconv.Itoa(i)+": enabled is required: a switch is on or off")
			return
		}
		prefs = append(prefs, domain.Preference{Topic: domain.Topic(p.Topic), Channel: domain.ChannelKind(p.Channel), Enabled: *p.Enabled})
	}
	if err := s.d.Inbox.SetPreferences(r.Context(), id, prefs); err != nil {
		s.fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// scheduleJSON is a recipient's quiet hours and digest cadence. Digest is a Go duration such as "30m",
// and "0s" means immediate.
type scheduleJSON struct {
	QuietStart string `json:"quiet_start"`
	QuietEnd   string `json:"quiet_end"`
	Timezone   string `json:"timezone"`
	Digest     string `json:"digest"`
	// CriticalIgnoresQuietHours restates, in every answer, that a critical notification is delivered
	// whatever the hour, so a client cannot present quiet hours as a promise they are not (D9).
	CriticalIgnoresQuietHours *bool `json:"critical_ignores_quiet_hours,omitempty"`
}

func toSchedule(s domain.DeliverySchedule) scheduleJSON {
	yes := true
	return scheduleJSON{QuietStart: s.QuietStart, QuietEnd: s.QuietEnd, Timezone: s.Timezone, Digest: s.Digest.String(), CriticalIgnoresQuietHours: &yes}
}

func (s *server) schedule(w http.ResponseWriter, r *http.Request, id domain.Identity) {
	sched, err := s.d.Inbox.Schedule(r.Context(), id)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toSchedule(sched))
}

func (s *server) setSchedule(w http.ResponseWriter, r *http.Request, id domain.Identity) {
	var body scheduleJSON
	if !decode(w, r, &body) {
		return
	}
	sched := domain.DeliverySchedule{QuietStart: body.QuietStart, QuietEnd: body.QuietEnd, Timezone: body.Timezone}
	if body.Digest != "" {
		d, err := time.ParseDuration(body.Digest)
		if err != nil || d < 0 {
			writeError(w, http.StatusBadRequest, `digest is a duration such as "30m" or "1h", and "0s" for immediate`)
			return
		}
		sched.Digest = d
	}
	if err := s.d.Inbox.SetSchedule(r.Context(), id, sched); err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toSchedule(sched))
}
