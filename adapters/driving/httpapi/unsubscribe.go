package httpapi

import (
	"errors"
	"html/template"
	"net/http"

	"github.com/truongpx396/intel-notification/domain"
)

// One-click unsubscribe, RFC 8058 (D34). A mail scanner or a link previewer fetches every URL in a
// message, so a GET changes nothing: it shows a page whose button posts. The POST is the one-click, and
// is the confirmation page's button. The token is read from the link and from nowhere else, and is the
// whole credential: the click comes from a mail client and has no session.

var confirmPage = template.Must(template.New("confirm").Parse(`<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Unsubscribe</title></head><body>
<h1>Unsubscribe from these emails?</h1>
<p>You will stop receiving this kind of notification by email. Your other notifications are not affected.</p>
<form method="post" action="?token={{.Token}}"><button type="submit">Unsubscribe</button></form>
</body></html>
`))

var donePage = template.Must(template.New("done").Parse(`<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Unsubscribed</title></head><body>
<h1>You have been unsubscribed.</h1>
<p>You will no longer receive this kind of notification by email. You can change this at any time in your <a href="{{.PreferencesURL}}">notification settings</a>.</p>
</body></html>
`))

var gonePage = template.Must(template.New("gone").Parse(`<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>This link has expired</title></head><body>
<h1>This unsubscribe link has expired or is not valid.</h1>
<p>You can still choose which notifications you receive in your <a href="{{.PreferencesURL}}">notification settings</a>.</p>
</body></html>
`))

// page writes an HTML page that holds nothing a scanner could run, and nothing a browser should keep:
// the token is in the URL, so the page must not leak it through a Referer.
func page(w http.ResponseWriter, status int, t *template.Template, data any) {
	h := w.Header()
	h.Set("Content-Type", "text/html; charset=utf-8")
	h.Set("Cache-Control", "no-store")
	h.Set("Referrer-Policy", "no-referrer")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'")
	w.WriteHeader(status)
	_ = t.Execute(w, data)
}

func (s *server) pageData(token string) struct{ Token, PreferencesURL string } {
	return struct{ Token, PreferencesURL string }{Token: token, PreferencesURL: s.prefsURL}
}

// unsubscribeConfirm is the page a link opens to. It makes no call to the preference writer, nor to
// anything else: it does not even look at the token beyond requiring one, because looking is the engine's
// to do on the POST.
func (s *server) unsubscribeConfirm(w http.ResponseWriter, r *http.Request) {
	token := r.URL.Query().Get("token")
	if token == "" {
		page(w, http.StatusGone, gonePage, s.pageData(""))
		return
	}
	page(w, http.StatusOK, confirmPage, s.pageData(token))
}

// unsubscribe disables exactly the one pair the token names. An invalid or expired token is a 410 page
// with a route to the recipient's preferences.
func (s *server) unsubscribe(w http.ResponseWriter, r *http.Request) {
	token := r.URL.Query().Get("token")
	if token == "" {
		page(w, http.StatusGone, gonePage, s.pageData(""))
		return
	}
	err := s.d.Inbox.Unsubscribe(r.Context(), token)
	switch {
	case err == nil:
		page(w, http.StatusOK, donePage, s.pageData(""))
	case errors.Is(err, domain.ErrInvalidUnsubscribeToken), errors.Is(err, domain.ErrExpiredUnsubscribeToken):
		page(w, http.StatusGone, gonePage, s.pageData(""))
	default:
		s.fail(w, err)
	}
}
