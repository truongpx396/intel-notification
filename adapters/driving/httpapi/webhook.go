package httpapi

import (
	"errors"
	"io"
	"net/http"

	"github.com/truongpx396/intel-notification/domain"
)

// webhook receives a provider's callback: a bounce, a complaint, a delivery status (NR-032, D14).
//
// The signature is verified over the raw bytes before anything is parsed, and a body that does not
// verify is refused with an empty 401 and never read as data. A forged bounce would otherwise suppress
// a real address, a denial of notification that looks like normal operation. This surface never parses
// a callback at all: it hands the verified bytes to the provider's endpoint, which does.
func (s *server) webhook(w http.ResponseWriter, r *http.Request) {
	hook := s.d.Webhooks[r.PathValue("channel")+"/"+r.PathValue("provider")]
	if hook == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxWebhook))
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			w.WriteHeader(http.StatusRequestEntityTooLarge)
		} else {
			w.WriteHeader(http.StatusBadRequest)
		}
		return
	}
	if err := hook.Verify(r.Header, body); err != nil {
		unauthorizedEmpty(w)
		return
	}
	switch err := hook.Handle(r.Context(), body); {
	case err == nil:
		w.WriteHeader(http.StatusNoContent)
	case errors.Is(err, domain.ErrUnknownDelivery):
		// The callback can beat the delivery's own record. A non-2xx makes the provider try again.
		w.WriteHeader(http.StatusNotFound)
	default:
		s.onError(err)
		w.WriteHeader(http.StatusInternalServerError)
	}
}

// unauthorizedEmpty is a 401 with no body and no challenge: a provider is not a user agent.
func unauthorizedEmpty(w http.ResponseWriter) { w.WriteHeader(http.StatusUnauthorized) }
