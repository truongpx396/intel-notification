package httpapi

import (
	"net/http"

	"github.com/truongpx396/intel-notification/domain"
)

type tenantJSON struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

type recipientJSON = tenantJSON

func (t tenantJSON) tenant() domain.Tenant { return domain.Tenant{Kind: t.Kind, ID: t.ID} }

// broadcast records a durable broadcast and returns 202 with its id: the audience is expanded by the
// worker, a page at a time, and the caller is not held for it (NR-020). The durable row is the audit
// trail. The body names no realm, and a body that does is refused: the realm is the operator's.
func (s *server) broadcast(w http.ResponseWriter, r *http.Request, _ domain.Realm) {
	var body struct {
		Tenant     tenantJSON        `json:"tenant"`
		Audience   string            `json:"audience"`
		Recipients []recipientJSON   `json:"recipients"`
		Topic      string            `json:"topic"`
		Priority   string            `json:"priority"`
		Data       map[string]any    `json:"data"`
		Title      string            `json:"title"`
		Body       string            `json:"body"`
		Payload    map[string]string `json:"payload"`
		IdemKey    string            `json:"idem_key"`
	}
	if !decode(w, r, &body) {
		return
	}
	// The key is what makes a retry safe. The engine refuses its absence too, and the surface says so
	// itself, so it does not depend on the engine behind it remembering to.
	if body.IdemKey == "" {
		writeError(w, http.StatusUnprocessableEntity, "idem_key is required: it is the broadcast's exactly-once identity, and a retry of this call without it would notify the audience twice")
		return
	}
	b := domain.BroadcastRequest{
		Tenant: body.Tenant.tenant(), Audience: body.Audience, Topic: domain.Topic(body.Topic), Priority: domain.Priority(body.Priority),
		Data: body.Data, Title: body.Title, Body: body.Body, Payload: body.Payload, IdemKey: body.IdemKey,
	}
	for _, rc := range body.Recipients {
		b.Recipients = append(b.Recipients, domain.Recipient{Kind: rc.Kind, ID: rc.ID})
	}
	receipt, err := s.d.Notifier.Broadcast(r.Context(), b)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, struct {
		BroadcastID string `json:"broadcast_id"`
		Applied     bool   `json:"applied"`
	}{receipt.BroadcastID, receipt.Applied})
}

// replay re-drives one dead letter, once: a second call is the same call (D43).
func (s *server) replay(w http.ResponseWriter, r *http.Request, realm domain.Realm) {
	if err := s.d.Admin.ReplayDeadLetter(r.Context(), realm, r.PathValue("id")); err != nil {
		s.fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// erase removes one recipient's personal data everywhere, in the operator's realm, and says what it
// removed (D35).
func (s *server) erase(w http.ResponseWriter, r *http.Request, realm domain.Realm) {
	var body struct {
		Tenant    tenantJSON    `json:"tenant"`
		Recipient recipientJSON `json:"recipient"`
	}
	if !decode(w, r, &body) {
		return
	}
	id := domain.Identity{Realm: realm, Tenant: body.Tenant.tenant(), Recipient: domain.Recipient{Kind: body.Recipient.Kind, ID: body.Recipient.ID}}
	if err := id.Validate(); err != nil {
		writeError(w, http.StatusUnprocessableEntity, "tenant and recipient must each name a kind and an id")
		return
	}
	removed, err := s.d.Admin.Erase(r.Context(), id)
	if err != nil {
		s.fail(w, err)
		return
	}
	if removed == nil {
		removed = map[string]int{}
	}
	writeJSON(w, http.StatusOK, struct {
		Removed map[string]int `json:"removed"`
	}{removed})
}

// tenantPreferences sets a tenant's defaults and locks, in the operator's realm, for the tenant in the
// path. Rows are applied in order and a refusal stops at its row, naming it: each is an upsert, so the
// request is safe to retry once the refused row is fixed.
func (s *server) tenantPreferences(w http.ResponseWriter, r *http.Request, realm domain.Realm) {
	var body []struct {
		Topic   string `json:"topic"`
		Channel string `json:"channel"`
		Enabled *bool  `json:"enabled"`
		Locked  bool   `json:"locked"`
	}
	if !decode(w, r, &body) {
		return
	}
	tenant := domain.Tenant{Kind: r.PathValue("tenant_kind"), ID: r.PathValue("tenant_id")}
	for i, row := range body {
		if row.Enabled == nil {
			writeError(w, http.StatusBadRequest, "enabled is required on every row")
			return
		}
		err := s.d.Admin.SetTenantPreference(r.Context(), realm, domain.TenantPreference{
			Tenant: tenant, Topic: domain.Topic(row.Topic), Channel: domain.ChannelKind(row.Channel), Enabled: *row.Enabled, Locked: row.Locked,
		})
		if err != nil {
			if isRefusal(err) {
				writeJSON(w, http.StatusUnprocessableEntity, errorBody{Error: err.Error(), Row: &i})
				return
			}
			s.fail(w, err)
			return
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

// putAddresses replaces one recipient's addresses on one channel, in the operator's realm: the REST twin of
// Catalog.PutAddresses, for operators and the e2e suite. The recipient and tenant are the body's, because they
// are the operator's data; a body that names a realm is refused, because the realm is the credential's
// (NR-009).
func (s *server) putAddresses(w http.ResponseWriter, r *http.Request, realm domain.Realm) {
	var body struct {
		Tenant    tenantJSON    `json:"tenant"`
		Recipient recipientJSON `json:"recipient"`
		Channel   string        `json:"channel"`
		Addresses []struct {
			Value    string            `json:"value"`
			Locale   string            `json:"locale"`
			Timezone string            `json:"timezone"`
			Meta     map[string]string `json:"meta"`
		} `json:"addresses"`
	}
	if !decode(w, r, &body) {
		return
	}
	id := domain.Identity{Realm: realm, Tenant: body.Tenant.tenant(), Recipient: domain.Recipient{Kind: body.Recipient.Kind, ID: body.Recipient.ID}}
	addrs := make([]domain.Address, len(body.Addresses))
	for i, a := range body.Addresses {
		addrs[i] = domain.Address{Value: a.Value, Locale: a.Locale, Timezone: a.Timezone, Meta: a.Meta}
	}
	stored, err := s.d.Catalog.PutAddresses(r.Context(), id, domain.ChannelKind(body.Channel), addrs)
	if err != nil {
		s.fail(w, err)
		return
	}
	type out struct {
		Value    string            `json:"value"`
		Locale   string            `json:"locale,omitempty"`
		Timezone string            `json:"timezone,omitempty"`
		Meta     map[string]string `json:"meta,omitempty"`
	}
	resp := make([]out, len(stored))
	for i, a := range stored {
		resp[i] = out{Value: a.Value, Locale: a.Locale, Timezone: a.Timezone, Meta: a.Meta}
	}
	writeJSON(w, http.StatusOK, struct {
		Addresses []out `json:"addresses"`
	}{resp})
}
