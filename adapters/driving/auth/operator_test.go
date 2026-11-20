package auth_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/truongpx396/intel-notification/adapters/driving/auth"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/notifytest"
	"github.com/truongpx396/intel-notification/ports"
)

// T049a, NR-009, D31. An operator's token administers one realm, and a request names none.

func TestAnOperatorTokenAdministersTheRealmItWasIssuedFor(t *testing.T) {
	t.Parallel()
	ops, err := auth.NewOperators(map[string]domain.Realm{"token-for-alpha": "alpha", "token-for-beta": "beta"})
	if err != nil {
		t.Fatal(err)
	}
	for token, want := range map[string]domain.Realm{"token-for-alpha": "alpha", "token-for-beta": "beta"} {
		r := httptest.NewRequest(http.MethodPost, "/admin/erasure", nil)
		r.Header.Set("Authorization", "Bearer "+token)
		if got, err := ops.Authenticate(r); err != nil || got != want {
			t.Errorf("%s: Authenticate = %q, %v, want %q", token, got, err, want)
		}
	}
	for name, header := range map[string]string{
		"no header": "", "a wrong token": "Bearer nope", "a prefix of a token": "Bearer token-for-alph",
		"a token and more": "Bearer token-for-alpha2", "another scheme": "Basic dG9rZW4tZm9yLWFscGhh", "no scheme": "token-for-alpha", "an empty bearer": "Bearer ",
	} {
		r := httptest.NewRequest(http.MethodPost, "/admin/erasure", nil)
		if header != "" {
			r.Header.Set("Authorization", header)
		}
		if got, err := ops.Authenticate(r); err == nil || got != "" {
			t.Errorf("%s: Authenticate = %q, %v, want a refusal and no realm", name, got, err)
		}
	}
	// A realm in the request is not read.
	r := httptest.NewRequest(http.MethodPost, "/admin/erasure?realm=beta", nil)
	r.Header.Set("Authorization", "Bearer token-for-alpha")
	r.Header.Set("X-Notify-Realm", "beta")
	if got, _ := ops.Authenticate(r); got != "alpha" {
		t.Errorf("Authenticate with a realm in the query and a header = %q, want alpha: the realm is the token's", got)
	}
}

func TestOperatorsRefuseTokensThatCouldNotBeTold(t *testing.T) {
	t.Parallel()
	if _, err := auth.NewOperators(map[string]domain.Realm{"": "alpha"}); err == nil {
		t.Error("an empty token was accepted: it would authenticate a request that sent none")
	}
	if _, err := auth.NewOperators(map[string]domain.Realm{"t": "Not A Realm"}); err == nil {
		t.Error("a token for a realm that is no realm was accepted")
	}
	// No operators at all authenticates no one.
	ops, err := auth.NewOperators(nil)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodGet, "/admin/x", nil)
	r.Header.Set("Authorization", "Bearer anything")
	if got, err := ops.Authenticate(r); err == nil || got != "" {
		t.Errorf("Authenticate with no tokens configured = %q, %v, want a refusal", got, err)
	}
}

func TestARoutedNotifierSendsEachCallToTheEngineOfTheRealmInItsContext(t *testing.T) {
	t.Parallel()
	engines := map[domain.Realm]*notifytest.ScriptedNotifier{"alpha": {}, "beta": {}}
	n := &auth.RoutedNotifier{Route: auth.Route(map[domain.Realm]ports.Notifier{"alpha": engines["alpha"], "beta": engines["beta"]})}
	b := domain.BroadcastRequest{Topic: "t", IdemKey: "k"}
	if _, err := n.Broadcast(auth.WithRealm(context.Background(), "beta"), b); err != nil {
		t.Fatal(err)
	}
	if _, err := n.Notify(auth.WithRealm(context.Background(), "alpha"), domain.Notification{IdemKey: "n"}); err != nil {
		t.Fatal(err)
	}
	if len(engines["beta"].Broadcasted()) != 1 || len(engines["alpha"].Broadcasted()) != 0 || len(engines["alpha"].Notified()) != 1 || len(engines["beta"].Notified()) != 0 {
		t.Fatalf("beta broadcasts %d, alpha broadcasts %d, alpha notifies %d, beta notifies %d, want each call in its own realm's engine",
			len(engines["beta"].Broadcasted()), len(engines["alpha"].Broadcasted()), len(engines["alpha"].Notified()), len(engines["beta"].Notified()))
	}
	if _, err := n.Broadcast(context.Background(), b); err == nil {
		t.Error("a call whose context carries no realm was routed: it must be refused, and never served by another realm's engine")
	}
	if _, err := n.NotifyTx(auth.WithRealm(context.Background(), "alpha"), nil, domain.Notification{}); !errors.Is(err, domain.ErrTxUnsupported) {
		t.Errorf("NotifyTx = %v, want ErrTxUnsupported", err)
	}
}
