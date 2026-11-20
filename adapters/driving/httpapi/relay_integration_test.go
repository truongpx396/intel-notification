//go:build integration

package httpapi_test

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/truongpx396/intel-notification/adapters/driven/channel/inapp"
	"github.com/truongpx396/intel-notification/adapters/driven/redis"
	"github.com/truongpx396/intel-notification/adapters/driving/httpapi"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/notifytest"
	"github.com/truongpx396/intel-notification/ports"
)

func TestMain(m *testing.M) { os.Exit(notifytest.Main(m, notifytest.WithRedis())) }

// sessionToken is a stand-in for a host's session layer: the identity, signed by nobody. The token
// verification of service mode is T050's; what is under test here is the relay, which trusts whatever
// its IdentityFunc says.
func sessionToken(id domain.Identity) string {
	raw, _ := json.Marshal(id)
	return base64.RawURLEncoding.EncodeToString(raw)
}

func identityOf(r *http.Request) (domain.Identity, error) {
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
	if err != nil {
		return domain.Identity{}, err
	}
	var id domain.Identity
	if err := json.Unmarshal(raw, &id); err != nil {
		return domain.Identity{}, errors.New("not a session")
	}
	return id, nil
}

// relayWorld builds a world over the Env's real PostgreSQL, the real in-app channel, and a stream that
// is Redis when the Env has one (sharded pub/sub on hash-tagged keys) and the kit's in-memory one when
// it does not. The relay is the real surface over the real, RLS-scoped inbox.
func relayWorld(opts ...notifytest.Option) func(t *testing.T) *notifytest.RelayWorld {
	return func(t *testing.T) *notifytest.RelayWorld {
		t.Helper()
		e := notifytest.NewEnv(t, opts...)
		var (
			pub ports.StreamPublisher
			sub ports.StreamSubscriber
		)
		if e.Redis != nil {
			s := redis.NewStream(e.Redis.Client)
			pub, sub = s, s
		} else {
			s := notifytest.NewStream(e.Probe)
			pub, sub = s, s
		}
		e.Channels.Register(inapp.New(pub))
		h, err := httpapi.New(httpapi.Deps{Inbox: e.InboxUseCase(t), Stream: sub, Identify: identityOf})
		if err != nil {
			t.Fatal(err)
		}
		srv := httptest.NewServer(h)
		t.Cleanup(srv.Close)
		return &notifytest.RelayWorld{Env: e, Relay: notifytest.NewRelay(srv.URL, sessionToken), Publisher: pub}
	}
}

// StreamIsolationContract (T047, NS-001) against the relay over the in-memory stream...
func TestStreamIsolationContract(t *testing.T) {
	t.Parallel()
	notifytest.StreamIsolationContract(t, relayWorld())
}

// ...and over Redis: sharded pub/sub on hash-tagged keys, the stream a deployment uses.
func TestStreamIsolationContractOnRedis(t *testing.T) {
	t.Parallel()
	notifytest.StreamIsolationContract(t, relayWorld(notifytest.WithRedis()))
}
