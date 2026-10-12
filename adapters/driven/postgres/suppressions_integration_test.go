//go:build integration

package postgres

import (
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/truongpx396/intel-notification/domain"
)

// T019: the suppression store (NR-006, NR-034, D13, D14, D35). A suppression is the
// one thing that outlives the recipient it was recorded for, so what it keeps and
// what it can hold are asserted against the database's own catalog, not the code's.

// hashOf is the key the dispatcher looks an address up by.
func hashOf(ch domain.ChannelKind, address string) []byte { return domain.SuppressionHash(ch, address) }

func (e *env) suppressed(realm domain.Realm, ch domain.ChannelKind, h []byte) bool {
	e.t.Helper()
	got, err := e.sup.Suppressed(e.t.Context(), realm, ch, h)
	if err != nil {
		e.t.Fatalf("Suppressed: %v", err)
	}
	return got
}

func (e *env) suppress(realm domain.Realm, ch domain.ChannelKind, h []byte, reason domain.SuppressionReason, until time.Time) {
	e.t.Helper()
	if err := e.sup.Suppress(e.t.Context(), realm, ch, h, reason, until); err != nil {
		e.t.Fatalf("Suppress: %v", err)
	}
}

// storedSuppression reads the one row for a key as the superuser: its reason, and
// when it expires (nil for never).
func (e *env) storedSuppression(realm domain.Realm, ch domain.ChannelKind, h []byte) (reason string, expires *time.Time) {
	e.t.Helper()
	if err := e.db.Admin.QueryRow(e.t.Context(),
		`SELECT reason, expires_at FROM channel_suppressions WHERE realm = $1 AND channel = $2 AND address_hash = $3`,
		string(realm), string(ch), h).Scan(&reason, &expires); err != nil {
		e.t.Fatalf("stored suppression: %v", err)
	}
	return reason, expires
}

// NR-006, D13, D14. A live suppression is found by the hash of its address; an
// expired one is not; zero is permanent.
func TestSuppressionIsLiveUntilItExpires(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	h := hashOf("email", "alice@example.com")

	if e.suppressed("aisat", "email", h) {
		t.Fatal("an address nobody suppressed reads as suppressed")
	}
	until := e.clock.Now().Add(time.Hour)
	e.suppress("aisat", "email", h, domain.SuppressSoftBounce, until)

	for _, step := range []struct {
		name string
		at   time.Time
		want bool
	}{
		{"just recorded", e.clock.Now(), true},
		{"one nanosecond before it expires", until.Add(-time.Nanosecond), true},
		{"at the instant it expires", until, false},
		{"long after", until.Add(24 * time.Hour), false},
	} {
		e.clock.Set(step.at)
		if got := e.suppressed("aisat", "email", h); got != step.want {
			t.Errorf("%s: Suppressed = %v, want %v", step.name, got, step.want)
		}
	}

	t.Run("zero is permanent", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t)
		e.suppress("aisat", "email", h, domain.SuppressHardBounce, time.Time{})
		if _, expires := e.storedSuppression("aisat", "email", h); expires != nil {
			t.Fatalf("a permanent suppression expires at %v", expires)
		}
		e.clock.Advance(100 * 365 * 24 * time.Hour)
		if !e.suppressed("aisat", "email", h) {
			t.Fatal("a permanent suppression lapsed")
		}
	})

	t.Run("a suppression recorded already expired does not suppress", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t)
		e.suppress("aisat", "email", h, domain.SuppressSoftBounce, e.clock.Now().Add(-time.Hour))
		if e.suppressed("aisat", "email", h) {
			t.Fatal("a suppression that ended before it began suppresses")
		}
	})
}

// D13. A suppression is for one address on one channel in one realm: the same hash
// on another channel or in another realm is a different address.
func TestSuppressionsAreKeptPerChannelAndRealm(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	h := hashOf("email", "alice@example.com")
	e.suppress("aisat", "email", h, domain.SuppressComplaint, time.Time{})

	for _, tc := range []struct {
		name  string
		realm domain.Realm
		ch    domain.ChannelKind
		hash  []byte
		want  bool
	}{
		{"the same address, channel and realm", "aisat", "email", h, true},
		{"the same hash on another channel", "aisat", "sms", h, false},
		{"the same hash and channel in another realm", "other", "email", h, false},
		{"another address", "aisat", "email", hashOf("email", "bob@example.com"), false},
		{"the same address text on another channel hashes apart", "aisat", "sms", hashOf("sms", "alice@example.com"), false},
	} {
		if got := e.suppressed(tc.realm, tc.ch, tc.hash); got != tc.want {
			t.Errorf("%s: Suppressed = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// Recording the same suppression twice is one row, and a later record can only
// make a suppression stronger: a complaint is not forgiven because a soft bounce
// came after it.
func TestSuppressMergesWithWhatIsThere(t *testing.T) {
	t.Parallel()
	h := hashOf("email", "alice@example.com")
	hour := time.Hour
	cases := []struct {
		name        string
		first       time.Duration // after the clock, 0 for permanent
		firstReason domain.SuppressionReason
		later       time.Duration
		laterReason domain.SuppressionReason
		wantExpires time.Duration // 0 for permanent
		wantReason  domain.SuppressionReason
		why         string
	}{
		{"the same record twice", hour, domain.SuppressSoftBounce, hour, domain.SuppressSoftBounce, hour, domain.SuppressSoftBounce, ""},
		{"a permanent suppression stays permanent after a soft bounce", 0, domain.SuppressComplaint, hour, domain.SuppressSoftBounce, 0, domain.SuppressComplaint, "a stale soft bounce must not undo a complaint, or we mail someone who complained"},
		{"an expiring suppression becomes permanent", hour, domain.SuppressSoftBounce, 0, domain.SuppressHardBounce, 0, domain.SuppressHardBounce, ""},
		{"a later expiry extends it", hour, domain.SuppressSoftBounce, 3 * hour, domain.SuppressSoftBounce, 3 * hour, domain.SuppressSoftBounce, ""},
		{"an earlier expiry does not shorten it", 3 * hour, domain.SuppressSoftBounce, hour, domain.SuppressSoftBounce, 3 * hour, domain.SuppressSoftBounce, ""},
		{"the latest reason of two expiring records wins", hour, domain.SuppressSoftBounce, hour, domain.SuppressInvalidAddress, hour, domain.SuppressInvalidAddress, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			at := func(d time.Duration) time.Time {
				if d == 0 {
					return time.Time{}
				}
				return e.clock.Now().Add(d)
			}
			e.suppress("aisat", "email", h, tc.firstReason, at(tc.first))
			e.suppress("aisat", "email", h, tc.laterReason, at(tc.later))

			if n := e.count(`SELECT count(*) FROM channel_suppressions`); n != 1 {
				t.Fatalf("%d rows for one address, want 1", n)
			}
			reason, expires := e.storedSuppression("aisat", "email", h)
			if reason != string(tc.wantReason) {
				t.Errorf("reason = %q, want %q: %s", reason, tc.wantReason, tc.why)
			}
			switch {
			case tc.wantExpires == 0 && expires != nil:
				t.Errorf("expires at %v, want never: %s", expires, tc.why)
			case tc.wantExpires != 0 && (expires == nil || !expires.Equal(at(tc.wantExpires))):
				t.Errorf("expires at %v, want %v", expires, at(tc.wantExpires))
			}
		})
	}

	t.Run("a suppression that lapsed is renewed by a new record", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t)
		e.suppress("aisat", "email", h, domain.SuppressSoftBounce, e.clock.Now().Add(time.Hour))
		e.clock.Advance(2 * time.Hour)
		if e.suppressed("aisat", "email", h) {
			t.Fatal("setup: the suppression should have lapsed")
		}
		e.suppress("aisat", "email", h, domain.SuppressInvalidAddress, e.clock.Now().Add(time.Hour))
		if !e.suppressed("aisat", "email", h) {
			t.Fatal("a new record did not renew a lapsed suppression")
		}
	})

	t.Run("concurrent records of one address are one row", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t)
		var wg sync.WaitGroup
		errs := make(chan error, 16)
		for i := range 16 {
			wg.Go(func() {
				reason := domain.SuppressSoftBounce
				var until time.Time
				if i%2 == 0 {
					reason, until = domain.SuppressComplaint, time.Time{}
				} else {
					until = e.clock.Now().Add(time.Duration(i) * time.Hour)
				}
				errs <- e.sup.Suppress(t.Context(), "aisat", "email", h, reason, until)
			})
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatalf("a concurrent Suppress failed: %v", err)
			}
		}
		if n := e.count(`SELECT count(*) FROM channel_suppressions`); n != 1 {
			t.Fatalf("%d rows after 16 writers on one address", n)
		}
		if _, expires := e.storedSuppression("aisat", "email", h); expires != nil {
			t.Fatalf("expires at %v: a permanent record was among the writers, so it must stay permanent", expires)
		}
	})
}

// NR-034, D35. A suppression holds a hash and a reason, and cannot hold an address:
// found by reading the column list from the catalog and scanning what was stored,
// not from anything the code says. The address "alice@example.com" is hashed
// by the caller and never reaches the store.
func TestSuppressionHoldsNoPlaintextAddress(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	const address = "alice@example.com"
	e.suppress("aisat", "email", hashOf("email", address), domain.SuppressHardBounce, time.Time{})
	e.suppress("aisat", "sms", hashOf("sms", "+14155550100"), domain.SuppressSoftBounce, e.clock.Now().Add(time.Hour))

	rows, err := e.db.Admin.Query(t.Context(), `
SELECT column_name, data_type FROM information_schema.columns
 WHERE table_schema = 'public' AND table_name = 'channel_suppressions' ORDER BY ordinal_position`)
	if err != nil {
		t.Fatal(err)
	}
	var columns []string
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			t.Fatal(err)
		}
		columns = append(columns, name+" "+typ)
	}
	rows.Close()
	want := []string{"realm text", "channel text", "address_hash bytea", "reason text", "detail text",
		"created_at timestamp with time zone", "expires_at timestamp with time zone"}
	if !slices.Equal(columns, want) {
		t.Errorf("columns = %v\nwant     %v: a new column is a place an address could be kept", columns, want)
	}

	// Every row, every column, as text: no address, no part of one.
	dump, err := e.db.Admin.Query(t.Context(), `SELECT t::text FROM channel_suppressions t`)
	if err != nil {
		t.Fatal(err)
	}
	defer dump.Close()
	n := 0
	for dump.Next() {
		var row string
		if err := dump.Scan(&row); err != nil {
			t.Fatal(err)
		}
		n++
		for _, leak := range []string{address, "alice", "example.com", "+14155550100", "4155550100"} {
			if strings.Contains(strings.ToLower(row), leak) {
				t.Errorf("the row %q contains %q", row, leak)
			}
		}
	}
	if n != 2 {
		t.Fatalf("scanned %d rows, want 2", n)
	}

	var hashLen int
	var detail *string
	if err := e.db.Admin.QueryRow(t.Context(),
		`SELECT octet_length(address_hash), detail FROM channel_suppressions WHERE channel = 'email'`).Scan(&hashLen, &detail); err != nil {
		t.Fatal(err)
	}
	if hashLen != 32 || detail != nil {
		t.Errorf("hash is %d bytes and detail = %v: want the 32-byte hash and no diagnostics from this port", hashLen, detail)
	}
}

// NR-034, D35. Erasing a recipient removes their personal data and keeps what
// stops someone who complained from being mailed again.
func TestSuppressionSurvivesErasure(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	id := ident("w1", "alice")
	h := hashOf("email", "alice@example.com")
	e.suppress("aisat", "email", h, domain.SuppressComplaint, time.Time{})
	nid := e.notification(id, "k")
	e.enqueue(row{id: id, notification: nid, shard: 1, channel: "email"})

	counts, err := e.s.Erase(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	if counts["notifications"] != 1 || counts["notification_outbox"] != 1 {
		t.Fatalf("Erase removed %v, want the recipient's rows gone", counts)
	}
	if !e.suppressed("aisat", "email", h) {
		t.Fatal("erasing the recipient lifted the suppression, so a complaint resumes the mail")
	}
	if n := e.count(`SELECT count(*) FROM channel_suppressions`); n != 1 {
		t.Fatalf("%d suppressions after Erase, want the one kept", n)
	}
}

// What the store will not take.
func TestSuppressRefuses(t *testing.T) {
	t.Parallel()
	good := hashOf("email", "alice@example.com")
	cases := []struct {
		name   string
		realm  domain.Realm
		ch     domain.ChannelKind
		hash   []byte
		reason domain.SuppressionReason
		text   string
	}{
		{"no realm", "", "email", good, domain.SuppressHardBounce, "realm"},
		{"no channel", "aisat", "", good, domain.SuppressHardBounce, "channel"},
		{"a hash that is too short", "aisat", "email", good[:16], domain.SuppressHardBounce, "32 bytes"},
		{"a hash that is too long", "aisat", "email", append(slices.Clone(good), 0), domain.SuppressHardBounce, "32 bytes"},
		{"no hash", "aisat", "email", nil, domain.SuppressHardBounce, "32 bytes"},
		{"an unknown reason", "aisat", "email", good, "annoyed", "reason"},
		{"no reason", "aisat", "email", good, "", "reason"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			e.suppress("aisat", "sms", hashOf("sms", "+14155550100"), domain.SuppressHardBounce, time.Time{}) // a control: a good call is accepted
			err := e.sup.Suppress(t.Context(), tc.realm, tc.ch, tc.hash, tc.reason, time.Time{})
			if err == nil || !strings.Contains(err.Error(), tc.text) {
				t.Fatalf("err = %v, want a refusal that says %q, from the store and not from a constraint", err, tc.text)
			}
			if n := e.count(`SELECT count(*) FROM channel_suppressions`); n != 1 {
				t.Fatalf("%d rows after a refused call, want only the control", n)
			}
		})
	}

	t.Run("Suppressed refuses a malformed key too", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t)
		for name, args := range map[string]struct {
			realm domain.Realm
			ch    domain.ChannelKind
			hash  []byte
		}{"a short hash": {"aisat", "email", good[:8]}, "no channel": {"aisat", "", good}, "no realm": {"", "email", good}} {
			if _, err := e.sup.Suppressed(t.Context(), args.realm, args.ch, args.hash); err == nil {
				t.Errorf("%s: accepted", name)
			}
		}
		if _, err := e.sup.Suppressed(t.Context(), "aisat", "email", good); err != nil { // a control
			t.Fatalf("a well-formed lookup: %v", err)
		}
	})

	t.Run("every reason the schema accepts is accepted", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t)
		for i, r := range []domain.SuppressionReason{domain.SuppressHardBounce, domain.SuppressComplaint,
			domain.SuppressUnsubscribe, domain.SuppressInvalidAddress, domain.SuppressTokenRevoked, domain.SuppressSoftBounce} {
			h := hashOf("email", "user"+string(rune('a'+i))+"@example.com")
			e.suppress("aisat", "email", h, r, time.Time{})
			if reason, _ := e.storedSuppression("aisat", "email", h); reason != string(r) {
				t.Errorf("stored reason %q, want %q", reason, r)
			}
		}
	})
}
