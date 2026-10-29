//go:build integration

package app_test

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	notify "github.com/truongpx396/intel-notification"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/internal/notifytest"
)

// T037: NS-007, tenant fairness, at a backlog that spans many claim batches.
//
// A tenant that has exhausted its channel budget holds a due backlog on every shard,
// several claim batches deep. A quiet tenant sends one notification to a recipient on
// every shard in each pass. The dispatcher is driven one pass at a time (a pass is one
// Drain of a batch from each shard, which is what Dispatcher.Run repeats), and the
// quiet tenant's delay is counted in passes from the pass in which a notification became
// due to the pass in which a channel was asked to send it. Nothing sleeps and nothing
// reads the wall clock, so the number does not move with the machine: wall time under
// load is the load test's (T055).
//
// The comparison holds the quiet tenant's traffic fixed and varies one thing: whether a
// neighbour with an exhausted budget and a deep backlog exists. The baseline is the
// quiet tenant alone. A neighbour whose budget is not exhausted is a tenant with a
// backlog to send, and the quiet tenant waits behind it in either design, as it should
// in a queue that is first come first served; exhaustion is what NS-007 is about.
//
// T033's exhausted-tenant case proves the mechanism with two rows; this proves the
// inequality.

const (
	fairBatch  = 3  // the batch a pass claims from a shard: small, so a backlog spans many of them
	fairDepth  = 12 // due rows the neighbour holds on every shard: four claim batches
	fairPasses = 30 // passes in which the quiet tenant sends
)

var (
	fairQuiet = domain.Tenant{Kind: "workspace", ID: "quiet"}
	fairNoisy = domain.Tenant{Kind: "workspace", ID: "noisy"}
)

type fairResult struct {
	delays    []int // passes from due to claimed, one per quiet notification
	neighbour int   // sends to the neighbour
	pending   int   // the neighbour's rows still in the queue at the end
}

// p95 is the nearest-rank 95th percentile.
func (r fairResult) p95() int {
	d := append([]int(nil), r.delays...)
	sort.Ints(d)
	return d[(len(d)*95+99)/100-1]
}

func TestAnExhaustedNeighbourDoesNotSlowAQuietTenant(t *testing.T) {
	t.Parallel()
	var baseline, exhausted fairResult
	// The comparison runs when both scenarios have finished: a cleanup of the parent runs
	// after its parallel subtests, which is what lets them run at once, each on a
	// database of its own.
	t.Cleanup(func() {
		if t.Failed() || len(baseline.delays) == 0 || len(exhausted.delays) == 0 {
			return // a scenario failed, or was filtered out by -run: there is nothing to compare
		}
		base, exh := baseline.p95(), exhausted.p95()
		t.Logf("p95 due-to-claimed: %d passes alone, %d passes beside an exhausted neighbour (%d quiet notifications each)",
			base, exh, len(baseline.delays))
		if base != 1 {
			t.Errorf("baseline p95 = %d passes, want 1: with no neighbour a notification is claimed in the pass it became due, "+
				"and the measure means nothing if that is not so", base)
		}
		if exh*100 > base*110 {
			t.Errorf("p95 beside the exhausted neighbour = %d passes against %d alone, want within 10%% (NS-007): "+
				"the neighbour's backlog is occupying the claims", exh, base)
		}
		if exhausted.neighbour != 0 {
			t.Errorf("the exhausted neighbour was sent %d times, want none: its budget was spent", exhausted.neighbour)
		}
		want := fairDepth * notify.Config{}.WithDefaults().Shards
		if exhausted.pending != want {
			t.Errorf("%d of the neighbour's rows are still queued, want all %d waiting for the reset: deferred, not dropped or dead-lettered",
				exhausted.pending, want)
		}
	})

	t.Run("baseline: the quiet tenant alone", func(t *testing.T) {
		t.Parallel()
		baseline = runFairness(t, false)
	})
	t.Run("a neighbour exhausted with a deep backlog", func(t *testing.T) {
		t.Parallel()
		exhausted = runFairness(t, true)
	})
}

func runFairness(t *testing.T, neighbour bool) fairResult {
	t.Helper()
	e := notifytest.NewEnv(t)
	shards := e.Config.WithDefaults().Shards
	e.Topics.Register(notifytest.Realm, "t", domain.TopicDef{DefaultChannels: []domain.ChannelKind{"email"}, DefaultPriority: domain.PriorityInfo})
	e.Templates.Put(notifytest.Realm, "t", "email", "en", domain.RenderedContent{Subject: "S", Body: "B"})

	var (
		mu      sync.Mutex
		claimed = map[string]int{}
		pass    atomic.Int64
	)
	email := notifytest.NewScripted("email", domain.ChannelCapabilities{NeedsAddress: true, Dedup: domain.DedupProvider, DedupWindow: time.Hour}).
		OnDeliver(func(_ context.Context, d domain.Delivery) {
			mu.Lock()
			defer mu.Unlock()
			claimed[d.Notification.IdemKey] = int(pass.Load())
		})
	e.Channels.Register(email)

	quiet := recipientsPerShard(fairQuiet, shards)
	for _, id := range quiet {
		e.Addresses.Put(id, "email", domain.Address{Channel: "email", Value: id.Recipient.ID + "@example.com"})
	}

	if neighbour {
		noisy := recipientsPerShard(fairNoisy, shards)
		for s, id := range noisy {
			e.Addresses.Put(id, "email", domain.Address{Channel: "email", Value: id.Recipient.ID + "@example.com"})
			for i := range fairDepth {
				send(t, e, id, fmt.Sprintf("n-%d-%d", s, i))
			}
		}
		// The budget runs out after the rows were queued: the planner, which peeks at
		// enqueue, did not see it, so the rows are due now, and the backlog is real.
		e.Quotas.SetBudget(domain.QuotaKey{Realm: notifytest.Realm, Tenant: fairNoisy, Channel: "email"}, 0, notifytest.Epoch.Add(time.Hour))
		for s := range shards {
			if due := count(t, e, `SELECT count(*) FROM notification_outbox WHERE tenant_id = $1 AND shard = $2 AND next_attempt_at <= now()`,
				fairNoisy.ID, s); due != fairDepth {
				t.Fatalf("shard %d holds %d due rows of the neighbour, want %d: the scenario needs a backlog %d batches deep",
					s, due, fairDepth, fairDepth/fairBatch)
			}
		}
	}

	runPass := func(p int) {
		pass.Store(int64(p))
		for s := range shards {
			if _, err := e.Dispatcher.Drain(t.Context(), domain.Shard(s), fairBatch); err != nil {
				t.Fatalf("Drain(shard %d) in pass %d: %v", s, p, err)
			}
		}
	}
	due := map[string]int{}
	for p := range fairPasses {
		for s, id := range quiet {
			key := fmt.Sprintf("q-%d-%d", s, p)
			due[key] = p
			send(t, e, id, key)
		}
		runPass(p)
	}
	// What is still unclaimed is claimed in the passes that follow.
	for p := fairPasses; p < fairPasses+100; p++ {
		mu.Lock()
		left := len(due) - countQuiet(claimed)
		mu.Unlock()
		if left == 0 {
			break
		}
		runPass(p)
	}

	var res fairResult
	mu.Lock()
	defer mu.Unlock()
	for key, dueAt := range due {
		at, ok := claimed[key]
		if !ok {
			t.Fatalf("notification %s was never sent", key)
		}
		res.delays = append(res.delays, at-dueAt+1)
	}
	for key := range claimed {
		if strings.HasPrefix(key, "n-") {
			res.neighbour++
		}
	}
	res.pending = count(t, e, `SELECT count(*) FROM notification_outbox WHERE tenant_id = $1`, fairNoisy.ID)
	return res
}

func countQuiet(claimed map[string]int) int {
	n := 0
	for key := range claimed {
		if strings.HasPrefix(key, "q-") {
			n++
		}
	}
	return n
}

// recipientsPerShard finds, for each shard, a recipient of the tenant that hashes to it.
func recipientsPerShard(tenant domain.Tenant, shards int) []domain.Identity {
	out := make([]domain.Identity, shards)
	for found, i := 0, 0; found < shards; i++ {
		id := domain.Identity{Realm: notifytest.Realm, Tenant: tenant,
			Recipient: domain.Recipient{Kind: "user", ID: fmt.Sprintf("u%d", i)}}
		if s := int(domain.ShardFor(id, shards)); out[s] == (domain.Identity{}) {
			out[s] = id
			found++
		}
	}
	return out
}
