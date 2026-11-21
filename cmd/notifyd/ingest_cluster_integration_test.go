//go:build integration

package main

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/truongpx396/intel-notification/domain"
)

// NS-003, NR-035, D58. The ingest bus is replicated (R3) so that a publish the bus has acknowledged survives the
// loss of a node, and the service creates its stream with the replicas NOTIFY_INGEST_REPLICAS says. This is shown
// on a real three-node JetStream cluster: the node that leads the stream is destroyed, the stream keeps every
// message it had acknowledged, a new leader is elected, and the service goes on ingesting, each notification
// delivered once.

// natsCluster is three JetStream servers that form one cluster, each reachable from the test.
type natsCluster struct {
	urls  []string
	nodes map[string]testcontainers.Container // by server name
}

func startNATSCluster(ctx context.Context, t *testing.T) *natsCluster {
	t.Helper()
	net, err := network.New(ctx)
	if err != nil {
		t.Fatalf("create a network: %v", err)
	}
	t.Cleanup(func() { _ = net.Remove(context.WithoutCancel(ctx)) })
	names := []string{"n1", "n2", "n3"}
	routes := make([]string, len(names))
	for i, name := range names {
		routes[i] = "nats://" + name + ":6222"
	}
	c := &natsCluster{nodes: map[string]testcontainers.Container{}}
	for _, name := range names {
		ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image: "nats:2.11-alpine",
				Cmd: []string{
					"-js", "-sd", "/data", "--server_name", name,
					"--cluster_name", "notify", "--cluster", "nats://0.0.0.0:6222", "--routes", strings.Join(routes, ","),
				},
				ExposedPorts:   []string{"4222/tcp"},
				Networks:       []string{net.Name},
				NetworkAliases: map[string][]string{net.Name: {name}},
				WaitingFor:     wait.ForLog("Server is ready"),
			},
			Started: true,
		})
		if err != nil {
			t.Fatalf("start NATS node %s: %v", name, err)
		}
		t.Cleanup(func() { _ = ctr.Terminate(context.WithoutCancel(ctx)) })
		host, err := ctr.Host(ctx)
		if err != nil {
			t.Fatal(err)
		}
		port, err := ctr.MappedPort(ctx, "4222/tcp")
		if err != nil {
			t.Fatal(err)
		}
		c.urls = append(c.urls, fmt.Sprintf("nats://%s:%s", host, port.Port()))
		c.nodes[name] = ctr
	}
	// The cluster is formed when it can place a stream on three servers.
	nc, err := nats.Connect(strings.Join(c.urls, ","))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		_, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "PROBE", Subjects: []string{"probe.>"}, Replicas: 3})
		if err == nil {
			_ = js.DeleteStream(ctx, "PROBE")
			return c
		}
		if time.Now().After(deadline) {
			t.Fatalf("the cluster did not form in 60s: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func TestAnIngestStreamOfThreeReplicasSurvivesTheLossOfItsLeader(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	cluster := startNATSCluster(ctx, t)
	w := startServiceWith(t, serviceOpts{natsURL: strings.Join(cluster.urls, ","), natsReplicas: "3"})
	if _, err := w.catalog.UpsertTopic(ctx, "invoice_overdue", domain.TopicDef{DefaultChannels: []domain.ChannelKind{"in_app"}, DefaultPriority: domain.PriorityInfo}); err != nil {
		t.Fatal(err)
	}

	nc, err := nats.Connect(strings.Join(cluster.urls, ","), nats.MaxReconnects(-1), nats.ReconnectWait(100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	subject := "notify.ingest.local." + domain.TenantToken(wsTenant)
	// publish waits for the bus's acknowledgement, which is the promise that it holds the message on a quorum, and
	// tries again while a leader is being elected: a publish that fails then was never acknowledged.
	publish := func(n int) {
		t.Helper()
		body := fmt.Sprintf(`{"tenant":{"kind":"workspace","id":"w1"},"recipient":{"kind":"user","id":"ada"},"topic":"invoice_overdue","idem_key":"invoice:%d:overdue","title":"Overdue"}`, n)
		deadline := time.Now().Add(60 * time.Second)
		for {
			_, err := js.Publish(ctx, subject, []byte(body))
			if err == nil {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("publish %d: %v", n, err)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	notified := func() int {
		var n int
		_ = w.db.Admin.QueryRow(ctx, `SELECT count(*) FROM notifications WHERE realm = 'local'`).Scan(&n)
		return n
	}
	waitFor := func(want int) {
		t.Helper()
		if err := eventually(90*time.Second, func() error {
			if got := notified(); got != want {
				return fmt.Errorf("%d notifications, want %d", got, want)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}

	const first, second = 20, 10
	for i := 0; i < first; i++ {
		publish(i)
	}
	waitFor(first)

	stream, err := js.Stream(ctx, "NOTIFY_INGEST")
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.Config.Replicas != 3 || info.Cluster == nil || len(info.Cluster.Replicas) != 2 {
		t.Fatalf("the ingest stream = %+v, want one of three replicas: the service creates it with NOTIFY_INGEST_REPLICAS", info.Config)
	}
	leader := info.Cluster.Leader
	victim, ok := cluster.nodes[leader]
	if !ok {
		t.Fatalf("the stream's leader is %q, which is none of the nodes", leader)
	}

	// The leader is lost, all of it: its container is stopped and removed, with its disk.
	if err := victim.Terminate(ctx); err != nil {
		t.Fatalf("destroy the leader %s: %v", leader, err)
	}

	// A new leader is elected, and it holds every message the bus had acknowledged.
	if err := eventually(90*time.Second, func() error {
		got, err := stream.Info(ctx)
		if err != nil {
			return err
		}
		if got.Cluster == nil || got.Cluster.Leader == "" || got.Cluster.Leader == leader {
			return fmt.Errorf("no new leader yet (leader %q)", leader)
		}
		if got.State.Msgs != first {
			return fmt.Errorf("the stream holds %d messages, want the %d it had acknowledged", got.State.Msgs, first)
		}
		return nil
	}); err != nil {
		t.Fatalf("after losing %s: %v", leader, err)
	}

	// And the service goes on: new messages are accepted by the two that remain and each notifies once.
	for i := first; i < first+second; i++ {
		publish(i)
	}
	waitFor(first + second)
	time.Sleep(3 * time.Second) // a redelivery that over-notifies would show now
	if got := notified(); got != first+second {
		t.Fatalf("%d notifications after the loss, want exactly %d: each message once", got, first+second)
	}
}
