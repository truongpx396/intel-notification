package notifytest

import (
	"slices"
	"testing"
)

func TestMetricsCountsByNameAndLabels(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	if m.Count("notify.dlq.dead.count") != 0 || len(m.Events()) != 0 {
		t.Fatal("a new Metrics has counted something")
	}
	m.Inc("notify.dlq.dead.count", map[string]string{"channel": "email", "reason": "rejected"})
	m.Inc("notify.dlq.dead.count", map[string]string{"channel": "email", "reason": "max_attempts"})
	m.Inc("notify.dlq.dead.count", map[string]string{"channel": "sms", "reason": "rejected"})
	m.Inc("notify.lease.lost", nil)

	cases := []struct {
		name   string
		metric string
		labels map[string]string
		want   int
	}{
		{"every increment of a name, whatever its labels", "notify.dlq.dead.count", nil, 3},
		{"another name is counted apart", "notify.lease.lost", nil, 1},
		{"a name never incremented", "notify.quota.exhausted", nil, 0},
		{"one label narrows the count", "notify.dlq.dead.count", map[string]string{"channel": "email"}, 2},
		{"every given label must match", "notify.dlq.dead.count", map[string]string{"channel": "email", "reason": "rejected"}, 1},
		{"a label value no increment carried", "notify.dlq.dead.count", map[string]string{"channel": "push"}, 0},
		{"a label no increment carried at all", "notify.dlq.dead.count", map[string]string{"tenant": "w1"}, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := m.CountWith(tc.metric, tc.labels); got != tc.want {
				t.Fatalf("CountWith(%s, %v) = %d, want %d", tc.metric, tc.labels, got, tc.want)
			}
		})
	}
	if got := m.Count("notify.dlq.dead.count"); got != 3 {
		t.Fatalf("Count = %d, want 3", got)
	}
}

// An increment keeps the labels it was given, not the caller's map: a caller that
// reuses one map for the next increment must not rewrite the last.
func TestMetricsKeepsACopyOfTheLabels(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	labels := map[string]string{"channel": "email"}
	m.Inc("notify.lease.lost", labels)
	labels["channel"] = "sms"
	m.Inc("notify.lease.lost", labels)

	events := m.Events()
	if len(events) != 2 || events[0].Labels["channel"] != "email" || events[1].Labels["channel"] != "sms" {
		t.Fatalf("Events() = %+v, want email then sms: the first increment's labels changed under it", events)
	}
	events[0].Labels["channel"] = "push"
	if got := m.CountWith("notify.lease.lost", map[string]string{"channel": "email"}); got != 1 {
		t.Fatalf("a caller edited an event and changed the count to %d, want 1", got)
	}
	names := []string{}
	for _, e := range m.Events() {
		names = append(names, e.Name)
	}
	if !slices.Equal(names, []string{"notify.lease.lost", "notify.lease.lost"}) {
		t.Fatalf("Events() names = %v", names)
	}
}
