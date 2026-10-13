package notifytest

import (
	"maps"
	"sync"

	"github.com/truongpx396/intel-notification/ports"
)

// MetricEvent is one increment the engine reported.
type MetricEvent struct {
	Name   string
	Labels map[string]string
}

// Metrics is an in-memory ports.Metrics. It keeps every increment with its labels,
// so a test can ask both "was the alarm raised" and "for what": the dead-letter
// counter raised for a correct outcome is the failure NR-007 names.
type Metrics struct {
	mu     sync.Mutex
	events []MetricEvent
}

var _ ports.Metrics = (*Metrics)(nil)

// NewMetrics returns a Metrics that has counted nothing.
func NewMetrics() *Metrics { return &Metrics{} }

// Inc implements ports.Metrics.
func (m *Metrics) Inc(name string, labels map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, MetricEvent{Name: name, Labels: maps.Clone(labels)})
}

// Count is how many times name was incremented, whatever its labels.
func (m *Metrics) Count(name string) int { return m.CountWith(name, nil) }

// CountWith is how many increments of name carried every one of the given labels
// (and possibly others).
func (m *Metrics) CountWith(name string, labels map[string]string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, e := range m.events {
		if e.Name != name {
			continue
		}
		matches := true
		for k, v := range labels {
			if got, ok := e.Labels[k]; !ok || got != v {
				matches = false
				break
			}
		}
		if matches {
			n++
		}
	}
	return n
}

// Events lists every increment in the order it happened. The caller may keep it.
func (m *Metrics) Events() []MetricEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]MetricEvent, len(m.events))
	for i, e := range m.events {
		out[i] = MetricEvent{Name: e.Name, Labels: maps.Clone(e.Labels)}
	}
	return out
}
