package httpapi

import (
	"context"
	"fmt"
	"net/http"

	"github.com/truongpx396/intel-notification/ports"
)

// healthz is liveness: 200 while the process serves requests, and nothing else is asked. An orchestrator
// that restarted a healthy process because the database blinked would turn a blip into an outage.
func (s *server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// readyz is readiness: 200 when every check passes, and 503 naming the first that does not. The name is
// all it says: a health endpoint is often open, and the check's own error is for the operator.
func (s *server) readyz(w http.ResponseWriter, r *http.Request) {
	for _, c := range s.d.Ready {
		ctx, cancel := context.WithTimeout(r.Context(), readyTimeout)
		err := c.Run(ctx)
		cancel()
		if err != nil {
			s.onError(fmt.Errorf("readiness check %q: %w", c.Name, err))
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable", "failed": c.Name})
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// PartitionsCheck is the readiness check the contract names: PostgreSQL answers, and the partitions of
// the month in progress exist. Next month's missing partition is an alarm to provision it and not a
// reason to stop serving, so only this month's gap fails it.
func PartitionsCheck(m ports.Maintenance, clock ports.Clock) Check {
	return Check{Name: "partitions", Run: func(ctx context.Context) error {
		now := clock.Now().UTC()
		health, err := m.CheckPartitions(ctx, now)
		if err != nil {
			return fmt.Errorf("check partitions: %w", err)
		}
		for _, gap := range health.Missing {
			if gm := gap.Month.UTC(); gm.Year() == now.Year() && gm.Month() == now.Month() {
				return fmt.Errorf("no partition of %s for %s", gap.Table, gm.Format("2006-01"))
			}
		}
		return nil
	}}
}
