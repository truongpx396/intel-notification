package domain

import "time"

// Validate reports every rule s breaks, as a *ValidationError, or nil.
func (s DeliverySchedule) Validate() error { return nil }

// QuietUntil reports whether at falls inside the schedule's quiet hours and, if
// so, the instant they end.
func (s DeliverySchedule) QuietUntil(at time.Time) (end time.Time, quiet bool) {
	return time.Time{}, false
}
