package domain

// The counters the dispatcher raises, named as docs/operations.md alarms on them.
const (
	// MetricLeaseLost counts an outcome the dispatcher discarded because its lease
	// had been lost: a worker that stalled past ClaimLease, or a ClaimLease shorter
	// than a channel's send. Each may mean a duplicate on a DedupNone channel
	// (NR-004, D19). Label: channel.
	MetricLeaseLost = "notify.lease.lost"

	// MetricDeadLetter counts a delivery parked as a dead letter: a genuine
	// failure, never a suppressed or an unresolvable address (NR-007, D8). Labels:
	// channel, and reason (the terminal outcome that parked it).
	MetricDeadLetter = "notify.dlq.dead.count"
)

// The labels those counters carry.
const (
	LabelChannel = "channel"
	LabelReason  = "reason"
)
