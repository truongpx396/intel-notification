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

	// MetricPartitionMissing counts, each time the maintenance job checks, a
	// range-partitioned table with no partition for this month or the next: an insert
	// dated in it fails (D33). Labels: table, month (as 2006-01). The check runs hourly, so
	// any increase is a gap that is still open.
	MetricPartitionMissing = "notify.partition.missing"

	// MetricPartitionUnscoped counts, each time the maintenance job checks, a
	// notifications partition that recipient scoping does not hold on: an isolation
	// incident, because reading it by name shows every recipient's rows (D17). Label:
	// partition.
	MetricPartitionUnscoped = "notify.partition.unscoped"

	// MetricDBOldestXminAge is a gauge: the age in seconds of the oldest transaction
	// in the queue's database that pins vacuum (NR-037, D38). Zero when there is none.
	MetricDBOldestXminAge = "notify.db.oldest_xmin_age"

	// MetricDBCheckpointsRequested is a gauge: the checkpoints the server has been
	// forced into because the WAL filled, since its statistics were reset (NR-037,
	// D38). Alarm when it rises faster than the checkpoints taken on schedule.
	MetricDBCheckpointsRequested = "notify.db.checkpoints_requested"
)

// The labels those counters carry.
const (
	LabelChannel   = "channel"
	LabelReason    = "reason"
	LabelTable     = "table"
	LabelMonth     = "month"
	LabelPartition = "partition"
)
