package domain

import "time"

// PartitionGap is a month a range-partitioned table has no partition for. An
// insert dated inside it fails, because there is no default partition to catch
// it (D33).
type PartitionGap struct {
	Table string
	Month time.Time // the UTC start of the month
}

// PartitionHealth is what Maintenance.CheckPartitions reads from the catalog:
// the conditions behind the notify.partition.missing and
// notify.partition.unscoped alarms. The zero value is healthy.
type PartitionHealth struct {
	// Missing is each month, this one and the next, that a range-partitioned
	// table has no partition covering, ordered by month and then by table.
	Missing []PartitionGap
	// Unscoped names notifications, or any partition of it, that recipient
	// scoping does not hold on, sorted. Each one is an isolation incident: read
	// by name, it can show every recipient's rows (D17).
	Unscoped []string
}
