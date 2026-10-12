package domain

import "testing"

// A drop is the one place a notification is discarded for load, so it is a table:
// drop_non_essential drops only what is neither essential nor critical, by the
// priority that counts, which is the topic's own when the notification sets none
// (D5, NR-023).
func TestQuotaPolicyDrops(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		policy   QuotaPolicy
		def      TopicDef
		priority Priority
		want     bool
		why      string
	}{
		{"defer never drops", QuotaDefer, TopicDef{DefaultPriority: PriorityInfo}, PriorityInfo, false, "deferring discards nothing"},
		{"an unset policy never drops", "", TopicDef{DefaultPriority: PriorityInfo}, PriorityInfo, false, "only the named policy drops"},
		{"info on an ordinary topic", QuotaDropNonEssential, TopicDef{DefaultPriority: PriorityInfo}, PriorityInfo, true, ""},
		{"warning on an ordinary topic", QuotaDropNonEssential, TopicDef{DefaultPriority: PriorityInfo}, PriorityWarning, true, ""},
		{"critical on an ordinary topic", QuotaDropNonEssential, TopicDef{DefaultPriority: PriorityInfo}, PriorityCritical, false,
			"a critical notification always defers"},
		{"info on an essential topic", QuotaDropNonEssential, TopicDef{DefaultPriority: PriorityInfo, Essential: true}, PriorityInfo, false,
			"an essential topic always defers"},
		{"warning on an essential topic", QuotaDropNonEssential, TopicDef{DefaultPriority: PriorityInfo, Essential: true}, PriorityWarning, false, ""},
		{"critical on an essential topic", QuotaDropNonEssential, TopicDef{DefaultPriority: PriorityInfo, Essential: true}, PriorityCritical, false, ""},
		{"no priority on a topic that defaults to info", QuotaDropNonEssential, TopicDef{DefaultPriority: PriorityInfo}, "", true, ""},
		{"no priority on a topic that defaults to critical", QuotaDropNonEssential, TopicDef{DefaultPriority: PriorityCritical}, "", false,
			"critical only by its topic's default is still critical"},
		{"info on a topic that defaults to critical", QuotaDropNonEssential, TopicDef{DefaultPriority: PriorityCritical}, PriorityInfo, true,
			"the notification's own priority wins over the topic's"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.policy.Drops(tc.def, tc.priority); got != tc.want {
				t.Fatalf("%q.Drops(%+v, %q) = %v, want %v: %s", tc.policy, tc.def, tc.priority, got, tc.want, tc.why)
			}
		})
	}
}
