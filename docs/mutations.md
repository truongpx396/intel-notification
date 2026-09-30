# Mutation ledger

A test that cannot fail proves nothing ([testing.md](testing.md#mutation-check-what-matters)). This is
the record of the times a guarantee was broken on purpose, to see its test go red. A guarantee with no
row here has not been shown to be caught.

## How to add a row

1. Start from a green suite. A result on a red suite means nothing.
2. Break one guarantee with the smallest change that removes it, and keep the code valid. The test must
   fail on its **assertion**. A mutation that fails on a syntax error or an unused parameter has shown
   nothing: replace the comparison instead of deleting the parameter it uses.
3. Run only the suite that should catch it and read the failure. Record the line.
4. Revert the change.
5. If the suite stayed green, the mutation **survived** and the test has a gap. Record it as `SURVIVED`,
   strengthen the test, and add a second row showing it now dies.

Add a row in the same PR as the task that adds the guarantee ([tasks.md](../specs/001-notification-core/tasks.md#how-a-task-is-worked),
definition of done, point 3).

## Ledger

| Date | Task | Guarantee | Mutation | Result | Failure seen |
|---|---|---|---|---|---|
| 2026-09-30 | T016 | A claim skips rows another session holds ([D19](../specs/001-notification-core/design-decisions.md#d19)) | `queue.go` claim: `FOR UPDATE SKIP LOCKED` → `FOR UPDATE` | Killed by `TestClaimLeasesAndSkipsLockedRows` | `session B blocked on rows A holds instead of skipping them: claim: timeout: context deadline exceeded` |
| 2026-09-30 | T016 | `Retry` is fenced by the lease token | `retrySQL`: `AND lease_token = $2::uuid` → `AND $2::uuid IS NOT NULL` | Killed by `TestFencedWrites` (`retry_with_forged_token`, `retry_with_token_of_a_lapsed_lease`) | `stale retry: ok=true err=<nil> — the write should have been discarded` |
| 2026-09-30 | T016 | `Finish` is fenced by the lease token | `finishSQL`: `AND lease_token = $2::uuid` → `AND $2::uuid IS NOT NULL` | Killed by `TestFencedWrites` (`finish_with_forged_token`, `finish_with_token_of_a_lapsed_lease`) | `stale finish: ok=true err=<nil> — the write should have been discarded` |
| 2026-09-30 | T006 | A default partition is not coverage: a month covered only by one reads as missing ([D33](../specs/001-notification-core/design-decisions.md#d33)) | `missingPartitionsSQL`: `WHERE c.oid <> p.partdefid` → `WHERE c.oid <> 0` | Killed by `TestCheckPartitions` (`a_default_partition_is_not_coverage`) | `missing [], want [{dead_letters 2031-03-01 00:00:00 +0000 UTC}]` |
| 2026-09-30 | T006 | A month split across partitions is covered, by merging their ranges | `range_agg` merge removed: each partition is tested alone with `@>` | Killed by `TestCheckPartitions` (`a_month_split_across_two_partitions_is_covered`) | `missing [{notifications 2031-03-01 00:00:00 +0000 UTC}], want []` |
| 2026-09-30 | T006 | A hole between two partitions is a gap, not covered | `range_agg(…)` → `range_merge(range_agg(…))`, the hull of the partitions | Killed by `TestCheckPartitions` (`half_a_month_is_missing`, `this_month_missing_from_one_table`, `a_default_partition_is_not_coverage`) | `missing [], want [{notification_deliveries 2031-03-01 00:00:00 +0000 UTC}]` |
| 2026-09-30 | T006 | The whole month must be covered, not part of it | `tstzrange(m.lo, m.hi)` → `tstzrange(m.lo, m.lo + interval '1 day')` | Killed by `TestCheckPartitions` (`half_a_month_is_missing`) | `missing [], want [{notifications 2031-03-01 00:00:00 +0000 UTC}]` |
| 2026-09-30 | T006 | A table with no partition at all misses both months | `coalesce(cv.span @> …, false)` → `coalesce(…, true)` | Killed by `TestCheckPartitions` (`a_table_with_no_partitions_at_all`) | `missing [], want [{dead_letters 2031-03-01 00:00:00 +0000 UTC} {dead_letters 2031-04-01 00:00:00 +0000 UTC}]` |
| 2026-09-30 | T006 | The next month is checked as well as this one | `months`, `ends` → `months[:1]`, `ends[:1]` in the query arguments | Killed by `TestCheckPartitions` (`next_month_missing_from_one_table` and four more) | `missing [], want [{dead_letters 2031-04-01 00:00:00 +0000 UTC}]` |
| 2026-09-30 | T006 | The month is the UTC month of `at`, whatever location `at` carries | `CheckPartitions`: `at = at.UTC()` removed | Killed by `TestCheckPartitions` (`the_month_is_the_UTC_month`) | `missing [], want [{notifications 2031-05-01 00:00:00 +0000 UTC} …]` |
| 2026-09-30 | T006 | Row-level security enabled but not forced is unscoped (NS-001) | `unscopedSQL`: `NOT (c.relrowsecurity AND c.relforcerowsecurity)` → `NOT c.relrowsecurity` | Killed by `TestCheckPartitions` (`row-level_security_enabled_but_not_forced`, `the_parent_not_forced`) | `unscoped [], want [notifications_2031m03]` |
| 2026-09-30 | T006 | Row-level security disabled but forced is unscoped | the same predicate → `NOT c.relforcerowsecurity` | Killed by `TestCheckPartitions` (`row-level_security_disabled`) | `unscoped [], want [notifications_2031m04]` |
| 2026-09-30 | T006 | The `recipient_scope` predicate must be the parent's | `= parent.qual` → `IS NOT NULL` | Killed by `TestCheckPartitions` (`a_looser_predicate`) | `unscoped [], want [notifications_2031m03]` |
| 2026-09-30 | T006 | The `WITH CHECK` must be the parent's, so a write cannot leave the scope | `IS NOT DISTINCT FROM parent.wcheck` → compare the policy's check with itself | Killed by `TestCheckPartitions` (`a_looser_check_on_writes`) | `unscoped [], want [notifications_2031m03]` |
| 2026-09-30 | T006 | The scope must apply to every command, not reads only | `AND p.polcmd = '*'` → `AND p.polcmd IS NOT NULL` | Killed by `TestCheckPartitions` (`the_scope_for_reads_only`) | `unscoped [], want [notifications_2031m03]` |
| 2026-09-30 | T006 | `recipient_scope` must be a permissive policy, as the parent's is | `AND p.polpermissive AND p.polcmd` → `AND p.polcmd` | Killed by `TestCheckPartitions` (`the_scope_restrictive,_with_no_permissive_policy`) | `unscoped [], want [notifications_2031m03]` |
| 2026-09-30 | T006 | Any other permissive policy widens the scope | the other-policy test: `AND p.polpermissive` → `AND false` | Killed by `TestCheckPartitions` (`another_permissive_policy_widens_the_scope`) | `unscoped [], want [notifications_2031m03]` |
| 2026-09-30 | T006 | A restrictive policy narrows the scope and is not a finding | the other-policy test: `AND p.polpermissive` removed | Killed by `TestCheckPartitions` (`another_restrictive_policy_only_narrows_it`) | `unscoped [notifications_2031m03], want []` |
| 2026-09-30 | T006 | A partition of a partition is checked, at any depth | `pg_partition_tree('notifications')` → the parent and its direct children | Killed by `TestCheckPartitions` (`an_unscoped_partition_of_a_partition`) | `unscoped [], want [notifications_2031m04_all]` |
| 2026-09-30 | T006 | The parent table is checked, not only its partitions | `JOIN pg_class c ON c.oid = t.relid` → `… AND t.level > 0` | Killed by `TestCheckPartitions` (`the_parent_not_forced`) | `unscoped [], want [notifications]` |
| 2026-09-30 | T006 | An unbounded upper bound (`MAXVALUE`) is read as unbounded | `CASE WHEN b.v[2] = 'MAXVALUE' THEN NULL` → `THEN '2031-03-02'::timestamptz` | Killed by `TestCheckPartitions` (`an_unbounded_upper_bound_is_read`) | `missing [{dead_letters 2031-03-01 00:00:00 +0000 UTC} {dead_letters 2031-04-01 00:00:00 +0000 UTC}], want []` |
| 2026-09-30 | T006 | An unbounded lower bound (`MINVALUE`) is read as unbounded | `CASE WHEN b.v[1] = 'MINVALUE' THEN NULL` → `THEN '2020-05-11'::timestamptz` | Killed by `TestCheckPartitions` (`an_unbounded_lower_bound_is_read`) | `missing [{notifications 2020-05-01 00:00:00 +0000 UTC} {notification_deliveries 2020-05-01 …} {dead_letters 2020-05-01 …}], want []` |
| 2026-09-30 | T006 | Bounds are read correctly whatever the session time zone (`Pacific/Pago_Pago` here) | `::timestamptz` → `::timestamp AT TIME ZONE 'UTC'`, dropping the bound's offset | Killed by `TestCheckPartitions` (`bounds_are_read_in_any_session_time_zone`) | `missing [{notifications 2031-04-01 00:00:00 +0000 UTC} {notification_deliveries 2031-04-01 …} {dead_letters 2031-04-01 …}], want []` |

## Before this ledger

The queue's integration suite was checked against 20 mutations when T016 was built, every one caught
(two only after the tests were strengthened). That run was not written down, and the first three rows
are fresh runs of the mutations [testing.md](testing.md#mutation-check-what-matters) names as
examples, not a reconstruction of the twenty. The other guarantees the suite asserts — the fence on `Defer`,
`BindAddresses` and `DeferTenantChannel`; the disposition policy in `Finish`; digest sealing at
`DigestMax`; the erasure table list — have no row until someone re-runs their mutation. Do that when
the code around one changes.
