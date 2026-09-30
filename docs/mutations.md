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

## Before this ledger

The queue's integration suite was checked against 20 mutations when T016 was built, every one caught
(two only after the tests were strengthened). That run was not written down, and the three rows above
are fresh runs of the mutations [testing.md](testing.md#mutation-check-what-matters) names as
examples, not a reconstruction of the twenty. The other guarantees the suite asserts — the fence on `Defer`,
`BindAddresses` and `DeferTenantChannel`; the disposition policy in `Finish`; digest sealing at
`DigestMax`; the erasure table list — have no row until someone re-runs their mutation. Do that when
the code around one changes.
