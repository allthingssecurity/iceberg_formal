# MC-5 — Phase 1 investigation (evidence only, no verdict)

Finding: coordinator election runs only in `open()` of *added* partitions and is never retried; a
missed election leaves no coordinator (STALL). Source: model checking, counterexample
`spec/output/MC_hunt_s4_stall_bfs.out` (invariant `NoCoordinatorStall`, cfg `MC_hunt_s4_stall.cfg`).

Code audited at the **pristine** upstream snapshot `apache/iceberg@9fffeb6` (`git show HEAD:...`);
the per-finding worktree carries the Specula trace instrumentation (`git status`: 10 modified files +
`TlaHooks.java`, `tla/`, `src/tlaHarness/`), which was not used for the audit.

## Step 1 — Code audit

### Cited sites (pristine line numbers)

| Site | What it does |
|---|---|
| `CommitterImpl.java:141-151` `open(addedPartitions)` | the **only** place a coordinator is started: `if (hasLeaderPartition(addedPartitions)) startCoordinator();` (`:147`) |
| `CommitterImpl.java:78-91` `hasLeaderPartition` | creates a fresh `Admin`, live `describeConsumerGroups(connectGroupId)` (`KafkaUtils.java:44-54`), takes `groupDesc.members()` and calls `containsFirstPartition` — no group-state check (removed by #14395) |
| `CommitterImpl.java:94-120` `containsFirstPartition` | `findFirstTopicPartition(members)`; if `null` → `LOG.warn("Committer {} found no partitions assigned across all members, cannot determine leader")` and **`return false`** (`:99-103`, added by #15955) |
| `CommitterImpl.java:123-128` `findFirstTopicPartition` | min over `member.assignment().topicPartitions()`; `.orElse(null)` |
| `CommitterImpl.java:189-212` `save` / `processControlEvents` | never elects: only clears a fenced coordinator (`:199-202`, `coordinatorThread = null`) or throws `NotRunningException`; then `worker.process()` |
| `CommitterImpl.java:223-231` `startCoordinator` | no-op if `coordinatorThread != null` |
| `CommitterImpl.java:161-187` `close` | stops the coordinator only if `hasLeaderPartition(closedPartitions)` (another live describe) |

Nothing else in the sink (`IcebergSinkTask`, `Worker`, `Coordinator`, `CoordinatorThread`, `Channel`)
starts a coordinator or re-evaluates leadership. `Worker.receive` (`Worker.java:67-114`) only acts on a
`START_COMMIT` event, which only a running `Coordinator.process()` (`Coordinator.java:126-141`) sends.
`IcebergSinkTask.preCommit` returns `ImmutableMap.of()` (`IcebergSinkTask.java:99-104`): source offsets
are committed only inside the worker's DataWritten transaction (`Channel.send`, `Channel.java:83-118`).

### Call chain

Kafka consumer `ConsumerCoordinator.onJoinComplete` (after SyncGroup) →
`WorkerSinkTask$HandleRebalance.onPartitionsAssigned(added)` → (returns early if `added` is empty —
bytecode of connect-runtime 3.9.2, `Collection.isEmpty` → `return` before `openPartitions`) →
`IcebergSinkTask.open` (`IcebergSinkTask.java:57-60`) → `CommitterImpl.open` (`:141`) →
`hasLeaderPartition(added)` (`:147`) → `KafkaUtils.consumerGroupDescription` (`KafkaUtils.java:44`) →
`containsFirstPartition` → `findFirstTopicPartition`.

Reachable in normal operation: every rebalance that adds partitions to a task.

### Reachability of the trigger state

* Classic group protocol: the broker describes a group that is **not `Stable`** (PreparingRebalance /
  CompletingRebalance) with members **without metadata/assignment** (Kafka `GroupMetadata.summary` /
  `ClassicGroup.describe` → `describeNoMetadata`). The AdminClient turns that into empty
  `MemberAssignment`s → `findFirstTopicPartition` = `null` → not leader. To be confirmed empirically in
  Phase 2 (probe of the describe during a rebalance).
* The `open()` callback itself runs after SyncGroup (group Stable at that instant), but the describe is a
  separate, later RPC (new Admin client + metadata + FindCoordinator + DescribeGroups). Any membership
  change (rejoin of a kicked-out member, a restarted task's new consumer, a leave) that reaches the broker
  between the SyncGroup and the DescribeGroups puts the group back into PreparingRebalance → the
  describe sees no assignments.
* Field corroboration that the open-time non-Stable describe happens in production: issue #15293
  comment https://github.com/apache/iceberg/issues/15293#issuecomment-5598930908 (flysen, 2026-09-09):
  "the only thing ever logged is the benign, expected `CommitterImpl: found no partitions assigned
  across all members, cannot determine leader` transient that fires briefly on task startup".

### Persistence (why a missed election is not retried)

* Eager protocol: every rebalance revokes and re-adds all partitions → `open()` runs again on the owner
  of the first partition → re-election at the very next generation (which the racing membership change
  itself triggers). Expected: transient.
* Cooperative protocol (`CooperativeStickyAssignor`, KIP-429): a task that keeps the first partition
  across the follow-up generations never gets `open()` for it again (`onPartitionsAssigned` gets only
  newly added partitions and returns early if none). The sticky assignor keeps the *first* entries of a
  member's owned-partition list (`AbstractStickyAssignor` constrained assign keeps
  `ownedPartitions.subList(0, quota)`); the order is the `SubscriptionState` assignment order (HashMap
  iteration of `TopicPartition` hashes). Computed for topic `src`, partitions {0,1}: order `[src-0, src-1]`
  → the member holding both keeps `src-0`. So the task that missed the election keeps the first
  partition and no later `open()` re-elects. Expected: stall until an external event (task restart,
  a rebalance that moves partition 0).
* `processControlEvents` / `save` never elect (above).

### Trigger scenario (constructed)

Two sink tasks, cooperative-sticky assignor, source topic with 2 partitions; task A owns p0 and hosts
the coordinator, task B owns p1.
1. Task A leaves the group — kicked out (CE: poll-interval kick-out, lost-close stops its coordinator)
   or restarted/crashed (`close` → describe Stable → `stopCoordinator`; `stop()` → `close([])`).
2. Generation N (A absent): B is assigned {p0,p1}; `open([p0])` (or `[p0,p1]`) on B.
3. Before B's describe reaches the broker, A's new consumer joins (rejoin / restarted task) → group
   PreparingRebalance → describe returns members without assignments → `findFirstTopicPartition` null →
   WARN "found no partitions assigned across all members" → B not leader. No coordinator anywhere.
4. Gen N+1 (cooperative): B keeps p0, revokes p1 (`close([p1])`, no election there); gen N+2: A gets p1,
   `open([p1])` → first partition p0 is B's → not leader.
5. No `StartCommit` is ever sent; workers keep consuming and writing files; no table commits; no source
   offsets committed; tasks RUNNING; no error.

CE mapping (`MC_hunt_s4_stall_bfs.out`): states 2-8 = t1 owns {0,1}, coordinator C1 starts, one commit
round answered (DataWritten pending, `srcCommitted[0]=1`); state 9 = lost fault (owner → t2, `lostParts
t1={0,1}`, t2 `tStep=open`); states 10-12 = t1 lost-close: closeDiscard, closeElect stops C1 (`cTerm[1]
= TRUE`, `coordRef[t1]=0`); state 13 = t2 `open({0,1})` completes with no coordinator (`coordRef[t2]=0`)
→ `NoCoordinatorStall`. The CE stops there; the follow-up sticky generations (step 4 above) are argued in
the finding, not shown in the trace.

### Safeguards encountered (recorded, not dismissed)

* Eager rebalancing re-runs `open()` on every generation (would self-heal) — protocol dependent.
* A later rebalance that moves p0, or a task restart, re-elects — external events, no internal retry.
* `group.initial.rebalance.delay.ms` (broker default 3000 ms) suppresses the race at connector
  *startup* for tasks joining within the delay (note: the embedded test cluster defaults it to `0`,
  `EmbeddedKafkaCluster.addDefaultBrokerPropsIfAbsent`).
* KIP-848 `consumer` groups are described with current assignments regardless of state — the trigger
  as stated applies to the classic protocol.
* No data loss from the stall itself: source offsets are committed only with DataWritten. (A DataWritten
  already answered before the stall stays uncommitted in the control topic until a coordinator
  returns — loss only if control-topic retention expires first; argued, not part of this finding.)

## Step 2 — Developer knowledge (verbatim, with cites)

* `d8e7642d4e` (#10351, 2024-07): original election required `groupDesc.state() == STABLE` in
  `hasLeaderPartition`; otherwise not leader.
* #14395 (merged 2025-11-04, c235305c98) removed the STABLE check. PR comment (fenil25, 2025-10-24):
  "I realized if we always close the coordinator when a partiion is revoked; during race conditions, we
  can also end up with **no coordinators if we check for stability during open call for new
  partitions**. This ensures we never check for stability and just let Kafka Connect provide the list of
  partitions during open and close call". PR body: "Even during continuous rebalance, Kafka will always
  provide new partitions in opening and closing call." Reviewer (kumarpritam863): "if you see the
  consumer coordinator class of kafka, it does check stability before proceeding with the open and close
  calls."
  → After #14395 a non-Stable describe hit `orElseThrow(ConnectException("No partitions assigned, cannot
  determine leader"))` (task failure → restart → re-election).
* #15955 (merged 2026-04-21, a675f88b0d) "Do not fail if no partitions assigned": replaced the throw by
  `LOG.warn(...); return false`. PR body: "since we have removed the check there will be more instances
  of invalid calls (class when rebalance is going on or happening frequently) and **we should wait for
  the final rebalance to happen and simply return false**." → intent: a later rebalance re-runs the
  election.
* #11288 / #12372 (merged 2025-03-14, "Handle no coordinator and data loss in ICR mode"): developers
  treat a no-coordinator state as a bug ("Workers gets stuck as there is no-coordinator for emitting
  Start_Commit request in Incremental Cooperative Rebalancing[ICR] Mode") and state "In the ICR mode
  open receives only the newly added partitions, and open will not be called by connect framework if
  there are No New Partitions Assigned". Different trigger (close() stopped the coordinator on any
  revocation), fixed there.
* #17637 (open issue, close()-side duplicate coordinator): "Falling back to stopping when the elected
  partition is unknown is the safe direction, since an orphaned coordinator corrupts data while **a
  missing one is re-elected on the next `open()`**." #16016 comment (gdmachado,
  https://github.com/apache/iceberg/issues/16016#issuecomment-4587580400): "remember the leader partition
  in `open()` (stable there)". → developers believe the group is Stable at `open()` and that a missing
  coordinator is re-elected by the next `open()`.
* #18039 (merged 2026-10-02, fenced coordinator cleared in `processControlEvents`): "A stale coordinator
  will identify that it has been fenced, and exit the coordinator thread. We leave the worker task running
  and defer to Kafka Connect on determining its lifecycle." No re-election discussed.
* #17450 (closed unmerged 2026-09-21, "Rework commit-coordinator leader election"): "picking the task
  that owned the globally-lowest (topic, partition) from a transient, mid-rebalance member snapshot ...
  was racy during cooperative rebalancing"; proposed level-triggered reconcile in `save()`. Review
  (laskoviymishka) on that PR's own code: "When we clear a fenced coordinator here, we don't re-arm
  reconcileNeeded, so I think we can end up with no coordinator at all ... silently stops producing
  commits."
* Comments in code: `CommitterImpl.java:96` "Determine the first partition across all members to elect
  the leader"; no TODO/FIXME about retries.
* Tests: `TestCommitterImpl` covers `containsFirstPartition`/`hasLeaderPartition` with populated
  assignments and `testFencedCoordinatorIsClearedWithoutFailingTask` (asserts the fenced coordinator
  reference becomes `null`, no re-election asserted). No test for an empty-assignment describe at
  `open()` and none for re-election.

## Step 3 — Known-status / precedent

Searched (gh CLI, apache/iceberg issues + PRs, open/closed/merged, incl. recent): "hasLeaderPartition",
"cannot determine leader", "found no partitions assigned across all members", "coordinator election",
"leader election", "no coordinator", "re-elect", "coordinator never started", "no START_COMMIT",
"stops committing after rebalance", "cooperative sticky coordinator", "17637", "processControlEvents";
commit history of `CommitterImpl.java` (latest upstream change `dad714b8f0`/#18039, 2026-10-02, already
in the snapshot).

* Same site, different mechanism/consequence: #17637 / open PR #18687 (close() → duplicate coordinator);
  #16016 (zombie coordinator); #11288/#12372 (no coordinator via close() stopping on non-leader
  revocation — fixed in 1.9); #17193 / closed #17639 (cg-control group); #12608 (group-id mismatch).
* Closest: #14395 PR comment recognises "no coordinators if we check for stability during open call"
  for the *pre-#14395* code and considers it fixed by removing the client-side check; #15955 then
  reintroduced the not-leader outcome on a non-Stable describe with the stated expectation that the
  "final rebalance" re-elects. No issue/PR reports that the current `open()` path misses the election on
  a non-Stable describe and that, under cooperative rebalancing, nothing re-elects.
* Result: no existing report of THIS mechanism at THIS site found. Source is MC with an actual
  counterexample → no pre-filter drop; proceed to Phase 2.
