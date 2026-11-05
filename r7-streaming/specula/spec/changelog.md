# Changelog — iceberg-kafka-connect spec validation

Spec: `base.tla` / `MC.tla` / `Trace.tla` (apache/iceberg @ 9fffeb6, kafka-connect sink).
Traces: `../traces/*.ndjson` (Phase 2.5 harness, 8 traces). TLC output: `output/`.

## Round 1 - Trace Validation
Baseline: 7/8 traces passed; `zombie_coop_round2_rejected.ndjson` failed at line 60 (TraceMatched violated).
- [fix] RebalanceWith: one step is now one Connect-group generation. Cooperative: a partition still owned by another member is only revoked in that generation (left unowned, "moving") and assigned in the follow-up generation (`ConsumerCoordinator.onJoinComplete` revoke + `requestRejoin`, ConsumerCoordinator.java:336-433); before, both rounds were collapsed into one step, so the round-1 owner map {0:t2, 1:none} could not be expressed. New `sticky` argument (owned partitions keep their owner); MC's reactive `MCJoinRebalance` uses sticky generations so cooperative follow-up rounds cannot chain forever (Trace: zombie_coop_round2_rejected.ndjson)
- [fix] Rejoin (new action): a member whose assignment does not change gets no callback and keeps running with its old generation until a later poll processes the join response (groupMetadata updated in onJoinComplete, ConsumerCoordinator.java:351); until then its Worker's TxnOffsetCommit is rejected with ILLEGAL_GENERATION (GroupCoordinator.scala:1022-1043, no group-state exemption for transactional commits) -> WorkerAnswerRejected, healthy task FAILED (harness gap 7). Before, every member was forced through `open` at the rebalance, so the KIP-447 rejection of a non-lost member was unreachable (Trace: zombie_coop_round2_rejected.ndjson)
- [fix] Trace.tla: `SilentGenSync` (Rejoin before an accepted WorkerAnswer, deterministic); `Rebalance` maps to `RebalanceWith(lost, FALSE)`; the `Open` wrapper now requires "a coordinator started in the spec iff the event carries cpart/total" (it raised a TLC field-access error instead of rejecting the branch) (Trace: zombie_coop_round2_rejected.ndjson)
- [fix] harness/postprocess_trace.py: one Rebalance per real generation (no merging of the two cooperative rounds, matching the spec fix), and a generation's Rebalance is moved before a WorkerAnswerRejected of a member still on an older generation (the broker bumped the generation before rejecting). Regenerated callback_faults, zombie, zombie_validation, zombie_coop_round2_rejected from traces/raw (the other four are byte-identical)
- Result: 8/8 traces pass (TraceMatched checked); no regressions.

## Round 1 - Model Checking
- MC.cfg (BFS, 4 workers): completed exhaustively in 12 min 28 s, 62,226,113 states generated / 15,050,945 distinct, depth 58, no invariant or property violation (output/MC_round1_bfs.out). No spec or invariant change.

## Convergence (Round 1)
Round 1: all 8 traces pass and MC.cfg passes on the same spec; the model-checking round needed no modification -> converged after Round 1. Bug hunting started; its first counterexamples exposed three fidelity gaps (Case B) that MC.cfg's bounds (no legacy tasks, StaleViews = FALSE) could not reach. They were fixed and the loop re-run (Round 2).

## Bug hunting, first pass - spec fixes (Case B)
- [fix-spec] Open: a task running <=1.10 worker code ("legacy") no longer starts a coordinator. The first MC_hunt_s1_refsplit counterexample hosted the coordinator on the legacy task; a <=1.10 coordinator has no uuid in TableReference.equals and so never splits groups (TableReference.java:144-160 is >=1.11 code), and its commit logic is out of scope. Re-run: the same F1 violation with the coordinator on the >=1.11 task (Case C, bug-report Bug 1). No effect where LegacyTasks = {} (all traces, MC.cfg).
- [fix-spec] TotalViews: totalPartitionCount is exactly the assigned-partition count of the generation (was 1..count with StaleViews). The first MC_hunt_s5_readiness counterexample froze total = 1 at the initial Stable join. The range was a proxy for the cooperative round-1 view while rounds were collapsed; since Round 1 that view is explicit (moving partitions unowned), and a non-Stable describe has no assignments and never elects (CommitterImpl.java:97-104, 122-128). No effect where StaleViews = FALSE.
- [fix-spec] ResetTaskLocal (FailTask / Crash): `owner` (the generation's assignment as describeConsumerGroups reports it) keeps a failed member's partitions until the next generation; MC's reactive NeedsRebalance now fires when a partition is in no live member's `assign`. The second MC_hunt_s5_readiness counterexample let t2's open() elect itself (partial total 1) right after t1 FAILED, because the failed member's partitions were dropped from the described view at once; the broker still describes them (Stable, old generation) or describes no assignments (after the LeaveGroup), so t2 cannot be leader. Also makes zombie_coop_round2_rejected line 71 (t1's open does not elect) match without StaleViews.
- Runs made on the pre-fix spec were discarded (output kept only for configs whose counterexample does not touch the changed behaviour; all hunts were re-run on the final spec, see bug-report.md).

## Round 2 - Trace Validation
- 8/8 traces pass on the final spec (TraceMatched checked); no regressions. No trace change.

## Round 2 - Model Checking
- MC.cfg (BFS, 4 workers) on the final spec: completed exhaustively in 15 min 09 s, 78,384,870 states generated / 18,743,593 distinct, depth 61, no violation (output/MC_round2_bfs.out). (A first attempt aborted with a TLC evaluation error in the new MC NeedsRebalance -- `\/` inside an action is branched, so `assign[NoTask]` was evaluated -- rewritten with IF/THEN/ELSE; not a spec-semantics change.) No spec or invariant change.

## Convergence (Round 2)
Round 2: 8/8 traces and MC.cfg pass on the same, final spec with no modification -> converged after 2 rounds. All bug hunting reported in bug-report.md was re-run on this final spec (hunting pass 2); first-pass outputs are kept in output/hunt-pass1/ for the Case B analyses above.

## Bug hunting (final spec, pass 2) - see bug-report.md / findings.json
- [bug] Coordinator commitToTable / CommitState.tableCommitMap: one table under two TableReference keys (uuid null vs set, rolling upgrade <=1.10 -> >=1.11): the first group's floor (whole coordinator position) filters the other group's DataWritten -> LOSS (FloorSound, CheckpointSound; MC_hunt_s1_refsplit[_ckpt]). Bug 1 / MC-1.
- [bug] CommitterImpl.close / KafkaUtils.seekToLastCommittedOffsets: WakeupException from committed() after the buffers were discarded; Connect drops the next polled batch -> LOSS (SourceCommittedCovered; MC_hunt_s3_close_discard, MC_hunt_s3_eager). Bug 2 / MC-2.
- [bug] Coordinator.lastCommittedOffsets: floor only reachable through connector-snapshot ancestry; other writer + expiry + replay (crash before checkpoint at default `latest`, `earliest`, landed-unknown commit) -> DUPLICATES (NoDuplicateRows; MC_hunt_s2_floor_reachability[_latest], MC_hunt_s2_multitable). Bug 3 / MC-3.
- [bug] Coordinator.latestSnapshot vs SnapshotUtil.latestSnapshot: dropped commit branch -> read floor {} vs validator floor from main -> every commit fails (FloorReadValidatorAgree; MC_hunt_s2_branch; safety holds, MC_hunt_s2_branch_safety exhaustive). Bug 4 / MC-4.
- [bug] CommitterImpl.open: election only from added partitions, never retried; a non-Stable describe at the first partition's open() leaves no coordinator -> STALL (NoCoordinatorStall, EventuallyCommitted; MC_hunt_s4_stall, MC_hunt_s4_liveness). Bug 5 / MC-5.
- No violation: MC_hunt_s4_zombie (BFS depth 22 + simulation), MC_hunt_s4_zombie_2ctl (BFS depth 18 + simulation), MC_hunt_s3_faults_only (BFS depth 22 + simulation), MC_hunt_s1_onevariant (exhaustive, depth 74), MC_hunt_s4_multitable_unknown (exhaustive), MC_hunt_s2_branch_safety (exhaustive), MC_hunt_s5_readiness (BFS depth 43).
- Added configs: MC_hunt_s1_refsplit_ckpt.cfg (F1 end-to-end), MC_hunt_s2_floor_reachability_latest.cfg (F3 at the 9fffeb6 default reset), MC_hunt_s2_branch_safety.cfg (safety past the F4 witness).

## Result
Converged in 2 rounds (Round 1: 1 trace fixed via the cooperative-rebalance model + post-processor; Round 2: 3 Case B fixes found by the first hunting pass). Bug hunting: 5 bugs found (3 exactly-once violations: 2 LOSS, 1 DUP; 2 stalls), all on the final spec.
