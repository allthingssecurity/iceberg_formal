# CR-5 — confirmation record (Phase 2 + verdict)

- **Source**: Code Review (Scenario 4 residual; MC_hunt_s4_zombie.cfg / _2ctl.cfg / MC_hunt_s4_multitable_unknown.cfg returned no violation)
- **Novelty**: NEW. No filed report of this mechanism. The two-coordinator trigger is known (#17637, #16016, both open). PR #18039's review and the `Channel.java:165-169` javadoc treat the snapshot window as accepted design (developer awareness, not a bug report).
- **Status**: MASKED
- **Severity**: Low (data harm masked; residual effects are bounded)
- **Location**: kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/channel/Coordinator.java:303
- **Reproduction test**: repro/test_bugCR-5_zombie_coord_table_commit.sh (+ .java). Levels reached: 0 (scenario C) and 1 (scenarios A, B).
- **Persistent record**: confirmation/CR-5/issue.json (`specula findings validate` → valid)

Evidence: `investigation.md` (Phase 1), `repro/CR-5.log`, `repro/CR-5.out/run{1,2}/*.summary.json|*.sink.log`.

## Outcome

Two live coordinators were created through the real code path:

1. Task 0 fails (transient).
2. Task 1 starts coordinator B.
3. The operator restarts task 0.
4. Task 1's `close()` logs "found no partitions assigned across all members, cannot determine leader", so B is not stopped.
5. Task 0 starts C, whose `initTransactions()` fences B.

After being fenced, B did the following:

- **A.** Landed its in-flight Iceberg CAS 19.6 s after C started. No `CommitToTable` was published for it. Its `-coord` membership blocked C (`PreparingRebalance`, 2 members, both `[]`).
- **B.** Lost a CAS race to C. C had replayed from `-coord` offset 16 and committed the same `DataWritten`. The validator then rejected B: "stale offsets: Expected: {0=16} Committed: {0=33}".
- **C.** With 2 control partitions, B kept polling and owned `ctl-1`. It consumed the workers' answers to C's StartCommit 858bf701, and its timed-out partial commit landed them under B's own commit-id/task-id.

The hypothesized consequence did not occur in any of the 6 runs (3 scenarios × 2): no duplicates, no missing rows, no file added twice, no snapshot whose stored offsets claim missing files, no stranded `DataWritten`. The masks fired:

- Floor filter (`Coordinator.java:277-285`): C re-read `DataWritten@23` and added only its own 2 files; in C, "found nothing to commit".
- `SnapshotAncestryValidator` (`Coordinator.java:361-372`, `SnapshotProducer.java:386-388`): rejected B's racing CAS in scenario B.

See the final verdict text in the agent response for the full report.
