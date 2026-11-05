# Bug Report — Apache Iceberg Kafka Connect sink (exactly-once commit protocol)

Source: `apache/iceberg` @ 9fffeb6, module `kafka-connect`. Paths below: `CI` = `kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/channel/`, `KC` = `kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/`, `CORE` = `core/src/main/java/org/apache/iceberg/`. Kafka runtime references are Kafka 3.9.2 (`WorkerSinkTask`, `ConsumerCoordinator`, `GroupCoordinator`).

All results are on the final spec, which converged after 2 rounds (`changelog.md`). It was validated against 8 real traces and checked with `MC.cfg` (exhaustive, 18.7 M distinct states, no violation).

## Summary

- **Scenarios tested**: 5 (brief §2: S1 floor over-claim, S2 floor reachability, S3 close/rewind, S4 coordinator multiplicity, S5 completion accounting), 17 hunting configs.
- **Bugs found**: 5.
  - 3 violate exactly-once: Bug 1 (LOSS), Bug 2 (LOSS), Bug 3 (DUPLICATES).
  - 2 are permanent stalls: Bug 4, Bug 5.
  - All 5 match mechanisms from the Phase-1 code analysis (F1, F2, F3, F4, F9). This run turns them into model-checked counterexamples with the trigger conditions made explicit.
  - The known issues excluded by the target guidance (#18006 `latest` reset skip, the in-house null-offset seek, #15293 control-topic recreation) are switched off in every config and are not re-reported.
- **Answer to the goal question**: the sink is **not** exactly-once under all the listed conditions.
  - Records whose source offsets are committed can be missing from the table:
    - during a rolling upgrade across 1.11.0 (Bug 1);
    - when the connector is paused or resumed while a rebalance callback runs (Bug 2).
  - Committed records can be committed again (Bug 3). This needs another table writer and snapshot expiry, combined with a coordinator replay.
  - Under the modeled zombie-coordinator, partial-commit, multi-table and unknown-outcome conditions, no loss or duplicate was found (Not Reproduced table).
- **Configs run** (BFS 30 min, 1 worker each; simulation 30 min, depth 100, where the BFS depth was ≤ 25):
  - `MC_hunt_s1_refsplit`, `MC_hunt_s1_refsplit_ckpt`*, `MC_hunt_s1_onevariant`
  - `MC_hunt_s2_floor_reachability`, `MC_hunt_s2_floor_reachability_latest`*, `MC_hunt_s2_multitable`, `MC_hunt_s2_branch`, `MC_hunt_s2_branch_safety`*
  - `MC_hunt_s3_close_discard`, `MC_hunt_s3_eager`, `MC_hunt_s3_faults_only`
  - `MC_hunt_s4_zombie`, `MC_hunt_s4_zombie_2ctl`, `MC_hunt_s4_multitable_unknown`, `MC_hunt_s4_stall`, `MC_hunt_s4_liveness`
  - `MC_hunt_s5_readiness`
  - (* = variant added during hunting: an end-to-end confirmation, the 9fffeb6 default reset, and a safety-only run past an expected witness.)

---

## Bug 1: One table under two `TableReference` keys — the first group's floor silently drops the second group's DataWritten (LOSS)

- **Scenario**: S1 / MC-1(a) (analysis-report F1). Rolling upgrade from ≤1.10 to ≥1.11 with the coordinator already upgraded.
- **Severity**: High. Silent data loss; the only symptom is an INFO log "found nothing to commit". The trigger is a routine operation.
- **Invariant violated**: `FloorSound`. End-to-end: `CheckpointSound`.
- **Config**: `MC_hunt_s1_refsplit.cfg` (`RefVariants = {"legacy","v"}`, `LegacyTasks = {t1}`). End-to-end: `MC_hunt_s1_refsplit_ckpt.cfg`.
- **Counterexample**: 18 states, `spec/output/MC_hunt_s1_refsplit_bfs.out`. End-to-end, 20 states: `spec/output/MC_hunt_s1_refsplit_ckpt_bfs.out`.

### Trace Summary

1. Initial join. t1, still on ≤1.10 worker code, gets source partition 1. t2, on ≥1.11 code, gets partition 0, which is the first partition. t2's `open()` elects it and starts coordinator 1 (states 2-8).
2. Each task buffers one record. The coordinator sends StartCommit (control offset 0).
   - t1 answers in one transaction: `DataWritten(T1, table_uuid = null)` @1, `DataComplete` @2, and its source offset p1 → 1.
   - t2 answers: `DataWritten(T1, uuid = U)` @3, `DataComplete` @4, and p0 → 1 (states 9-11).
3. The coordinator consumes offsets 0-4, so its position is 5. It becomes ready and starts a full commit. `tableCommitMap()` holds **two** groups for T1: `<T1, uuid=null>` and `<T1, uuid=U>` (state 16).
4. Group `<T1, null>` runs `commitToTable`:
   - `committedOffsets = {}`;
   - `mergedOffsets = max({}, controlTopicOffsets = {0:5}) = {0:5}`;
   - the CAS lands snapshot 1 with t1's file and `kafka.connect.offsets = {"0":5}` (states 17-18).
   - t2's DataWritten @3 is now visible, not in T1, and below T1's floor 5, so **`FloorSound` is violated**.
5. End-to-end (`_ckpt`, states 19-20):
   - Group `<T1, U>` reads `committedOffsets = {0:5}`, its envelope @3 is filtered (3 < 5), and it logs "found nothing to commit".
   - Every table task "succeeded", so `commitConsumerOffsets()` checkpoints `-coord` at 5 and `clearResponses()` drops the buffer.
   - t2's record ⟨0,0⟩, whose source offset 1 is committed, is never written to T1. No later coordinator re-reads it. **`CheckpointSound` is violated** and the loss is permanent.
6. With parallel commit threads, the other order also loses the data:
   - both groups read `{}`;
   - the second CAS fails `SnapshotAncestryValidator` (`{}` ≠ `{0:5}`), the full commit rethrows, and the coordinator exits;
   - the task FAILS with `NotRunningException`;
   - after restart, the replay reads floor `{0:5}` and filters the group.

### Root Cause

- `CommitState.tableCommitMap()` groups envelopes by the full `TableReference`, and `TableReference.equals/hashCode` include `uuid` (added in d85f8a87a, 1.11.0).
- A DataWritten from a ≤1.10 worker decodes with `uuid == null`. `commitToTable` explicitly tolerates that, "to handle the upgrade case" (#15011). But that null reference forms its own commit group for the same physical table.
- Each group stores the coordinator's *entire* control position as the table's floor, not the offsets of the envelopes it actually committed. Envelopes of the same table in any other group therefore fall below the floor and are filtered forever.
- Other paths that produce two references for one table during an upgrade:
  - uuid-less DataWritten still in the control topic, read by an upgraded coordinator;
  - catalog-name or identifier spelling changes.

### Affected Code

- `CI/CommitState.java:139-144`: `groupingBy(tableReference())` uses the full reference, including the uuid.
- `kafka-connect/kafka-connect-events/src/main/java/org/apache/iceberg/connect/events/TableReference.java:144-160`: `uuid` is part of `equals`/`hashCode`.
- `KC/data/IcebergWriterFactory.java:72-78`: a ≥1.11 worker sets the table uuid; a ≤1.10 worker does not.
- `CI/Coordinator.java:256-263`: `uuid == null` passes the replaced-table check.
- `CI/Coordinator.java:270-275`: floor = `max(committed, controlTopicOffsets)`, i.e. the whole coordinator position.
- `CI/Coordinator.java:277-285`: filter `offset >= committedOffsets`.
- `CI/Coordinator.java:308-310`: silent "nothing to commit".
- `CI/Coordinator.java:216-217`: checkpoint and clear after all tasks.

### Recommendation

Group DataWritten by table identity (catalog, namespace, name) and keep the uuid only as a per-envelope consistency check. Either commit all envelopes of a physical table in one snapshot, or derive each group's stored floor from the envelopes it contains rather than from the coordinator's whole position. Add a unit test that mixes `uuid = null` and `uuid = X` envelopes for one table, with `commit.threads` set to 1 and to 2.

---

## Bug 2: `WakeupException` from `consumer.committed()` in `close()` — no rewind, and the next polled batch is dropped (LOSS)

- **Scenario**: S3 / MC-3 (analysis-report F2). A connector pause/resume (`WorkerSinkTask.transitionTo` → `consumer.wakeup()`) lands while a rebalance callback is inside `CommitterImpl.close()`.
- **Severity**: High. Silent loss with any protocol (cooperative, eager, lost); the task keeps running.
- **Invariant violated**: `SourceCommittedCovered`.
- **Config**: `MC_hunt_s3_close_discard.cfg` (cooperative) and `MC_hunt_s3_eager.cfg` (eager). `FixNullSeek = TRUE`, so the known null-offset seek is excluded.
- **Counterexample**: 15 states each, `spec/output/MC_hunt_s3_close_discard_bfs.out`, `spec/output/MC_hunt_s3_eager_bfs.out`.

### Trace Summary (cooperative)

1. t1 owns source partitions {0,1} and hosts coordinator 1 (states 2-5).
2. A churn rebalance moves partition 1 to t2. In round 1, t1 revokes {1} (state 6).
3. `close([1])` begins:
   - `stopWorker()` discards every buffer (state 7);
   - `hasLeaderPartition` returns false (state 8).
4. Pause/resume calls `consumer.wakeup()` while the callback runs (state 9).
5. `seekToLastCommittedOffsets` calls `consumer.committed()`, which throws `WakeupException`; only `IllegalStateException` is caught.
   - No partition is rewound.
   - `WorkerSinkTask.onPartitionsRemoved` stores the exception as `rebalanceException` (state 10).
6. t1 finishes the rebalance and keeps {0} (state 11).
7. The next `poll()` returns record ⟨0,0⟩, so the position is now 1. Then `pollConsumer` rethrows the stored `WakeupException`. `iteration()` catches it as a pause/resume wakeup and continues; **the batch is never passed to `put()`** (state 12).
8. t1 puts ⟨0,1⟩ and answers the next StartCommit (states 13-15). The answer's transaction commits source offset p0 → 2 with a DataWritten that holds only ⟨0,1⟩.
9. ⟨0,0⟩ is below the committed offset, in no DataWritten and not in the table. **`SourceCommittedCovered` is violated**, and the record is permanently lost.

The eager trace is the same: revoke all, re-open {0}, then the poll that rethrows drops ⟨0,0⟩. A second manifestation follows from the same steps: when a retained partition had buffered records that `stopWorker()` discarded, the skipped seek leaves its position past them, and they are lost too.

### Root Cause

- `CommitterImpl.close()` discards all buffered state first, then runs fallible rewind steps. `KafkaUtils.seekToLastCommittedOffsets` calls `consumer.committed()` outside its `IllegalStateException` catch.
- A wakeup pending during the callback makes `committed()` throw `WakeupException` (`ConsumerNetworkClient` disables wakeups only in `ConsumerCoordinator.close()`).
- Connect stores the exception and rethrows it only after `consumer.poll()` has returned records and advanced the positions. Because it is a `WakeupException`, Connect treats it as benign, so the task is not failed and nothing triggers a rewind.

### Affected Code

- `CI/CommitterImpl.java:161-187`: `close()` runs `stopWorker()` (`:163`, all buffers) before `hasLeaderPartition` (`:179`) and `seekToLastCommittedOffsets` (`:186`).
- `CI/KafkaUtils.java:60-85`: `committed()` at `:66-67` sits outside the `try/catch (IllegalStateException)` at `:75-82`.
- Kafka 3.9.2 runtime:
  - `WorkerSinkTask.java:204-207`: `transitionTo` calls `consumer.wakeup()`;
  - `:786-790`: the callback exception is stored;
  - `:501-505`: it is rethrown after `poll()` returned records;
  - `:248-262`: `WakeupException` is treated as benign.

### Recommendation

- Never let a `WakeupException` (or any exception) escape `close()` after the buffers were discarded without either rewinding or failing the task:
  - catch `WakeupException` around `committed()` and retry (a wakeup is one-shot);
  - or wrap any failure in a `ConnectException`, so that Connect fails the task (safe: offsets are not committed).
- Better still, seek *before* discarding the buffers, or rewind through `SinkTaskContext.offset(...)` instead of the raw consumer.

---

## Bug 3: The dedup floor is reachable only through the ancestry of the connector's own snapshots — replays re-commit data (DUPLICATES)

- **Scenario**: S2 / MC-2 (analysis-report F3).
- **Severity**: High. Duplicate rows, an exactly-once violation. It needs an environment composition: another writer, snapshot expiry, and a replay window.
- **Invariant violated**: `NoDuplicateRows`.
- **Config**: three configs, one per replay window:
  - `MC_hunt_s2_floor_reachability_latest.cfg`: 9fffeb6 default `auto.offset.reset = latest`, crash between table commit and checkpoint;
  - `MC_hunt_s2_floor_reachability.cfg`: `earliest`, which is PR #18006 or a user override through `iceberg.kafka.auto.offset.reset`, `KafkaClientFactory.java:55`;
  - `MC_hunt_s2_multitable.cfg`: unknown-outcome table commit that landed.
- **Counterexample**: 37 states (`spec/output/MC_hunt_s2_floor_reachability_latest_bfs.out`), 30 states (`..._floor_reachability_bfs.out`), 33 states (`..._s2_multitable_bfs.out`).

### Trace Summary (default `latest`, 37 states)

1. Commit cycle 1 completes and checkpoints `-coord` at offset 1 (states 6-11).
2. Cycle 2:
   - t1 answers StartCommit with DataWritten @3 (record ⟨0,0⟩); its source offset is committed (states 12-15);
   - the full commit lands snapshot 1, holding the files of @3, with `kafka.connect.offsets = {"0":4}` (states 16-18).
3. The coordinator host crashes after `appendOp.commit()` and before `commitConsumerOffsets()` (state 19).
4. The task restarts and a new coordinator starts at the committed `-coord` offset 1, so it re-buffers DataWritten @3 (states 20-28).
5. StartCommit 3: t1 answers with DataWritten @6 and a full commit begins (states 29-33).
6. Another writer (compaction or another job) commits snapshot 2 to main; custom summary properties are not carried forward (state 34). `expire_snapshots` removes snapshot 1, which is no longer a ref head (state 35).
7. `lastCommittedOffsetsForTable` walks the ancestry: 2 has no key, its parent 1 is expired, so the walk stops and returns `{}`.
   - @3 is not filtered.
   - The validator compares the expected `{}` with ancestry `{}` and passes.
   - Snapshot 3 lands with @3 again (states 36-37). **`NoDuplicateRows` is violated.**

Other windows reach the same mechanism:
- No prior checkpoint plus the `earliest` reset (30 states).
- `CommitStateUnknownException` after the CAS landed (33 states): the full commit rethrows, the task FAILS, and the restart replays from the last checkpoint.

### Root Cause

- The only durable replay guard is the `kafka.connect.offsets.<topic>.<group>` property of the *first ancestor* snapshot that carries it.
- Other writers do not carry the property forward. The ancestry walk stops at the first expired parent. "No property found" yields `Map.of()`, which both disables the filter and satisfies the validator.
- Any replay of already-committed DataWritten is therefore re-committed once the connector's last snapshot has expired behind a newer foreign snapshot. Such replays happen after:
  - a crash between table commit and checkpoint;
  - a landed-unknown commit;
  - multi-table partial success with `max-consecutive-failures = 1`;
  - lost `-coord` offsets.

### Affected Code

- `CI/Coordinator.java:395-405`: `lastCommittedOffsetsForTable` walks the ancestry of the branch head.
- `CI/Coordinator.java:407-415`: the first snapshot with the key wins, otherwise `Map.of()`.
- `CI/Coordinator.java:277-285`: no floor means no filter.
- `CI/Coordinator.java:361-372`: the validator check `expectedOffsets.equals(lastCommittedOffsets(ancestry))` passes for `{} == {}`.
- `CORE/util/SnapshotUtil.java:243-254`: ancestry iteration stops at a missing (expired) parent.
- `CORE/SnapshotProducer.java:297-304,377-389`: validator ancestry.
- Replay windows:
  - `CI/Coordinator.java:325` → `:216`: crash window;
  - `CI/Coordinator.java:176-179`: `CommitStateUnknownException` is rethrown.

### Recommendation

- Make the floor independent of snapshot retention. Options:
  - search all retained snapshots, or the connector's commit-id lineage, rather than only ancestors;
  - store the floor in table metadata that expiry does not remove (e.g., table properties, updated in the same commit);
  - refuse to commit envelopes below the `-coord` checkpoint when no floor is found but the table already contains connector snapshots.
- Document that `expire_snapshots` retention must exceed the worst-case coordinator replay window.

---

## Bug 4: After the commit branch is dropped, the read floor and the validator floor disagree — every later commit fails (STALL)

- **Scenario**: S2 / MC-5 (analysis-report F4). A commit branch is configured, and write-audit-publish fast-forwards main to the branch and removes the branch.
- **Severity**: Medium. Availability only: duplicate-freedom still holds.
  - Full commits fail with `ValidationException`, so the task FAILS on every restart.
  - Partial commits are retried forever.
  - Data reaches the table only after manual intervention; data whose control-topic retention expires first is lost.
- **Invariant violated**: `FloorReadValidatorAgree` (F4 witness). The safety-only run `MC_hunt_s2_branch_safety.cfg` completes exhaustively with `NoDuplicateRows` and `AtMostOneCommitPerDataWritten` holding.
- **Config**: `MC_hunt_s2_branch.cfg` (`UseBranch = TRUE`).
- **Counterexample**: 14 states, `spec/output/MC_hunt_s2_branch_bfs.out`.

### Trace Summary

1. The first commit lands on branch `b`: snapshot 1 with `kafka.connect.offsets = {"0":3}`; `b` points at 1 and main is empty (states 2-13).
2. An operator fast-forwards main to `b` and drops `b` (state 14).
3. The two floor readers now differ:
   - `Coordinator.latestSnapshot(table, b)` returns `table.snapshot("b")`, which is null, so `committedOffsets = {}`;
   - the validator's parent is `SnapshotUtil.latestSnapshot(base, "b")`, which falls back to main, whose ancestry carries `{0:3}`.
4. The disagreement is permanent. The commit that would recreate `b` is exactly the one that fails validation.

### Root Cause

The coordinator and Iceberg core resolve a missing branch differently. The coordinator reads "no snapshot, no floor"; `SnapshotProducer` falls back to main. The `SnapshotAncestryValidator` then compares two different ancestries.

### Affected Code

- `CI/Coordinator.java:388-400`: `latestSnapshot` returns `table.snapshot(branch)` (null when the ref is missing), which gives `Map.of()`.
- `CORE/util/SnapshotUtil.java:478-489`: `latestSnapshot(metadata, branch)` falls back to main.
- `CORE/SnapshotProducer.java:297-304,377-389`: the validator runs on the main-fallback ancestry.
- `CI/Coordinator.java:176-179`: `ValidationException` is fatal for full commits.
- `CI/Coordinator.java:165-174`: partial commits swallow it with no budget.

### Recommendation

Resolve the floor with the same rule as `SnapshotProducer` (fall back to main when the branch ref is missing). Alternatively, fail fast with an explicit configuration error when the configured commit branch does not exist.

---

## Bug 5: Coordinator election runs only in `open()` of *added* partitions and is never retried — a missed election leaves no coordinator (STALL)

- **Scenario**: S4 / MC-5 (analysis-report F9).
- **Severity**: Medium.
  - The connector silently stops committing: tasks stay RUNNING, no StartCommit is ever sent, and workers buffer without bound.
  - There is no loss: offsets are committed only with DataWritten.
  - It recovers only when some later rebalance *adds* the first partition to a task again.
- **Invariant violated**: `NoCoordinatorStall` (safety encoding) and `EventuallyCommitted` (liveness).
- **Config**: `MC_hunt_s4_stall.cfg` and `MC_hunt_s4_liveness.cfg` (`StaleViews = TRUE`).
- **Counterexample**: 13 states (`spec/output/MC_hunt_s4_stall_bfs.out`); 12 states plus stuttering (`spec/output/MC_hunt_s4_liveness_bfs.out`).

### Trace Summary (`MC_hunt_s4_stall`)

1. t1 owns {0,1} and hosts coordinator 1. A commit starts and t1 answers with DataWritten @1 (states 2-8).
2. t1 is kicked out (poll interval). The new generation gives {0,1} to t2 (state 9).
3. t1's lost-partition `close()` describes the group, sees partition 0 elsewhere, and calls `stopCoordinator()`, which terminates coordinator 1 (states 10-12).
4. t2's `open({0,1})` calls `hasLeaderPartition(added)`. Its `describeConsumerGroups` lands while the group is not Stable (t1 is rejoining after its lost-close), and a non-Stable classic group is described without member assignments. `findFirstTopicPartition` returns null, so t2 is not the leader (state 13).
5. Every task is up, DataWritten @1 is uncommitted, no coordinator can run, and no election or reactive rebalance is pending. **`NoCoordinatorStall` is violated.**
6. When t1's rejoin completes the follow-up generation, it is sticky: t2 keeps partition 0, so no `open()` of partition 0 happens and the stall persists.

In the liveness run, the coordinator host crashes, a rebalance gives partition 0 to t1, and t1's `open()` misses the election. The DataWritten is never committed although commit budget remains: **`EventuallyCommitted` is violated**.

### Root Cause

- `CommitterImpl` derives leadership once per `open(addedPartitions)` from a live describe: "is the first assigned partition among the *added* ones".
- If that describe is taken while a concurrent membership change makes the group non-Stable, no coordinator starts. Nothing re-evaluates leadership later:
  - `save()` and `processControlEvents()` never elect;
  - `startCoordinator()` runs only from `open()`.
- This is the election-side counterpart of the known de-election problem #17637, which rests on the same describe premise.

### Affected Code

- `CI/CommitterImpl.java:140-151`: `open()` elects only from `addedPartitions`.
- `CI/CommitterImpl.java:77-91`: `hasLeaderPartition` uses a live describe.
- `CI/CommitterImpl.java:122-128`: `findFirstTopicPartition` returns null for empty assignments.
- `CI/KafkaUtils.java:44-54`: `consumerGroupDescription`.
- `CI/CommitterImpl.java:198-212`: `processControlEvents` never (re)elects.

### Recommendation

Re-check leadership when no coordinator is running, e.g. in `processControlEvents()` with a back-off, using the task's full `context.assignment()`. Treat a describe without assignments as "unknown, retry" instead of "not leader".

**Model limitation**: `StaleViews` admits the non-Stable describe at any `open()`.
- The stall counterexample's interleaving is realistic once t2's `open()` is ordered after t1's lost-close rejoin; the spec admits that order (`CanAdd` ignores lost members).
- The liveness counterexample omits the concurrent membership change that a real non-Stable describe needs; a restart of the crashed task is the obvious candidate.
- The follow-up generation would keep partition 0 with its owner in both cases.

---

## Not Reproduced

| Scenario | Config | States Explored | Result |
|------------|--------|-----------------|--------|
| S4 zombie coordinators (MC-4: zombie keeps consuming and committing after the successor's `initTransactions`, lost member, task FAILED without join, `-coord` moves, partial commits swallowing fencing and validation errors, unknown outcome, timeout with late DataWritten) | `MC_hunt_s4_zombie.cfg` | BFS 30 min: 46,350,649 generated / 12,685,274 distinct, depth 22. Simulation 30 min (depth 100): 29,339,746 states checked, 259,523 traces (mean length 91) | No violation (`NoDuplicateRows`, `FloorSound`, `NoStrandedDataWritten`, `CheckpointSound`, `AtMostOneCommitPerDataWritten`, `SourceCommittedCovered`) |
| S4 with 2 control partitions (split between zombie and successor, never-seen partitions not checkpointed) | `MC_hunt_s4_zombie_2ctl.cfg` | BFS 30 min: 47,804,314 / 12,327,959, depth 18. Simulation 30 min: 25,069,484 states checked, 216,043 traces (mean length 100) | No violation (same six invariants) |
| S3 fault points other than `committed()` (fatal exception at every close/open step, lost member, KIP-447 rejection between cooperative rounds) | `MC_hunt_s3_faults_only.cfg` | BFS 30 min: 49,377,821 / 14,294,013, depth 22. Simulation 30 min: 31,811,659 states checked, 290,994 traces (mean length 94) | No violation (`SourceCommittedCovered`, `NoDuplicateRows`) |
| S1 one variant (retained buffer after CommitFailed with budget 2, `-coord` bounce, late DataWritten, checkpoint failure, crash/restart) | `MC_hunt_s1_onevariant.cfg` | Exhaustive: 21,056,940 / 8,696,217, depth 74 (24 min 42 s) | No violation (`FloorSound`, `NoStrandedDataWritten`, `CheckpointSound`, `SourceCommittedCovered`, `NoDuplicateRows`) |
| Q4 multi-table, one commit lands, the other fails or has an unknown outcome; retry budget 2; replay | `MC_hunt_s4_multitable_unknown.cfg` | Exhaustive: 28,455 / 17,309, depth 47 | No violation |
| S2 commit branch: safety after the F4 witness | `MC_hunt_s2_branch_safety.cfg` | Exhaustive: 2,667 / 1,277, depth 42 | No violation (`NoDuplicateRows`, `AtMostOneCommitPerDataWritten`, `FloorMonotone`) |
| S5 completion accounting (counts not deduplicated, frozen total, incremental move during a commit, 2 timeouts) | `MC_hunt_s5_readiness.cfg` | BFS 30 min: 33,886,976 / 11,414,493, depth 43 | No violation of `ValidThroughSound`, `FloorSound`, `NoDuplicateRows` within bounds. The F11/#18322 family is not reproduced: both first-pass counterexamples were fidelity gaps, see below |
| Known issues (#18006 `latest` skip, null-offset seek, #15293 control-topic recreation, #18012 deliberate skips) | all | — | Not testable by design: switched off (`CoordReset`, `FixNullSeek = TRUE`, `MaxRecreate = 0`, no table drop modeled) per the target guidance |

---

## Spec fixes during hunting (Case B)

The first hunting pass produced three counterexamples that the implementation cannot reach. Each was fixed in the base spec. Traces and `MC.cfg` were re-run as Round 2, and every config above was re-run on the final spec; first-pass outputs are in `spec/output/hunt-pass1/`. Details are in `changelog.md`.

1. **≤1.10 coordinator** (`Open`). The first refsplit counterexample hosted the coordinator on the legacy task. A ≤1.10 coordinator has no uuid in `TableReference.equals`, so it never splits groups. A legacy leader now starts no coordinator; the 1.10 coordinator logic is out of scope. The violation persisted with the upgraded coordinator (Bug 1).
2. **Frozen total** (`TotalViews`). With `StaleViews`, `totalPartitionCount` could be anything in 1..assigned. The range was a proxy for the cooperative round-1 view, which is explicit since Round 1. A non-Stable describe has no assignments and never elects. The total is now exactly the generation's assigned count.
3. **Described assignment after a failure** (`ResetTaskLocal`). A FAILED or crashed member's partitions were dropped from `owner` (the describe view) at once, letting another task's `open()` elect itself with a partial total. The broker still describes the old generation until the next one, or describes no assignments once the LeaveGroup lands. `owner` now keeps the entries, and MC's reactive rebalance keys on live members' `assign`.

## Observation from trace validation (not a model-checking finding)

Trace `zombie_coop_round2_rejected` shows a *healthy* task FAILING during a cooperative rebalance. Its Worker answered a StartCommit with source offsets after the round-2 join had completed but before its consumer processed the join response. The broker rejects `TxnOffsetCommit` with `ILLEGAL_GENERATION` (`GroupCoordinator.scala:1022-1043`), and `Channel.send` rethrows out of `put()`.

The rejection is safe: the transaction aborts and the records are re-delivered. It is an availability problem (analysis-report F15 family): the failed task stays FAILED until restarted. The spec models it since Round 1 (`Rejoin`), and all hunts include it.
