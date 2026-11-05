# r7 flink: exactly-once of Iceberg's Flink sinks (Specula method, by hand)

- **Track workspace:** `r7/flink/`. It contains:
  - `modeling-brief.md` (with the archaeology record)
  - `spec/` (TLA+, configs, logs, changelog, bug-report)
  - `harness/` (trace rig)
  - `traces/`
  - `repro/` (JUnit)
  - `calib/`
  - `results/`
  - `src-rel/` (released source jars)
- **Code under test:** released jars only, from Maven Central.
  - `iceberg-flink-2.3:1.12.0` with Flink 2.3.0.
  - For calibration only: `iceberg-flink-2.1:1.11.0` and `1.12.0`, with Flink 2.1.1.
- Nothing was posted upstream and nothing was committed.

## Tools

**Operator rig** (`harness/.../V1Rig.java`). It drives the released FlinkSink v1 operators through Flink 2.3 `OneInputStreamOperatorTestHarness`: two `IcebergStreamWriter` subtasks (real `RowDataTaskWriterFactory`, Avro) and one `IcebergFilesCommitter`.
- The rig only picks the schedule.
- The writers' real output records travel to the committer on per-writer FIFO channels. The barriers are aligned, and the rig refuses an unaligned `snapshot`.
- Failover closes all operators and restores a fresh committer from the latest completed checkpoint's `OperatorSubtaskState`. It keeps the same `OperatorID`, and gets a new `JobID` for a resubmit.
- Writers that had finished before the barrier stay finished (FLIP-147). The others restart, and uncovered records are re-fed.
- Lost ack: the `TraceOps` seam (a wrapper over the real InMemoryCatalog `TableOperations`) lets the commit land and then throws a `CommitStateUnknownException`. The fault is attributed by identity.
- **Oracle:** rows read back with `IcebergGenerics`, and the landed snapshot summaries.
- Every scenario name is an enum, so an unknown name throws.

**MiniCluster job** (`repro/.../ReproF1MiniClusterTest.java`). A real Flink 2.3 local job in STREAMING mode:
- checkpoint every 100 ms;
- bounded source `fromSequence(1,400)`, parallelism 2;
- the map in subtask 1 sleeps 5 ms per record (input skew);
- `FlinkSink.forRowData(...).writeParallelism(2).distributionMode(NONE)`;
- no fault injected.

**TLA+** (`spec/base.tla`, `MC.tla`, `Trace.tla`). It models:
- the writers and the committer exactly as IFC/ISW code, annotated `file:line` to the 1.12.0 sources jar;
- Flink channels with barriers and EndOfData, aligned or unaligned;
- the checkpoint coordinator, with notifications that may be lost or arrive late;
- failover to the latest completed checkpoint, and a new job id from a savepoint;
- lost ack, and aborted checkpoints;
- the shared invariants **NoLoss / NoDup / MarkerMonotonic**.

**Trace validation:**
- 10 NDJSON traces from the rig, 189 lines in total, checked with `Trace.tla`.
- Silent actions are only `TriggerCheckpoint` and `Crash`. Each is constrained to the next trace line.
- Not traced (MC only): the aborted checkpoint `SnapshotState(c,FALSE)` and unaligned delivery.

## Calibration

The fixed bug is #16008 / PR #16011, merged 2026-06-01, first release 1.12.0: DynamicCommitter looked up the max committed checkpoint id with the job id of the batch's *last* request, so a replayed committable of the previous job id was committed again. Harness: `calib/.../CalJobIdChangeTest.java`. It drives the released `DynamicCommitter` exactly like the upstream regression test, with an InMemoryCatalog and a row read-back. 20 runs per row.

| Check | 1.11.0 (pre-fix) | 1.12.0 (fixed) | Tool |
|---|---|---|---|
| mixed batch {replay J1@10, new J2@11} | **20/20 BAD** (dupRows=2, dupFileEntries=1, rows=[1,1,2,2,3,4]) | 0/20 (J1@10 signalled alreadyCommitted) | released jars |
| control: J2@11 only | 0/20 | 0/20 | released jars |
| control: replay J1@10 only | 0/20 (alreadyCommitted) | 0/20 | released jars |
| model sanity: unaligned checkpoints must show known #15846 (`MC_hunt_unaligned`) | NoLoss violated (late result overwrites key N before notify N) | — | TLC |
| trace validation discriminates: 2 hand-mutated traces (wrong final units; maxc not advanced) | rejected 2/2 | — | TLC |

The calibration is at the committer API, as in upstream's own test. Whether a mixed batch can actually reach the committer inside Flink 2.3 is a separate question, answered under Refuted.

## Findings

### r7-F1 (REPRODUCED, Major, no upstream report): FlinkSink v1 silently drops a writer's final files when a bounded job's writers finish at different times

**Mechanism.** Every `IcebergStreamWriter.endInput()` flushes under the same key, `END_INPUT_CHECKPOINT_ID = Long.MAX_VALUE` (ISW 35, 83-92).
- `IcebergFilesCommitter.writeToManifestUptoLatestCheckpoint` *puts* each key into `dataFilesPerCheckpoint`, which replaces an existing uncommitted entry (IFC 430-435). It also puts `EMPTY` when the key is absent (IFC 426-428).
- Since FLIP-147 (always on in Flink 2.x), checkpoints keep running after some writers have finished.
- Sequence:
  1. Writer A ends; its END results are consumed by the committer.
  2. A checkpoint completes. Its `snapshotState` moves A's files into `dataFilesPerCheckpoint[END]`.
  3. `notifyCheckpointComplete` commits only `headMap(c)` (IFC 262), so END stays pending.
  4. Writer B ends; its END results are flushed by `endInput` (IFC 417-421), and `put(END, B)` overwrites A's entry.
  5. Only B's files are committed (marker END).
- The checkpoint already recorded A's source as finished, so nothing replays A's records.
- Aligned checkpoints are enough. No failure, no fault, and no error message are needed.

**Evidence.**

| Run (released 1.12.0 / Flink 2.3) | Trigger | Control | Tool |
|---|---|---|---|
| Operator rig, `S4_STAGGERED_END` (A ends, ckpt 2 completes, B writes one more row and ends) vs `S4C_END_TOGETHER` (no ckpt between the ends) | **20/20 lost 1 of 5 rows** (row of A's final flush) | 0/20 | operator harness |
| Operator rig, `S7_STAGGERED_END_EMPTY` (TLC's minimal path: B's END result is empty) vs `S7C` | **20/20 lost 1 of 2 rows** | 0/20 | operator harness |
| MiniCluster job, skewed bounded source, `writeParallelism(2)` vs same job with `writeParallelism(1)` | **18/20, 18/20, 17/20 runs (three samples) lost 200 of 400 rows**, always exactly one whole source split (ids 1..200 or 201..400); no error, job FINISHED | 0/20 lost in all three samples | Flink MiniCluster |
| `MC_hunt_endkey_overwrite` (bounded, aligned, no faults) | NoLoss violated, 14-state counterexample | `MC.cfg` with an unbounded source passes | TLC |
| Counterfactual: merge instead of put (`FIXA`), together with the F2 fix | — | NoLoss holds, 40.86M distinct states (`MC_cf_fixAB_noloss`) | TLC |

**Who is exposed.** FlinkSink v1 (the non-experimental sink, and the one users of #15846 stay on) with:
- STREAMING mode with checkpointing;
- a bounded source (Kafka with stopping offsets, a bounded file source, `fromSequence`, CDC snapshot-only, backfills);
- write parallelism of 2 or more, where the writers' inputs end at different times (unkeyed, `distributionMode(NONE)` chained writers);
- at least one checkpoint between the first and the last writer finishing.

With hash/range distribution the writers end almost together, so the window is small but not zero.

IcebergSink (v2) and the dynamic sink are not affected. Their aggregators stamp their own checkpoint ids (IWA 92-123, DWRA 104-149). This is reasoned only, not run.

**Fix direction.** Merge into an existing uncommitted entry instead of replacing it. At minimum, do not let `put(END, EMPTY)` or `put(END, later)` replace a flushed END entry. The counterfactual model confirms the merge closes NoLoss for this path.

**Duplicate search.**
- gh issues and PRs searched: endInput, END_INPUT_CHECKPOINT_ID, 9223372036854775807, Long.MAX_VALUE, FLIP-147, "final checkpoint", dataFilesPerCheckpoint, EMPTY_MANIFEST_DATA, writeToManifestUptoLatestCheckpoint, "bounded missing rows", "parallelism finished subtask data loss", "endInput overwrite". No report found.
- The same overwriting `put` is the root of open **#15846** (PR #15913 closed). There, reporters and maintainers treat it as unaligned-only: "the existing aligned-checkpoint committer tests all pass … the committer's assumption only breaks under unaligned checkpoints".
- r7-F1 is a different trigger that needs no unaligned checkpoints and no fault. It shows that the defect is reachable with default settings.
- Not in r5 or r6 (E-4, E-33 and E-45 are different code).

### r7-F2 (REPRODUCED, Major, symptom partly known via #2033): a failover that restores a checkpoint holding the END key stamps `flink.max-committed-checkpoint-id = Long.MAX_VALUE`; the next failover drops every pending checkpoint, and the marker moves backwards

**Mechanism.** On restore, IFC 194-201 commits `tailMap(L)` of the restored map with marker `uncommittedDataFiles.lastKey()`.
- That marker is `Long.MAX_VALUE` whenever a writer had finished before the restored checkpoint, and the END entry was not yet committed (its notify was lost, or the failure came first).
- The field `maxCommittedCheckpointId` stays at L (IFC 191).
- Two outcomes follow:
  - **(a) Loss.** The next failover's lookup returns MAX (SU 85-107). `tailMap(MAX, false)` is empty, so the pending checkpoints of that state are dropped (IFC 196). After that every `notifyCheckpointComplete` is skipped, because `c > MAX` is false (IFC 241).
  - **(b) Marker regression.** If notify(c) arrives before another failover, it commits with marker c < MAX, so MarkerMonotonic is violated for the job.

**Evidence (released 1.12.0, operator rig, 20 runs each).**

| Scenario | Result | Control |
|---|---|---|
| `S5_RESTORE_END_KEY`: w0 ended; ckpt 1; failover; w1 writes row 3; ckpt 2 (notify lost); failover | **20/20 lost row 3** (markers END, END) | `S5C_RESTORE_NO_END_KEY` (w0 not ended at ckpt 1): 0/20 |
| `S6_MARKER_BACKWARDS`: same, but notify(2) arrives after the first failover | **20/20 markers END → 2 → END** (no loss) | S5C: monotonic 20/20 |
| TLC `MC_hunt_endkey_restore` | MarkerMonotonic violated | — |
| TLC `MC_hunt_endkey_restore_loss_fixA` (F1 fixed, to isolate F2) | NoLoss violated, same path as S5 | `MC_cf_fixA_only` violated, `MC_cf_fixAB` (F2 fixed: restored checkpoint id as marker) passes NoLoss |

**Severity.** Major (data loss). It needs a bounded input, plus two failovers after the first writer finished, the second before the next checkpoint's notify. That window is narrower than F1's. When F1 also applies, F1 usually loses the data first.

**Duplicate search.**
- **#2033** (closed, stale) reported a MAX marker after which the restored job never committed. It was caused by a Flink bug (FLINK-21132, endInput on a non-drain stop-with-savepoint) and fixed in Flink, with no Iceberg change.
- r7-F2 reaches the MAX marker through an ordinary failover, and adds the `tailMap(MAX)` loss and the marker regression. **New trigger and new consequence of a known symptom.**
- By reasoning only (not run): a new job restored from a `stop-with-savepoint --drain` savepoint looks up the drained job's MAX marker and skips every notify until its own next failover. That is the #2033 symptom through a legitimate path.

## Refuted / no violation

- **v1 on an unbounded, aligned stream.** Crash ≤2, lost ack ≤1, aborted checkpoint ≤1, new job id from savepoint ≤1, notify lost or late in any order: `MC_conv_faults` passes (33.96M distinct states, MaxCkpt 3).
  - `MC_hunt_recovery` (MaxCkpt 4): BFS stopped at the 30 min cap after 138,988,520 distinct states, depth 29, no violation (incomplete). A 10 min simulation (depth ≤80) checked 191,558,568 states over 3,063,630 random traces (mean length 24): no violation.
  - Rig runs S1 normal, S2 lost notify + restore, S3 lost ack (CSUE after landing) + restore: 0/20 lost, 0/20 dup, markers monotonic.
- **IcebergCommitter `last.jobId()`** (IC 140-142, the same pattern as pre-#16011, and claimed in declined #18374). It is not reachable on Flink 2.3.
  - `CommitterOperator` commits one checkpoint per `Committer.commit` call (CommitterOperator 164-174, CheckpointCommittableManagerImpl 145-161).
  - IcebergSink emits one committable per checkpoint from a single aggregator subtask, so a batch never mixes job ids.
  - Code reasoning only, not run.
- **F1 in IcebergSink and the dynamic sink.** Not applicable, since the aggregators assign their own ids (reasoning).

## Known, not re-reported

- **endInput commit followed by failover before the final checkpoint replays and re-commits** (TLC `MC_hunt_endinput_dup`: NoDup; `MC_cf_fixAB`: MarkerMonotonic). This is acknowledged upstream: #2033 comment, and the #18123 review ("we would then double commit").
- The job-id-reuse family: #18098/#18123 (fixed after 1.12.0), #18303 (dynamic, open), and the restore-before-first-commit residual from the #18123 review.
- #14425 / #17901 (in-flight commit).
- E-4, E-33, E-45.

## Status table

| id | Claim | Status | Decided by |
|---|---|---|---|
| r7-F1 | END-key overwrite loses a finished writer's final files (aligned, no fault) | **confirmed** | operator harness 20/20 vs 0/20 (×2); MiniCluster 18/20 vs 0/20; TLC |
| r7-F2 | restore of END key stamps MAX: later restore drops pending; marker regresses | **confirmed** | operator harness 20/20 vs 0/20; TLC |
| endInput-failover duplicate | NoDup | known (design limitation) | TLC only |
| v2 mixed-job batch | duplicate | refuted on Flink 2.3 (unreachable) | code reasoning |
| v1 unbounded recovery paths | none | no violation within bounds | TLC + harness |

## Process notes

- Two spec fidelity fixes during hunting (`spec/changelog.md` C1, C2). Both were spurious counterexamples caused by my own modelling, and both are documented.
- The harness refused to create `r7/flink/analysis-report.md`, treating it as a report file. The audit trail (archaeology table and coverage counts) is in `modeling-brief.md` §7.1 instead.
- One subagent did part of the read-only `gh` archaeology. Its results were checked against the sources and against my own searches.
- Committer state is read by reflection for traces only, never written. No released class is shadowed.

## Re-run from a clean shell (for independent verification)

All commands use JDK 21 through the `gw` launcher and released jars from Maven Central. The Gradle cache is warm, so no new downloads are needed. Each Gradle test run prints PASSED; the test asserts that its controls are clean, and the counts are written to `r7/flink/results/`.

```
cd /Users/I074560/Downloads/experiments/iceberg-verify/r7/flink/harness

# r7-F1 and r7-F2 at operator level, triggers next to controls (about 1 min; 20 runs per scenario)
./gw test --tests 'org.apache.iceberg.flink.sink.ReproOperatorLevelTest' -Dr7.runs=20
cat ../results/repro-operator.txt
#  expect (deterministic):
#   S4_STAGGERED_END        20/20 lost=1 rows=[1,2,4,5]      S4C_END_TOGETHER        20/20 lost=0
#   S7_STAGGERED_END_EMPTY  20/20 lost=1 rows=[2]            S7C_END_TOGETHER_EMPTY  20/20 lost=0
#   S5_RESTORE_END_KEY      20/20 lost=1 rows=[1,2,4] markers=[1000,1000]   S5C_RESTORE_NO_END_KEY 20/20 lost=0
#   S6_MARKER_BACKWARDS     20/20 lost=0 markerBackwards=true markers=[1000,2,1000]
#   S1/S2/S3 (normal, lost notify+restore, lost ack+restore) 20/20 lost=0 dupRows=0
#   (1000 encodes Long.MAX_VALUE in the report)

# r7-F1 end to end on a Flink 2.3 MiniCluster, no fault injected (about 2 min, 20 runs per mode)
./gw test --tests 'org.apache.iceberg.flink.sink.ReproF1MiniClusterTest' -Dr7.runs=20
cat ../results/repro-f1-minicluster.txt
#  expect (timing dependent): TRIGGER_TWO_WRITERS_SKEWED loses 200/400 rows in most runs
#     (three samples: 18/20, 18/20, 17/20), the lost ids always one whole source split
#     (1..200 or 201..400); CONTROL_ONE_WRITER_SKEWED 0 lost in 20/20 (all three samples)

# calibration (#16008 / PR #16011): iceberg-flink-2.1 + Flink 2.1.1
cd ../calib && ./gw test -PicebergVersion=1.11.0 -Dr7.runs=20 && ./gw test -PicebergVersion=1.12.0 -Dr7.runs=20
cat ../results/calib-16011-1.11.0.txt ../results/calib-16011-1.12.0.txt
#  expect: 1.11.0 MIXED_BATCH 20/20 dupRows=2; 1.12.0 MIXED_BATCH 20/20 dupRows=0; controls 0 on both

# traces + TLA+
cd ../harness && ./gw test --tests '*V1TraceScenariosTest*'      # regenerates ../traces/v1_*.ndjson
cd ../spec && ./run.sh trace all                                 # expect 10/10 "No error has been found"
./run.sh mc MC.cfg                                               # pass, 19,110 distinct
./run.sh mc MC_conv_faults.cfg                                   # pass, 33,957,444 distinct (~4 min)
./run.sh mc MC_hunt_endkey_overwrite.cfg; python3 pp.py output/MC_hunt_endkey_overwrite.log   # NoLoss violated
./run.sh mc MC_hunt_endkey_restore.cfg                           # MarkerMonotonic violated
./run.sh mc MC_hunt_endkey_restore_loss_fixA.cfg                 # NoLoss violated (F2 isolated)
./run.sh mc MC_cf_fixAB_noloss.cfg                               # pass, 40,864,133 distinct (~5 min)
./run.sh mc MC_hunt_unaligned.cfg                                # NoLoss violated (known #15846)
```

## Hand-over: per finding

### r7-F1: precondition, chain, overlaps

**Realistic precondition.** No operator mistake and no failure are needed. A correct caller hits it by running:
- the FlinkSink v1 builder (`FlinkSink.forRowData(...)...append()`);
- in STREAMING mode with checkpointing on;
- on a source that ends (bounded Kafka offsets, a bounded file source, a backfill or snapshot-only CDC job);
- with write parallelism ≥ 2;
- where the writer subtasks finish at different times and at least one checkpoint completes in between.

Unkeyed / `distributionMode(NONE)` writers chained to skewed source splits make this likely. With hash or range shuffles all writers end at nearly the same moment, which narrows the window.

**Not affected.** IcebergSink v2 and the dynamic sink (reasoning only). Unbounded streams (TLC, and harness S1–S3).

**Source chain (1.12.0 sources jar):**
1. `IcebergStreamWriter.java:35` sets `END_INPUT_CHECKPOINT_ID = Long.MAX_VALUE`.
2. ISW:83-92: `endInput()` calls `flush(END_INPUT_CHECKPOINT_ID)`.
3. ISW:104-118: emits `FlinkWriteResult(END, files)`.
4. `IcebergFilesCommitter.java:408-415`: `processElement` groups the result under key END.
5. IFC:206-224: `snapshotState(c)` calls `writeToManifestUptoLatestCheckpoint(c)` (IFC:425-439) and stores `dataFilesPerCheckpoint`, now including key END, into checkpoint state.
6. IFC:230-244 and IFC:262: `notifyCheckpointComplete(c)` commits only `headMap(c, true)`, so END stays pending.
7. The later writer runs `endInput()`, and its END results arrive.
8. IFC:417-423: committer `endInput()` calls `writeToManifestUptoLatestCheckpoint(END)`, which does `put(END, …)` at IFC:430-435, or `put(END, EMPTY)` at IFC:426-428. This replaces the pending END entry.
9. IFC:422 → 256-285: `commitUpToCheckpoint` commits only the replacement.

The finished writer's source is already marked done in the completed checkpoint (FLIP-147), so nothing replays the lost records.

**Overlaps.**
- Same overwriting `put` as open **apache/iceberg#15846** (https://github.com/apache/iceberg/issues/15846; PR https://github.com/apache/iceberg/pull/15913, closed unmerged). That issue is about *unaligned* checkpoints. Its reporters and maintainers state that aligned checkpoints are safe; F1 shows they are not, for the END key.
- The END key came in with PR **#10526** (https://github.com/apache/iceberg/pull/10526, 1.7.0).
- No overlap with E-4, E-33 or E-45.
- No issue or PR reports this trigger (searches listed above).

### r7-F2: precondition, chain, overlaps

**Realistic precondition.** It is the same bounded multi-writer job as F1, plus task failures. A correct caller can hit it: TaskManager loss or any task exception is enough, and no operator action is needed. But it needs two failovers in a narrow window:
1. Failover #1 after one writer finished and a checkpoint holding its END entry completed, but before the committer's endInput.
2. Failover #2 after a later checkpoint completed but before its `notifyCheckpointComplete` commit ran.

Only the data of the checkpoint restored at failover #2 is lost. Notifies after that are skipped, because the field is MAX (IFC:241), but the final endInput still commits whatever is still pending. With a single failover the only effect is that the marker moves backwards (S6), with no data loss. **Rare. Major only because of the loss branch.**

**Source chain:**
1. IFC:182-192: restore looks up `getMaxCommittedCheckpointId(table, restoredJobId, …)` (`SinkUtil.java:85-107`), which returns L.
2. IFC:194-196: `tailMap(L, false)`, which includes END.
3. IFC:199-201: `maxUncommittedCheckpointId = lastKey()`, which is Long.MAX_VALUE. `commitUpToCheckpoint(…, MAX)` then commits with summary `flink.max-committed-checkpoint-id = 9223372036854775807` (IFC:390).
4. The field stays at L (IFC:191; the restore commit never updates it).
5. Failover #2: the lookup now returns MAX, so `tailMap(MAX, false)` is empty (IFC:196) and the restored pending checkpoint is dropped.
6. Afterwards `checkpointId > maxCommittedCheckpointId` is false (IFC:241), so notifies are skipped.
7. S6 branch: notify(c) after failover #1 commits marker c < MAX (IFC:243, 390), so the marker moves backwards.

**Overlaps.**
- Symptom known from **apache/iceberg#2033** (https://github.com/apache/iceberg/issues/2033, closed stale): a MAX marker after which the job never commits. That was triggered by Flink bug FLINK-21132 (endInput on a non-drain stop-with-savepoint), not by a failover restore, and it did not report the `tailMap` loss or the marker regression.
- Related "skip until the checkpoint id exceeds the table marker" behaviour: #10892, and #18098 / PR #18123 (https://github.com/apache/iceberg/pull/18123).
- No overlap with E-4 (expiry), E-33 or E-45.

### No new bug found (coverage)

**FlinkSink v1 on unbounded aligned streams.** TLC `MC_conv_faults` completed exhaustively: 33,957,444 distinct states, MaxCkpt 3, crash ≤2, lost ack ≤1, aborted checkpoint ≤1, new job id ≤1, notifications lost or late in any order. `MC_hunt_recovery` (MaxCkpt 4) was stopped at its 30-min cap after 138,988,520 distinct states, depth 29, no violation (incomplete). A 10-min simulation of the same config checked 191,558,568 states over 3,063,630 random traces: no violation. Harness S1–S3: 0/20 loss or dup.

**IcebergSink v2 and the dynamic sink.** No new bug found. Reasoning only: one checkpoint per commit call on Flink 2.3, and aggregator-assigned checkpoint ids. Calibrated committer-level harness only.

## Lead re-run (2026-11-04, independent of the track)

- Source: `iceberg-flink-2.3-1.12.0-sources.jar` SHA-1 1e89cf6d… matches Maven Central; the extracted
  IcebergStreamWriter / IcebergFilesCommitter / SinkUtil are byte-identical to the jar. F1 chain re-read at
  ISW 35, 83-92 and IFC 217, 262, 418-434 of that source: confirmed.
- Operator level (`harness/`, ReproOperatorLevelTest, 20 runs): identical to the track's table. S4 and S7 lose a row
  20/20, controls S4C/S7C 0/20; S5 loses a row 20/20, control S5C 0/20; S6 marker backwards 20/20; S1-S3 clean.
- MiniCluster (ReproF1MiniClusterTest, 20 runs, public API only, restarts disabled, rows read back from the table):
  trigger lost exactly one whole source split (200/400 rows) in 17/20 runs; control (writeParallelism 1, same skew)
  0/20. Fourth independent sample, consistent with the track's 18, 18, 17.
- Calibration #16008/#16011: 1.11.0 MIXED_BATCH dupRows=2 20/20; 1.12.0 dupRows=0 20/20; controls clean on both.
- Upstream: no issue or PR matches the F1 trigger (gh search, 6 queries). Root cause is shared with OPEN #15846
  (the `put` that replaces a pending checkpoint's manifest); maintainers there concluded the loss needs unaligned
  checkpoints, and the fix PR #15913 was closed unmerged. **Novelty, lead wording: new trigger of the open #15846
  root cause, reachable with aligned checkpoints and no failure; not a separate root cause.**
- F2 is reproduced at operator level only (S5/S6). Not yet run end to end on a MiniCluster; treat as REPRODUCED
  (operator level), lower confidence than F1.
