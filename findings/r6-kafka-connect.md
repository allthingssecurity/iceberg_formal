# r6 kafka-connect: exactly-once of the Iceberg Kafka Connect sink

Track workspace: `r6/kafka-connect/` (harness/, tla/, src-rel/ = released source jars 1.8.1..1.12.0,
results.txt = harness matrix, pr12372.diff). Released jars only (Maven Central). Nothing posted
upstream, no commits.

## Tools

**Executable harness** (`r6/kafka-connect/harness`, `./run.sh <ver> <scenario> <runs> [k=v]`).
Drives the released `IcebergSinkTask` / `CommitterImpl` / `Worker` / `Coordinator` (no connector
class modified) with a deterministic schedule:

- Kafka: in-JVM `FakeKafka` behind a classpath shadow of the package-private
  `org.apache.iceberg.connect.channel.KafkaClientFactory` (identical API 1.8.1..1.12.0; the run
  aborts if the shadow is not the loaded class). Modelled: control topic log, read_committed
  transactions (records + `sendOffsetsToTransaction` offsets visible atomically), transactional.id
  fencing, `auto.offset.reset` exactly as the factory sets it (`latest` via putIfAbsent), consumer
  groups with committed offsets, EAGER rebalance of control-topic groups (client default
  RangeAssignor: members restart from the committed offset), KIP-447 generation check on the
  source-group offsets. Unknown client methods throw.
- Connect runtime: `Emu` emulates WorkerSinkTask: real `WorkerSinkTaskContext` (connect-runtime
  3.9.2, the class `KafkaUtils` reflects on) over a proxy source consumer (assignment / committed /
  seek / position / groupMetadata); cooperative (ICR) revoke-then-assign with `close(revoked)`
  while still assigned; eager control mode; put/open/close exceptions fail the task (close(all) +
  stop), operator restart in the heal phase; process crash = no callbacks, all clients dead.
  `Admin.describeConsumerGroups` during close(): `target` (default, most favourable), `partial`
  (cooperative first round: revoked partition owned by nobody), `unstable` (not Stable, empty).
- Catalog: every task's catalog (`iceberg.catalog.catalog-impl`) shares one InMemoryCatalog;
  per-instance closed flag; fault plan on `TableOperations.commit` (CFE/CSUE/RTE before or after
  apply, crash of the committing process before/after apply), injected exception classes are
  attributed structurally.
- Coordinator thread is lock-stepped: an empty control-topic poll parks until the scheduler steps it.
  commit.interval-ms=0, commit.timeout-ms=2000 with explicit sleeps.
- Oracle: reads the table back (IcebergGenerics), compares (partition, offset) with what was produced
  and with the connect group's committed source offsets: `lost` = offset < committed but not in the
  table (permanent), `dupRows`, `dupFiles` = data file path added by >1 snapshot. Every run ends with
  a fault-free heal phase (restart failed tasks, reassign orphans, commit cycles).

**TLA+** (`r6/kafka-connect/tla/KcSink.tla`, `./run.sh <cfg>|all`): tasks, source partitions with
fetch positions and committed offsets, worker buffers, control topic as a log, coordinator
(position, buffer, controlTopicOffsets, current commit, phases table/offsets), coordinator group
offset, table rows + stored control offset, crashes/restarts, ICR revoke/assign, eager rewind of the
coordinator, table-commit failure. Invariants NoLoss (every record below the committed source
offset is in the table or still recoverable from the control topic) and NoDup. Knobs: CLOSE
(1.8.1 vs 1.9.0+), SEEKNULL, CRESET, REPLAY (1.11 put vs 1.12 max/skip), FAIL (retry vs terminate).
Bounds: 2 tasks, 2 partitions, 2 records each, 2 commits, control topic <= 7 entries.

## Calibration

| check | pre-fix | fixed | tool |
|---|---|---|---|
| #12372 ICR close-without-open data loss (harness `icr-close`) | 1.8.1: **5/5 BAD** (lost 10 = p0@10..19 each run) | 1.9.0: 0/5, also 1.10.0 / 1.11.0 / 1.12.0: 0/5 | harness |
| #12372 in the model (`cal1_prefix` / `cal1_fixed`) | NoLoss violated (poll, close(P1) drops buffer, reopen, commit) | no error, 1.89M distinct states | TLC |
| #16282 / #17340 eager rewind of a zombie coordinator, fixed in 1.12.0 by #17933 (high-water mark) + skip guard (harness `zombie-rewind replay=1`; needs describe=partial during close) | 1.10.0 **5/5**, 1.11.0 **5/5** BAD (dupFiles=1, dupRows=10) | 1.12.0: 0/5 | harness |
| same, control `replay=0` (clean leader move) | 1.10.0 / 1.11.0 / 1.12.0: 0/5 | | harness |
| #16282 in the model (`cal2_put_rewind` / `cal2_max_rewind`) | NoDup violated | no error, 8.08M distinct states | TLC |
| smoke (no faults, no rebalance) | 0/5 on all five releases | | harness |

## Findings

### F1 (confirmed, Major, looks new): ICR rebalance before a partition's first commit loses the records the task had already consumed for the partitions it keeps

1.9.0+ `CommitterImpl.close(revoked)` (1.12.0 lines 161-186) calls `stopWorker()`, which throws
away the worker's unannounced buffered records for **all** partitions of the task, then
`KafkaUtils.seekToLastCommittedOffsets` (1.12.0 lines 60-80) rewinds the retained partitions to the
group's committed offset, but only `if (offsetAndMetadata != null)`. A retained partition that has
never had a committed offset (new connector before its first commit, partition added to a topic,
new topic matched by a regex) is not rewound: the framework keeps its fetch position past the
discarded records, the next worker answer commits a higher source offset, and the skipped records
are never written. Silent: no lag, no error.

- Harness `icr-nocommit` (T0 owns P0,P1, consumed both, P1 moves to T1 before any commit):
  1.9.0 / 1.10.0 / 1.11.0 / 1.12.0 each **5/5 BAD, lost=10 (p0@0..9)**.
  Controls: `precommit=1` (P0 already has a committed offset) 0/5 on all four;
  `eager=1` (eager protocol: positions refetched, auto.offset.reset=earliest) 0/5 on all four.
- TLC `f1_seeknull_skip`: NoLoss violated in 6 states (Poll P0@0, Revoke P1, Poll P0@1, StartCommit,
  Respond). Same model with SEEKNULL=earliest (`cal1_fixed`) passes.
- Assumptions: consumer-level cooperative assignment (CooperativeStickyAssignor or KIP-848 protocol,
  the mode #12372 targets; with the default eager assignor the bug does not occur);
  `KafkaConsumer.committed()` returns null for never-committed partitions (documented);
  WorkerSinkTask does not touch retained partitions on cooperative revocation.
- Fix direction: for null committed offsets seek to the beginning (or to the position of the first
  record the discarded writer saw), or keep the worker's data for retained partitions.
- Duplicate search (gh, read-only): "seekToLastCommittedOffsets", "committed offset null seek",
  "cooperative", "ICR", "incremental cooperative", "revoked partitions", "Kafka Connect seek",
  "KafkaUtils", "data loss": no issue/PR reports it. PR #17450 (closed) and #16843 (closed) touch
  `seekToLastCommittedOffsets` but keep the null skip.

### F2 (confirmed on 1.12.0, Major, DUPLICATE of open PR #18006; related closed #17551): coordinator replaced before its first control-topic checkpoint starts at `latest` and skips answered DataWritten

`KafkaClientFactory.createConsumer` puts `auto.offset.reset=latest` (line 55) for the coordinator
group too. Workers commit source offsets in the same transaction as DataWritten, before the table
commit. If the coordinator group has no committed offset yet and the coordinator is replaced before
it commits, the new coordinator starts at the log end: the files are never committed, the source
offsets are.

- Harness `coord-latest` (coordinator task crashes after workers answered; P0 moves): 1.9.0 /
  1.10.0 / 1.11.0 / 1.12.0 each **5/5 BAD, lost=20**. Controls: `precommit=1` 0/5,
  `reset=earliest` (iceberg.kafka.auto.offset.reset) 0/5 on all four.
- New in 1.12.0: the first-ever table commit failing with a non-CommitFailedException (or CFE with
  the default max-consecutive-failures=1) now terminates the coordinator, the task fails, and the
  restarted coordinator starts at `latest`: harness `fault kind=RTE_BEFORE precommit=0`
  **1.12.0 5/5 BAD (lost=20)** vs 1.9.0 / 1.10.0 / 1.11.0 0/5 (they keep the buffer and retry);
  control `precommit=1` 0/5 on all. This is the same root cause; plausibly the "lost after restart
  with Glue errors" symptom of #13752.
- TLC: `f2_latest_move`, `f2_latest_crash`, `f2_latest_fail` violate NoLoss; `f2_earliest_fail`
  (CRESET=earliest) passes (13.0M distinct states).
- Window: until the coordinator group's first committed offset (new connector / new control topic or
  group id / expired group offsets).

### Refuted / no violation on 1.12.0 (harness, 5 runs each, all 0/5; 1.11.0 identical)

- CSUE after apply, CSUE before, crash of the coordinator before / after the table commit
  (before the control-offset commit), RuntimeException before / after apply, CommitFailedException
  x5: single table and two tables (multi-table, stopOnFailure) - no loss, no duplicate rows or files.
  The per-table stored control offset + `envelope.offset >= stored` filter handles recovery.
- #16282 on 1.12.0: refuted (0/5); reproduced on 1.10.0/1.11.0 (see calibration).
- #13752 as "drop on Glue/catalog exception": with a checkpoint present, refuted on 1.9.0..1.12.0
  (`fault RTE_BEFORE precommit=1` 0/5); the loss shows only through F2 (no checkpoint).
- Stale commitId responses: accepted by design (DataWritten of any commit id buffered); no
  violation found in harness or model.

### Side observation (Minor, availability)

1.9.0/1.10.0 `containsFirstPartition` throws `ConnectException("No partitions assigned, cannot
determine leader")` when describe returns no assignments (eager revocation with a Stable-but-empty
view); in WorkerSinkTask a close() exception fails the task. Not a safety issue once failure is
handled; not pursued.

## TLC results (`r6/kafka-connect/tla/results.txt`)

```
cal1_prefix: Error: Invariant NoLoss is violated. 1929 states generated, 1120 distinct states found, 639 states left on queue. 
cal1_fixed: Model checking completed. No error has been found. 4737648 states generated, 1885928 distinct states found, 0 states left on queue. 
cal2_put_rewind: Error: Invariant NoDup is violated. 537656 states generated, 231068 distinct states found, 100286 states left on queue. 
cal2_max_rewind: Model checking completed. No error has been found. 21726613 states generated, 8075783 distinct states found, 0 states left on queue. 
f1_seeknull_skip: Error: Invariant NoLoss is violated. 1251 states generated, 745 distinct states found, 477 states left on queue. 
f2_latest_crash: Error: Invariant NoLoss is violated. 687 states generated, 365 distinct states found, 240 states left on queue. 
f2_latest_move: Error: Invariant NoLoss is violated. 263 states generated, 144 distinct states found, 85 states left on queue. 
f2_latest_fail: Error: Invariant NoLoss is violated. 559 states generated, 315 distinct states found, 203 states left on queue. 
f2_earliest_fail: Model checking completed. No error has been found. 36220170 states generated, 13020018 distinct states found, 0 states left on queue. 
rel112_all: Error: Invariant NoLoss is violated. 909 states generated, 527 distinct states found, 366 states left on queue. 
fix112_all: Model checking completed. No error has been found. 162312270 states generated, 55235585 distinct states found, 0 states left on queue. 
fix112_all_retry: Model checking completed. No error has been found. 182633140 states generated, 60608300 distinct states found, 0 states left on queue. 
wit_fix_commits: Error: Invariant W_AllCommitted is violated. 28718 states generated, 12865 distinct states found, 7342 states left on queue. 
wit_fix_rewinds: Error: Invariant W_Rewound is violated. 265 states generated, 147 distinct states found, 92 states left on queue. 
neg_unknown_close: Error: Assumption line 39, col 8 to line 39, col 36 of module KcSink is false. 
```

`rel112_all` = released 1.12.0 knobs (SEEKNULL=skip, CRESET=latest, REPLAY=max, FAIL=terminate,
crash+rewind+fail on): NoLoss violated (F1/F2). `fix112_all*` = 1.12.0 with SEEKNULL=earliest and
CRESET=earliest, everything on (MaxMoves=1; with MaxMoves=2 it passed 117M distinct states / 6 GB
without finishing and was stopped): no error. Witnesses show full commit and the rewind are reachable
in the fixed model; an unknown knob value is rejected by ASSUME. Model limits: one coordinator
variable (a zombie is modelled only as the rewind it causes), no multi-table, no equality deletes,
2x2x2 bounds.

## Process note

While stopping my own oversized TLC run I used `pkill -f tlc2.TLC` (twice) and `pkill -f harness.Main`;
these match by command line and could have stopped another r6 track's TLC/harness JVM running at the
same moment. Other tracks should re-check runs interrupted around 14:07-14:29 IST.

## Re-run

```
cd r6/kafka-connect/harness
./run.sh 1.8.1  icr-close 5 ; ./run.sh 1.9.0 icr-close 5                  # calibration 1
./run.sh 1.11.0 zombie-rewind 5 ; ./run.sh 1.12.0 zombie-rewind 5         # calibration 2
./run.sh 1.12.0 icr-nocommit 5 ; ./run.sh 1.12.0 icr-nocommit 5 precommit=1 ; ./run.sh 1.12.0 icr-nocommit 5 eager=1   # F1
./run.sh 1.12.0 coord-latest 5 ; ./run.sh 1.12.0 coord-latest 5 reset=earliest                                         # F2
./run.sh 1.12.0 fault 5 kind=RTE_BEFORE precommit=0 ; ./run.sh 1.11.0 fault 5 kind=RTE_BEFORE precommit=0              # F2 via 1.12 terminate
./matrix.sh 5      # everything -> ../results.txt
cd ../tla && ./run.sh all     # -> out/<cfg>.log
```
