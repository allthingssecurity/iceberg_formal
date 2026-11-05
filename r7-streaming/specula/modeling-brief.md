# Modeling Brief: Apache Iceberg Kafka Connect sink — exactly-once commit protocol

## 1. System Overview

- **System**: `apache/iceberg` module `kafka-connect` @ `9fffeb6` (2026-11-02), Java, ≈1,550 LOC of protocol logic
  (`channel/*`, `IcebergSinkTask`, `data/SinkWriter`, `kafka-connect-events`); docs claim exactly-once via KIP-447.
- **Category A — Distributed / message-passing**: Connect sink tasks on several workers plus one elected coordinator
  thread interact only via Kafka (control topic, two consumer groups, transactions) and a CAS catalog; the adversary is
  crash, rebalance, zombie instances and unknown commit outcomes, not memory ordering.
- **Protocol**: on `StartCommit` each Worker sends ONE Kafka txn {DataWritten(files per table), DataComplete(assignment),
  source offsets → Connect group}. The coordinator buffers DataWritten, commits each table with a **floor** (control-topic
  offsets in the snapshot summary), then checkpoints its `connect-<name>-coord` group offsets.
- **Choices that matter**: (1) source offsets live only in the Connect group (`preCommit` returns `{}`); the table stores
  *control-topic* offsets; (2) the floor written for T is the coordinator's whole control position, while files are
  grouped per `TableReference`; (3) dedup = ancestry walk to the first snapshot with the key + `SnapshotAncestryValidator`;
  table commits are not Kafka-fenced; (4) leadership from `describeConsumerGroups` snapshots at `open/close`;
  (5) `close()` discards all buffers, then rewinds by `seek`.
- **Concurrency**: per task one Connect thread (put/flush/open/close + Worker); a coordinator thread on the elected task;
  per-table commit executor threads; `terminate()` does not join. Details and atomicity map: `analysis-report.md` §1.

**Priority-question coverage** (finding IDs F* refer to `analysis-report.md` §3.2):

| Q | Code-analysis status | Where |
|---|---|---|
| Q1 task rebalance/crash/restart | No dup path. LOSS: known null-offset seek (#18767) and **new F2** (`WakeupException` in `close()`, any protocol); otherwise safe (atomic worker txn, KIP-447, `requireStable`) | S3, MC-3, T-3 |
| Q2 coordinator move/crash/zombie | Per-table at-most-once holds (validator + floor); LOSS only via known #18006; liveness defects F5–F10 | S4, MC-4, MC-5 |
| Q3 timeout + late DataWritten | Late files committed once; floors never cover unconsumed records; valid-through-ts wrong both ways (F11, #17077) | S5, MC-4 |
| Q4 multi-table failure/unknown | Retry/restart safe — **except new F1** (one table under two `TableReference`s loses a group); skips by design (#18012) | S1, MC-1, T-1 |
| Q5 offsets missing/older than floors | Recreated control topic ⇒ LOSS (known #15293, F12); missing `-coord` offsets ⇒ `latest` ⇒ LOSS (known #18006); **floor unreachable ⇒ DUP on replay (F3)**; PR #18006's `earliest` makes dedup depend on snapshot retention | S1, S2, MC-1, MC-2 |

## 2. Scenarios

### Scenario 1: The stored floor claims more than the commit contained (HIGH)

**Mechanism**: `commitToTable` stores `max(floor, coordinator position)` — the coordinator's *entire* control position —
as table T's floor, but commits only one `TableReference` group; every T-envelope below that position that is not in
this commit is filtered forever, although its worker already committed the source offsets.

**Evidence**:
- Historical: #15293 (open, 5 reports: recreated control topic ⇒ silent loss), #12357, #16360 (closed fix), #18012 (skips
  then checkpoint), #18006 (`latest` jump), 8da07dcae/#14525 (floor = max), #17340/#17933 (floor regression ⇒ dup).
- Code (**F1, new, confirmed**): `CommitState.java:139-144` groups by full ref; `TableReference.java:144-160` puts `uuid`
  in `equals/hashCode` (d85f8a87a, 1.11.0); `Coordinator.java:256` passes `uuid==null`; `:270-285` floor + filter;
  `:308-310` silent skip; `:216-217` checkpoint + clear. ≤1.10 (no uuid) and ≥1.11 DataWritten for one table during a
  rolling upgrade — the "upgrade case" #15011 meant to handle — split into two groups; the second is silently filtered
  (sequential) or fails validation, fails the task and is filtered on replay (parallel). Also catalog-name/identifier changes.
- Code (F12): in-memory high-water mark (`Channel.java:126-139`) skips every record of a recreated control topic ⇒ stall first.

**Affected code paths**: `CommitState.tableCommitMap`; `Coordinator.doCommit/commitToTable`; `IcebergWriterFactory.createWriter:72-78`.

**Suggested modeling approach**:
- Variables: DataWritten field `ref = <table, variant>`; floors via `snaps[T]` (S2); `cPos[k][c]`, `cBuf[k]`; ghost `rows[T]`.
- Actions: per group `ReadFloor(k, ref)` (filter; merged = max(floor, cPos)) and `CasCommit(k, ref)` as separate steps,
  groups of one table in parallel or sequence; `SkipGroup(k, ref)` (NoSuchTable/UUID mismatch).
- Constants: `RefVariants ∈ {{"v"}, {"legacy","v"}}`; env `ControlTopicReset` (default OFF — known #15293).

**Priority**: High. **Rationale**: confirmed loss with a routine trigger maintainers believed handled; also explains #15293/#12357.

### Scenario 2: The dedup floor is only reachable through the commit branch's snapshot ancestry (HIGH)

**Mechanism**: The only durable replay guard is the first ancestor snapshot carrying the connector's key; other writers do
not carry it forward, the walk stops at expired parents, and the coordinator reads it from a different branch head than
the validator — so it can vanish (re-commit ⇒ DUP) or disagree with the validator (⇒ permanent STALL).

**Evidence**:
- Historical: reviewer wombatu-kun on #17925 ("if that snapshot expires, replay registers the same files again"; unfiled);
  #17340/#16282; #14510/#14506/#14515; PR #18006 (`earliest` ⇒ every coordinator start is a full replay).
- Code (F3): `Coordinator.java:395-415` (first ancestor with key, else `{}`), `SnapshotUtil.java:243-254`,
  `SnapshotProducer.java:394-473` (summary not carried forward), `Coordinator.java:371` (`{}=={}` passes). Replay windows:
  crash between `:325/340` and `:216`; multi-table partial success (max-consecutive-failures 1); `-coord` offsets lost/reset.
- Code (**F4, confirmed**): floor read via `table.snapshot(branch)` (`Coordinator.java:388-393`, null if missing) vs the
  validator's `SnapshotUtil.latestSnapshot` main fallback (`:478-489`) ⇒ every commit fails once main carries the key
  (WAP branch dropped after fast-forward; branch switched): full ⇒ task FAILED each restart; partial ⇒ retried forever.

**Affected code paths**: `Coordinator.lastCommittedOffsetsForTable/lastCommittedOffsets/latestSnapshot/offsetValidator`; `SnapshotProducer.apply/runValidations`.

**Suggested modeling approach**:
- Variables: `snaps[T]` = sequence of <floor|NONE, writer, live>; `branchExists[T]`; derived `ReadFloor(T)` vs `ValidatorFloor(T)`.
- Actions: `OtherWriterCommit(T)`, `ExpireSnapshots(T)` (keep newest k), `DropBranch(T)`, `LoseCoordOffsets`.
- Constant `CoordReset ∈ {"latest","earliest"}` (`earliest` = PR #18006).

**Priority**: High. **Rationale**: exactly-once becomes conditional on table-maintenance policy; the pending upstream fix widens the window.

### Scenario 3: `close()` discards all buffered state before fallible rewind steps (HIGH)

**Mechanism**: `CommitterImpl.close()` stops the Worker (dropping every partition's buffer) and only then runs fallible
steps (Admin describe, `consumer.committed()`, `seek`); any outcome that neither rewinds nor kills the task leaves
positions past records whose offsets the next worker transaction commits.

**Evidence**:
- Historical: #12372/51abab10e (fixed), #11298, **#18767 / in-house null-offset seek (KNOWN)**, #16843, #13756, #16366.
- Code (**F2, new, confirmed incl. Kafka 3.9.2**): `KafkaUtils.java:66-67` `committed()` (outside the catch at `:77`) raises
  a pending wakeup (`ConsumerNetworkClient.java:252-295,527-532`; disabled only in `ConsumerCoordinator.close()` `:978`);
  Connect wakes the consumer on every pause/resume (`WorkerSinkTask.java:204-207`), stores the callback exception
  (`:789`), rethrows it after `poll()` returned records (`:497-505`) and treats it as a benign wakeup (`:248-262`) ⇒ task
  continues, retained partitions not rewound, polled batch dropped (eager/cooperative/lost) ⇒ LOSS.
- Election coupling (F9): `open()` elects only from *added* partitions (`CommitterImpl.java:141-151`); never retried.

**Affected code paths**: `CommitterImpl.close/open/hasLeaderPartition`, `KafkaUtils.seekToLastCommittedOffsets`, `Worker.stop`; runtime `WorkerSinkTask` callbacks.

**Suggested modeling approach**:
- Variables: `pos[t][p]`, `srcCommitted[p]` (Nat|NULL), `wBuf[t]`, `closeStep[t]`, `stored[t] ∈ {none,fatal,wakeup}`, `owner[p]`, `gen`.
- Actions: `CloseDiscard`, `CloseElect`, `CloseSeek` (each may raise `fatal` ⇒ task FAILED, or `wakeup` if `PauseResume`
  fired during close); `PollReturn(t)` delivers or drops the batch per `stored[t]`; `RevokeEager/RevokeCooperative/Lose`; `Open`.
- Constants: `FixNullSeek = TRUE` (exclude known #18767); `Protocol ∈ {"eager","cooperative"}` (KIP-848 ≈ cooperative).

**Priority**: High. **Rationale**: must-cover runtime interaction; second loss path independent of the known one; 5 historical fixes.

### Scenario 4: More than one or no coordinator; table commits are unfenced (MEDIUM)

**Mechanism**: The stable coordinator `transactional.id` fences a superseded coordinator's sends and checkpoints, but not
its consumption, `-coord` group membership or Iceberg commits; `terminate()` neither joins nor interrupts the thread.

**Evidence**:
- Historical: #17637/#18687, #16016/#16020/#16156, #17349/#17350/#17910, #13593/#13752, #17551/#13995 (fixed by
  #17552/#18039), #16282, #14395, 0747b6044 ⇒ 357020aa6 (guard removed by mistake) ⇒ cced19af0.
- Code (confirmed unless noted): F5 `shutdownNow()` drains queued table tasks, `Tasks.waitFor` `future.get()` has no
  timeout ⇒ thread hangs forever; F6 `Channel.java:123-151` never re-checks `terminated` ⇒ under continuous traffic (shared
  default control topic) a zombie keeps the `-coord` partition; F7 long commit ⇒ Kafka `CommitFailedException` (≠
  Iceberg's, `Coordinator.java:57,176`) ⇒ task FAILED; F8 (plausible) zero coordinators via `CommitterImpl.java:199-203,224`;
  F10 (plausible) `Tasks.throwOne` masks `ProducerFencedException` ⇒ task FAILED.

**Affected code paths**: `CoordinatorThread.run/terminate/isFenced`, `Coordinator.process/commit/terminate`, `Channel.consumeAvailable/commitConsumerOffsets`, `CommitterImpl.processControlEvents/startCoordinator/stopCoordinator`.

**Suggested modeling approach**:
- Variables: `coord[k]` = <task, epoch, alive, terminated, cPos, cHwm, cCache, cBuf, phase, pending>; `coordEpoch`;
  `cgOwner[c]`, `cgCommitted[c]`; `leaderView[t]`.
- Actions: `Elect(t)` from a nondeterministic (stale/empty) view; `StartCoordinator` bumps epoch; `Terminate(k)` only sets a
  flag; zombies keep `Consume`/`CasCommit`; `CgRebalance` moves control partitions (position := checkpoint or reset).
  Check 1 and 2 control partitions.

**Priority**: Medium. **Rationale**: bug-dense history; safety argued to hold (bounded confirmation answers Q2); confirmed liveness defects.

### Scenario 5: Completion accounting, partial commits and valid-through-ts (MEDIUM)

**Mechanism**: A commit is "complete" when a summed DataComplete count reaches a total frozen at coordinator
construction; counts are not deduplicated per partition, DataWritten of any commit id are carried forward, and partial
(timeout) commits swallow every exception without a budget.

**Evidence**:
- Historical: #18322, #17925, #17077/#17080, #17811/#17593, #17193/#18290/#18030/#14818/#11818/#11796, #13117, #16389.
- Code (F11): `Coordinator.java:106-107,149-153,166-174`; `CommitState.java:51-71,117-128,146-165`; `Worker.java:79-90`;
  all passes refuted loss/dup for late DataWritten — consequences are META and STALL.

**Affected code paths**: `CommitState.addResponse/addReady/isCommitReady/validThroughTs`, `Coordinator.process/commit`.

**Suggested modeling approach**: `cCount[k]`, `cTotal[k]`, `cReady[k]`; actions `Timeout(k)`, `WorkerAnswerLate`,
`IncrementalMove(p, t→t')` during a commit; ghost coverage per full commit for `ValidThroughSound`.

**Priority**: Medium. **Rationale**: Q3 coverage; mostly known/metadata; its safety parts are cheap regression invariants.

## 3. Modeling Recommendations

### 3.1 Model

| What | Why | How |
|---|---|---|
| Worker answer = one atomic Kafka txn with KIP-447 generation check | base of all scenarios; env semantics verified (report §3.1 E1-E3) | action `WorkerAnswer`; guard `taskGen[t]=gen`, else task FAILED |
| Floor per (table, key) = max(floor, coordinator position); commit groups keyed by `TableReference` | S1 (F1), #15293 | `ReadFloor` + `CasCommit` per group; `RefVariants` |
| Snapshot ancestry: other writers, expiry, branch existence, validator's main fallback | S2 (F3, F4) | `snaps[T]`, `ReadFloor` vs `ValidatorFloor` |
| `-coord` checkpoint after all tables; reset `latest`/`earliest`; never-seen partitions never checkpointed | S1/S2/S4, #18006 | `cgCommitted[c]`, `CoordReset` |
| Eager/cooperative/lost revocation; `close` split discard/elect/seek with {fatal, wakeup}; `PauseResume` | S3 (F2) | `closeStep`, `stored`, `PollReturn` |
| Coordinator instances with epochs; zombies that keep consuming / committing | S4 | `coord[k]`; `Terminate` does not disable actions |
| Table-commit outcomes: ok, CommitFailed, unknown-ok, unknown-fail, ValidationException | Q4, S2, S4 | nondeterministic outcome in `CasCommit` |
| Partial path swallows all errors; full path fatal unless CommitFailed within budget | S4/S5, #13117, #16434 | `Timeout(k)`; `MaxConsecFailures ∈ {1,2}` |
| Task crash/restart (in-memory state lost, nothing sent afterwards) | Q1/Q2 | `Crash(t)`, `Restart(t)` |

### 3.2 Do Not Model

| What | Why |
|---|---|
| Data-file encoding, writers, schema evolution, SMT routing | out of scope per guidance; rows are abstract `<p, off>`; SMT offset identity fixed (3f18cd4f6) |
| Delete files / `RowDelta` / equality deletes | unreachable: writers append-only (`RecordUtils.java:163-179`); #17455 disputed |
| LSO blocking, txn timeouts, session/poll timers, record-size limits, thread leaks per se | liveness/size only — tests T-4..T-8; model only the consequence (zombie stays in `-coord`) |
| Known #15293 / #18006 / #18767 / #17637 as default behaviour | keep switches (`ControlTopicReset` OFF, `CoordReset`, `FixNullSeek`, stale views) so known traces do not mask new ones |
| CommitToTable/CommitComplete payloads, metrics, catalog-specific error mapping (#16659) | informational / abstracted into outcome nondeterminism |

## 4. Proposed Extensions

| Extension | Variables | Purpose | Scenario |
|---|---|---|---|
| TableReference variants | `ref=<table,variant>`; `RefVariants` | one table split across commit groups | S1 |
| Floor & ancestry | `snaps[T]`, `branchExists[T]` | read floor vs validator floor; expiry; other writers | S1, S2 |
| Checkpoint & reset | `cgCommitted[c]`, `cgOwner[c]`, `CoordReset` | replay start; never-seen partitions | S1, S2, S4 |
| Close decomposition | `closeStep[t]`, `stored[t]`, `pauseDuringClose` | discard-then-rewind non-atomicity | S3 |
| Coordinator instances | `coord[k]`, `coordEpoch` | zombies, fencing, lingering | S4 |
| Readiness accounting | `cCount`, `cTotal`, `cReady`, ghost coverage | partial/full, valid-through | S5 |
| Ghost history | `rows[T]` (bag), `dwIn[T]` | invariants | all |

## 5. Proposed Invariants

| Invariant | Type | Description | Targets |
|---|---|---|---|
| NoDuplicateRows | Safety | each source record at most once in each table it is routed to | Q1-Q4, S2, S4 |
| SourceCommittedCovered | Safety | every record below the Connect committed offset is in a visible DataWritten for each routed table, or in that table | Q1, S3 |
| FloorSound | Safety | every visible DataWritten for T at (c,e) with e < ReadFloor(T)[c] is in T | S1, Q3 |
| CheckpointSound | Safety | every DataWritten at (c,e) with e < cgCommitted[c] is in its table (intentional skips excepted) | S1, S4, Q5 |
| NoStrandedDataWritten | Safety | every visible DataWritten not in its table is ≥ its table's floor and (≥ cgCommitted[c] or buffered by a live unfenced coordinator) | S1, S2, Q5 |
| AtMostOneCommitPerDataWritten | Safety | no DataWritten contributes files to the same table twice | S2, S4 |
| FloorMonotone | Safety | ReadFloor(T)[c] never decreases except via env actions | S2 |
| ValidThroughSound | Safety (secondary) | a full commit's valid-through covers only partitions whose DataComplete for it was received | S5 |
| EventuallyCommitted | Liveness | under weak fairness and no further faults, every visible DataWritten reaches its table | S4, S2 |

## 6. Findings Pending Verification

### 6.1 Model-Checkable

| ID | Description | Expected invariant violation | Scenario |
|---|---|---|---|
| MC-1 | With `RefVariants={legacy,v}` (rolling upgrade across 1.11.0), can one group's floor claim another group's DataWritten (sequential/parallel group commits, full/partial cycles, replay after the task failure)? Then with one variant: does any other path leave a T-envelope below the committing coordinator's position outside the commit (retained buffer after CommitFailed, `-coord` partition bounce, late DataWritten)? | FloorSound, NoStrandedDataWritten (end-to-end SourceCommittedCovered ∧ EventuallyCommitted) | S1 |
| MC-2 | With `OtherWriterCommit`+`ExpireSnapshots`, which replay windows re-commit a DataWritten: crash between table commit and checkpoint; multi-table partial success with `MaxConsecFailures=1`; `-coord` offsets lost with `CoordReset="earliest"` (PR #18006)? | NoDuplicateRows, AtMostOneCommitPerDataWritten | S2 |
| MC-3 | With `FixNullSeek=TRUE`, eager/cooperative/lost revocations, exception injection after the discard and `PauseResume` during close: can a record below the Connect committed offset be absent from every DataWritten? Are there exception points other than `committed()` with the same effect? | SourceCommittedCovered | S3 |
| MC-4 | Zombie keeps consuming and committing tables after the successor's `initTransactions`; control partitions move between them (1 and 2 partitions); partial commits swallow fencing/validation errors; two tables with unknown outcomes; timeouts with late DataWritten. Does per-table exactly-once survive? (predicted yes — any trace is a new bug) | NoDuplicateRows, FloorSound, NoStrandedDataWritten | S4, S5 |
| MC-5 | Can a visible DataWritten stay uncommitted forever: fenced coordinator cleared while `startCoordinator()` is a no-op (F8), election missed on added partitions (F9), branch missing with the key on main (F4)? | EventuallyCommitted | S4, S2 |

### 6.2 Test-Verifiable

| ID | Description | Suggested test approach |
|---|---|---|
| T-1 | F1: DataWritten for one table with `uuid=null` and `uuid=X` lose one group | Coordinator unit test, `commitThreads` 1 and 2; assert both files committed |
| T-2 | F4: missing commit-branch + key on main ⇒ permanent ValidationException | Coordinator unit test; expect branch creation or a clear config error |
| T-3 | F2: `WakeupException` from `committed()` in `close()` loses buffered + polled records | WorkerSinkTask harness, MockConsumer throwing WakeupException in `onPartitionsRevoked` |
| T-4 | F5: coordinator thread hangs after `terminate()` with queued table tasks | `commitThreads=1`, 2 tables, block first commit, `terminate()`, assert thread exits |
| T-5 | F6: terminated coordinator spins in `consumeAvailable` under continuous traffic | MockConsumer returning ≥1 record per poll after `terminate()` |
| T-6 | F7: long commit ⇒ `-coord` checkpoint Kafka `CommitFailedException` ⇒ task FAILED | integration test, small `max.poll.interval.ms`, slow catalog stub |
| T-7 | F10: `Tasks.throwOne` surfaces ValidationException over ProducerFencedException | unit test, two tables, one fenced send |
| T-8 | F13: one DataWritten per table result never split ⇒ `RecordTooLarge` crash loop | many files/table/worker vs `max.request.size` |
| T-9 | F14: `CommitToTable.snapshotId` read after refresh may be another writer's snapshot | unit test with interleaved append (`Coordinator.java:343`) |

### 6.3 Code-Review-Only

| ID | Description | Suggested action |
|---|---|---|
| C-1 | NoSuchTable/UUID-mismatch skip drops acknowledged data, then checkpoints (`Coordinator.java:251-263,216`; #18012) | decide hold/fail vs drop; at least count and alert |
| C-2 | Partial-commit catch-all without budget (`:166-174`): poison DataWritten retried forever; fenced zombie lingers | bound retries; treat fencing as terminal |
| C-3 | Kafka vs Iceberg `CommitFailedException` name clash (`:57,176`); fencing detected by exact class (`CoordinatorThread.java:78-81`) | classify by cause chain |
| C-4 | `uuid==null` events bypass the replaced-table check (`Coordinator.java:256`) | upgrade + table replace ⇒ schema/location mismatch risk |
| C-5 | `completeWrite()` clears buffers before `send()` (`Worker.java:74`) — safe only because every exception kills the task | document; guard any future retry path |
| C-6 | Files referenced only by uncommitted DataWritten look orphaned to `remove_orphan_files` during long outages | document orphan-age > control retention / outage |
| C-7 | Floor key has no control-topic identity (`Coordinator.java:108-110`; #15293) | store topic id / detect offset regression |
| C-8 | Worker txn id regenerated per `taskConfigs()`; coordinator txn id changed by #18039 (mixed-version coordinators unfenced) | release/migration note |
| C-9 | `-coord` checkpoint only for control partitions this coordinator read (`Channel.java:174-184`) — widens #18006 | seek to stored floors on start |

## 7. Reference Pointers

- **Analysis report**: `analysis-report.md` — §1 recon/atomicity map; §2 archaeology (178 commits / 42 core / 22 bug
  fixes, 30 issues, 11 open + 14 closed PRs); §3.2 findings F1–F15 with verification; §3.4 refuted hypotheses R1–R18.
- **Source** (`kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/channel/`): `Coordinator.java`
  126-231 cycle, 242-359 per-table commit, 361-415 floor/validator, 434-447 terminate; `CommitState.java` 51-165;
  `Channel.java` 83-118 txn send, 122-152 consume/HWM, 171-213 checkpoint; `CommitterImpl.java` 78-245; `Worker.java`
  68-114; `KafkaUtils.java` 60-85. Core: `SnapshotProducer.java` 297-389/486-539, `SnapshotUtil.java` 243-254/478-489, `Tasks.java`.
- **Environment (Kafka 3.9.2)**: `ConsumerCoordinator.java` 202/351/978/1443; `ConsumerNetworkClient.java` 252-295/527-532;
  `WorkerSinkTask.java` 204-207/248-262/427-448/497-505/712-794; `GroupCoordinator.scala` transactional offset validation.
- **Issues/PRs**: S1 #15293 #12357 #18012 #14979 #15011; S2 #17925 #18006 #14510; S3 #18767 #12372 #16843; S4 #17637
  #18687 #16016 #17349 #17551 #18039; S5 #18322 #17077 #17811 #18030 #13117.
- **Reference**: Iceberg sink commit coordination (control topic + coordinator, KIP-447); no prior TLA+ spec exists.
