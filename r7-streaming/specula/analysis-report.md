# Analysis Report — Apache Iceberg Kafka Connect sink (exactly-once commit protocol)

Audit trail for `modeling-brief.md`. Target: `apache/iceberg` @ `9fffeb63b1605a7c3acf218e58b7eeb43bdb1b01`
(2026-11-02), module `kafka-connect/`. Method: Specula `code-analysis` skill (Phase 0 classification,
Phase 1 reconnaissance, Phase 2 bug archaeology, Phase 3 deep analysis, Phase 4 brief).

All paths below are relative to the source checkout unless prefixed:
`CI=kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/channel`,
`KC=kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect`,
`EV=kafka-connect/kafka-connect-events/src/main/java/org/apache/iceberg/connect/events`.

Evidence sources:
- Source checkout (read-only; shallow, 1 commit).
- Full-history blobless clone of apache/iceberg (scratch, `/tmp/specula-iceberg-hist/iceberg.git`) for git
  mining; analysis restricted to commits reachable from `9fffeb6`.
- GitHub issues/PRs via `gh` (full comment threads).
- Environment semantics: Kafka **3.9.2** sources (the version pinned in `gradle/libs.versions.toml:75`), fetched
  read-only: `ConsumerCoordinator.java`, `AbstractCoordinator.java`, `GroupCoordinator.scala`, `ClassicGroup.java`,
  `ConsumerGroup.java`, `OffsetMetadataManager.java`, `TransactionManager.java`, Connect `WorkerSinkTask.java`,
  `WorkerSinkTaskContext.java`; Iceberg core `SnapshotProducer.java`, `util/Tasks.java`, `util/SnapshotUtil.java`.

---

## 0. System category

**Category A — Distributed / message-passing.** Independent processes (Connect sink tasks on several workers,
one elected coordinator thread) communicate only through Kafka (control topic, two consumer groups,
transactions) and an external catalog (compare-and-swap table commit). The correctness question is a
crash/rebalance/failover protocol question (durable state split across Kafka consumer-group offsets, control-topic
events and Iceberg snapshot summaries), not a lock-free memory question. Intra-process threading (task thread,
coordinator thread, commit executor) matters only at a few lifecycle points (terminate/stop) and is modeled as
action interleaving, not memory ordering. Not BFT (crash-fault, plus zombies bounded by Kafka fencing).

---

## 1. Reconnaissance

### 1.1 Core files and scale (in-scope logic ≈ 1,550 LOC)

| File | LOC | Role |
|---|---:|---|
| `KC/IcebergSinkTask.java` | 110 | Connect `SinkTask`; `put`→`save`, `flush`→`save(null)`, `preCommit` returns **empty map** (`:100-104`), `open/close` delegate |
| `CI/CommitterImpl.java` | 246 | Lifecycle: lazy Worker, coordinator election (`hasLeaderPartition` via `describeConsumerGroups`), `close` = stopWorker + maybe stopCoordinator + `seekToLastCommittedOffsets` |
| `CI/Worker.java` | 125 | Answers `StartCommit`: `completeWrite()` then one transaction {DataWritten*, DataComplete, source offsets} |
| `CI/Channel.java` | 241 | Shared producer/consumer/admin; transactional `send`; `consumeAvailable` with high-water-mark skip; `commitConsumerOffsets` (monotone, transactional) |
| `CI/Coordinator.java` | 448 | Commit cycle, partial (timeout) commit, per-table commit with stored control offsets + `SnapshotAncestryValidator` |
| `CI/CommitState.java` | 166 | commitBuffer (DataWritten), readyBuffer (DataComplete), partition counting, timeout, valid-through-ts |
| `CI/CoordinatorThread.java` | 87 | Coordinator loop thread; fenced vs. error termination |
| `CI/KafkaUtils.java` | 100 | `consumerGroupDescription`, `consumerGroupMetadata(context)`, `seekToLastCommittedOffsets` (reflection on `WorkerSinkTaskContext.consumer`) |
| `CI/KafkaClientFactory.java` | 68 | Producer `initTransactions()`; consumer `read_committed`, `auto.offset.reset` default **latest** (`putIfAbsent`, `:55`), no auto-commit |
| `KC/data/SinkWriter.java` | 145 | Per-table writers; source offsets keyed by original topic/partition, `offset+1` (`:76-95`); `completeWrite` clears (`:59-70`) |
| `EV/*` | ~900 | Avro control events: StartCommit, DataWritten, DataComplete, CommitToTable, CommitComplete |
| `KC/IcebergSinkConfig.java`, `KC/IcebergSinkConnector.java` | 535+71 | ids/defaults: commit interval 300 s, timeout 30 s, max-consecutive-failures **1**; worker txn suffix `-txn-<uuid>-<i>` regenerated per `taskConfigs()` (`IcebergSinkConnector.java:52-64`); coordinator txn id stable `prefix + connect-<name> + "-coordinator"` (`IcebergSinkConfig.java:463-465`) |

Writers are append-only (`KC/data/RecordUtils.java:163-179`: `UnpartitionedWriter` / `PartitionedAppendWriter`),
so `deleteFiles` are empty in practice and the `RowDelta` branch (`CI/Coordinator.java:326-341`) is effectively unused
in this snapshot; duplicates therefore surface as duplicate rows (no PK dedup).

### 1.2 Thread / process domains

| Domain | Runs | Notes |
|---|---|---|
| Connect task thread (one per task) | `put/flush/open/close/preCommit/stop`, all Worker logic (`Worker.process()` at end of every `save`, `CommitterImpl.java:190-212`) | Rebalance callbacks (`open/close`) run on this thread inside `consumer.poll()` |
| Coordinator thread `iceberg-coord` (elected task only) | `Coordinator.process()` loop (`CoordinatorThread.java:40-64`) with its own producer/consumer/admin | Not joined by `terminate()` (`CoordinatorThread.java:83-86`, `Coordinator.java:434-447`) |
| Commit executor `iceberg-committer-%d` | per-table `commitToTable` in parallel (`Coordinator.java:207-213`) | `shutdownNow()` on terminate interrupts in-flight catalog commits |

### 1.3 Durable state and its writers

| State | Location | Written by | When |
|---|---|---|---|
| Source offsets | Connect consumer group `connect-<name>` | **only** Worker transactions (`Channel.java:104-107`); the runtime never commits (`preCommit` empty) | atomically with DataWritten/DataComplete |
| Control events | control topic (default 1 partition, auto-created, delete retention) | Worker txns; coordinator single-event txns | read_committed consumers |
| Coordinator progress | group `connect-<name>-coord` offsets | `Channel.commitConsumerOffsets` txn (`Channel.java:171-213`) | after **all** tables of a cycle committed |
| Dedup watermark | snapshot summary `kafka.connect.offsets.<controlTopic>.<connectGroupId>` = JSON {ctrlPartition → next offset} (`Coordinator.java:108-110,318,332`) | coordinator table commit | per table, = max(stored, coordinator position) (`:270-275`) |
| Snapshot metadata | `kafka.connect.commit-id`, `task-id`, `valid-through-ts` | coordinator | per table commit |

In-memory only: SinkWriter files + sourceOffsets; `CommitState` buffers; `Channel.controlTopicOffsets` (high-water
mark, never reset, `Channel.java:126-139`); `Channel.committedOffsets` (last committed -coord offsets, `:177-211`);
`membersWhenWorkerIsCoordinator` → `totalPartitionCount` (snapshot at election, `Coordinator.java:106-107`).

### 1.4 Atomicity boundaries (what one TLA+ action may cover)

1. **Worker answer** (`Worker.receive` → `Channel.send`, `Worker.java:74-111`, `Channel.java:83-118`): one Kafka
   transaction {DataWritten per (table, partition-spec), DataComplete(commitId, assignment), TxnOffsetCommit(source
   offsets, Connect consumer groupMetadata)} — all visible or none. `completeWrite()` clears the in-memory buffer
   *before* the transaction; failure ⇒ exception out of `put()` ⇒ task killed (no Connect `RetriableException` is
   thrown anywhere in the sink; verified by grep) ⇒ re-delivery from committed offsets.
2. **Coordinator per-table commit** (`Coordinator.java:242-359`): (a) `loadTable` + read stored offsets + filter
   envelopes (`:250-301`) — a *read* step; (b) `terminated` check (`:303-306`); (c) `AppendFiles.commit()` = Iceberg
   CAS with internal retries only on `CommitFailedException`, validator re-run on every attempt after refresh
   (`core/.../SnapshotProducer.java:297-304,377-389,486-536`); (d) `send(CommitToTable)`. (a) and (c) must be
   separate actions (TOCTOU closed only by the validator).
3. **Coordinator offsets** (`Coordinator.java:216` → `Channel.java:171-213`): separate txn, only if all tables in
   the cycle succeeded (`Tasks` default `throwFailureWhenFinished=true`, `core/.../util/Tasks.java:77,396-400`).
4. **Buffer clear + CommitComplete** (`Coordinator.java:217-223`), `endCurrentCommit` in `finally` (`:198-200`).
5. **close(partitions)** (`CommitterImpl.java:161-187`): stopWorker → (describeConsumerGroups) maybe stopCoordinator
   → `seekToLastCommittedOffsets`; each call can throw, leaving later steps undone (Connect then fails the task).

### 1.5 One commit cycle

`Coordinator.process` (`:126-141`): interval elapsed & no commit in progress → new commitId, `StartCommit` txn →
workers (each at the end of its next `put/flush`) answer once per StartCommit seen → coordinator buffers every
DataWritten regardless of commitId (`CommitState.java:51-59`), counts DataComplete assignments only for the current
commitId (`:61-71`) → when count ≥ `totalPartitionCount` → full commit; else after 30 s → partial commit
(`validThroughTs = null`, all exceptions swallowed, `Coordinator.java:166-174`). `commitBuffer` is cleared only after a
fully successful cycle (`:217`); on failure it is retained and re-committed in a later cycle, relying on the stored-offset
filter (`:277-285`) for idempotence.

### 1.6 Fencing / zombie defenses (as built)

| Defense | Protects | Gap (by construction) |
|---|---|---|
| KIP-447 groupMetadata in Worker `TxnOffsetCommit` (`Channel.java:105-106`) | stale-generation worker cannot commit DataWritten+offsets | none found (see §3.1) |
| Worker `transactional.id` = prefix+`worker`+`-txn-<uuid>-<i>` | same task index, same `taskConfigs()` generation | new uuid on every `taskConfigs()` ⇒ no producer fencing across reconfiguration (KIP-447 still applies) |
| Stable coordinator `transactional.id` (#18039) | zombie coordinator's sends / offset commits after the new coordinator's `initTransactions()` | Iceberg table commits are outside Kafka transactions (documented `Channel.java:165-169`) |
| `SnapshotAncestryValidator` (`Coordinator.java:361-381`) | commit whose expected stored offsets changed since read | relies on stored offsets being *visible* in the branch ancestry and on the reader/validator agreeing on the branch head (§3.1 B1, B2) |
| Stored-offset filter (`Coordinator.java:277-285`) | replayed DataWritten already in the table | same reliance |
| High-water-mark skip (`Channel.java:126-139`, #17713/#17933) | coordinator re-processing after -coord rebalance rewind | in-memory only |

---

## 2. Bug archaeology

Issue/PR verification ran as parallel passes (≈10 items each); every item below was read with its full comment
thread (`gh issue view --comments`, `gh pr view --comments`, inline review comments via the API), fix commits were
located in the history clone and checked with `merge-base --is-ancestor <sha> 9fffeb6`.

### 2.1 Git history

The provided checkout is shallow (one commit); history was mined from a full blobless clone, restricted to commits
reachable from `9fffeb6`. Module history starts at `d1a3c1045` (2024-01-10, #8701); pre-donation history
(tabular-io/iceberg-kafka-connect) is not in this repo.

- Commits touching `kafka-connect/`: **178** (75 are dependency bumps; 58 touch main sources).
- Commits touching the core protocol files: **42** (`channel/` alone: 25).
- Keyword mining (fix, bug, race, deadlock, loss, lost, duplicate, dup, zombie, fence, rebalance, offset,
  coordinator, commit, crash, corrupt, leak, inconsistent, wrong, stale, timeout, retry, recover): union 93 commits
  (≈49 after removing bumps), 24 touching core files; 6 of the 22 core bug fixes are missed by keywords and were found
  by diff review.
- **Bug-fix commits on core files: 22, all analyzed (main-source diff + PR description).**

| Commit | PR | Date | Fix | Root cause | Component | Sev |
|---|---|---|---|---|---|---|
| 2a68edc04 | #10347 | 2024-05 | DataWritten delete-files element reused field id | logic | event serde | Low |
| 30e761ebe | #10739 | 2024-07 | put/flush silently skip when committer null (bundled) | error handling | lifecycle | Low (enabled #12372 loss) |
| 0747b6044 | #10814 | 2024-09 | `terminated` guard + executor shutdown so a stopped coordinator cannot commit | race | fencing | Critical |
| 3c6c62654 | #11303 | 2024-10 | worker-props regex | config | config | Low |
| 51abab10e | #12372 | 2025-03 | cooperative rebalance: per-partition open/close, election on added partitions, seek on close | logic | lifecycle+election | Critical (loss) |
| 357020aa6 | #13670 | 2025-08 | NPE in close before init — **and silently removed the `terminated` guard (1.10.0–1.10.2 shipped without it)** | missing check / regression | lifecycle | Medium + regression |
| cced19af0 | #13756 | 2025-09 | task stop always stops coordinator; guard restored | logic | lifecycle | High |
| c235305c9 | #14395 | 2025-11 | drop STABLE-group election check (left two coordinators) | race | election | Critical |
| 8da07dcae | #14525 | 2025-11 | merge stored offsets with prior (max) instead of overwrite | logic | dedup floor | Critical |
| 6b28b0edc | #14510 | 2025-11 | offset `SnapshotAncestryValidator` re-checked on refresh/retry | TOCTOU | table-commit validation | Critical |
| d85f8a87a | #14979 | 2026-01 | table UUID in `TableReference` (**also added `uuid` to `equals/hashCode`**, see F1) | missing check | table identity | High |
| b2696b959 | #15011 | 2026-01 | UUID mismatch skips; null UUID tolerated "for the upgrade case" | logic | table identity | Medium |
| 3f18cd4f6 | #15880 | 2026-04 | source offsets keyed by original topic/partition/offset (SMTs) | logic | source-offset commit | Critical |
| a675f88b0 | #15955 | 2026-04 | "no partitions in group" returns false instead of throwing | error handling | election | High |
| 2fe32aa8f | #16237 | 2026-05 | full-commit failures rethrown instead of swallowed | error handling | commit | High |
| 26169f736 | #16438 | 2026-05 | CME in `tableConfig` killed coordinator between table commits | race | multi-table commit | High |
| a165b1c15 | #16453 | 2026-06 | `isCommitReady` O(N²) → O(1) counter | perf/logic | readiness | Medium |
| be43e3174 | #16434 | 2026-07 | bounded retry on Iceberg `CommitFailedException` (default 1) | error handling | commit | Medium |
| 4d725363c | #17933 | 2026-09 | control-topic offsets as high-water mark | logic | control consumption | Critical (prod dups #17340) |
| 1019497e3 | #17552 | 2026-09 | `-coord` offset committed only if above this instance's last commit | race (zombie) | control consumption | Critical (loss #17551) |
| ef99ad3f8 | #17713 | 2026-09 | skip replayed control records below in-memory next offset | logic | control consumption | Critical |
| dad714b8f | #18039 | 2026-10 | stable coordinator `transactional.id`; fenced coordinator cleared | missing fencing | fencing | Critical (unreleased at T) |

Hotspots (bug fixes per file): `Coordinator` 11, `CommitterImpl` 6, `IcebergSinkConfig` 6, `CoordinatorThread` 4,
`Channel` 4 (all Sep–Oct 2026), `IcebergSinkTask` 3, `Worker` 2, others ≤1. 9 of the 22 fixes are Critical
(exactly-once violations) and 6 of those landed in the last 3 months before the snapshot ⇒ the coordinator replay /
dedup machinery is young and still moving.

Mechanism groups (from the diffs):
- **G1 coordinator replay after rebalance/restart, protected only by control-offset arithmetic** — 8da07dcae,
  6b28b0edc, 4d725363c, ef99ad3f8, 1019497e3. Residual: floor reachability (B2/F3), floor-vs-group split (F1),
  per-instance monotonic cache (`Channel.java:177-178`) instead of broker value.
- **G2 zombie/duplicate coordinators (election + fencing)** — 0747b6044, 357020aa6 (regression), cced19af0,
  c235305c9, a675f88b0, 51abab10e, dad714b8f. Residual: describe-based (de-)election (#17637), no thread join,
  table commits unfenced (validator only), partial path swallows fencing.
- **G3 worker lifecycle under cooperative rebalance (discard + rewind)** — 51abab10e, 30e761ebe, 357020aa6,
  cced19af0. Residual: null-committed-offset partitions not rewound (KNOWN, #18767).
- **G4 failure classification / timeouts / partial commits** — 2fe32aa8f, be43e3174, a165b1c15, 26169f736.
  Residual: partial commits swallow every exception without a budget; skipped tables advance offsets.
- **G5 source-offset identity** — 3f18cd4f6 (low residual). **G6 table identity** — d85f8a87a, b2696b959 (F1).

### 2.2 GitHub issues deeply read (30)

Consequence legend: **LOSS** = source offsets committed but rows never reach the table; **DUP** = duplicate rows;
STALL = commits stop (liveness); META = wrong snapshot metadata (valid-through-ts).

| # | State | Classification | Mechanism (code path) | Consequence | Fix / in snapshot? | Q |
|---|---|---|---|---|---|---|
| 16282 | open | Confirmed (2 prod incidents) | Duplicate coordinator churns `-coord` group → eager reassignment rewinds live coordinator inside the same `Channel` → regressed `controlTopicOffsets` + double-counted DataComplete → floor stamped below own contents. Original "new coordinator replays stale DataWritten" claim refuted in-thread (floor filter drops it) | DUP | #17933, #17713, #18039 in snapshot; root cause #17637 unfixed | Q2,Q1 |
| 17340 | closed | Confirmed (149 files/111,927 rows prod) | `consumeAvailable` last-write-wins `put` regressed position; lower value went into floor and `-coord` offsets; replay tail passed filter; `AppendFiles` has no path-level dedup | DUP | #17933 `4d725363c` (+#17713) in snapshot | Q1,Q2,Q5 |
| 17551 | closed | Confirmed (bug label) | Zombie coordinator kept committing frozen `-coord` offsets (last writer wins); after retention the next coordinator falls back to `auto.offset.reset=latest` and skips answered DataWritten | LOSS | #17552 `1019497e3` + #18039 `dad714b8f` in snapshot (#18039 not in 1.12.0) | Q2,Q5 |
| 13995 | closed (stale) | Confirmed post-hoc | Sawtooth `-coord` offsets from zombie re-commits | rework; precursor of 17551 | root cause fixed by #17552/#18039/#17933 | Q2,Q5 |
| 16016 | open | Confirmed (docker repro, 4–5 coordinator threads) | `terminate()` never joins the thread (runs on closed catalog); `close()` gate `hasLeaderPartition()` false mid-rebalance | STALL, two coordinators | unfixed (#16020, #16843 closed stale); mitigated by #13756, #18039 | Q2,Q1 |
| 17349 | open | Confirmed | No `join` in `stopCoordinator()`; phantom member stays in `-coord` group; blocks operator offset reset | operational | #17350 open | Q2,Q5 |
| 17455 | open | **Disputed/False** (verified) | Claims equality-delete dup after crash; upstream writers are append-only (`RecordUtils.java:163-179`), RowDelta unreachable | none | n/a | — |
| 16659 | open | **Disputed/False** (vendor 400 on conflict) | `BadRequestException` not retried by `SnapshotProducer` | STALL (task fails) | won't fix | Q4 |
| 17077 | open | Confirmed | `addReady` buffers every DataComplete; `validThroughTs` reads all ⇒ earlier/null (never later) | META | #17080 approved, not merged | Q3,Q2 |
| 17637 | open | Confirmed (unit repro; prod reports) | `close()` re-derives leadership from a live describe mid-rebalance; empty/partial assignments ⇒ old coordinator not stopped; another task's `open()` starts a second | two coordinators → validator conflicts → FAILED task; feeds 16282/17551 | #18687 open | Q2,Q1 |
| 15293 | open, 266 d | Confirmed (5 independent reports, JDBC/Glue/BigQuery) | Recreated control topic restarts offsets near 0; floor filter (`Coordinator.java:277-285`) drops every new envelope; cycle still checkpoints and clears (`:216-217`) | **LOSS** until new offsets pass old floor | none (#16360 closed stale) | **Q5** |
| 12549 | closed | User error | Reporter's own compaction job | none | n/a | — |
| 13752 | open | Confirmed; dup of 13593 | Zombie coordinator; dumps show 2 consumers in `-coord` group splitting control partitions | STALL (loss claim partly retracted) | #13756 in snapshot | Q1,Q2 |
| 13593 | open | Confirmed (7+ reporters) | `stop()` closed coordinator only if `hasLeaderPartition()`; coordinator outlived catalog; 1.9.x swallowed errors | STALL | #13670 + #13756 in snapshot | Q1,Q2 |
| 15878 | closed | Confirmed | 1.10 `commit()` swallowed every exception ⇒ task RUNNING while workers kept committing source offsets | silent STALL | #16237 + #16434 in snapshot | Q3,Q4 |
| 13536 | closed (stale) | Uncertain | First StartCommit only after a full interval; STABLE-group election gate (pre-#14395) | delay | #14395 in snapshot | Q5 |
| 11805 | closed (stale) | Uncertain (harmless per code) | Concerns the Worker's transient `cg-control-<uuid>` group (never commits) | none | n/a | Q5 |
| 18067 | open | Uncertain | Worker control consumer fenced in single-member `subscribe()` group (= #18030 mechanism); operator workaround deletes internal topics ⇒ sets up #15293 | STALL | #18030 open | Q1,Q3,Q5 |
| 12357 | closed (stale) | **Design defect** | Same floor filter as 15293 after Kafka state loss; other writers' snapshots lack the key | LOSS + orphan files | none | Q5 |
| 13117 | open, 532 d | Design defect | Partial-commit failures swallowed without limit (`Coordinator.java:166-174`); no cap on consecutive timeouts | silent STALL | partly mitigated (#16237/#16434/#16433) | Q3 |
| 17193 | open | Confirmed (repro) | Worker created on first non-empty put; join reply handled after session timeout ⇒ UNKNOWN_MEMBER_ID loop; Worker never sees StartCommit | STALL | #18290, #18030 open | Q1,Q3 |
| 17811 | open | Confirmed | Worker started only on non-empty `save`; revoke-only cooperative rebalance leaves no Worker ⇒ every round times out ⇒ `validThroughTs=null` | STALL/META | #17593 open | Q1,Q3 |
| 14818 | closed (stale) | Confirmed | Same as 17193 (zero-duration polls) | STALL | #14816/#16084 closed | Q1,Q3 |
| 11818 | closed (stale) | Confirmed symptom | Join failure / lazy worker on low-volume topics | STALL | none | Q1,Q3 |
| 11796 | closed (stale) | Confirmed | Control consumer polled only from put/flush; StartCommits answered in late bursts; stale DataWritten carried forward | delay, partial commits | none | Q3 |
| 11703 | closed | User error | Wrong `iceberg.connect.group-id` ⇒ election describes a dead group ⇒ no coordinator | STALL | n/a | Q2 |
| 13067 | closed (stale) | Uncertain (2 reporters) | One of 3 control partitions lags, ~1/3 data missing: stuck read_committed LSO behind a hanging txn, or a second `-coord` member owning the partition (pre-#18039) | reported LOSS (unconfirmed) | none | Q2,Q5 |
| 13787 | closed (stale) | Uncertain (doc gap) | Control-topic sharing via `event.groupId()` filter; floor keyed by topic+group | none | none | Q5 |
| 15844 | open | User error (retention) | Control topic retention set by broker; if coordinator is down longer than retention, answered DataWritten are deleted and `-coord` offset falls out of range ⇒ `latest` | LOSS under short retention (env) | docs PR closed | Q5 |
| 16389 | open | Design defect | Any table failure skips checkpoint/clear/CommitComplete ⇒ buffer grows each cycle; keeping it is the safety mechanism | liveness spiral | #18007 open | Q4,Q3 |

Totals (issues): **30 deeply read** — 17 confirmed, 3 design defects, 2 disputed/false (17455, 16659), 3 user
error (12549, 11703, 15844), 5 uncertain (13536, 11805, 18067, 13067, 13787). Collected: 276 issue hits across 14
keyword searches (≈150 kafka-connect-related after filtering); the 30 above are every issue touching the commit
protocol, offsets, coordinator, rebalance or data loss/dup.

### 2.3 Open bug-fix PRs reviewed (11)

| PR | Claimed trigger | Consequence | Real in snapshot? | Review | Q |
|---|---|---|---|---|---|
| 18006 | `-coord` partition without committed offset ⇒ successor resets to `latest`, skips answered DataWritten | **LOSS** | yes `KafkaClientFactory.java:55`; also every control partition never seen by the coordinator is never checkpointed (`Channel.java:174-184`) | unreviewed, stale (KNOWN per guidance) | Q2,Q5 |
| 18012 | missing/replaced table ⇒ `commitToTable` returns normally ⇒ cycle checkpoints and clears | **LOSS** for that table | yes `Coordinator.java:251-263,216-223` (current policy) | none | Q4 |
| 18030 | Worker control consumer `subscribe()` in a single-member random group never completes join on idle tasks | STALL | yes `Channel.java:228-233`, `Worker.java:52` | none | Q3,Q1 |
| 18290 | same root as #17193 (1 s initial poll < 3 s initial rebalance delay; later polls > session timeout) | STALL | yes | author defers to #18030 | Q3 |
| 18322 | expected partition count frozen at coordinator construction; assignment grows ⇒ early "full" commit | META (valid-through overstated); shrink ⇒ every round times out | yes `Coordinator.java:106-107`, `CommitState.java:117-128` | minimal; coverage fix #17925 closed | Q3,Q1,Q2 |
| 18687 | `close()` describes group mid-rebalance, decides "not leader", keeps coordinator | two coordinators; validator conflict fails the other coordinator's full commit ⇒ task FAILED | yes `CommitterImpl.java:78-106,179` | none | Q2 |
| 18767 | cooperative revoke discards buffers of kept partitions; seek rewinds only partitions with a committed offset | **LOSS** | yes `CommitterImpl.java:163,186`, `KafkaUtils.java:68,74` (KNOWN in-house finding) | none | Q1 |
| 17593 | idle tasks never start a Worker ⇒ never answer StartCommit | STALL/META | yes | stale | Q3,Q1 |
| 17910 | `Channel.stop()` closes producer first, no timeout ⇒ hung close keeps `-coord` member | hung coordinator | ordering present, hang unproven | reviewer pushback | Q2 |
| 17350 | `stopCoordinator()` does not join; clients closed later on the coordinator thread after catalog closed | phantom member/zombie | yes `CoordinatorThread.java:59-63,83-86` | competing approaches | Q2 |
| 17080 | DataComplete of other commitIds included in `validThroughTs` min | META (early/null, never late) | yes `CommitState.java:63,146-165` | approved | Q2,Q3 |

Notable reviewer-raised, still-unfixed concerns (cross-checked in code):
- wombatu-kun on #17925: the only dedup floor that survives a restart is the `kafka.connect.offsets` summary found by
  walking ancestry (`Coordinator.java:395-415`); **if that snapshot expires, replay registers the same files again**;
  `distinctByKey` (`:292`) only dedups within one commit. (Independently derived as B2 in §3.1.)
- dilverse on #18290: even after the join fix, idle tasks poll the control topic only from put/flush (~60 s) — longer
  than the 30 s commit timeout ⇒ every round partial, no watermark.
- #16843 (closed): if the group describe in `close()` throws (`KafkaUtils.java:50-53`), `close()` stops after
  discarding the Worker and before the seek ⇒ Connect fails the task (stall, not silent loss).

### 2.4 Closed / merged PR discussions (12) and design discussion on the UUID PRs (2)

| PR | State | Scenario addressed | Outcome | Concern still unaddressed in snapshot |
|---|---|---|---|---|
| 17925 | closed (stale) | readiness must cover every expected partition (replayed/overlapping/foreign DataComplete inflate the count) | approved by peer, bot-closed | count-based readiness (`CommitState.java:69,122`), frozen total (`Coordinator.java:106-107`); stale DataComplete in `validThroughTs` |
| 17376 | closed | harden coordinator (fixed txn id, `earliest`, skip, retry) | parts landed via #17713, #18039 | `earliest` never landed (`KafkaClientFactory.java:55`) |
| 17450 | closed | election on task thread + hardening | approved, stale | describe-based election still used (`CommitterImpl.java:78-128,179`) |
| 16360 | closed | recover after control-topic recreation | stale; #15293 open | floor above new offsets drops everything (`Coordinator.java:281-282`) — Q5 LOSS |
| 16366 | closed | tolerate Kafka `CommitFailedException`/`InvalidProducerEpochException` in worker txn on rebalance | stale | worker aborts + rethrows ⇒ task FAILED (`Channel.java:109-116`) — availability |
| 16156 | closed | join coordinator thread on stop | stale | no join (`CommitterImpl.java:240-245`) |
| 16020 | closed | zombie coordinator when S3 writes fail | stale | same; no try/finally in `IcebergSinkTask.java:67-83` |
| 11298 | closed | no coordinator + data loss under cooperative rebalance | superseded by #12372 | de-election still fails open |
| 14506 / 14515 | closed | commit validation against refreshed state | superseded by #14510 (+#14509 API) | none (empty-vs-non-empty now fails via `equals`, `Coordinator.java:371`) |
| 17713 | merged `ef99ad3f8` | skip already-consumed control records | in snapshot | handles only backward rewinds within one coordinator instance |
| 18039 | merged `dad714b8f` | stable coordinator txn id fencing | in snapshot | rolling upgrade from pre-#18039 versions unfenced (different txn id); table commits unfenced (`Channel.java:165-169`) |
| 14979 | merged `d85f8a87a` | record table UUID in DataWritten | in snapshot (1.11.0) | reviewer singhpk234: "what if someone does a library upgrade and it has events without the uuid?" — bryanck: they would be skipped; fixed in #15011 by tolerating null **in the check only**; `uuid` stays in `TableReference.equals/hashCode` ⇒ F1 |
| 15011 | merged `b2696b959` | skip on UUID mismatch; null tolerated "to handle the upgrade case, where existing messages in the control topic do not have the UUID set" | in snapshot (1.11.0) | the grouping split (F1) was not considered |

Distinct unaddressed concerns from these discussions: readiness/watermark (N1), coordinator `latest` reset (N2 =
#18006), floor expiry (N3 = B2/F3), control-topic reset (N4 = #15293), zero-coordinator state after a fenced
coordinator is cleared while `open()` is a no-op (N5, liveness), fail-open de-election (N6 = #17637), worker txn
failure on rebalance kills the task (N7, availability).

### 2.5 Coverage statistics

| Item | Count |
|---|---|
| Commits in module / core files / core bug fixes analyzed | 178 / 42 / 22 (all) |
| Issues collected (keyword searches) / kafka-connect related / deeply read (full threads) | 276 / ≈150 / **30** |
| Issues confirmed / design defect / disputed-false / user error / uncertain | 17 / 3 / 2 / 3 / 5 |
| Open bug-fix PRs reviewed (all with bug-fix intent touching the protocol) | 11 (10 real in snapshot, 9 root causes) |
| Closed/merged PR discussions read | 14 (12 + #14979, #15011) |
| False positives excluded (issues + hypotheses) | 2 issues (17455, 16659) + 18 hypotheses (§3.4) |

---

## 3. Deep analysis

### 3.1 Items verified directly in the main context

**Environment semantics checked (all against Kafka 3.9.2 sources):**

- E1. `ConsumerCoordinator.groupMetadata` is assigned only at construction (`ConsumerCoordinator.java:202`) and in
  `onJoinComplete` (`:351`); it is **not** reset when the member leaves/is reset. A worker whose consumer left the group
  (poll timeout) therefore still sends its stale generation/member id; the broker rejects it (`GroupCoordinator.scala`
  `validateOffsetCommit`, transactional path `:1007`). ⇒ Hypothesis "zombie worker commits with NO_GENERATION and
  bypasses KIP-447" **refuted**.
- E2. Classic consumer always fetches committed offsets with `requireStable=true` (`ConsumerCoordinator.java:1443-1444`,
  retries on `UNSTABLE_OFFSET_COMMIT` `:1498`). ⇒ `seekToLastCommittedOffsets` / position init cannot read a
  pre-transaction committed offset while a Worker transaction's offsets are pending. Hypothesis **refuted**.
- E3. Connect `WorkerSinkTask`: `closePartitions(…, lost=false)` → `commitOffsets(closing=true)` → `task.preCommit`
  (empty) → `finally task.close(partitions)` (`WorkerSinkTask.java:427-448,668-683`); `onPartitionsAssigned` sets
  tracked offsets from `consumer.position` and calls `task.open` only for non-empty sets (`:712-756`);
  `onPartitionsRemoved` skips empty sets and filters `messageBatch` (`:774-794`); exceptions in callbacks are stored and
  rethrown after `poll()` (task fails). `rewind()` applies only `context.offsets()` (unused by the sink, `:640-658`).
- E4. Iceberg `Tasks.runParallel` defaults to `throwFailureWhenFinished=true` and throws one of the collected
  exceptions (`Tasks.java:77,396-400`) ⇒ a failed table commit blocks `commitConsumerOffsets`. Which exception class
  is thrown when several tables fail decides retry-vs-terminate in `Coordinator.commit` (`:176`) — safety-neutral
  because the retry path re-reads stored offsets.
- E5. `SnapshotProducer.apply()` refreshes and re-runs the ancestry validator on **every** retry attempt
  (`SnapshotProducer.java:297-304,377-389`); `CommitStateUnknownException` is rethrown without retry (`:538-539`).
  The validator thus also prevents self-duplication when a first attempt actually succeeded but was reported as
  `CommitFailedException`.
- E6. `SnapshotUtil.ancestorsOf` stops at the first parent that cannot be looked up (`SnapshotUtil.java:243-254`):
  expired snapshots end the walk.
- E7. Catalog is built by `CatalogUtil.buildIcebergCatalog` without `CachingCatalog` (`KC/CatalogUtils.java`); stale
  cached tables are in any case caught by the validator.

**Code-path inconsistencies found directly (to be cross-checked against the deep-analysis passes):**

- B1. *Branch-head divergence.* The coordinator reads stored offsets from `table.snapshot(branch)`, which returns
  `null` for a branch that does not exist (`Coordinator.java:388-393`; `api/.../Table.java:380-387`) ⇒ expected
  offsets `{}`; the validator inside `SnapshotProducer` uses `SnapshotUtil.latestSnapshot(base, branch)`, which falls
  back to **main**'s head when the ref is missing (`SnapshotUtil.java:478-489`). If main's ancestry carries this
  connector's offsets key (connector previously committed to main, or a WAP branch was fast-forwarded into main and then
  dropped), every commit fails with `ValidationException` (non-retryable ⇒ coordinator terminates ⇒ task FAILED) until
  the branch is created by hand. Liveness/availability, not loss: DataWritten stay in the control topic.
- B2. *Stored offsets are only as durable as the snapshot that carries them.* Custom summary properties are not carried
  forward by other writers' snapshots (`SnapshotProducer.summary(previous)` copies only totals, `:394-473`); the lookup
  takes the first ancestor that has the key (`Coordinator.java:407-415`) and the walk stops at expired parents (E6).
  If the connector's last snapshot is expired while newer non-connector snapshots remain (idle connector — empty
  cycles create no snapshot, `:308-310` — plus compaction/other writers plus `expire_snapshots`), the coordinator reads
  `{}`; the validator then also compares `{}` with `{}` and passes. Any replay of already-committed DataWritten at
  that point (crash between table commit and `commitConsumerOffsets`, -coord offsets reset/expired, or a coordinator that
  reads the control topic from `earliest` as proposed by #18006) re-commits them ⇒ duplicate rows.
  Status: mechanism verified in code; triggerability depends on environment composition ⇒ model-checking target with an
  explicit `SnapshotMaintenance` environment action.
- B3. *Skipped tables advance the coordinator past their DataWritten.* `NoSuchTableException` and table-UUID mismatch
  `return` without error (`Coordinator.java:251-263`); the cycle then commits -coord offsets and clears the buffer
  (`:216-217`) ⇒ files for that table are dropped although their source offsets were committed by the worker
  transaction. Intentional per log text; upstream open PR #18012 proposes failing instead. Recorded as design-level loss.

### 3.2 Deep-analysis passes and cross-referenced findings

Four parallel passes, each reading its files in full against the Kafka 3.9.2 / Iceberg core sources: (P-C) coordinator
commit path + catalog semantics; (P-L) task lifecycle, rebalance, election; (P-W) Worker/Channel transactions;
(P-M) multi-coordinator and thread interplay. Every finding below was re-verified in the main context (re-read lines,
compensating mechanisms, full path, design intent, triggerability); "verified" notes what was checked independently.

Consequence legend as in §2.2. ID = brief cross-reference.

| ID | Finding | Evidence (file:line) | Trigger | Consequence | Verdict | Found by / verified |
|---|---|---|---|---|---|---|
| **F1** (S1, MC-1, T-1) | **One physical table under two `TableReference` keys: first group's floor claims the second group's DataWritten** | `CommitState.java:139-144` (groupingBy full ref); `TableReference.java:144-160` (`uuid` in equals/hashCode, d85f8a87a, first released 1.11.0); `Coordinator.java:256` (null uuid passes), `:270-275` floor = max(stored, position), `:277-285` filter, `:308-310` silent skip, `:216-217` checkpoint+clear; `IcebergWriterFactory.java:72-78` | DataWritten for the same table from ≤1.10.x workers (no `table_uuid`; optional Avro field ⇒ null) and ≥1.11 workers in one cycle (rolling upgrade, coordinator upgraded first); also catalog-name / identifier spelling change with in-flight DataWritten. Sequential group commits: 2nd group "nothing to commit" (INFO) and dropped. Parallel: 2nd fails `ValidationException` ⇒ full commit terminates ⇒ task FAILED ⇒ replay reads the 1st group's floor ⇒ dropped | **LOSS** (source offsets already committed by workers) | **CONFIRMED** by code reading | P-C; verified by me: release tags (`git tag --contains d85f8a87a` ⇒ 1.11.0), diff of d85f8a87a (adds uuid to equals), PR threads #14979 (singhpk234 asked about upgrades) and #15011 ("handle the upgrade case" — null tolerated in the *check* only); no upstream report (searched) |
| **F2** (S3, MC-3, T-3) | **`WakeupException` from `consumer.committed()` inside `close()` is treated by Connect as a benign wakeup** | `CommitterImpl.java:163` (discard) → `:186` → `KafkaUtils.java:66-67` (not covered by the `IllegalStateException` catch `:77`); `ConsumerNetworkClient.java:252-295,527-532` (wakeup raised in `poll(future,timer)`), wakeups disabled only in `ConsumerCoordinator.close()` `:978`; `WorkerSinkTask.java:204-207` (`transitionTo` ⇒ `consumer.wakeup()`), `:786-790` (stored), `:497-505` (rethrown after records returned), `:248-262` (handled as pause/resume, task continues) | Connector pause/resume (any target-state transition) while a revoke/lost callback is executing `close()` (window = Worker stop + new Admin describe + `committed()`, ≥ hundreds of ms) | **LOSS**: retained partitions not rewound ⇒ discarded buffer [C,P) lost; the batch returned by that `poll()` is dropped while positions advanced (eager, cooperative and lost alike); later worker txns commit past both | **CONFIRMED** by code reading (Iceberg + Kafka 3.9.2 client + Connect runtime) | P-L; verified by me in all four files; no upstream report (searched) |
| **F3** (S2, MC-2) | Floor reachable only via ancestry of connector snapshots | `Coordinator.java:395-415`, `SnapshotUtil.java:243-254`, `SnapshotProducer.java:394-473`, `Coordinator.java:371` | connector's last snapshot expired while newer non-connector snapshots remain (idle connector creates no snapshots, `:308-310`) + a replay window (crash between `:325/340` and `:216`; multi-table partial success with default max-consecutive-failures 1; `-coord` offsets lost/reset to `earliest`) | **DUP** (and dangling file refs if compaction+expiry deleted the files) | PLAUSIBLE (mechanism confirmed; trigger needs env composition) | B2 (me) + P-C F3 + reviewer wombatu-kun on #17925 |
| **F4** (S2, MC-5, T-2) | Branch-head divergence between floor read and validator | `Coordinator.java:388-393` vs `SnapshotUtil.java:478-489`; `Table.java:380-387`; `SnapshotProducer.java:298-304,381-388` | `commit-branch` set to a missing branch while main carries the connector key (WAP branch dropped after fast-forward; switching an existing connector to a branch) | **STALL**: every commit `ValidationException`; full ⇒ task FAILED on every restart; partial ⇒ retried forever, buffer never cleared | **CONFIRMED** | B1 (me) + P-C F2 |
| F5 (S4, T-4) | Coordinator thread hangs forever after `terminate()` when table tasks were queued | `Coordinator.java:437-441` (`shutdownNow` drains queue), `Tasks.java` `waitFor` (`future.get()` no timeout), `CoordinatorThread.java:59-63,83-86` | more tables in a cycle than `commit.threads` and a close/stop mid-commit | STALL/leak: `stop()` never runs; consumer stays in `-coord` until poll-timeout; successor delayed | **CONFIRMED** (P-M checked JDK semantics in jshell) | P-C F6, P-M F1; `waitFor` re-read by me |
| F6 (S4, T-5) | Terminated coordinator never leaves `consumeAvailable` under continuous control traffic | `Channel.java:123-151` (no `terminated` check), `Coordinator.java:136`, `CoordinatorThread.java:49-57`; default shared control topic `IcebergSinkConfig.java:101` | records arrive at <1 s intervals (busy shared control topic) after `terminate()` | STALL: zombie keeps the only `-coord` partition; successor sees nothing; commits stall while traffic lasts | CONFIRMED path / PLAUSIBLE trigger | P-M F2; re-read by me |
| F7 (S4, T-6) | Long commit phase ⇒ checkpoint with stale `-coord` member ⇒ Kafka `CommitFailedException` ⇒ task FAILED | `Channel.java:199`; `ConsumerCoordinator.java:202,351,969-971`; `AbstractCoordinator.java:1519-1522`; `Coordinator.java:57,176-179` | per-table commits (Iceberg retries up to 30 min total by default) exceed `max.poll.interval.ms` (300 s) on the coordinator consumer | crash (task FAILED, manual restart); safe (replay dedups) unless F3 | CONFIRMED (mechanism) | P-C F4, P-M F4 |
| F8 (S4, MC-5) | Zero coordinators after a fenced coordinator is cleared while `startCoordinator()` was a no-op | `CommitterImpl.java:141-151,199-203,223-231` | p0 bounces A→B→A while A's old coordinator (not stopped, #17637) is still referenced; B stops its coordinator; A's old one hits the fence and is cleared | STALL (no StartCommit until p0 is re-opened) | PLAUSIBLE | P-M F3, PR review on #17376/#17450 (N5) |
| F9 (S3/S4, MC-5) | Election only from *added* partitions; missed election never retried | `CommitterImpl.java:147`, `:97-104`; classic describe returns no assignments for non-Stable groups (P-C/P-L, #17637 premise) | cooperative: the coordinator's task stops; first partition added to T2 while the group is not Stable | STALL (no coordinator) | PLAUSIBLE | P-L F2 |
| F10 (S4, T-7) | Benign races fail the task: validator conflict is fatal; `Tasks.throwOne` can surface `ValidationException` over `ProducerFencedException` | `Coordinator.java:176-179`; `CoordinatorThread.java:78-81`; `Tasks.java` `throwOne` | zombie round overlapping successor; fenced zombie with two tables | crash (task FAILED) | PLAUSIBLE | P-M F6 |
| F11 (S5) | Readiness count not deduplicated; total frozen at construction | `CommitState.java:61-71,117-128`; `Coordinator.java:106-107`; `Worker.java:79-90` | incremental partition move during a commit (two owners answer), partitions added without re-election | META: valid-through-ts overstated (premature "full" commit) | PLAUSIBLE (known family #18322/#17925) | P-W F3, P-M F7, P-C F7 |
| F12 (S1) | High-water mark makes live instances skip every record of a recreated control topic | `Channel.java:126-139`; then `Coordinator.java:277-285` | control topic deleted/recreated (operator fix, auto-create, cluster switch) | STALL then LOSS (extends known #15293) | CONFIRMED (extension of known) | P-W F1 |
| F13 (6.2 T-8) | One DataWritten per table result is never split | `Worker.java:94-106`, `Channel.java:87-103` | many files/table/worker per cycle > `max.request.size` | deterministic crash loop | PLAUSIBLE | P-W F5 |
| F14 (6.2 T-9) | `CommitToTable.snapshotId` read from the refreshed head | `Coordinator.java:343`; `SnapshotProducer.java:555` (post-commit refresh) | another writer commits right after | META (downstream events) | CONFIRMED (low) | P-C F5 |
| F15 (availability) | Worker txn failure (fencing/timeout/KIP-848 epoch bump) kills the task | `Channel.java:109-116`; `WorkerSinkTask.java:624-636`; `TransactionManager.java:1697-1701` | eviction during long put(), coordinator unavailability, KIP-848 epoch race | crash; safe (txn atomic) | CONFIRMED (classic) / PLAUSIBLE (KIP-848) | P-L F4, P-W F4; #16366 |
| — | Known, re-derived independently (not counted as new) | #18006 (`latest`, plus never-seen control partitions), #18767 (null-offset seek), #18012 (skips), #15293, #17637, #17077, #18322 | — | — | KNOWN | P-C, P-L, P-W, P-M, archaeology |

Cross-referencing notes:
- F1 and F12/#15293/#18012/#18006 share one mechanism (the floor is the coordinator position, not the max offset of what
  was included) ⇒ Scenario 1. F3, F4 and the `earliest` proposal share "floor reachability" ⇒ Scenario 2. F2 and
  #18767/#12372 share "discard first, rewind later" ⇒ Scenario 3. F5–F10/F15 and #17637/#16016/#17349 share coordinator
  multiplicity/lifecycle ⇒ Scenario 4. F11/#18322/#17077/#17811 ⇒ Scenario 5.
- The four deep passes independently refuted every duplicate path for zombie coordinators (P-C R1–R4, P-M R1–R10) — the
  per-table at-most-once argument (P-M §3) is: snapshot k is accepted only if the latest stored floor in the refreshed
  ancestry equals what its builder read; it contains only envelopes ≥ that floor and stores a floor strictly greater than
  every included offset. Preconditions: (a) no `latest` jump (#18006), (b) checkpoint only after all tables and only by
  the newest epoch, (c) the latest connector snapshot stays reachable (F3), (d) control offsets never reset (#15293),
  (e) floor reader and validator use the same ancestry (F4), (f) skips are deliberate (#18012) — and, from F1,
  (g) all envelopes of a table are committed in one group per cycle.

### 3.3 Priority questions (target guidance) — handling

| Q | Answer from code analysis | Evidence | What MC must settle |
|---|---|---|---|
| Q1 | No duplicate path. Loss: known null-offset seek (#18767) and **F2** (wakeup during close). Eager, lost, stop, crash are otherwise safe: atomic worker txn, KIP-447 (E1), requireStable (E2), Worker stopped synchronously on every close | §3.1 E1-E3, F2, P-L §3 table | MC-3 with the known bug disabled; KIP-848 ≈ cooperative callbacks |
| Q2 | No skipped/repeated commit except via known #18006; zombie table commits are bounded by validator+floor; liveness defects F5–F10 | §3.2, P-M §3 | MC-4 (safety composition), MC-5 (liveness) |
| Q3 | Late DataWritten committed exactly once; floor never covers unconsumed records (`Channel.java:139` before `receive`); valid-through-ts wrong both ways (F11, #17077) | P-C R6, P-W D, F11 | MC-4 with timeouts; ValidThroughSound secondary |
| Q4 | Retry/restart after per-table failure/unknown outcome is safe (filtered by floors; unknown outcome rethrown, buffer kept) — **except F1** (one table, two groups) and deliberate skips (#18012) | P-C R4/R5/R7, F1 | MC-1 |
| Q5 | New/recreated control topic ⇒ LOSS (#15293, F12); missing `-coord` offsets ⇒ `latest` ⇒ LOSS (#18006, never-seen partitions); floors missing ⇒ DUP on replay (F3); `earliest` makes dedup depend on snapshot retention; new connector name ⇒ fresh key/groups (safe); Connect-group reset ⇒ re-ingestion (operator intent; the table stores no source offsets, so it cannot be detected) | §2.2, F3, F12 | MC-1 (reset OFF/ON), MC-2 |
| Additional exploration | Paths where committed source offsets get ahead of the table: F1, F2, #15293/F12, #18006, #18767, #18012; control-topic retention shorter than a coordinator outage (env, #15844); orphan-file cleanup of files referenced only by uncommitted DataWritten (C-6) | §3.2 | MC-1, MC-3 |

### 3.4 Refuted hypotheses and excluded false positives

| # | Hypothesis | Why refuted (evidence) |
|---|---|---|
| R1 | Zombie worker whose consumer left the group commits with NO_GENERATION and bypasses KIP-447 | `groupMetadata` replaced only on join (`ConsumerCoordinator.java:202,351`); stale member/generation rejected (`GroupCoordinator.scala` txn validation) |
| R2 | `seekToLastCommittedOffsets`/position init reads a stale committed offset while a worker txn is pending | classic consumer always uses `requireStable=true` (`ConsumerCoordinator.java:1443-1444,1498`) |
| R3 | Failed table commit still advances `-coord` offsets | `Tasks.throwFailureWhenFinished=true` (`Tasks.java:77,396-400`) |
| R4 | Unknown outcome (CommitStateUnknown) on one table + success on another ⇒ dup/loss on retry | rethrown without cleanup (`SnapshotProducer.java:538-539`); buffer kept; retry re-reads floor; CAS+validator exclude double landing |
| R5 | `throwOne` hiding CommitStateUnknown behind CommitFailed ⇒ unsafe in-process retry | retry re-reads floors ⇒ still safe (only classification differs) |
| R6 | Iceberg-internal retry after a commit that actually landed duplicates files | validator re-runs on every attempt after refresh (`SnapshotProducer.java:297-304,377-389`) ⇒ ValidationException |
| R7 | Zombie double-commits its stale buffer / advances `-coord` offsets | validator + floor; stable txn id fencing (`IcebergSinkConfig.java:463-465`, `KafkaClientFactory.java:49`) |
| R8 | Fenced zombie keeps committing after the partial path swallows the fence | no new commit without a StartCommit, which fails on a fenced producer; liveness only (≤ one interval) |
| R9 | Control partitions split between zombie and successor ⇒ dup/loss | floor per (table, control partition) with max-merge; only newest epoch checkpoints ⇒ liveness only |
| R10 | High-water-mark skip hides unprocessed records after partition moves | skip only drops offsets this instance already processed; forward jumps start at a checkpoint that advances only after all tables commit |
| R11 | Executor threads race on `controlTopicOffsets`/`commitState` | coordinator thread parked in `waitFor` during table commits; reads only |
| R12 | Offsets committed for a revoked partition / records of revoked partitions reach `put()` | Worker stopped synchronously on every close (`CommitterImpl.java:163`); `messageBatch` survives only after Connect `RetriableException`, never thrown by the sink (grep) |
| R13 | Connect runtime rewinds to stale offsets | only if `preCommit` throws (`WorkerSinkTask.java:435-441`); it cannot |
| R14 | Worker double-answers a StartCommit / answers with mismatched rows-vs-offsets | HWM skip + fresh `latest` consumers; offsets recorded with rows and cleared together (`SinkWriter.java:59-95`) |
| R15 | Stale cached `Table` makes the filter read old floors | no `CachingCatalog` (`CatalogUtils.java`); validator would reject anyway |
| R16 | Interrupted in-flight catalog commit is cleaned up after landing | strict cleanup default; landed commit deduped later |
| R17 | Compaction/other writers' snapshots confuse floors | ancestry walk skips snapshots without the key (`Coordinator.java:410-413`) — harmful only once the key-bearing snapshot is expired (F3) |
| R18 | Merging several rounds into one RowDelta breaks equality-delete ordering (#13752 t3hw, #17455) | latent only: writers append-only (`RecordUtils.java:163-179`) |
| X1 | Issue #17455 (dup PK rows after crash) | false positive for this snapshot (no delete writers) |
| X2 | Issue #16659 (Databricks 400) | vendor spec violation (`BadRequestException` not retryable by design) |

---

## 4. Synthesis

- **Scenarios** (brief §2): S1 floor over-claim (High; F1 new LOSS + #15293/#18012/#18006), S2 floor reachability
  (High; F3 DUP, F4 STALL, interplay with PR #18006), S3 discard-then-rewind in `close()` (High; F2 new LOSS + known
  #18767), S4 coordinator multiplicity/lifecycle (Medium; safety argued, F5–F10 liveness), S5 readiness/partial
  commits/valid-through (Medium; META/STALL, mostly known).
- **Category** carried forward: A (distributed). The base spec should be a message-passing model with explicit crash,
  rebalance, epoch fencing and catalog CAS with nondeterministic outcomes; trace validation (later phase) should log
  control-topic events with (partition, offset), table floors, `-coord` checkpoints and Connect committed offsets.
- **Answer to the guidance goal at code-analysis level**: the design's exactly-once argument is sound in its core
  (atomic worker transaction + floor/validator), but it is **conditional**: on no rolling-upgrade/config change that
  splits a table's `TableReference` (F1), on no pause/resume during a rebalance callback (F2), on the connector's last
  snapshot surviving table maintenance (F3), on the control topic never being recreated (#15293) and on the coordinator
  never resetting to `latest` (#18006); plus the known cooperative null-offset seek (#18767). Several liveness defects can
  leave a connector silently stalled or FAILED (F4–F10), which turns into loss only when control-topic or source retention
  expires.

## 5. Run notes

- The provided checkout is shallow; git mining used a separate blobless clone (no builds or code execution from it).
- MCP servers `inv_checking_tool`, `spec_analyzer`, `tracedebugger` failed to connect in this session; no model checking
  or trace tooling was used in this phase (none is required by the code-analysis skill).
- All verdicts are from code reading; nothing was reproduced by executing the system.
