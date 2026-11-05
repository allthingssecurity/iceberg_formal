# MC-3 — Phase 1 investigation (evidence only, no verdict)

Finding: the Coordinator's only durable replay guard (the `kafka.connect.offsets.<ctl>.<group>`
snapshot-summary "floor") is found only by walking the ancestry of the branch head, which stops at
the first expired parent. After a foreign (non-connector) snapshot becomes the head and the
connector's last snapshot is expired, the floor reads `{}` and a replayed, already-committed
DataWritten is committed a second time.

Source: model checking, real counterexample `spec/output/MC_hunt_s2_floor_reachability_latest_bfs.out`
(invariant `NoDuplicateRows`, 37 states). Code audited: pristine `apache/iceberg@9fffeb6`
(`git show HEAD:<path>` in the worktree; the worktree itself carries the Specula trace
instrumentation, so line numbers below are from the pristine HEAD blobs).

## Counterexample, decoded (per-step variable diffs, `notes/tracediff.py`)

| CE state | Action | Effect |
|---|---|---|
| 2-5 | JoinRebalance, Open(t1), CoordStart | t1 owns src p0, coordinator #1 starts; `-coord` group starts at `latest` (=0, empty topic) |
| 6-11 | StartCommit c1, timeout, empty partial commit | `-coord` checkpoint `cgCommitted = 1` |
| 12-13 | StartCommit c2 (ctl off 1); worker answers DW(c2, row <0,0>) @2 + DC @3 in one txn | source offset p0 committed = 1 |
| 14-18 | coordinator #1 reads DW/DC, full commit, ReadFloor `{}` (empty table), CAS | snapshot S1 `{floor {0:4}, files DW@2, writer conn}` |
| 19 | **Crash of t1** after `appendOp.commit()`, before `commitConsumerOffsets()` | `cgCommitted` stays 1 |
| 20-24 | Restart, rejoin, coordinator #2 starts from the `-coord` checkpoint 1 | |
| 25-28 | coordinator #2 re-reads SC c2 @1, **DW(c2) @2 (replay, buffered)**, DC c2 @3 | |
| 29-33 | StartCommit c3 @4; worker answers DW(c3, row <0,1>) @5 + DC @6; commit ready | buffer = {DW@2, DW@5} |
| 34 | **OtherWriterCommit**: snapshot S2, no key, parent S1 | head = S2 |
| 35 | **ExpireSnapshot S1** | S1 no longer resolvable |
| 36 | ReadFloor: ancestry S2 -> parent S1 missing -> stop -> `exp = {}` | filter passes DW@2 |
| 37 | CAS: S3 adds DW@2's files again + DW@5 | row <0,0> twice -> `NoDuplicateRows` violated |

## Step 1 — Code audit (pristine 9fffeb6)

### Cited sites
- `Coordinator.java:395-405` `lastCommittedOffsetsForTable`: `latestSnapshot(table, branch)`, then
  `SnapshotUtil.ancestorsOf(snapshot.snapshotId(), table::snapshot)` -> `lastCommittedOffsets`.
- `Coordinator.java:407-415` `lastCommittedOffsets`: first snapshot whose summary contains
  `snapshotOffsetsProp`, else `Map.of()`.
- `SnapshotUtil.java:243-254` (ancestor iterator): `parentId = next.parentId(); next =
  lookup.apply(parentId); if (next != null) ... return true; return false;` — iteration ends at the
  first parent the table can no longer resolve (expired).
- `Coordinator.java:270` `committedOffsets = lastCommittedOffsetsForTable(table, branch)`.
- `Coordinator.java:277-285` replay filter: `minOffset == null || envelope.offset() >= minOffset` —
  with `{}` every buffered envelope passes.
- `Coordinator.java:292` `distinctByKey(ContentFile::location)` — dedups only inside one commit.
- `Coordinator.java:313-325` `table.newAppend().validateWith(offsetValidator(id, committedOffsets))`
  ... `appendOp.commit()`. (`newAppend()` = MergeAppend: no path-level dedup against the table.)
- `Coordinator.java:361-372` `SnapshotAncestryValidator.validate`: `expectedOffsets.equals(
  lastCommittedOffsets(baseSnapshots))`, with `baseSnapshots` built by `SnapshotProducer.java:381-384`
  `SnapshotUtil.ancestorsOf(parentSnapshot.snapshotId(), base::snapshot)` — same truncated walk, so
  `{}.equals({})` passes.
- `Coordinator.java:203-217` `doCommit`: `commitToTable` for every table, *then*
  `commitConsumerOffsets()` (`Channel.java:171-213`: separate Kafka transaction,
  `sendOffsetsToTransaction(..., consumer.groupMetadata())` for group `connectGroupId + "-coord"`,
  `Coordinator.java:98`), then `clearResponses()`. Table commit and checkpoint are not atomic.
- `Coordinator.java:159-201` `commit()`: partial-commit failure -> warn, return (buffer kept);
  full-commit non-`CommitFailedException` (e.g. `CommitStateUnknownException`) -> rethrow
  ("not retryable, terminate immediately"); `CommitFailedException` -> retry up to
  `commitMaxConsecutiveFailures` (buffer kept).
- `CoordinatorThread.java:49-63`: exception -> `terminated`, `coordinator.stop()` (no checkpoint).
- `CommitterImpl.java:198-208`: terminated, non-fenced coordinator -> `NotRunningException` from
  `put()` -> the task FAILS; it is restarted by an operator / REST.
- `Channel.java:228-233` `start()`: `consumer.subscribe(controlTopic)` -> resumes at the `-coord`
  committed offset (`KafkaClientFactory.java:55`: `auto.offset.reset` default `latest`).
- `Channel.java:122-152` `consumeAvailable`: skip guard on the in-memory `controlTopicOffsets` map —
  per coordinator instance only (a new instance starts empty).
- `CommitState.java:51-59` `addResponse`: buffers a DataWritten even with no commit in progress
  ("this can happen during recovery"); `CommitState.java:139-144` `tableCommitMap` = whole buffer.
- `Worker.java:94-111`: DataWritten + DataComplete are sent in one transaction together with the
  source offsets (`sendOffsetsToTransaction`), so the source offsets advance when the DataWritten is
  published, not when the table commit lands. A replayed DataWritten is never re-read from source.

### Call chain (production)
`WorkerSinkTask` -> `IcebergSinkTask.open` -> `CommitterImpl.open` -> `hasLeaderPartition` ->
`startCoordinator` -> `CoordinatorThread.run` -> `Coordinator.start` (`Channel.start`, subscribe
`-coord`) -> loop `Coordinator.process` -> `consumeAvailable` -> `receive` ->
`CommitState.addResponse/addReady` -> `commit` -> `doCommit` -> `commitToTable` ->
`lastCommittedOffsetsForTable` -> filter -> `appendOp.commit()` -> `commitConsumerOffsets`.
All public/normal paths; nothing test-only.

### Reachability
Two independent preconditions, both reachable in normal operation:
1. **A replay of already-committed DataWritten** (the `-coord` checkpoint lags the table):
   - process crash between `Coordinator.java:213` (table commit returned) and `:216` (the CE);
   - the table commit lands but the client sees `CommitStateUnknownException` (catalog/REST timeout)
     -> full commit rethrows -> coordinator terminates -> task FAILED -> restart replays;
   - partial-commit failure / `CommitFailedException` on another table after one table landed
     (multi-table) -> buffer kept, re-committed next cycle in the same process;
   - `send(CommitToTable)` or `commitConsumerOffsets()` throwing after the table commit landed
     (Kafka transaction timeout, fencing);
   - lost / never-written `-coord` offsets with an `earliest` reset (PR #18006 proposal).
2. **The floor unreachable from the head**: the branch head is a snapshot without the key
   (Spark `rewrite_data_files` / `rewrite_manifests`, another application or another connector
   group writing the table, row-level DML) **and** the connector's last key-carrying snapshot was
   expired (`expire_snapshots`, e.g. `retain_last => 1` after compaction, or `older_than` after a
   long outage). Custom summary properties are not carried forward by `SnapshotProducer`.

### Safeguards encountered (recorded, not dismissed — Phase 2 must show whether they fire)
- (a) the floor filter `Coordinator.java:277-285` — the guard under test;
- (b) `SnapshotAncestryValidator` `Coordinator.java:361-372` — compares two ancestry-derived floors;
- (c) `distinctByKey` `:292` — intra-commit only;
- (d) `Channel.consumeAvailable` skip guard `Channel.java:126-135` — per instance only;
- (e) table UUID check `Coordinator.java:256` — unrelated;
- (f) Iceberg core: `AppendFiles` (MergeAppend) performs no file-path dedup against live files.
No other guard was found between the replayed envelope and `appendOp.commit()`.

### Trigger scenario (concrete, operational)
1. Connector commits snapshot S1 (files F1, `kafka.connect.offsets` = {0: k}).
2. The checkpoint of the `-coord` group is not advanced past S1's DataWritten (crash in the window,
   or `CommitStateUnknownException` for a landed commit -> task FAILED).
3. While the task is down (or before the next commit), table maintenance runs: a compaction /
   manifest rewrite / other writer commits S2 on `main` (no key), then `expire_snapshots` removes S1.
4. The task is restarted; the new coordinator resumes the control topic at the stale checkpoint,
   buffers the replayed DataWritten(F1), and at its next commit reads the floor `{}`.
5. F1 is appended again in S3 -> every row of F1 is read twice; the source offsets of those rows were
   committed long ago, so nothing re-delivers or reconciles them.

## Step 2 — Developer knowledge (verbatim, with cites)

- Docs: `docs/docs/kafka-connect.md:39` "* Exactly-once delivery semantics"; `:61-62` "The sink
  relies on KIP-447 for exactly-once semantics."
- `Channel.java:158-170` (javadoc of `commitConsumerOffsets`): the offset transaction "covers only the
  consumer offset commit, not the Iceberg table snapshot commit ... that case is guarded separately at
  the Iceberg level by the SnapshotAncestryValidator offset validator".
- `CommitState.java:55-57`: "Received commit response when no commit in progress, this can happen
  during recovery" — replays during recovery are an expected input.
- Tests: `TestControlTopicReplay.doesNotCommitReplayedDataFilesTwice` comment "replaying records
  before the commit is ready, since a snapshot would record these offsets and filter them" — the
  developers rely on the snapshot floor to filter replays. `TestCoordinator`
  `testCoordinatorCommittedOffsetValidation` / `...Merging` exercise the validator / merge with
  key-carrying snapshots only; no test has a foreign head snapshot or an expired connector snapshot.
- History (GitHub API, the worktree is shallow): `d8e7642d4e` (#10351, "Commit coordination") added
  `lastCommittedOffsetsForTable` (parent walk); `6b28b0edc8` (#14510, "validate offsets for refreshed
  table state on commit": "validates that another commit hasn't moved the kafka offsets") added the
  validator; `29a144adca` (#14650) refactored it. No commit message discusses expired ancestors.
- Tracker:
  - **PR #18006** (open, nahidupa, "Read the control topic from earliest in the coordinator"), body:
    "Recovery requires retained control-topic announcements and, for deduplication, the relevant
    per-table offset boundary in reachable snapshot history. Ordinary commits do not automatically
    carry that boundary forward. Expiring its snapshots can permit duplicate file registration during
    historical replay without a valid Kafka checkpoint. This PR does not implement a durable
    checkpoint redesign or a missing-boundary policy."
  - **PR #17925** (closed unmerged 2026-10-24), "Out of scope": "The only cross-restart deduplication
    floor is the `kafka.connect.offsets` summary that `lastCommittedOffsetsForTable` finds by walking
    snapshot ancestry. Custom summary properties are not propagated by `SnapshotProducer`, so once
    every snapshot carrying it has expired the floor is gone. That is independent of this change and
    of #18006."
  - **PR #18401** (open, core, "Carry forward custom snapshot properties through replace commits"):
    opt-in table property `commit.snapshot-property-carry-forward.enabled`; author: "snapshot
    expiration can remove the last snapshot that still had the key, and after that nothing in the
    history has it ... A non-replace commit that doesn't set the key still ends the chain." Reviewer
    (pvary): "depending on the head's summary is brittle". Not merged; would not cover foreign appends.
  - #13763 (open): duplicate file paths in a sink-connector table whose only other writer is Spark
    maintenance (compaction, manifest rewrite, snapshot expiry); "The operation-type we are seeing
    before that failure/duplicate containing commits were maintenance jobs (replace type)". Root cause
    not identified in the thread.
  - #16282 (open) / #17340 (closed): same symptom via the crash-between-commit-and-checkpoint replay,
    attributed to the control-offset watermark regression, fixed by #17933 (present at
    `Channel.java:139`, `merge(..., Long::max)`). A #17340 comment notes the filter "only protects a
    partition it already knows about" (`minOffset == null` branch) but not expiry.

## Step 3 — Known-status

- Same mechanism at the same site (`lastCommittedOffsetsForTable` ancestry walk loses the floor after
  expiry; replay re-registers files) is described in the public tracker in PR #18006 (open) and
  PR #17925 (closed), both as an explicitly unaddressed limitation; no fix merged; PR #18401 is an
  open, opt-in, core-level partial mitigation. => **KNOWN (cite: https://github.com/apache/iceberg/pull/18006;
  also https://github.com/apache/iceberg/pull/17925; fix-status: unfixed)**.
- Persistent-findings lookup: no prior record (`specula findings lookup` -> `[]`).
- Source is MC with an actual counterexample -> the code-review x known pre-filter does not apply;
  proceed to Phase 2.
