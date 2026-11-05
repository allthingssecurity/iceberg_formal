# CR-1 — Phase 1 investigation (evidence only)

Finding: control-topic offsets carry no topic identity. A recreated control topic is skipped by the
in-memory high-water mark (HWM) in `Channel.consumeAvailable` and filtered by the stored floor in
`Coordinator.commitToTable`. Claimed effect: STALL, then LOSS. Source: code review (Scenario 1
residual; never model-checked, `MaxRecreate = 0`).

Code base audited: pristine `HEAD` 9fffeb63b1605a7c3acf218e58b7eeb43bdb1b01, read with
`git show HEAD:<path>` because the worktree carries TLA+ instrumentation. Upstream `main`
(ea73769d12, 2026-11-04) is 23 commits ahead of this HEAD and none of them touch `kafka-connect/`
(`gh api repos/apache/iceberg/compare/9fffeb6...main`). This is therefore current upstream code.
All line numbers below refer to pristine HEAD.

## Step 1 — Code audit

### Cited sites

| Site | What it does |
|---|---|
| `channel/Channel.java:57` | `Map<Integer, Long> controlTopicOffsets`, keyed by **partition number only** (no topic id or epoch) |
| `channel/Channel.java:126-135` | `nextOffset = controlTopicOffsets.get(partition)`; `if (nextOffset != null && record.offset() < nextOffset) continue;`, which **silently skips** the record (DEBUG log only) |
| `channel/Channel.java:139` | `controlTopicOffsets.merge(partition, offset + 1, Long::max)`: the map never decreases |
| `channel/Channel.java:171-213` | `commitConsumerOffsets`: commits a partition only if `offsetToCommit > committedOffsets.get(p)` (in-memory, also partition-keyed) |
| `channel/Coordinator.java:97-102` | coordinator control consumer group = `connectGroupId + "-coord"` |
| `channel/Coordinator.java:108-110` | floor key `kafka.connect.offsets.<controlTopic>.<connectGroupId>`, with no topic id or epoch |
| `channel/Coordinator.java:270` | `committedOffsets = lastCommittedOffsetsForTable(table, branch)` (stored floor from the snapshot summary) |
| `channel/Coordinator.java:271-275` | stored offsets merged with `Long::max`, so the floor never decreases |
| `channel/Coordinator.java:277-285` | DataWritten kept only if `envelope.offset() >= committedOffsets.get(partition)` |
| `channel/Coordinator.java:308-310` | all filtered: logs "found nothing to commit to table ..., skipping" and returns normally |

### Related code on the path

- `channel/Worker.java:50-53`: the worker's control consumer group is `controlGroupIdPrefix + UUID.randomUUID()`. It is transient and never commits, so each new Worker starts at `auto.offset.reset`.
- `channel/KafkaClientFactory.java:55`: `auto.offset.reset` defaults to `latest`.
- `channel/Worker.java:67-114`: on StartCommit, `sinkWriter.completeWrite()` runs and then `send(events, results.sourceOffsets())` (line 111).
- `channel/Channel.java:98-117`: DataWritten + DataComplete records and `sendOffsetsToTransaction(sourceOffsets, connect consumer group metadata)` go out in **one Kafka transaction**. Source offsets are committed when the worker answers StartCommit.
- `IcebergSinkTask.java:99-104`: `preCommit` returns an empty map. The worker transaction is the only path that commits source offsets.
- `channel/Coordinator.java:126-141`: `process` calls `consumeAvailable`, then checks the commit timeout. `doCommit` (203-231) with an empty commit map still calls `commitConsumerOffsets` and sends CommitComplete, so a timed-out round "succeeds" with 0 tables.
- `channel/CommitState.java:105-115`: timeout check. `CommitState.java:117-137`: readiness check.
- `channel/CommitterImpl.java`:
  - A Worker is created lazily on `save` with records (189-196, 214-221) and is stopped on **every** `close(partitions)` (161-163).
  - The coordinator starts on `open` when the task holds the leader partition (141-151). It stops only when the closed partitions include the leader partition (178-182) or the task stops (172-176).
  - Under cooperative rebalancing, restarting a non-leader task therefore recreates the Workers (fresh, empty HWM) and keeps the running Coordinator (stale HWM).

### Call chains (all normal entry points)

- Worker path: Connect `WorkerSinkTask` → `IcebergSinkTask.put/flush` → `CommitterImpl.save` → `Worker.process` → `Channel.consumeAvailable` → `Worker.receive(StartCommit)` → `Channel.send` (DataWritten + DataComplete + source offsets).
- Coordinator path: `CoordinatorThread.run` → `Coordinator.process` → `Channel.consumeAvailable` → `Coordinator.receive` → `CommitState` → `commit` → `doCommit` → `commitToTable` → `AppendFiles.commit`.

### Reachability

The control topic is an ordinary Kafka topic. Deleting and recreating it uses the Admin API or `kafka-topics.sh`. The topic is also **auto-created by default** (`docs/docs/kafka-connect.md:263-264`). A recreated topic restarts its offsets at 0. Realistic triggers:

- an operator recreates it (wrong partition count, retention, or cleanup policy; troubleshooting, as the #15293 reporter did);
- it is deleted by accident and auto-created again;
- a cluster switch or migration (#15293).

No code path compares a topic id, an epoch, or a log-start/end offset against the remembered positions.

### Trigger scenarios

- **T1, STALL (HWM; running instances).**
  1. The connector runs and commits; every running Channel holds `controlTopicOffsets = {0: H}`.
  2. The control topic is deleted and recreated, so new offsets start at 0. The consumers resume on the new topic (no committed offset → `latest`).
  3. Every new record with offset < H is skipped by every running Channel:
     - workers never see StartCommit, so they never send DataWritten or commit source offsets;
     - the coordinator never sees responses, times out, and "commits" 0 tables.
  4. No table commit happens until the new topic's offsets exceed H, or until the Channels are recreated.
- **T2, LOSS (HWM; partial restart).** Start from the T1 state.
  1. A non-leader task restarts (a REST restart, a task failure, or a worker roll). Under cooperative rebalancing its Workers are recreated with an empty HWM, while the coordinator keeps HWM H.
  2. A fresh Worker answers the coordinator's StartCommit (offset < H) and commits its source offsets in the same transaction as DataWritten.
  3. The coordinator skips that DataWritten and DataComplete (offset < H).
  4. The records' source offsets are committed, but the records are never in the table. The coordinator also never commits a control-topic checkpoint for them (`commitConsumerOffsets` needs offset > in-memory committed H), so a later coordinator does not re-read them. A floor-respecting re-read would filter them anyway (offset < floor).
- **T3, LOSS (stored floor; after a coordinator restart).** A restarted coordinator has an empty HWM but reads the floor `{0: F}` from the snapshot. Each new DataWritten with offset < F is filtered at `Coordinator.java:277-285`, while the workers have already committed the source offsets. This is exactly the #15293 / PR #16360 mechanism (see Step 3).

### Safeguards and masks recorded (Phase 2 must prove or refute them)

- S1. A send during the deletion window could fail (missing topic, or `max.block.ms` exceeded). The thread dies, the task fails, and an operator restart clears the HWM, which leaves only T3. Needs testing: does a running connector survive a quick delete and recreate?
- S2. T1 self-heals once the new topic's offsets pass H. While stalled, the topic grows by about 4 offsets per commit round (StartCommit, CommitComplete, and 2 transaction markers). A long-running connector's H can be in the thousands to millions.
- S3. Any rebalance that restarts the coordinator clears its HWM. That turns T1 into T3, not into a recovery.
- S4. `SnapshotAncestryValidator` (`Coordinator.java:361-381`) compares expected and stored offsets. It is irrelevant here because no append is attempted.
- S5. `CommitState.addResponse` logs "Received commit response when no commit in progress". Skipped records never reach it.

## Step 2 — Developer knowledge (verbatim, not classified)

- **Comment at the site.** `Channel.java:127-128`: "A rebalance can rewind the consumer to the committed offset, which can lag the in-memory position. Skip already-processed records." The guard's stated purpose is a same-topic rewind.
- **PR #17713** (ef99ad3f84, merged 2026-09-17) introduced the skip guard: "Fix coordinator potentially committing files from prior commit in certain rebalance scenarios". Its body shows the rewind example (committed offset 100, Channel next offset 110, "The next poll() can return offsets 100-109 again"). It does not mention topic deletion, recreation, or offset reset (searched its body, issue comments, and review comments).
- **PR #17933** (4d725363c3, merged 2026-09-08) made the map monotonic (`put` → `merge(..., Long::max)`) for #17340. It has no mention of recreation.
- **PR #17552** (1019497e35, merged 2026-09-08) commits coordinator offsets only when they are greater. A comment there reads: "If rewound out of retention, the consumer falls back to its auto offset reset strategy, causing potential data loss." It is about stale coordinators, not recreation.
- **PR #14525** ("Merge control topic and last persisted offsets", 8da07dcae8). Reviewer singhpk234 asked about a control-topic offset lower than the committed one. Maintainer bryanck replied: "This is the correct behavior. The offsets in the table correspond to the data committed. If the user did want to roll back the offsets, they also need to roll back the offsets in the table, either by rolling back the snapshot or by updating the offsets manually. (The hope was we'd have tools to help with that at some point.)" The stored floor is intended to be durable; lowering it is a manual operator task. This concerns the stored floor, not the in-memory HWM.
- **Docs** (`docs/docs/kafka-connect.md`):
  - line 39 lists "Exactly-once delivery semantics" as a feature;
  - lines 263-264: the control topic "will be automatically created" when `auto.create.topics.enable` is true (the default);
  - there is no guidance on recreating the control topic or resetting the table offsets.
- **Tests:**
  - `TestControlTopicReplay.handlesReplayedControlRecordsOnce` / `doesNotCommitReplayedDataFilesTwice` assert that a same-topic seek-back replay is skipped;
  - `TestChannel` asserts the monotonic map;
  - `TestCoordinator.testCoordinatorCommittedOffsetMerging` asserts the max-merge with stored offsets;
  - no test covers a deleted or recreated control topic, or a control-topic offset reset.

## Step 3 — Known-status / precedent

Searches run (`gh search issues` / `gh search prs`, repo apache/iceberg, 2026-11-05):

- "15293", "control topic recreated", "control topic deleted", "Nothing to commit to table";
- "kafka.connect.offsets", "controlTopicOffsets", "control topic offsets reset";
- "Skipping already-consumed control topic offset", "delete control topic", "control-iceberg";
- "control topic retention", "kafka cluster recreation", "kafka connect recreate";
- "kafka connect nothing to commit", "kafka connect high-water mark", "kafka connect topic id", "kafka connect stall no commits".

I also read the commit history of `Channel.java` and `Coordinator.java` on main, and the open control-topic PRs #18006, #18030, #17350, #17593, #18290, and #16576 (closed).

- **Stored-floor part (T3; `Coordinator.java:108-110, 270-285`): KNOWN, unfixed.**
  - https://github.com/apache/iceberg/issues/15293 (open, label `bug`, 1.9.2-1.11.0): "Metadata is not committed after Kafka cluster recreation". The logs show "Nothing to commit to table ..., skipping", Parquet files accumulate, and consumer lag drops to zero.
  - https://github.com/apache/iceberg/pull/16360 ("Kafka Connect: recover from metadata commit stall after Kafka cluster recreation", closed unmerged as stale on 2026-07-19) states the root cause: "The control topic resets to offset 0 on the new cluster, but the Iceberg snapshot still stores committed offsets from the old cluster (e.g. `{"0":100}`). The deduplication filter in `Coordinator.commitToTable()` rejects every `DataWritten` event whose offset is below the stored baseline, so all data files are silently dropped." This is the same mechanism at the same site.
- **In-memory HWM part (T1/T2; `Channel.java:126-139`): no report found.**
  - The skip guard did not exist when #15293 was filed (2026-02-11, 1.9.2) or when #16360 was closed (2026-07-19). It was added on 2026-09-17 by #17713.
  - None of #15293's comments, #16360, #17713, #17933, #17552, #18039, #18006, #18030, #17350, #17593, #18290, or #16576 mention a recreated or reset control topic together with the in-memory HWM.
- **Pre-filter decision.** The finding is code-review-sourced, but it is **not** an exact duplicate: its HWM site and mechanism (running instances skip the new topic) are unreported. Only its stored-floor half is #15293 / #16360. → **Proceed to Phase 2.** Reproduction must isolate the HWM consequence. The stored-floor half is recorded as KNOWN (#15293 / #16360, unfixed).

Persistent-findings lookup (`specula findings lookup` with queries on control topic recreated, controlTopicOffsets, consumeAvailable, kafka.connect.offsets, commitToTable offset filter) returned `[]`.
