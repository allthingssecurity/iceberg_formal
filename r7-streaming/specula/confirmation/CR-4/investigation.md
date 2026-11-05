# CR-4 — Phase 1 investigation (evidence only)

Finding: "The -coord checkpoint covers only control partitions this coordinator has read, so a
successor starts never-checkpointed partitions at the reset position (widens #18006)".
Source: code review (modeling-brief §6.3 C-9). No MC counterexample exists for this finding
(MC_hunt_s4_zombie_2ctl.cfg only ran with CoordReset = earliest).

Code under test: worktree `HEAD` = apache/iceberg `9fffeb6` (2026-11-02). The worktree's main
sources carry the TLA trace instrumentation (`git status`: 10 modified files + TlaHooks/TlaTrace);
all line numbers below are from **pristine `HEAD`** (`git show HEAD:<path>`), which is what the
finding cites. Upstream history (GitHub API, `commits?path=`) shows no later change to
`Channel.java` (last: #18039, 2026-10-02) or `KafkaClientFactory.java` (last: #10351, 2024-07-11),
so `HEAD` matches current `main` for the cited sites.

Persistent-findings lookup (`specula findings lookup --query commitConsumerOffsets
--query controlTopicOffsets --query "coord checkpoint" --query auto.offset.reset`): `[]`.

## Step 1 — Code audit

Paths relative to `kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/`.

### Cited code
* `channel/Channel.java:139` (`consumeAvailable`) — `controlTopicOffsets.merge(record.partition(),
  record.offset() + 1, Long::max)`: an entry exists only for control partitions from which **this
  Channel instance** has consumed at least one record (any group's record; the update is before the
  `groupId` filter at :143). The map is per instance and starts empty.
* `channel/Channel.java:171-184` (`commitConsumerOffsets`) — the offsets to commit are built only by
  iterating `controlTopicOffsets()` (:174); a partition without an entry is never put in
  `offsetsToCommit`, so the `connect-<name>-coord` group receives no committed offset for it.
  :178 only filters non-increasing values. :194-208 commits via `sendOffsetsToTransaction`.
* `channel/Coordinator.java:216` (`doCommit`) — the only caller of `commitConsumerOffsets()`, after
  all `commitToTable` calls succeed, for both full and partial (timeout) commits; also for an
  **empty** commit (empty `commitMap`), so a coordinator checkpoints its own StartCommit partition.
* `channel/Coordinator.java:97-102` — the coordinator's consumer group is
  `config.connectGroupId() + "-coord"` (persistent, shared by every coordinator instance).
* `channel/KafkaClientFactory.java:55` — `consumerProps.putIfAbsent(AUTO_OFFSET_RESET_CONFIG,
  "latest")`; used for the coordinator's consumer and the workers' transient consumers alike.
  Overridable only globally through `iceberg.kafka.auto.offset.reset`.

### How control partitions are chosen
* `channel/Channel.java:75,94` — each Channel instance has `producerId = UUID.randomUUID()` and
  keys every record with it, so each instance writes all its events to one control partition
  (`murmur2(key) % partitions`). A new `Worker` is created on every first non-empty `save` after a
  `close` (`CommitterImpl.java:190-221`, `startWorker`/`stopWorker`), a new `Coordinator` on each
  election (`CommitterImpl.java:223-231`), so after restarts/rebalances events land on fresh,
  random partitions.

### Worker answer = point of no return for source data
* `channel/Worker.java:94-111` — the answer to StartCommit (DataWritten per table + DataComplete) is
  sent in one Kafka transaction together with the source offsets
  (`Channel.send` :98-117, `sendOffsetsToTransaction(..., consumerGroupMetadata(context))`), i.e.
  the sink's `connect-<name>` group offsets advance past those records atomically with the
  announcement. `IcebergSinkTask.preCommit` returns no offsets (`IcebergSinkTask.java:100-104`).
  After this the records are never re-delivered; only the control-topic DataWritten carries them.

### Coordinator replacement
* `CommitterImpl.close` (`:161-187`) stops the coordinator when the task is stopped
  (`closedPartitions` empty → `stopCoordinator`) or loses the leader partition;
  `CoordinatorThread.terminate` → `Coordinator.terminate` (no commit, no checkpoint on stop).
  The buffered `commitState` is discarded. A new coordinator is created on the next election.
* The successor's consumer (same `-coord` group) starts each control partition at its committed
  offset if one exists, otherwise at `auto.offset.reset` = `latest` (read_committed: the LSO).

### Call chain (public entry points → cited code)
Connect runtime → `IcebergSinkTask.put/flush` → `CommitterImpl.save` → `Worker.process` →
`Channel.consumeAvailable` → `Worker.receive(StartCommit)` → `Channel.send(events, sourceOffsets)`.
Coordinator thread: `CoordinatorThread.run` → `Coordinator.process` → `consumeAvailable`
(`Channel.java:139`) → `receive` → `commit` → `doCommit` → `commitConsumerOffsets`
(`Coordinator.java:216` → `Channel.java:171-184`). Reachable in normal operation.

### Trigger scenario (constructed)
Preconditions: control topic with ≥ 2 partitions (auto-created topics get the broker's
`num.partitions`; the docs only show `--partitions 1` as an example for manual creation; the code
and tests support several, e.g. `TestCoordinator.testCommitConsumerMixedPartitionsRewindOrAdvance`).
1. Coordinator C1 runs and completes at least one commit (possibly empty) → `-coord` group has
   committed offsets for the partitions C1 has read, e.g. only its own StartCommit partition `pc`.
2. A worker producer (new Worker instance after a restart/rebalance, or the first Worker created
   when data starts flowing) hashes to a partition `q` that no coordinator has ever checkpointed.
   It answers a StartCommit: DataWritten+DataComplete on `q`, source offsets committed atomically.
3. Before C1 finishes the round (it waits for every DataComplete, or for
   `commit.timeout-ms`, default 30 s), C1 is replaced (task restart/crash, rebalance moving the
   leader partition, connector restart).
4. Successor C2: `pc` resumes at its checkpoint, `q` has no committed offset → `latest` → the
   DataWritten on `q` is never read. Its records' source offsets are committed → never re-delivered
   → missing from the table (loss).
With 1 control partition the partition is checkpointed by C1's first commit (its own StartCommit),
so after the first commit this scenario cannot occur; it then reduces to the PR #18006 window.

### Safeguards encountered (to be proven/disproven in Phase 2)
* Kafka read_committed + the transactional answer: the DataWritten is visible iff the source
  offsets are committed (no partial state) — does not help the successor, which never reads it.
* `Channel.consumeAvailable` replay guard (:126-135) and the table-floor filter
  (`Coordinator.commitToTable` :270-285) only handle re-reads, not skipped records.
* No resend path: `Worker.receive` answers only new StartCommits with data written since the last
  answer (`SinkWriter.completeWrite`); the old DataWritten is not re-sent.
* An idle/slow task keeps the round open until `commit.timeout-ms` (wider window); with every task
  answering promptly the window is the time to collect all answers plus the table commit.

## Step 2 — Developer knowledge (verbatim, cited)

* `Coordinator.java:96` comment: "pass consumer group ID to which we commit low watermark offsets".
* `Channel.java:158-170` Javadoc of `commitConsumerOffsets`: "committing a partition's offset only
  when it advances past the last committed value" — says nothing about partitions not read.
* `Channel.java:127-128`: "A rebalance can rewind the consumer to the committed offset, which can lag
  the in-memory position. Skip already-processed records."
* PR #18006 (open, https://github.com/apache/iceberg/pull/18006, created 2026-09-08, marked stale
  2026-10-31, no reviews): "A coordinator that has not completed a commit has no control-topic
  checkpoint. If it consumes and buffers a file announcement, then is replaced, a replacement using
  `auto.offset.reset=latest` can skip that retained announcement." Change: default the
  coordinator consumer to `earliest`. Its new code comment: "The group only gets a committed offset
  once a commit succeeds, so a coordinator that starts before that must read from the beginning".
  Its docs text: "When a partition has no committed offset or its offset is out of range, the
  coordinator reads from the earliest retained offset." Its tests use a single control partition
  (`CTL_PARTITION = new TopicPartition(CTL_TOPIC_NAME, 0)`) and the pre-first-commit window only.
* PR #17376 and #17450 (closed, not merged): "Recovery reads from `earliest`. The coordinator's
  `-coord` consumer group now defaults to `auto.offset.reset=earliest`, so a fresh or expired group
  re-reads uncommitted control events instead of skipping to the log end and dropping their data."
* Issue #17551 (closed; fixed by PR #17552, merged 2026-09-08): data loss when a zombie re-commits an
  old `-coord` offset that falls out of retention; "Coordinator C Auto offset reset to latest, skips
  all control topic records up to tip, data loss occurs". Fixed at the commit site (never commit a
  lower offset), not by changing the reset policy.
* PR #17933 (merged 2026-09-08, closes #17340): `controlTopicOffsets` made a high-water mark;
  "`commitConsumerOffsets()` commits it for the consumer group, so the next restart resumes from the
  regressed offset" — about regression, not coverage.
* PR #18767 (open): worker-side `seekToLastCommittedOffsets` for source partitions without a
  committed offset — different consumer (the Connect source consumer) and site.
* PR #17593 (open): idle tasks never start a Worker, so commits complete only at the timeout.
* Docs (`docs/docs/kafka-connect.md:261-271`): control topic auto-created if
  `auto.create.topics.enable`; manual example `--partitions 1`; no single-partition requirement.
* Tests: `TestCoordinator.testCommitConsumerMixedPartitionsRewindOrAdvance` (2 control partitions,
  both already in `controlTopicOffsets`); `TestChannel.controlTopicOffsetsAreTrackedPerPartition`.
  No test covers a control partition absent from `controlTopicOffsets` at checkpoint time.
* Searches run (gh, apache/iceberg, issues+PRs, open/closed/merged): `commitConsumerOffsets`,
  `controlTopicOffsets`, `"-coord" consumer group`, `coordinator auto.offset.reset`,
  `control topic partitions coordinator offsets`, `coordinator checkpoint control topic partition`,
  `control topic multiple partitions kafka connect`, `earliest`, `coordinator`, plus all
  `"Kafka Connect"` PRs updated since 2026-08-01 (bodies of 17 coordinator-related PRs grepped for
  earliest/reset/checkpoint/partition coverage). Local Specula case-studies dir: no entries.

## Step 3 — Known-status / pre-filter

The harmful final step (successor's `-coord` consumer resets a checkpoint-less partition to
`latest`) has been reported before for other triggers: #18006 (no commit completed yet),
#17376/#17450 (fresh or expired group), #17551 (stale out-of-range offset; fixed separately at the
commit site by #17552). None reports this finding's trigger/site: after the coordinator **has**
completed commits, `commitConsumerOffsets` (Channel.java:171-184) still leaves every partition it
never read without an offset, so the window recurs for each control partition first used by a new
producer. #18006's stated precondition ("has not completed a commit") does not hold in this
scenario, and its comment states the opposite assumption. #17551 shows upstream treats a different
trigger of the same reset as its own bug with its own fix.

Determination: not an exact duplicate of an existing report → **not dropped**; proceed to Phase 2.
Novelty (for the verdict header): NEW, with the relation to #18006 recorded (merging #18006 as
proposed would remove this consequence; a fix of #18006 that only handles a group with no
committed offsets at all would not).
