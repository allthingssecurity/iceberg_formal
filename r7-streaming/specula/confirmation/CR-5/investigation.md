# CR-5 — Phase 1 investigation (evidence only, no verdict)

Finding: "A superseded coordinator is fenced only for its sends and checkpoints: it keeps consuming,
keeps -coord membership and can still commit to Iceberg". Source: code review (Scenario 4 residual;
MC_hunt_s4_zombie*.cfg / MC_hunt_s4_multitable_unknown.cfg returned **no violation**), so the source
is `Code Review`, not MC.

All line numbers refer to the pristine commit `9fffeb6` (`git show HEAD:<path>`); the worktree
carries the Phase-3 trace instrumentation as uncommitted changes, which the reproduction removes.

## Step 1 — Code audit

### Cited sites (pristine)

| Site | What the code does |
|---|---|
| `IcebergSinkConfig.java:463-465` | `coordinatorTransactionalId() = transactionalPrefix() + connectGroupId() + "-coordinator"` — stable per connector (added by PR #18039, merged 2026-10-02). |
| `KafkaClientFactory.java:43-51` | `createProducer(txnId)` calls `initTransactions()` → a new coordinator bumps the epoch and fences the old producer. |
| `Channel.java:78-118` (`send`) | every coordinator send (StartCommit / CommitToTable / CommitComplete) is its own transaction; a fenced producer fails at `commitTransaction()` (ProducerFencedException). |
| `Channel.java:158-213` (`commitConsumerOffsets`) | `-coord` checkpoint = separate transaction (fenced). Javadoc lines 165-169: *"This transaction covers only the consumer offset commit, not the Iceberg table snapshot commit … a stale coordinator can still land a snapshot in the window between the new coordinator's initTransactions() and its own fenced offset commit; that case is guarded separately at the Iceberg level by the SnapshotAncestryValidator offset validator, not by epoch fencing."* |
| `Channel.java:122-152` (`consumeAvailable`) | consumer is not transactional → a fenced coordinator keeps polling; in-memory high-water mark `controlTopicOffsets` (merge `Long::max`) and skip of already-consumed records. |
| `Coordinator.java:97-102` | consumer group `connectGroupId + "-coord"` (shared by every coordinator instance, `auto.offset.reset=latest`, read_committed). |
| `CoordinatorThread.java:40-64, 83-86` | loop `while (!terminated) coordinator.process()`; any exception ends the loop; `terminate()` sets flags + `Coordinator.terminate()` (exec `shutdownNow` + await 1 min); the thread is never joined. |
| `CommitterImpl.java:160-187` | `close()` stops the coordinator only if `hasLeaderPartition(closedPartitions)` — a live Admin describe of the Connect group. |
| `CommitterImpl.java:223-231` | `startCoordinator()` guard `coordinatorThread == null` is per task instance. |
| `Coordinator.java:203-231` (`doCommit`) | per-table `commitToTable` on `exec`, then `commitConsumerOffsets()`, `clearResponses()`, `send(CommitComplete)`. |
| `Coordinator.java:242-359` (`commitToTable`) | loads table; `committedOffsets = lastCommittedOffsetsForTable` (floor, line 270); `mergedOffsets = max(floor, controlTopicOffsets)`; **floor filter** `envelope.offset() >= floor[partition]` (277-285); `terminated` checked once (303-306); `appendOp/deltaOp.validateWith(offsetValidator(floor))` + `commit()` (313-341); **then** `send(CommitToTable)` (343-349). |
| `Coordinator.java:361-381` (`offsetValidator`) | `SnapshotAncestryValidator`: `expectedOffsets.equals(lastCommittedOffsets(baseSnapshots))`, message "stale offsets". |
| `core/.../SnapshotProducer.java:296-389, 486-498` | validator runs in `apply()` after `refresh()` on every commit attempt; `ValidationException` is not retried (only `CommitFailedException`). → floor read and CAS are linearised per table. |
| `Coordinator.java:159-201` (`commit`) | partial (timeout) path swallows any RuntimeException (incl. a fenced CommitToTable send, CR-12); full path rethrows everything except Iceberg `CommitFailedException`. |

### Call chain / reachability

`WorkerSinkTask` (Connect runtime) → `IcebergSinkTask.open/close/put` → `CommitterImpl.open/close/save`
→ `startCoordinator()` → `new Coordinator(...)` (producer `initTransactions()` = fencing point) →
`CoordinatorThread.run()` → `Coordinator.process()` → `consumeAvailable` → `receive(DATA_COMPLETE)` →
`commit(false)` or `isCommitTimedOut()` → `commit(true)` → `doCommit` → `commitToTable` → catalog CAS.

Two live coordinators of one connector are reachable through normal operations:

1. **Close-time describe misses** (upstream #17637, open; also #16016 comments): under an eager
   rebalance the closing task describes the Connect group while it is `PreparingRebalance`; members
   carry no assignment, `findFirstTopicPartition()` returns null, `hasLeaderPartition()` is false and
   `stopCoordinator()` is skipped (`CommitterImpl.java:99-104,179-182`). The task that next opens the
   first partition starts a second coordinator, whose `initTransactions()` fences the first.
2. **Coordinator host kicked out** (poll interval exceeded) while its coordinator thread keeps
   running; the other task is elected; the old one stops only when its host's `close(lost)` runs.
3. Task failure path: `terminate()` does not join (#16016).

What the superseded coordinator can still do until its next transactional operation:
consume the control topic (non-transactional consumer), stay in the `-coord` group (it is polled every
≤1 s, so it keeps or wins control partitions in `-coord` rebalances), buffer DataWritten, read floors
and run the Iceberg CAS (`appendOp.commit()` happens *before* the fenced `send(CommitToTable)`).
Its next send fails: StartCommit (next interval), CommitToTable (after a landed CAS),
the checkpoint or CommitComplete. On the partial path the CommitToTable failure is swallowed (CR-12),
so it survives until its next StartCommit (≤ one commit interval).

### Trigger scenario (constructed)

Connect cluster, 2 sink tasks, source topic with 2 partitions, 1-partition control topic (documented
default, `docs/docs/kafka-connect.md:261-272`), eager (default Range) assignment:

1. Task 0 (owner of `src-0`) is restarted via REST → task 1 is elected and starts coordinator **B1**.
2. Task 0 rejoins → eager rebalance → task 1's `close(all)` runs during `PreparingRebalance`
   → "found no partitions assigned across all members" → B1 is **not** stopped.
3. Task 0 gets `src-0` → `open` → starts coordinator **C2** → `initTransactions()` fences B1.
4. B1 and C2 are both live and both members of `connect-<connector>-coord`; whichever is assigned the
   control partition consumes the workers' DataWritten/DataComplete (also the answers to C2's StartCommits).
5. If B1 has a commit in progress, it times out, commits its buffer to the table (CAS lands — not fenced),
   then its CommitToTable send fails (fenced, swallowed on the partial path); B1 dies at its next StartCommit.
6. C2 (once it owns the control partition) re-reads from the last `-coord` checkpoint (B1's last
   pre-fence one), buffers the same DataWritten and commits.

Variant (zombie CAS racing the successor's CAS): a slow catalog commit in B1 (between floor read and
CAS) while C2 commits the same DataWritten.

### Safeguards encountered (recorded, not yet proven)

* Per-table **floor filter** (`Coordinator.java:270-285`): envelopes below the table-stored control
  offsets are dropped; stored offsets are the committing coordinator's high-water marks (merged with `max`).
* Per-table **SnapshotAncestryValidator** (`Coordinator.java:314/328, 361-381`; `SnapshotProducer.java:386-388`):
  a CAS whose floor read is stale fails with `ValidationException` ("stale offsets").
* In-memory high-water mark / skip (`Channel.java:126-139`, PR #17933).
* Fencing of the checkpoint (`Channel.java:171-213`) — the zombie cannot move the `-coord` offsets.

## Step 2 — Developer-knowledge search (verbatim evidence)

* **PR #18039** (merged 2026-10-02, "use a stable coordinator transactional id to fence stale
  coordinators", fixes #18038): *"This can prevent stale ("zombie") coordinators from committing stale
  consumer offsets … A stale coordinator will identify that it has been fenced, and exit the
  coordinator thread."* Review comment (laskoviymishka, Channel.java:149): *"it's the offset commit
  that's fenced — the table snapshot commit in Coordinator.doCommit() runs outside any Kafka
  transaction and still relies on SnapshotAncestryValidator for the stale-coordinator case. A stale
  coordinator can still land a stale snapshot in the window between the new coordinator's
  initTransactions() and its own fenced offset commit; that's caught at the Iceberg level, not by
  epoch fencing. I'd make that boundary explicit here"* — author: *"Agree with that, updated"* (→ the
  javadoc at `Channel.java:165-169`). Another review comment (IcebergSinkConfig.java:456): *"Snapshot
  validation still protects the data, but the fencing guarantee has a gap exactly during the transition."*
* **#17637** (open, 2026-08-13): "duplicate coordinator after rebalance because close() re-derives
  leadership from an unstable consumer group" — reports the two-coordinator *trigger* (close-time
  describe); asserts "an orphaned coordinator corrupts data" without a data-level demonstration.
* **#16016** (open): zombie coordinator thread (terminate() does not join; close() gate). Comment
  (2026-07-13, 1.11.0, before #18039): the leaked coordinator "stays in the `<connect-group>-coord`
  consumer group … periodically commits its frozen in-memory offsets back to the group".
* **#17551** (closed 2026-09-08): data loss from persistent zombie coordinators rewinding the `-coord`
  offsets (fixed by #17552 / #18039 fencing).
* **#16282** (open) / **#17340** (closed): duplicate file registration during coordinator recovery —
  attributed to the control-topic watermark regressing on re-read, fixed by **#17933** (merged
  2026-09-08, in this tree: `Channel.java:126-139`).
* **#17077** (open): `CommitState.validThroughTs()` includes DataComplete of other commit ids
  ("zombie coordinators") — a different site (`CommitState.java:146`).
* Tests: `TestCoordinator.testCoordinatorCommittedOffsetValidation` (*"prevents commits when another
  independent commit has updated the offsets during the commit process"*, expects
  `ValidationException` "stale offsets"); `TestCoordinator.testFencedCoordinatorCannotCommitOffsets`;
  `TestCommitterImpl.testFencedCoordinatorIsClearedWithoutFailingTask`; integration
  `TestCoordinatorFencing` (offset fencing only). No test covers a fenced coordinator's table commit.
* Readers of the zombie-affected labels: `kafka.connect.commit-id` / `kafka.connect.task-id` are only
  asserted present in `IntegrationTestBase.java:133-134`; nothing in the sink consumes
  `CommitToTable` / `CommitComplete` (Worker handles only START_COMMIT, `Worker.java:68-72`).

## Step 3 — Known-status / precedent

Searched (gh, 2026-11-05): `zombie coordinator`, `stale coordinator`, `SnapshotAncestryValidator`,
`coordinator fenced`, `two coordinators`, `duplicate coordinator`, `multiple coordinators` (issues and
PRs, open+closed, incl. recently merged #18039/#17933/#17713/#17552).

* The two-coordinator **trigger** is reported (#17637, #16016) — a different aspect (when a
  coordinator is not stopped), not this finding's mechanism.
* The fact that a fenced coordinator can still land a table snapshot is **acknowledged as accepted
  design** in PR #18039's review and javadoc, not filed as a defect. Developer awareness ≠ a report.
* No issue/PR reports that a fenced coordinator's consumption / `-coord` membership / table commit
  skips, repeats or misattributes a commit.

→ Not "code-review × already reported". **No pre-filter drop; proceed to Phase 2.**
Novelty if a defect is confirmed: NEW for this mechanism (trigger known: #17637, #16016).
