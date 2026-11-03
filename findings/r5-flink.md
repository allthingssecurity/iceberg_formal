# r5 Flink reproductions: E-45, E-4, E-33

Harness: `r5/flink/` (Gradle, JDK 21, `./gw` launcher copied from dst/).
Released code under test, all from Maven Central:
- `org.apache.iceberg:iceberg-flink-2.3:1.12.0` (the artifact id is `iceberg-flink-2.3`; 1.12.0 is the only version published for it), plus iceberg-core/api/data 1.12.0.
- Flink 2.3.0 (`flink-streaming-java`, `flink-runtime`, both also with the `tests` classifier for `OneInputStreamOperatorTestHarness`, `TwoInputStreamOperatorTestHarness`, `MockEnvironment`, `MockOperatorCoordinatorContext`, `EventReceivingTasks`), plus `flink-table-runtime` 2.3.0 and `hadoop-client-api/runtime` 3.4.2, which are needed only to load the classes.

The `iceberg/` checkout is `main` at 9fffeb6, not 1.12.0. I read line numbers from the 1.12.0 `-sources.jar`, and give the checkout lines next to them. For every function involved, the 1.12.0 code and the checkout code are the same, except where noted.

Reproduce everything (about 2 minutes):

```
cd /Users/I074560/Downloads/experiments/iceberg-verify/r5/flink && ./gw test -Dr5.runs=20
# results: r5/flink/build/r5/{e45,e4,e4-dynamic,e33}.txt (each < 3 KB)
# one candidate: ./gw test --tests '*E45*' -Dr5.runs=20   (or '*E4Max*', '*E4Dynamic*', '*E33*')
```

Fixtures (`src/test/java/org/apache/iceberg/r5/`):
- `InMemoryCatalog` and `InMemoryFileIO`, with Avro data files and real puffin DVs (`BaseDVFileWriter`).
- `FaultOps` wraps the real `TableOperations`. When armed, the next `commit()` is first applied by the delegate, so the metadata lands, and then the armed exception is thrown (a lost ack). It also wraps `FileIO` so that `deleteFile` can be made to fail. Every injection is counted, and every failed delete path is recorded.
- Faults are attributed by exception class and by exact path, never by message text.
- Logs are set to `error`, and stdout is kept out of the JUnit XML.
- All scenario names are enums, so an unknown name throws.

---

## E-45: EqualityConvertCommitter deletes live DVs on a non-cleanable lost-ack failure

**Status: CONFIRMED. Severity: Major (data corruption: the current snapshot references deleted DV files, and every scan of the affected data file fails).**
Decided by: Flink `TwoInputStreamOperatorTestHarness` driving the released `EqualityConvertCommitter`, with core `RowDelta` run against the same fault as calibration.

Source (1.12.0 sources jar / checkout):
- `flink/v2.3/flink/src/main/java/org/apache/iceberg/flink/maintenance/operator/EqualityConvertCommitter.java`, 1.12.0 lines 215-226 (checkout 224-234): `catch (CommitStateUnknownException e) { throw e; } catch (Exception e) { deleteUncommittedDVs(); throw e; }`. Then `deleteUncommittedDVs` at 271-290 (checkout 280-297).
- Core does the opposite: `core/src/main/java/org/apache/iceberg/SnapshotProducer.java:538-545` (checkout 540-541) cleans only `if (!strictCleanup || e instanceof CleanableFailure)`.
- A production source of this fault is `core/.../rest/HTTPClient.java:327-328` (checkout 358). When the response cannot be read (for example a socket read timeout after the POST was sent), the IOException is wrapped in `RESTException`. That exception is not a CSUE and not a CleanableFailure.
  - `SocketTimeoutException` is an `InterruptedIOException`, which `ExponentialHttpRequestRetryStrategy` does not retry (lines 103-122).
  - `RESTTableOperations.commit` maps only CSUE (lines 225-235).

Minimal sequence (staging branch == target branch == main, format v3):
1. Append data file D with ids 1, 2, 3.
2. Write a puffin DV deleting position 0 of D.
3. Feed the committer `DVWriteResult([dv], [])` and an `EqualityConvertPlan` with `stagingSnapshotId` and `mainSnapshotId = head`.
4. Arm the lost-ack fault, then send the done-timestamp watermark.
5. `RowDelta.commit()` lands the snapshot. The fault then throws, and the committer's `catch (Exception)` deletes the DV.

Results (`build/r5/e45.txt`, 20 runs per row):

| fault after commit landed | engine | injected | snapshot landed and references DV | DV file exists | read |
|---|---|---|---|---|---|
| none (control) | Flink committer | 0 | 20/20 | 20/20 | ids=[2,3] 20/20 |
| none (control) | core RowDelta | 0 | 20/20 | 20/20 | ids=[2,3] 20/20 |
| `RuntimeException` | Flink committer | 1 per run | 20/20 | **0/20** | **NotFoundException 20/20** |
| `RuntimeException` | core RowDelta | 1 per run | 20/20 | 20/20 | ids=[2,3] 20/20 |
| `RESTException(SocketTimeoutException)` | Flink committer | 1 per run | 20/20 | **0/20** | **NotFoundException 20/20** |
| `RESTException(SocketTimeoutException)` | core RowDelta | 1 per run | 20/20 | 20/20 | ids=[2,3] 20/20 |
| `RuntimeIOException` | Flink committer | 1 per run | 20/20 | **0/20** | **NotFoundException 20/20** |
| `RuntimeIOException` | core RowDelta | 1 per run | 20/20 | 20/20 | ids=[2,3] 20/20 |
| CSUE | Flink committer | 1 per run | 20/20 | 20/20 | ids=[2,3] 20/20 |
| CSUE | core RowDelta | 1 per run | 20/20 | 20/20 | ids=[2,3] 20/20 |

How faults were attributed:
- Flink committer: in every faulted run, exactly one injection and exactly one `ERROR_STREAM` side-output record whose exception class matches the injected class. The harness copies side-output records through a serializer, so object identity cannot be used there.
- core RowDelta: the injected instance is itself on the cause chain of the exception that reached the caller.

What the core contrast shows: under the identical fault, core keeps its files, which is the #10373 policy. The committer's own `catch` is therefore what deletes the DV. The CSUE row shows the committer's existing CSUE guard works; only the non-CSUE, non-cleanable branch is wrong.

Not established:
- I did not run a real REST server. The fault is injected in `TableOperations.commit` after the delegate commit, using the exception class `HTTPClient` would throw.
- Only in-place mode (staging == target) was tested, with no equality-delete files in the plan. The separate-target-branch mode and the "eq deletes removed in the same commit" variant were not run.
- I did not check whether a later maintenance cycle repairs the table. Reading the code, the planner resumes from `COMMITTED_STAGING_SNAPSHOT_PROPERTY`, which did land, so a repair looks unlikely; this was not run.
- 1.20 was not run (the inventory says the code is identical).

---

## E-4: max-committed-checkpoint lookup returns -1 after expiry, and the restored checkpoint is committed again

**Status: CONFIRMED. Severity: Major (duplicate commit, so duplicate rows).** Two conditions are needed:
- (i) the job's last Flink snapshot is expired while the job is down, after other writers committed on top of it;
- (ii) the restored checkpoint's temp manifest still exists. It always exists for `IcebergSink` with post-commit maintenance tasks (`tableMaintenanceEnabled`), because the committer never deletes it. For the v1 `IcebergFilesCommitter`, for the v2 sink without maintenance, and for `DynamicCommitter`, it exists only when the swallowed best-effort delete failed (E-9).

When the manifest was deleted (the default success path), the same trigger does not duplicate. Instead the restore fails with `NotFoundException` on every attempt from that checkpoint: a crash loop, which is availability, not data.

Decided by:
- v1: Flink `OneInputStreamOperatorTestHarness` driving the released `IcebergFilesCommitter`, through `snapshot` / `notifyOfCompletedCheckpoint` / `initializeState(snapshot)`, with the same `JobID` and `OperatorID`. This is the pattern of upstream `testRecoveryFromValidSnapshot` with an expiry added.
- v2 and Dynamic: direct calls to `IcebergCommitter.commit` / `DynamicCommitter.commit`, with a stub `CommitRequest`. The restore is modelled as a fresh committer instance receiving the checkpointed committable again, which is what `CommitterOperator` does for restored committables. The `CommitterOperator` itself was not run.

Source:
- `flink/v2.3/flink/src/main/java/org/apache/iceberg/flink/sink/SinkUtil.java:85-107`. The walk stops at line 103 when `table.snapshot(parentId)` is null.
- `IcebergFilesCommitter.java:191-202`: the restore path, `tailMap(max, false)`, then commit.
- `IcebergCommitter.java:141-152` (checkout 145-165). The checkout adds `isRestored`, but on a restore it still walks.
- `IcebergCommitter.java:195-197` (checkout ~210): with `tableMaintenanceEnabled`, it does not delete temp manifests. `IcebergSink.java:213` sets `maintenanceEnabled = !maintenanceTasks.isEmpty()`.
- `FlinkManifestUtil.java:158-187`: the swallowed delete.
- `dynamic/DynamicCommitter.java:150-185, 190-209`: the same walk, through `SnapshotUtil.ancestorsOf`. Its in-retry `MaxCommittedCheckpointIdValidator` (340-361) reuses that walk over the same broken ancestry, so it does not catch the duplicate.

Minimal sequence (format v2, main branch):
1. The job commits checkpoints 1..3, one data file (2 rows) each. The state is captured at `snapshot(3)`, before `notifyOfCompletedCheckpoint(3)` commits; the summary then says max-ckpt=3.
2. The job crashes before checkpoint 4 completes, so the latest completed checkpoint (3) still holds checkpoint 3's committable.
3. While the job is down, another writer appends twice. Then `expireSnapshots().expireOlderThan(now).retainLast(2)` runs; the job's last snapshot is measured as expired.
4. The job restores from checkpoint 3. The lookup measures -1, and checkpoint 3 is committed again.

Results (`build/r5/e4.txt`, `build/r5/e4-dynamic.txt`, 20 runs per row; 10 distinct rows expected):

| mode | committer | job snap expired | lookup | delete faults | temp manifest survived | restore | rows / distinct | duplicate file entries |
|---|---|---|---|---|---|---|---|---|
| control: no expire | v1, v2, v2+maint, Dynamic | no | 3 | 0 | (only v2+maint) | ok, v2/Dyn signal `alreadyCommitted` | 10/10 | 0 (20/20 each) |
| control: no expire + delete fault | v1, v2, Dynamic, v2+maint | no | 3 | 1 (v2+maint: 0) | yes | ok, `alreadyCommitted` | 10/10 | 0 (20/20 each) |
| expire | v1 | yes | -1 | 0 | no | **FAILED: NotFoundException** 20/20 | 10/10 | 0 |
| expire | v2 maint=false | yes | -1 | 0 | no | **FAILED: NotFoundException** 20/20 | 10/10 | 0 |
| expire | Dynamic | yes | — | 0 | no | **FAILED: NotFoundException** 20/20 | 10/10 | 0 |
| expire | **v2 maint=true** | yes | -1 | 0 | yes | ok (re-committed) | **12/10** 20/20 | **1** |
| expire + delete fault | **v1** | yes | -1 | 1 | yes | ok (re-committed) | **12/10** 20/20 | **1** |
| expire + delete fault | **v2 maint=false** | yes | -1 | 1 | yes | ok (re-committed) | **12/10** 20/20 | **1** |
| expire + delete fault | **Dynamic** | yes | — | 1 | yes | ok (re-committed) | **12/10** 20/20 | **1** |

How to read the table:
- "duplicate file entries" counts how many times checkpoint 3's data file path appears more than once in `planFiles()` of the head snapshot.
- The delete fault fails exactly the one `deleteFile` issued for checkpoint 3's temp manifest (1 injected per run), and that exact path is then measured as still existing.
- The controls rule out the harness: the same harness, with the same delete fault but no expiry, gives 0 duplicates in 20/20 runs.

DynamicCommitter question: it behaves the same as the v1 and v2 committers. It duplicates when the manifest survives and crash-loops with NotFound otherwise. Its in-retry validator does not prevent this.

Not established:
- How often production downtime exceeds the expiry age. The trigger needs the job's last snapshot to be expired while the job is down, which in turn needs other writers to commit on the branch and an expiry threshold shorter than the outage.
- The variant where the job's own post-commit maintenance (compaction plus expire) removes its last snapshot before a crash, with no outage, was not run.
- `CommitterOperator` / `IcebergSink` end-to-end restore was not run (the committer was driven directly).
- 1.20 was not run.

---

## E-33: BaseCoordinator static PENDING_RELEASE_EVENTS / LOCK_RELEASE_CONSUMERS

**Status: CONFIRMED (mechanism, at coordinator level). Severity: Minor.**
- (a) fails the job, which then restarts, and `resetToCheckpoint` clears the statics.
- (b) silently stops delivering lock releases to another job's TriggerManager, so that job's maintenance stalls.

Neither loses or duplicates a table commit.

Decided by: the released `TriggerManagerCoordinator` and `LockRemoverCoordinator`, driven only through their public `OperatorCoordinator` API (`start`, `executionAttemptReady`, `handleEventFromOperator`, `close`). It uses Flink's `MockOperatorCoordinatorContext` and `EventReceivingTasks`, and waits on the coordinator thread through `checkpointCoordinator`.

Source: `flink/v2.3/flink/src/main/java/org/apache/iceberg/flink/maintenance/operator/BaseCoordinator.java`:
- 47-49: the statics.
- 86-90: `PENDING.forEach(this::handleReleaseLock)`.
- 103: `PENDING.add` inside that forEach.
- 122-125 and 142-145: global clears.
- 209: `context.failJob`.

The 1.12.0 code and the checkout code are identical.

Results (`build/r5/e33.txt`, 20 runs each):

| scenario | outcome |
|---|---|
| (a) release for table B arrives before B registers, then table A registers (same classloader) | job failed with `ConcurrentModificationException` 20/20. PENDING is left with 2 copies of B's event, because `clear()` is skipped |
| control: one table, release arrives before its own registration | no failure, release delivered 20/20 |
| (b) Y registered; another job's LockRemoverCoordinator `close()`s; then Y releases | release delivered to Y's TriggerManager 0/20, left in PENDING 20/20, no failure reported |
| control: no other close | delivered 1/1 in 20/20 |

Not established:
- Whether real operator start-up ordering produces (a) (a release for one table before that table's registration, followed by another table's registration) in an actual multi-table job or session cluster.
- The downstream effects. The permanent `lockTime` stall in TriggerManagerOperator, and the duplicate-release `checkArgument` (E-36) from the two copies of B's pending event, were not run.

---

## Summary

| id | status | severity | one line |
|---|---|---|---|
| E-45 | confirmed | Major | A non-CSUE, non-cleanable lost-ack (`RESTException` read timeout, `RuntimeIOException`, plain RTE) makes EqualityConvertCommitter delete the DV that the landed snapshot references, so reads fail with NotFound in 20/20 runs. Core RowDelta under the same fault keeps the file (0/20). |
| E-4 | confirmed | Major | After expiry removes the job's last snapshot, the lookup returns -1 and the restored checkpoint is committed again. The result is duplicate rows (12 vs 10, 20/20) when the temp manifest survives: always for IcebergSink with maintenance, otherwise after a swallowed delete failure. With the manifest gone, restore crash-loops on NotFound. Controls give 0 duplicates. DynamicCommitter behaves the same. |
| E-33 | confirmed (mechanism) | Minor | A cross-table pending release makes `registerLock` throw ConcurrentModificationException, then failJob (20/20). Another job's `close()` wipes consumers so releases are never delivered (0/20 delivered). No lost or duplicate table commit. |
