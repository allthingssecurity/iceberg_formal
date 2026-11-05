# CR-6: Coordinator thread hangs forever after terminate() when table commits are queued

- **Source**: Code Review
- **Novelty**: NEW
- **Status**: REPRODUCED
- **Severity**: Medium
- **Location**: kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/channel/Coordinator.java:437
- **Reproduction test**: `repro/test_bugCR-6_coordinator_hang_after_terminate.sh` (escalation **Level 0**)

Source note: code-review finding (Scenario 4 residual). No model-checking counterexample exists.
Novelty note: the issue tracker and PRs (open, closed and merged, including recent merges) were
searched; no report describes this mechanism. Related reports describe the same *symptom class* with
other mechanisms: #17349 (phantom `-coord` consumer after stop, attributed to the missing `join`),
#16016 (zombie coordinator thread, attributed to the missing `join` or the leader-gate skip), and the
unmerged join-based PRs #16156, #16020, #16843, #17350, #17376 and #17450. A join would not release a
thread parked on a drained future. Details are in `investigation.md`, Step 2 and Step 3.

## Description

`Coordinator.doCommit` (Coordinator.java:207-213) submits one `commitToTable` task per table to a
fixed pool of `iceberg.control.commit.threads` threads with an unbounded queue (Coordinator.java:111-121).
The coordinator thread then waits in `Tasks.waitFor`, which calls `future.get()` with no timeout
(Tasks.java:506-513). `Coordinator.terminate()` calls `exec.shutdownNow()` and discards the returned
list (Coordinator.java:437). The JDK drains the queued tasks without completing or cancelling their
`FutureTask`s. When a cycle has more tables than commit threads and the task closes its partitions or
stops mid-commit, the coordinator thread therefore parks forever on a drained future.
`CoordinatorThread.run` never reaches `coordinator.stop()` (CoordinatorThread.java:59-63), so the
non-daemon `iceberg-coord` thread and its producer, consumer and admin clients leak. The stale `-coord`
consumer also stays in the group until `max.poll.interval.ms` (300 s by default) expires, and until
then the successor coordinator receives no control-topic partition and commits nothing.

## Trigger scenario

1. The connector writes to more tables than `iceberg.control.commit.threads`. The default is
   `2 × availableProcessors` (24 on the 12-core test host). Multi-table or dynamic routing makes this
   common, and containers with CPU limits make the default small.
2. A commit cycle enters `doCommit`: T table commits run and N−T wait in the pool queue.
   `iceberg-coord` blocks in `Tasks.waitFor → FutureTask.get()`.
3. The leader task stops or loses its partitions: task restart, stop or delete, or a rebalance that
   revokes the leader partition. The path is `WorkerSinkTask` → `IcebergSinkTask.close(partitions)` →
   `CommitterImpl.close` ("lost leader partition. Stopping coordinator.", CommitterImpl.java:179-182) →
   `stopCoordinator()` → `CoordinatorThread.terminate()` → `Coordinator.terminate()` → `shutdownNow()`.
   `IcebergSinkTask.stop()` → `close([])` (CommitterImpl.java:172-175) reaches the same call.
4. The running commits are interrupted and complete, the pool terminates, and `awaitTermination`
   returns true, so `close()`/`stop()` return normally. The queued tasks are drained. `iceberg-coord`
   reaches the first drained future and parks forever.
5. The successor coordinator, on the same or another task, joins `<group>-coord`. The zombie member
   is kept alive by its heartbeat thread until `max.poll.interval.ms` and blocks the rebalance, so the
   successor has no assignment and commits 0 tables per cycle for about 5 minutes. After the poll
   timeout the zombie leaves the group and commits resume. The thread and its three Kafka clients stay
   leaked for the life of the worker JVM, and each recurrence adds another.

## Developer intent

- PR #10814 (bryanck, who introduced `terminated` + `shutdownNow` + `awaitTermination`), in review:
  "We still call it [Channel#stop()] **to clean up the Kafka consumer and producer. The consumer isn't
  thread safe so I left that call on the Coordinator thread**". After `terminate()`, the coordinator
  thread is meant to reach `stop()`.
- Existing test `TestCoordinatorThread.testRun` (src/test/.../TestCoordinatorThread.java:43-46):
  `coordinatorThread.terminate(); verify(coordinator, timeout(1000)).stop();`. The contract is that
  `stop()` runs within 1 s of `terminate()`. The test uses a mocked Coordinator, so a real mid-commit
  `doCommit` is never exercised.
- Users observe the consequence class: #17349 (OPEN) reports a phantom `-coord` consumer after stopping
  a connector; a commenter wrote "Even after deleting the connector, the -coord consumer group remains
  active, which prevents us from adjusting its offsets". No comment, TODO or test treats an orphaned
  coordinator thread as acceptable.

## Reproduction result

**Level reached: 0 (black-box).** The system under test is pristine `HEAD` 9fffeb6, copied with
`git archive` and byte-compared with git for all cited files. It runs in one JVM: an embedded Kafka
3.9.2 (KRaft) broker, one Kafka Connect worker with the real `IcebergSinkConnector` (`tasks.max=1`),
and a shared in-memory Iceberg catalog that only *counts* commits. The connector writes every record
to 60 tables. Only public interfaces are used: connector config and a task restart through the Connect
REST API, and records produced to the source topic. There are no failpoints, delays or code changes.
The restart is timed by passive observation, the same way an operator would read jstack or a catalog
request log: it fires when `iceberg-coord` is inside `Coordinator.doCommit → Tasks.waitFor` and at
least one cycle-2 table commit has landed.

Command (run in chunks so each fits the tool timeout; the default runs all of them):
```
cd .specula-output/repro
SCENARIOS="build control" bash test_bugCR-6_coordinator_hang_after_terminate.sh
SCENARIOS="default"       bash test_bugCR-6_coordinator_hang_after_terminate.sh
SCENARIOS="queued summary" bash test_bugCR-6_coordinator_hang_after_terminate.sh
```

Output of the final `queued` run (`repro/CR-6.queued.log`; samples between t+20 s and t+296 s are
identical and omitted):
```
CR6 [queued +   3.09s] setup: 60 tables, iceberg.control.commit.threads=2 (connector default here = 2 x 12 = 24), Kafka consumer max.poll.interval.ms = default (300000)
CR6 [queued +  10.77s] cycle 1 committed normally: all 60 tables contain 5 rows (connector commits landed so far: 60)
CR6 [queued +  10.87s] -coord consumer group 'connect-iceberg-sink-coord' members before restart: [new[ctl-0]]
CR6 [queued +  12.22s] coordinator observed inside Coordinator.doCommit -> Tasks.waitFor (cycle-2 table commits landed so far: 2 of 60); requesting REST restart of task 0
CR6 [queued +  12.40s] restart returned after 0.18s; cycle-2 table commits landed by the old coordinator: 6 of 60
CR6 [queued +  12.62s] t+   0.2s | old iceberg-coord alive=true state=WAITING inTasks.waitFor=true | iceberg-coord threads=1, iceberg-committer threads=0 | -coord members=[OLD[ctl-0]] | tables with all 10 rows: 6/60
CR6 [queued +  14.84s] t+   2.4s | old iceberg-coord alive=true state=WAITING inTasks.waitFor=true | iceberg-coord threads=2, iceberg-committer threads=0 | -coord members=[OLD[],new[]] | tables with all 10 rows: 6/60
CR6 [queued + 308.80s] t+ 296.4s | old iceberg-coord alive=true state=WAITING inTasks.waitFor=true | iceberg-coord threads=2, iceberg-committer threads=0 | -coord members=[OLD[],new[]] | tables with all 10 rows: 6/60
CR6 [queued + 319.44s] t+ 306.6s | old iceberg-coord alive=true state=WAITING inTasks.waitFor=true | iceberg-coord threads=2, iceberg-committer threads=2 | -coord members=[new[ctl-0]] | tables with all 10 rows: 36/60
CR6 [queued + 330.43s] t+ 317.2s | old iceberg-coord alive=true state=WAITING inTasks.waitFor=true | iceberg-coord threads=2, iceberg-committer threads=2 | -coord members=[new[ctl-0]] | tables with all 10 rows: 60/60
CR6 [queued + 330.43s] old coordinator thread at end: alive=true state=WAITING daemon=false, top frames:
        at java.base/jdk.internal.misc.Unsafe.park(Native Method)
        at java.base/java.util.concurrent.locks.LockSupport.park(LockSupport.java:221)
        at java.base/java.util.concurrent.FutureTask.awaitDone(FutureTask.java:500)
        at java.base/java.util.concurrent.FutureTask.get(FutureTask.java:190)
        at org.apache.iceberg.util.Tasks.waitFor(Tasks.java:513)
        at org.apache.iceberg.util.Tasks$Builder.runParallel(Tasks.java:361)
        at org.apache.iceberg.util.Tasks$Builder.run(Tasks.java:204)
        at org.apache.iceberg.util.Tasks$Builder.run(Tasks.java:199)
        at org.apache.iceberg.connect.channel.Coordinator.doCommit(Coordinator.java:210)
        at org.apache.iceberg.connect.channel.Coordinator.commit(Coordinator.java:161)
        at org.apache.iceberg.connect.channel.Coordinator.receive(Coordinator.java:152)
        at org.apache.iceberg.connect.channel.Channel.consumeAvailable(Channel.java:145)
        at org.apache.iceberg.connect.channel.Coordinator.process(Coordinator.java:136)
        at org.apache.iceberg.connect.channel.CoordinatorThread.run(CoordinatorThread.java:51)
CR6 [queued + 330.43s] old -coord consumer client.id=451e82b2-37a2-44d6-8e60-030f621fe793 still open (JMX kafka.consumer app-info registered) = true
CR6 [queued + 330.43s] client threads alive: kafka-coordinator-heartbeat-thread|connect-iceberg-sink-coord=2, kafka-producer-network-thread=7, kafka-admin-client-thread=4
```
Sink and Kafka log lines from the same run:
```
04:47:42.039 [task-thread-iceberg-sink-0] INFO ...CommitterImpl - Committer iceberg-sink-0 lost leader partition. Stopping coordinator.
04:47:42.040 [task-thread-iceberg-sink-0] INFO ...CommitterImpl - Committer iceberg-sink-0 stopped. Closing coordinator.
04:47:42.201 [task-thread-iceberg-sink-0] INFO ...CommitterImpl - Task iceberg-sink-0 elected leader, starting commit coordinator
04:52:42.001 [kafka-coordinator-heartbeat-thread | connect-iceberg-sink-coord] WARN ...ConsumerCoordinator - [Consumer clientId=451e82b2-37a2-44d6-8e60-030f621fe793, groupId=connect-iceberg-sink-coord] consumer poll timeout has expired. ...
successor "completed commit ..., committed to 0 table(s)" during the stall: 9 times
```
Summary lines of all scenarios on the final script version:
```
CR6-SUMMARY scenario=queued tables=60 commitThreads=2 restartInsideDoCommit=true cycle2LandedByOldCoordinator=6/60 oldThreadExitedAfterSec=never oldThreadHungInTasksWaitFor=true oldMemberLeftGroupAfterSec=306.6 successorAssignedAfterSec=306.6 allDataCommittedAfterSec=317.2 oldConsumerStillOpen=true newCoordinatorStarted=true duplicates=false verdict=COORDINATOR_THREAD_HUNG
CR6-SUMMARY scenario=default tables=60 commitThreads=default(24) restartInsideDoCommit=true cycle2LandedByOldCoordinator=28/60 oldThreadExitedAfterSec=never oldThreadHungInTasksWaitFor=true oldMemberLeftGroupAfterSec=307.0 successorAssignedAfterSec=307.0 allDataCommittedAfterSec=307.0 oldConsumerStillOpen=true newCoordinatorStarted=true duplicates=false verdict=COORDINATOR_THREAD_HUNG
CR6-SUMMARY scenario=control tables=60 commitThreads=60 restartInsideDoCommit=true cycle2LandedByOldCoordinator=60/60 oldThreadExitedAfterSec=0.2 oldThreadHungInTasksWaitFor=false oldMemberLeftGroupAfterSec=0.2 successorAssignedAfterSec=3.2 allDataCommittedAfterSec=0.2 oldConsumerStillOpen=false newCoordinatorStarted=true duplicates=false verdict=COORDINATOR_THREAD_EXITED
RESULT: BUG REPRODUCED - with queued table commits the terminated coordinator thread stays
        blocked in Tasks.waitFor (never reaches coordinator.stop()); the control without
        queued commits exits normally.
```

Lines that show the bug:
- `old iceberg-coord alive=true state=WAITING inTasks.waitFor=true` holds for the whole run (317 s),
  with `iceberg-committer threads=0`. The old pool has terminated, so nothing can ever complete the
  futures the thread waits on. The stack ends in `FutureTask.get ← Tasks.waitFor(Tasks.java:513) ←
  Coordinator.doCommit`.
- `old -coord consumer ... still open = true` and 2 `-coord` heartbeat threads: `Channel.stop()` never
  ran, so the clients leaked.
- `-coord members=[OLD[],new[]]` until t+306.6 s. The successor got `ctl-0` only after Kafka's
  poll-timeout eviction at 04:52:42, exactly 300 s after the zombie's last poll. Meanwhile it committed
  0 tables 9 times.

Expected (correct) behaviour, shown by the control (60 threads, no queued tasks, the same restart
inside cycle-2 `doCommit`): the old thread exits in 0.2 s, its consumer is closed and leaves the group
in 0.2 s, and the successor is assigned in 3.2 s.

Repeatability: the hang reproduced in 3 of 3 runs whose trigger landed while table commits were
queued: `queued` run 1 (`CR-6.queued.run1.log`, 7/60 committed, member left at 306.9 s), `queued`
run 2, and `default` run 2. The first `default` and `control` runs (`*.run1-trigger-on-cycle1-tail.log`)
used an earlier trigger that fired on the tail of cycle 1's `doCommit`. By then all 60 tasks had been
dequeued, so nothing was drained and the thread exited, which is consistent with the mechanism. The
trigger was then tightened to require a landed cycle-2 commit, and both scenarios were rerun as above.
No data loss and no duplicates occurred: after the stall, all 60 tables held all 10 rows exactly once.

### Confirmation checklist

1. **Level 0 or 1 alone?** Yes. Level 0 used only public interfaces and normal operations (Connect REST
   config and task restart, producing records). The restart was timed by passive observation. Nothing
   in the system was injected, delayed or patched.
2. N/A (no Level 2 or 3).
3. **Real consumer/caller observing the wrong outcome:**
   - `CommitterImpl.stopCoordinator()` (CommitterImpl.java:240-245) relies on `terminate()` leading to
     `stop()`, but `CoordinatorThread.run` never reaches `coordinator.stop()` (CoordinatorThread.java:59-63).
   - The successor `Coordinator` started by `CommitterImpl.startCoordinator()` (CommitterImpl.java:223-231)
     gets no `ctl-0` assignment for 306.6 s and 307.0 s. Its `consumeAvailable` (Channel.java:122-152)
     sees nothing, and its commits land 0 tables, so the Iceberg tables get no commits for about 5 minutes.
4. **Permanent or resolved?** The hung thread and the unclosed producer, consumer and admin are
   **permanent**: still WAITING with the consumer still registered at the end of the run, and no
   executor thread is left to complete the futures. The successor stall ends when Kafka's
   `max.poll.interval.ms` eviction removes the zombie member. That bounds the stall but does not prevent
   it, and it does not undo the leak, which grows with each occurrence. This is a live consequence,
   not a masked one.

## Recommendation

1. In `Coordinator.terminate()`, cancel what `shutdownNow()` returns, so that no future is left that can
   never complete: `for (Runnable r : exec.shutdownNow()) { if (r instanceof Future) ((Future<?>) r).cancel(false); }`.
2. **This must be paired with a guard in `doCommit`**. `Tasks.waitFor` *ignores* `CancellationException`
   (Tasks.java:515-516). If the running table commits succeeded and the queued ones were cancelled,
   `run()` returns normally and `doCommit` would go on to `commitConsumerOffsets()` and `CommitComplete`
   (Coordinator.java:216-223) for tables that were never committed. A successor would then start past
   those `DataWritten` events, losing data. Add a check before `commitConsumerOffsets()`, such as
   `if (terminated) throw new ConnectException("Coordinator terminated during commit")`, or verify that
   every table task actually ran.
   An alternative is to interrupt the `iceberg-coord` thread from `CoordinatorThread.terminate()`.
   `waitFor` already cancels all futures and rethrows on interrupt. The interrupt flag would have to be
   cleared before `coordinator.stop()`, or the client `close()` calls may themselves be interrupted.
3. A bounded `join` in `stopCoordinator()` (as in #17350 and #16156) is useful but not sufficient on
   its own: with this bug the join just times out and the thread still leaks.
4. Add regression tests:
   - a unit test with `commitThreads=1` and two tables: block the first commit, call `terminate()`,
     assert that the thread exits and `stop()` runs;
   - this end-to-end scenario: restart during a commit with more tables than threads, and assert that
     the old `-coord` member leaves promptly.
