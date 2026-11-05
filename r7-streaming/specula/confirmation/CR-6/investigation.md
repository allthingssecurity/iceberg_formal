# CR-6 — Phase 1 investigation (evidence only, no verdict)

Finding: "Coordinator thread hangs forever after terminate() when table commits are queued:
shutdownNow() drops tasks that Tasks.waitFor still blocks on". Source: code review (Scenario 4
residual; the model has no invariant and no counterexample for it).

Code under audit = pristine upstream `HEAD` 9fffeb63b1605a7c3acf218e58b7eeb43bdb1b01 (an ancestor of
apache/iceberg `main`, 24 commits behind it on 2026-11-05; none of those 24 commits touch
`kafka-connect/` or `core/.../util/Tasks.java`). The per-finding `worktree/` is instrumented with
TLA+ trace hooks (`TlaHooks`, 483-line diff on Coordinator.java), so every line number below refers
to `git show HEAD:<path>`, not to the instrumented working copy.

## Step 1 — Code audit

### Cited code (pristine HEAD)

| Site | What it does |
|---|---|
| `Coordinator.java:111-121` | `exec = new ThreadPoolExecutor(commitThreads, commitThreads, keepAlive, ms, new LinkedBlockingQueue<>(), daemon "iceberg-committer-%d")`. The queue is unbounded, so tasks beyond `commitThreads` wait in it. |
| `Coordinator.java:207-213` | `doCommit`: `Tasks.foreach(commitMap.entrySet()).executeWith(exec).stopOnFailure().run(entry -> commitToTable(...))`, which submits one task per table. |
| `Tasks.java:307-358` | `runParallel` calls `service.submit(Runnable)` for every item and keeps the returned `FutureTask`s. |
| `Tasks.java:361` / `506-513` | `waitFor(futures)` loops `for (Future f : futures) f.get();` with **no timeout**. It cancels futures only when the **waiting** thread is interrupted (`530-540`). |
| `Coordinator.java:303-306` | The `terminated` flag is checked inside `commitToTable` just before the snapshot commit. Only a task that actually *starts* sees it. |
| `Coordinator.java:434-447` | `terminate()`: `terminated = true; exec.shutdownNow();` (**return value discarded**), then `awaitTermination(1, MINUTES)`. |
| `CoordinatorThread.java:40-64` | `run()`: `while (!terminated) coordinator.process();` and then `coordinator.stop()` at **59-63**. |
| `CoordinatorThread.java:83-86` | `terminate()`: `terminated = true; coordinator.terminate();`. It does **not** interrupt the `iceberg-coord` thread and does not join it. |
| `CommitterImpl.java:240-245` | `stopCoordinator()`: `coordinatorThread.terminate(); coordinatorThread = null;`. |
| `Channel.java:228-240` | `start()` → `consumer.subscribe(controlTopic)` (group membership in `<connect-group>-coord`). `stop()` → `producer.close(); consumer.close(); admin.close();`. |

JDK semantics relied on: `ThreadPoolExecutor.shutdownNow()` interrupts the workers, removes the
not-yet-started tasks from the queue (`drainQueue()`) and *returns* them. It does not cancel them, so
their `FutureTask`s stay in state NEW and `get()` without a timeout never returns. Phase 2 verifies
this on the local JDK.

### Call chain (all reachable during normal operation)

Coordinator side (thread `iceberg-coord`):
`CoordinatorThread.run` → `Coordinator.process` → `Channel.consumeAvailable` → `Coordinator.receive(DATA_COMPLETE)`
→ `commit(false)` (or `process` → `isCommitTimedOut` → `commit(true)`) → `doCommit` → `Tasks...run`
→ `runParallel` → `waitFor` → `FutureTask.get()`.

Task side (Connect task thread), concurrently:
- rebalance: `WorkerSinkTask` → `IcebergSinkTask.close(partitions)` → `CommitterImpl.close` → `hasLeaderPartition` → `stopCoordinator` → `CoordinatorThread.terminate` → `Coordinator.terminate` → `exec.shutdownNow()`;
- stop / delete / restart: `IcebergSinkTask.stop` → `close()` → `committer.close(List.of())` → `stopCoordinator` → same.

### Trigger scenario

1. The connector writes to N tables (multi-table / dynamic routing) and N exceeds
   `iceberg.control.commit.threads` (default `2 × availableProcessors`; in containers with CPU
   limits this is often 2-4).
2. A commit cycle completes. `doCommit` submits N `commitToTable` tasks: the first T start and the
   remaining N−T sit in the `LinkedBlockingQueue`. `iceberg-coord` blocks in `future[0].get()`.
3. While a running table commit is in flight (catalog round trips plus manifest writes take hundreds
   of ms to seconds), the task closes the leader partition (rebalance) or the task is stopped.
4. `terminate()` → `shutdownNow()` interrupts the running commit(s) and silently drains the N−T
   queued tasks. The running futures complete (normally or exceptionally) and the pool terminates,
   so `awaitTermination` returns true and `close()`/`stop()` returns normally.
5. `iceberg-coord` moves on to `future[T].get()`, a drained future that never completes, and blocks
   forever. `CoordinatorThread.run` never reaches `coordinator.stop()` (59-63): the producer,
   consumer and admin are never closed, and the thread (non-daemon, plain `Thread`) leaks.
6. Successor: a new coordinator (same or another task) subscribes to the same `<group>-coord` group.
   The zombie consumer is still a member, kept alive by its heartbeat thread until
   `max.poll.interval.ms` (default 300 s; KafkaClientFactory does not override it) expires after its
   last `poll`. Until then the successor's consumer gets no assignment, so it sees no
   DataWritten/DataComplete and the commits stall. After that the member leaves the group, but the
   thread and the three Kafka clients stay leaked for the life of the worker JVM. Every recurrence
   adds another.

### Safeguards encountered (recorded, not dismissed)

- `terminated` check at `Coordinator.java:303`: it only protects tasks that *run*. Drained tasks never run.
- `waitFor`'s cancel-on-interrupt (`Tasks.java:530-540`): it fires only if `iceberg-coord` itself is
  interrupted, and nothing interrupts it (`CoordinatorThread.terminate` does not, and neither does
  Connect, which only interrupts or abandons its own task thread).
- `awaitTermination(1 min)` waits for the *pool*, not for the coordinator thread, and returns
  normally once the running commit ends.
- Producer fencing (stable `coordinatorTransactionalId`, #18039): a successor's
  `initTransactions()` fences the zombie producer. That limits writes, not the leak or the group
  membership.
- Consumer heartbeat thread: after `max.poll.interval.ms` it sends LeaveGroup, which bounds the
  successor delay. It does not stop the thread or client leak.

## Step 2 — Developer-knowledge search (verbatim evidence)

Local history: the checkout is a shallow single-commit clone, so `git blame` only shows `^9fffeb6`.
Upstream history comes from the GitHub API (`gh api repos/apache/iceberg/commits?path=...`).

- **PR #10814** (merged 2024-09-10, bryanck), "Kafka Connect: Stop commits on terminated
  coordinator", introduced `terminated` + `shutdownNow` + `awaitTermination`: "When a rebalance is
  triggered and `close()` is called on the sink task, here we immediately flag the coordinator as
  terminated, shut down the commit executor, and await termination from the main sink task thread
  ... This doesn't solve all edge cases but should be an improvement to fencing out zombie
  coordinators." Review thread (singhpk234 asked whether `Channel#stop()` is still called), bryanck:
  "**We still call it to clean up the Kafka consumer and producer. The consumer isn't thread safe so
  I left that call on the Coordinator thread**, rather than the main task thread." So the developers
  intend that, after `terminate()`, the coordinator thread runs `stop()` and closes its Kafka
  clients.
- **Existing test** `TestCoordinatorThread.testRun` (src/test/.../TestCoordinatorThread.java:43-46):
  `coordinatorThread.terminate(); verify(coordinator, timeout(1000)).stop(); assertThat(coordinatorThread.isTerminated()).isTrue();`.
  The developers assert that `stop()` runs within 1 s of `terminate()`. The test uses a mocked
  Coordinator, so it never exercises a real mid-commit `doCommit`.
- **PR #18540** (merged 2026-10-29) "Core: Block on unfinished futures in Tasks.waitFor instead of
  polling" changed `waitFor` from a `isDone()`+`sleep(10)` loop to a blocking `get()`. Both versions
  wait forever on a future that never completes, so the hang does not depend on #18540.
  **PR #18213** (merged 2026-09-24) "Do not retry Tasks after interrupt" added `causedByInterruption`.
- **Issue #17349** (OPEN, 2026-07-24) "Coord consumer group on iceberg control topic with phantom
  consumer after stopping a connector" (same *symptom class*: a leftover `-coord` member after
  stop). The reporter attributes it to `stopCoordinator()` not joining the thread and proposes
  `coordinatorThread.join(30000)` (PR #17350, open). Comment by whitepowder (2026-09-28): "Even after
  deleting the connector, the -coord consumer group remains active, which prevents us from adjusting
  its offsets". The report does not mention queued commit tasks, `shutdownNow` or `Tasks.waitFor`.
- **Issue #16016** (OPEN) "zombie coordinator thread when writing to S3 fails". The comments name two
  leak paths: "terminate() doesnt join" and "close() skips stopCoordinator() ... during a rebalance
  (hasLeaderPartition false)". Neither is the drained-future mechanism. **PR #16020** (closed, unmerged)
  proposed a join.
- **PR #16156** (closed, stale) "Prevent zombie coordinator during rebalance" proposed `join` after
  `terminate` and noted that the old executor thread "may still be inside commitToTable() executing
  an Iceberg commit (e.g. an HTTP call to Glue/HMS) that doesn't respond to interrupts" and "The old
  coordinator's Kafka consumer remains in the -coord consumer group". It assumes the thread exits
  and does not identify drained queued futures. **PR #16843** (closed) "make sink cleanup robust",
  **#17376** and **#17450** (closed) all add a bounded join ("possible resource leak" log). None was merged.
- **Issue #15452** (closed as stale) "Thread hanging/unclean consumer group after broker restart" is a
  generic report with no mechanism. **PR #15569** (closed) attributes it to `Channel.stop()` close
  without timeouts and to swallowed fatal producer errors, which is a different mechanism.
- No TODO/FIXME or design note near `terminate()`/`doCommit`.

## Step 3 — Known-status / precedent

Searches (issue tracker and PRs, open, closed and merged, including the last 2 months): `shutdownNow`,
`shutdownNow coordinator`, `waitFor`, `Tasks.waitFor`, `FutureTask`, `coordinator thread hang/leak/stuck`,
`iceberg-coord thread`, `awaitTermination coordinator`, `"Timed out waiting for coordinator shutdown"`,
`commit threads`, `iceberg.control.commit.threads`, `drained queued tasks`, `future never completes`,
`multiple tables coordinator hang`. I also scanned the patches of 99 "Kafka Connect" PRs updated since
2026-03-01 for added lines touching `shutdownNow|awaitTermination|terminate|waitFor|join(|cancel(|interrupt`
in Coordinator/CoordinatorThread/CommitterImpl/Tasks, read the hits (#16843, #18039, #17450, #17376,
#16434, #16156, #15207, #15569), and checked the main-branch commit history of all four cited files.

Result: **no issue, PR, CVE or advisory reports THIS mechanism** (shutdownNow draining queued per-table
commit futures that `Tasks.waitFor` still blocks on, so `iceberg-coord` never reaches `stop()`).
Related reports (#17349, #16016, #15452) describe the same *consequence class* (a leaked coordinator
thread or phantom `-coord` member) and attribute it to other mechanisms (no join, leader-gate skip,
close without timeout). Their proposed join-based fixes would *not* fix this mechanism: a join would
time out and still leak. Under the skill's rule, a matching shape or symptom with a different
mechanism is not KNOWN. Persistent-findings registry lookup (`specula findings lookup`): no records.

Pre-filter: code-review-sourced, not already reported → **not dropped; proceed to Phase 2.**
