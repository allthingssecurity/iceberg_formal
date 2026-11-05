# CR-12 — Phase 1 investigation (evidence only)

Finding: "The partial-commit path swallows every exception with no retry budget, so a poison
DataWritten is retried forever and a fenced coordinator keeps looping."
Source: code review. Model checking under `MC_hunt_s4_zombie.cfg` found **no violation**, so this is
**not** MC-sourced. Base commit: `apache/iceberg @ 9fffeb6` (2026-11-02), pristine
`HEAD:kafka-connect/...` (the worktree's sink sources carry the trace instrumentation; every line
number below is from the pristine `git show HEAD:` text).

Persistent-findings lookup (`specula findings lookup` with partialCommitFailures / commit(true) /
partial commit retry / Coordinator.commit) returned `[]`. No earlier record exists.

## Step 1 — Code audit

### Cited code (pristine)

`channel/Coordinator.java`
- `:126-141` `process()`: when the interval is reached, `startNewCommit()` + `send(StartCommit)`
  (`:127-134`), then `consumeAvailable(1s)` (`:136`), then **`if (commitState.isCommitTimedOut())
  commit(true);`** (`:138-140`).
- `:144-157` `receive()`: DATA_WRITTEN → `commitState.addResponse` (buffered). DATA_COMPLETE →
  `addReady`, and `commit(false)` only if `isCommitReady(totalPartitionCount)` (`:151-153`).
- `:159-201` `commit(boolean partialCommit)`:
  - `:165-174` `catch (RuntimeException e) { if (partialCommit) { partialCommitFailures.incrementAndGet();
    LOG.warn("Partial commit {} failed for task {}, will retry", ...); return; } ...`. Every
    RuntimeException is swallowed. No exception classification, no counter limit.
  - `:176-180` full path: `if (!(e instanceof CommitFailedException)) { // CommitStateUnknownException,
    ValidationException, ForbiddenException, NPE, anything else -- not retryable, terminate
    immediately; throw e; }`
  - `:182-191` full path: `consecutiveCommitFailures++` against `config.commitMaxConsecutiveFailures()`
    (default **1**, `IcebergSinkConfig.java:110-112`), throws when the limit is reached.
  - `:198-200` `finally { commitState.endCurrentCommit(); }` (sets currentCommitId=null and clears
    readyBuffer, `CommitState.java:95-99`). It does **not** clear `commitBuffer`.
- `:203-231` `doCommit`: per-table `commitToTable` via `Tasks.foreach(...).executeWith(exec)
  .stopOnFailure()` (`:207-213`). Only after every table succeeds: `commitConsumerOffsets()`
  (`:216`), `commitState.clearResponses()` (`:217`), `send(CommitComplete)` (`:219-223`). So on a
  failure the buffered DataWritten (`CommitState.commitBuffer`) stay. Every later cycle re-groups
  the whole buffer (`CommitState.tableCommitMap`, `:139-144`) and retries it.
- `:430-432` `partialCommitFailureCount()` is package-private and used only by a unit test.
  `git grep -i "metric|MBean|Sensor"` over `kafka-connect/kafka-connect/src/main` finds nothing,
  so nothing exposes the counter outside the JVM (no JMX metric).
- `Tasks.throwOne` (`core/.../util/Tasks.java:603-617`) rethrows the original exception type, so
  the type reaching `commit()` is the catalog's exception (e.g. RuntimeIOException,
  ForbiddenException).

`channel/CoordinatorThread.java:49-57`: `while (!terminated) { try { coordinator.process(); } catch
(Exception e) { error = e; terminated = true; } }`. The thread ends only if `process()` throws.
The partial path never throws.

`channel/CommitterImpl.java:198-212` `processControlEvents()` (runs on every `put()` via `save`,
`:190-196`): only `coordinatorThread.isTerminated()` raises `NotRunningException` (→ the Connect
task goes FAILED), and only when the termination is not fencing (`isFenced()`,
`CoordinatorThread.java:78-81`). A coordinator stuck in the partial-failure loop is never
terminated, so the task stays RUNNING.

`channel/Worker.java:68-114`: on StartCommit the worker completes its files and sends DataWritten +
DataComplete **and commits the source offsets in the same Kafka transaction**
(`send(events, results.sourceOffsets())`, `:111`; `Channel.send` `:104-107` uses
`sendOffsetsToTransaction`). `IcebergSinkTask.preCommit` returns no offsets. So the source offsets
of every record whose file is stuck in the coordinator buffer are already committed.

`channel/Channel.java:171-213` `commitConsumerOffsets()`: the `-coord` control-topic checkpoint
advances only after all tables commit, so during a stall the checkpoint stays put.

### When is the partial path the steady state? (reachability)

`commit(true)` runs whenever a started commit is not complete within `commit.timeout-ms`
(default 30 s), i.e. `receivedPartitionCount < totalPartitionCount`. `totalPartitionCount` is the
sum of every member's assignment (`Coordinator.java:106-107`).

- **Idle task (upstream #17811, OPEN, unfixed in this tree).** `CommitterImpl.save` creates the
  Worker only when `sinkRecords` is non-empty (`CommitterImpl.java:191-194`). A task whose
  partitions get no records never has a Worker and never answers StartCommit, so **every** commit
  round times out and is partial. #17811 says: "A Kafka Connect sink task that holds partitions but
  receives no records never answers the coordinator's commit requests, so every commit round
  stalls ... Each round falls back to a partial commit". The fix PR #17593 is still OPEN (checked
  2026-11-05). `close()` also stops the worker on every rebalance (`CommitterImpl.java:163`), so a
  task stays worker-less after a rebalance until its next record arrives.
- A slow or stalled worker, or a `commit.timeout-ms` that is short relative to worker response
  time.

Reachable through normal operation: a connector with `tasks.max ≥ 2` where at least one task's
partitions are idle (low-volume or skewed-key topic). In that deployment the full-commit path
(`:176-191`, the budget and classification) is never reached. Every commit failure goes through
`:165-174`.

### Trigger scenario (constructed)

1. Connector `tasks.max=2` on a 2-partition topic. Records arrive on only one partition. The idle
   task never answers StartCommit (#17811), so every round times out and `commit(true)` runs.
2. A permanent, non-retryable commit failure starts. Examples: the catalog returns 403
   `ForbiddenException` because the principal lost the table privilege, or the metadata location
   becomes unwritable (RuntimeIOException). Data-file writes by the workers are unaffected.
3. Each round: StartCommit → the active worker answers DataWritten/DataComplete and commits its
   source offsets → timeout → `commit(true)` → `commitToTable` throws → `:165-174` swallows it
   ("will retry") → `finally` ends the commit. The buffer is kept and grows. The `-coord`
   checkpoint is not advanced. The coordinator thread keeps running.
4. Result: the task stays RUNNING forever. The table receives nothing. Source offsets keep
   advancing, so consumer lag shows healthy. `commitBuffer` grows without bound. The only signal
   is a WARN line per round.
   Contrast: if all tasks answer, the same exception goes through `commit(false)` → `:176-179`
   rethrows → CoordinatorThread terminates → `NotRunningException` → task FAILED.

Fenced-coordinator sub-claim: a fenced coordinator's partial commit that fails with
`ProducerFencedException` (from `send(CommitToTable)` or `commitConsumerOffsets`) is swallowed the
same way. But `endCurrentCommit()` leaves no commit in progress, so the next action is
`process()`'s StartCommit `send()` at the next interval (`:127-133`). That call runs outside
`commit()`, so its ProducerFencedException leaves `process()` and terminates the thread. Expected
bound: termination is delayed by at most one commit interval, not indefinitely. Phase 2 will measure
this.

### Safeguards / masks recorded (Phase 2 must test whether they fire)

- Snapshot-offset dedup (`Coordinator.java:270-285`) and `SnapshotAncestryValidator`
  (`:361-381`) make the retries safe: no duplicates, and CommitStateUnknown retries are deduped.
  MC_hunt_s4_zombie found no safety violation. These guard **safety**, not liveness or surfacing.
- The buffer is kept and the `-coord` checkpoint is not advanced. If the fault heals, the next
  cycle commits everything, so no loss while the coordinator's memory or control-topic retention
  lasts.
- `partialCommitFailures` counter: not exported (see above).
- Fenced coordinator: the next StartCommit send throws outside `commit()`, so termination is
  bounded (see above).

## Step 2 — Developer-knowledge search (verbatim, cited)

Tracker searches (gh, apache/iceberg, issues + PRs incl. merged/closed): "partialCommitFailures",
"max-consecutive-failures", "commitMaxConsecutiveFailures", "\"partial commit\"",
"\"partial commits\"", "isCommitTimedOut", "coordinator fenced", "zombie coordinator",
"\"retry budget\"", "\"loop forever\"", "partial commit swallow", and longer phrasings. Upstream
commit history of `Coordinator.java`, `CoordinatorThread.java`, `CommitterImpl.java`,
`CommitState.java` and `Channel.java` since 2026-04/09 was checked through the GitHub API. The latest
commit touching `Coordinator.java` is #18039 (2026-10-02). It is included in this tree, and nothing
newer exists.

- **PR #16237** (merged 2026-05-18, fixes #15878), "Surface commit failures instead of silently
  swallowing them": "Rethrow all exceptions from `doCommit()` on full-commit failures. Partial-commit
  failures (triggered by `commitState.isCommitTimedOut()`) are logged at WARN and swallowed since
  the coordinator will retry on the next cycle." "This ensures commit failures surface to
  operators by terminating the coordinator thread, which transitions the Connect task to FAILED".
  Reviewer laskoviymishka, on an intermediate swallow-and-retry version: "`CleanableFailure` ... also
  includes permanent errors like 400/401/403. With this change, bad credentials can loop forever at
  WARN, which is pretty close to the original bug." and "I'd rather go back to the old shape:
  rethrow on full commit failure, swallow only the timed-out partial commit cleanup failure. If we
  really want retries here, I think we need to explicitly list retryable exception types, bound
  the retries, and document the buffer lifecycle."
- **Issue #15878** (closed by #16237): "Connector enters silent broken state after
  CommitFailedException ... no data written, no error surfaced": "Connector status remains
  RUNNING; No exception is surfaced to the Connect framework ... Source topic offsets advance
  normally, giving the appearance of healthy operation".
- **Issue #16392 / PR #16433** (merged 2026-06-17): "On overloaded Connect clusters, partial commit
  failures (triggered by `commitState.isCommitTimedOut()`) can become dominant. Operators should
  have visibility". Added the AtomicLong counter, "Exposed via package-private
  `partialCommitFailureCount()` for testability ... The counter can be wired to a Kafka Connect
  `Sensor`/MBean for JMX exposure in a separate PR". It is not wired in this tree.
- **Issue #16393 / PR #16434** (merged 2026-07-16): bounded retry for the **full** path only. "Only
  `CommitFailedException` is retried -- all other exceptions (CommitStateUnknownException,
  ValidationException, ForbiddenException) are fatal immediately"; "Only full-commit success resets
  the counter -- partial-commit success does not renew the retry budget". Reviewer: "it retries
  permanent failures like `ValidationException` / `ForbiddenException`, which won't heal by
  waiting."
- **Issue #17811** (OPEN): idle tasks make every round a partial commit (quoted above). Fix
  #17593 is OPEN.
- **Issue #16389** (OPEN proposal), laskoviymishka: "`Coordinator.commit(true)` still emits
  `COMMIT_COMPLETE` when the partial commit succeeds. So a Coordinator that is degraded and only
  making partial progress looks healthy to workers", and "it avoids the partial-commit blind spot".
- **PR #18012** (OPEN draft): bounds incomplete (missing/replaced-table) attempts, including
  timeout-partial ones, with `max-consecutive-failures`, but "Other partial-commit exception
  handling ... [is] unchanged."
- **PR #18039** (merged 2026-10-02): "A stale coordinator will identify that it has been fenced,
  and exit the coordinator thread. We leave the worker task running".
- Code comment `Coordinator.java:177-178`: "CommitStateUnknownException, ValidationException,
  ForbiddenException, NPE, anything else -- not retryable, terminate immediately". This applies
  to the full path only.
- Tests: `TestCoordinator.testPartialCommitFailureMetric` (`TestCoordinator.java:548-586`) asserts
  that one partial failure increments the counter. No test asserts behavior across repeated partial
  failures or with a permanent exception class. The harness scenario `partial_commit` covers one
  swallowed CommitFailedException.

## Step 3 — Known-status / precedent

No issue, PR, CVE or advisory reports **this** defect: the partial-commit catch at
`Coordinator.java:165-174` swallows every exception class with no budget, so a permanent failure
loops forever with the task RUNNING whenever commits are partial. Related but different:
- #16237/#15878: the full path (fixed). Its review explicitly **chose** to keep swallowing partial
  failures. That is a design decision, not a report of this defect.
- #16433: the observability counter (not a report of unbounded retry).
- #16434: bounds the full path only.
- #18012: missing/replaced-table skip-as-success, a different mechanism. It explicitly leaves
  this one unchanged.
- #17811: idle-task stalls (the enabler, not this defect).
- #16016: leaked coordinator thread caused by `close()` gating (different root cause).
- #17910: hanging coordinator in the consumer group (different root cause).

Pre-filter: code-review-sourced but **not already reported**, so it is NOT dropped and proceeds to
Phase 2. Novelty, unless Phase 2 changes the picture: **NEW**.
