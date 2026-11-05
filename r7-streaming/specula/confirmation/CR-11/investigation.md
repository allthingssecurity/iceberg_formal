# CR-11 — Investigation record (Phase 1, evidence only)

Finding: *Commit completion is a summed DataComplete count against a total frozen at coordinator
start, so a commit can be declared full early and its valid-through timestamp overstated.*

- Source classification: **Code Review**. The finding JSON has `"source": "code-review"` and
  `"counterexample": null`. The model-checking run for this scenario (`MC_hunt_s5_readiness.cfg`)
  found **no** `ValidThroughSound` violation within bounds; both first-pass counterexamples were
  fidelity gaps. Per the skill, a no-violation MC run counts as code review, not MC.
- Severity (as filed): Medium.
- Code base: worktree HEAD `9fffeb63b1605a7c3acf218e58b7eeb43bdb1b01` (2026-11-02). The worktree
  carries the Specula trace instrumentation (`git diff --stat`: 10 files). Every line cited below is
  from the **pristine** HEAD blob (`git show HEAD:<path>`). The cited lines match the finding's
  `affected_code` exactly. The instrumentation only adds read-only accessors to `CommitState` and
  hooks/locks to `Coordinator`; it does not change the readiness logic.

## Step 1 — Code audit

### Cited code (pristine HEAD)

| Site | What it does |
|---|---|
| `channel/Coordinator.java:81,106-107` | `private final int totalPartitionCount = members.stream().mapToInt(desc -> desc.assignment().topicPartitions().size()).sum();`. Computed once, in the constructor, from the group description the coordinator was elected from. It is final and never refreshed. |
| `channel/Coordinator.java:149-153` | `DATA_COMPLETE` → `commitState.addReady(envelope)`; `if (commitState.isCommitReady(totalPartitionCount)) commit(false);` (a **full** commit). |
| `channel/CommitState.java:61-71` | `addReady`: `readyBuffer.add(dataComplete)` for **every** commit id. For the current commit id it does `receivedPartitionCount += dataComplete.assignments().size()`. The sum is **not deduplicated** by (topic, partition). |
| `channel/CommitState.java:117-128` | `isCommitReady`: `receivedPartitionCount >= expectedPartitionCount`, which logs "received responses for all {} partitions". |
| `channel/CommitState.java:146-165` | `validThroughTs(false)` = min of the timestamps over every assignment in `readyBuffer`, but only when all of them are non-null. It covers only the DataComplete events received so far. |
| `channel/CommitState.java:51-59` | `addResponse`: every `DataWritten`, whatever its commit id, is appended to `commitBuffer`. `commitBuffer` is cleared only after a successful `doCommit` (`:101-103`, called from `Coordinator.java:217`). Late files are therefore carried into the next commit. |
| `channel/Coordinator.java:203-231,318-323,332-337,343-349` | `doCommit` → `commitToTable`. This writes `kafka.connect.valid-through-ts` into the snapshot summary and emits `CommitToTable` and `CommitComplete` events carrying the same `validThroughTs`. |

### Call chain (all reachable in normal operation)

1. Kafka Connect `WorkerSinkTask` rebalance listener → `IcebergSinkTask.open(partitions)` →
   `CommitterImpl.open` (`CommitterImpl.java:141-151`) → `hasLeaderPartition(addedPartitions)`
   (`:78-91`: Admin `describeConsumerGroups` on the connect group). If the first partition is in
   the added set, `membersWhenWorkerIsCoordinator = members` → `startCoordinator()` (`:223-231`).
   That method builds a `Coordinator` **only if `coordinatorThread == null`**, so a coordinator
   that is already running is never rebuilt with a new member list.
2. Worker side: `IcebergSinkTask.put` → `CommitterImpl.save` → `processControlEvents` →
   `Worker.process` → `Worker.receive(StartCommit)` (`Worker.java:67-114`). This builds
   `DataComplete(commitId, context.assignment()…)`, which lists the task's **whole current
   assignment** with each partition's last-written timestamp. It is sent in one Kafka transaction
   with the source offsets (`Channel.java:83-118`, `sendOffsetsToTransaction`).
3. Coordinator thread: `CoordinatorThread.run` → `Coordinator.process` → `consumeAvailable` →
   `receive(DATA_COMPLETE)` → `addReady` / `isCommitReady(totalPartitionCount)` → `commit(false)` →
   `doCommit` → `validThroughTs(false)` → snapshot property and events.

### Reachability and trigger scenarios (constructed, not executed)

- **T1: partitions added without re-election (cooperative / incremental assignment).** Two cases
  produce this: `Admin.createPartitions` on a source topic, or a new topic matching `topics.regex`.
  Under `CooperativeStickyAssignor` or the KIP-848 consumer protocol, the new partitions reach
  existing tasks through `open(added)` only. The coordinator host's `open(added)` does not contain
  the first partition, so `hasLeaderPartition` is false and nothing is re-elected. Even if it
  were, `startCoordinator()` is a no-op while a thread exists. `totalPartitionCount` keeps the old
  value.

  Example: two tasks, the source topic grows from 2 to 4 partitions, so each task now reports 2.
  The total stays 2. The first DataComplete to arrive already reaches 2, so the commit is declared
  full. It contains only that task's DataWritten files, and its valid-through is the min over that
  task's partitions only. The other task's records with earlier timestamps are not in the table.
- **T2: duplicate DataComplete during an incremental move.** Worker X answers `StartCommit(c)`
  with a list that includes partition p. A cooperative rebalance then revokes p from X
  (`close([p])` stops X's worker) and assigns it to Y via `open([p])`. Y's worker keeps running
  and has not processed `StartCommit(c)` yet, so Y answers `c` and also lists p. p is counted
  twice, and the sum can reach the total while a third owner Z has not answered.

  Example: the tasks hold {p0}, {p1}, {p2}. The task with p2 answers and then leaves. p2 moves to
  the task with p1, which answers with [p1, p2]. The sum is 3 out of 3, but p0 never answered.
- Under eager (default Range) rebalancing, a full rebalance usually stops and restarts the
  coordinator, which recomputes the total. *Not verified:* `close()` re-describes the group and
  may see a group that is still rebalancing, so the old coordinator could survive. This possibility
  was noted and not pursued; the drop decision does not depend on it.

### Safeguards and masks encountered (recorded, not proven here)

- No data loss or duplication follows from an early full commit:
  - The late `DataWritten` envelopes stay in `commitBuffer` (`CommitState.java:51-59`) and are
    committed in the next cycle.
  - Their source offsets were already committed in the worker's own transaction
    (`Channel.java:98-117`).
  - The snapshot control-offset filter (`Coordinator.java:270-285`) stops re-commits.

  The analysis report also expects metadata and stall consequences, not loss.
- When the frozen total is too high (for example, owned partitions disappear), every cycle ends by
  timeout as a partial commit with a null valid-through (`Coordinator.java:138-140`,
  `CommitState.java:146-152`). That is latency or a stall, not a wrong watermark.
- The consequence that survives the masks is the wrong completeness watermark:
  `kafka.connect.valid-through-ts` in the snapshot summary and `validThroughTs` in
  `CommitToTable`/`CommitComplete`. It is read by external consumers of the snapshot summary or
  the control topic. Nothing inside the sink reads it.

## Step 2 — Developer knowledge (verbatim evidence; no classification)

- **Docs and comments state the intended meaning:**
  - `Worker.java:76-78`: "include all assigned topic partitions even if no messages were read from
    a partition, as the coordinator will use that to determine when all data for a commit has been
    received".
  - `CommitState.java:123-126`, log text: "Commit {} ready, received responses for all {}
    partitions".
  - `kafka-connect-events/.../CommitComplete.java:72-75`, Javadoc: "Valid-through timestamp, which
    is the min-of-max record timestamps across **all workers** for the commit."
- **Tests:** `TestCommitState.testIsCommitReady` (`TestCommitState.java:37-60`) builds payload1
  with `ImmutableList.of(tp, tp)`, the same mock partition twice, and asserts
  `isCommitReady(3)` is true. The suite therefore asserts cardinality semantics. No test asserts
  coverage, or covers expansion or moves.
- **History** (the local clone has one squashed commit, so history comes from upstream via
  `gh api`):
  - `CommitState.java` was changed only by #10351 (2024-07-11, original design: a summed
    re-scan) and #16453 (2026-06-17, "Make CommitState.isCommitReady() O(1)": a running sum with a
    commit-id filter).
  - Issue #16361, which #16453 closed, had proposed "use a map to cache the topic partition names
    we have seen so far". The merged change kept the summed count instead.
  - `Coordinator.java` was last changed by #18039 (2026-10-02, stable coordinator transactional
    id). That change does not touch readiness.
- **Issue tracker:**
  - **apache/iceberg#17925**, "Kafka Connect: Require every expected partition before completing
    a commit" (PR, created 2026-09-02). It was closed **unmerged** by the stale bot on 2026-10-24
    ("This is not a judgement on the merit of the PR"). Its Problem section quotes
    `receivedPartitionCount += dataComplete.assignments().size(); … if (receivedPartitionCount >=
    expectedPartitionCount)` and says the total is compared "against the number of partitions the
    coordinator was constructed with". It states "Cardinality does not establish coverage" and
    lists inputs including "two workers transiently claiming the same source partition while an
    assignment moves". The consequence it gives: "The coordinator then performs a **full** commit
    — a subset of the cycle's data, stamped with a `kafka.connect.valid-through-ts` the table does
    not satisfy."
  - **apache/iceberg#18322**, "Kafka Connect: Verify assignment freshness before full commits"
    (PR, **open**, unmerged, last updated 2026-10-08). Its Problem section: "The coordinator's
    source partition count was fixed at construction. If the source assignment expands while that
    coordinator remains active, reports from the old partitions can admit a full commit before the
    added partition reports. The resulting snapshot and completion event can carry a completeness
    timestamp that does not cover the current assignment." It defers the dedup or coverage part to
    #17925.
  - Related but different mechanisms:
    - #17077 (issue, open) and #17080 (PR, open): `validThroughTs()` does not filter `readyBuffer`
      by commit id.
    - #16282 (issue, open): duplicate files during coordinator recovery.

## Step 3 — Known-status / precedent

How each clause of CR-11 maps to an existing report, all at the same sites:

| CR-11 clause | Existing report |
|---|---|
| Total frozen at coordinator construction (`Coordinator.java:106-107`) | #17925 ("number of partitions the coordinator was constructed with"); #18322 ("source partition count was fixed at construction") |
| Summed, non-deduplicated DataComplete count (`CommitState.java:61-71`, `:117-128`) | #17925 (quotes the two lines; "Cardinality does not establish coverage") |
| Trigger: incremental move, two owners answer for one partition | #17925 ("two workers transiently claiming the same source partition while an assignment moves") |
| Trigger: partitions added without re-election | #18322 ("If the source assignment expands while that coordinator remains active…") |
| Consequence: early full commit; overstated valid-through (`CommitState.java:146-165`) | #17925 ("full commit — a subset … stamped with a valid-through-ts the table does not satisfy"); #18322 ("completeness timestamp that does not cover the current assignment") |
| Carry-forward of any-commit-id DataWritten (`CommitState.java:51-59`) | Explanatory: it is why the harm is metadata only. It is not a separate defect claim. #18322 "keep[s] buffered responses". |

- Fix status: **unfixed**. Upstream `main` on 2026-11-05 still has
  `receivedPartitionCount += dataComplete.assignments().size()` and the final
  `totalPartitionCount` (see `known-status-evidence.txt`). No later commit touches readiness:
  `CommitState.java` was last changed 2026-06-17 and `Coordinator.java` 2026-10-02 (#18039,
  unrelated).
- Search coverage:
  - `gh search issues --include-prs` (open and closed) for `totalPartitionCount`,
    `receivedPartitionCount`, `isCommitReady`, "valid-through kafka connect", "expected partition
    coverage", "cooperative rebalance coordinator partition count", "partitions added
    coordinator", and "DataComplete duplicate partition".
  - The upstream commit history of the three cited files.
  - The open and closed state and comments of #17925, #18322, #16361, #16453 and #16282.
- The persistent-findings registry was **not** consulted: the dispatcher forbids reading other
  findings, and the upstream match already decides the pre-filter.

**Pre-filter result:** the finding is code-review-sourced, **and** existing upstream reports
(#17925, #18322) already describe this exact defect: the same mechanism at the same sites, with
the same consequence. The skill says open-vs-fixed does not matter, and a stale-bot closure is
explicitly "not a judgement on the merit". →
**`Status: DROPPED (code-review × known, cite: https://github.com/apache/iceberg/pull/17925, https://github.com/apache/iceberg/pull/18322)`**.
Per the skill, Phase 2 is skipped and no `repro/` test is written.

Evidence file: `known-status-evidence.txt` in this directory holds the captured `gh` output.
