# MC-4 — Phase 1 investigation (evidence only, no verdict)

Finding: after the connector's commit branch is dropped (WAP: main fast-forwarded to the branch, then the
branch ref removed), the coordinator's *read floor* and the *validator floor* disagree, so every later
commit fails (`FloorReadValidatorAgree`, cfg `MC_hunt_s2_branch.cfg`, CE `spec/output/MC_hunt_s2_branch_bfs.out`).

Code base: `apache/iceberg @ 9fffeb63b1605a7c3acf218e58b7eeb43bdb1b01`. Line numbers below are the
**pristine** HEAD sources (the run's `source/` and `worktree/` carry TLA instrumentation; all reads here
used `git show HEAD:` or the pristine clone `confirmation/MC-4/pristine-src`).

## Step 1 — Code audit

### Cited sites (pristine line numbers)

| Site | Code | Behaviour |
|---|---|---|
| `kafka-connect/.../channel/Coordinator.java:265` | `String branch = config.tableConfig(id).commitBranch();` | branch from `iceberg.tables.default-commit-branch` / `iceberg.table.<t>.commit-branch` (`IcebergSinkConfig.java:71,398-401`) |
| `Coordinator.java:270` | `committedOffsets = lastCommittedOffsetsForTable(table, branch)` | the **read floor** ("expected offsets") |
| `Coordinator.java:388-393` | `latestSnapshot(table, branch)`: `branch == null ? table.currentSnapshot() : table.snapshot(branch)` | `Table.snapshot(String)` (`api/.../Table.java:380-387`) returns **null** when the ref does not exist |
| `Coordinator.java:395-400` | `if (snapshot == null) return Map.of();` | dropped branch ⇒ read floor = `{}` |
| `Coordinator.java:314-316 / 328-331` | `newAppend()/newRowDelta().validateWith(offsetValidator(id, committedOffsets)); op.toBranch(branch)` | validator carries the read floor |
| `Coordinator.java:361-372` | `validate(baseSnapshots) { lastCommittedOffsets = lastCommittedOffsets(baseSnapshots); return expectedOffsets.equals(lastCommittedOffsets); }` | strict equality |
| `core/.../SnapshotProducer.java:299,304` | `parentSnapshot = SnapshotUtil.latestSnapshot(base, targetBranch); runValidations(parentSnapshot);` | the **validator floor** is computed from `parentSnapshot`'s ancestry (`SnapshotProducer.java:377-388`) |
| `core/.../util/SnapshotUtil.java:478-489` | `ref = metadata.ref(branch); if (ref == null) return metadata.currentSnapshot();` | dropped branch ⇒ **falls back to main's head**. Javadoc :471-472: "If branch does not exist, the table's latest snapshot is returned" (intended core semantics: a commit to a missing branch creates it from main) |
| `SnapshotProducer.java:386-388` | `ValidationException.check(valid, "Snapshot ancestry validation failed: %s", errorMessage())` | message `Cannot commit to <t>, stale offsets: Expected: {} Committed: {...}` (`Coordinator.java:376-378`) |
| `SnapshotProducer.java:498` | `.onlyRetryOn(CommitFailedException.class)` | `ValidationException` is not retried inside `commit()` |
| `core/.../util/Tasks.java:603-616` | `throwOne` → `castAndThrow` | `doCommit` rethrows the `ValidationException` unchanged |
| `Coordinator.java:165-180` | partial: catch, `partialCommitFailures++`, WARN "will retry", return; full: `if (!(e instanceof CommitFailedException)) throw e;` (:176-179) | full path kills the coordinator, partial path swallows |
| `CoordinatorThread.java:49-56` | any exception from `process()` ⇒ `terminated = true; error = e` | |
| `CommitterImpl.java:198-207` | terminated & not fenced ⇒ `throw new NotRunningException("Coordinator unexpectedly terminated on committer ...", error)` | thrown from `save()` ⇐ `IcebergSinkTask.put/flush` (`IcebergSinkTask.java:86-97`) ⇒ Connect marks the task FAILED |
| `CommitState.java:95-103` | `endCurrentCommit()` keeps `commitBuffer`; only `clearResponses()` (after success, `Coordinator.java:217`) clears it | partial path re-attempts the same envelopes every interval |
| `SnapshotProducer.java:517` | `update.setBranchSnapshot(newSnapshot, targetBranch)` | only a *successful* commit would recreate the branch — and that is exactly the commit that fails |

So with a commit branch `B` configured and `B` absent while main's ancestry carries this connector's
`kafka.connect.offsets.<ctl>.<group>` property:
read floor (`Coordinator:398`) = `{}` ≠ validator floor (`SnapshotUtil:485` → main ancestry) = `{p: n, ...}`
⇒ `ValidationException` on **every** commit, regardless of the data being committed.

The asymmetry is only reachable when a branch is configured: with `branch == null` both sides use
`currentSnapshot()`; with an existing branch both sides resolve the same ref.

### Call chain (public entry → cited code)

Kafka Connect `WorkerSinkTask` → `IcebergSinkTask.put()` (`IcebergSinkTask.java:86`) → `CommitterImpl.save()` →
`processControlEvents()`; leader task runs `CoordinatorThread.run()` → `Coordinator.process()` →
`consumeAvailable` → `receive(DATA_COMPLETE)` → `isCommitReady` → `commit(false)` (full) — or
`isCommitTimedOut()` → `commit(true)` (partial) → `doCommit` → `commitToTable` →
`lastCommittedOffsetsForTable` (`:270`) → `appendOp.toBranch(branch)…commit()` →
`SnapshotProducer.commit` → `apply()` → `SnapshotUtil.latestSnapshot(base, branch)` (`:299`) → `runValidations` → throw.
All entry points are the normal runtime path; no special configuration beyond the documented commit-branch option.

### Reachability / trigger scenario (all documented, public operations)

1. Connector configured with `iceberg.tables.default-commit-branch=audit` (documented in `docs/docs/kafka-connect.md:71,81`).
2. Connector commits ≥1 batch to `audit` (snapshot summary carries the offsets property).
3. Operator publishes per Iceberg's own **Audit Branch** workflow (`docs/docs/branching.md:77-104`):
   `CALL system.fast_forward('db.tbl','main','audit')` (Java: `manageSnapshots().fastForwardBranch("main","audit")`).
4. The `audit` ref disappears, by either documented path:
   - `ALTER TABLE … DROP BRANCH audit` (`docs/docs/spark-ddl.md:602-607`; Java `manageSnapshots().removeBranch("audit")`), or
   - automatically: the doc's own branch is `RETAIN 7 DAYS` and "The branch reference will be removed when
     `expireSnapshots` is run 1 week later" (`branching.md:91,104`); ref age = now − head snapshot ts
     (`core/.../RemoveSnapshots.java:289-293`), so an idle connector branch (paused connector / quiet table) expires.
5. Next commit cycle: read floor `{}` vs validator floor `{0: n}` ⇒ `ValidationException`.

Model CE (14 states): S2–S12 one full commit; S13 commit lands on branch b (`heads=[b↦1,main↦0]`, snapshot floor `{0↦3}`);
S14 `branchDrops` fault: `heads=[b↦0, main↦1]` (main fast-forwarded to b, b removed) ⇒ invariant violated.
This is exactly steps 2–4 above.

### Safeguards / masks encountered (recorded, not judged)

- **Safety**: workers commit source offsets in the same Kafka transaction as their DataWritten/DataComplete
  (`Channel.java:98-108`, `Worker.java:108-111`); the coordinator's control-topic offsets only advance after a
  successful table commit (`Coordinator.java:216`, `Channel.java:171-212`), so failed commits are re-read from the
  control topic after restart → no loss / no duplicates *while the control topic retains the events*.
- **Manual recovery**: an operator re-creating `audit` at main's head makes both floors equal again.
- **Possible self-healing**: only if main's ancestry stops carrying the offsets property (all connector snapshots
  on main expired *and* a non-connector snapshot on top) — then validator floor `{}` = read floor `{}`.
- The `commitMaxConsecutiveFailures` retry budget (#16434) applies only to `CommitFailedException`; it does not cover this.

## Step 2 — Developer-knowledge search

Local repo is shallow (1 commit), so history/blame came from upstream GitHub (`gh api`, 2026-11-05).

- Upstream history of `Coordinator.java`: `d8e7642d4e` #10351 (2024-07-11, original coordinator incl. branch floor
  read) → `6b28b0edc8` **#14510** (2025-11-08) adds the `SnapshotAncestryValidator` offset check → `29a144adca`
  #14650 refactor → `2fe32aa8f7` **#16237** (2026-05-18) makes full-commit failures fatal → … → `dad714b8f0` #18039
  (2026-10-02, latest; already in this snapshot). **Upstream `main` (ea73769d12, 2026-11-04) `Coordinator.java` and
  `SnapshotUtil.java` are byte-identical to the snapshot** ⇒ site unchanged, no fix landed.
- #14510 body (verbatim): "validates that another commit hasn't moved the kafka offsets that would not be seen by
  the committer due to the table refreshing during the commit process." → the validator's intent is to detect a
  *concurrent committer*; nothing about branch resolution.
- #14506 (closed predecessor) review by singhpk234 on `Coordinator.java:306`: "if expectedOffset is null and
  lastCommittedOffset is non empty then we should fail ?" — the empty-expected case was discussed only as a
  concurrency guard.
- #16237 body (verbatim): "Rethrow all exceptions from `doCommit()` on full-commit failures. Partial-commit failures
  (triggered by `commitState.isCommitTimedOut()`) are logged at WARN and swallowed since the coordinator will retry on
  the next cycle." and "This ensures commit failures surface to operators by terminating the coordinator thread,
  which transitions the Connect task to FAILED via `CommitterImpl.processControlEvents()`." Its test list includes
  "`testCoordinatorCommittedOffsetValidation`: verifies that `ValidationException` (stale offsets) propagates."
- Test `TestCoordinator.testCoordinatorCommittedOffsetValidation` (comment verbatim): "prevents commits when another
  independent commit has updated the offsets during the commit process" (main branch only).
- **No kafka-connect test exercises a real commit branch** (only a Mockito stub `noOpAppend.toBranch(any())`,
  `TestCoordinator.java:228`).
- `SnapshotUtil.latestSnapshot(TableMetadata,String)` Javadoc (`:471-472`) documents the main fallback as intended
  core behaviour (new branch starts from main). Its upstream history (#9131 … #17700) shows no change to it.
- Docs: commit branch is a supported option (`kafka-connect.md:71,81`); WAP fast-forward + branch removal/expiry is
  the documented Iceberg workflow (`branching.md:77-104`, `spark-ddl.md:602-607`).

## Step 3 — Known-status / precedent

Searches (REST search API + `gh`), issues **and** PRs, open/closed/merged: `"commit-branch" kafka`,
`"default-commit-branch"`, `commitBranch`, `"stale offsets"`, `"stale offsets" "Expected: {}"`,
`"Snapshot ancestry validation failed"`, `SnapshotAncestryValidator`, `lastCommittedOffsetsForTable`,
`kafka toBranch`, `kafka connect "drop branch"`, `kafka connect "fast_forward"`, `"kafka connect" "audit branch"`,
`latestSnapshot branch does not exist`; plus a patch scan of all 81 "Kafka Connect" PRs updated since 2026-06-01
for changes to `latestSnapshot` / `lastCommittedOffsetsForTable` / `snapshot(branch)` / `commitBranch`.
Result: **no issue/PR/advisory reports this mechanism** (hits were the validator's own PRs #14506/#14510/#14650/#16237,
unrelated stall/duplicate issues #16282/#17340/#16360/#13593, and loose token matches #17080/#14797, checked and unrelated).
Persistent-findings registry lookup (`specula findings lookup`): `[]`.

Source = MC with an actual counterexample ⇒ no pre-filter applies; proceed to Phase 2.
Novelty evidence: NEW (nothing found for THIS mechanism at THIS site; upstream main unfixed as of 2026-11-04).
