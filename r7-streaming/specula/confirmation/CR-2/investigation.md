# CR-2 — Phase 1 investigation (evidence record)

Finding: *NoSuchTable / table-UUID-mismatch skip drops acknowledged DataWritten and then
checkpoints past them (LOSS)*. Source: **code review**. No counterexample exists, because table
drop, rename and replace are not modeled. The finding itself notes "related #18012".

Code audited: the **pristine** snapshot `9fffeb63b1605a7c3acf218e58b7eeb43bdb1b01`, read with
`git show HEAD:<path>`. The confirmation worktree carries uncommitted TLA trace instrumentation
(`TlaHooks`, `TlaTrace`, ~775 changed lines). It was not used for any conclusion. The snapshot's
`Coordinator.java` is byte-identical to upstream `main` HEAD `ea73769d12` (2026-11-04). See
`known-status-check.log` §4.

Persistent-findings lookup ran before the analysis:
`specula findings ... lookup --query commitToTable --query NoSuchTableException --query "table uuid" --query "skip commit"`
returned `[]`. The run context has `previous: null` and no inherited records.

## Step 1 — Code audit

### Cited sites (pristine line numbers)
- `Coordinator.java:249-254`. `catalog.loadTable` throws `NoSuchTableException`. The code calls
  `LOG.warn("Table not found, skipping commit: ...")` and then `return;`.
- `Coordinator.java:256-263`. If `tableReference.uuid() != null` and it differs from
  `table.uuid()`, the code logs `LOG.warn("Skipping commits to table ... target table mismatch")`
  and calls `return;`.
- `Coordinator.java:207-217`. `doCommit` runs `commitToTable` for every `TableReference` group
  through `Tasks.foreach(...).stopOnFailure()`. A normal return counts as success. The method then
  reaches the comment `// we should only get here if all tables committed successfully...`, calls
  `commitConsumerOffsets()` at `:216`, calls `commitState.clearResponses()` at `:217`, and sends
  `CommitComplete` at `:219-223`.
- `CommitState.java:101-103`. `clearResponses()` runs `commitBuffer.clear()`, which discards every
  buffered `DataWritten` envelope, including the skipped group.
- `Channel.java:171-213`. `commitConsumerOffsets()` commits `controlTopicOffsets()`. These offsets
  advance in `consumeAvailable` (`Channel.java:139`) past every consumed record, including the
  skipped `DataWritten` records. The commit goes to the `<connect-group>-coord` group, so a
  restarted coordinator does not re-read those records.

### Source-offset side (why the records are not re-delivered)
- `Worker.java:94-111`. On `StartCommit`, the worker builds one `DataWritten` per writer result.
  Each carries `writeResult.tableReference()`. The worker adds `DataComplete` and calls
  `send(events, results.sourceOffsets())`.
- `Channel.java:98-108`. That `send` runs one Kafka transaction:
  `producer.send(...)` for each event, then
  `producer.sendOffsetsToTransaction(offsetsToCommit, KafkaUtils.consumerGroupMetadata(context))`,
  then `commitTransaction()`. The **connect consumer group's source offsets therefore commit
  atomically with the `DataWritten` event**, before the coordinator ever looks at the table.
- `IcebergSinkTask.java:100-104`. `preCommit` returns `ImmutableMap.of()`, so the Connect runtime
  does no other offset commit. The worker's transaction is the only one.

### Where the table UUID in the event comes from
- `SinkWriter.java:58-69`. `completeWrite()` calls `writers.clear()`, so each commit cycle builds
  new writers on its first record (`SinkWriter.java:140-144`).
- `IcebergWriterFactory.java:57-80`. `createWriter` calls `catalog.loadTable(identifier)` (or
  auto-creates the table), then `TableReference.of(catalog.name(), identifier, table.uuid())`.
  The UUID is captured **when the cycle's writer is created**.
- `TableReference.java:144-161`. `equals` and `hashCode` include `uuid`.
  `CommitState.tableCommitMap()` groups envelopes by `TableReference`
  (`CommitState.java:139-144`).

### Call chain (entry point to cited code)
`CoordinatorThread.run` (`:49-57`) → `Coordinator.process` (`:126-141`) → `Channel.consumeAvailable`
→ `Coordinator.receive(DATA_COMPLETE)` (`:149-153`) → `commit(false)` → `doCommit` (`:203`) →
`commitToTable` (`:242`). A timeout follows a second path: `process` → `isCommitTimedOut` →
`commit(true)` → `doCommit`. Every commit cycle takes one of these paths. **Reachable in normal
operation.**

### Trigger scenario (constructed, not executed — see Step 3)
1. The connector ingests into `db.t` (UUID `U1`) with static routing. During commit cycle *k*, the
   first record routed to `db.t` causes worker W to load `db.t` (`U1`) and start writing data
   file F.
2. During cycle *k* (the default `iceberg.control.commit.interval-ms` is 300000), an operator does
   one of the following:
   - (a) `DROP TABLE db.t`
   - (b) `ALTER TABLE db.t RENAME TO db.t2`
   - (c) drops and re-creates `db.t`, giving it a new UUID `U2`. With
     `iceberg.tables.auto-create-enabled`, the worker can perform the re-create itself on the
     next cycle's first record.
3. `StartCommit(k)` arrives. In one Kafka transaction W sends `DataWritten{(db.t,U1), F}`, sends
   `DataComplete`, and commits source offsets `O`.
4. The coordinator receives all `DataComplete` and calls `commit(false)` → `commitToTable(db.t)`.
   - In cases (a) and (b), `loadTable(db.t)` throws `NoSuchTableException` → WARN, then `return`.
   - In case (c), `U2 != U1` → WARN, then `return`.
5. `doCommit` treats the return as success. `commitConsumerOffsets()` moves `-coord` past the
   `DataWritten`. `clearResponses()` discards it, and `CommitComplete` goes out.
6. Result: F is in no table. In case (b), F is missing from the table that still exists as
   `db.t2`. Source offsets `O` are committed, so the records of cycle *k* are never re-delivered.
   Restoring the table afterward does not bring them back.

### Safeguards encountered (recorded, not proven)
- None re-delivers the records. The worker's offset commit happens before the coordinator's
  check. Nothing ever reads `CommitComplete` to roll back. The `-coord` checkpoint moves past the
  envelopes.
- The only recovery path is manual, as a maintainer stated (Step 2): a new control-topic consumer
  group with offsets reset to before the events. It depends on control-topic retention and on the
  data files still existing; `DROP ... PURGE` may delete them.
- `Tasks...stopOnFailure()` and the `CommitFailedException` retry path in `commit()` do not
  apply, because the skip is a normal return, not an exception.

## Step 2 — Developer-knowledge search

### Issue tracker / PRs (via `gh api`; the local clones are shallow, depth 1)
- **PR #14979** "Kafka Connect: validate table uuid on commit" (merged 2026-01-09, danielcweeks).
  It added the UUID to `TableReference` and the commit-time check. Review thread on
  `Coordinator.java`, verbatim:
  - singhpk234: "instead of skipping should we fail instead, otherwise can it lead to data loss
    as the offsets will committed."
  - danielcweeks: "This is one of the difficult things with KC and errors like this.  If we fail,
    then you have events in the control topic, which will remain.  The only options at that point
    are to clear the control topic or move the offsets forward.  However, doing that can result
    in losing commit data for other tables since events for multiple tables are all intermixed.
    I don't think there's really a safe recovery and if the data needs to be recovered, you would
    create a new consumer group for the control topic and reset the consumer offset back to prior
    to these events."
  - bryanck: "+1 to skipping, we also skip if the table name is not found. ..."
- **PR #15011** "Kafka Connect: fix table UUID check" (merged 2026-01-09, bryanck): "First, it
  skips the commit if the UUIDs don't match, currently it just logs a warning but continues on.
  Second, it only compares the UUID if the message had it set." The `return` at `:262` comes from
  this PR.
- **PR #18012** "Kafka Connect: Fail incomplete commits for missing or replaced tables". Author
  nahidupa, opened 2026-09-08. **open, draft, unmerged**, labels `docs,stale,KAFKACONNECT`; marked
  stale 2026-10-31. It has no maintainer review; the only comment is the stale bot. Problem
  statement, verbatim: "Missing-table and UUID-mismatch paths in `commitToTable` previously
  returned normally. The enclosing cycle could then advance its control-topic checkpoint, clear
  buffered file responses, and publish `CommitComplete` although those files were not registered.
  Restoring the original table afterward did not restore the discarded in-memory response."
  - Its diff changes exactly the cited lines. It makes `commitToTable` return `false` on both skip
    paths and throws `IncompleteCommitException` (a `CommitFailedException`) before
    `commitConsumerOffsets()` and `clearResponses()`.
  - It adds `TestCoordinatorTableLookup.java`, 13 regression tests. The PR says that restoring
    skip-as-success makes 12 of the 13 fail.
  - It adds a docs section, "Missing or replaced tables during commit".
  - It is cross-referenced from #18006 and #18322.
- Other candidates checked; none is the same mechanism or site:
  - #13752 (open): Glue connection-pool / rebalance message loss, a duplicate of #13593.
  - #15878 (closed): a swallowed `CommitFailedException`, fixed by #16237.
  - #17819 (closed unmerged 2026-10-27): records dropped by the worker-side `NoOpWriter` under
    dynamic routing. That is a different site (`IcebergWriterFactory.java:65-66`).
  - #17376 and #17450 (closed unmerged): coordinator hardening; they do not touch the skip paths.
  - #15651 (closed): commitId group separation.
- Recently closed or merged Kafka Connect PRs (`label:KAFKACONNECT closed:>2026-09-15`, 39 results)
  include no fix for this mechanism. Upstream `main` still contains both `return;` skips and the
  unconditional checkpoint (`known-status-check.log` §4).

### Comments / docs
- The code comment at `Coordinator.java:215` reads `// we should only get here if all tables
  committed successfully...`. The skip paths reach it without committing.
- `docs/docs/kafka-connect.md:39` advertises "Exactly-once delivery semantics".
  `kafka-connect.md:363` documents skipping records for a **non-existent table under dynamic
  routing**, which is the worker-side `NoOpWriter`. Nothing documents the coordinator dropping
  in-flight files at commit time.

### Tests
- No upstream test in `kafka-connect/kafka-connect/src/test/` sets up a missing table or a UUID
  mismatch at commit, or asserts the skip behavior. A `git grep` for
  mismatch/NoSuchTable/dropTable/skipp in `channel/` tests found nothing relevant.

## Step 3 — Known-status / precedent (the one allowed drop)

- **Same mechanism, same site — reported.** PR #18012's Problem statement describes exactly this
  defect: the missing-table and UUID-mismatch returns in `commitToTable` lead to a checkpoint
  advance, cleared responses and `CommitComplete` for unregistered files. Its diff changes exactly
  the cited lines `:251-262` and `:215-217`. The same data-loss concern ("can it lead to data loss
  as the offsets will committed") was also raised against this exact skip in the PR #14979 review,
  and the maintainers kept the skip as a deliberate trade-off.
- **Fix status: unfixed.** #18012 is open, draft, stale and unmerged. Upstream `main` @
  `ea73769d12` is identical to the snapshot.
- The tracker search covered open issues, open PRs, and Kafka Connect PRs closed or merged since
  2026-09-15. Commands and real output are in `known-status-check.sh` / `known-status-check.log`.
- **Pre-filter applies.** The finding is code-review-sourced and an existing PR already reports
  this exact defect, so the outcome is
  `Status: DROPPED (code-review × known, cite: https://github.com/apache/iceberg/pull/18012)`.
  Per the skill (`phases/01-investigation.md` Step 3; `guide.md` "The only pre-filter"), **no
  `repro/` test is written**. This matches the pipeline contract in
  `prompts/confirmation/orchestrate.md:64` ("one `repro/` test per non-dropped finding") and
  `confirmlib.py:_validate_final_artifacts`, which requires a repro artifact only for
  `REPRODUCED`.
- No `issue.json` is written. The persistent registry stores only unresolved `REPRODUCED`,
  `ENV_LIMITED` and `MASKED` findings (`references/persistent-findings.md`).
