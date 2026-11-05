# CR-14 — Phase 1 investigation (evidence only, no verdict)

Finding: `CommitToTable.snapshotId` is read from the refreshed table head after the commit and can
name another writer's snapshot. Source: code review (modeling-brief §6.2 T-9 / analysis-report F14);
no model-checking counterexample exists for it, so it is **Code Review**-sourced.

Artifact: `apache/iceberg @ 9fffeb63b1605a7c3acf218e58b7eeb43bdb1b01` (run.json `artifact_git_sha`).
The confirmation worktree (`CR-14/worktree`) carries the Specula trace instrumentation (10 modified
sink sources; `commitToTable` is restructured and holds a global trace lock across the CAS and the
`CommitToTable` send, see `.specula-output/harness/INSTRUMENTATION.md` "CoordCasCommit ... [L]"). All
line numbers below are from the **pristine** `HEAD` blobs (`git show HEAD:<path>`), which are what
the reproduction builds (`CR-14/pristine-src`, a `git archive HEAD` export, verified identical).

## Step 1 — Code audit

### Cited code (pristine)

`kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/channel/Coordinator.java`

```java
312      if (deleteFiles.isEmpty()) {
313        AppendFiles appendOp =
314            table.newAppend().validateWith(offsetValidator(tableIdentifier, committedOffsets));
...
318        appendOp.set(snapshotOffsetsProp, offsetsJson);
319        appendOp.set(COMMIT_ID_SNAPSHOT_PROP, commitState.currentCommitId().toString());
320        appendOp.set(TASK_ID_SNAPSHOT_PROP, taskId);
...
325        appendOp.commit();
326      } else {
327        RowDelta deltaOp = table.newRowDelta().validateWith(...);
...
340        deltaOp.commit();
341      }
342
343      Long snapshotId = latestSnapshot(table, branch).snapshotId();
344      Event event =
345          new Event(
346              config.connectGroupId(),
347              new CommitToTable(
348                  commitState.currentCommitId(), tableReference, snapshotId, validThroughTs));
349      send(event);
351      LOG.info("Coordinator {} completed commit to table {}, snapshot {}, commit ID {}, ...",
           taskId, tableIdentifier, snapshotId, ...);
...
388  private Snapshot latestSnapshot(Table table, String branch) {
389    if (branch == null) {
390      return table.currentSnapshot();
391    }
392    return table.snapshot(branch);
393  }
```

`snapshotId` is the **current head of the target branch as seen by `table` after `commit()`
returns**, not the id of the snapshot that `appendOp`/`deltaOp` created.

### What `table`'s head is after `commit()` (Iceberg core at the same SHA)

* `core/.../SnapshotProducer.java:486` `commit()`: the CAS is `taskOps.commit(base, updated.withUUID())`
  (`:535`). Immediately after it succeeds:
  ```java
  551  // at this point, the commit must have succeeded. after a refresh, the snapshot is loaded by
  552  // id in case another commit was added between this commit and the refresh.
  ...
  555  Snapshot saved = ops.refresh().snapshot(newSnapshotId.get());
  ```
  then `notifyListeners()` (`:580`). So the `ops` shared with `table` are **re-read from the catalog**
  after the CAS; core itself loads its own snapshot **by id** because "another commit was added
  between this commit and the refresh" is possible. Core's own event for the same commit
  (`FastAppend.updateEvent()`, `FastAppend.java:175-181`) also uses the producer's own
  `snapshotId()`, not the head.
* Metastore-style catalogs (`BaseMetastoreTableOperations`: InMemory, JDBC, Hive, Glue, Nessie, ...):
  `commit()` → `doCommit` (CAS, `:127`) → `deleteRemovedMetadataFiles` (`:128`) → `requestRefresh()`
  (`:129`); `refresh()` → `doRefresh()` reads the **latest** metadata pointer (`:86-102`;
  `InMemoryCatalog.java:440-447` reads `tables.get(id)`; CAS is `tables.compute` at `:466-484`);
  `current()` returns the cached refreshed metadata (`:70-75`).
* REST catalog (`RESTTableOperations`): `commit()` stores the POST response (`:227`, `:240`), but the
  subsequent `ops.refresh()` at SnapshotProducer `:555` is a **GET of the latest table metadata**
  (`RESTTableOperations.java:166-175`); `current()` returns it (`:161-163`).
* `BaseTable.currentSnapshot()` = `ops.current().currentSnapshot()` (`BaseTable.java:164-166`);
  `Table.snapshot(String)` = `snapshot(refs().get(name).snapshotId())` (`Table.java:380-387`).

Hence, for every catalog type, the head read at Coordinator `:343` reflects whatever the catalog
held at the moment of SnapshotProducer's post-commit refresh (`:555`). If any other writer commits
to the same branch between the connector's CAS (`:535`) and that refresh, `:343` returns **the
other writer's snapshot id**. The window contains the metastore's post-CAS work (`:128`, logging),
the commit response transfer (REST), and the refresh's own catalog round-trip.

### Call chain (reachable in normal operation)

Kafka Connect runtime → `IcebergSinkTask.open` (`IcebergSinkTask.java:58-59`) →
`CommitterImpl.open` → `hasLeaderPartition` → `startCoordinator()` (`CommitterImpl.java:147-149,
223-228`) → `CoordinatorThread.run()` loop `coordinator.process()` (`CoordinatorThread.java:40-56`)
→ `Coordinator.process()` (`:126-141`) → `consumeAvailable` → `receive()` DATA_COMPLETE →
`commit(false)` (`:150-153`) [or timeout → `commit(true)` `:138-140`] → `doCommit` (`:203-231`) →
`commitToTable` (`:242-359`) → `appendOp.commit()` (`:325`) / `deltaOp.commit()` (`:340`) →
**`:343`** → `send(CommitToTable)` (`:349`, `Channel.send` `:78-110`: one Kafka transaction on the
control topic). This runs on every commit cycle that has files for a table.

### Trigger scenario

1. Connector commit cycle k: workers answer `StartCommit` with `DataWritten`/`DataComplete`.
2. Coordinator's `appendOp.commit()` CAS succeeds → snapshot `S_conn` (summary
   `kafka.connect.commit-id = k`) becomes the head of `main` (or the configured branch).
3. Before SnapshotProducer's post-commit refresh, another writer commits to the same branch:
   compaction (`rewrite_data_files` → `replace`), `rewrite_manifests`, a second connector or
   batch/streaming job appending to the same table, a row-level DELETE/MERGE (e.g. GDPR purge).
   Its snapshot `S_other` (parent `S_conn`) becomes the head.
4. Refresh loads `S_other` as head; Coordinator `:343` reads `S_other.snapshotId()` and `:349`
   publishes `CommitToTable(commitId=k, snapshotId=S_other)`; the log line at `:351-357` prints it too.

Natural correlation: a writer whose own CAS lost to the connector's commit gets
`CommitFailedException`, refreshes and retries — so its commit tends to land right after the
connector's. Production windows are milliseconds (metastore round-trips, object-store metadata
read, REST RTT), not microseconds.

### Safeguards / masks encountered (recorded, not judged)

* None in the sink: after `send` the coordinator never re-reads or corrects the event; the event is
  durably committed to the control topic in a Kafka transaction (`Channel.java:98-108`).
* No in-repo consumer of the field: `Worker.receive` drops everything but START_COMMIT
  (`Worker.java:68-72`); `Coordinator.receive` handles only DATA_WRITTEN/DATA_COMPLETE (`:143-157`).
  The only in-repo "readers" are the coordinator log line (`:351-357`) and tests.
* Table contents and stored offsets are unaffected (the commit itself is correct).
* Existing test `TestCoordinator.assertCommitTable` (`TestCoordinator.java:476-485`) checks commitId,
  table reference and valid-through-ts but **not** `snapshotId`; no test has a concurrent writer.

## Step 2 — Developer-knowledge search

* **Event contract (javadoc)** — `kafka-connect-events/.../CommitToTable.java:31-36`:
  "A control event payload for events sent by a coordinator that indicates it has completed a commit
  cycle. Events with this payload are not consumed by the sink, they are informational and can be
  used by consumers to trigger downstream processes." The payload carries `snapshot_id`
  (field id 10_402, optional long) next to `commit_id` and `table_reference`.
* **PR #16917** (closed unmerged 2026-08-24, stale; https://github.com/apache/iceberg/pull/16917),
  "Fix CommitToTable payload Javadoc": "`CommitToTable` is a per-table event carrying
  `tableReference` and `snapshotId`; the coordinator sends one per committed table" — wording fix
  only, does not discuss which snapshot id is read.
* **Iceberg core comment** — `SnapshotProducer.java:551-552` (quoted above): core developers
  explicitly expect "another commit was added between this commit and the refresh" and load their
  own snapshot by id.
* **Origin** — the line came in with PR #10351 "Kafka Connect: Commit coordination" (merged
  2024-12; https://github.com/apache/iceberg/pull/10351), ported from tabular-io; its review
  threads on `Coordinator.java` (ajantha-bhat, fqaiser94, bryanck, danielcweeks) discuss delete
  support, partition specs, zombies, thread pools and naming — nothing about the snapshot id.
* **PR #16576** (closed unmerged; docs for the control topic) calls the control topic internal and
  lists `CommitToTable` in the per-commit event flow; no statement on `snapshotId` semantics.
* **User docs** — `docs/docs/kafka-connect.md` (pristine) never mentions `CommitToTable` or the
  snapshot id; the "Control topic" section (`:261-273`) only covers creation.
* No TODO/FIXME/"by design" comment near Coordinator `:343`. Local git history is a 1-commit
  snapshot (artifact clone is shallow), so blame is uninformative; upstream history used instead.

## Step 3 — Known-status / precedent

Searches run on 2026-11-05 (gh CLI, authenticated):

* `gh search issues --repo apache/iceberg --include-prs` for: `"CommitToTable" snapshot`,
  `CommitToTable`, `COMMIT_TO_TABLE`, `CommitToTable snapshotId`, `kafka connect snapshot id
  CommitToTable wrong`, `kafka-connect "latestSnapshot" snapshotId`, `kafka connect commit snapshot
  id concurrent writer`, `kafka connect snapshot id compaction`, `kafka connect concurrent commit
  snapshot id`, `kafka connect wrong snapshot`, `kafka connect currentSnapshot after commit`,
  `kafka connect "snapshot_id"`, `Kafka Connect: committed snapshot id`, `kafka connect snapshot id
  event`, `control topic snapshot id` → 0 reports of this mechanism (hits are about zombies,
  offsets, duplicates, docs, Javadoc wording #16917).
* Diff scan of the 150 most recent PRs (open, closed **and merged**) titled "Kafka Connect" that
  touch `Coordinator.java`/`CommitToTable.java`: none changes the `CommitToTable` snapshot-id
  source; only #10351 (origin) adds the line. (#14506/#14515 touch `latestSnapshot` for offset
  validation — a different aspect.)
* Upstream `main` (head f72900ee02, 2026-11-04T23:47Z): `Coordinator.java:343` is still
  `Long snapshotId = latestSnapshot(table, branch).snapshotId();`; the file's commit history on main
  (17 commits, latest #18039 2026-10-02) contains no fix.
* `databricks/iceberg-kafka-connect` (ex-tabular-io) issues/PRs: `snapshot id`, `CommitTablePayload`,
  `COMMIT_TABLE`, `commit table event snapshot`, `control topic consumer snapshot` → nothing for this
  mechanism (#253 is an event-decoder VTTS fix).
* GitHub code search `CommitToTable snapshotId language:Java` → 18 hits, all copies/forks of the
  Iceberg or Tabular sources; no independent public consumer of the field found.
* Specula persistent findings: `specula findings lookup` (CommitToTable / snapshotId /
  latestSnapshot / Coordinator.commitToTable) → `[]`; context has no previous run.

Result: **no existing issue/PR/CVE/dataset entry reports this defect** → not DROPPED; proceed to
Phase 2. Novelty, if confirmed: NEW.
