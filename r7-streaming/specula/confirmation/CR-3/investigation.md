# CR-3 — Phase 1 investigation (evidence only, no verdict)

Finding: DataWritten with a null table uuid bypass the replaced-table check in
`Coordinator.commitToTable`, so files written for a dropped table can be committed into a
recreated table that has the same identifier.

Source: code review (modeling-brief §6.3 C-4). No model-checking counterexample exists for
this mechanism, so it is recorded as `Code Review`.

Line numbers refer to the pristine tree, `git show HEAD:<path>` at commit 9fffeb6 (upstream
`main`). The `source/` tree and the finding worktree both carry Specula TLA trace
instrumentation as uncommitted edits. The reproduction removes that instrumentation first.

## Step 1 — Code audit

### Cited code (pristine HEAD)

`kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/channel/Coordinator.java`

- `:247-250`: `commitToTable` loads the table **by identifier** with
  `catalog.loadTable(tableIdentifier)`, so after a drop and recreate it gets the
  replacement table.
- `:256-263`: the guard is
  `if (tableReference.uuid() != null && !tableReference.uuid().equals(table.uuid())) { LOG.warn("Skipping commits ... target table mismatch"); return; }`.
  When the uuid is null the check short-circuits, and the code falls through to the commit.
- `:270`: `committedOffsets = lastCommittedOffsetsForTable(table, branch)` reads offsets from
  the **new** table. A freshly created table has no snapshot, so this is `{}`.
- `:277-285`: the offset filter keeps envelopes with `minOffset == null || offset >= minOffset`.
  Because the map is empty, every envelope is kept.
- `:287-293`: data files from the kept payloads are collected (`recordCount() > 0`, distinct
  by location).
- `:312-325`: `table.newAppend().validateWith(offsetValidator(tableIdentifier, committedOffsets))`,
  then `dataFiles.forEach(appendOp::appendFile)` (`:324`), then `appendOp.commit()` (`:325`).
- `:343-349`: a `CommitToTable` event is sent. `doCommit` (`:216-223`) then commits the
  control-topic offsets and sends `CommitComplete`.

`kafka-connect/kafka-connect-events/.../TableReference.java`

- `:52-58`: `table_uuid` (field id 10_604) is an **optional** field.
- `:105-125`: `put()` maps a position to a field id using the **writer** schema. If the writer
  schema lacks 10_604, `uuid` is never assigned and stays `null`.
- `:143-160`: `equals`/`hashCode` include `uuid`, which affects grouping in
  `CommitState.tableCommitMap()` (the MC-1 mechanism, not this one).

`kafka-connect/kafka-connect-events/.../AvroUtil.java:53-70`

- `encode` uses `AvroEncoderUtil.encode(event, event.getSchema())`, which embeds the writer
  schema. `decode` reads with that embedded schema, so events from older versions decode
  without error.

`kafka-connect/kafka-connect/.../channel/CommitState.java:51-59`

- `addResponse` buffers **every** DataWritten, whatever its commit id, and only logs
  "Received commit response when no commit in progress, this can happen during recovery".

Worker side:

- In ≥1.11 (current), `data/IcebergWriterFactory.java:72-78` stamps `table.uuid()` from the
  table loaded **when the writer is created**. If that is null it warns: "Table {} does not
  have a UUID, this may cause issues with commit coordination on table replace".
- In ≤1.10.2, `channel/Worker.java` (tag `apache-iceberg-1.10.2`, lines 83-88) builds
  `TableReference.of(config.catalogName(), writeResult.tableIdentifier())`. That version's
  TableReference schema has only `catalog`, `namespace` and `name` (no `table_uuid`), and
  1.10.0, 1.10.1 and 1.10.2 are the same. `table_uuid` first ships in 1.11.0 (released
  2026-05-20, release notes list #14979 and #15011).

### Call chain (normal entry points)

```
CoordinatorThread → Coordinator.process()                         Coordinator.java:124-141
  → Channel.consumeAvailable() → AvroUtil.decode(record.value())  Channel.java:122-152
      (legacy writer schema ⇒ TableReference.uuid == null)
  → Coordinator.receive(DATA_WRITTEN) → CommitState.addResponse    Coordinator.java:144-148
  → receive(DATA_COMPLETE) → isCommitReady → commit(false)         :149-153
     (or commit timeout → commit(true), :138-140)
  → doCommit → tableCommitMap() → commitToTable(ref{uuid=null})    :203-213
  → loadTable(identifier) [new table] → guard skipped → offsets {} → appendFile → commit
```

This runs on every commit cycle. No special configuration is needed.

### Reachability: how a ≥1.11 coordinator receives a null-uuid DataWritten

- **(A) Rolling upgrade of Connect workers from 1.10.x to ≥1.11.** During the roll, tasks on
  workers that have not been upgraded yet answer the new coordinator's StartCommit with
  legacy DataWritten. The StartCommit/DataComplete schema is the same in both versions.
- **(B) Full-stop upgrade with retained events.** A 1.10.x coordinator stops before
  committing. Its workers' legacy DataWritten stay in the control topic beyond the
  coordinator group's committed offset. The new coordinator replays them, and
  `CommitState.addResponse` buffers them. #15011 describes exactly this case.
- **(C) Tables whose metadata has no uuid.** `table-uuid` is optional in v1. The guard never
  applies to them, even when every worker is current.

The drop and recreate (new uuid, same identifier) has to happen after the legacy writer
loaded the table and wrote files, and before the coordinator's `loadTable`. In (A) that
window is the commit interval (default 5 min) plus commit time. In (B) it is the whole
upgrade downtime.

**REPLACE TABLE is not a trigger.** `TableMetadata.buildReplacement`
(core/.../TableMetadata.java:710-748) uses `new Builder(this)`, and that constructor copies
`base.uuid` (`:981`). A replaced table therefore keeps its uuid, and the guard does not fire
for uuid-bearing events either. The finding's "or replaced" is imprecise. The relevant
trigger is drop + recreate, or any other way to put a different table under the same
identifier.

### Safeguards on the path (recorded here; Phase 2 tests whether they fire)

1. The uuid guard at `:256` is skipped when the uuid is null.
2. The control-topic offset filter at `:277-285` uses the **new** table's snapshot offsets,
   which are empty, so every file passes.
3. `offsetValidator` (SnapshotAncestryValidator, `:361-380`) compares the expected offsets
   (`{}`, from the new table) with the new table's base snapshots (`{}`). The check passes.
4. Catalog-side `AssertTableUUID` (REST) compares against the table loaded at `:250`, which
   is the new table. The check passes.
5. `MergingSnapshotProducer` requires `spec(file.specId())` to exist. Spec id 0 exists in
   every new table, so spec-0 files are accepted.
6. Append does no validation of data-file schema, field ids, partition tuple or location.

### Trigger scenario (concrete)

1. Table `db.tbl` has uuid U1 and schema `(1:id long, 2:data string)`. A ≤1.10.2 sink task
   writes source records into a Parquet file under U1's location.
2. A ≥1.11 coordinator sends StartCommit C (case A). Alternatively, the 1.10 coordinator
   stops before committing (case B).
3. An operator drops `db.tbl` (non-purge, so files stay: JDBC, Hive, Glue, REST) and
   recreates it with uuid U2, possibly with a new schema, spec or location.
4. The legacy task replies with DataWritten(C, TableReference{catalog, [db], tbl}, no uuid,
   files) and DataComplete(C).
5. The coordinator loads `db.tbl`, gets U2, skips the guard, and appends U1's files to U2
   with offsets for the control topic. It then commits the control-group offsets and sends
   CommitComplete.
6. Expected per the guard's contract (as for uuid-bearing events): "Skipping commits to
   table ... due to target table mismatch".

## Step 2 — Developer-knowledge search (verbatim, with citations)

- **#14979** "Kafka Connect: validate table uuid on commit" (merged 2026-01-09, in 1.11.0).
  Description: "Since the coordinator loads the table at time of commit and then processes
  the events, this can result in a number of issues if the table was dropped/moved and
  recreated including: - mismatched data file schema (field ids/types don't match the new
  table) - path mismatches where referenced data files are outside of the new table location
  - other potential metadata mismatches like referenced data files that no longer exist or
  invalid row ids".
- **#14979 review thread** (Coordinator.java):
  - singhpk234: "I wonder the case where lets say some one does a library upgrade and it has
    events without the uuid in the tableReference, what would happen in this case ?"
  - bryanck: "The events will be skipped. We should probably only check the UUID if the ref
    UUID is not null, even though that isn't ideal."
  - singhpk234: "instead of skipping should we fail instead, otherwise can it lead to data
    loss as the offsets will committed."
  - danielcweeks: "...I don't think there's really a safe recovery..."
  - bryanck: "+1 to skipping".
- **#14979 review thread** (IcebergWriterFactory.java:73):
  - amogh-jahagirdar: "...it preserves the existing behavior in case somehow it is [null]."
  - danielcweeks: "this is due to the spec stating that the `table-uuid` is optional in v1.
    It's safer to just account for it being null..."
- **#15011** "Kafka Connect: fix table UUID check" (merged 2026-01-09, in 1.11.0): "...it only
  compares the UUID if the message had it set. This is to handle the upgrade case, where
  existing messages in the control topic do not have the UUID set because they were created
  before this change was added." The test fixture was changed to `TableReference.of(...,
  null)`.
- **#17623** (merged 2026-08-15, 1.12.0) removed the deprecated `TableReference.of(String,
  TableIdentifier)` and `IcebergWriterResult(TableIdentifier, ...)`. "Callers now construct
  a TableReference with an explicit table UUID; the two tests pass null to preserve their
  previous behavior." It did not change the decode tolerance or the guard.
- **Code comment**, IcebergWriterFactory.java:74-76: "Table {} does not have a UUID, this may
  cause issues with commit coordination on table replace".
- **Tests**: `TestCoordinator` builds its DataWritten with a `null` uuid (`:307`, `:526`,
  `:577`), so the suite depends on the tolerance. No test exercises the mismatch guard. No
  test covers drop/recreate or mixed-version events.
- **Docs**: `docs/docs/kafka-connect.md` has nothing on the uuid check, upgrades, or
  drop/recreate.

Summary: the null tolerance is a deliberate upgrade-compatibility trade-off that the authors
called "not ideal". The same PR series lists the harms of committing old-table files into a
recreated table.

## Step 3 — Known-status / precedent

Persistent-findings registry lookup (`specula findings lookup`: commitToTable, table uuid,
TableReference, replaced table, uuid mismatch) returned `[]`.

Upstream tracker search (GitHub issues **and** PRs, open, closed and merged; run 2026-11-05):

- Queries: `"Kafka Connect" uuid null`, `"table mismatch"`, `recreated table`, `dropped table
  recreated`, `"table_uuid" kafka`, `upgrade uuid control topic`, `"target table mismatch"`,
  `"replaced table"`, `legacy DataWritten`, plus a PR search for `uuid` + `Kafka Connect`.
- Upstream commit history for `Coordinator.java`, `TableReference.java` and
  `IcebergWriterFactory.java` up to 2026-10-02 (#18039): no change to the null short-circuit
  since #15011.

Candidates checked:

- **#18012** (open, draft) "Fail incomplete commits for missing or replaced tables". This is
  the opposite side: when the guard **fires**, it stops discarding the responses and
  checkpointing. It keeps `tableReference.uuid() != null &&` unchanged, so it does not cover
  this mechanism.
- **#14979 / #15011**: these introduced the tolerance on purpose. The review question is
  developer awareness of a trade-off, not a filed bug report.
- **#18006, #17376, #15651, #16360, #18067**: different mechanisms (startup offset reset,
  hardening, per-commit-id grouping, cluster recreation, recovery). None mentions the
  null-uuid bypass.

Under the skill's rule, "known" requires an existing report of this exact defect. A
developer's awareness of a trade-off does not count. No issue or PR reports the null-uuid
bypass at `Coordinator.java:256` as a defect, so the pre-filter does **not** drop this
finding and it proceeds to Phase 2. Provisional Novelty is NEW.
