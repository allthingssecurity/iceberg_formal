# MC-1 — Phase 1 investigation (evidence only, no verdict)

Finding: one physical table under two `TableReference` keys (uuid `null` from a <=1.10 worker,
uuid `U` from a >=1.11 worker); the first group's commit stores the coordinator's whole
control-topic position as the table floor and the second group's `DataWritten` are then filtered.

Source audited: `apache/iceberg @ 9fffeb6` (pristine `git show HEAD:` copies under
`confirmation/MC-1/pristine/`; the run's `worktree/` and `source/` carry TLA instrumentation in
kafka-connect, so line numbers below are from the pristine files). Released 1.10.0 sources fetched
from tag `apache-iceberg-1.10.0` (`2114bf6`) into `confirmation/MC-1/upstream-1.10.0/`.

Persistent-findings lookup (`specula findings lookup --query tableCommitMap --query TableReference
--query FloorSound --query CheckpointSound --query uuid`): `[]` (no prior record).

## Step 1 — code audit

### Cited sites (pristine HEAD)

* `CommitState.java:139-144` — `tableCommitMap()` = `commitBuffer.stream().collect(groupingBy(dw.tableReference()))`
  (a `HashMap` keyed by the full `TableReference`). `commitBuffer` holds every buffered
  `DataWritten` regardless of commit id (`addResponse`, `:51-59`).
* `TableReference.java:143-161` — `equals`/`hashCode` = `catalog, namespace, name, uuid`.
  `put(TABLE_UUID)` (`:119-121`) leaves `uuid == null` when the writer schema had no `table_uuid`.
* `IcebergWriterFactory.java:72-78` (HEAD) — every HEAD writer is created with
  `TableReference.of(catalog.name(), identifier, table.uuid())` → uuid = the table's UUID.
* 1.10.0 `Worker.java:104` — legacy workers send `TableReference.of(config.catalogName(),
  writeResult.tableIdentifier())`; the 1.10.0 `TableReference` has no uuid field at all
  (`grep uuid` on the 1.10.0 file: no hits). PR #14979 (merged 2026-01-09, first released in
  1.11.0) added the field, added `uuid` to `equals`/`hashCode`, and switched the worker to send it.
* `Coordinator.java:203-213` (`doCommit`) — `Tasks.foreach(commitMap.entrySet()).executeWith(exec)
  .stopOnFailure().run(e -> commitToTable(e.getKey(), e.getValue(), controlTopicOffsets(), vtts))`.
  `exec` has `iceberg.control.commit.threads` threads (default `availableProcessors()*2`,
  `IcebergSinkConfig.java:227-229`).
* `Coordinator.java:256-263` — uuid check is skipped when `tableReference.uuid() == null`
  (PR #15011, "to handle the upgrade case"), so the legacy group IS committed.
* `Coordinator.java:270-275` — `committedOffsets` = floor read from the table's latest
  `kafka.connect.offsets.<control>.<group>` summary; `mergedOffsets` = max(floor,
  `controlTopicOffsets`) where `controlTopicOffsets` (`Channel.java:126-156`) is the coordinator's
  consumer position (last consumed offset + 1) on every control partition — i.e. it covers every
  envelope in `commitBuffer`, of all groups.
* `Coordinator.java:277-285` — envelopes with `offset < committedOffsets[partition]` are dropped.
* `Coordinator.java:308-310` — if nothing is left: log "found nothing to commit to table ...,
  skipping" and return normally.
* `Coordinator.java:312-325 / 328-340` — the append/row-delta stores `mergedOffsets` and validates
  with `offsetValidator(committedOffsets)` (`:361-381`, `SnapshotAncestryValidator`; core
  `SnapshotProducer.java:386-388` throws `ValidationException` "Snapshot ancestry validation
  failed" — not retried by the commit loop).
* `Coordinator.java:215-223` — after all groups returned: `commitConsumerOffsets()` (advances the
  `-coord` group past every consumed envelope), `clearResponses()`, `CommitComplete`.
* `Coordinator.java:159-201` (`commit`) — a non-`CommitFailedException` from a full commit is
  rethrown → `CoordinatorThread.run` (`:39-47`) terminates with the error →
  `CommitterImpl.processControlEvents` (`:198-208`) throws `NotRunningException` → task FAILED.
* `Worker.java:67-114` + `Channel.java:83-118` — a worker answers `StartCommit` with its
  `DataWritten`s + `DataComplete` in ONE Kafka transaction that also `sendOffsetsToTransaction`
  the source offsets (`IcebergSinkTask.preCommit` returns `{}`), so once a `DataWritten` is in the
  control topic its source records are committed and will not be redelivered.

### Call chain (entry points → cited code)

Kafka Connect `WorkerSinkTask` → `IcebergSinkTask.open/put` → `CommitterImpl.open` (elects the
task holding the first source partition, `CommitterImpl.java:78-128`, starts `CoordinatorThread`)
→ `Coordinator.process()` → `Channel.consumeAvailable` → `Coordinator.receive` (`addResponse`,
`addReady`) → `isCommitReady` → `commit(false)` → `doCommit` → `CommitState.tableCommitMap()` →
`commitToTable` per group → `commitConsumerOffsets`. Worker side: `put` → `CommitterImpl.save` →
`Worker.process` → `Worker.receive(StartCommit)` → `send(events, sourceOffsets)`.
All on the normal path; no test hooks.

### Reachability

The split needs, in ONE coordinator commit, `DataWritten`s for the same table with uuid `null`
and with uuid `U`, decoded by a >=1.11 coordinator (a <=1.10 coordinator's `TableReference`
ignores `table_uuid` and has a uuid-free `equals`, so it never splits). Natural ways:

1. **Rolling upgrade (mixed-version tasks)** — the Connect worker hosting the coordinator task
   (owner of the first source partition) runs >=1.11 while another worker still runs <=1.10 and
   both write the same table. Election rules are the same in both versions (min topic-partition;
   1.10.0 additionally requires a STABLE group), worker `transactional.id`s are per-task in both
   (`prefix + "worker" + suffix`), coordinator ids differ (`...coordinator<suffix>` vs
   `<group>-coordinator`), and the event schemas are Avro-resolvable both ways (the 1.10.0
   `TableReference.put` ignores unknown ids). Every commit cycle during the mixed window splits.
2. **Upgrade with uncommitted legacy events in the control topic** — the case PR #15011 targets:
   legacy `DataWritten` beyond the `-coord` checkpoint are re-read by the upgraded coordinator and
   buffered (`addResponse` accepts them outside a commit); the next commit also contains upgraded
   workers' `DataWritten` for the same table.

Model counterexample (`spec/output/MC_hunt_s1_refsplit_bfs.out`, 18 states): t1 = legacy, t2 = v;
t2 owns source partition 0 and hosts the coordinator (state 5); StartCommit at ctl offset 0
(state 9); t1 answers DW(T1, legacy)@1 + DC@2 (state 10, `srcCommitted[1]=1`); t2 answers
DW(T1, v)@3 + DC@4 (state 11, `srcCommitted[0]=1`); the coordinator consumes all (states 12-16,
position 5) and starts the full commit with two groups `<<T1,"legacy">>`, `<<T1,"v">>`; the
legacy group reads (merged `{0:5}`) and lands a snapshot with floor `{0:5}` holding only the
legacy rows (states 17-18). FloorSound is violated: the v group's DW@3 is below the stored floor
and not in the table. (`MC_hunt_s1_refsplit_ckpt` per the finding: checkpoint reaches 5.)

### Safeguards encountered (recorded, not judged)

* `offsetValidator` CAS (`Coordinator.java:361-381`): with parallel commit threads both groups
  read the same floor; the loser's retry fails validation → `ValidationException` → coordinator
  terminates → task FAILED. It does not restore the loser's files; after a restart the replay
  re-reads both groups and the floor (`:277-285`) filters both.
* `distinctByKey(location)` and the `-coord` replay guard (`Channel.java:126-135`) only dedupe; no
  mechanism re-sends a `DataWritten` after its transaction committed the source offsets.
* No upgrade guidance in `docs/docs/kafka-connect.md` (no "upgrade"/"rolling"/"drain" text).

### Trigger scenario (concrete)

Kafka Connect cluster with two workers, sink `tasks.max=2`, one source topic with 2 partitions,
all records routed to `db.tbl`. Worker B (upgraded, HEAD) runs task 0 (owns `src-0` → coordinator);
worker A (still 1.10.0) runs task 1 (`src-1`). Records arrive on both partitions; at the commit
interval the coordinator sends StartCommit; both tasks answer (source offsets committed); the
coordinator groups the two `DataWritten` under two keys and commits the first; the second is
"nothing to commit" (sequential) or fails the CAS validation (parallel) and is filtered on
replay; the `-coord` checkpoint passes both. Expected: the table lacks one task's records although
the `connect-<name>` group has committed their offsets.

## Step 2 — developer knowledge (verbatim, with cites)

* PR #14979 "Kafka Connect: validate table uuid on commit" (merged 2026-01-09,
  https://github.com/apache/iceberg/pull/14979) — added `uuid` to `TableReference` incl.
  `equals`/`hashCode`. Review (Coordinator.java:211), singhpk234: "I wonder the case where lets
  say some one does a library upgrade and it has events without the uuid in the tableReference,
  what would happen in this case ?" — bryanck: "The events will be skipped. We should probably
  only check the UUID if the ref UUID is not null, even though that isn't ideal." — danielcweeks
  (Coordinator.java:237): "The envelopes are already collected by TableReference before reaching
  this point. All we need is to ensure that the latest loaded table is consistent with the table
  reference used to partition the messages." — singhpk234 (:237): "instead of skipping should we
  fail instead, otherwise can it lead to data loss as the offsets will committed."
* PR #15011 "Kafka Connect: fix table UUID check" (merged 2026-01-09,
  https://github.com/apache/iceberg/pull/15011): "it only compares the UUID if the message had it
  set. This is to handle the upgrade case, where existing messages in the control topic do not
  have the UUID set because they were created before this change was added." → stated intent:
  legacy uuid-less events must still be committed after an upgrade.
* PR #15651 (closed unmerged, https://github.com/apache/iceberg/pull/15651) proposing per-commitId
  sub-groups per table: "Per-group offset isolation: stale groups write only their own envelope
  offsets to the Iceberg snapshot, not the global consumer position. This prevents an offset
  poisoning scenario where subsequent groups' envelopes would be incorrectly filtered out by
  Iceberg's offset-based deduplication." (PR #15710, closed: "`offsetsJson` ... only stored on the
  last batch to prevent `lastCommittedOffsetsForTable()` from filtering out subsequent batches.")
  → developers know that several groups of one table under the global-position floor poison each
  other, but only discussed it for a grouping that was never merged; neither mentions the existing
  uuid-based split.
* PR #18012 (open draft, https://github.com/apache/iceberg/pull/18012) — fails missing-table /
  UUID-mismatch skips; "responses for different UUIDs under one name requires operator
  intervention". It does not touch the null-vs-U split (the null group is still committed and the
  U group is filtered by the floor, a normal return).
* PR #17623 (merged 2026-08-15): removed the deprecated uuid-less `TableReference.of` — HEAD can
  no longer produce uuid-less references except for tables whose metadata has no uuid (v1).
* Comments/docs: no TODO/FIXME at the sites; no upgrade notes in the Kafka Connect docs.
* Tests: `TestCoordinator` builds every `DataWritten` with `TableReference.of(..., null)` (single
  group); `TestControlTopicReplay` uses `table.uuid()`; no test mixes null and non-null uuids for
  one table, and none asserts the current split behaviour.

## Step 3 — known-status / precedent

Tracker searches run 2026-11-05 via the GitHub API (issues + PRs, open/closed/merged): "table
uuid" kafka; "TableReference" kafka; "rolling upgrade"; upgrade control topic uuid; "nothing to
commit"; offsets floor same table; "commitToTable"; "tableCommitMap"; "null uuid";
uuid "control topic" existing messages; mixed version workers; "table_uuid"; group by table
identifier; "kafka.connect.offsets" lost; data loss coordinator; is:pr is:open CommitState.
Upstream commits on `CommitState.java`, `Coordinator.java`, `TableReference.java`,
`IcebergWriterFactory.java` since 2026-08-01: only #18039 (2026-10-02, already in 9fffeb6) and
#17623 — no fix after HEAD (main head `ea73769`, 2026-11-04).
No issue/PR/CVE reports THIS mechanism (uuid-bearing `TableReference` key splitting one table
into two groups, the first group's floor dropping the second). #15651/#15710 discuss the floor
poisoning shape for a different, unmerged grouping (precedent at a different site — not a match).
Source = MC with an actual counterexample → no pre-filter drop; proceed to Phase 2.
