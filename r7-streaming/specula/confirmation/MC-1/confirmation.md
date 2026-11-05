# MC-1 — confirmation result

- **Status**: REPRODUCED (escalation Level 0 for variant A; variant B adds only a Level-1 timing aid in the test's catalog)
- **Source**: MC (counterexample `spec/output/MC_hunt_s1_refsplit_bfs.out`, FloorSound violated at state 18)
- **Novelty**: NEW (tracker + merged/closed PR search 2026-11-05, see `investigation.md` Step 3)
- **Location**: `kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/channel/CommitState.java:139`
  (grouping key = full `TableReference`, `TableReference.java:143-161`), with the floor/filter at
  `Coordinator.java:270-285` and the skip at `Coordinator.java:308-310`
- **Reproduction**: `repro/test_bugMC-1_refsplit_upgrade.sh` (+ `repro/test_bugMC-1_refsplit_upgrade/`),
  logs `repro/MC-1.log` (canonical run) and `repro/MC-1.validation-path.log` (variant B, HEAD group first)

## What ran
Pristine apache/iceberg @ 9fffeb6 (git archive of the worktree HEAD, no TLA instrumentation; the
script diffs CommitState/Coordinator against `git show HEAD:` and greps for `TlaHooks`).
JUnit test in package `org.apache.iceberg.connect.channel` of `:iceberg-kafka-connect:iceberg-kafka-connect`
drives this tree's real `Coordinator`, `Worker`, `SinkWriter`/`IcebergWriterFactory` on a real
`HadoopCatalog` table `db.tbl`; Kafka clients are `MockProducer`/`MockConsumer` as in the module's own
`TestCoordinator`/`TestControlTopicReplay`/`TestWorker`. The legacy task's StartCommit answer is
produced in a separate JVM from the released 1.10.0 jars only (`LegacyPeer110`, same constructor
calls as 1.10.0 `Worker.receive`, data file written by the 1.10.0 Parquet appender).

## Outcome (both variants, every run: 10 runs × 2 variants, all triggered)
- Two `DataWritten` for the same table decode as `identifiers equal=true, TableReference.equals=false
  (uuids null vs <table uuid>)` → two commit groups.
- The first group's snapshot stores `kafka.connect.offsets...={"0":9}` (the coordinator's whole
  position); the other group's `DataWritten` (@5 or @7) is below it.
- Variant A (threads=1): second group "found nothing to commit to table db.tbl, skipping" in the same
  commit; `-coord` checkpoint 9; later cycles, failover from the checkpoint and a full replay from
  offset 3 re-read both DataWritten and skip both — table stays at 3 of 6 rows.
- Variant B (threads=4, both groups read the same floor): either the HEAD group lands and the legacy
  group's CAS retry fails `ValidationException: ... stale offsets: Expected: {} Committed: {0=9}`, or the
  legacy group lands and the coordinator then throws `IllegalArgumentException: Invalid field position: 3`
  while encoding its `CommitToTable` (separate defect, see below); either way the coordinator dies, the
  restarted coordinator replays from checkpoint 3 and skips both groups; checkpoint moves to 12/13.
- The dropped rows' source offsets were committed in the answering worker's transaction
  (`{src-0=3}` observed from the real HEAD Worker; legacy `src-1=3` per 1.10.0 `Channel.send:96-105`).

## Side observation (different mechanism/site — not part of MC-1's verdict)
A `DataWritten` decoded from a <=1.10 message keeps the 3-field writer schema in its `TableReference`
(`TableReference(Schema)` ctor); when that group commits, `new CommitToTable(..., tableReference, ...)`
is re-encoded with the HEAD 4-field schema and `TableReference.get(3)` throws
(`AvroUtil.positionToId:88` ← `TableReference.get:129` ← `Channel.send:92`) after the snapshot landed:
the coordinator terminates on every commit that contains legacy-decoded `DataWritten`.
