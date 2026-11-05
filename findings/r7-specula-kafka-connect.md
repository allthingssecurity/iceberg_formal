# r7 Specula pipeline on the Iceberg Kafka Connect sink — lead-verified findings

Specula run `20261104-163223-c58c` (`~/Downloads/experiments/specula/runs/`), guidance
`r7/specula-run/iceberg-kafka-connect-guidance.md`. Specula modelled main @ 9fffeb6; every finding below that is
marked CONFIRMED was re-run by the lead on the RELEASED Maven Central jars.

## MC-1 — rolling upgrade from <=1.10 to >=1.11 silently drops one task's committed records (CONFIRMED, Major, NEW)

**Mechanism.** A <=1.10 worker sends `TableReference.of(catalog, id)` with no uuid (1.10.0 `Worker.java:104`);
a >=1.11 worker sends the table uuid (1.12.0 `IcebergWriterFactory.java:72-78`). The 1.12.0 coordinator groups
`DataWritten` by the full `TableReference` (`CommitState.java:139`), whose `equals`/`hashCode` include the uuid
(events 1.12.0 `TableReference.java:143-161`), so one table becomes two commit groups. The first group's snapshot
stores the coordinator's whole control-topic position as the `kafka.connect.offsets` floor
(`Coordinator.java` 1.12.0: merged offsets, then filter `envelope.offset() >= minOffset`); the second group's
`DataWritten` is below that floor and is skipped ("nothing to commit") or, with parallel commit threads, its retry
fails the offset validator and the coordinator dies; on restart the replay filters both. The workers have already
committed the source offsets in the same Kafka transaction as their answer, so the records are never redelivered.

**Precondition.** A rolling upgrade of the Connect cluster across 1.11.0 while the connector is running: the task
hosting the coordinator runs >=1.11 and another task still runs <=1.10, both writing the same table, or
uncommitted <=1.10 `DataWritten` still in the control topic when the upgraded coordinator starts (the case PR
#15011 meant to support). No operator mistake; no failure injection.

**Lead re-run on released jars** (`r7/lead-verify/mc1/`, 2026-11-05):
- System under test: `org.apache.iceberg:iceberg-kafka-connect:1.12.0` (SHA-1 8b6b8a3c… = Maven Central), events
  1.12.0, Kafka clients 3.9.2. No Iceberg source compiled. Legacy answer produced in a separate JVM from the
  released 1.10.0 jars only (`LegacyPeer110`; `iceberg-kafka-connect-events-1.10.0` and `iceberg-core-1.10.0`
  SHA-1 match Maven Central). Test: Specula's `TestBugMC1RefSplit` (real Coordinator, Worker, SinkWriter,
  HadoopCatalog table; MockProducer/MockConsumer as in the module's own tests).
- Trigger: 9 runs × 2 variants (sequential and 4 commit threads): **18/18 lost 3 of 6 committed records**
  (table rows 3, expected 6). Variant B additionally kills the coordinator (`ValidationException` or
  `IllegalArgumentException`).
- Control (lead-added, `-Dmc1.control=true`): the identical legacy answer (same data file, commit id, offsets)
  re-encoded with the table uuid, i.e. what a >=1.11 worker sends; only `TableReference.uuid` differs:
  **3 runs × 2 variants, 0 of 6 lost, table rows 6**.
  (A first control attempt was invalid: the flag was not forwarded to the test JVM and the runs were
  triggers; fixed in build.gradle and re-run. Those runs are counted with the triggers above.)
- Run: `cd r7/lead-verify/mc1 && ./gw test -Dmc1.legacyCp="$(cat legacy/cp.txt)" -Dmc1.peerClasses=$PWD/legacy/classes -Dmc1.workDir=$PWD/work/x [-Dmc1.control=true]`

**Code identity.** `CommitState.java`, `IcebergWriterFactory.java`, events `TableReference`/`DataWritten`/
`CommitToTable` are byte-identical between the 1.12.0 release and main @ 9fffeb6; `Coordinator`/`Worker`/`Channel`
differ only in transactional-id naming and the coordinator's offset-commit transaction.

**Novelty.** No issue or PR reports it (lead `gh` searches 2026-11-05; search sanity-checked by finding #14979).
Developer record: in PR #14979 (added the uuid) a reviewer asked what happens on a library upgrade with uuid-less
events; PR #15011 states uuid-less events must still be handled after an upgrade. Closed, unmerged #15651/#15710
discuss floor poisoning between groups of one table for a different grouping. None covers this split.

## MC-1 side defect — coordinator crashes on legacy-decoded events (CONFIRMED in the same runs, Major/availability)

A `DataWritten` decoded from a <=1.10 message keeps the 3-field writer schema in its `TableReference`; re-encoding
it inside `CommitToTable` with the 4-field schema throws `IllegalArgumentException: Invalid field position: 3`
after the snapshot has landed, so the coordinator terminates on every commit that contains legacy events.
Observed on released 1.12.0 in variant-B runs. Not separately searched beyond the queries above (no hits).

## MC-4 — dropping (or expiring) the connector's commit branch stalls every later commit (CONFIRMED, Minor/availability, NEW)

**Mechanism (released 1.12.0).** The coordinator reads its offset floor from `table.snapshot(branch)` (`Coordinator.java`
383-388), which is `null` once the branch ref is gone, so it expects `{}`. The commit's ancestry validator resolves the
parent with `SnapshotUtil.latestSnapshot(metadata, branch)`, which falls back to main's head when the ref is missing
(core `SnapshotUtil.java` 478-486), and finds main's `kafka.connect.offsets` floor. `{}` vs `{0=n}`: every commit
fails with `ValidationException: ... stale offsets`, the coordinator dies, and a restarted task fails the same way.

**Precondition.** The connector commits to a branch (`iceberg.tables.default-commit-branch` / per-table
`commit-branch`, e.g. Iceberg's documented audit-branch (WAP) workflow) and that branch is dropped after being
published, or expires through `max-ref-age`. Workaround: re-create the branch.

**Lead re-run on released jars** (`r7/lead-verify/mc1/.../TestLeadMC4BranchDrop.java`; same released-1.12.0
setup as MC-1: real Coordinator, Worker, SinkWriter, HadoopCatalog; written by the lead, not Specula's test):
3 runs. Trigger (publish audit -> main, drop audit): 3 runs x 3 commit cycles all `ValidationException`, main stays
at 3 rows. Control (publish, keep audit): 3 runs x 3 cycles all succeed, audit at 12 rows. In the trigger the
source offsets advance to 12 while only 3 records are in any table: the other 9 exist only as DataWritten on the
control topic (recoverable while retained; loss if the stall outlasts control-topic retention, argued only).
Run: `./gw test --tests 'org.apache.iceberg.connect.channel.TestLeadMC4BranchDrop'`.

**Novelty.** Specula found no issue/PR for this mechanism; not re-searched beyond its record.

## CR-3 — upgrade: <=1.10 events for a dropped table are committed into a re-created table of the same name (CONFIRMED, Major/corruption, NEW)

**Mechanism.** `Coordinator.commitToTable` loads the table by name and rejects a `DataWritten` only when its
`TableReference.uuid` is non-null and differs (`Coordinator.java` ~256; the null short-circuit added by PR #15011 "to
handle the upgrade case"). <=1.10 workers send no uuid, so their files, written for the dropped table U1 (its schema,
location), are appended to the new table U2; the offset filter and validator read U2's empty history and pass.

**Precondition.** Upgrade across 1.11 (rolling, or uncommitted <=1.10 events replayed after a full-stop upgrade) AND
a DROP + CREATE of the target table under the same name while those events are pending. REPLACE TABLE is not a trigger.

**Lead re-run on released jars** (`r7/lead-verify/cr3/`): Specula's `Cr3Repro` recompiled against the released
`iceberg-kafka-connect-1.12.0` classpath (program reports "Apache Iceberg 1.12.0 (commit 212aa894)"); legacy peer
from released 1.10.2 jars (SHA-1 match Maven Central). 2 runs x 6 scenarios, identical:
- A1 rolling/JDBC and B1 replay/JDBC: U2 (id string) references U1's file (id long); typed reads fail 3/3 rows
  (`Not an instance of java.lang.CharSequence`); persists after restart.
- A2 rolling/HadoopCatalog: U2 references U1's deleted file; `IcebergGenerics.read(U2)` -> `NotFoundException`.
- Controls A1c/A2c (current worker, uuid-bearing): "Skipping commits ... target table mismatch", U2 has 0 snapshots.
- A0 (no replacement): coordinator terminates after committing a 1.10.2 event = the MC-1 side defect.
A first lead run passed an empty scenario filter: 0 scenarios ran and the driver still printed "REPRODUCED"
(vacuous `allAsExpected`); discarded, re-run with `all` (653-line logs, 6 scenarios each).
Run: `cd r7/lead-verify/cr3 && java -cp "current-classes:$(cat current.classpath)" org.apache.iceberg.connect.channel.Cr3Repro run "legacy-classes:legacy-lib/*" all`

## Other Specula findings — status (2026-11-05 ~03:45)

- MC-2 (pause/resume `wakeup()` during `close()`): Specula verdict MASKED (53 + 59 throwing closes on an embedded
  Connect cluster, zero loss: the task is always restarted/reassigned and re-reads). Not a live bug.
- MC-3 (snapshot expiry removes the dedup floor): REPRODUCED, KNOWN (PR #18006) = our K1.
- MC-5 (coordinator election never retried -> silent stall, no commits, tasks RUNNING): Specula REPRODUCED (embedded
  Kafka 3.9.2 + 2-worker Connect, Level-1 latency in the test catalog / network). NEW. Lead verification pending.
- CR-1 (control topic recreated under a running connector -> in-memory high-water mark added in 1.12.0 by #17713
  skips the new topic's DataWritten -> permanent loss): Specula REPRODUCED on an embedded cluster. Symptom family
  KNOWN (#15293), this mechanism NEW. Lead verification pending.
- CR-2 (table dropped/renamed/recreated mid-cycle -> skipped records lost): DROPPED as KNOWN (#18012).
- 14 further code-review findings: in progress.
