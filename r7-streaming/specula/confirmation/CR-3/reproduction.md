# CR-3 — Phase 2 reproduction record

## Test

- Entry point: `repro/test_bugCR-3_legacy_uuid_bypass.sh`. Sources live in
  `repro/test_bugCR-3_legacy_uuid_bypass/`.
- Command: `cd .specula-output/repro && timeout 2400 bash test_bugCR-3_legacy_uuid_bypass.sh all`
- Output: `repro/CR-3.log` (run 1) and `repro/CR-3.run2.log` (run 2). The summaries are
  identical and both runs exit 0.
- Escalation level: **0**.
  - Pristine upstream `main` 9fffeb6. The Specula TLA instrumentation was reverted in this
    finding's worktree, and the preflight confirms the sources match HEAD and the compiled
    `Coordinator.class` has no hooks.
  - The legacy peer runs the **released Iceberg 1.10.2** sink jars from Maven Central
    (`iceberg-kafka-connect` and `-events` sha1 verified), unmodified.
  - The system under test is driven only through normal entry points:
    `Coordinator.start()/process()`, `Worker.save()/process()`, and the `Catalog` API for
    DROP/CREATE through a separate client.
  - Kafka brokers are emulated with Kafka's `MockProducer`/`MockConsumer`, the same approach
    the project's own `TestCoordinator` uses. The task context is the real
    `WorkerSinkTaskContext`.
  - No state injection, stubs, failpoints, sleeps or source patches.

## Results

| Scenario | Worker | Path | Catalog | Outcome |
|---|---|---|---|---|
| A1 | 1.10.2 (no uuid) | rolling upgrade | JDBC (drop keeps files) | U1's file committed into U2 (`id long` file under `id string` table); typed read fails `Not an instance of java.lang.CharSequence: 1` |
| A1c | current (uuid U1) | rolling upgrade | JDBC | `Skipping commits to table db.tbl due to target table mismatch`; U2 untouched |
| B1 | 1.10.2 (no uuid) | full-stop upgrade, retained events replayed | JDBC | same as A1. After the restart: `found nothing to commit` (offset filter), so the bad snapshot is final |
| A2 | 1.10.2 (no uuid) | rolling upgrade | Hadoop (drop deletes dir) | U2 references a deleted file; `IcebergGenerics.read(U2)` throws `NotFoundException ... does not exist` |
| A2c | current (uuid U1) | rolling upgrade | Hadoop | rejected; U2 untouched |
| A0 | 1.10.2 (no uuid) | rolling, **no** replacement | JDBC | separate defect (see below) |

**Permanence.** Each crashed run was followed by a modeled task restart: a new Coordinator
resumes from the coordinator group's checkpoint. In every case the snapshot holding U1's
files remains in U2. No downstream mechanism reverts or masks the append.

## Separate defect observed on the same path (not CR-3)

A ≥1.11 coordinator that commits any 1.10.x DataWritten throws
`IllegalArgumentException: Invalid field position: 3`. The throw happens **after**
`appendOp.commit()` (Coordinator.java:325), while it sends `CommitToTable` (:343-349).

The reason: the decoded legacy `TableReference` keeps the 3-field writer schema, and
`TableReference.get(3)` (TableReference.java:129) calls
`AvroUtil.positionToId(3, legacySchema)` (AvroUtil.java:88-89).

Consequences:

- The `CoordinatorThread` exits ("Coordinator error during process, exiting thread",
  CoordinatorThread.java:51-53).
- `CommitterImpl.processControlEvents` throws `NotRunningException` (CommitterImpl.java:198-207),
  so the task fails.
- Control offsets and CommitComplete are not written.

Scenario A0 shows this with no table replacement involved, so it is independent of CR-3. An
upstream search for "Invalid field position" found no Kafka Connect report. It should be
filed as its own finding. It does **not** mask CR-3, because the bad append is already
durable when the throw happens.

## Worktree state

The worktree (`confirmation/CR-3/worktree`) was reverted to pristine HEAD to test the real
code. The removed trace instrumentation is backed up in
`confirmation/CR-3/instrumentation-backup/` (`tracked.patch`, `untracked.tar`, and file
lists). It can be restored with `git apply tracked.patch && tar -xf untracked.tar`, or with
`ICEBERG_SRC=<worktree> bash .specula-output/harness/apply.sh`.
`gradle/wrapper/gradle-wrapper.jar` (gitignored) was copied from `source/`.
