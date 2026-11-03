# r5: catalog commit protocols: JDBC lost acknowledgement, Hive/DynamoDB lease without fencing (2026-11-03)

Harness: `r5/catalogs/` (own Gradle project, wrapper copied from dst/). Everything that confirms runs released
Iceberg 1.12.0 jars from Maven Central (`iceberg-core`, `iceberg-hive-metastore` plus its `tests` classifier),
`org.xerial:sqlite-jdbc:3.53.4.0`, and Hive 2.3.10 (the HMS version Iceberg 1.12.0 tests against). The TLA+ model
lives in `tla/lease/`. Reports are under `r5/catalogs/build/r5/` (28 KB). The per-run work dirs are deleted.

| id | status | severity | decided by | one line |
|---|---|---|---|---|
| A2-28 | **confirmed**, with a different consequence than the inventory predicted | **Major** | real JdbcCatalog + SQLite, fault injected at the driver | The 08007-after-apply makes JDBC report CommitFailedException for a commit that landed. A lone writer recovers, because its retry finds its own snapshot id. But a commit by another writer C inside the retry window gets **rolled back off main**: C was told success, its data disappears from scans, and the next expiry deletes C's data file. 20/20 in each of 5 operations; control 0/20. |
| A2-28 (predicted consequences) | refuted | none | same harness | No duplicate data files and no deleted reachable files in any of 6 single-writer operations (20/20 each). The predicted re-apply on top of the writer's own commit does not happen. |
| A2-28b (spurious failure) | confirmed | Minor | same harness | Operations with no retry loop (`ops.commit`, `updateSchema().commit()`) get CommitFailedException although the change landed (20/20). That is wrong reporting, and there is no data loss. |
| C-8 / C-3 | **confirmed** | **Major** (needs a process pause longer than `hive.txn.timeout`) | real embedded HMS 2.3.10 (Iceberg's TestHiveMetastore) + Hive's own lock reaper | With the hive lock on, A stalls inside its alter_table and its heartbeats stop, so HMS reaps A's lock. B takes the lock and commits (told success). A resumes and overwrites B. B's snapshot is not on main: 10/10, and 10/10 again when A's heartbeat resumes together with A (A then reports CommitStateUnknown). Controls with no loss: heartbeats live 0/10, no stall 0/10, lock disabled (expected-location CAS) 0/10. TLA+ reproduces the same result (calibration). |
| C-11 / C-13 | **candidate** (DESIGN result only) | would be Major | TLA+ `tla/lease` | The model loses a commit with DynamoDB lease + Glue without VersionId once a process pause is allowed (`dyn_pause`). It is safe without pauses (`dyn_nopause`) and safe with the VersionId CAS (`dyn_pause_cas`). No AWS here, so this is not confirmed. |

---

## A2-28: JDBC CAS UPDATE lands, then the client gets SQLState 08007

### Reproduce
```
cd r5/catalogs && ./gw test -Dr5.runs=20                         # all 12 scenarios, fault + control
./gw test -Dr5.runs=20 -Dr5.scenario=fast-append-cw,merge-append-cw,txn-fast-append-cw,overwrite-cw,delete-cw
# reports: build/r5/jdbc-summary-{all,cw}.txt, first-run detail in jdbc-detail-{all,cw}.txt
# -Dr5.scenario=<comma list>|all, -Dr5.mode=fault|control|both; unknown names throw IllegalArgumentException
```
Files: `src/test/java/r5/FaultDriver.java`, `JdbcLostAckTest.java`, `LocalFileIO.java`.

### Fault
- `FaultDriver` is a `java.sql.Driver` for `jdbc:r5fault:<path>`. It delegates to `jdbc:sqlite:<path>`.
- It recognises the CAS UPDATE structurally: the SQL text must equal JdbcUtil's own private constants `V0_DO_COMMIT_SQL` / `V1_DO_COMMIT_TABLE_SQL`, which it reads by reflection.
- For that statement it runs the real `executeUpdate()` (autocommit, so the row is durable). Then, while armed, it throws `new SQLException(..., "08007")`.
- The rows returned by every CAS execution are recorded (`casRows`). The number of injected faults is counted, and the harness throws unless a fault run injected exactly 1 fault and a control run injected 0.
- Catalog: `JdbcCatalog` with `jdbc.schema-version=V1`, a local-disk FileIO (Iceberg's `Files.localInput/localOutput`) that records every delete, and a format v2 table.
- Setup per run: commit S0 = append A. Then the operation under test (adds B) runs with the fault armed.
- Final state is read through a fresh catalog: snapshot history, live manifest entries per path, `planFiles()` tasks per path, manifest-list/manifest existence, and deleted-but-reachable files.

### What actually happens (casRows show it)
1. **TableOperations level** (`ops-direct`, `add-column`): `casRows=[1, 0]`.
   - The UPDATE landed (1 row). ClientPoolImpl reconnected and re-ran it (core `ClientPoolImpl.java:76-79`; JdbcClientPool retries by default, `JdbcClientPool.java:69`, and 08007 is retryable, `:50`). The re-run matched 0 rows.
   - Then `JdbcTableOperations.java:160-164` threw **CommitFailedException** for a commit that is in the catalog (`metadataChanged=true`, `schemaHasX=true`): 20/20 for both. Control: success, 20/20.
2. **Normal callers with their own retry**: `fast-append`, `merge-append` (`table.newAppend()`), `txn-fast-append`, `overwrite`, `delete`, and also `casRows=[1, 0]`. There is no third CAS.
   - SnapshotProducer's Tasks retry calls `apply()` again. The snapshot id is fixed per producer (`SnapshotProducer.java:713-722`), so `base.snapshot(newSnapshot.snapshotId()) != null` and the "rollback operation" branch (`:505-507`) sets main to the snapshot that main already points at.
   - `updated.changes().isEmpty()` → return (`:522-528`).
   - Post-commit cleanup deletes only the second attempt's manifest list (`:562-566`). Nothing reachable is deleted.
   - Result: **0/20 bad in every operation.** No duplicate data file (each path has exactly 1 live entry and 1 scan task), no missing file, and only one snapshot adds B.
   - The inventory's predicted consequences (re-apply on top of its own commit, duplicate files, or cleanup of the landed snapshot's files) are **refuted** for these operations.
3. **The real consequence: a concurrent writer is rolled back** (`*-cw`).
   - Same harness. A hook in the driver runs once, right after A's CAS UPDATE has executed on the database and before A's client sees the result. The hook runs in the fault run and in the control run.
   - In the hook, writer C commits `newFastAppend(C)` from its own JdbcCatalog and is told success.
   - Fault run: `casRows=[1 (A lands), 1 (C lands), 0 (A's ClientPool re-run), 1 (A's retry)]`.
   - A's retry refreshes, sees its own snapshot S_A in the snapshot list, and takes the rollback branch `setBranchSnapshot(S_A, main)` (`SnapshotProducer.java:505-507`). That is a real change, so it commits, and **main is moved back from S_C to S_A**.
   - In the transaction case the same code runs inside the `BaseTransaction.commitSimpleTransaction` retry (`BaseTransaction.java:358-369`).
   - Results: C told success, A told success, `ConMain=false`. C's file is absent from both manifest entries and `planFiles()` (for example `{A=1, B=1}` instead of `{A=1, B=1, C=1}`). After a routine `expireSnapshots().expireOlderThan(now).retainLast(1)`, **C's data file is deleted** (`C_FILE_DELETED_BY_EXPIRY`).

| scenario | fault: C acked but not on main / C's file deleted by expiry | control (hook runs, no fault) |
|---|---|---|
| fast-append-cw | 20/20 / 20/20 | 0/20 / 0/20 |
| merge-append-cw | 20/20 / 20/20 | 0/20 / 0/20 |
| txn-fast-append-cw | 20/20 / 20/20 | 0/20 / 0/20 |
| overwrite-cw | 20/20 / 20/20 | 0/20 / 0/20 |
| delete-cw | 20/20 / 20/20 | 0/20 / 0/20 |

Minimal sequence: A's CAS UPDATE applies → C commits on top → A's driver reports 08007 → ClientPoolImpl re-runs the UPDATE (0 rows) → CommitFailedException → A refreshes → its snapshot id is already in the table → "rollback" sets main = S_A → C's commit is gone from main. A later expiry makes the loss permanent.

**Severity Major.** A commit that was acknowledged is lost, and its data is later deleted. A correct caller can reach this: two writers on one JDBC-catalog table, plus one connection-level failure that hides the result of a committed UPDATE (08006/08007 after a network drop is exactly what those SQLStates mean). The window lasts from A's UPDATE until A's retry refresh, which includes the retry backoff (default min 100 ms).

The root cause is the CommitFailedException for a landed commit. CommitFailedException is supposed to mean "definitely not committed". SnapshotProducer's rollback branch then silently turns the false failure into a revert.

**Hive comparison (reading only).** HiveTableOperations never maps an ambiguous persist failure to CommitFailedException without checking:
- If HMS says "The table has been modified", it runs `checkCommitStatusStrict(newMetadataLocation, …)` (`hive-metastore/.../HiveTableOperations.java:366-379`) and throws CommitFailedException only if its own metadata location is not found.
- For any other error it runs `checkCommitStatus` (`:380-387`) and maps the result to SUCCESS or CommitStateUnknown (`:389-396`). CommitStateUnknown is not retried and not cleaned up (`SnapshotProducer.java:538-539`).

JdbcTableOperations has no commit-status check at all, and the connection-level retry sits below it. The residual Hive gap is A2-23: the strict check can miss when `previousFiles` was pruned. That was not run here.

### Not established
- The tests used SQLite only, with no H2 and no Postgres/MySQL. The fault is synthetic at the JDBC driver boundary: I did not cut a real network connection after a server-side commit.
- There is one concurrent-writer timing (deterministic, inside the window). I did not measure how wide the window is in practice.
- Data files are placeholder bytes. "Rows read" is `planFiles()` tasks and record counts, not an actual Parquet read.
- I did not run releases before 1.12.0.
- I did not check other non-SnapshotProducer updates (expiry, replace-sort-order, …) beyond `updateSchema`.

---

## C-8 / C-3: Hive lock lease with no fencing

### Feasibility
An embedded HMS runs from Maven Central:
- `iceberg-hive-metastore:1.12.0:tests` contains `TestHiveMetastore`.
- Plus `hive-metastore:2.3.10`, `hive-exec:2.3.10:core` (same excludes as iceberg's build.gradle), `hadoop-client:3.4.3`, Derby 10.15.2.0 and assertj.

It resolved and started on the first try (`./gw hiveTest`, `build.gradle` source set `hiveTest`).

Lock expiry in Hive 2.3.10 happens only in `TxnHandler.performTimeOuts` → `timeOutLocks` (javap: no other caller). That runs in `AcidHouseKeeperService`, which an HMS starts when the compactor initiator is on and the txn manager supports ACID. The test starts Hive's own `AcidHouseKeeperService` on the same Derby DB:
- `hive.txn.manager=DbTxnManager`, `hive.support.concurrency=true`, reaper interval 250 ms, `hive.txn.timeout=3 s`.
- The harness throws if the reaper's isAliveCounter never moves.

### Reproduce
```
cd r5/catalogs && ./gw hiveTest -Dr5.runs=10      # all 5 modes, ~3 min
# -Dr5.mode=<comma list>|all; unknown names throw. Reports: build/r5/hive-summary.txt, hive-detail.txt (run-0 call timelines)
```
File: `src/hiveTest/java/org/apache/iceberg/hive/HiveLeaseTest.java` (in Iceberg's package, to reach the protected `HiveTableOperations` constructor).

### Harness
- Writers A and B model two processes. Each has its own `HiveClientPool(4)` and its own catalog name (`procA`/`procB`), so the in-JVM `commitLockCache` lock (`MetastoreLock.java:446-447`) is not shared.
- Two harness bugs were found and fixed before any result counted:
  1. The reaper was not running (no ACID txn manager).
  2. With the catalogs' shared static CachedClientPool (size 2), A's two stalled threads exhausted B's pool. That blocked B's `checkLock` and got B's own WAITING lock reaped.
- Each writer's ClientPool hands its actions a `java.lang.reflect.Proxy` of `IMetaStoreClient`.
  - For A it blocks the first `alter_table*` call (persistTable, which is after `ensureActive()` at `HiveTableOperations.java:329`).
  - In `hbpaused` modes it also blocks A's `heartbeat` calls. This models a process freeze (long GC, VM suspend), or a partition during which A's request is in flight.
  - It logs every lock/checkLock/unlock/heartbeat/alter call with its result.
- The stall ends when B's thread has finished, or after 9 s. Final state is read through a fresh HiveCatalog.
- Lock config: `iceberg.hive.lock-heartbeat-interval-ms=1000`, `lock-check-max-wait-ms=300`, `lock-timeout-ms=30000`.

### Results (10 runs per mode)

| mode | A told | B told | B finished during A's stall | A's lock still live at resume | B's commit lost |
|---|---|---|---|---|---|
| stall-hbpaused (trigger) | success 10/10 | success 10/10 | 10/10 | 0/10 | **10/10** |
| stall-hbpaused-joint (heartbeat resumes together with A) | CommitStateUnknown 10/10 | success 10/10 | 10/10 | 0/10 | **10/10** |
| stall-hblive (control: same stall, heartbeats flow) | success | success | 0/10 (B waits) | 10/10 | 0/10 |
| nostall (control) | success | success | n/a | n/a | 0/10 |
| nolock-stall-hbpaused (control/fix: `iceberg.engine.hive.lock-enabled=false`, expected location sent) | success (after 1 retry) | success | 10/10 | n/a | 0/10 |

Run-0 timeline, trigger mode (ms):
- 121: A `lock` → ACQUIRED.
- B's `lock` → WAITING, then B polls `checkLock` (WAITING) until about 3.2 s.
- The reaper drops A's lock. 3514: B `checkLock` → ACQUIRED. 3530: B `alter_table` ok, so B is told success.
- 3550: A's released `alter_table` ok. The write carries no expected location, because the lock is on (`HiveTableOperations.java:332-333`).
- 3552: A `unlock`. 3586: A's held heartbeat → NoSuchLockException.
- Final: `live={A, S0}`, `BonMain=false`.

In the joint variant the heartbeat fails before A's second `ensureActive()` (`:334`), so A gets CommitStateUnknown (the message at `:340-343` warns about exactly this overwrite). B's commit is already overwritten anyway.

In the lock-disabled control, A's first `alter_table` gets MetaException ("table has been modified": HMS 2.3.10 enforces the expected-parameter CAS). A retries and both commits survive.

Minimal sequence: A locks, reads m1 and checks it → A freezes (commit thread and heartbeat thread) for longer than `hive.txn.timeout` → HMS reaps A's lock → B locks, reads m1, writes m2, success → A resumes and writes m3 (based on m1) with no CAS → m2 is lost. A reports SUCCESS, or CommitStateUnknown if a heartbeat failure is seen first.

**Severity Major, with this condition stated.** An acknowledged commit is lost. The trigger is a freeze or partition of the lock holder that lasts longer than `hive.txn.timeout`, which is 300 s by default (heartbeat every 240 s, first beat at 120 s). Timeouts in the test were scaled down to 3 s / 1 s. Such pauses do occur in production (VM suspend/live migration, container freeze, very long GC, network partition with the request queued), but they are rare.

The design is acknowledged in the code's own error text. The fix shape already exists and is confirmed here: lock disabled → expected-location CAS (0/10).

Source: `hive-metastore/src/main/java/org/apache/iceberg/hive/MetastoreLock.java:140-151` (lock + heartbeat start), `:154-168` (ensureActive is a local flag check), `:468-499` (heartbeat stops after one failure); `HiveTableOperations.java:254-336`.

### Not established
- I ran only Hive 2.3.10 HMS on Derby, with no Hive 3/4. I believe Hive 3+ runs the housekeeper by default, but I did not verify it.
- Timeouts were scaled down. The freeze is injected at the client proxy rather than by a real GC pause or VM suspend.
- The stall point was after the first `ensureActive`. A freeze before `:329` was explored only in the TLA+ model (it also loses the commit there when the heartbeat thread has not yet run after resume).
- C-4 (heartbeat dies on RuntimeException) and C-6 (lost lock response) were not run.

---

## TLA+ design model: `tla/lease/LeaseCommit.tla` (C-3/C-8 calibration, C-11/C-13 candidate)

Run: `tla/lease/run.sh all` (regenerates the .cfg files from `configs.py`, rejects unknown names through `configs.get()` which raises KeyError, then calls `tla/run2.sh`). Results are in `tla/lease/results.txt` and the logs in `tla/lease/out/`. `python3 tla/trace.py lease <cfg>` prints a counterexample.

**Model.**
- Two writers. The catalog holds a pointer plus the set of writers in the current lineage. A global clock ticks, but it may not pass a heartbeat that is due for a non-paused process, so a lease can lapse only through a pause.
- `LOCK="hms"`:
  - Lock acquired if free, heartbeat every HB.
  - A reaper drops the lock after T without a beat.
  - A heartbeat that finds the lock gone sets the failure flag and stops.
  - Commit steps: check `loc==base`, ensureActive (local flag), write (no CAS unless `CAS`), ensureActive → CommitStateUnknown.
- `LOCK="dynamo"`:
  - Acquire: put-if-absent, otherwise observe the version, wait LEASE, then put-if-version-unchanged.
  - The owner heartbeats a new version, and heartbeat failures are only logged.
  - Glue commit steps: checkMetadataLocation, then write without VersionId unless `CAS`.
- Constants: HB=1, T=2, LEASE=2, MaxTime=7, MaxRetry=1.
- The spec's `ASSUME LOCK \in {"hms","dynamo"}` rejects unknown lock names (`neg_unknown_lock` → "Assumption … is false").

| config | expectation | TLC result |
|---|---|---|
| hms_wit_both / dyn_wit_both | both writers can succeed (witness) | violated as intended |
| hms_wit_lapse / dyn_wit_lapse | lease can lapse inside the critical section with pauses | violated as intended |
| hms_nopause_wit_both / dyn_nopause_wit_both | no-pause controls are not vacuous | violated as intended |
| hms_nopause_nolapse / dyn_nopause_nolapse | without pauses the lease never lapses | no error (2,630 / 27,750 states) |
| hms_nopause / dyn_nopause | NoLostCommit without pauses | **no error** |
| **hms_pause** | NoLostCommit with pauses (matches the HMS run) | **violated** (18-state trace: A locks, pauses, is reaped, resumes; B locks and checks; A writes, success; B writes from the same base) |
| hms_pause_cas | with expected-location CAS | **no error** (75,152 states) |
| **dyn_pause** | NoLostCommit with pauses | **violated** (14-state trace: A acquires, pauses; B observes v1, waits LEASE; A resumes and checks; B's conditional put wins; both write from base 0; A's commit lost, both told success) |
| dyn_pause_cas | Glue VersionId CAS | **no error** (400,266 states) |
| neg_unknown_lock | unknown name rejected | TLC: assumption false |

Calibration: the model's Hive configuration reproduces the confirmed real-HMS outcome (lost commit with pauses, safe without pauses, safe with CAS). That raises confidence in the shared machinery used for DynamoDB. It does not confirm anything about `DynamoDbLockManager`/`GlueTableOperations` code.

The DynamoDB transitions come from the slice-C reading (§6.2): `aws/.../dynamodb/DynamoDbLockManager.java:175-236`, `:254-278`, `:377-386`; `aws/.../glue/GlueTableOperations.java:158` (checkMetadataLocation) and `:328-330` (VersionId set only when `lockManager == null`). Those code lines were read and cited but not executed. **C-11/C-13 therefore stay candidates.** A confirmation needs a run against DynamoDB Local plus a Glue stub, or real AWS, with the same pause harness.

Not modelled: lock-acquire timeouts, lost lock responses (C-6), the in-JVM lock, the commit-status check, more than two writers, or clock skew (DynamoDB compares no clocks, so skew does not matter there).
