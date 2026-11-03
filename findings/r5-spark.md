# r5-spark: engine reproductions of D-31, D-2, C-55/D-36 (2026-11-03)

All runs use released jars from Maven Central: `org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0`
with `org.apache.spark:spark-sql_2.13:4.1.1`, `local[2]`, JDK 21 (with the usual `--add-opens`). D-31 was
also run on `iceberg-spark-runtime-3.5_2.13:1.12.0` with Spark 3.5.6. The catalog is
`spark.sql.catalog.x=org.apache.iceberg.spark.SparkCatalog` with `catalog-impl=r5spark.FaultyHadoopCatalog`
(a `HadoopCatalog` subclass on a local temp dir, `cache-enabled=false`).

Harness: `r5/spark/` (build.gradle, `src/test/java/r5spark/*.java`). Each test class runs in its own JVM
(`forkEvery = 1`). Results are written to `r5/spark/build/r5/*.tsv`, each a few KB.
`-PsparkLine=4.1|3.5` selects the Spark line. Any other value throws, as do unknown mode, target or
ordering names in the tests.

I checked the 1.12.0 `-sources.jar` for every line cited below. The `iceberg/` checkout is on `main`. It
has the same code, and the only line numbers that differ are given in parentheses.

| id | status | severity | one line |
|---|---|---|---|
| D-31 | **CONFIRMED** | Major | Lost ack on `add_files` (v2) means the live snapshot references a deleted manifest and every read fails. 10/10 on Spark 4.1 and 10/10 on Spark 3.5. All 30 control runs per line are clean (no fault: 10 on v2 and 10 on v1; v1 with the fault: 10). |
| D-2 | **CONFIRMED** (branch route) | Major | A streaming epoch replayed after the driver dies between the Iceberg commit and `commits/N` is appended twice on branch `b`: 5/5 runs. Writing to main, or restarting without a replay, gives 0/5. |
| C-55 / D-36 | **CONFIRMED** | Major | The executor eq-delete cache is shared across an int→long promotion, so deleted rows reappear. Sequential runs fail in either order (5/10 and 7/10), concurrent runs 9/10. With the cache off, 0/10 in every ordering. |

---

## D-31: `add_files` deletes committed manifests on CommitStateUnknownException

- **Status:** CONFIRMED, Major (data loss: the table becomes unreadable after a fault that occurs in production, a lost commit acknowledgement).
- **Tool:** a real engine run (Spark 4.1.1 + Iceberg 1.12.0 runtime; Spark 3.5.6 + the 3.5 runtime), calling `CALL x.system.add_files(...)`.
- **Command:**
  ```
  cd /Users/I074560/Downloads/experiments/iceberg-verify/r5/spark
  ./gw test --tests r5spark.D31AddFilesLostAckTest -Dr5.runs=10                 # Spark 4.1
  ./gw test --tests r5spark.D31AddFilesLostAckTest -Dr5.runs=10 -PsparkLine=3.5 # Spark 3.5
  ```
  Output: `build/r5/d31-4.1.tsv` and `build/r5/d31-3.5.tsv`.
- **Fault:** `FaultyHadoopCatalog.LoseAckOps.commit` calls the real `HadoopTableOperations.commit` and lets it
  return normally (the new metadata is current), then throws `CommitStateUnknownException`. Only commits that
  add a snapshot are faulted, decided structurally by `metadata.snapshots().size() > base.snapshots().size()`.
  This filter is needed because `add_files` first commits a property update (the default name mapping): a first
  version of the harness faulted that commit, so `add_files` aborted before the append. That run was discarded
  as a harness artifact. Injections are counted, and the test asserts exactly 1 per fault run and 0 per control.
- **Sequence:** (1) Spark writes 100 rows as 4 parquet files. (2) `CREATE TABLE ... ('format-version'='2')`.
  (3) Arm one fault, then `add_files(table => 'db.t', source_table => '`parquet`.`<dir>`')`. (4) The append
  commit lands, then the ack is lost. `SparkTableUtil.importSparkPartitions` catches the Throwable and deletes
  the staged manifests. The user receives `CommitStateUnknownException`. (5) Reload the table. The current
  snapshot is the `add_files` snapshot and its manifest list exists. For each manifest, check
  `FileIO.newInputFile(path).exists()`. (6) Run `SELECT count(*), sum(id)`.
- **Counts (identical on Spark 4.1.1 and Spark 3.5.6, 10 runs each):**

  | mode | injected | snapshot landed | manifests missing | query |
  |---|---|---|---|---|
  | v2 + fault | 10/10 | 10/10 | 1 of 1 in 10/10 (`stage-*-task-*-manifest-*.avro`) | fails 10/10 (`NotFoundException <- FileNotFoundException`) |
  | v2, no fault | 0 | 10/10 | 0 | `count=100 sum=4950` 10/10 |
  | v1 + fault | 10/10 | 10/10 | 0 | `count=100 sum=4950` 10/10 |
  | v1, no fault | 0 | 10/10 | 0 | `count=100 sum=4950` 10/10 |

  The v1 control shows why the format version matters. On v1 without inheritance, `appendManifest` rewrites
  the staged manifests into snapshot-owned copies, so deleting the staged originals is harmless. On v2,
  snapshot-id inheritance commits the staged files as-is, so the deleted files are the committed manifests.
- **Iceberg source:** `iceberg/spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/SparkTableUtil.java:651-664`
  (`catch (Throwable e) { deleteManifests(...); throw e; }`). Spark 3.5 has the same code at
  `iceberg/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/SparkTableUtil.java:906-919` (1.12.0
  sources jar: 909-919). Core keeps added manifests on an unknown commit state
  (`core/.../SnapshotProducer` / `MergingSnapshotProducer.cleanUncommittedAppends`). Compare the correct
  guard in `RewriteManifestsSparkAction.java:397-418` (D-29).
- **Not established:** Spark 4.2 was not run. It is the same code per the inventory, but it is unverified
  here. Only `add_files` from a path-based parquet source was run. `importSparkTable` from a catalog table, and
  `SparkTableUtil` called directly, take the same `importSparkPartitions` path but were not exercised. I did not
  check whether some catalog's retry or reconciliation could rewrite the manifests. Nothing does in this
  harness. Whether the data can be recovered (the data files are untouched, only the manifests are gone) was
  not explored.

## D-2: streaming epoch idempotency checks main while committing to a branch

- **Status:** CONFIRMED for the branch-via-`IcebergSource` route. Major (a duplicated commit, so duplicate
  rows, after a driver failure, which Spark's replay is designed to handle).
- **Tool:** a real Spark structured streaming run (Spark 4.1.1 + Iceberg 1.12.0 runtime).
- **Command:**
  ```
  cd /Users/I074560/Downloads/experiments/iceberg-verify/r5/spark
  ./gw test --tests r5spark.D2StreamingBranchReplayTest -Dr5.runs=5
  ./gw test --tests r5spark.D2xBatchBranchRoutingTest     # side check, batch routing
  ```
  Output: `build/r5/d2-4.1.tsv` and `build/r5/d2x-4.1.tsv`.
- **Sequence:** (1) Create a v2 table. `INSERT (-1)` gives main a snapshot. For branch targets, also run
  `ALTER TABLE ... CREATE BRANCH b`. (2) One parquet file with ids 0..9 goes into an input dir. (3)
  `readStream.parquet(dir).writeStream.format("iceberg").option("branch","b").option("checkpointLocation",ck)
  .trigger(AvailableNow).start("x.db.t")` and await termination. This commits epoch 0 to `b` and writes
  `ck/commits/0`. (4) Simulate a driver death after the Iceberg commit and before Spark's commit log:
  delete `ck/commits/0` and `.0.crc`. The test checks that the file is gone. (5) Restart the same query on the
  same checkpoint. Spark replays batch 0 with the same queryId and epochId 0, and `commits/0` is rewritten
  (asserted). (6) Count rows with `id >= 0` on `x.db.t.branch_b` and on main.
- **Counts (5 runs per cell):**

  | target route | replay | duplicated rows | detail |
  |---|---|---|---|
  | `start(t)` + `option("branch","b")` | yes | **5/5** | b has 20 rows. Two snapshots carry the same `queryId/epochId=0` |
  | `start(t)` + `option("branch","b")` | no (restart only) | 0/5 | b has 10 rows, 1 streaming snapshot |
  | `start(t)` to main | yes | 0/5 | main has 10 rows, 1 streaming snapshot (the check skipped the replay) |
  | `toTable(t)` to main | yes / no | 0/5 / 0/5 | main has 10 rows |

  The control shows the replay itself works and the check works on main. The no-replay control shows the
  restart alone does not add rows.
- **Iceberg source:** `iceberg/spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/source/SparkWrite.java:582-617`.
  `findLastCommittedEpochId` starts at `table.currentSnapshot()` (605), which is main, while
  `commitOperation` does `operation.toBranch(branch)` (263-264). The branch comes from
  `IcebergSource.java:130-131` (the `branch` option becomes a `branch_b` selector, then
  `SparkTable.create(table, branch)`).
- **Routes that do not reach D-2 in 1.12.0 / Spark 4.1.1 (observed; root cause not analyzed):**
  - `writeStream...option("branch","b").toTable(t)`: the epoch is committed to **main**, not `b` (5/5, with or without a replay, no duplicates). The `branch` write option is only validated (`SparkWriteBuilder.java:77`). The branch passed to `SparkWrite` is the identifier's branch (`SparkWriteBuilder.java:139`, `SparkTable.java:188`).
  - `spark.wap.branch=b` with `write.wap.enabled=true`, through either `start(t)` or `toTable(t)`: the epoch also goes to **main** (5/5 each). The WAP branch path is therefore not reachable for D-2 here. The `spark.wap.id` / `stageOnly()` variant was not tested.
  - Side check, batch: `df.writeTo(t).option("branch","b").append()` also lands on main (5 rows on main, 0 on b). `INSERT INTO t` with `spark.wap.branch=b` lands on b. So the `branch` option is silently ignored on catalog-resolved writes in this release. This is a possible separate wrong-target-write issue. I did not check the docs to see whether `option("branch")` is documented for `writeTo` and `toTable`. It is not part of this task, and its severity is not assessed.
  - `toTable("x.db.t.branch_b")` with HadoopCatalog **created a new nested table** `db.t.branch_b`, because Spark's `toTable` creates tables it cannot find. A first harness version did this, and its rows were discarded as a harness artifact.
- **Not established:** Spark 3.5 and 4.2 were not run for D-2. A replay after a real driver kill (not a
  deleted commit-log entry) was not tried. The two leave the checkpoint in the same state, but that is
  assumed, not shown. Complete-mode streaming (`StreamingOverwrite`) on a branch was not run.

## C-55 / D-36: executor eq-delete cache ignores the projection's types

- **Status:** CONFIRMED, Major (wrong query results: rows removed by an equality delete are returned).
- **Tool:** a real Spark run (Spark 4.1.1 + Iceberg 1.12.0 runtime, local[2], so driver and executor share one JVM).
- **Command:**
  ```
  cd /Users/I074560/Downloads/experiments/iceberg-verify/r5/spark
  ./gw test --tests r5spark.C55EqDeleteCacheTest -Dr5.runs=10
  ```
  Output: `build/r5/c55-4.1.tsv`.
- **Setup (per table):** (1) Create a v2 table `(id int, data string)` and append ids 0..9 as one file.
  (2) Write an equality-delete parquet file on `id` with ids 3 and 7, using Iceberg's own
  `GenericFileWriterFactory`, then `newRowDelta().addDeletes(..)`. This snapshot is S1, with the int schema
  (asserted). (3) `updateSchema().updateColumn("id", long)`, then `INSERT (100L)`, which creates S2.
  Expected results: time travel to S1 gives `count=8 sum=35`, and the current scan gives `count=9 sum=135`.
  Each ordering and cache setting uses a fresh table, so cache groups never collide. The cache is switched
  with the session conf `spark.sql.iceberg.executor-cache.enabled`.
- **Counts (10 runs each):**

  | ordering | cache on | cache off | wrong result when it fails |
  |---|---|---|---|
  | time-travel(int) then current(long) | **5/10 wrong** | 0/10 | current returns `count=11 sum=145`: ids 3 and 7 reappear |
  | current(long) then time-travel(int) | **7/10 wrong** | 0/10 | time travel returns `count=10 sum=45`: no deletes applied |
  | both started concurrently (time travel and current) | **9/10 wrong** | 0/10 | current returns `count=11 sum=145` |
  | time travel twice | 0/10 | 0/10 | — |
  | current twice | 0/10 | 0/10 | — |

  **Ordering that triggers it:** whichever query loads the delete file into the cache first fixes the Java
  type of the cached keys (Integer or Long). The other schema version then misses every key in
  `StructLikeSet`, so in both sequential orders the *second* query is wrong. Concurrent runs fail almost
  always.
- **Measurement:** before the second sequential query, the test counts the table's entries in
  `SparkExecutorCache.state` (read-only reflection). Every wrong result (12/12 sequential) had the entry
  present. Every run where the entry was gone (3/20) was correct. In 5 runs the entry was present at the
  check and the result was still correct. I believe the cache was invalidated in between, by the async
  close of Q1's broadcast (`SerializableTableWithSize.close` → `invalidate`), but I did not prove it. This
  likely invalidation is why sequential runs fail only some of the time.
- **Iceberg source:** `iceberg/data/src/main/java/org/apache/iceberg/data/BaseDeleteLoader.java:108-116`
  (key `deleteFile.location()` at 111, value `readEqDeletes(deleteFile, projection)`, set built with
  `StructLikeSet.create(projection.asStruct())` at 103). The Spark group key is
  `iceberg/spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/source/BaseReader.java:264`
  (`table().name()`; 1.12.0 sources jar: 254). `SparkExecutorCache.java:113` builds `group + "_" + key`.
- **Not established:** a multi-executor cluster was not run. Each executor has its own cache, so the bug
  needs both queries' tasks on the same executor JVM within the cache lifetime. Long-lived executors make that
  likely, but how often it happens in practice is not measured. Only int→long promotion was tested. Other
  type-changing evolutions (float→double, decimal widening) were not run. The prefix over-invalidation part of
  D-36 (`t` vs `t2`) was not tested; it only affects performance. Spark 3.5 and 4.2 were not run for C-55.
