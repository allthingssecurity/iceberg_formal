# r5 summary (2026-11-03)

Every row below was reproduced on released Iceberg 1.12.0 jars from Maven Central, calling Iceberg's own classes,
with a control in which the bad outcome does not occur. "Lead re-run" = the lead session re-ran the reproduction
itself after the agent reported. Severity bar for Major: data loss, corruption, wrong results, or a lost/duplicated
commit, reachable by a correct caller under a realistic fault or schedule. Faults are injected; see each file.

| # | id | what goes wrong | trigger | trigger vs control | tool | lead re-run | upstream | file |
|---|---|---|---|---|---|---|---|---|
| 1 | A2-9 | default expiry deletes live manifests, reports success | manifest-list read of a retained snapshot fails 4× | 20/20 vs 0/20; reachable-cleanup calibration throws 20/20; 50% transient 2/40 | DST + new read fault | yes | no match (#15487 shows such read failures occur in production, different loop) | r5-dst-core.md |
| 2 | A1-29 | txn(append + rewriteManifests.clusterBy) keeps pass-1 entry seq numbers; a concurrent eq-delete is applied to the txn's new rows | concurrent writer during txn | 191/200 stale, 186/200 wrong delete attachment, 0 in controls | DST (lever: forcing the rewrite → 0/200; filter,withmeta lever → no effect) | yes | no match | r5-dst-core.md |
| 3 | B-122 | Avro variant columns silently swapped between concurrent writers (generic Record writer; Kafka Connect path) | two threads writing v3 Avro files | 100/100 files wrong vs 0/100 sequential | stress (gate: #16686 1.11.0 50/50, 1.12.0 0/50) | yes | no match | r5-races.md |
| 4 | D-31 | Spark add_files deletes the manifests a landed commit uses; every read fails | CommitStateUnknown after commit lands | 10/10 (Spark 4.1 and 3.5) vs 0/10 ×3 | engine harness | yes | **#13443**, stale-closed, never fixed | r5-spark.md |
| 5 | D-2 | Spark streaming to a branch appends a replayed epoch twice | driver failure before commit log write | 5/5 vs main 0/5 | engine harness | read results | no match | r5-spark.md |
| 6 | C-55 | equality-deleted rows return when queries on two schema versions share the executor cache (default on) | time travel across int→long promotion | 5–9/10 vs 0/10 (cache off, same schema) | engine harness | read results | root cause **#15039** (open); new trigger | r5-spark.md |
| 7 | E-45 | Flink EqualityConvertCommitter deletes DVs its own landed snapshot references | non-cleanable error after commit lands | 20/20 vs core RowDelta same fault 0/20 | Flink test harness | yes | no match | r5-flink.md |
| 8 | A2-28 | JDBC connection drop after the CAS applied → retry rolls back a concurrent writer's acknowledged commit | SQLState 08007 after UPDATE + a concurrent writer | 20/20 ×5 ops vs 0/20 | JdbcCatalog on SQLite | yes | **#18743 / PR #18745** (open, 2026-11-01) | r5-catalogs.md |
| 9 | C-8 | Hive commit with lock on overwrites another writer's acknowledged commit | client freeze > hive.txn.timeout (default 300 s) | 10/10 + 10/10 vs 0/10 ×3 | embedded HMS 2.3.10 + reaper; TLA+ calibrated | yes | same root as **#14096** (stale-closed); new trigger. Lock-free mode is the documented mitigation | r5-catalogs.md |
| 10 | B-19/B-20 | HadoopCatalog's in-JVM lock lets two committers in; commits reported successful are lost | heartbeat stalled ≥15 s (timeouts not configurable for HadoopCatalog) | 68/4000 successes lost, 10/10 runs vs 0 | stress, short-timeout config via reflection | no | no match (#16847 covers only the heartbeat map) | r5-races.md |

**Conditional rows:** 9 and 10 need a process/heartbeat stall longer than a lease (300 s for Hive, 15 s for
HadoopCatalog); neither stall was a real GC pause. Read them as Major-if-it-happens.

**Not counted as Major**
- E-4 (Flink re-commit after expiry): reproduced 20/20, but maintainers declined the fix (#18374) and treat snapshot
  retention as the operator's responsibility.
- C-11/C-13 (DynamoDB lock + Glue): TLA+ design result only.
- Open defect (two sequence numbers per manifest): cause now confirmed (r5-open-defect-cause.md); v3 second
  observable: overlapping `_row_id` ranges, 200/200. Metadata-level; no query run.
- Minors: A2-1, A1-18, B-18, B-24, B-133/D-10, C-54, E-33, A2-28b.

**Novelty:** no upstream match for 1, 2, 3, 5, 7, 10; new trigger of a known root cause for 6, 9; known for 4, 8.
