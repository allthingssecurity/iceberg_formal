# Hunting concurrency and failure bugs in Apache Iceberg

This repository is an evidence-gathering exercise on [Apache Iceberg](https://github.com/apache/iceberg)
(main @ `9fffeb63b16`, released code tested = **1.12.0** from Maven Central). It combines formal methods
(TLA+, Spin), a deterministic simulation harness that runs Iceberg's real commit code, stress tests, and
engine-level reproductions in Spark and Flink.

**Status:** nothing here has been reported upstream yet. Every "confirmed" finding was reproduced on
released 1.12.0 jars, calling Iceberg's own classes, with a control run where the bad outcome does not
happen. Faults (lost acknowledgements, read failures, connection drops, process freezes) are *injected*.
How often each fault happens in your production system is up to you to judge.

---

## The idea in one paragraph

You cannot model-check a whole codebase: TLA+ and Spin check *designs*, not code. So the work went in
two steps. **Step 1:** a mechanical inventory of every concurrency construct in the repository
(`synchronized`, `volatile`, atomics, concurrent maps, caches, thread pools, retry loops,
compare-and-set): 1,036 sites in 249 files, read into 417 rows. Each row records what shared state the
construct guards, whether that state survives a commit retry, and which tool could decide it.
**Step 2:** route each suspicious row to the tool that can actually decide it, and calibrate that tool
first on a bug that is already fixed: it must fail on the old release and pass on the new one.
A tool that has never caught a known bug tells you nothing when it finds nothing.

## Tools used, and what each can and cannot see

| Tool | What it is | Used for | Blind spot |
|---|---|---|---|
| **TLA+ / TLC** (`tla/`) | a specification language plus a model checker that explores every interleaving of a protocol design | the commit protocol (`tla/commit`), and the lock-lease protocols for Hive and DynamoDB (`tla/lease`) | says nothing about whether the Java matches the model |
| **Spin / Promela** (`spin/`) | a model checker for concurrent processes | the object store: process death mid-write, truncated manifests, cleanup safety | same: it checks the design |
| **DST**, deterministic simulation testing (`dst/`) | runs Iceberg's **real** commit code against a fake catalog and fake storage, with a seeded scheduler that interleaves writers and injects faults. A failing seed replays exactly. | commits, transactions, branches, snapshot expiry, manifest rewrites, format v2/v3 | interleaves only at storage/catalog calls, so it cannot see a race between two lines of one method |
| **Stress harnesses** (`r5/races/`) | many real threads hitting the same real objects, checked against an oracle the code cannot influence | instruction-level races: the variant writer, the in-memory lock, counters, double-checked locking | probabilistic: a clean run is weak evidence |
| **Engine harnesses** (`r5/spark/`, `r5/flink/`) | local Spark 4.1 / 3.5 sessions and Flink 2.3 operator test harnesses on the released Iceberg runtime jars | Spark `add_files`, streaming writes, the executor cache; the Flink committers and maintenance | local mode only; no real cluster |
| **Catalog harnesses** (`r5/catalogs/`) | the real `JdbcCatalog` on SQLite with a fault-injecting JDBC driver; an embedded Hive Metastore 2.3.10 with Hive's own lock reaper | lost acknowledgements and lock-lease expiry | the Hive timeouts were scaled down from 300 s to 3 s |

**Calibration (recall).** The race harness fails on 1.11.0 (50/50) and passes on 1.12.0 (0/50) for
apache/iceberg#16686. DST catches 4 of 4 reinstated known bugs (`findings/r2-dst-harness.md` §2). The
TLA+ lease model reproduces the real Hive Metastore result before its DynamoDB variant is trusted. New
fault types were each checked against a code path that is known to handle them correctly.

## Results

### Major: data loss, corruption, wrong results, or a lost or duplicated commit

| # | What goes wrong, in plain words | Needs | Trigger vs control | Upstream |
|---|---|---|---|---|
| 1 | **Snapshot expiry deletes manifests that live snapshots still use, and reports success.** The default cleanup ignores a failed read of a kept snapshot's manifest list (`IncrementalFileCleanup`). | a manifest-list read that fails 4 times in a row | 20/20 vs 0/20; 2/40 at a 50% transient fault rate | no match found |
| 2 | **A transaction that appends and then rewrites manifests keeps stale sequence numbers**, so an equality delete committed concurrently is applied to the transaction's new rows. | a concurrent writer | 186/200 vs 0 | no match found |
| 3 | **Concurrent Avro writers swap variant column values silently.** A single JVM-wide writer shares one buffer. Affects Iceberg's generic record writer (e.g. Kafka Connect), not the Spark or Flink writers. | two threads writing v3 Avro files | 100/100 files wrong vs 0/100 | no match found |
| 4 | **Spark `add_files` deletes the manifests of a commit that actually succeeded**, and then every read fails. | the catalog's answer is lost after the commit lands | 10/10 vs 0/10 (Spark 4.1 and 3.5) | apache/iceberg#13443, closed by the stale bot, never fixed |
| 5 | **Spark streaming to a branch writes a replayed batch twice.** The duplicate check looks at `main`, not at the branch. | a driver crash between the Iceberg commit and Spark's commit log | 5/5 vs 0/5 on main | no match found |
| 6 | **Deleted rows come back in Spark** when a time-travel query and a current query share the executor cache across a schema change (int→long). The cache is on by default. | queries on two schema versions | 5–9/10 vs 0/10 | root cause apache/iceberg#15039 (open); this trigger is new |
| 7 | **Flink's equality-delete converter deletes the deletion-vector files its own successful commit uses.** It ignores the cleanup rule that core follows. | a non-retryable error after the commit lands | 20/20 vs 0/20 for core's own `RowDelta` | no match found |
| 8 | **JDBC catalog: after a connection drop, the retry silently undoes another writer's acknowledged commit.** | a dropped connection after the update applied, plus a concurrent writer | 20/20 on 5 operations vs 0/20 | apache/iceberg#18743 and PR #18745 (open) |
| 9* | **Hive catalog with locking on: a frozen writer overwrites a commit made after its lock expired.** No expected location is sent while the lock is held. | a freeze longer than `hive.txn.timeout` (default 300 s) | 10/10 vs 0/10 | same root cause as apache/iceberg#14096 (stale-closed); lock-free mode is the documented mitigation |
| 10* | **HadoopCatalog's in-process lock lets two committers in**, and commits reported successful are lost. | a heartbeat stalled for about 15 s (not configurable) | 68 of 4,000 lost vs 0 | no match found |

\* Conditional: needs a pause longer than a lock lease. Read these as "Major if it happens".

### The long-open defect from earlier rounds: cause now found

Two concurrent transactions leave one manifest recorded with **two different sequence numbers** in two
reachable snapshots. **Cause, confirmed by experiment:** when a transaction is retried, two caches that
are keyed by file path only (`ManifestFilterManager.filteredManifests` and
`SnapshotProducer.manifestsWithMetadata`) each return the first attempt's manifest object, which still
carries the old sequence number. Each cache masks the other: clearing either one alone changes nothing
(200/200), clearing both fixes it (0/200), and no new files are written in any case.
On format v3 the same stale object also gives two live files overlapping `_row_id` ranges (200/200).
This is shown at the metadata level only; no query was run against it.
See `findings/r5-open-defect-cause.md`.

### Minor

| id | What | Why it is Minor |
|---|---|---|
| A2-1 | A retried snapshot expiry also removes snapshots that its own retention policy keeps | nothing reachable is lost; time travel to them fails cleanly |
| A1-18 | A retried `RewriteFiles` fails with a spurious "Missing required files to delete" | no data changes; re-running succeeds |
| A2-28b | JDBC reports `CommitFailedException` for a commit that landed (single writer) | wrong report, no data loss |
| B-18 | the static heartbeat map in `LockManagers` is an unsynchronized `HashMap` | leaks heartbeat tasks |
| B-24 | concurrent `refresh()` can move HadoopTableOperations' cached version backwards | stale reads only; commits stay safe |
| B-133 / D-10 | lost increments in the rewrite commit service's success counter | can fail an action that actually succeeded; no data loss |
| C-54 | Azure SAS credential policy can throw an NPE on first concurrent use | rare crash, no data impact |
| E-33 | Flink maintenance coordinator: a `ConcurrentModificationException`, or one job stalling another's maintenance | no commit lost or duplicated |

### Not counted

- **E-4, Flink re-commits a checkpoint after snapshot expiry.** Reproduced 20/20 (duplicates or a
  crash loop), but maintainers declined a fix (apache/iceberg#18374) and treat snapshot retention as
  the operator's responsibility.
- **DynamoDB lock with Glue.** TLA+ shows a lost commit only if pauses are allowed. That is a design
  result only, since no AWS was available.
- **One security-relevant candidate is withheld** from this public copy and marked "redacted" in the
  inventory, pending a private report per Apache's security process.

## Where to read more

| File | Contents |
|---|---|
| `findings/r5-summary.md` | the table above, with tools and file pointers |
| `findings/r5-concurrency-inventory.md` | all 417 rows, routing, and the retry-surviving state of every `PendingUpdate` |
| `findings/r5-dst-core.md`, `r5-races.md`, `r5-spark.md`, `r5-flink.md`, `r5-catalogs.md` | each finding: exact commands, counts, Iceberg source lines, and what was **not** established |
| `findings/r5-duplicates.md` | the upstream duplicate search |
| `findings/r1`–`r4`, `iceberg-commit-tla.md`, `r3-spin-store.md` | earlier rounds: the TLA+ commit model, the Spin store model, the code sweep, and the DST harness |
| `PROJECT-NOTES.md` | the original TLA+ runner notes |

## Reproducing

You need JDK 17+; each `gw` script locates a JDK and forwards to the Gradle wrapper. Dependencies are
the released Iceberg jars from Maven Central, so no Iceberg build is needed.

```bash
cd dst && ./gw test                                              # 32 DST tests, ~6 min
cd dst && ./gw test --tests '*ExpiryReadFaultTest*'              # finding 1
cd dst && ./gw test --tests '*TxnRewriteManifestsTest*'          # finding 2
cd dst && ./gw test --tests '*MetastoreFlipTest.causeTable*'     # the open defect's cause
cd r5/races && ./jrun.sh 1.12.0 races.VariantRace concurrent 50 2 20000 fixed   # finding 3
cd r5/spark && ./gw test --tests r5spark.D31AddFilesLostAckTest  # finding 4 (also D2*, C55*)
cd r5/flink && ./gw test -Dr5.runs=20                            # finding 7 and E-4
cd r5/catalogs && ./gw test -Dr5.runs=20 && ./gw hiveTest -Dr5.runs=10           # findings 8, 9
tla/lease/run.sh all                                             # the lease models (needs TLC; see run.sh)
```

`r5/races/jrun.sh` and the TLA+ runners contain paths from the original machine; adjust them for yours.

## Lessons that shaped the method

- Suspect your own checker before Iceberg: six harness invariants were wrong at some point, each found
  by extending the harness, not by running more seeds.
- Measure what actually happened ("5,000 entries evicted"), not what you called ("invalidateAll()
  succeeded").
- Attribute faults by exception class and call site, never by message text.
- Unknown config names must throw; a silent fallback once produced a meaningless clean result.
- Breadth beats depth: the earlier rounds aimed three tools at 2,000 well-defended lines and found
  nothing new. This round widened the search to the whole repository and found most of the above in
  the engine and catalog modules.
