# Hunting concurrency and failure bugs in Apache Iceberg

This repository is an evidence-gathering exercise on [Apache Iceberg](https://github.com/apache/iceberg)
(main @ `9fffeb63b16`, released code tested = **1.12.0** from Maven Central). It combines formal methods
(TLA+, Spin), a deterministic simulation harness that runs Iceberg's real commit code, stress tests, and
engine-level reproductions in Spark and Flink.

**Status:** three findings have draft fix PRs open upstream (see [Upstream PRs](#upstream-prs)); nothing
else has been reported. Every "confirmed" finding was reproduced on
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

## Round 6: streaming ingestion, object-store drops, REST catalog

Three new tracks, each with the same rules (`r6/RULES.md`): calibrate on a fixed bug first, control runs,
released jars only.

| Track | Tools | Calibration (pre-fix release fails, fixed release passes) |
|---|---|---|
| **Kafka Connect sink** (`r6/kafka-connect/`) | TLA+ model of the coordinator protocol (`KcSink.tla`) + a harness driving the real released sink classes with an in-JVM fake Kafka and a deterministic schedule | #12372: 1.8.1 5/5 vs 1.9.0 0/5. #16282/#17340: 1.11.0 5/5 vs 1.12.0 0/5. The model agrees on both. |
| **Object store** (`r6/objstore/`) | real `S3FileIO` against an in-JVM S3 server (Adobe S3Mock) behind a fault proxy that cuts uploads, drops acknowledgements and closes reads early | PUT retry #5282: 1.0.0 10/10 vs 1.1.0 0/10. Read retry #10433/#11335: 1.6.1 10/10 vs 1.7.0 0/10. Planted damage caught 3/3 per kind. |
| **REST catalog** (`r6/rest/`) | DST with Iceberg's REST server in the commit loop and HTTP faults | **still running**, results not yet in |

### Round 6 Major findings

| # | What goes wrong, in plain words | Needs | Trigger vs control | Upstream |
|---|---|---|---|---|
| R6-1 | **Kafka Connect skips records after a cooperative rebalance.** When some partitions are revoked, the task throws away its buffer for all of them, then rewinds only the partitions that have a committed offset. A kept partition that has never committed silently skips what was buffered. | cooperative (ICR) rebalancing, a partition with no commit yet (new connector, new topic or partition) | 5/5 on 1.9.0–1.12.0 vs 0/5 (eager rebalancing; or after a first commit) | no match found |
| R6-2 | **A replacement Kafka Connect coordinator skips data-file events** written before it started, because it reads the control topic from `latest`. On 1.12.0 a failed first table commit now triggers this. | coordinator replaced before its first control-topic commit | 5/5 on 1.9.0–1.12.0 vs 0/5 (`earliest`) | open PR #18006 |
| R6-3 | **With the `urlconnection` HTTP client, one early-closed S3 read during a merge commit drops committed files from the table**, and the commit reports success. The client reports a short read as end-of-file and nothing checks the length. | `http-client.type=urlconnection` (not the default) and a connection closed mid-read | 5/5 (20/20 at random offsets) vs 0/5 with the default client (the commit fails cleanly) | root cause in open PR #15792 (framed as a read error); the data loss is unreported |

Round 6 Minors: S3 range reads (`readFully`/`readTail`) are never retried; a failed multipart start leaves a
truncated, unreferenced object; lost acknowledgements leak multipart uploads or replace the original error.
Refuted: with the default S3 client, 130 fault runs never left a referenced file missing or truncated; Kafka
Connect handles `CommitStateUnknownException`, coordinator crashes and repeated `CommitFailedException`
correctly on 1.11.0 and 1.12.0. Camel has no Iceberg component today, so Camel→Iceberg goes through
Kafka Connect or another writer.

Details: `findings/r6-kafka-connect.md`, `findings/r6-objstore.md`.

## Round 7: streaming ingestion, with Specula

Does data streamed from Kafka or Flink land in Iceberg exactly once? This round used the
[Specula](https://github.com/specula-org/Specula) workflow (code analysis, a TLA+ model checked against
traces from the real code, model checking, then reproduction of every suspect) on the Kafka Connect sink,
and the same steps by hand on the Flink sinks. New, real bugs include record loss and table corruption
during a rolling upgrade across 1.11, record loss when the control topic is re-created or has several
partitions, a bounded Flink job dropping a writer's last batch, and several ways ingestion silently stops
while Kafka lag looks healthy. The write-up, in plain words, with what was checked on released jars and
what was not: [`r7-streaming/README.md`](r7-streaming/README.md).

## Upstream PRs

Draft fix PRs, each with a regression test that fails on main and passes with the fix:

- [apache/iceberg#18762](https://github.com/apache/iceberg/pull/18762): Core: fail incremental cleanup when a retained manifest list cannot be read (Major #1)
- [apache/iceberg#18763](https://github.com/apache/iceberg/pull/18763): Spark 4.2: keep imported manifests when the commit state is unknown (Major #4)
- [apache/iceberg#18764](https://github.com/apache/iceberg/pull/18764): Core: don't share the Avro variant writer across threads (Major #3)

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
cd r6/kafka-connect/harness && ./run.sh 1.12.0 icr-nocommit 5    # R6-1 (control: add eager=1 or precommit=1)
cd r6/kafka-connect/harness && ./run.sh 1.12.0 coord-latest 5    # R6-2 (control: add reset=earliest)
cd r6/kafka-connect/harness && ./run.sh 1.8.1 icr-close 5        # calibration #12372 (then 1.9.0)
cd r6/kafka-connect/tla && ./run.sh all                          # the Kafka Connect model
cd r6/objstore && ./gw -q harness -Dos.mode=fin-merge -Dos.client=urlconnection -Dos.runs=5   # R6-3 (control: -Dos.client=apache)
cd r6/objstore && ./gw -q harness -Dos.mode=calib-read -PicebergVersion=1.6.1                # calibration (then 1.7.0)
```

The Kafka Connect runs keep their capped logs in `r6/kafka-connect/harness/logs/` and `tla/logs/` (the
scripts write to `out/`, which is not committed).

`r5/races/jrun.sh`, `r6/kafka-connect/*/run.sh` and the TLA+ runners contain paths from the original machine; adjust them for yours.

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
