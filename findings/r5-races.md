# r5-races: concurrency reproductions on released Iceberg jars

Harness: `r5/races/` (Gradle project, own wrapper). All results come from the real classes in
the Maven Central jars (`iceberg-core`/`-api`/`-data`/`-parquet`/`-orc`/`-azure`, 1.12.0, plus
1.11.0 for the gate). I did not modify, rebuild or check out anything in `iceberg/`.
`iceberg/` is at `main`. I diffed `ValueWriters`, `LockManagers`, `HadoopTableOperations`,
`BaseCommitService` and `VendedAzureSasCredentialPolicy` against the `apache-iceberg-1.12.0` tag
(`gh api .../contents/...?ref=apache-iceberg-1.12.0`, saved under `r5/races/ref/`). They are
byte-identical, so the line numbers below hold for both. `ManifestFilterManager` for 1.11.0 and
1.12.0 is in `r5/races/ref/MFM-*.java`.

**Tool used:** plain multi-threaded stress tests (CyclicBarrier start, or free-running threads),
not jcstress or Lincheck. The candidates are races between whole Iceberg operations (file write,
commit, refresh), not between single fields, and they need real files, catalogs and the
heartbeat scheduler. Neither jcstress nor Lincheck model-checking can run those as they are.
Every check compares against an oracle that does not depend on the code under test: expected
bytes, expected counts, and files that must still be live.

Reproduce everything: `cd r5/races && ./run-all.sh` (writes `build/r5/*.txt`, one line per run).
`./jrun.sh <icebergVersion> <mainClass> args...` runs one harness with plain `java`. It uses
the classpath that `./gw -q printClasspath` prints, and the same log flags as the Gradle `run`
task. The log level is `error`, and `InMemoryLockManager` is set to `off` because it logs one
ERROR per failed release. The harness counts those failures itself.

## Summary

| id | status | severity | one line |
|---|---|---|---|
| gate #16686 | **discriminates** | (known) | 1.11.0: 50/50 trials give a wrong `deleted-duplicate-files`. 1.12.0: 0/50. 1 thread on either version: 0/50 |
| B-122 | **confirmed** | **Major** | Two threads writing Avro variant columns: 100/100 files read back other rows' variant values with no error (fixed-length values). 97/100 files are unreadable and 3/100 writes throw (mixed lengths). Sequential control: 0/100 + 0/100 |
| B-19 / B-20 | **confirmed (mechanism + lost commits) under a heartbeat stall; not reproduced without one** | Major only under a ≥15 s heartbeat stall, otherwise Minor | With the heartbeat stalled past the timeout: mutual exclusion is broken about 8,800 times per 30 s, with B-19 and B-20 signatures seen directly. B-20 also removed a healthy owner's lock. Real HadoopTableOperations commits are lost in 10/10 runs (68 lost of 4000 reported successes). Healthy heartbeat: 0 violations (fast config and the production 3 s/15 s defaults) and 0 lost commits in 10/10 runs (fast config; the commit harness was not run at the production defaults) |
| B-18 (side finding) | confirmed (structural) | Minor | After every LockRace run, all locks released, the static `HEARTBEATS` HashMap reports sizes from -2 to 11 with only 2 keys in use. It is corrupted, even at production timeouts |
| B-24 | **confirmed (backward reads); refuted for commits** | Minor | Concurrent `refresh()` on one HadoopTableOperations moves cached `current()` back one version 12–38 times per 30 s (1-refresher control: 0). Commits stay safe: 0 lost, 0 read-your-writes misses, and no commit succeeded on stale metadata |
| B-133 / D-10 | **confirmed (lost increments)** | Minor | Real BaseCommitService, 8 offering threads, no-op commits: 20/20 trials undercount, 22,285 increments lost of 3.2 M. With real Iceberg commits: 0/40 trials (8000 commits). 1-thread control: 0 |
| C-54 | **confirmed** | Minor | Real policy in a real azure-core pipeline, provider shared as in ADLSFileIO: 4 NPEs at the unpublished `azureSasCredentialPolicy` field in 4 × 20,000 concurrent first-request trials (3 trials). Sequential control: 0 in 2 × 20,000 |

---

## Step 0: calibration gate, apache/iceberg#16686

**What #16686 fixed.** `ManifestFilterManager.filterManifests` (`core/.../ManifestFilterManager.java:212-236`)
filters manifests in parallel on `workerPoolSupplier.get()` (`:224`). In 1.11.0,
`filterManifestWithDeletedFiles` does `deleteFiles.add(fileCopy)` (a shared, non-thread-safe
`DataFileSet`) and `duplicateDeleteCount += 1` (a plain `int` field) from those worker threads. See
`ref/MFM-1.11.0.java:538, 545`. In 1.12.0 the count is a per-manifest `AtomicInteger` (`:509, 546`),
stored as `Pair<Set<F>, Integer>` per filtered manifest (`:571`). `deleteFiles.addAll(...)` now
runs once on the calling thread (`:232`).

**Harness:** `races.Calibration` drives the real path: InMemoryCatalog → format-v2 table →
32 `appendManifest` fast appends. Each manifest holds 2000 distinct data files, each written twice
(so 2000 duplicate deletes per manifest). Then
`table.newDelete().deleteFromRowFilter(alwaysTrue()).scanManifestsWith(pool).commit()`, which runs
StreamingDelete → MergingSnapshotProducer.apply → filterManifests. The oracle is the snapshot
summary: `deleted-data-files` and `deleted-duplicate-files` must each be 32 × 2000 = 64000.
Each trial makes exactly one commit attempt (asserted: exactly one new snapshot), which excludes
the separate retry double-count also fixed in #16686. The pool records how many of its tasks
actually ran at once (`minMaxOverlap`).

Command: `./gw -q run -PicebergVersion=<v> -Pmain=races.Calibration -Pargs="50 32 2000 <threads>"`

| version | pool threads | trials with wrong `deleted-duplicate-files` | observed range | wrong `deleted-data-files` | tasks overlapping (min of max) |
|---|---|---|---|---|---|
| 1.11.0 | 8 | **50/50** | 47,928 – 60,420 (expected 64,000) | 0/50 | 8 |
| 1.11.0 | 1 (control) | 0/50 | 64,000 | 0/50 | 1 |
| 1.12.0 | 8 | **0/50** | 64,000 | 0/50 | 8 |
| 1.12.0 | 1 (control) | 0/50 | 64,000 | 0/50 | 1 |

The gate passes. A plain-thread stress harness on the real operation path shows this
fixed race on the broken release, does not show it on the fixed release, and shows nothing when
single-threaded. So the zero counts later in this report mean something at this level of contention.
They are not proof that a race is absent. The harnesses run at most minutes, and a window of tens
of nanoseconds (C-54) needed about 80,000 trials before it showed up at all.

---

## B-122: VariantWriter.INSTANCE shares its buffer across the JVM (confirmed, Major)

**Source** (`iceberg/core/src/main/java/org/apache/iceberg/avro/ValueWriters.java`):
- `variants()` returns the singleton `VariantWriter.INSTANCE` (`:117-118`, `:445`). Each
  `DataWriter`/`BaseWriteBuilder` call gets the same object (`core/.../data/avro/DataWriter.java:107`,
  `core/.../avro/BaseWriteBuilder.java:70`).
- Its `VariantMetadataWriter`/`VariantValueWriter` extend `VariantBinaryWriter`, whose
  `reusedBuffer` is a plain instance field (`:387`). `serialize()` reallocates it or changes its
  limit (`ensureCapacity`, `:402-409`), writes into it, then resets position and limit (`:411-417`).
  `write()` passes it to `encoder.writeBytes` (`:398`). Nothing is synchronized.

**Harness:** `races.VariantRace`. Two threads each open
`Avro.writeData(outputFile).schema(...).createWriterFunc(org.apache.iceberg.data.avro.DataWriter::create)`,
the writer GenericAppenderFactory/GenericFileWriterFactory use for Avro. The table is format v3
with schema `(writer int, row int, v variant)`. After a CyclicBarrier, each thread writes 20,000
rows whose variant value is a deterministic function of (writer, row). Each finished data file is
appended to its own v3 table, so one broken file cannot hide another. It is then read back with
`IcebergGenerics.read(table)`. Each variant read back is re-serialized into a fresh buffer and
compared byte for byte with the expected value, also serialized into a fresh buffer.
Each file is classified: clean / reads without error but wrong values ("silent") / read throws / write threw.
- `fixed` shape: every value is a 64-character string (same encoded length).
- `varied` shape: string lengths from 1 to 3000 (some above the 2048-byte initial buffer), with every fifth value an int64.
- Control: the same harness with the two writers run one after the other on one thread.

Command: `./jrun.sh 1.12.0 races.VariantRace <concurrent|sequential> 50 2 20000 <fixed|varied>`

| shape | mode | files | clean | silently wrong | wrong rows in those | unreadable | write threw |
|---|---|---|---|---|---|---|---|
| fixed | concurrent | 100 | 0 | **100** | 51,337 of 2,000,000 | 0 | 0 |
| fixed | sequential (control) | 100 | 100 | 0 | 0 | 0 | 0 |
| varied | concurrent | 100 | 0 | 0 | – | **97** (62 `IllegalArgumentException` "STRING payload extends past buffer", 35 `ArrayIndexOutOfBoundsException`) | **3** (`DataFileWriter$AppendWriteException` ← `IndexOutOfBoundsException` in `ByteBuffer.put`) |
| varied | sequential (control) | 100 | 100 | 0 | 0 | 0 | 0 |

Example of a silently wrong row (fixed shape): the file written by writer 0 has, at row 12, the value
`"1:00000013:BBBB…"`, which is writer **1**'s row 13. It reads back without any error.

**Minimal sequence:** T1 `serialize(v1)` fills `reusedBuffer` → T2 `serialize(v2)` overwrites the
bytes and changes the limit → T1 `encoder.writeBytes(reusedBuffer)` writes v2's bytes, or a mix of both, into
T1's file. Equal-length values give a structurally valid but wrong variant (silent corruption). Different
lengths give an undecodable variant (the file is committed but cannot be read) or an exception during the write.

**Severity, Major:** silent data corruption. Committed files return the wrong variant values. Nothing
beyond two threads in one JVM writing Avro variant columns through Iceberg's generic `Record` writer is needed:
`org.apache.iceberg.data.avro.DataWriter`, registered for `Record`/AVRO in
`data/.../GenericFormatModels.java`, used by GenericFileWriterFactory and GenericAppenderFactory. A concrete
production caller is the Kafka Connect sink. `kafka-connect/.../connect/data/RecordUtils.java:137-150`
builds a `GenericFileWriterFactory` with the table's file format. `RecordConverter.java:601-711` builds
`ShreddedObject`/`ValueArray`/primitive variants, which are not `Serialized` and so take the shared-buffer
path at `ValueWriters.java:398`. Several sink tasks run in one Connect worker JVM. Scope: format-v3 tables with
a variant column and Avro data files (`write.format.default=avro`).

**Correction to the inventory:** Spark and Flink are **not** affected. `SparkAvroWriter` uses
`SparkValueWriters.variants()` (`spark/v4.2/.../SparkValueWriters.java:117-133`) and
`FlinkAvroWriter` uses `FlinkValueWriters.variants()` (`flink/v2.3/.../FlinkValueWriters.java:170-190`).
Both are their own singletons and delegate to the stateless `ValueWriters.bytes()`. They do not use
the shared-buffer `VariantBinaryWriter`.

**Not established:** I did not run Kafka Connect itself. I did not check which release introduced the
singleton (I did not run 1.11.0 for this). I did not check `InternalWriter`/`GenericAvroWriter` callers
for variant use.

---

## B-19 / B-20: InMemoryLockManager (HadoopCatalog/HadoopTables default lock)

**Source** (`iceberg/core/src/main/java/org/apache/iceberg/util/LockManagers.java`):
- Heartbeat (`:216-226`): `lastContent = LOCKS.get(id)`, then `LOCKS.replace(id, lastContent, new
  InMemoryLockContent(ownerId, …))`. It never checks that `lastContent` belongs to `ownerId` (B-19).
- release (`:254-273`): `get` (`:255`), owner check (`:261`), then an unconditional
  `HEARTBEATS.remove(id).cancel(false)` and `LOCKS.remove(id)` (`:270-271`). This is check-then-act (B-20).
- `HEARTBEATS` is a static unsynchronized `HashMap` (`:182`) (B-18).
- What a real catalog gets: `HadoopCatalog.java:120` and `HadoopTables.java:217` call `LockManagers.from(props)`.
  Without `lock-impl`, that returns the static `LOCK_MANAGER_DEFAULT` (`:40`), built from an empty
  map. I checked against the 1.12.0 jar: `from({lock.heartbeat-timeout-ms=10, …})` returns the
  default instance with timeout 15000, interval 3000 and acquire interval 5000. `lock-impl=…$InMemoryLockManager`
  throws `IllegalArgumentException` because there is no no-arg constructor. **So HadoopCatalog
  users cannot shorten these timeouts.** The harness builds InMemoryLockManager through its
  package-private `Map` constructor (by reflection) to choose a configuration.
- The lock protects only `exists(dst)` → `rename(src,dst)` in `HadoopTableOperations.renameToFinal`
  (`hadoop/HadoopTableOperations.java:363-387`). On a local filesystem `rename` overwrites an
  existing `dst`.

### Direct mutual exclusion (`races.LockRace`)

8 threads, 2 entity ids, 30 s per run, hold time 0–10 ms. Each holder increments a per-key
occupancy counter; any value above 1 is a violation. While holding the lock, each thread samples the real static `LOCKS` map
by reflection and classifies the recorded owner **by identity**:
- `null`: my entry was removed by someone else's release(). That is the B-20 signature, because only
  release() and close() remove entries and close() is never called.
- `exitedOwner`: the recorded owner's release() had already returned. Its heartbeat rewrote the entry
  afterwards, which is the B-19 signature.
- `liveOther`: another current holder took over after my lease expired.

Configurations (an unknown name throws):
- `prod`: 3000/15000 ms, the defaults every HadoopCatalog uses.
- `fast`: 2/20 ms.
- `stall`: interval 200 ms, timeout 5 ms. This models a heartbeat stalled past its timeout.
- `mixed`: even threads use a healthy 2/50 ms manager and odd threads the stalled one. Both share
  the static maps, which isolates B-20's effect on a holder whose own heartbeat is healthy.

Command: `./jrun.sh 1.12.0 races.LockRace <config> 8 2 30 10000` (3 runs each)

| config | acquisitions | violations (2+ inside) | max inside | release()=false | holds that saw own entry removed (B-20 sig.) | holds that saw entry rewritten to an already-released owner (B-19 sig.) | holds that saw a live other owner (expiry steal) | live locks left after quiesce | HEARTBEATS.size() after quiesce (2 keys) | thread errors | mixed: violations among healthy holders only / healthy holds whose entry was removed |
|---|---|---|---|---|---|---|---|---|---|---|---|
| prod | 10336 | 0 | 1 | 0 | 0 | 0 | 0 | 0 | 3 | 0 | – |
| prod | 10512 | 0 | 1 | 0 | 0 | 0 | 0 | 0 | 2 | 0 | – |
| prod | 10479 | 0 | 1 | 0 | 0 | 0 | 0 | 0 | 1 | 0 | – |
| fast | 10504 | 0 | 1 | 0 | 0 | 0 | 0 | 0 | 6 | 0 | – |
| fast | 10581 | 0 | 1 | 0 | 0 | 0 | 0 | 0 | 7 | 0 | – |
| fast | 10439 | 0 | 1 | 0 | 0 | 0 | 0 | 0 | 8 | 0 | – |
| stall | 14309 | 8882 | 3 | 7181 | 717 | 23 | 6764 | 0 | 11 | 1 | – |
| stall | 14429 | 8747 | 3 | 7175 | 667 | 213 | 6685 | 0 | 3 | 0 | – |
| stall | 14490 | 8915 | 3 | 7144 | 710 | 30 | 6746 | 0 | -2 | 0 | – |
| mixed | 11749 | 2753 | 3 | 2241 | 237 | 4 | 2125 | 0 | 3 | 0 | 1 / 0 |
| mixed | 11793 | 2839 | 3 | 2285 | 227 | 9 | 2156 | 0 | -1 | 0 | 1 / 1 |
| mixed | 11561 | 2486 | 3 | 1996 | 220 | 9 | 1883 | 0 | 2 | 1 | 0 / 0 |

Reading the table:
- `prod` and `fast` (a healthy heartbeat): **0 violations in 6 × 30 s** (about 63,000 acquisitions).
- `stall`: about 8,800 violations per 30 s and up to 3 holders at once. Most are expiry steals. The
  B-20 signature (holds whose own entry was removed by someone else's release()) appears about 700
  times per run. The B-19 signature (an entry rewritten to an owner whose release() had already
  returned) appears 23–213 times per run.
- `mixed`: here the healthy holders' own heartbeats run every 2 ms against a 50 ms timeout. Even so,
  two healthy holders were inside together 2 times in 3 × 30 s (1, 1, 0). Once, a healthy holder saw
  its own entry removed while it held the lock (0, 1, 0). That is B-20 acting on its own: a stalled
  owner's late release() deleted a healthy owner's lock, and another healthy owner then acquired.

### User-visible effect: lost commits through real HadoopTableOperations (`races.HadoopLockCommit`)

4 threads each own a `HadoopTableOperations` (the real class, via its protected constructor; a
subclass adds nothing). All work on one local-FS table and share one lock manager. Each thread
makes 100 fast appends of a uniquely named data file, with commit retries effectively unlimited. A
commit that returns has been reported successful. Fast appends never remove files, so each such
file must be live in the final table. Lost = reported successful but absent. A delegating
`LockManager` wrapper counts how many holders are inside per entity (`lockOverlaps`). It does not
change any decision.

Command: `./jrun.sh 1.12.0 races.HadoopLockCommit <stall|fast> 4 100` (10 runs each). Here
`stall` means interval 200 ms and timeout 1 ms; `fast` means 2/50 ms.

| config | runs with a lost commit | lost / reported successful | lock overlaps (sum) | commit exceptions |
|---|---|---|---|---|
| stall | **10/10** | **68 / 4000** (3, 7, 8, 8, 4, 9, 4, 5, 12, 8) | 356 | 0 |
| fast (control) | 0/10 | 0 / 4000 | 0 | 0 |

In every lost case, `commit()` returned normally and the snapshot count in the final metadata equals
the number of live files. The losing commit's `vN.metadata.json` was overwritten by the second
holder's rename. Its snapshot is gone from history, and nothing reported an error.

**What this does and does not show:**
- Confirmed: on the real 1.12.0 class, a heartbeat that runs later than `heartbeat-timeout-ms`
  lets a second owner in. The B-20 signature (a held entry removed by someone else's release,
  `null` samples) and the B-19 signature (an entry rewritten to an owner that had already released,
  `exitedOwner` samples) both appear. The result is lost commits through real HadoopTableOperations.
- Not reproduced with a healthy heartbeat: in `prod` (the only configuration a HadoopCatalog can
  have) and in `fast`, there were 0 violations in 3 × 30 s each and 0 lost commits in 10 runs. Each
  breach needed at least one owner whose lease had expired. B-20 makes this worse: in `mixed`, one
  expired owner's release() removed a *healthy* owner's lock, and 2 overlaps between healthy owners
  followed. So one expired lease can break exclusion for owners whose leases are fine.
- Production reachability: with the fixed 3 s/15 s defaults, a breach needs the heartbeat
  to be late by about 15 s while a commit is between acquire and release. Causes would be a long GC
  pause or a frozen or suspended JVM or container. Contention on the shared 4-thread `scheduler()` would
  also need to last that long. I did not produce such a pause with HadoopCatalog itself (no SIGSTOP experiment).
- Severity: **Major (lost commit) under a ≥15 s heartbeat stall during a commit. Minor without one.**
  The leftovers then are the stale-owner rewrite (B-19), which can keep a `vN.metadata.json` lock alive
  and make stale committers for that version wait for the 3-minute acquire timeout, and B-18. This is
  a lease lock without fencing, so any lease longer than the pause is safe. The defect is that
  release and the heartbeat do not check who owns the entry, which widens one expiry into further breaches.

### B-18 side finding: HEARTBEATS map corruption

At the end of every LockRace run, all locks are released (`releaseFalse=0` in prod/fast) and `LOCKS`
is empty. Yet `HEARTBEATS.size()` is 3, 2, 1 (prod) and 6, 7, 8 (fast), and **-2 and -1** in a stall run and
a mixed run, with only 2 keys ever used. A correct HashMap cannot report more entries than it has
distinct keys, nor a negative size, and after every successful release it should hold 0 entries. This is concurrent `put`/`remove` on the static `HashMap` (`:182`, `:210-214`,
`:270`). The consequences I can name are leaked heartbeat futures, which die on their next run with the
caught NPE (`:224-225`), and one thread failure (`errors=1`, class not recorded) in a stall run.
Minor: no data effect was shown.

---

## B-24: concurrent HadoopTableOperations.refresh() (confirmed regress; commits safe)

**Source** (`iceberg/core/src/main/java/org/apache/iceberg/hadoop/HadoopTableOperations.java`):
`refresh()` (`:102-128`) is unsynchronized. It reads `version` (`:103`), scans forward for newer
files, then calls `updateVersionAndMetadata(ver, …)`, which applies any value `!=` the current one (`:94`),
not only a greater one. Then it sets `shouldRefresh = false` (`:123`). `commit()` sets `shouldRefresh = true` (`:171`).

**Harness:** `races.RefreshRace`. One shared ops object (the real class; a subclass only counts
`commit()` calls and their outcome by exception class) on a local-FS table. R refresher threads loop
`ops.refresh()`. Two reader threads loop `ops.current()`, which is what `BaseTable.currentSnapshot()`
uses, and each checks that the version it sees never decreases. An independent writer advances the
table through its own HadoopTables instance. Optionally, a committer appends through the shared table and
checks read-your-writes right after each commit. A `LocalFileSystem` subclass registered as
`fs.file.impl` adds a random 0..2 ms delay to each `getFileStatus`, modelling HDFS/object-store stat
latency. It changes timing only.

Command: `./jrun.sh 1.12.0 races.RefreshRace <refreshers> <seconds> <statLatencyMaxMicros> <sharedCommitter on|off>`

| refreshers | stat latency | shared committer | seconds | external commits | regress episodes seen by `current()` readers (reads) | max regress | shared-ops commit attempts: ok / CommitFailedException | read-your-writes misses | lost |
|---|---|---|---|---|---|---|---|---|---|
| 4 | 0–2 ms | off | 30 | 288 | **38** (380,532 reads) | 1 version | – | – | 0 |
| 1 (control) | 0–2 ms | off | 30 | 267 | 0 | 0 | – | – | 0 |
| 4 | none | off | 30 | 300 | **12** (20,099 reads) | 1 version | – | – | 0 |
| 4 | 0–2 ms | on | 60 | 351 | **29** (244,028 reads) | 1 version | 248 / 35 | 0 | 0 |
| 0 (control) | 0–2 ms | on | 60 | 304 | 0 | 0 | 257 / 123 | 0 | 0 |

With a single refresher, `current()` never went backwards in 30 s. With 4 concurrent refreshers it
went back one version 12–38 times per 30 s, with or without the stat latency. `refresh()`'s own
return value never went backwards (0 in all runs), because each refresh scans forward from the
regressed version. Only cached `current()` reads (BaseTable.currentSnapshot(), scan planning) see
the regression.

**Minimal sequence:** T1 reads version 5 and finds no v6 → v6 appears → T2 scans, finds v6, and calls
`updateVersionAndMetadata(6)` → T1 calls `updateVersionAndMetadata(5)`, and `5 != 6` re-reads v5
→ `current()` returns v5 until the next refresh.

**Commits:** a commit on the regressed state uses `nextVersion = 6`. `renameToFinal` then finds `v6`
already there and throws CommitFailedException (`:368-376`), and the retry refreshes forward. Nothing
succeeded on stale metadata: 0 lost files, and 0 read-your-writes misses in 248 shared commits. The
`shouldRefresh=false`-overwrites-`true` race (`:123` vs `:171`) never showed up as a stale read after an
own commit. I measured no extra commit failures from the regression either. The run with 4
refreshers had fewer CommitFailedExceptions (35) than the 0-refresher control (123), because the
refreshers keep the cache fresh against the external writer. Any extra retries the regression causes
are lost in that effect.

**Severity, Minor:** stale and backward reads of `current()` from a shared table object (one version
back, until the next refresh), plus extra commit retries. No data loss and no wrong commit. A reader
that plans a scan from the regressed `current()` reads a snapshot one version old, which is
snapshot-consistent data that was current a moment earlier.

---

## B-133 / D-10: BaseCommitService.succeededCommits++ (confirmed lost increments, Minor)

**Source:** `iceberg/core/src/main/java/org/apache/iceberg/actions/BaseCommitService.java:62`
(`private int succeededCommits`) and `:230` (`succeededCommits++`). The increment runs in
`commitReadyCommitGroups`, which `offer()` calls on each rewrite-pool thread (`:145-151`). Its consumer is
`spark/v4.2/.../RewriteDataFilesSparkAction.java:363`
(`failedCommits = totalCommits - succeededCommits()`).

**Harness:** `org.apache.iceberg.actions.CounterRace`. This is the real abstract BaseCommitService from the
1.12.0 jar with a minimal subclass. The subclass lives in that package only to reach the package-private
constructor. T threads `offer()` groups concurrently with `rewritesPerCommit=1`. The oracle is the number of
`commitOrClean` calls that returned, asserted equal to `results().size()`. `noop` does no work in
`commitOrClean`. `commit` does a real Iceberg fast append on an InMemoryCatalog table.

Command: `./jrun.sh 1.12.0 org.apache.iceberg.actions.CounterRace <noop|commit> <threads> <groupsPerThread> <trials>`

| work | threads | trials undercounting | lost increments / successful commits |
|---|---|---|---|
| noop | 8 | **20/20** | **22,285 / 3,200,000** |
| noop | 1 (control) | 0/20 | 0 / 400,000 |
| commit (real Iceberg commit) | 8 | 0/40 | 0 / 8,000 |
| commit | 1 (control) | 0/10 | 0 / 2,000 |

**Severity, Minor:** the counter undercounts, so `failedCommits` overcounts. With the default
`partial-progress.max-failed-commits` (= `max-commits`), `failedCommits > maxFailedCommits` cannot
hold, so the only effect is a spurious WARN. With a lower user setting, the action can throw
"more than the maximum allowed failures" after its commits have already landed. No data is lost.
With real commit latency, two increments rarely collide (0 in 8000).

---

## C-54: VendedAzureSasCredentialPolicy publish order (confirmed, Minor)

**Source:** `iceberg/azure/src/main/java/org/apache/iceberg/azure/adlsv2/VendedAzureSasCredentialPolicy.java:33-34, 60-78`.
Inside the monitor, the volatile `azureSasCredential` is assigned (`:68`) before the plain
`azureSasCredentialPolicy` (`:69-70`). A second thread that sees the credential set skips the
monitor, calls `update`, and dereferences the policy field in `processSync` (`:57`) or in
`process` (`:48-50`). The window lies in program order (it needs no memory reordering) and lasts
one `AzureSasCredentialPolicy` constructor call.

**Harness:** `races.SasPolicyRace`. A fresh real policy (built by reflection) per trial sits in a real
azure-core `HttpPipeline` (`HttpPipelineBuilder`, `sendSync`) with a stub transport. The SAS token comes
from `adls.sas-token.<account>` properties, so no HTTP is made. W threads send their first request at a
barrier. A failure is attributed structurally: an NPE whose top frame is
`VendedAzureSasCredentialPolicy.processSync`, whose only dereference is the plain policy field. The first
shared run below matched on the class only. Each success must carry the SAS signature in the URL.
`fresh` builds a new provider per trial, so the token is fetched under the provider's
`SimpleTokenCache`. `shared` reuses one provider with its token already cached and makes a new policy per
trial, which is how `ADLSFileIO.buildClient` creates them: one provider per FileIO, one new policy per
new (host, container) client (`azure/.../ADLSFileIO.java:143-158`).

Command: `./jrun.sh 1.12.0 races.SasPolicyRace <concurrent|sequential> <trials> 4 <fresh|shared>`

| provider | mode | runs × trials × threads | trials with NPE at the unpublished field | NPEs | other failures |
|---|---|---|---|---|---|
| fresh | sequential (control) | 1 × 200 × 2 | 0 | 0 | 0 |
| fresh | concurrent | 1 × 2000 × 4 | 0 | 0 | 0 |
| shared | concurrent | 4 × 20000 × 4 | **3** (1, 0, 2, 0 per run) | **4** | 0 |
| shared | sequential (control) | 2 × 20000 × 4 | 0 | 0 | 0 |

The rate is about 4 in 320,000 requests (about 3.8 per 100,000 first-request races). The first
shared run happened while another harness was loading the CPU, and the hits may depend on preemption.

**Severity, Minor:** the first concurrent request(s) on a newly built ADLS client throw NPE out
of the Azure pipeline, so that one I/O operation fails. No wrong data is written or read, and a
retry succeeds once the field is published. #13276 fixed the same shape in S3FileIO/GCSFileIO. I did
not run the pre-#13276 calibration for that class.

## What I did not establish (all candidates)

- No Kafka Connect run (B-122's production caller) and no Spark run (D-10's Spark action). No multi-JVM experiments.
- B-19/B-20: no real ≥15 s pause injected into a HadoopCatalog commit. Production reachability of the
  lost commit rests on the stall-config experiment plus the code path, not on a production-config run.
- B-24: no run with the default 4 commit retries to see whether regressions ever exhaust them. Not tested on HDFS or object stores (local FS plus injected stat latency only).
- C-54: the rate is low (about 1 in 27,000 trials) and may depend on load. The `process` (async) path was not exercised.
- B-122: pre-1.12.0 releases not checked.
