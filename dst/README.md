# Deterministic simulation testing for the Iceberg commit path

Runs the **real** `org.apache.iceberg` commit code — `SnapshotProducer`,
`MergingSnapshotProducer`, `BaseRewriteFiles`, `BaseOverwriteFiles`,
`BaseReplacePartitions`, `BaseRowDelta` — against a fake `Catalog`/`TableOperations` and a fake
`FileIO`, with several writers interleaved by a seeded scheduler and faults drawn from the same
seed. A seed fixes the whole run, so a failing seed replays byte for byte.

Nothing in the Iceberg source tree is touched: the harness depends on **released artifacts from
Maven Central** (`iceberg-api`, `iceberg-core`), and runs against any release from 1.8.1 to 1.12.0.

## Running it

A JDK 17 or newer is all that is needed; `./gw` finds one and forwards to `./gradlew`.

```bash
./gw test                             # every test, 500 seeds per sweep (~30 s)
./gw test -Ddst.seeds=5000            # a longer sweep
./gw test -Ddst.seed=53               # replay one seed, full log into build/dst/
./gw test -PicebergVersion=1.10.2     # against another release
```

Outside the test runner:

```bash
./run.sh 53                           # one seed, default faults
./run.sh 53 aggressive                # one seed, every fault the harness has
./run.sh sweep 1000 default           # a sweep, with statistics
./run.sh sweep 1000 seqahead          # a sweep with the sequence-number hazard turned on
./run.sh sweep 1000 dupdelete         # the duplicate-delete workload (apache/iceberg#16686)
./run.sh sweep 1000 expiry-quiet      # snapshot expiry concurrent with writers
./run.sh sweep 500 orphan0            # orphan cleaning with no age cutoff
./run.sh sweep 500 orphan200          # orphan cleaning with a cutoff past the window
ICEBERG_VERSION=1.11.0 ./run.sh sweep 1000 dupdelete   # the same, on pre-fix bytecode
ICEBERG_VERSION=1.10.2 ./run.sh sweep 1000 default
./scorecard.sh                        # every recall row -> build/scorecard.txt
./versions.sh                         # every release -> build/versions.txt
```

## r5 additions (findings/r5-dst-core.md)

```bash
./gw test --tests '*ExpiryReadFaultTest*' -Ddst.r5.readfault.seeds=20   # A2-9, manifest-list read fault
./gw test --tests '*ExpiryRetentionTest*'                              # A2-1, retention invariant R
./gw test --tests '*TxnRewriteManifestsTest*' -Ddst.r5.txnrewrite.seeds=200   # A1-29
./gw test --tests '*RequiredDeletesTest*' -Ddst.r5.requireddeletes.seeds=200  # A1-18
./gw test --tests '*MetastoreFlipTest.causeTable*' --tests '*MetastoreFlipTest.v3FirstRowId*'
```

New, all off by default: a seeded manifest-list read fault (`FaultConfig.readFailManifestListPct`,
scoped by call stack to cleanup reads of retained snapshots, counted per Iceberg call site), expiry
by policy (`expireRetainLast`, `expireIncrementalCleanup`), ManageSnapshots and transaction actions,
`ScriptRun` for pinned per-fiber scripts, and `FilterCacheProbe.setMode` plus a `rewrite` group.

## What is faked, and where the line is

| Class | Stands in for | Faults it can produce |
|---|---|---|
| `FakeTableOperations` | a catalog with an atomic compare-and-set pointer (REST, Hive, Glue, Nessie) | reject; land then report the answer lost; report the answer lost without landing; a stale `refresh()` |
| `FakeFileIO` / `FakeStore` | an object store | fail before any byte; store a prefix then throw; store every byte then throw (lost acknowledgement); store a prefix and report success; fail a delete |

Faults sit on the files **Iceberg itself** writes: manifests, manifest lists, the metadata object.
Data and delete files are written by the engine, not by `iceberg-core`, so the workload writes them
through a fault-free view; faulting them would only abort writers before they reach the code under
test.

`writeSilentTruncatePct` (a prefix stored, `close()` returning success) is the one fault that needs
a store weaker than S3's single-shot `PUT`. It is off by default, and any violation it causes is
marked **excused**, because no code in Iceberg can keep a table readable after the store has lost
bytes it acknowledged. Only unexcused violations count.

## Invariants

| | Statement |
|---|---|
| I0 | the fakes agree with their own ledger (a failure here is a harness bug, and must fail loudly) |
| I1 | nothing a reachable snapshot points at is missing, and nothing is shorter than the length recorded for it |
| I2 | a writer told its commit succeeded finds its snapshot in the ancestry of the pointer |
| I3 | a writer's payload is applied at most once, including after `CommitStateUnknownException` |
| I4 | what a snapshot showed when it was created is what it still shows at the end |
| I5 | a file is deleted only once nothing can reach it any more (the retention rule) |
| I6 | the harness was in control: no unowned thread crossed the boundary, nothing hung |
| I7 | a row an equality delete removed never comes back |
| I8 | no file carries a data sequence number greater than the snapshot a reader finds it in |
| I9 | a snapshot that deleted no data file does not claim to have deleted a duplicate of one |

## The scheduler

Writers run on real threads, so the real Iceberg code runs with its real locks and
`ThreadLocal`s. Every call across the fake boundary parks the thread; the scheduler waits until
every live writer is parked, then releases one chosen by the seeded RNG. Exactly one writer runs at
a time and the sequence of choices is a function of the seed. The unit of interleaving is one
fake-boundary call, so a race between two lines inside a single Iceberg method is **not** explored;
that is the job of the TLA+ and Promela models in this project, or of jcstress.

Two details the harness had to get right, both of which cost a debugging round:

- **No lock may be held across a park.** The first `FakeTableOperations` parked inside its own
  `synchronized` method and deadlocked every run. The pointer swap now happens in a short block
  that cannot park, which is also what makes it atomic.
- **No RNG bound may depend on a byte count, and nothing random may reach the trace.** Iceberg
  names manifests after a `UUID.randomUUID()` and writes Avro with the gzip codec, so those random
  bytes change the *compressed* length of a manifest list between two runs of one seed.
  `CommitUuid` replaces that UUID with a counter by reflection, and the truncation fault draws a
  percentage rather than `nextInt(byteCount)`.

## Known limits

- Instruction-level races. The thread-safety half of apache/iceberg#16686 needs a genuinely
  parallel worker pool, which this harness replaces with a same-thread executor on purpose. Its
  other half, a duplicate-delete count carried across commit retries, is reachable and is I9.
- Releases before 1.12.0 have no `SnapshotUpdate.writeManifestsWith`, so manifest writes run on
  Iceberg's own worker pool. With `-Diceberg.worker.num-threads=1` they are serialised inside the
  turn of the writer that submitted them, but the harness cannot interleave another writer inside
  one manifest-write batch. The escape counter reports this and the violation is excused.
- One table, one branch, two partitions, three to five writers, format version 2.
- Snapshot expiry runs the real `RemoveSnapshots`. Orphan cleaning is part real and part model: the
  reachability decision is real Iceberg code (`ReachableFileUtil` plus the real manifest readers),
  the directory listing and the age cutoff stand in for the Spark action, which is not in
  `iceberg-core`.
- No `RewriteManifests`, no delete-file compaction, no REST catalog in the loop.
- Instruction-level races are out of reach by design (see "The scheduler").

The log is capped: `Trace` keeps the last 200 decisions in memory while the SHA-256 covers every
decision ever made, and the Gradle test task does not copy stdout into the report. That is a direct
lesson from the Camel harness, where a looping test once wrote a 31 GB log and filled the disk.
