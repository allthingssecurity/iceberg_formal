# r2: a deterministic simulation harness for the Iceberg commit path (2026-11-02)

Harness: `dst/`, about 3,100 lines of main code and 400 of tests. It depends on **released
artifacts from Maven Central** (`org.apache.iceberg:iceberg-api`, `iceberg-core`) and runs against
any release from **1.8.1 to 1.12.0**; the headline numbers are 1.12.0, the current release.
Nothing in `iceberg/` was modified; that checkout was used only to read source and to look up the
API contracts quoted below.

## Summary

For a reader who wants the result without the method.

**One defect, confirmed and reproducible.** Two threads, each running one Iceberg `Transaction` with
two operations, no injected faults: afterwards two *reachable* snapshots of the same table resolve
the same data file to two different `data_sequence_number` values. Since that number decides which
equality deletes apply to a file's rows, two readers of one table see different rows. Iceberg
accepts the resulting metadata as valid — `lastSequenceNumber` monotone, every snapshot in bounds,
`TableMetadata.buildFrom(...).build()` and a `TableMetadataParser` round-trip both pass — on
**300 of 300 seeds**, 0 rejected. It reproduces identically under Iceberg's real
`BaseMetastoreTableOperations`, so the harness's fake catalog is excluded. Iceberg 1.12.0.
`./gw test --tests '*MetastoreFlipTest*'`.

**A second, smaller observation.** `TableMetadata.java:1256-1261` exempts a snapshot with no parent
from the "sequence number must exceed the last" check (`|| snapshot.parentId() == null`). This needs
no concurrency and contradicts the assumption in PyIceberg `iceberg-python#24` that Java rejects
that case. Independent of the above; §6.

**Cause: not established.** Three mechanisms were proposed and each refuted by direct experiment
(§6). What is known: the field is `sequence_number`; the manifest is correct in the snapshot that
added it and stale in the next snapshot of the same transaction; both manifest lists involved were
written in the *second* commit attempt, so it is not a stale file; and emptying three path-keyed
caches at the moment the base moves changes nothing, while forcing a manifest rewrite to new paths
fixes it. No fix is proposed, and the one candidate on the table is expected not to work.

**Everything else was clean.** 0 unexplained violations in roughly 54,000 runs across five releases,
four fault profiles, snapshot expiry, manifest rewriting, transactions, branches, cherrypick,
fast-forward, rollback and format v3 with deletion vectors. Recall against known bugs: 4 of 4
behaviours that could be put back are caught, one of them against real pre-fix bytecode (§2).

**The caveat that matters.** Six of the harness's own invariants were wrong at some point during
this work, each found by extending the harness rather than by running more seeds. A clean sweep says
the invariants written did not fire. Nothing here was filed upstream.

Round 1 (`findings/r1-code-hunt.md`) ended with: reading the code cannot find the remaining
defects, because the protocol-level behaviour is correctly guarded. This round runs the code.

**Result, stated plainly: the harness finds no defect that a correct caller can reach.** Over
1,000 seeds per fault profile on five releases, and 5,000 seeds per profile on the current release,
every invariant holds, and every violation that does appear is fully explained by a fault the
harness itself injected. No Major, no new Minor in the commit protocol.

It does find one thing, and it is the hazard round 1 had already identified by reading (`M1`), now
demonstrated rather than argued: `RewriteFiles.dataSequenceNumber(long)` — public, not deprecated,
with no in-tree caller — accepts any value with no precondition, and a value ahead of the table's
sequence number both switches off equality-delete conflict detection and writes metadata in which
a data file is ordered after deletes that do not exist yet. It reproduces on **406 of 1,000 seeds
with no injected faults at all**, on concurrency alone. That is a missing precondition on an API,
not a defect correct callers hit, so it stays a Minor — the same verdict round 1 gave it, now with
a seed and a trace behind it.

The evidence that the harness can see anything is the recall scorecard below. The sharpest row
needs no switch at all: resolve the previous release instead of the current one and the harness
fails on **1,000 of 1,000 seeds**, with the same trace hash, on a bug Iceberg fixed in 1.12.0
(apache/iceberg#16686). That is recall measured against real pre-fix bytecode rather than against
an imitation of it.

## 1. What the harness does

Several writer threads run the real `SnapshotProducer` and its subclasses — `MergingSnapshotProducer`,
`BaseRewriteFiles`, `BaseOverwriteFiles`, `BaseReplacePartitions`, `BaseRowDelta` — against:

- `FakeTableOperations`: a catalog whose `commit(base, metadata)` is an **atomic compare-and-set**
  on one in-memory pointer, which is what a REST, Hive, Glue or Nessie catalog gives you. It keeps
  the whole metadata history so the invariants can walk ancestry afterwards. It can reject a
  commit, land it and then report the answer lost (`CommitStateUnknownException`), report the
  answer lost without landing, or serve a stale `refresh()`.
- `FakeFileIO` over an in-memory `FakeStore`: every `newOutputFile(...).create()` can succeed, throw
  before any byte, store a prefix and throw, store every byte and throw (a lost acknowledgement),
  or store a prefix and report success. Every write, delete and byte length goes into a ledger.

The bytes are real. The real `ManifestWriter` writes real Avro into the store and the invariants
read it back with the real `ManifestFiles.read`, so a truncated object produces a real Avro decode
failure, not a simulated one.

A seeded scheduler puts the writers into one total order: every call across the fake boundary parks
the thread, and the scheduler waits until every live writer is parked before releasing one chosen by
the seeded RNG. Faults are drawn from the same RNG. Each writer picks one of `append`, `rewrite`,
`overwrite`, `replace-partitions` or `row-delta` (position or equality deletes) from the seed, and
draws whether to call the `validate*` methods — which is how the round-1 Minor M2 (the Spark
overwrite paths validate nothing when `isolation-level` is unset) gets exercised from both sides.

Iceberg's own worker pools are injected with a same-thread executor through `scanManifestsWith` and
`writeManifestsWith`, so on 1.12.0 no thread the harness does not own ever crosses the boundary.

### Invariants

| | Statement | Unexcused violations on 1.12.0 |
|---|---|---|
| I0 | the fakes agree with their own ledger | 0 |
| I1 | nothing a reachable snapshot points at is missing, and nothing is shorter than the length recorded for it | 0 |
| I2 | a writer told its commit succeeded finds its snapshot in the ancestry of the pointer | 0 |
| I3 | a writer's payload is applied at most once, including after `CommitStateUnknownException` | 0 |
| I4 | what a snapshot showed when it was created is what it still shows at the end | 0 |
| I5 | a file is deleted only once nothing can reach it any more (the retention rule) | 0 |
| I6 | the harness was in control: no unowned thread crossed the boundary, nothing hung | 0 |
| I7 | a row an equality delete removed never comes back | 0 as a faithful caller; see §4 |
| I8 | no file carries a data sequence number greater than the snapshot a reader finds it in | 0 as a faithful caller; see §4 |
| I9 | a snapshot that deleted no data file does not claim to have deleted a duplicate of one | 0 on 1.12.0; **1000/1000 on 1.11.0 and earlier**, see row G |

I1 is the one the task most wanted exercised. It is reached: in the aggressive profile the store
silently loses the tail of a closed write, and I1 then fires on about 10% of seeds — every time
attributable to that fault and therefore excused, because no code in Iceberg can keep a table
readable after the store has lost bytes it acknowledged. That attribution is structural: the
reader records *which* file failed, rather than matching on the text of the exception. An earlier
version matched on the message, and that produced an apparent 12-of-200 cross-version signal on
1.8.1 that was entirely my own bug — older Iceberg puts the `InputFile`'s `toString()` in the
message where newer Iceberg puts the path. Worth recording as the kind of false positive this
method produces.

### Determinism

`DeterminismTest` asserts, and `./gradlew test` runs:

- 40 seeds, each run twice: byte-identical decision log and identical SHA-256, identical writer
  outcomes, identical violations.
- seed 7 replays to the same SHA-256 after 200 other seeds have run in the same JVM — the check
  that no state leaks between runs.
- 40 seeds produce more than 30 distinct schedules, so the search is not collapsing.

Two things had to be fixed to get there, and both are worth passing on:

- **A lock must never be held across a park.** The first `FakeTableOperations` parked inside its own
  `synchronized` method; every run deadlocked. The pointer swap now happens inside a short block
  that cannot park, which is also exactly what makes it atomic.
- **Nothing random may reach a decision.** Iceberg names manifests after a `UUID.randomUUID()` and
  writes Avro with the gzip codec, so those random bytes changed the *compressed* length of a
  manifest list between two runs of one seed. Because the truncation fault originally drew
  `nextInt(byteCount)`, one byte of difference sent the rest of the run down another path.
  The fault now draws a percentage, and `CommitUuid` replaces the random UUID with a counter by
  reflection so manifest paths and byte lengths replay exactly.

### Self-checks

A harness that reports nothing is indistinguishable from one that cannot see anything. `SelfCheckTest`
breaks the fakes on purpose and insists the invariants notice:

| Deliberate bug | Caught |
|---|---|
| the catalog's compare-and-set accepts a stale commit (a lost update) | yes, on every seed |
| one manifest of a committed snapshot is deleted after the run | 50 of 50 seeds |
| one data file of a committed snapshot is dropped | 50 of 50 seeds |
| no faults at all: nothing may be violated, and every successful writer's files must be live | holds on 100 of 100 seeds |

I0 is the standing version of the same idea: it compares the store against the fakes' own ledger
every run, so a fake that stops recording what it did fails loudly instead of turning every clean
sweep into a false negative.

### The one false alarm, and what it cost

Raising the sweep from 1,000 to 5,000 seeds turned up seed 3205, which I7 reported as a defect on
the faithful workload. It was not one; the invariant's premise was too loose, and it is worth
recording in full because this is the failure mode that makes a harness worse than no harness.

```
snap 1000 (seq 1)  seed append: data3, data4 at sequence number 1
snap 1001 (seq 2)  w3 commits equality delete del1, sequence number 2
snap 1003          w1 rewrites data3 -> data1, data2, supplying dataSequenceNumber(2)
```

w1 planned its rewrite against snapshot 1001, which already contained `del1`, and supplied that
snapshot's sequence number — exactly what `use-starting-sequence-number` is specified to do. I7
then saw output files at sequence number 2 alongside a delete at sequence number 2 and called the
rows resurrected. But a compaction that starts *after* a delete is visible is supposed to apply
that delete while rewriting, so those rows are not in the output at all, and the equal sequence
number is correct.

I7 now also requires the delete to be **newer than the snapshot the rewrite planned against**,
which is the condition that makes the premise true. Seed 3205 is clean, with the same trace
SHA-256 — only the invariant changed, not the run — and the sequence-ahead sweeps are unchanged
(406 of 1,000, I8 1,316 occurrences and I7 90). An invariant that fires on correct behaviour will
be switched off by the first reviewer who checks one, so the tightening matters more than the
finding it removed.

One more practical lesson, this one about the log rather than the logic: Iceberg's `Tasks` writes a
WARN with a full stack trace on every commit retry, and the harness forces thousands of retries.
At `defaultLogLevel=warn` a 500-seed run put 16 MB of captured stderr into the JUnit XML — Gradle
writes it to disk even with `showStandardStreams = false`. With log level `error` and
`reports.junitXml { includeSystemOutLog = false; includeSystemErrLog = false }`, a 5,000-seed run
writes 3.3 KB. The Camel harness's 31 GB test log was the same mistake.

## 2. Recall scorecard

Same shape as `camel-lean/community/dst-recall-scorecard.md`. "Pre-fix behaviour" is either a
switch on the fakes (named in the table) or an older released jar; the control for every row is the
same sweep with the switch off, which must stay clean. 1,000 seeds per row, Iceberg 1.12.0 unless
stated. Reproduce with `dst/scorecard.sh`.

**Recall: 4 of the 4 behaviours that could be put back are caught**, one of them (row G) against
real pre-fix bytecode from a released jar rather than a switch on the fakes. Three further rows
drive a pre-condition and confirm the shipped guard refuses it. One row is out of reach and says so
(§7). **False alarms on shipped code: 0 unexplained violations in 34,000 runs** — 10,000 in the
release matrix (§3), 4,000 in the control rows below, and 20,000 in the four 5,000-seed sweeps of
`./gw test -Ddst.seeds=5000`. One false alarm turned up on the way and was fixed; it was in an
invariant of mine, not in Iceberg, and §1 describes it rather than quietly dropping it.

| # | Bug or behaviour | Area | How it is expressed | Seeds | Failing with it on | First failing seed | Failing on 1.12.0 as shipped | Replay SHA-256 ×2 |
|---|---|---|---|---|---|---|---|---|
| | *control: faithful workload, default faults* | | – | 1000 | – | – | **0** | OK |
| | *control: faithful workload, aggressive faults* | | – | 1000 | – | – | **0** (186 excused, on 101 seeds) | OK |
| | *control: faithful workload, no faults* | | – | 1000 | – | – | **0** | OK |
| | *control: catalog faults only* | | – | 1000 | – | – | **0** | OK |
| | *control: a table with history before the writers start* | | – | 1000 | – | – | **0** | OK |
| A | a landed commit reported as a plain `CommitFailedException` (pre-`CommitStateUnknownException`: #4687, #4719, #4836, #4893; cleanup rules #8397, #10373) | `SnapshotProducer.commit` retry loop and post-commit cleanup | `PreFix.RETRY_ON_UNKNOWN_STATE` on the fake catalog | 1000 | **28** (default faults) / **39** (catalog faults only) | 53 / 38 | 0 | OK |
| B | cleanup running on a failure that is not a `CleanableFailure` (#8397, #10373) | `SnapshotProducer.commit` catch block | `strictCleanup=false` on the fake catalog | 1000 | 0 — see note | – | 0 | OK |
| C | duplicate snapshot id allocated, leading to data loss (#4747, fixed in 0.14.0) | `TableMetadata.addSnapshot` | the fake hands out an id already in the metadata (2,917 times over the sweep) | 1000 | 0 — the guard refuses every one | – | 0 | OK |
| D | a rewrite that supplies no starting sequence number (#3480 added the parameter, #7218 made Flink use it) | `BaseRewriteFiles`, `MergingSnapshotProducer.validateNoNewDeletesForDataFiles` | `PreFix.REWRITE_WITHOUT_SEQUENCE_NUMBER` | 1000 | 0 — the commit is refused instead | – | 0 | OK |
| E | `dataSequenceNumber(long)` accepts a value ahead of the table (round-1 `M1`, **still live**) | `RewriteFiles`, `MergingSnapshotProducer.setNewDataFilesDataSequenceNumber` | `sequenceAheadRewrite` on the workload | 1000 | **240** (default faults) / **406** (no faults at all) | 2 / 2 | **240 / 406 — not fixed** | OK |
| F | a catalog pointer that is not an atomic compare-and-set (#5036 added Hive lock heartbeats; `HadoopTableOperations` has this by design and documents it) | the catalog contract itself | `SelfCheck.NON_ATOMIC_POINTER` | 1000 | **1000** / **1000** (no faults) | 1 / 1 | 0 (an atomic pointer is the default) | OK |
| G | duplicate-delete count accumulated across commit retries (#16686, merged 2026-07-03) | `ManifestFilterManager` | **the pre-fix jar itself**: `ICEBERG_VERSION=1.11.0` | 1000 | **1000** (I9 ×2000) | 1 | 0 | OK |
| H | sequence-number conflict not retryable on concurrent branch commits (#15126, merged 2026-03-10) | `rest/CatalogHandlers` | **not built** (§7) | | | | | |

"Replay SHA-256 ×2" is `DeterminismTest`: every row's configuration replays to a byte-identical
decision log, and a failing seed replays to the same hash after hundreds of other seeds have run in
the same JVM.

### Notes on the rows

**A is the strongest row and the most useful one for the catalog work.** Seed 38, catalog faults
only, no store faults at all:

```
w1 overwrite            -> success
w2 overwrite            -> success
w3 replace-partitions   -> success
catalog attempts:
  (setup)  snap=1000  landed=true   answer=ok
  w1       snap=1003  landed=true   answer=ok
  w2       snap=1001  landed=false  answer=stale
  w3       snap=1002  landed=false  answer=stale
  w2       snap=1001  landed=true   answer=landed-reported-failed   <-- the pre-fix answer
  w3       snap=1002  landed=false  answer=stale
  w3       snap=1002  landed=true   answer=ok
  w2       snap=1001  landed=false  answer=stale
  w2       snap=1001  landed=true   answer=ok
violations:
  [I2] w3 was told its replace-partitions succeeded, snapshot 1002 was accepted,
       but it is not in the ancestry of the pointer
```

The mechanism is worth spelling out, because it is not the obvious one. `SnapshotProducer` memoises
its snapshot id across retries, so w2's retry re-commits **snapshot 1001**, which by then is already
in the metadata. `SnapshotProducer.commit()` sees `base.snapshot(newSnapshot.snapshotId()) != null`
and takes the rollback branch — `update.setBranchSnapshot(newSnapshot.snapshotId(), targetBranch)` —
so the pointer moves *back* to 1001 and w3's acknowledged snapshot 1002 is dropped from the
ancestry. The damage is not a double-apply but a silent rollback over a concurrent writer's
committed work. Across the sweep the same behaviour also shows as I5 (80 files that a committed
snapshot references were deleted) and I1-present, when the retry then gives up and `cleanAll()`
removes manifests of a snapshot that did land.

This is exactly what `CommitStateUnknownException` exists to prevent, and it is why row A is the
one requirement a catalog must not get wrong (§6).

**B came out at 0, and the reason is informative rather than a gap.** With `strictCleanup=false`,
`SnapshotProducer.commit()` runs `cleanAll()` on any `RuntimeException`. In the shipped code
`CommitStateUnknownException` is caught and rethrown *before* that branch is reached, so the one
case where cleanup would destroy committed data never gets there. The faults that do reach it are
write failures, and in those the commit has not landed, so cleaning up is correct. 0 of 1000 seeds
is the right answer and it says the ordering of those two catch blocks is load-bearing.

**C drives the pre-condition but cannot run the pre-fix code.** #4747 was fixed in 0.14.0, and the
harness compiles against 1.8.1 and later, so no available jar has the bug. What is measurable is
that the harness reaches the guard: it handed out a duplicate snapshot id 2,917 times across 1,000
seeds and `TableMetadata` refused every one. That is coverage of the guard, not recall of the bug,
and the table says so.

**D is round-1 finding 1 confirmed from the other side.** With no sequence number supplied, Iceberg
keeps equality-delete conflict detection on, so a concurrent equality delete fails the rewrite
instead of being silently out-ranked: 0 of 1000 seeds, with the rewrite writers failing on
`ValidationException` rather than committing. Round 1 argued this from source lines; the sweep now
shows it happening.

**G is the only row that runs real pre-fix bytecode, and it is the cleanest.** No switch on the
fakes: the harness simply resolves an older released jar. The workload seeds the table with a
manifest holding the same data file twice — the only way to make a duplicate, since
`ManifestFilterManager` keeps its "already deleted" set per manifest — and then has every writer
run `newDelete().deleteFromRowFilter(alwaysTrue())`, so one wins and the others retry with nothing
left to delete.

Seed 1 on both releases, **byte-identical trace SHA-256** (`d5d8e0f7…`), so the schedule is the
same and only the code differs:

```
1.12.0   snapshot 1003 delete {manifests-replaced=2, changed-partition-count=0, total-data-files=1, …}
1.11.0   snapshot 1003 delete {manifests-replaced=2, deleted-duplicate-files=1, changed-partition-count=0, …}
                                                     ^^^^^^^^^^^^^^^^^^^^^^^^^
```

Snapshot 1003 deleted nothing — it is the writer that lost the race and found the table already
empty on its retry — yet 1.11.0 reports that it removed a duplicate. The pre-fix
`duplicateDeleteCount` was a field on the filter manager, so the count its first, rejected attempt
accumulated was still there when the retry committed.

| Release | Date | Seeds | Failing | I9 occurrences |
|---|---|---|---|---|
| 1.12.0 | 2026-09-25 | 500 | **0** | 0 |
| 1.11.0 | 2026-05-15 | 500 | **500** | 1000 |
| 1.10.2 | 2026-05-13 | 500 | **500** | 1000 |
| 1.9.2 | 2025-07-04 | 500 | **500** | 1000 |
| 1.8.1 | 2025-02-24 | 500 | **500** | 1000 |

Two occurrences per seed, which is the two writers that retried. I9 fires on every release that has
the bug, on none that does not, and on no faithful workload (`default`, `aggressive`, `quiet`,
`catalog-only`, `deephistory`: 0 of 500 each). That is the shape a recall row should have.

The damage is a wrong number in the snapshot summary rather than lost data, so it is a Minor — but
`deleted-duplicate-files` is exactly what a maintenance job reads to decide whether a table needs
compacting, and it was wrong on any table where a delete ever lost a race.

**F is the control for the whole method.** Turning the compare-and-set into an unconditional write
breaks something on every single seed, with or without faults — I5 on all 1,000 and I2 on 769 to
1,982 occurrences. A lost update is not a property that degrades gracefully, which matters for §6.

## 3. The harness on five releases

The same faithful workload against every release the harness compiles against. 1,000 seeds each.

| Release | Date | Default faults | Aggressive faults | Escapes |
|---|---|---|---|---|
| 1.12.0 | 2026-09-25 | **0**/1000 | **0**/1000 (186 excused, on 101 seeds) | none |
| 1.11.0 | 2026-05-15 | **0**/1000 | **0**/1000 (195 excused) | 2 per seed, excused |
| 1.10.2 | 2026-05-13 | **0**/1000 | **0**/1000 (194 excused) | 2 per seed, excused |
| 1.9.2 | 2025-07-04 | **0**/1000 | **0**/1000 (208 excused) | 2 per seed, excused |
| 1.8.1 | 2025-02-24 | **0**/1000 | **0**/1000 (208 excused) | 2 per seed, excused |

Every excused violation on every row is attributable to the harness's own silent-truncation fault,
or to the missing `writeManifestsWith` injection point on releases before 1.12.0. 1.9.2 and 1.8.1
produce identical totals, which is what one expects when nothing in the commit path changed between
them; the harness refuses to run if the resolved classpath and the requested release disagree, so
that is a real result rather than a half-switched build. (An earlier version of the runner did not
check, and I did get one mis-measured pair out of it before adding the guard.)

Per 1,000 seeds the default profile lands about 2,300 commits, has about 880 commits rejected for a
stale pointer, 175 rejected by the catalog outright and 215 answered as unknown, and leaves about
820 orphan files. So the sweeps are reaching the retry and cleanup paths, not idling.

## 4. The one finding: `dataSequenceNumber(long)` has no precondition

Round 1 recorded this as `M1`, latent, from source lines alone. It now has a seed and a trace.
Line numbers below are from the `iceberg/` checkout (apache/iceberg main @ `9fffeb63b16`), which is
ahead of the 1.12.0 jar the sweeps ran against; the behaviour is the same on both, which is what
the sweeps show.

`RewriteFiles.dataSequenceNumber(long)` (api/src/main/java/org/apache/iceberg/RewriteFiles.java:128)
is **not deprecated** — every `rewriteFiles(Set, ...)` overload is, since 1.3.0 — and its javadoc
recommends it:

> Configure the data sequence number for this rewrite operation. This data sequence number will be
> used for all new data files that are added in this rewrite. This method is helpful to avoid commit
> conflicts between data compaction and adding equality deletes.

It goes straight through `BaseRewriteFiles.dataSequenceNumber` (BaseRewriteFiles.java:79-82) to
`MergingSnapshotProducer.setNewDataFilesDataSequenceNumber` (MergingSnapshotProducer.java:781-783):

```java
protected void setNewDataFilesDataSequenceNumber(long sequenceNumber) {
  this.newDataFilesDataSequenceNumber = sequenceNumber;
}
```

No precondition. Two things follow from a value above the table's last sequence number:

1. Supplying *any* value sets `ignoreEqualityDeletes = true` in
   `validateNoNewDeletesForDataFiles` (MergingSnapshotProducer.java:475-478), so a concurrently
   committed equality delete no longer fails the commit. That coupling is correct when the number
   is at or below the replaced files' — which is the case round 1 verified — and the whole point of
   the parameter.
2. The replacement files are then written with a data sequence number *above* every delete the
   table has. A reader applies an equality delete only to files whose data sequence number is lower
   than the delete's, so the rows the delete removed become visible again, and the resulting
   metadata records a file ordered after deletes that do not exist yet.

Seed 2, with **no faults injected at all** — three concurrent writers and nothing else:

```
w1 append              -> success
w2 overwrite (validated) -> failed: ValidationException: Found conflicting files ...
w3 rewrite             -> success
catalog attempts:
  (setup) snap=1000 landed=true  answer=ok
  w1      snap=1001 landed=true  answer=ok
  w2      snap=1002 landed=false answer=stale
  w3      snap=1003 landed=false answer=stale
  w3      snap=1003 landed=true  answer=ok
violations:
  [I8] data1 carries data sequence number 6 but snapshot 1003, which is where a reader
       finds it, has sequence number 3; the file is ordered after deletes that do not exist yet
  [I8] data2 carries data sequence number 6 ... (same)
```

w3 built its rewrite against snapshot 1000, computed a sequence number from it, lost the race to
w1, refreshed, retried, and committed as snapshot 1003 — whose sequence number is 3. The number it
had already handed to `dataSequenceNumber(long)` is 6, and nothing checks it at any point between
the call and the manifest. Reproduce with `./run.sh 2 seqahead-quiet`; it fires on 406 of the first
1,000 seeds.

I7, the row-level statement of the same thing, fires on 90 occurrences in the same sweep: a seed
data file carrying sequence number 1 is replaced by a rewrite output carrying 6, while an equality
delete committed in between carries 2, so the rows that delete removed are visible again.

The harness's `sequenceAheadRewrite` deliberately passes `current.sequenceNumber() + 5`, which no
faithful caller would do. That is the honest limit of this finding: it is a missing pre-condition,
reachable only by a caller that supplies an out-of-range value, not a defect that correct callers
hit.

**Severity: Minor, latent.** No caller in the Iceberg tree passes a sequence number ahead of the
table: `RewriteDataFilesSparkAction` uses the starting snapshot's. A `grep` of main code finds no
in-tree caller of `RewriteFiles.dataSequenceNumber(long)` at all. So this is a public,
non-deprecated, documented API with no in-tree user and no precondition — reachable by exactly the
external compaction and maintenance jobs its javadoc points at. One line closes it:

```java
Preconditions.checkArgument(
    sequenceNumber <= base.lastSequenceNumber(),
    "Cannot set a data sequence number %s above the table's last sequence number %s",
    sequenceNumber, base.lastSequenceNumber());
```

### Cross-check against the patch prepared in `upstream/`

While this round was running, a sibling session reached the same conclusion from source reading and
prepared `upstream/02-rewrite-data-sequence-number.md` with a patch (not raised). Two independent
methods converging on one defect is the strongest thing in this round: static reading found it, and
the harness now reproduces it on 406 of 1,000 seeds with no faults injected.

**Their first check is exactly right and is what I8 measures:** `suppliedDataSequenceNumber <=
base.lastSequenceNumber()`. A number to inherit always comes from an already committed file, so
anything above the table's last sequence number is wrong by construction.

**Their second check looks too strict, and the harness says so with numbers.** The patch also
requires `suppliedDataSequenceNumber <= replaced.dataSequenceNumber()` for every replaced file.
Iceberg's own rewrite action does not satisfy that. `RewriteDataFilesCommitManager.commitFileGroups`
(core/src/main/java/org/apache/iceberg/actions/RewriteDataFilesCommitManager.java:96-100) supplies

```java
RewriteFiles rewrite = table.newRewrite().validateFromSnapshot(startingSnapshotId);
if (useStartingSequenceNumber) {
  long sequenceNumber = table.snapshot(startingSnapshotId).sequenceNumber();
  rewrite.dataSequenceNumber(sequenceNumber);
}
```

— the sequence number of the snapshot compaction *started from*, not of the files being replaced.
On any table with history those files were added by older snapshots and carry lower numbers, so
`supplied > replaced.dataSequenceNumber()` is the normal case, not the pathological one. It is also
safe: a rewrite that plans against a newer snapshot has already applied every delete visible in it,
so lifting the output above those deletes changes nothing a reader sees.

The harness measures this directly. With the table given a history before the writers start
(`./run.sh sweep 1000 deephistory`, four seed appends, so the replaced files sit at data sequence
number 1 while the table has advanced):

| Mode | Committed rewrites | …supplying above the replaced file | Unexplained violations |
|---|---|---|---|
| `deephistory` (default faults) | 258 | **258** | 0/1000 |
| `deephistory-quiet` (no faults) | 518 | **518** | 0/1000 |

Every single committed rewrite supplies a number above the file it replaces, mirroring what the
Spark action does, and nothing breaks. Under the patch as written, all 776 of those would raise
`ValidationException`. The patch's own new test (`testDataSequenceNumberAtTableLastSequenceNumber`)
passes only because it replaces `FILE_A`, whose `dataSequenceNumber()` is null for a hand-built
`DataFile` and is therefore skipped — so the test suite would not catch the regression either.

My reading: keep the `base.lastSequenceNumber()` bound, drop the per-replaced-file bound. That
leaves the check that I8 shows is needed and removes the one that would reject
`rewriteDataFiles`. This is a note for whoever owns that patch, not a change I made — I did not
touch `upstream/`, and the same point is in the shared notes folder.

Nothing was filed upstream; this round is evidence gathering, as instructed.

## 5. Snapshot expiry and orphan cleanup

These are the two operations whose job is to delete files that something referenced recently, so
they are the only place where a delete is legal. Round 2 originally skipped them and I5 said
bluntly "nothing a committed snapshot ever referenced may be deleted". That form calls every
correct expiry a defect, so the invariant had to learn retention first.

### What I5 says now

Every delete is recorded against the catalog pointer version in force when it happened. A delete is
legal exactly when the file is unreachable from the refs at that version and stays unreachable at
every later version. The harness computes the reachable set of the whole table at each pointer
version, eagerly, through an observer view that neither parks nor injects faults, so the timeline
exists before anything has been deleted.

Turning expiry on immediately broke three invariants, all three of them mine:

| Reported | Actually |
|---|---|
| I5: a manifest list was deleted while still reachable | `FakeStore.measureDeletesAgainst` was never wired up, so every delete recorded pointer version 0 |
| I1: "snapshot 1000 cannot be read: not in the metadata" | `TableView.reachable` added a snapshot id before checking it existed. Expiry removes a snapshot but leaves its child's `parentId` pointing at it, so an ancestry walk reaches ids that are gone |
| I4: "snapshot 1000 showed a different set of rows" | same cause: the expired snapshot was wrongly in the reachable set, so I4 compared its captured files against an empty read |

The same lesson as the I7 false alarm in §1, one level up: an invariant written before a feature
existed quietly encodes the absence of that feature. Here it encoded "snapshots are never removed".

### Expiry: clean

One writer runs `expireSnapshots().expireSnapshotId(id)` against the oldest snapshot that is not a
ref head, while the others keep committing. By id rather than by age, because `RemoveSnapshots`
reads `System.currentTimeMillis()` in its constructor and every snapshot in a run lands within a
millisecond or two, so `expireOlderThan` would be a coin toss rather than a decision the seed
controls. Both of its pools are injected (`planWith`, `executeDeleteWith`); the delete pool is
already a direct executor by default.

| Mode | Seeds | Expiries committed | Files deleted | Unexplained violations |
|---|---|---|---|---|
| `expiry-quiet` (no faults) | 1000 | 1000 | 4206 | **0** |
| `expiry` (default faults) | 1000 | 779 | 1526 | **0** |

`RemoveSnapshots.commit()` moves the pointer inside a retry loop that re-reads `base` on each
attempt, and only then runs `cleanExpiredSnapshots()`, which takes a fresh `ops.refresh()` and
diffs it against the metadata the expiry started from. So a writer that commits in between is
accounted for, and the sweeps agree.

To show that a clean sweep means something, `SelfCheck.EXPIRY_OVER_DELETES` gives the expiry a
`deleteWith` callback that also removes one manifest of the snapshot still at the head of main —
the failure mode an expiry actually has, a wrong reachability computation. I5 catches it on **every
seed** where the expiry deleted anything.

### Orphan cleanup: the cutoff is the only protection

`DeleteOrphanFilesSparkAction` is not in `iceberg-core`, so the cleaner here is part real and part
model, and the split matters: the reachability decision — the part that can be wrong — is real
Iceberg code (`ReachableFileUtil.manifestListLocations`, `statisticsFilesLocations`, and the real
manifest readers). The directory listing and the age filter stand in for what Spark supplies. Its
reads go through the table's own `FileIO`, so they park and writers interleave with the cleaner.
Ages are measured in scheduler steps rather than milliseconds, since the harness has no clock.

| Age cutoff (scheduler steps) | Failing seeds / 1000 | Invariants |
|---|---|---|
| 0 | **595** | I1-present ×1594, I5 ×1409, I1-readable ×67, I4 ×32 |
| 10 | **17** | I1-present ×49, I5 ×38 |
| 20 | **1** | I1-present ×2, I5 ×2 |
| 30 | 0 | – |
| 200 | 0 | – |

The cleaner is doing real work at every rung: even at a cutoff of 200 steps it removes 2,355 files
across the sweep, so the clean rows are a cleaner that ran and found genuine orphans, not one that
sat idle.

The worst case is seed 95 (`./run.sh 95 orphan20`): w3 writes its two rewrite output files, spends
the next twenty-odd boundary crossings reading and writing manifests, and commits snapshot 1002 —
by which time the cleaner has deleted both files as unreachable orphans. The committed snapshot
references two objects that are not there.

**This is not a defect.** It is documented behaviour: the Spark action defaults `olderThan` to three
days precisely because of this window. What the sweep adds is the shape of the requirement, stated
as a measurement rather than a warning: the commit protocol offers an orphan cleaner no handshake
at all. There is no lease, no "in-flight write" marker, nothing a cleaner could consult. A file
written but not yet committed is indistinguishable from a file abandoned by a dead job. The age
cutoff is the whole of the protection, and it has to exceed the longest write-to-commit gap any
writer on the table can have.

The step numbers are a lower bound scaled to this workload, not a wall-clock claim: here a writer
writes its data files and commits a few dozen boundary crossings later, while a real Spark rewrite
writes for minutes before committing. The ladder shows that the window exists and that it is
bounded only by the cutoff, not how many seconds it is.

## 6. Swarm testing, and three places the commit path cannot be driven

Everything up to here held the configuration fixed and varied only the schedule. That explores one
shape of run very thoroughly and every other shape not at all, and it is the main reason the first
rounds found nothing new: the workload was three writers, one operation each, one table, one branch,
format v2, about forty boundary crossings per seed.

`FaultConfig.swarm(seed)` draws the configuration from the seed instead — 2 to 5 writers, 1 to 4
operations each, format v2 or v3, fault severity from none to harsh, and manifest rewriting,
snapshot expiry and the ref operations in or out of the mix. The configuration is a function of the
seed, so a failing seed still replays exactly. Deepening the schedules alone took a seed from about
forty crossings to several hundred.

Four more operations went in with it, all of them untouched before and all much less exercised than
a plain append: **transactions** (`BaseTransaction`, which has its own retry and cleanup),
**branch commits** (`toBranch`), **stage-and-cherrypick** (`stageOnly()` then
`ManageSnapshots.cherrypick`), **fast-forward** and **rollback**.

### What it found: the harness loses control in three places

Not a correctness defect, so this is not the data-loss bug this round was hunting. It is a concrete,
small, reviewable gap, and it is the reason a controlled scheduler cannot cover the whole commit
surface:

| Where | What happens | Injection points offered |
|---|---|---|
| `SnapshotProducer` (1.12.0) | fine | `scanManifestsWith`, `writeManifestsWith`, `deleteWith` |
| `SnapshotProducer` (before 1.12.0) | manifest writes run on `ThreadPools.getWorkerPool()` | `scanManifestsWith` only; `writeManifestsWith` was added in 1.12.0 |
| `RemoveSnapshots` | fine | `planWith`, `executeDeleteWith`, `deleteWith` |
| `BaseTransaction` | overrides each operation's `deleteWith` with its own collector, then batches every path through `CatalogUtil.deleteFiles`, which has no `ExecutorService` parameter and uses `ThreadPools.getWorkerPool()` | **none** |
| `ManageSnapshots` | `cherrypick`, `setCurrentSnapshot`, `rollbackTo` each run an internal `SnapshotProducer` (`CherryPickOperation`, `SetSnapshotOperation`) inside a transaction the caller never sees | **none** — the interface declares no executor and no delete callback |

The `BaseTransaction` one has a side effect beyond testability. `BaseTransaction` sets
`deleteWith(enqueueDelete)` on every operation it hands out (BaseTransaction.java:127), and
`SnapshotProducer.deleteWith` refuses to be set twice. So a caller who wants its own delete callback
— which is what the API is for, routing deletes through an audit log or a soft-delete path — cannot
have one inside a transaction, and the cleanup deletes go straight to `FileIO` instead. That is
worth a line in the javadoc at least.

The harness reports these rather than hiding them: `Scheduler` records which boundary labels an
unmanaged thread crossed and a stack trace of the first one, and the escape violation is excused
only when the escaped labels match one of the two known gaps. Over 2,000 swarm seeds that accounts
for all 513 escapes and leaves nothing unexplained.

### Finding: two concurrent transactions leave one manifest with two sequence numbers

**Iceberg accepts the metadata as valid, and two reachable snapshots of that same table still
resolve one data file to two different data sequence numbers.** Both halves hold in the same run on
**300 of 300 seeds**, 0 rejected. Reproduces under Iceberg's real `BaseMetastoreTableOperations`
with no injected faults. The fake catalog is excluded. Not filed upstream; this round is evidence
gathering, as instructed.

Seed 1, the whole artifact (`./gw test --tests '*MetastoreFlipTest*'` →
`build/dst/both-halves-report.txt`):

```
Iceberg's own metadata-level validation: ACCEPTED
  every snapshot sequenceNumber <= lastSequenceNumber=6
  TableMetadata.buildFrom(...).build() -> ok    (runs TableMetadata.java:835)
  TableMetadataParser round-trip       -> ok
and yet, across REACHABLE snapshots only:
  data file w2o1-d0.parquet resolves to data_sequence_number [3, 5]
  data file w2o1-d1.parquet resolves to data_sequence_number [3, 5]
  manifest ...004-m0.avro listed with sequence_number [5, 3]
snapshots: 1000(seq1) 1001(seq2) 1002(seq3) 1005(seq4) 1003(seq5) 1004(seq6)
refs: [main]
```

One linear `main`, every snapshot reachable, no branches, no rollback, no manifest rewrite. Two
threads, one `Transaction` each.

Why the guard does not catch it, which is the point: Java's protection is at the **metadata.json**
level — `TableMetadata.java:835` (`snap.sequenceNumber() <= lastSequenceNumber`, in
`indexAndValidateSnapshots`, which runs in the constructor and therefore on every `build()` and
every parse) and `TableMetadata.java:1785`. The observable here is one level below them: a data
file's *inherited* `data_sequence_number`, which lives in the manifest list entry's
`sequence_number` and is resolved at read time by `ManifestReader`. Nothing re-reads manifest lists
at commit time, so no metadata-level check can see two snapshots disagreeing there.

Not a duplicate of `apache/iceberg#15126` ("Core: Make sequence number conflicts retryable in
concurrent commits", merged March 2026, 1.11.0, for issue #15001). That change only wraps
sequence-number validation failures in a retryable exception type for the REST commit path — a new
`RetryableValidationException` plus changes in `TableMetadata` and `rest/CatalogHandlers`. It does
not touch `BaseTransaction` or `applyUpdates`, and these sweeps run on 1.12.0, which contains it.

Same family as PyIceberg `iceberg-python#24`, where a transaction appending to a new branch lets a
parentless snapshot reuse a taken sequence number and `last-sequence-number` move backwards. That
reporter states that Java rejects this. For the parentless case Java does **not**:
`TableMetadata.java:1256-1261` exempts it explicitly —

```java
RetryableValidationException.check(
    formatVersion == 1
        || snapshot.sequenceNumber() > lastSequenceNumber
        || snapshot.parentId() == null,          // <- the exemption
    "Cannot add snapshot with sequence number %s older than last sequence number %s", ...);
```

— which is worth recording separately, though it is not the path this finding takes: the narrowing
below shows no branch is needed, and every snapshot in seed 1 has a parent.

The shape is small:

> Two threads. Each opens a `Transaction`, performs two operations in it (an append and a row
> delta), and commits the transaction. Nothing else. Afterwards, one manifest file is recorded in
> different reachable snapshots' manifest lists with **different `sequence_number` values** — for
> example `[5, 3]`.

Because an entry whose own `data_sequence_number` is null inherits the number recorded for its
manifest, and that number decides which equality deletes apply to those rows, two reachable
snapshots disagreeing about it means a reader sees different rows depending on which snapshot it
loads. A later `rewriteManifests` writes the number explicitly and makes whichever value it read
permanent.

#### The narrowing

Same harness, same store, same scheduler, Iceberg's real metastore operations, 2,000 seeds per row,
no faults. The check is `SequenceNumberFlip.findReachable`: one manifest path listed with more than
one `sequence_number` across the snapshots a ref can still reach.

| Script (writer 1 / writer 2) | Seeds with the flip |
|---|---|
| `transaction` / `transaction` | **2000 / 2000** |
| `append, transaction` / `transaction` | 1900 / 2000 |
| `append, append` / `append` | 0 / 2000 |
| `append, rewrite-manifests` / `append` | 0 / 2000 |
| `append, branch-append` / `append` | 0 / 2000 |
| `transaction` / — (one writer) | **0 / 2000** |
| `transaction, transaction` / — (one writer) | **0 / 2000** |

So it needs transactions, and it needs **two of them concurrently**. One writer doing one or two
transactions never shows it. Appends, manifest rewrites and branch commits never show it.

#### Why the catalog is excluded

The same fixed two-writer script, the same seeds, against two catalogs that differ in exactly the
property that was in doubt:

| Catalog | Metadata | Compare-and-set | Seeds with the flip (reachable only) |
|---|---|---|---|
| `FakeTableOperations` | objects in memory | object identity | 479 / 2000 |
| `FakeMetastoreOperations` on Iceberg's real `BaseMetastoreTableOperations` | real `metadata.json`, re-parsed by `TableMetadataParser.read` on every `refresh()` | the metadata **location**, i.e. content | **490 / 2000** |

Essentially the same rate. The in-memory metadata and the identity compare-and-set are not the
cause. Reproduce with `./gw test --tests '*MetastoreFlipTest*'`; the numbers land in
`build/dst/flip-report.txt` and `build/dst/narrow-report.txt`.

A sibling session pointed out — correctly, and I verified it at the source — that Iceberg's own
`TestTables` would **not** have decided this: `TestTables.TestTableOperations.commit` also begins
`if (base != current)` and its `refresh` reads a static in-memory map, so it has both of the
properties in doubt. Subclassing the real `BaseMetastoreTableOperations` was the way to get a
genuine JSON round-trip and a content compare-and-set without pulling in Hadoop.

#### The cause, by controlled experiment

A sibling session proposed `ManifestFilterManager`'s `filteredManifests` cache and, more usefully,
proposed the experiment: rather than adding seeds, invalidate a reused cache at the moment the base
moves and see whether the disagreement goes away. That design was right and it found the answer,
though not the cache it predicted.

`FilterCacheProbe` clears a chosen group of caches, by reflection, every time the fake catalog
refreshes — the moment `BaseTransaction.applyUpdates` (BaseTransaction.java:439-456) resets `base`
and re-commits the reused `PendingUpdate` objects. `transaction / transaction`, 200 seeds, real
metastore operations:

| Cache cleared on refresh | Seeds with the flip | Collections cleared |
|---|---|---|
| nothing (control) | 200/200 | 0 |
| `ManifestFilterManager.filteredManifests` + `filteredManifestResults` | 200/200 | 1600 |
| `ManifestMergeManager.mergedManifests` | 200/200 | 0 — never populated here |
| **`MergingSnapshotProducer.cachedNewDataManifests` + `cachedNewDeleteManifests`** | **0/200** | 1200 |
| all three | 0/200 | 2800 |

So the filter cache is **not** the cause: 1,600 clearings changed nothing. The cause is the
producer's cache of the manifests it wrote for its own new files
(`MergingSnapshotProducer.java:108` and `:112`). Clearing exactly those two collections when the
base moves removes the disagreement completely with nothing else altered.

The reuse path is explicit (`MergingSnapshotProducer.java:1149-1164`):

```java
private List<ManifestFile> newDataFilesAsManifests() {
  if (hasNewDataFiles && !cachedNewDataManifests.isEmpty()) {
    cachedNewDataManifests.forEach(file -> deleteFile(file.path()));
    cachedNewDataManifests.clear();
  }
  if (cachedNewDataManifests.isEmpty()) { ... write them ... }
  return cachedNewDataManifests;
}
```

The cached manifests are rewritten only when `hasNewDataFiles` is true, that is when the caller has
added more data files since the last write. A `BaseTransaction` re-apply adds none — the payload is
unchanged — so `hasNewDataFiles` is false and the first pass's manifests are returned as they are.
`prepareNewDataManifests` then refreshes only the snapshot id on them:

```java
manifest -> GenericManifestFile.copyOf(manifest).withSnapshotId(snapshotId()).build()
```

The snapshot id is updated; the sequence number is not. So a manifest written during the first pass
is committed under a snapshot that has taken a different, higher sequence number, and the manifest
list entry records the first pass's value.

#### Which field carries the stale value: `sequence_number`

A sibling session worked out the asymmetry that makes this possible, and I verified both halves at
source. `ManifestWriter.toManifestFile` (ManifestWriter.java:224-249) always writes
`sequence_number` as `UNASSIGNED_SEQ`, while `min_sequence_number` is concrete whenever any entry
had an assigned number. `V2Metadata.IndexedManifestFile` then treats them differently: for either
field it substitutes this commit's number **only if the stored value is UNASSIGNED**, otherwise it
returns the stored value untouched (`V2Metadata.java:112-122`, and the same shape in
`V3Metadata.java:99-123`). So an UNASSIGNED field self-heals on a re-apply and a concrete one does
not.

Recording both fields side by side for every manifest-list entry of every reachable snapshot in a
failing run (`./gw test --tests '*MetastoreFlipTest.whichFieldCarriesTheStaleValue*'` →
`build/dst/which-field-report.txt`), over 100 failing seeds:

| | seeds |
|---|---|
| `sequence_number` disagrees between snapshots | **100 / 100** |
| `min_sequence_number` disagrees between snapshots | 100 / 100 |
| neither disagrees, so it is at the entry level | 0 / 100 |

Both fields disagree, and they move together. So it is `sequence_number` itself, not the
`min_sequence_number`-only case — and that matters, because it means something hands the
manifest-list wrapper a **concrete** `sequence_number`, which case 4 then passes straight through.

Seed 1 shows the direction, which is the opposite of what I had assumed:

```
manifest 000000000004-m0.avro, addedBy=1003
  in snapshot 1003 (seq 5, the snapshot that ADDED it):  sequence_number=5   correct
  in snapshot 1004 (seq 6, the next snapshot of the SAME transaction): sequence_number=3   stale
```

The manifest is right in the snapshot that added it and wrong in its child. Snapshot 1004 is the
transaction's second operation, which inherits the manifest from its parent's manifest list. Since
`toManifestFile` never produces a concrete `sequence_number`, the object carrying 3 must have been
**re-read from a manifest list** — the pass-1 version of the parent's list, when the first operation
had provisionally taken sequence number 3 — rather than produced fresh. That also explains why
clearing `cachedNewDataManifests` fixes it: forcing the first operation to rewrite its manifest
invalidates the second operation's stale read of it.

#### Three caches refuted, and the only effective lever is path churn

A sibling session then proposed `SnapshotProducer.manifestsWithMetadata` with a complete chain, and
the source facts are right: it is a `final LoadingCache<ManifestFile, ManifestFile>`
(SnapshotProducer.java:103) built once per producer, keyed on a path-only `equals`/`hashCode`, whose
loader returns the key object verbatim when `file.snapshotId() != null`
(SnapshotProducer.java:139-141), and the single lookup is at `:330` inside `writeManifestList`. All
verified.

They also proposed the right experiment: measure the flip rate **and** whether the manifest paths
change, so that cause can be told apart from lever. Adding `withmeta` as a fourth probe group, and
counting entries actually evicted rather than just calls made, 200 seeds of
`transaction / transaction`:

| Cleared on every refresh | Flip | `manifestsWithMetadata` entries evicted | Manifest writes (path churn) |
|---|---|---|---|
| nothing (control) | 200/200 | 0 | 1200 |
| `filteredManifests` + `...Results` | 200/200 | 0 | 1200 |
| **`manifestsWithMetadata`** | **200/200** | **5000** | 1200 |
| **`cachedNewData/DeleteManifests`** | **0/200** | 0 | **1600** |
| all of them | 0/200 | 5000 | 1600 |

So `manifestsWithMetadata` is refuted too, and properly: the cache really was populated, 5,000
entries were evicted across the run, and the disagreement survived. Three path-keyed caches have now
been emptied at the moment the base moves with no effect at all.

That inverts the framing we had been working with. Path churn is not merely *how* the
`cachedNewDataManifests` lever masks a stale cache lookup — it is the only thing that fixes this,
and no cache eviction does. The reading that now fits every measurement is that **nothing is being
served from a cache**: the transaction's second operation writes a manifest list that records the
first operation's *pass-1* manifest at its pass-1 sequence number because that is genuinely what it
read, and clearing `cachedNewDataManifests` works only because it makes the first operation rewrite
to a new path, so the stale manifest is never referenced. That points at the ordering inside
`BaseTransaction.applyUpdates` and `SnapshotProducer.apply` rather than at any cache.

#### Which attempt wrote the list that disagrees: both of them

One more reading settles what kind of thing the stale value is, and it needs no hypothesis at all.
`SnapshotProducer.manifestListPath()` (SnapshotProducer.java:624-635) is
`String.format("snap-%d-%d-%s", snapshotId(), attempt.incrementAndGet(), commitUUID)`, and
`attempt` is a per-producer counter (`:110`). So the middle number in a manifest-list filename says
which `apply()` wrote it. For seed 1:

```
snapshot 1004 seq=6  manifestList=snap-1004-2-...   written in attempt 2
      000000000004-m0.avro  sequence_number=3   <- stale
snapshot 1003 seq=5  manifestList=snap-1003-2-...   written in attempt 2
      000000000004-m0.avro  sequence_number=5   <- correct
```

**Both lists were written in attempt 2, and they disagree.** That eliminates the two readings that
would have been easy to fix: op2's list was not left over from pass 1, and op1 did rewrite. In pass
2, op2 wrote a *fresh* manifest list that records op1's manifest at op1's *pass-1* sequence number,
while op1's own equally fresh pass-2 list records it at the pass-2 number.

So the stale value is neither a stale file nor an unrewritten list. It is a stale value held in
memory that survives into a newly written manifest list — and the three caches that could plausibly
hold it have each been emptied at the moment the base moves, with no effect.

That is where the instrument stops being able to help, and it is a precisely posed question for
whoever picks it up: in pass 2, where does op2's `apply()` get a `ManifestFile` for op1's manifest
carrying the pass-1 sequence number, given that op1's pass-2 manifest list on disk carries the
pass-2 one?

Three mechanisms have now been proposed and all three refuted by the same instrument. One candidate
fix is on the table and is not merely untested but **expected not to help**: if emptying
`manifestsWithMetadata` entirely changes nothing, bypassing it cannot. Recorded for completeness —

```java
.run(index -> {
      ManifestFile file = manifests.get(index);
      manifestFiles[index] = file.snapshotId() != null ? file : manifestsWithMetadata.get(file);
    });
```

A methodological note worth keeping, because it nearly cost a wrong conclusion. The first
`manifestsWithMetadata` run reported "2876 cleared", which counted *successful `invalidateAll()`
calls* and said nothing about whether the cache had held anything. Changing the counter to read
`estimatedSize()` before invalidating turned that into "5,000 entries actually evicted", which is
what makes the refutation stand. "Cleared an empty cache" and "cleared a populated cache and it did
not help" are different conclusions, and only the second is evidence.

Reproduce the attribution with
`./gw test --tests '*MetastoreFlipTest.narrowTheScript*' -Ddst.invalidateFilterCache=new`
(and `=filter`, `=merge`, `=all`, or omit it for the control).

#### Superseded hypothesis

My earlier guess was that the manifest *lists* written during the intermediate commits kept the
provisional numbers. The experiment above shows it is the producer's cached new-file manifests
instead. The prediction that "one writer never retries, which is why one writer never shows it"
survives and is confirmed: without a competing commit, `applyUpdates` never takes its re-apply
branch at all.

#### What was ruled out along the way

The original sighting, before the narrowing: `./run.sh 905 duo` — two writers, no faults, trace
SHA-256 `336ce6ad4a9de...` every time:

```
w1 #1 replace-partitions (validated) -> success
w1 #2 rewrite-manifests              -> success
w1 #3 row-delta(pos) (validated)     -> success
w1 #4 branch-append(b1)              -> success
w2 #1 transaction                    -> success

snapshot graph (id, parent, seq, operation, data files with their data sequence numbers):
  1003 parent=1002 seq=4 append    -> data1@4 data2@4 ...
  1004 parent=1003 seq=5 delete    -> data1@3 data2@3 ...     <-- same files, seq 3
  1005 parent=1003 seq=6 replace   -> ... data1@3 data2@3     <-- the manifest rewrite
  1006 parent=1007 seq=9 overwrite -> ... data1@4             <-- seq 4 again
```

The observable, confirmed down to raw paths and byte lengths: **one manifest file** — identical
path, identical `length`, identical `snapshotId` — is recorded in different manifest lists with
different `sequence_number` values. In the 20,000-seed swarm the same shape appears as
`.../00000000-0000-4000-8000-000000000005-m0.avro`, `addedBy=1003`, `len=7134`, listed with
`sequenceNumber=9` by snapshots 1003, 1014, 1015 and with `sequenceNumber=2` by snapshots 1005,
1012, 1013.

Why that matters: in v2 and v3 an entry whose own `data_sequence_number` is null **inherits the
sequence number recorded for its manifest**, and that number is what decides which equality deletes
apply to those rows. Two snapshots disagreeing about it are two different tables. A subsequent
manifest rewrite then writes the number explicitly and makes whichever value it read permanent,
which is how I10 noticed.

| Ruled out | How |
|---|---|
| my reflective `commitUUID` override | `DST_COMMIT_UUID=random ./run.sh 905 duo` reproduces identically, same trace hash |
| two manifests collapsed by my alias function | printed raw paths, lengths and `snapshotId`: one file |
| the same path appearing twice in one snapshot with two sequence numbers | `TableView.ambiguousSequenceNumbers` detects that case and it does not fire here |
| injected faults | every fault percentage is 0 in this configuration |
| a sequential-logic bug | 0 of 4,000 single-writer seeds; it needs two writers interleaving |
| the `manifestsWithMetadata` cache in `SnapshotProducer` | a sibling session checked it: `addMetadata` passes `manifest.sequenceNumber()` straight through so an UNASSIGNED stays UNASSIGNED, and the inheritance happens later in the manifest-list wrapper, which guards it with `Preconditions.checkState(commitSnapshotId == wrapped.snapshotId(), ...)` and would throw rather than write a wrong number |
| `TestTables` as a discriminator | verified at source: identity compare-and-set and a static in-memory metadata map, so it shares both properties with the fake |

#### Still open

The mechanism is not traced to a line, and the cheapest way to do that is to read
`BaseTransaction.commitTransaction` and `applyUpdates` against the narrowing above rather than to
run more seeds. Also untested: whether the flip survives a `HadoopTables` catalog on a local
temp directory, which would add a real version-file compare-and-set on top of the JSON round-trip;
`BaseMetastoreTableOperations` already supplies the property that was in doubt, so this would be
belt and braces rather than a new discriminator.

I also tried round-tripping the metadata inside `FakeTableOperations.refresh()` as a cheaper check,
and it cannot work: that fake's compare-and-set is an object identity check, so a re-parsed copy is
never the pointer, every commit is rejected as stale and nothing lands (`steps=0`). That is recorded
next to the compare-and-set in `FakeTableOperations`. Subclassing the real base class sidestepped it
entirely.

### What it did not find

Apart from the candidate above: no other unexplained violation across the swarm sweeps, including
format v3 with deletion vectors, transactions, branch commits, cherrypick, fast-forward and
rollback. Five of my own invariants had to be corrected on the way there, which is the other honest
headline of this section:

| Invariant | Was wrong because |
|---|---|
| I3 | counted a payload globally rather than per ref, so a branch commit or a cherrypick looked like a double-apply |
| I2 | required a committed snapshot to be in the *final* ancestry, which expiry, rollback and branch-replace all legally break; it now asserts visibility at the pointer version where the commit landed, and keeps the stronger form only for runs where nothing was ever removed |
| I10 | blamed every snapshot a fiber landed, so a writer that rewrote manifests once and appended next was reported for the append |
| I2 again | required every writer to have landed a commit, but `rollback` to the snapshot already current, and `fast-forward` when main is already at the branch head, are legitimate no-ops: `SnapshotProducer.commit()` returns without calling the catalog when `changes()` is empty |
| payload naming | files were named per fiber, so a writer's second operation reused the first's paths and every "applied at most once" and "never deleted while reachable" check fired on the harness's own doing |

Each of those was found by *extending* the harness, never by running more seeds of it. That is the
load-bearing caveat on every clean number in this document: a clean sweep says the invariants I
wrote did not fire, and on this evidence my invariants were wrong about as often as Iceberg was.

## 7. What the harness cannot reach

Stated so a reader can see what a clean sweep does and does not mean.

- **Instruction-level races.** The unit of interleaving is one fake-boundary call. A race between
  two lines inside a single Iceberg method is not explored. `apache/iceberg#16686` ("Core: Fix
  thread conflict when deleting duplicate files in manifest", merged 2026-07-03) is half out of
  reach for this reason: the thread-safety of `deleteFiles` and `duplicateDeleteCount` needs a
  genuinely parallel worker pool, which the harness deliberately replaces with a same-thread
  executor, so that half is untested here. Its other half — the same count carried across commit
  retries — is row G, and the harness catches it on every pre-fix release.
- **The REST catalog.** The fake catalog is a compare-and-set pointer, not an HTTP server.
  `apache/iceberg#15126` ("Make sequence number conflicts retryable in concurrent commits", merged
  2026-03-10) fixes `CatalogHandlers` so a sequence-number validation failure comes back as a
  retryable `CommitFailedException`. Without a REST catalog in the loop the client sees a
  `ValidationException` either way, so the fix has no observable effect here. Not built, honestly.
- **`RewriteManifests` and the `rewrite_position_delete_files` action.** Snapshot expiry and orphan
  cleanup are now covered (§5), but manifest rewriting is not, and neither is compaction of delete
  files.
- **Releases before 1.12.0** have no `SnapshotUpdate.writeManifestsWith`, so manifest writes run on
  Iceberg's own worker pool. With `-Diceberg.worker.num-threads=1` they are serialised inside the
  turn of the writer that submitted them, but the harness cannot interleave another writer inside
  one manifest-write batch. The escape counter reports this every run and the violation is excused
  rather than hidden. This is also a small argument for the injection point 1.12.0 added: it is
  what makes the commit path testable under a controlled scheduler.
- **One shape of table.** One table, one branch, two partitions, three to five writers, format
  version 2. No branches or tags beyond `main`, no schema evolution, no partition-spec evolution,
  no v3 deletion vectors.
- **Orphans are counted, not judged.** A write whose bytes land while the caller is told it failed
  leaves an unreferenced file. The harness counts these (roughly 0.8 per seed in the default
  profile) but does not treat them as violations, because Iceberg deliberately does not delete
  files when it cannot know whether they are referenced.

## 8. What this gives the catalog work

For Polaris / BDC Catalog, the harness is also a conformance test for the catalog side, because
`FakeTableOperations` *is* the catalog contract. What the clean sweeps depend on:

1. **`commit` must be an atomic compare-and-set on the pointer.** `SelfCheck.NON_ATOMIC_POINTER`
   turns that off and every single seed then breaks — a writer is told it succeeded and its snapshot
   is not in the ancestry, and files a committed snapshot references are deleted. This is not a
   property that degrades gracefully.
2. **A commit whose answer is lost must be reported as unknown, never as a plain failure.** Row A
   of the scorecard is exactly this: a catalog that answers `CommitFailedException` for a commit
   that landed makes the client retry a committed payload, and the retry is treated as a rollback
   to its own snapshot, which moves the pointer backwards over a concurrent writer's work.
3. **Reads should be strongly consistent.** The stale-`refresh()` fault is off by default because a
   REST or Hive catalog does not have it. If a catalog does, the post-commit cleanup path in
   `SnapshotProducer.commit()` relies on `ops.refresh()` finding the snapshot it just wrote, and
   falls back to skipping cleanup when it does not — safe, but it leaks manifests.
4. **The metadata object must be written before the pointer moves, and a failed write must leave
   the pointer alone.** That is the order the fake implements, and it is what makes a failed commit
   a no-op rather than a corrupt table.
5. **Any maintenance job that deletes files needs an age cutoff longer than the longest
   write-to-commit gap on the table, and nothing in the protocol will tell it what that is.** §5
   measures this: at a cutoff of zero an orphan cleaner destroys 595 of 1,000 tables, and the
   protocol offers no lease or in-flight marker it could consult instead. If BDC Catalog ever runs
   cleanup on behalf of a table it does not control the writers of, that cutoff is a contract with
   those writers, not a tuning knob. Snapshot expiry is the safe case by contrast, because
   `RemoveSnapshots` derives what to delete from committed metadata rather than from a listing.

## 9. Reproducing

```bash
cd dst
./gw test                            # every test, 500 seeds per sweep, about 30 s
./gw test -Ddst.seeds=5000           # a longer sweep
./gw test -Ddst.seed=53              # replay one seed, full log into build/dst/
./scorecard.sh                       # every recall row -> build/scorecard.txt
./versions.sh                        # every release x profile -> build/versions.txt
./run.sh sweep 1000 seqahead-quiet   # the finding in §4, no faults injected

# row G, the strongest recall row: same schedule, different jar
./run.sh sweep 1000 dupdelete                          # 1.12.0  -> 0/1000
ICEBERG_VERSION=1.11.0 ./run.sh sweep 1000 dupdelete   # pre-fix -> 1000/1000
./run.sh 1 dupdelete                                   # one seed, with snapshot summaries

# maintenance (§5)
./run.sh sweep 1000 expiry-quiet                       # expiry concurrent with writers -> 0
./run.sh sweep 1000 orphan0                            # orphan clean, no cutoff -> 595/1000
./run.sh 95 orphan20                                   # the worst-case window
```

`dst/README.md` has the design, the fault list and the limits in more detail.
