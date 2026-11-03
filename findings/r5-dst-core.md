# r5: DST on the core candidates A2-9, A2-1, A1-29, A1-18, and two additions for the open defect (2026-11-03)

Every result below comes from running Iceberg **1.12.0** from Maven Central (`iceberg-core`,
`iceberg-api`) inside the `dst/` harness: real `RemoveSnapshots`, `IncrementalFileCleanup`,
`ReachableFileCleanup`, `BaseTransaction`, `BaseRewriteManifests`, `BaseRewriteFiles`, and Iceberg's
own scan planning, against the fake store, fake catalog and seeded scheduler. Source lines refer to
`iceberg/` (main at 9fffeb6). For the two classes where the line numbers carry the argument I checked
them against the 1.12.0 bytecode: in `IncrementalFileCleanup`, `retry(3).suppressFailureWhenFinished()`
is on both loops, the first loop's body is `lambda$cleanFiles$1` (lines 131+) and the second's is
`lambda$cleanFiles$3` (179+).

## Summary

| id | status | severity | decided by | one line |
|---|---|---|---|---|
| A2-9 | **confirmed** | **Major** | DST + new manifest-list read fault | A read fault on a retained snapshot's manifest list that outlasts the retries is swallowed. The default (incremental) expiry then deletes manifests the current snapshot still references, and the expiry reports success. 20/20 seeds lose data, 0/20 in the control. ReachableFileCleanup with the same fault throws and deletes nothing (20/20). |
| A2-1 | **confirmed** (mechanism + both effects) | Minor | DST + new retention invariant R | A retried expiry removes snapshots that its own policy, applied to the base it commits against, keeps. 84/84 retried seeds, 0 without a retry. When the concurrent commit puts a ref on an expiring snapshot, the expiry fails with a non-retryable `IllegalArgumentException` (84/84). No reachable file is lost. |
| A1-29 | **confirmed** | **Major** | DST + Iceberg's own scan planning | `txn(append + rewriteManifests().clusterBy)` concurrent with another writer: the rewritten manifest keeps pass-1 data sequence numbers. With a concurrent equality delete, Iceberg's scan planning attaches that delete to the transaction's newly appended rows after the manifest rewrite but not before it (186/200, 0 in every control). The lever that forces a rewrite on re-apply removes it (0/200). The A1-1 lever does not. |
| A1-18 | **confirmed** | Minor | DST, attributed by exception class, reported path and reflection | A RewriteFiles that ages off delete file D in pass 1 fails on re-apply with `ValidationException` "Missing required files to delete" naming only D, a file the caller never asked to delete. This happens 180/200 times on a plain commit retry and 200/200 inside a transaction. 0 in five controls. |
| open defect | cause table now asserted; **v3 has a second observable** | (same defect) | DST (`MetastoreFlipTest`) | Cause table reproduced and asserted: `filter` 200/200, `withmeta` 200/200, `filter,withmeta` 0/200, manifest writes 1200 in every row. On v3 the stale pass-1 instance also carries a stale `first_row_id`, so two distinct live data files in the head snapshot get overlapping `_row_id` ranges (200/200). Joint eviction removes this too (0/200). |

`cd dst && ./gw test` at the end of this round: **32/32 pass** (26 existing + 6 new).

## Harness changes (dst/, owned this round)

All additions are off by default. With the new fault off no RNG draw is made, so the existing
schedules are byte-identical. The 26 pre-existing tests still pass (see the end).

- **Manifest-list read fault** (`FaultConfig.readFailManifestListPct`, `readFailScope`).
  `FakeFileIO.newInputFile(path).newStream()` throws `UncheckedIOException` for `snap-*.avro` reads.
  The fault is seeded, its percentage is configurable and every injection is counted.
  - Scope `CLEANUP_RETAINED` limits it to reads issued from a `FileCleanupStrategy` frame (decided
    from the call stack with `StackWalker`) of a manifest list whose snapshot is still in the
    catalog's current metadata.
  - Each injection is recorded with the Iceberg frame that issued the read, e.g.
    `IncrementalFileCleanup.lambda$cleanFiles$1`. Which loop was hit is therefore decided by the
    call site, not by the message.
  - Cleanup reads are counted per site even when the fault is off, so a control run shows which
    strategy ran.
  - An unknown scope name throws.
- **Expiry by policy** (`FaultConfig.expireRetainLast`, `expireIncrementalCleanup`). Uses
  `expireOlderThan(Long.MAX_VALUE).retainLast(n)`, which is time-independent and names no snapshot
  id, so Iceberg chooses IncrementalFileCleanup on a main-only table (RemoveSnapshots.java:390-397).
  A strategy can be forced through the package-private `withIncrementalCleanup(boolean)`
  (RemoveSnapshots.java:383), reached by reflection.
- **New workload actions:**
  - `expire-policy`;
  - `ms:create-keep-branch`, `ms:set-min-main`, `ms:replace-keep-to-head`, `ms:branch-at-oldest`,
    `ms:tag-oldest` (ManageSnapshots);
  - `transaction-rewrite-manifests`;
  - `transaction-dv` (the round-2 transaction in the shape v3 accepts; a separate action so the v3
    behaviour of the existing sweeps is unchanged);
  - `Workload.registerAction` for scenario-specific actions.
- **Ledger and probe:**
  - `FakeTableOperations.historyFibers()` records which fiber landed each metadata version, so an
    expiry's base B and result A can be paired.
  - `FilterCacheProbe.setMode(String)` sets the probe mode at runtime, so one test can assert several
    rows.
  - New probe group `rewrite`: empties `BaseRewriteManifests.rewrittenManifests`, but only on the
    owning writer's own refresh. It is not part of `all`.
- **Test-side helpers:**
  - `ScriptRun`: `Simulation.run` with pinned scripts, a disarmed setup step and the same invariants;
  - `Reports`: capped report files under `build/dst/`;
  - `org.apache.iceberg.DstEntries`: a test-only bridge to the package-private
    `ManifestReader.entries()`, so checks can read each entry's own snapshot id, status and sequence
    numbers.
- **build.gradle** forwards the new `dst.r5.*` seed properties.

Logs stay capped: slf4j stays at `error`, no stack traces are printed, and reports are at most 64 KB
each.

---

## A2-9: IncrementalFileCleanup swallows a failed read of a retained manifest list. CONFIRMED, Major

**Reproduce:**

```
cd dst && ./gw test --tests '*ExpiryReadFaultTest*' -Ddst.r5.readfault.seeds=20 -Ddst.r5.readfault.transientSeeds=40
# report: dst/build/dst/r5-a2-9-readfault.txt   (default run uses 3 seeds; each faulted seed costs ~7 s of real back-off)
```

**Workload:** `FaultConfig.expiryReadFault(strategy, pct)`.
- 4 seed appends, then 3 fibers. w1 runs the real `RemoveSnapshots` (`retainLast(1)`, cleanup on,
  both pools same-thread). w2 and w3 run main-only writers.
- No other faults.
- The read fault hits only cleanup reads of manifest lists of snapshots still in the current
  metadata.

| row (seeds 1..20) | seeds with a reachable file lost | I1-present / I1-readable / I5 / I4 | expiry | manifests deleted by the expiry, of which reachable at the end | injected faults (site) |
|---|---|---|---|---|---|
| control: default strategy, no fault | **0/20** | 0/0/0/0 | 20 ok | 0 / 0 | none. Cleanup reads: `IncrementalFileCleanup.lambda$cleanFiles$1/retained=20`, `$3/expired=63` |
| **trigger: default strategy, 100%** | **20/20** | 113/40/60/26 | **20 reported success** | **63 / 63** | `IncrementalFileCleanup.lambda$cleanFiles$1/retained=80` (4 per seed = retry(3)+1), none elsewhere |
| calibration: ReachableFileCleanup forced, 100% | 0/20 | 0/0/0/0 | **20 failed, `UncheckedIOException`** | **0** / 0 | `ReachableFileCleanup.lambda$pruneReferencedManifests$1/retained=80` |
| control: ReachableFileCleanup forced, no fault | 0/20 | 0 | 20 ok | 0 / 0 | none |
| transient: default strategy, **50% per read** (40 seeds) | **2/40** | 8/2/8/2 | 40 ok | 8 / 8 | `lambda$cleanFiles$1/retained=35` of 73 eligible reads |

**What this shows:**
- The default row shows `IncrementalFileCleanup` sites, so Iceberg chose the incremental strategy
  itself.
- Every injected fault hit the first loop (`lambda$cleanFiles$1`, IncrementalFileCleanup.java:120-160)
  and none hit the second. The attribution is therefore clean.
- The calibration row is the decisive control. The *same* fault on the *same* reads of the
  equivalent loop of ReachableFileCleanup (`pruneReferencedManifests`, ReachableFileCleanup.java:115-118,
  `throwFailureWhenFinished`) makes the expiry throw, and it deletes no manifest. So the fault does
  not cause the loss; the read-failure handling of IncrementalFileCleanup does.
- The 50% row is the transient case: four consecutive failures per snapshot, about 1/16. That works
  out to 2 of 40 seeds.

**Minimal sequence (seed 1):**
1. 4 appends on main. Expiry `retainLast(1)` commits and removes 3 snapshots.
2. `cleanExpiredSnapshots` → `IncrementalFileCleanup`. The first loop reads the head's manifest list.
   It fails 4 times (1 s, 2 s, 4 s back-off) and the failure is swallowed (`:122`), so
   `validManifests` is empty.
3. The second loop reads the 3 expired lists. Each of their manifests is `!validManifests.contains`,
   so it is added to `manifestsToDelete` (`:224-226`) and deleted at `:282`.
4. The head and every snapshot committed after it list those manifests. The I1 report says
   `snapshot 1005 cannot be read: ... NotFoundException`. A concurrent `overwrite` writer then fails
   with `NotFoundException`.
5. `expire.commit()` returns normally.

**Severity: Major.**
- Committed data is lost: the current snapshot references manifests that no longer exist, and the
  table cannot be read or written.
- It is reached by a correct caller under a production fault: a read error that outlasts ~7 s of
  retries (throttling, a network partition, a credentials refresh).
- It is on the default path: the core `ExpireSnapshots` API with file cleanup on and no snapshot ids
  named, on a table with no non-main snapshots. Flink's `TableMaintenance`
  `ExpireSnapshotsProcessor` uses exactly this shape (`expireOlderThan`/`retainLast`, cleanup on).
  Spark's `ExpireSnapshotsSparkAction` sets `CleanupLevel.NONE` and does its own reachability, so it
  is not affected.

**Not established:**
- I did not run pre-1.12.0 releases.
- I did not test `liveDeleteManifests` → `unreferencedDVs` (a v3 DV file deleted while live). The same
  swallow feeds it (IncrementalFileCleanup.java:137, :373-395) but the workload had no v3 DVs.
- I did not test a fault on a non-manifest-list read in the same loop.

---

## A2-1: RemoveSnapshots.idsToRemove accumulates across commit retries. CONFIRMED, Minor

**Reproduce:**

```
cd dst && ./gw test --tests '*ExpiryRetentionTest*' -Ddst.r5.retention.seeds=200
# report: dst/build/dst/r5-a2-1-retention.txt
```

**Invariant R (new):**
- E is an expiry whose commit landed. B is the catalog metadata it compare-and-set from, and A is
  the metadata it produced. Both are paired through `historyFibers()`.
- O is the set of snapshots that a fresh `RemoveSnapshots`, configured identically, removes when
  `apply()` is run on B. It runs over a read-only `TableOperations` pinned to B, so the oracle is
  Iceberg's own retention code.
- R: `ids(B) − ids(A) ⊆ O`. An expiry may not remove a snapshot that its own policy, evaluated on
  the metadata it actually replaced, keeps.
- R is time-independent here: `expireOlderThan(Long.MAX_VALUE)`, and no ref sets a max age.
- R holds in every control, including a concurrent one where the expiry retried 36 times.

| row, 200 seeds, `retainLast(1)` | expiry landed / retried / failed | R violated (seeds, snapshots) | IAE after retry | other invariants |
|---|---|---|---|---|
| control serial: create-keep-branch, then expire | 200 / 0 / 0 | 0 | 0 | clean |
| control serial: expire, then create-keep-branch | 200 / 0 / 0 | 0 | 0 | clean |
| control serial: branch-at-oldest, then expire | 200 / 0 / 0 | 0 | 0 | clean |
| control concurrent: expire ‖ append | 200 / **36** / 0 | **0** | 0 | clean |
| expire ‖ createBranch(keep, head).setMinSnapshotsToKeep(keep,100) | 200 / 84 / 0 | **84, 252** | 0 | clean |
| expire ‖ setMinSnapshotsToKeep(main,100) | 200 / 84 / 0 | **84, 252** | 0 | clean |
| expire ‖ replaceBranch(keep, head).setMinSnapshotsToKeep(keep,100), with keep@oldest | 200 / 84 / 0 | **84, 168** | 0 | clean |
| expire ‖ createBranch(old, oldest) | 116 / 84 / **84** | 0 | **84 `IllegalArgumentException`** | clean |
| expire ‖ createTag(t-old, oldest) | 116 / 84 / **84** | 0 | **84 `IllegalArgumentException`** | clean |

**Results:**
- R fires exactly on the retried seeds (84/84) and on no seed without a retry. This is asserted.
- Seed 3:
  - B has `keep@1003 minSnapshotsToKeep=100` and snapshots [1000..1003].
  - The expiry removed [1000, 1001, 1002]. A fresh expiry on B removes [].
- The precondition (RemoveSnapshots.java:215-222) does throw: "Cannot expire 1000. Still referenced
  by refs: [old]".
  - It is a non-retryable `IllegalArgumentException`. The caller never named 1000; that id came from
    pass 1's derived set.
  - A fresh expiry would have succeeded and kept 1000.

**Mechanism:**
- `idsToRemove` is a field (:69). It gets the derived ids of every pass (:233-236) and is never
  cleared.
- `commit()` re-runs `internalApply()` on each `CommitFailedException` (:361-376, call at :371).
- Pass 2's `idsToRetain` keeps the snapshots, but `removeSnapshots(idsToRemove)` (:237) removes the
  pass-1 ids anyway.

**Severity: Minor.**
- Nothing reachable is lost: I1, I4 and I5 are clean on every row, and the file cleanup is
  reachability-based.
- Time travel to the removed snapshots gives a clean "snapshot not found", not wrong rows.
- In every R-violating seed the final metadata equals the result of the serial order "expire, then
  the ref change". For example, seed 3 ends with {1003}, main and keep@1003 min=100, which is also
  what the serial control produces. So the outcome is a legal linearisation of two overlapping
  operations. What it violates is the retry contract: `internalApply()` re-reads `base` on purpose,
  and its result is ignored.
- The IAE case costs availability: the expiry fails and must be re-created.

**Not established:**
- I did not try the BaseTransaction re-apply of an expiry (`txn.expireSnapshots()`).
- I did not try refs with `maxSnapshotAgeMs`, which R as defined cannot judge without a pinned clock.

---

## A1-29: BaseRewriteManifests.newManifests reused across a transaction re-apply. CONFIRMED, Major

**Reproduce:**

```
cd dst && ./gw test --tests '*TxnRewriteManifestsTest*' -Ddst.r5.txnrewrite.seeds=200
# report: dst/build/dst/r5-a1-29-txn-rewrite.txt   (default run: 60 seeds)
```

**Workload:** v2 (one row v3), 2 seed appends, no faults.
- w1: `txn = newTransaction(); txn.newAppend()` (2 files, partitions a and b);
  `txn.rewriteManifests().clusterBy(partition)`; `commitTransaction()`.
- w2: `append`, or `append` then a RowDelta with an **equality delete in partition a**.

**Checks:**
- *origin*: every live entry of an appended data file, in every reachable snapshot, has
  `dataSequenceNumber == fileSequenceNumber ==` the sequence number of the snapshot named by the
  entry's own `snapshot_id`. Entries are read through `ManifestReader.entries()`.
- *one file, two data seqs*: the cross-snapshot check from round 2.
- *scan planning*: Iceberg's own `table.newScan().useSnapshot(id).planFiles()`, comparing the
  equality deletes each `FileScanTask` carries in a manifest-rewrite snapshot and in its parent. A
  rewrite must not change them.
- *eqDeleteMisapplied*: the same scan-planning question, computed directly from the sequence
  numbers.

| row, 200 seeds | w1 re-applied | origin wrong (seeds) | one file two data seqs | scan planning differs across the rewrite | eq delete misapplied |
|---|---|---|---|---|---|
| control: ONE writer, txn(append+rewriteManifests) | 0 | 0 | 0 | 0 | 0 |
| control: append, rewrite-manifests (no txn) ‖ append | 183 (plain retries) | 0 | 0 | 0 | 0 |
| control: ONE writer, txn, then eq-delete(a) | 0 | 0 | 0 | 0 | 0 |
| control: append, rewrite-manifests ‖ append, eq-delete(a) | 193 | 0 | 0 | 0 | 0 |
| trigger: txn ‖ append | 191 | **191** | **191** | 0 (no eq delete present) | 0 |
| trigger v3: txn ‖ append | 191 | **191** | **191** | 0 | 0 |
| **trigger: txn ‖ append, eq-delete(a)** | 200 | **200** | **200** | **186** | **186** |
| lever `rewrite` (force requiresRewrite on w1's re-apply) | 200 | **0** | **0** | **0** | **0** |
| lever `filter,withmeta` (the A1-1 caches) | 200 | 200 | 200 | 186 | 186 |

"Re-applied" is read from the manifest-list name: `snap-<id>-<attempt>-…` with attempt ≥ 2
(SnapshotProducer.java:624-635). `BaseTransaction` re-applies when `refresh()` shows the base moved,
*before* it calls the catalog (BaseTransaction.java:439-456 inside :365-369), so CAS rejections
undercount it.

**Seed 1 (eq-delete row):**

```
1000(seq1,append) 1001(seq2,append) 1003(seq3,append, w2) 1005(seq4,delete, w2 eq delete in a)
1002(seq5,append, w1)  1004(seq6,replace, w1's rewriteManifests)
head 1004 reads w1o1-d0.parquet at dataSeq=3 (EXISTING, snapshot_id=1002); snapshot 1002 has seq 5
scan planning: w1o1-d0.parquet equality deletes  in 1002 = []   in 1004 = [w2eq-deletes-eq0.parquet]
```

**Mechanism:**
- In pass 1, the rewrite reads the append's manifest P (provisional seq 3) and writes its entries as
  EXISTING with explicit seq 3 (`writer.existing`, BaseRewriteManifests.java:372-379).
- On re-apply, P keeps its path (MergingSnapshotProducer `cachedNewDataManifests`), so
  `requiresRewrite` (:206-219) is false. The pass-1 `newManifests` (:60, :186) are committed under
  snapshot 1004.
- The append itself now has seq 5. The equality delete committed at seq 4, between w1's two passes,
  ends up applying to w1's rows (4 > 3), although by commit order it must not (4 < 5).

**Attribution:**
- The `rewrite` lever empties `rewrittenManifests` only on w1's own refresh, which forces a rewrite
  from the current base. Every bad row drops to 0 and every transaction still succeeds.
- The A1-1 joint eviction has no effect.
- A first version of the lever cleared on every fiber's refresh. That hit w1 in the middle of its
  own pass-1 `apply()` and failed `validateFilesCounts` on 193/200 seeds. That was the lever's fault,
  not Iceberg's, and the lever now fires only on the owner's refresh.

**Severity: Major.**
- The query results are wrong: the head snapshot drops w1's rows that match an equality delete
  committed *before* them, while the parent snapshot shows them. A manifest rewrite must be
  content-preserving.
- It needs a correct but uncommon caller: `rewriteManifests` inside a transaction with an append.
  It also needs a concurrent commit and, for wrong rows, a concurrent equality delete (CDC/upsert) in
  the same partition.
- Without an equality delete, the stale seq is still a cross-snapshot disagreement (191/200).

**Not established:**
- I did not test real rows.
- The scan-planning check is Iceberg's DeleteFileIndex over the real manifests. The data files hold
  no Parquet, so "rows" means "which delete files a scan task applies".
- I did not examine v3 `first_row_id` on the rewritten entries.

---

## A1-18: ManifestFilterManager.deleteFiles sticky across re-apply. CONFIRMED, Minor

**Reproduce:**

```
cd dst && ./gw test --tests '*RequiredDeletesTest*' -Ddst.r5.requireddeletes.seeds=200
# report: dst/build/dst/r5-a1-18-required-deletes.txt   (default run: 100 seeds)
```

**Setup (disarmed):**
- seed0 data files in partitions a and b (seq 1).
- Position delete D in a (seq 2).
- An unrelated append in b (seq 3).

**The compaction:**
- It is a `RewriteFiles` replacing both seed0 files, with
  `validateFromSnapshot(current)` and `dataSequenceNumber(current seq)`.
- That makes the minimum live data seq 3, so Iceberg ages D off on its own (MergingSnapshotProducer.java:1025-1034,
  ManifestFilterManager.java:523-526). The caller never names D.
- The concurrent commit is `newRowDelta().removeDeletes(D)`.

| row, 200 seeds | rewrite ok / failed | re-applied | failure class | message names D and no caller file | D in `deleteFilterManager.deleteFiles` after failure |
|---|---|---|---|---|---|
| control: rewrite alone | 200 / 0 | 0 | – | – | – |
| control: txn rewrite alone | 200 / 0 | 0 | – | – | – |
| control serial: remove-D, then rewrite | 200 / 0 | 0 | – | – | – |
| control: rewrite ‖ unrelated append | 200 / 0 | **140** | – | – | – |
| control: txn rewrite ‖ unrelated append | 200 / 0 | (txn) | – | – | – |
| **trigger: rewrite ‖ remove-D** (plain SnapshotProducer retry) | 20 / **180** | 180 | `ValidationException` ×180 | **180/180** | **180/180** |
| **trigger: txn rewrite ‖ remove-D** | 0 / **200** | (txn) | `ValidationException` ×200 | **200/200** | **200/200** |

**Attribution:**
- The exception class is the first check.
- The reported path set is compared against D's and the replaced files' paths. The message lists
  only D.
- By reflection after the failure, D is still in the producer's `deleteFilterManager.deleteFiles`
  (ManifestFilterManager.java:75, added at :232 in pass 1). `validateRequiredDeletes` (:285-291)
  then demands it in pass 2, because `BaseRewriteFiles` always sets `failMissingDeletePaths`
  (BaseRewriteFiles.java:34).
- The 140 retried seeds of the "unrelated append" control show that a retry alone does not trip it.
  D has to disappear under it.

**Finding beyond the inventory:** this is **not transaction-only**. A plain commit retry
(`SnapshotProducer.commit`'s `Tasks.retry`) re-applies the same producer and fails the same way
(180/200).

**Severity: Minor.** A compaction fails with a misleading, non-retryable `ValidationException`
naming a file the caller never asked to delete. No data is lost or changed, and re-running the
compaction from scratch succeeds.

**Not established:** the second half of Notable 4, that D stays `markedForDelete` under a pass-2
`minSequenceNumber` that no longer ages it off. I found no realistic sequence where the minimum
decreases between passes.

---

## Open defect: the cause table as a test, and a v3 observable

**Reproduce:**

```
cd dst && ./gw test --tests '*MetastoreFlipTest.causeTable*' --tests '*MetastoreFlipTest.v3FirstRowId*' -Ddst.r5.v3.seeds=200
# reports: dst/build/dst/r5-cause-table.txt, r5-v3-first-row-id.txt
```

**Cause table (asserted).** `transaction / transaction`, real `BaseMetastoreTableOperations`, v2,
200 seeds:

| `dst.invalidateFilterCache` | flip | manifest writes | `manifestsWithMetadata` entries evicted |
|---|---|---|---|
| off | 200/200 | 1200 | 0 |
| filter | 200/200 | 1200 | 0 |
| withmeta | 200/200 | 1200 | 5000 |
| **filter,withmeta** | **0/200** | **1200** | 5000 |

The test asserts each count, asserts that the manifest-write count equals the control's (no path
churn), and asserts that eviction was non-empty.

**v3 `first_row_id`.** The same two concurrent transactions on format v3, with the row delta as a
deletion vector on the transaction's own file (`transaction-dv`). 200 seeds:

| row | manifest seq flip | manifest listed with ≥2 `first_row_id` | data file resolving to ≥2 `first_row_id` | overlapping row-id ranges of two distinct live files in one reachable snapshot |
|---|---|---|---|---|
| ONE writer (control) | 0 | 0 | 0 | 0 |
| **txn-dv / txn-dv** | **200** | **200** | **200** | **200** |
| txn-dv / txn-dv, `filter,withmeta` | 0 | 0 | 0 | 0 |

Seed 1:
- Snapshot 1003 (w2's append) has `firstRowId=24, addedRows=8`. Its manifest `…004-m0` is listed
  with `first_row_id=24` in 1003 and with **16** in the head 1004.
- In the head, `w2o1-d0.parquet` therefore resolves to row ids 16..19, the same as
  `w1o1-d0.parquet@16` (snapshot 1002, firstRowId 16). So one reader of the head sees duplicate
  `_row_id` values.

**Mechanism:**
- The stale pass-1 `ManifestFile` instance carries a non-null `firstRowId`.
- `ManifestListWriter.V3Writer.prepare` keeps a non-null `firstRowId` as-is
  (ManifestListWriter.java:198-199) and assigns a fresh one only to manifests with null.
- The joint eviction removes it, so it is the same defect.

**Severity:** it adds a v3 observable that is worse in kind. Row lineage promises unique `_row_id`s,
and these collide inside a single snapshot. I did not re-judge the defect's severity here.

**Not established:** I did not check a consumer of row lineage (e.g. a merge or CDC reader) that
reads the duplicates.

---

## Test suite

`cd dst && ./gw test` after all changes: **BUILD SUCCESSFUL, 32 tests, 0 failures** (5 min 37 s). These are the 26
pre-existing tests (unchanged, all green) plus 6 new: `ExpiryReadFaultTest`, `ExpiryRetentionTest`,
`TxnRewriteManifestsTest`, `RequiredDeletesTest`, `MetastoreFlipTest.causeTable` and
`MetastoreFlipTest.v3FirstRowId`. The default seed counts are lower than the counts in the tables
above (3, 200, 60, 100, 200, 100) to keep the suite fast. The tables come from the explicit
`-D...seeds` commands given in each section. The new tests assert only their controls and
calibrations, never that Iceberg is still broken, so a fix upstream does not turn them red.
`build/test-results` is 60 KB and `build/dst` is 48 KB.
