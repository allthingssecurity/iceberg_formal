# r5: cause of the two-sequence-number defect (2026-11-03)

**Status: cause CONFIRMED by controlled experiment (DST, real BaseMetastoreTableOperations, Iceberg 1.12.0).**
The defect itself is the one in findings/r2-dst-harness.md §6. Only metadata has been verified wrong; no query was run.

## The mechanism

Inside one `BaseTransaction`, op1 writes manifest P and op2 (a merging producer, here a row delta) inherits it.
When the transaction's commit is retried, `BaseTransaction.applyUpdates` (BaseTransaction.java:439-456) re-applies
both operations. op1 reuses P (same path, `MergingSnapshotProducer.cachedNewDataManifests`, :1149-1164) and its
pass-2 manifest list gives P the new sequence number N+1. op2 then receives P@N+1, but two caches in op2, both
keyed on the manifest **path alone** (`GenericManifestFile.equals/hashCode`, GenericManifestFile.java:406-419),
each hand back the **pass-1 instance P@N**:

1. `ManifestFilterManager.filteredManifests` (ManifestFilterManager.java:93): pass 1 stored the identity entry
   P@N → P@N (:377 or :393); pass 2's lookup at :371 returns P@N.
2. `SnapshotProducer.manifestsWithMetadata` (SnapshotProducer.java:103): unbounded Caffeine cache whose loader
   returns the key object verbatim when `snapshotId() != null` (:139-141). Pass 1 stored P@N; pass 2's
   `get(P@N+1)` at :330 hits the path-equal key and returns P@N.

`V2Metadata.IndexedManifestFile` (V2Metadata.java:112-122) passes an already-assigned `sequence_number` through
unchanged, so op2's pass-2 manifest list records P at N while op1's records it at N+1.

**Each cache masks the other.** If only (1) is cleared, (2) still returns P@N. If only (2) is cleared, (1)
returns P@N and the loader stores it verbatim. That is why every single-cache eviction in r2 §6 showed no effect,
and why the one "all" row that cleared both also cleared `new` and was confounded by path churn.
This mechanism was proposed by the r5 inventory reading (slice A1, rows A1-1/A1-20) and then tested.

## The experiment

`cd dst && ./gw test --tests '*MetastoreFlipTest.narrowTheScript*' -Ddst.flip.seeds=200 -Ddst.invalidateFilterCache=<mode>`
(`FilterCacheProbe` now accepts comma-separated groups and throws on an unknown name; verified: `=bogus` fails the test.)
Row `transaction only` (two writers, one transaction each, no faults):

| cleared at every refresh | flip | caches cleared | manifest writes (path churn) | manifestsWithMetadata entries evicted |
|---|---|---|---|---|
| nothing (control) | 200/200 | 0 | 1200 | 0 |
| `filter` | 200/200 | 1600 | 1200 | 0 |
| `withmeta` | 200/200 | 1200 | 1200 | 5000 |
| **`filter,withmeta`** | **0/200** | 2800 | **1200** | 5000 |
| `new` (r2's lever) | 0/200 | 1200 | 1600 | 0 |

The joint eviction removes the disagreement with manifest writes unchanged at 1200, so no new paths are written
and path churn cannot be the explanation. Row `append + transaction`: 189/200 → 0/200 under the joint eviction,
same pattern. Single-writer rows stay 0/200 under every mode. The control and single-cache numbers match r2 §6
exactly (200/200; 1600 cleared; 5000 evicted), so the probe edit did not change behaviour.
Raw reports: /private/tmp/claude-501/-Users-I074560-Downloads-claude2-exp/756a4cbb-3726-4f08-b4e1-d779e01a2bbb/scratchpad/narrow-*.txt

## What this establishes and what it does not

- Established: the stale value is the pass-1 `ManifestFile` instance held by two path-keyed caches that survive a
  `BaseTransaction` re-apply. Evicting both at the moment the base moves is sufficient to remove it; evicting
  either alone is not.
- A fix has a spec now: neither cache may return an entry whose `sequenceNumber`/`minSequenceNumber`/`firstRowId`
  differ from the lookup key's. Options: key both caches on (path, sequenceNumber, minSequenceNumber, firstRowId);
  invalidate both when a transaction re-applies; or have `manifestsWithMetadata` load `addMetadata` only for
  `snapshotId()==null` and **not** cache entries that already carry metadata. Not implemented, not tested.
- Not established: the read-side consequence (wrong rows returned by a query). Same caveat as r2.
- Not established: whether the v3 `first_row_id` is stale through the same instance (overlapping row-id ranges).
  That is assigned to the DST reproduction agent.
