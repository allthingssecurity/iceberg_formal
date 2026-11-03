# r4: a deliberate hunt for a Major (2026-11-02)

apache/iceberg main @ `9fffeb63b16`. Source-line reads and greps only; nothing built or run.

The brief for this round was to go wherever I thought a Major could be, rather than to chase the model's
findings. **Nothing Major found.** This file records what was swept so the next round does not repeat it, and
one artifact that is worth keeping: a complete enumeration of every file-deletion site in `core` with its
guard, which is the hand proof that complements the Spin model in `r3-spin-store.md`.

## Method

Two patterns, both chosen because the models say they are where data loss lives, and both finite enough to
enumerate rather than sample. This is the approach that produced the Major in the Camel r46 round, where
reading leads had run dry and a pattern search over the whole tree found it.

- **Pattern A**, sequence-number comparisons. The table spec rule is that a delete file applies to data files
  with a data sequence number *strictly* lower than its own. A single `<=` where `<` belongs would
  over-delete, silently, everywhere.
- **Pattern B**, file deletion. From `r3-spin-store.md`, the dangerous operation is deleting a file while some
  reachable snapshot still references it. Every such call site in `core` is enumerable.

## Pattern A: clean

Two hits in the whole of `core` and `api`:

```
TableMetadata.java:835   snap.sequenceNumber() <= lastSequenceNumber
TableMetadata.java:1785  formatVersion == 1 || snapshot.sequenceNumber() <= lastSequenceNumber
```

Both are metadata validity checks, asserting that no snapshot claims a sequence number beyond the table's
last. Non-strict is correct there. No delete-applicability comparison uses `<=` anywhere.

## Pattern B: every deletion site in core, and its guard

| Site | What it deletes | Guard | Verdict |
|---|---|---|---|
| `SnapshotProducer.java:98` | anything, via `defaultDelete` | n/a, it is the sink | — |
| `SnapshotProducer.java:777` | manifests of this operation not in `committed` | ownership argument, `MergingSnapshotProducer:1120-1132` | safe, Spin-checked |
| `FastAppend.java:206` | manifests written by an earlier `apply()` | only when `hasNewFiles`; retries only follow a definitive `CommitFailedException` | safe |
| `FastAppend.java:190-191` | `rewrittenAppendManifests` only | explicitly does **not** touch `appendManifests` | safe |
| `ManifestMergeManager.java:117` | the newly created merged manifest | `merged` is always a new file | safe |
| `ManifestFilterManager.java:350` | the newly created filtered copy | `if (!manifest.equals(filtered))`, so never the original | safe |
| `ManifestFilterManager.java:361` | all filtered manifests, `committed = EMPTY_SET` | called only from filter-criteria setters (137, 145, 178, 193), before any commit | safe |
| `MergingSnapshotProducer.java:1150, 1178` | cached new data/delete manifests | same ownership argument | safe |
| `BaseTransaction.java:436` | uncommitted transaction files | `onlyRetryOn(CommitFailedException)` at 311, 364 | safe |
| `BaseTaskWriter.java:143` | files of an aborted task writer | writer-local, nothing committed | safe |
| `HadoopTableOperations.java:411` | metadata files | filesystem catalog, already documented unsafe (`r1`) | known |
| `RewriteDataFilesCommitManager.java:129` | added data files, on abort | see below | safe |
| `RewritePositionDeletesCommitManager.java:94` | added delete files, on abort | see below | safe |

The two rewrite commit managers were the lead I thought most likely to be a Major, because they make their own
cleanup decision rather than going through `SnapshotProducer`. Both are *stricter* than core:

```java
} catch (CommitStateUnknownException e) {
  LOG.error("Commit state unknown for {}, cannot clean up files because they may have been committed successfully.", ...);
  throw e;
} catch (Exception e) {
  if (e instanceof CleanableFailure) { ... abort ... }
```

Unknown state is caught *first* and rethrown with no cleanup, and everything else cleans only on
`CleanableFailure`. Core's equivalent is `!strictCleanup || e instanceof CleanableFailure`
(`SnapshotProducer:541`), which is weaker in principle but equivalent in practice because
`requireStrictCleanup()` defaults to true and nothing overrides it.

I also checked the async partial-progress path, which is newer and where I expected the gap:
`BaseCommitService.commitReadyCommitGroups` polls the batch out of `completedRewrites` *before* calling
`commitOrClean`, so a batch that hits unknown state is no longer in the queue that `close()`'s abort loop
(`BaseCommitService.java:189-195`) drains. It is therefore never deleted. The one cost is reporting: the
`catch (Exception e)` at 231 logs "Ignoring" and the batch is counted in neither `succeededCommits` nor
`committedRewrites`, so a partial-progress rewrite under-reports after an unknown commit. A retry then fails
loudly because `RewriteFiles` calls `failMissingDeletePaths()`, so it is not silent. Nit, not a defect.

## The Flink equality-convert lead, and why it died

`flink/*/maintenance/operator/EqualityConvert*` was my best remaining target: active code (the repo HEAD is a
backported fix to it) reasoning directly about the rule the TLA+ model formalises. It also contains an
explicitly stated assumption, which is usually where a Major hides:

> On a separate target branch the committer reassigns data sequence numbers, so every match is deleted;
> event-time ordering prevents over-deletion.

So on a separate target branch the sequence check is switched off entirely
(`EqualityConvertPKIndex.java:277`, `!stagingOnTargetBranch || ...`) and correctness rests on event-time phase
ordering instead. My hypothesis was over-deletion: event-time ordering separates phases, not sequence numbers,
so a main-table row re-inserted at a sequence number *above* an unconverted equality delete would be falsely
deleted, which is data loss.

The hypothesis is wrong, and the reason is worth writing down because it is a property of the format rather
than of this code. **All files added by one commit share that snapshot's sequence number, and a delete file
applies only to data with a strictly lower one, so an equality delete never applies to data added in its own
commit.** A re-insert is therefore either in the same commit as the delete, where phase ordering handles it
because staging rows register timers at a strictly later phase, or in a later commit, which is a later
conversion cycle. On the shared branch the sequence comparison implements exactly the same rule directly.
The two modes are equivalent and the author's reasoning holds.

That property is also the honest explanation for why the equality-delete anomaly in `iceberg-commit-tla.md`
finding 2 is not a defect, and I did not state it that cleanly in round 1.

## Where this leaves things

Four rounds. TLA+ found four protocol anomalies, all guarded. The code hunt found two Minors. Spin cleared two
open properties and reproduced one documented race with one undocumented nuance. This round swept two patterns
and the best remaining reading target, and found nothing.

**I do not think there is a Major reachable by reading in the area these models cover**, and I would rather
say that than keep probing code that several maintainers have already been through. The commit path, the
cleanup paths and the sequence-number rules are correct, the guards are in the right places, and where the
reasoning only exists as comments the models now confirm the comments.

What is actually worth sending upstream from four rounds is small and honest:

1. A docs PR on `DeleteOrphanFiles`: the javadoc warns about a short retention interval but not that the
   comparison is between the object store's modification times and the driver's `System.currentTimeMillis()`,
   so clock skew shortens the interval invisibly (`r3`).
2. A precondition on `RewriteFiles.dataSequenceNumber(long)`, which silently disables equality-delete
   conflict validation with no check that the supplied number is at most the replaced files' minimum (`r1`).
3. A dev-list question on whether Spark's `isolation-level` write option should fall back to a table property
   the way the DELETE/UPDATE/MERGE levels do, since the plain overwrite paths currently validate nothing by
   default (`r1`).

The remaining chance of a Major rests on the DST harness, which is the only lane that executes the code rather
than reading it. Every verdict across all four rounds came from reading or from a model.
