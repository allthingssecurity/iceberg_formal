# r1: code hunt against the TLA+ findings (2026-11-02)

Checkout: `iceberg-verify/iceberg`, apache/iceberg main @ `9fffeb63b1605a7c3acf218e58b7eeb43bdb1b01`
(2026-11-02, shallow). Method: take each anomaly the model produced (`findings/iceberg-commit-tla.md`)
and give it a verdict at source lines. Source-line reads only; nothing was built or run.

**Result: no Majors. Four of four model findings are already guarded in Iceberg, two Minors found on the way.**
That is the honest outcome and it is worth stating plainly: the model's value this round was not a bug, it was
(a) independently deriving the sequence-number/validation coupling that Iceberg actually implements, which is
how we know the model is faithful, and (b) pointing at the two places where the guard is opt-in and nothing
enforces it.

## Verdicts

| # | Model finding | Verdict | Anchor |
|---|---|---|---|
| 1 | compaction resurrects rows a concurrent delete removed | **no defect**, both guards present and correctly coupled | BaseRewriteFiles.java:135-140, MergingSnapshotProducer.java:478 |
| 2 | equality delete misses concurrently appended rows | **no defect**, specified behaviour; serializable path present where it can help | TableProperties.java:432-449, SparkPositionDeltaWrite.java:293-295 |
| 3 | non-atomic catalog loses an accepted commit | **by design and documented**; the trace is what you get on a store without atomic rename | HadoopTableOperations.java:161, HadoopCatalog.java javadoc |
| 4 | commit applied twice after a lost answer | **refuted**; never retried, and the recommended token already exists | SnapshotProducer.java:498, 538, 552; RESTTableOperations.java:228-266 |
| M1 | `dataSequenceNumber(long)` silently disables equality-delete validation, unchecked | Minor, latent (no production caller) | MergingSnapshotProducer.java:781-783 |
| M2 | Spark overwrite paths validate nothing when `isolation-level` is unset | Minor, asymmetry with DELETE/UPDATE/MERGE | SparkWriteConf.java:473-478, SparkWrite.java:379-384, BaseReplacePartitions.java:31-32 |

## 1. Compaction versus a concurrent delete: no defect

`BaseRewriteFiles.validate` (BaseRewriteFiles.java:135-140):

```java
protected void validate(TableMetadata base, Snapshot parent) {
  validateReplacedAndAddedFiles();
  if (!replacedDataFiles.isEmpty()) {
    validateNoNewDeletesForDataFiles(base, startingSnapshotId, replacedDataFiles, parent);
```

and that overload passes (MergingSnapshotProducer.java:475-478):

```java
validateNoNewDeletesForDataFiles(
    base, startingSnapshotId, null, dataFiles, newDataFilesDataSequenceNumber != null, parent);
                                              ^^^^ ignoreEqualityDeletes
```

This is the model's two guards, coupled exactly as the model showed they must be:

- a sequence number was supplied, so the new data files inherit a number at or below the replaced files'. Any
  existing equality delete still has a higher number and therefore still applies, so equality deletes are
  safely ignored and only position deletes (which name the replaced paths) must fail the commit.
- no sequence number, so the new files get a fresh higher number and equality deletes would stop applying.
  Therefore **any** new delete fails the commit.

`rw_fix_seq` and `rw_fix_validate` in the model both pass clean, and Iceberg implements both. The
`validateNoNewDeletesForDataFiles` body (MergingSnapshotProducer.java:519-553) also defaults
`startingSnapshotId` to null, which makes `startingSequenceNumber` fall back to
`TableMetadata.INITIAL_SEQUENCE_NUMBER` and validates against every ancestor: conservative in the right
direction.

### M1 (Minor, latent): the supplied sequence number is never checked

`setNewDataFilesDataSequenceNumber` (MergingSnapshotProducer.java:781-783) is a bare field assignment:

```java
protected void setNewDataFilesDataSequenceNumber(long sequenceNumber) {
  this.newDataFilesDataSequenceNumber = sequenceNumber;
}
```

Nothing checks that `sequenceNumber` is at most the minimum data sequence number of the replaced files. A
caller passing a higher value gets both halves of the hazard at once: equality-delete validation is skipped
(because the field is non-null) **and** the new files outrank existing equality deletes, so those deletes stop
applying. That is the model's `rw_serializable` trace, reached through the public API with no error anywhere.

Not live: no production caller in `core`, `spark` or `flink` passes an explicit sequence number, and the
`rewriteFiles(Set, Set, long)` overload is deprecated since 1.3.0 (RewriteFiles.java:154-158). So this is a
trap for third-party and future callers, not a bug today. Fix is one Precondition in
`BaseRewriteFiles.dataSequenceNumber` (BaseRewriteFiles.java:79-82) comparing against the replaced files.

## 2. Equality delete versus a concurrent append: no defect

The model's conclusion was that this outcome is serializable and that serializable isolation cannot prevent it
when the delete commits first. The code agrees, in two places.

The Flink equality-convert operator documents the rule verbatim
(EqualityConvertPKIndex.java:260-261, and the test at 277):

```java
// An equality delete with sequence S deletes only data rows with sequence < S. Rows at or
// above S are re-inserts the delete does not affect; keep them indexed for a later delete.
...
boolean sequenceMatch =
    !stagingOnTargetBranch || deleteSeq == null || pos.dataSequenceNumber() < deleteSeq;
```

And where serializable isolation *can* help (the deleter retrying after a concurrent append landed), Spark
calls the right validation. DELETE, UPDATE and MERGE all default to serializable
(TableProperties.java:432-449, `*_ISOLATION_LEVEL_DEFAULT = "serializable"`), and
`SparkPositionDeltaWrite.java:293-295`:

```java
if (isolationLevel == SERIALIZABLE) {
  rowDelta.validateNoConflictingDataFiles();
}
```

My prediction that an engine might call the wrong validation is refuted for Spark: `SparkWrite.java:379-384`
and `424-429` call `validateNoConflictingData()` **and** `validateNoConflictingDeletes()` for SERIALIZABLE and
only the deletes variant for SNAPSHOT, and `validateNoConflictingData()` counts 3 in every Spark version
v3.5 through v4.2, so there is no backport divergence either.

## 3. Non-atomic catalog: by design, and documented

`HadoopTableOperations.commit` (line 161) is explicit:

```java
// this rename operation is the atomic commit operation
renameToFinal(fs, tempMetadataFile, finalMetadataFile, nextVersion);
...
// update the best-effort version pointer
writeVersionHint(nextVersion);
```

The class javadoc (line 53) reads "TableOperations implementation for file systems that support atomic
rename", and `HadoopCatalog`'s says "requires that the underlying file system supports atomic rename". So the
model's `cat_rename` trace is not a bug report, it is the documented consequence of using this catalog on a
store that does not provide atomic rename. The version hint being "best-effort" is the same admission.

Keep this one. It is the eleven-step formal argument for putting Polaris or BDC Catalog in the path, and it is
more use in a design review than the javadoc sentence.

## 4. Double apply after a lost answer: refuted, and the fix already exists

Three separate mechanisms, all of which the model's `unk_fix_token` config predicted were necessary.

The retry loop never retries an unknown outcome (SnapshotProducer.java:498):

```java
.onlyRetryOn(CommitFailedException.class)
```

and the catch rethrows without cleaning anything (SnapshotProducer.java:538-540):

```java
} catch (CommitStateUnknownException commitStateUnknownException) {
  throw commitStateUnknownException;
}
```

The client-generated snapshot ID **is** the idempotency token, and it is used to decide cleanup
(SnapshotProducer.java:551-555):

```java
// it might not be known which commit attempt succeeded in some cases, so this only cleans
// up the one that actually did succeed.
Snapshot saved = ops.refresh().snapshot(newSnapshotId.get());
```

with the `else` branch at 567-570 explicitly declining to clean up when the refresh could not load the
snapshot. And the REST catalog implements the refresh-and-compare resolution directly
(RESTTableOperations.java:228-232 and `reconcileOnSimpleUpdate` at 243-266):

```java
} catch (CommitStateUnknownException e) {
  // Lightweight reconciliation for snapshot-add-only updates on transient unknown commit state
  if (updateType == UpdateType.SIMPLE && reconcileOnSimpleUpdate(updates, e)) {
    return;
  }
```

`expectedSnapshotIdIfSnapshotAddOnly` (line 272 onward) is appropriately paranoid: it bails on multiple
snapshot adds, on any non-main ref, on any other update type, and on main being pointed somewhere other than
the snapshot just added.

Also checked and clean: `requireStrictCleanup()` defaults to **true** (TableOperations.java:124-129) and
nothing overrides it, so the `!strictCleanup || e instanceof CleanableFailure` branch
(SnapshotProducer.java:541) only cleans on exceptions explicitly marked `CleanableFailure`
(CommitFailedException, ValidationException and eight others). I went looking for a cleanup-on-ambiguity hole
here and there isn't one.

So the design recommendation from the model stands, but as a recommendation for **our** catalog rather than
for Iceberg: Polaris and BDC Catalog need to accept and record the snapshot id (or a client commit id) so a
refresh can resolve an unknown commit, because that is what Iceberg's client already expects of a catalog.

### M2 (Minor): the Spark overwrite paths validate nothing by default

`SparkWriteConf.isolationLevel()` (SparkWriteConf.java:473-478) is:

```java
return confParser.enumConf(IsolationLevel::fromName)
    .option(SparkWriteOptions.ISOLATION_LEVEL)
    .parseOptional();
```

No default, no `.tableProperty(...)` fallback. And `SparkWrite` gates all validation on a non-null value
(SparkWrite.java:379-384, same shape at 424-429):

```java
if (isolationLevel == SERIALIZABLE) { ... } else if (isolationLevel == SNAPSHOT) { ... }
```

`BaseReplacePartitions` validates only what was explicitly switched on, and both flags start false
(BaseReplacePartitions.java:31-32, validate at 89-107). `BaseOverwriteFiles.validate` (line 136 onward) is the
same pattern.

So `df.writeTo(t).overwritePartitions()` and overwrite-by-filter, run without the `isolation-level` write
option, commit with **no** conflict validation, while DELETE, UPDATE and MERGE on the same table default to
serializable. A concurrent delete to the replaced partitions is lost with no error.

Sceptic's case, and it is a decent one: that is arguably what "overwrite these partitions" means, and
last-writer-wins is the declared semantics. But the asymmetry with the row-level commands is undocumented, and
the only hint is a one-line comment on the option (SparkWriteOptions.java:76). Worth raising as a docs or
default question on the dev list rather than as a bug: should `isolation-level` fall back to a table property
the way the DELETE/UPDATE/MERGE levels do?

## What this means for the next round

Every protocol-level anomaly the model can express is already guarded in Iceberg, which is the expected outcome
for a format whose spec is this carefully written, and it is the same shape as r46 in the Camel work where no
world was built and the Major came from elsewhere. Majors here will come from the layers the model cannot see:

1. The DST harness, which runs the real `SnapshotProducer` retry loop and the real engine commit paths against
   a fake `Catalog` and `FileIO` with injected faults. All four of this round's verdicts were reached by
   reading; none of them exercised the code.
2. The object store model, still unwritten, covering crash before the pointer move, truncated manifest writes
   and whether orphan cleanup can be made safe. Nothing in this hunt touched that.
3. New code in the sequence-number area. `flink/*/maintenance/operator/EqualityConvert*` is active work (the
   HEAD commit is a backported fix to it) and it reasons directly about the rule the model formalises. It is
   well commented and I found nothing wrong in the slice I read, but it is the right place to look next.
