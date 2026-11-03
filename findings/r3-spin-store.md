# r3: Spin model of the object store and cleanup (2026-11-02)

Spin Version 6.5.2 (6 December 2019). Model: `spin/IcebergStore.pml`. Runner: `spin/run.sh` copied from
`camel-lean/spin/run.sh` (builds in `spin/build/<name>`, keeps the trail, appends to `spin/results.txt`).
Reproduce: `./run.sh IcebergStore.pml <name> "<-D defines>" "-E"`. Trails replay with
`cd build/<name> && spin -t -p <defines> m.pml`.

This covers what the TLA+ model deliberately left out: the object store, process death mid-write, and the
cleanup paths. Promela earns its place here because the interesting states are "the file exists but is
truncated" and "the process died between two I/O calls", which are natural as process death and awkward as
TLA+ actions.

**No Major. Two properties came back clean, which is itself the answer to a question we had open, and two
configs broke in ways that are already documented or need caller error.**

## Results

| Config | Claim | Verdict | States |
|---|---|---|---|
| base | crash mid-write, two writers, post-commit cleanup | **0 errors** | 9,004 |
| reuse | a writer appends a manifest a live snapshot already references | live manifest deleted | 115 |
| reuse_fix | same, cleanup skips anything a live snapshot references | **0 errors** | 73,213 |
| orphan_default | orphan removal with the default 3-day interval | **0 errors** | 2,561 |
| orphan_short | orphan removal with a short interval or a skewed clock | dangling reference | 954 |

Two assertions carry the properties. At the commit site, every file the new snapshot references must be
`COMPLETE`, which catches dangling and truncated references. At every delete site, no live snapshot may
reference the file being deleted. A `Chain` monitor process re-checks both for every live snapshot in every
reachable state, and also asserts that the snapshot log stays a single chain rather than assuming it.

## The clean results, which are the useful part

**`base`: a crash during a manifest write never corrupts the table.** 9,004 states, no errors. A writer can
die before writing any bytes, or after writing a prefix (leaving a `PARTIAL` file), at any interleaving with
the other writer, and no committed snapshot ever references a file that is missing or truncated. This is the
"garbage, not corruption" property that Iceberg's design is supposed to give you, and it is now checked rather
than argued. It is the answer to the second question we started from: if the object store drops the connection
halfway through a manifest list write, the table is not left in a partial metadata state. What you get is a
storage leak.

**`base` also clears the cleanup ownership argument.** `SnapshotProducer.deleteUncommitted`
(SnapshotProducer.java:772-785) deletes every manifest of the operation that the new snapshot does not
reference, and it compares against that one snapshot's manifests, never against other live snapshots:

```java
for (ManifestFile manifest : manifests) {
  if (!committed.contains(manifest)) {
    deleteFile(manifest.path());
```

The safety of that rests entirely on an ownership argument written as comments in
`cleanUncommittedAppends` (MergingSnapshotProducer.java:1120-1132). The model confirms the argument holds for
every interleaving of ordinary use: manifests the operation wrote itself are its own, and
`rewrittenAppendManifests` are fresh copies made by `copyManifest` (MergingSnapshotProducer.java:333-337), so
no other snapshot can reference them.

**`orphan_default`: the default retention interval is sufficient.** 2,561 states, no errors. With the 3-day
default (DeleteOrphanFilesSparkAction.java:129) a file written by an in-flight commit is far too new to be a
listing candidate, so the race cannot be entered.

## `orphan_short`: the documented race, with the interleaving and one nuance that is not documented

954 states, assertion violated at depth 84, 304-step trail. Final state: snapshot 1 is live and the pointer,
it references files 0 and 1, and `fstate[1] = 0`, meaning absent. The writer's manifest was deleted and then
committed.

The interleaving, from the trail:

```
  6  Cleaner  fstate[f] != ABSENT        file is present on disk
  7  Cleaner  referenced_anywhere(f, r)  computed against the CURRENT pointer
 ...           r == 0                     not referenced, so it is an orphan
 --  Writer   commit                      the snapshot that references it lands
 84  Cleaner  do_delete(f)                deletes a file the table now references
```

The decision "this file is unreferenced" is evaluated against the pointer as it was *before* the commit, and
nothing re-checks it afterwards. The class javadoc already says so:

> It is dangerous to call this action with a short retention interval as it might corrupt the state of the
> table if another operation is writing at the same time.

So the race is a known limitation, not a find. **What the javadoc does not say is that the comparison is
between two different clocks.** The candidate filter is `file.getModificationTime() < olderThanTimestamp`
(DeleteOrphanFilesSparkAction.java:437, 503), where the modification time comes from the object store and
`olderThanTimestamp` defaults to `System.currentTimeMillis() - 3 days` on the Spark driver
(DeleteOrphanFilesSparkAction.java:129). A store whose clock runs behind the driver's shortens the effective
interval by the skew, with no warning and no way for the action to notice. So the real requirement is:

> retention interval > maximum commit duration + maximum clock skew between the object store and the driver

That is worth saying out loud in our own runbook whatever upstream does, and it is a reasonable docs PR: the
javadoc warns about the interval you choose and says nothing about the skew you do not control.

## `reuse`: the ownership argument has one hole, and it needs caller error to reach

115 states, assertion violated at depth 59. If a writer appends a manifest file that a live snapshot already
references, and this commit merges that manifest away, the manifest is not in the new snapshot's set, so
`deleteUncommitted` deletes it and the older snapshot dangles.

Structurally possible: `appendManifests` do flow into the merge pass
(`prepareNewDataManifests`, MergingSnapshotProducer.java:1138, feeding `apply()`'s `unmergedManifests`), so a
manifest handed in by the caller can be merged into a new one and drop out of `committed`.

But reaching it needs `appendManifest(M)` with the same `M` in two separate commits. That duplicates every
data file in M, so a caller doing it has already corrupted their table with duplicate rows before cleanup
deletes anything. Verdict: Minor, defensive at best. `reuse_fix` shows the obvious guard works (73,213 states
clean) but it costs a scan of every live snapshot's manifests on every commit, which is a real cost for a
problem nobody can reach by accident. I would not propose it.

I also checked the variant that would have been serious. `committed.contains(manifest)` is an equality test,
and `committed` comes from parsed metadata (`saved.allManifests(io)`, SnapshotProducer.java:558) while
`appendManifests` holds the caller's own object with a null snapshot id. If equality included the snapshot id,
every committed append manifest would fail the check and be deleted. It does not:
`GenericManifestFile.equals` compares `manifestPath` only (GenericManifestFile.java:406-414), with
`hashCode` on the same field. Clean. The only residue is that `equals` guards on
`other instanceof GenericManifestFile`, so a caller supplying a custom `ManifestFile` implementation whose
equality is identity-based would fail the containment check and have its committed manifest deleted. Every
practical path produces a `GenericManifestFile` (that is what `ManifestWriter.toManifestFile` returns), so
this is an API contract note, not a defect.

## Honest status after three rounds

TLA+ found four protocol anomalies, all already guarded in the code. The code hunt found two Minors. Spin has
now cleared the two properties that were genuinely open (crash mid-write cannot corrupt, and the cleanup
ownership argument is sound) and reproduced one documented hazard with one undocumented nuance. **No Major.**

That is not for want of looking, and I would rather say so than promote a Minor. It is also roughly the
expected outcome: three rounds of reading and modelling against a format whose spec is carefully written and
whose commit path has had years of adversarial attention. Protocol-level defects are where the maintainers
have already been.

The remaining odds, in order:

1. **The DST harness**, running now in a separate session. It is the only thing so far that executes the real
   code rather than reading it or modelling it. Every verdict in rounds 1 to 3 came from reading or from a
   model; none of them ran `SnapshotProducer`. On Camel this is exactly where the Majors came from.
2. **New code in the sequence-number area.** `flink/*/maintenance/operator/EqualityConvert*` is active work
   (the HEAD commit is a backported fix to it) reasoning directly about the rule the TLA+ model formalises. It
   is well commented and the slice I read was correct, but unreviewed new code in a formalised area is the
   best remaining target for reading.
3. **The streaming contract**, still unmodelled: offsets in the snapshot summary, writer fencing after
   failover, no duplicates and no gaps. That one is less likely to find an Iceberg defect and more likely to
   find one in Tableflow, the Flink sink or the Camel adapter, which is where the portfolio question actually
   sits.
