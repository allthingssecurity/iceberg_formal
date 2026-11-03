# Round 6 rules (shared by every r6 track)

Workspace: /Users/I074560/Downloads/experiments/iceberg-verify. Your track writes ONLY under r6/<track>/ and
findings/r6-<track>.md.

Hard constraints
- iceberg/ is a READ-ONLY reference checkout (apache/iceberg main @ 9fffeb63b16, shallow). Read it; never modify it,
  switch its branch, or build it. Test against RELEASED jars from Maven Central (pick versions with -PicebergVersion).
- Do not touch dst/, r5/, tla/commit, tla/lease, spin/, upstream/, or the iceberg-pr* clones (copy code out if useful).
- No git commits anywhere. Nothing filed, posted, commented or opened upstream (no gh issue/pr/comment). `gh` read-only
  lookups of issues/PRs are fine.
- JDK 21: /Users/I074560/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home. Copy the `gw` launcher pattern from
  r5/catalogs/gw (Gradle wrapper + JDK finder; copy r5/catalogs/gradle/ and gradlew too). Never run the iceberg/ gradlew.
- TLA+: TLC is run via tla/run2.sh (read it and tla/lease/run.sh for the jar location and conventions); put your own
  copy of a runner under r6/<track>/.

Method rules
- Calibrate first: before trusting a harness, it must FAIL on the release before a known fix and PASS on the first
  release containing it. Report both counts. A harness that never caught a known bug proves nothing by passing.
- Every positive needs a control run where the bad outcome does not occur (same harness, fault off or fixed release).
- Cap every log (head/tail, or bounded files, no multi-MB outputs). Unknown config/mode names must throw, never fall back.
- Attribute faults structurally (exception class, injected-fault id, call site), never by matching exception text.
- Measure what actually happened (files on disk, rows read back, snapshots in metadata, offsets committed), not what
  was called.
- Status of every claim is explicit: confirmed / candidate / refuted, with the tool that decided it.
- Before claiming "new", search upstream issues/PRs (gh search, read-only) for duplicates and record the result.
- Severity: Major = data loss, duplicates, corruption, wrong results, lost/duplicated commit. Minor = everything else.
