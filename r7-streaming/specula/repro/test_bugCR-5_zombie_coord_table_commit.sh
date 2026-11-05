#!/usr/bin/env bash
# CR-5 reproduction driver (bug-confirmation Phase 2).
#
# Finding: a superseded (fenced) coordinator keeps consuming, keeps -coord membership and can still
# run its Iceberg CAS; hypothesis: two live coordinators skip, repeat or misattribute a commit.
#
# Runs the PRISTINE sink (apache/iceberg@9fffeb6, kafka-connect tree == HEAD, verified below) inside
# an in-JVM KRaft broker + 2-worker Kafka Connect cluster (EmbeddedConnectCluster). Two live
# coordinators are created through the real code path (transient task failure + REST restart ->
# eager rebalance -> close() describes a rebalancing group -> the old coordinator is not stopped,
# upstream #17637). Timing aid (Level 1, environment only): a slow catalog commit call.
#
# Scenarios (JUnit methods in test_bugCR-5_zombie_coord_table_commit.java):
#   A  zombieCasLandsAfterFencing                         zombie's CAS lands after the successor's initTransactions()
#   B  successorCasLandsFirst                             successor commits the same DataWritten first, zombie CAS races
#   C  zombieConsumesSuccessorAnswersTwoControlPartitions 2 control partitions, zombie consumes successor's answers
#
# Usage: bash test_bugCR-5_zombie_coord_table_commit.sh            (SCENARIOS="A B C" by default)
#        SCENARIOS="A" bash test_bugCR-5_zombie_coord_table_commit.sh
# Exit code: 0 = ran, hypothesized consequence NOT observed; 1 = consequence observed (bug);
#            2 = setup/build failure; 3 = a scenario did not reach its precondition (inconclusive).
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
OUT_ROOT="$(cd "$HERE/.." && pwd)"                          # .specula-output
WT="${CR5_WORKTREE:-$OUT_ROOT/confirmation/CR-5/worktree}"   # git worktree of the source repo
OUT="$HERE/CR-5.out"
SNIPPET="$OUT_ROOT/confirmation/CR-5/notes/cr5Repro.gradle.snippet"
SCENARIOS="${SCENARIOS-A B C}"   # SCENARIOS="" = setup + build only
PROJECT=":iceberg-kafka-connect:iceberg-kafka-connect"
TEST_CLASS="org.apache.iceberg.connect.cr5.TestBugCR5ZombieCoordinator"
mkdir -p "$OUT"

echo "==> CR-5 reproduction, worktree $WT"
cd "$WT" || exit 2

# 1. pristine sink: drop any trace instrumentation in kafka-connect/ (only this subtree is touched)
git checkout HEAD -- kafka-connect/kafka-connect/src/main kafka-connect/build.gradle || exit 2
git clean -fdq -- kafka-connect/kafka-connect/src/main kafka-connect/kafka-connect/src/tlaHarness \
  kafka-connect/kafka-connect/src/cr5Repro || exit 2
if ! git diff --quiet HEAD -- kafka-connect; then
  echo "kafka-connect tree differs from HEAD after restore"; exit 2
fi
echo "    kafka-connect/ == HEAD ($(git rev-parse --short HEAD)); sink main sources pristine"

# 2. test-only source set + the scenarios
cat "$SNIPPET" >> kafka-connect/build.gradle
DEST=kafka-connect/kafka-connect/src/cr5Repro/java/org/apache/iceberg/connect/cr5
mkdir -p "$DEST"
cp "$HERE/test_bugCR-5_zombie_coord_table_commit.java" "$DEST/TestBugCR5ZombieCoordinator.java"
git diff --stat HEAD -- kafka-connect | tail -1
if ! git diff --quiet HEAD -- kafka-connect/kafka-connect/src/main; then
  echo "main sources changed?!"; exit 2
fi

echo "==> Building (compileCr5ReproJava)"
if ! timeout 1500 ./gradlew "$PROJECT:compileCr5ReproJava" --console=plain -q > "$OUT/build.log" 2>&1; then
  echo "BUILD FAILED"; tail -40 "$OUT/build.log"; exit 2
fi

status=0
for s in $SCENARIOS; do
  case "$s" in   # bash 3.2 (macOS): no associative arrays
    A) m=zombieCasLandsAfterFencing; n=A_zombie_cas_lands_first ;;
    B) m=successorCasLandsFirst; n=B_successor_cas_lands_first ;;
    C) m=zombieConsumesSuccessorAnswersTwoControlPartitions; n=C_zombie_consumes_two_ctl_partitions ;;
    *) echo "unknown scenario $s"; continue ;;
  esac
  rm -f "$OUT/$n.summary.json" "$OUT/$n.sink.log"
  echo ""
  echo "==> Scenario $s ($m)"
  # a hung scenario is itself a finding: no retry
  timeout 900 ./gradlew "$PROJECT:cr5ReproTest" --tests "$TEST_CLASS.$m" \
      -Dcr5.out.dir="$OUT" -Dcr5.log.file="$OUT/$n.sink.log" --console=plain > "$OUT/$n.gradle.log" 2>&1
  rc=$?
  [ "$rc" -eq 124 ] && echo "    TIMEOUT (possible deadlock) in scenario $s"
  grep -E "CR5\| " "$OUT/$n.gradle.log" | sed 's/^/    /' | cut -c1-600
  grep -E "PASSED|FAILED|BUILD (SUCCESSFUL|FAILED)" "$OUT/$n.gradle.log" | head -3 | sed 's/^/    gradle: /'
  if [ ! -f "$OUT/$n.summary.json" ]; then
    echo "    no summary written (rc=$rc)"; status=$(( status > 2 ? status : 2 )); continue
  fi
  # evidence from the sink's own log (pristine code, INFO for org.apache.iceberg.connect)
  if [ -f "$OUT/$n.sink.log" ]; then
    echo "    --- sink log evidence ($n.sink.log)"
    grep -E "cannot determine leader|lost leader partition|elected leader|was fenced by a newer coordinator|Coordinator error during process|ProducerFencedException|stale offsets|found nothing to commit|Partial commit .* failed|Coordinator unexpectedly terminated" \
      "$OUT/$n.sink.log" | grep -v "^\s*at " | cut -c1-330 | head -40 | sed 's/^/      /'
  fi
  python3 -I - "$OUT/$n.summary.json" "$s" "$OUT/$n.sink.log" <<'PY'
import json, sys, os
summary = json.load(open(sys.argv[1])); scen = sys.argv[2]; log = sys.argv[3]
logtext = open(log).read() if os.path.exists(log) else ""
def n(k): return len(summary.get(k, []))
harm = {k: n(k) for k in ["duplicates", "missingBelowCommitted", "missingProduced", "filesAddedTwice", "floorViolations", "strandedDataWritten"]}
cT = summary.get("cThreadFirstSeenMs")
commits = summary.get("connectorCommitAttempts", [])
zombie_landed = [c for c in commits if c.get("taskId") == "iceberg-sink-1" and c.get("outcome") == "LANDED" and cT is not None and c.get("endMs", 0) > cT]
zombie_lost = [c for c in commits if c.get("taskId") == "iceberg-sink-1" and c.get("outcome") != "LANDED" and cT is not None and c.get("endMs", 0) > cT]
print(f"    --- evaluation scenario {scen}: runResult={summary.get('runResult')} maxLiveCoordinators={summary.get('maxLiveCoordinatorThreads')}")
print(f"    harm counters: {harm}")
print(f"    rows={summary.get('rowsInTable')} produced={summary.get('produced')} sourceCommitted={summary.get('sourceCommitted')}")
for c in zombie_landed:
    print(f"    ZOMBIE CAS LANDED after the successor started: snapshot={c['snapshotId']} end=+{c['endMs']}ms > C first seen +{cT}ms, files={len(c['addedFiles'])}, announcedByCommitToTable={c.get('announcedByCommitToTable')}, rounds={c.get('filesFromRounds')}")
for c in zombie_lost:
    print(f"    zombie CAS attempt NOT landed after the successor started: outcome={c['outcome']} end=+{c['endMs']}ms files={len(c['addedFiles'])} error={str(c.get('error'))[:160]}")
if scen in ("A", "C"):
    pre = summary.get("maxLiveCoordinatorThreads", 0) >= 2 and len(zombie_landed) > 0
elif scen == "B":
    pre = summary.get("maxLiveCoordinatorThreads", 0) >= 2 and len(zombie_lost) > 0 and "stale offsets" in logtext
else:
    pre = False
same = []
if scen == "B":
    landed_c = [c for c in commits if c.get("taskId") == "iceberg-sink-0" and c.get("outcome") == "LANDED"]
    for z in zombie_lost:
        for c in landed_c:
            common = set(z["addedFiles"]) & set(c["addedFiles"])
            if common:
                same.append((c["snapshotId"], len(common)))
    print(f"    successor snapshots that landed the files the zombie tried to add: {same}")
print(f"    precondition reached: {pre}")
bad = any(v > 0 for v in harm.values())
print(f"    VERDICT-LINE scenario {scen}: " + ("CONSEQUENCE OBSERVED (skip/repeat/inconsistent offsets)" if bad else "no skip / repeat / offset-file inconsistency") + ("" if pre else " [precondition NOT reached: inconclusive]"))
sys.exit(1 if bad else (0 if pre else 3))
PY
  ev=$?
  [ "$ev" -eq 1 ] && status=1
  [ "$ev" -eq 3 ] && [ "$status" -eq 0 ] && status=3
done

echo ""
echo "==> overall exit status $status (0 = ran, consequence not observed; 1 = consequence observed; 3 = inconclusive)"
exit $status
