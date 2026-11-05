#!/bin/bash
# MC-5 reproduction (Specula bug confirmation)
#
# Finding: CommitterImpl elects the commit coordinator only in open(addedPartitions), from a live
# describeConsumerGroups. A classic group that is not Stable is described WITHOUT member assignments,
# so findFirstTopicPartition() == null and the task is not leader (CommitterImpl.java:99-103, 147);
# nothing re-evaluates leadership later (save()/processControlEvents never elect, CommitterImpl.java
# :189-212). Under cooperative rebalancing the task that missed the election keeps partition 0 and
# never gets open() for it again -> no coordinator, no StartCommit, no table commit, silently.
#
# System under test: the PRISTINE sink (apache/iceberg@9fffeb6, kafka-connect main sources unmodified
# -- verified below), embedded Kafka 3.9.2 broker (KRaft) + 2-worker Kafka Connect cluster in one JVM,
# 2 sink tasks, CooperativeStickyAssignor (classic protocol), shared in-memory Iceberg catalog.
#
# Escalation ladder (run in order; stops at the first phase that triggers unless ALL=1):
#   L0     Level 0  pure black box: Connect REST restart of the coordinator host (task owning src-0),
#                   all consumer settings at their defaults.
#   L0hb   Level 0  same, with a normal consumer setting (consumer.override.heartbeat.interval.ms=200)
#                   so the surviving task re-joins promptly (no failpoints, no code change).
#   L1cat  Level 1  timing assistance in the ENVIRONMENT: catalog initialization latency (60-200 ms,
#                   what a REST/Glue/JDBC catalog spends in IcebergSinkTask.start()) for the restarted
#                   task, so its new consumer joins after the survivor's generation, as in production.
#   L1prx  Level 1  as L1cat (500 ms) plus TCP latency (4 s) on new connections to the sink's
#                   iceberg.kafka.bootstrap.servers while the coordinator host restarts.
#   L1kick Level 1  model-counterexample path: the coordinator host is kicked out (catalog latency
#                   stalls its put() past max.poll.interval.ms), its lost-close stops the coordinator,
#                   the other task opens src-0 while the kicked-out task re-joins (+ 3 s bootstrap latency).
#
# Usage: bash test_bugMC-5_coordinator_stall.sh [ICEBERG_SOURCE_TREE]
#        PHASES="L0 L0hb L1prx L1kick" ALL=0 MC5_OUT=<dir>
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPRO_DIR="$HERE/test_bugMC-5_coordinator_stall"
JAVA_DIR="$REPRO_DIR/java"
SRC="${1:-$HERE/../confirmation/MC-5/worktree}"
SRC="$(cd "$SRC" && pwd)"
OUT="${MC5_OUT:-$HERE/MC-5-runs}"
PHASES="${PHASES:-L0 L0hb L1cat L1prx L1kick}"
ALL="${ALL:-0}"
GRADLE_PROJECT=":iceberg-kafka-connect:iceberg-kafka-connect"
mkdir -p "$OUT"

# 1. the system under test must be the pristine sink
if ! git -C "$SRC" diff --quiet HEAD -- kafka-connect/kafka-connect/src/main \
    kafka-connect/kafka-connect-events/src/main; then
  echo "ABORT: sink main sources differ from HEAD:"
  git -C "$SRC" diff --stat HEAD -- kafka-connect/kafka-connect/src/main
  exit 2
fi
untracked="$(git -C "$SRC" ls-files --others --exclude-standard -- kafka-connect/kafka-connect/src/main)"
if [ -n "$untracked" ]; then
  echo "ABORT: untracked files in sink main sources: $untracked"
  exit 2
fi
echo "SUT: $(git -C "$SRC" rev-parse HEAD) -- kafka-connect main sources pristine (git diff empty)"

# 2. install the repro source set (idempotent; only kafka-connect/build.gradle gets a new block)
BUILD_GRADLE="$SRC/kafka-connect/build.gradle"
if ! grep -q ">>> MC-5 repro (Specula) >>>" "$BUILD_GRADLE"; then
  sed "s|@MC5_JAVA_DIR@|$JAVA_DIR|" "$REPRO_DIR/mc5repro.gradle.snippet" >> "$BUILD_GRADLE"
fi

cd "$SRC" || exit 1
echo "==> compiling pristine sink + repro source set (log: $OUT/build.log)"
if ! timeout 2400 ./gradlew "$GRADLE_PROJECT:compileMc5ReproJava" --console=plain -q \
    > "$OUT/build.log" 2>&1; then
  echo "BUILD FAILED"
  tail -40 "$OUT/build.log"
  exit 3
fi

summarize() {
  python3 -I - "$1" <<'PY'
import json, sys
try:
    r = json.load(open(sys.argv[1]))
except Exception as e:
    print(f"  (no result file: {e})"); sys.exit(1)
print(f"  run={r.get('run')} mode={r.get('mode')} level={r.get('level')} "
      f"heartbeat={r.get('heartbeatIntervalMs')} proxyDelayMs={r.get('proxyDelayMs')}")
for it in r.get("iterations", []):
    print(f"  iter {it['iteration']:>2}: host(src-0) before=task{it['coordinatorHostBefore']} "
          f"initLatency={it.get('catalogInitDelayMs', 0)}ms "
          f"after=task{it['ownerOfP0After']} coordThreads={it['coordinatorThreads']} "
          f"startCommitsSinceSettled={it['startCommitsSinceSettled']} group={it['groupAfter']}"
          + (f" TASK FAILED {it['taskFailed']} (not MC-5): {it.get('taskFailedReason', '')[:160]}"
             if it.get('taskFailed') else ""))
if not r.get("stallDetected"):
    print("  RESULT: no stall"); sys.exit(1)
s = r["stall"]; b = s["before"]; a = s["after"]; c = s["rightBugCheck"]
print(f"  STALL after iteration {s['iteration']}: observed {s['observedSec']} s "
      f"({s['sinceSettledSec']} s since the group settled = {s['commitIntervalsMissed']} commit intervals)")
print(f"    coordinatorThreads {b['coordinatorThreads']} -> {a['coordinatorThreads']}, "
      f"StartCommits during stall = {s['startCommitsDuringStall']}")
print(f"    table snapshots {b['table']['snapshots']} -> {a['table']['snapshots']}, "
      f"rows {b['table']['rows']} -> {a['table']['rows']}")
print(f"    produced {b['produced']} -> {a['produced']}; source end offsets {a['sourceEndOffsets']}")
print(f"    source committed offsets {b['sourceCommitted']} -> {a['sourceCommitted']}")
print(f"    tasks {a['tasks']}; group {a['group']}")
print(f"    right-bug: holder of src-0 = task{c.get('holderOfP0')}, opened src-0 in window = "
      f"{c.get('holderOpenedP0InWindow')}, logged 'found no partitions assigned' after it = "
      f"{c.get('holderLoggedNoAssignmentsAfterOpen')}, elected after it = {c.get('holderElectedAfterOpen')}")
for line in c.get("nonStableDescribesInWindow", [])[:12]:
    print(f"    describe: {line}")
h = r.get("healing")
if h:
    print(f"  HEAL (restart holder task{h.get('holderOfP0')}): coordinatorBack={h.get('coordinatorBack')} "
          f"newSnapshot={h.get('newSnapshotAfterHeal')} firstStartCommitAfter={h.get('secondsToFirstStartCommit')} s")
    eo = h.get("exactlyOnce", {})
    print(f"    exactly-once after heal: {eo}")
ok = (a["coordinatorThreads"] == 0 and s["startCommitsDuringStall"] == 0
      and a["table"]["snapshots"] == b["table"]["snapshots"]
      and all(v == "RUNNING" for v in a["tasks"].values())
      and c.get("holderLoggedNoAssignmentsAfterOpen") and not c.get("holderElectedAfterOpen"))
print("  RESULT: " + ("MC-5 STALL TRIGGERED (right mechanism)" if ok else "stall seen but checks incomplete"))
sys.exit(0 if ok else 1)
PY
}

triggered=""
for phase in $PHASES; do
  case "$phase" in
    L0)     args=(-Dmc5.mode=restart -Dmc5.level=0 -Dmc5.iterations=12) ;;
    L0hb)   args=(-Dmc5.mode=restart -Dmc5.level=0 -Dmc5.iterations=20 -Dmc5.heartbeat.ms=200
                  -Dmc5.commit.timeout.ms=6000) ;;
    L1cat)  args=(-Dmc5.mode=restart -Dmc5.level=1 -Dmc5.iterations=16 -Dmc5.heartbeat.ms=200
                  -Dmc5.commit.timeout.ms=6000 -Dmc5.catalog.init.delays=60,100,150,200) ;;
    L1prx)  args=(-Dmc5.mode=restart -Dmc5.level=1 -Dmc5.iterations=6 -Dmc5.heartbeat.ms=200
                  -Dmc5.commit.timeout.ms=6000 -Dmc5.catalog.init.delays=500
                  -Dmc5.proxy.delay.ms=4000) ;;
    # commit interval (15 s) > host stall (7.5 s): like the counterexample, no StartCommit is pending
    # when the kicked-out host wakes up (otherwise its worker answers it and the broker rejects the
    # stale-generation offset commit -> task FAILED, a different path)
    L1kick) args=(-Dmc5.mode=kickout -Dmc5.level=1 -Dmc5.iterations=3 -Dmc5.heartbeat.ms=200
                  -Dmc5.commit.interval.ms=15000 -Dmc5.commit.timeout.ms=20000
                  -Dmc5.proxy.delay.ms=3000 -Dmc5.max.poll.interval.ms=6000
                  -Dmc5.kickout.extra.ms=1500) ;;
    # optional control (not part of the default ladder): same trigger as L1prx under the EAGER
    # protocol (RangeAssignor) -- every rebalance re-opens all partitions, so a missed election
    # should be repaired by the next generation (no persistent stall expected)
    CTLeager) args=(-Dmc5.mode=restart -Dmc5.level=1 -Dmc5.iterations=6 -Dmc5.heartbeat.ms=200
                  -Dmc5.commit.timeout.ms=6000 -Dmc5.proxy.delay.ms=4000 -Dmc5.assignor=range) ;;
    *) echo "unknown phase $phase"; continue ;;
  esac
  # optional extra -D settings, e.g. MC5_EXTRA_ARGS="-Dmc5.stall.observe.sec=330"
  read -r -a extra <<< "${MC5_EXTRA_ARGS:-}"
  rm -f "$OUT/$phase.sut.log" "$OUT/$phase.result.json"
  echo ""
  echo "==> phase $phase: ${args[*]} ${extra[*]:-}"
  start=$(date +%s)
  timeout 1800 ./gradlew "$GRADLE_PROJECT:mc5ReproTest" \
      --tests org.apache.iceberg.connect.mc5.Mc5CoordinatorStallTest \
      -Dmc5.run.name="$phase" -Dmc5.out.dir="$OUT" -Dmc5.sut.log="$OUT/$phase.sut.log" \
      "${args[@]}" ${extra[@]+"${extra[@]}"} --console=plain > "$OUT/$phase.gradle.log" 2>&1
  rc=$?
  echo "  gradle rc=$rc ($(( $(date +%s) - start )) s; log $OUT/$phase.gradle.log)"
  [ "$rc" -eq 124 ] && echo "  TIMEOUT (possible hang in the system under test)"
  if summarize "$OUT/$phase.result.json"; then
    triggered="$triggered $phase"
    [ "$ALL" = "1" ] || break
  fi
done

echo ""
if [ -n "$triggered" ]; then
  echo "MC-5 REPRODUCED in phase(s):$triggered (pristine sink; outputs in $OUT)"
  exit 0
fi
echo "MC-5 NOT TRIGGERED in phases: $PHASES (outputs in $OUT)"
exit 1
