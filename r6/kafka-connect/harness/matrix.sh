#!/bin/bash
# Runs the whole harness matrix; appends SUMMARY lines to ../results.txt (one line per config).
# usage: ./matrix.sh [N]     (N runs per config, default 5)
cd "$(dirname "$0")"
N=${1:-5}
OUT=../results.txt
: > $OUT
run() { V=$1; shift; ./run.sh $V "$1" $N "${@:2}" 2>/dev/null | grep SUMMARY >> $OUT; }
for v in 1.8.1 1.9.0 1.10.0 1.11.0 1.12.0; do
  run $v smoke
  run $v icr-close                       # calibration 1 (#12372, fixed in 1.9.0)
done
for v in 1.10.0 1.11.0 1.12.0; do
  run $v zombie-rewind replay=1          # calibration 2 (#16282 / #17340, fixed in 1.12.0 by #17933)
  run $v zombie-rewind replay=0
done
for v in 1.9.0 1.10.0 1.11.0 1.12.0; do
  run $v icr-nocommit                    # F1 trigger
  run $v icr-nocommit precommit=1        # F1 control: partition already has a committed offset
  run $v icr-nocommit eager=1            # F1 control: eager protocol
  run $v coord-latest                    # F2 trigger
  run $v coord-latest precommit=1        # F2 control: coord group has a checkpoint
  run $v coord-latest reset=earliest     # F2 control: user override
  run $v fault kind=RTE_BEFORE precommit=0   # F2 via first-commit failure
  run $v fault kind=RTE_BEFORE precommit=1
done
for v in 1.11.0 1.12.0; do
  for k in CSUE_AFTER CSUE_BEFORE CRASH_AFTER CRASH_BEFORE RTE_AFTER; do
    run $v fault kind=$k precommit=1
    run $v fault kind=$k precommit=1 tables=2
  done
  run $v fault kind=CFE_BEFORE count=5 precommit=1
  run $v fault kind=CFE_BEFORE count=5 precommit=1 tables=2
  run $v fault kind=RTE_BEFORE precommit=1 tables=2
  run $v fault kind=CRASH_AFTER precommit=0
done
echo done >> $OUT
