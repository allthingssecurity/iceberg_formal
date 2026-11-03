#!/usr/bin/env bash
# Reproduces every table in findings/r5-races.md. Results go to build/r5/*.txt (one line per run).
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p build/r5 build/alt
for v in 1.11.0 1.12.0; do ./gw -q printClasspath -PicebergVersion=$v > build/cp-$v.txt; done
J=/Users/I074560/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home/bin
"$J/javac" -nowarn --release 17 -d build/alt -cp "$(cat build/cp-1.12.0.txt)" \
  src/main/java/races/*.java src/main/java/org/apache/iceberg/actions/*.java
mkdir -p build/alt-1.11.0
"$J/javac" -nowarn --release 17 -d build/alt-1.11.0 -cp "$(cat build/cp-1.11.0.txt)" src/main/java/races/Calibration.java

# Step 0: calibration gate (#16686), 1.11.0 must fail, 1.12.0 must pass; threads=1 is the control
for v in 1.11.0 1.12.0; do for t in 8 1; do
  ./gw -q run -PicebergVersion=$v -Pmain=races.Calibration -Pargs="50 32 2000 $t" | grep RESULT
done; done > build/r5/calibration.txt

# B-122
for sh in fixed varied; do for m in concurrent sequential; do
  ./jrun.sh 1.12.0 races.VariantRace $m 50 2 20000 $sh | grep -E "^RESULT|^EXAMPLE"
done; done > build/r5/variant.txt

# B-19/B-20 direct mutual exclusion (3 x 30 s per config)
for c in prod fast stall mixed; do for i in 1 2 3; do
  ./jrun.sh 1.12.0 races.LockRace $c 8 2 30 10000 | grep RESULT
done; done > build/r5/lockrace.txt

# B-19/B-20 user-visible effect: real HadoopTableOperations commits, lost commits
for c in stall fast; do for i in $(seq 1 10); do
  ./jrun.sh 1.12.0 races.HadoopLockCommit $c 4 100 | grep RESULT
done; done > build/r5/hadoop-lock-commit.txt

# B-24
for a in "4 30 2000 off" "1 30 2000 off" "4 30 0 off" "4 60 2000 on" "0 60 2000 on"; do
  ./jrun.sh 1.12.0 races.RefreshRace $a | grep -E "RESULT|SHARED"
done > build/r5/refresh.txt

# B-133 / D-10
for a in "noop 8 20000 20" "noop 1 20000 20" "commit 8 25 40" "commit 1 200 10"; do
  ./jrun.sh 1.12.0 org.apache.iceberg.actions.CounterRace $a | grep RESULT
done > build/r5/counter.txt

# C-54
for a in "sequential 200 2 fresh" "concurrent 2000 4 fresh" "concurrent 20000 4 shared" \
         "concurrent 20000 4 shared" "concurrent 20000 4 shared" "concurrent 20000 4 shared" \
         "sequential 20000 4 shared" "sequential 20000 4 shared"; do
  ./jrun.sh 1.12.0 races.SasPolicyRace $a | grep RESULT
done > build/r5/sas.txt
