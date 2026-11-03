#!/bin/bash
# usage: spin/run.sh <model.pml> <name> "<-D defines for spin and cc>" "<pan flags>" [liveness]
# Builds in <modeldir>/build/<name>, keeps the trail (if any) as <modeldir>/<name>.trail, appends a line to
# <modeldir>/results.txt. Gate: 1-min load < 12, disk > 19 GB. pan memory capped (-DMEMLIM=2048), timeout 1200.
set -u
SPIN=~/homebrew/bin/spin
model=$(cd "$(dirname "$1")" && pwd)/$(basename "$1"); name=$2; defs=${3:-}; panflags=${4:-}; live=${5:-}
dir=$(dirname "$model"); b=$dir/build/$name
while :; do
  load=$(sysctl -n vm.loadavg | awk '{print int($2)}')
  free=$(df -g /Users/I074560 | awk 'NR==2{print $4}')
  if [ "$load" -lt 12 ] && [ "$free" -gt 19 ]; then break; fi
  echo "gate: load=$load free=${free}G, waiting" >&2; sleep 30
done
rm -rf "$b"; mkdir -p "$b"; cp "$model" "$b/m.pml"; cd "$b" || exit 1
$SPIN -a $defs m.pml > spin.out 2>&1 || { echo "$name: SPIN ERROR $(tail -3 spin.out)" | tee -a "$dir/results.txt"; exit 1; }
if [ -n "$live" ]; then cflags="-O2 -DMEMLIM=2048 $defs"; else cflags="-O2 -DSAFETY -DMEMLIM=2048 $defs"; fi
cc $cflags -o pan pan.c 2> cc.out || { echo "$name: CC ERROR"; cat cc.out; exit 1; }
start=$(date +%s.%N 2>/dev/null || date +%s)
( ulimit -f 2000000; timeout 1200 ./pan -m100000 $panflags > pan.out 2>&1 )
end=$(date +%s.%N 2>/dev/null || date +%s)
errs=$(grep -o "errors: [0-9]*" pan.out | head -1)
states=$(grep -o "[0-9.e+]* states, stored" pan.out | head -1)
depth=$(grep -o "depth reached [0-9]*" pan.out | head -1)
kind=$(grep -E "^pan:[0-9]+: " pan.out | tail -1 | sed -E 's/^pan:[0-9]+: //; s/ \(at depth ([0-9]+)\)/ @depth \1/')
t=$(grep -o "pan: elapsed time [0-9.e+-]* seconds" pan.out | head -1 | awk '{print $4}')
line="$name [$defs] [$panflags]: $errs; $kind; $states; $depth; ${t}s"
if [ -f m.pml.trail ]; then
  cp m.pml.trail "$dir/$name.trail"
  $SPIN -t -p -g -l $defs m.pml > trail.txt 2>&1
  steps=$(grep -c "proc .* (" trail.txt)
  line="$line; trail $steps steps"
fi
echo "$line" | tee -a "$dir/results.txt"
