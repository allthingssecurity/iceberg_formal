#!/bin/bash
# usage: tla/run.sh <cfg>...|all   regenerates .cfg files, refuses unknown names, runs TLC (log capped)
set -euo pipefail
cd "$(dirname "$0")"
J=/Users/I074560/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home/bin
TOOLS=/Users/I074560/Downloads/experiments/redis-lean/tla/tools/tla2tools.jar
python3 gencfg.py .
if [ "$1" = all ]; then names=$(cat cfglist.txt); else names="$*"; fi
mkdir -p out states
for n in $names; do
  python3 -c "import sys; ns={}; exec(open('configs.py').read(), ns); ns['get'](sys.argv[1])" "$n"
  $J/java -Xmx3g -XX:+UseParallelGC -cp $TOOLS tlc2.TLC -workers 4 -deadlock -metadir states/$n -config $n.cfg KcSink > out/$n.full 2>&1 || true
  head -c 300000 out/$n.full > out/$n.log; rm -f out/$n.full; rm -rf states/$n
  echo "== $n: $(grep -E 'Invariant .* is violated|Assumption .* is false|No error has been found|distinct states found|^Error:' out/$n.log | grep -v 'The behavior up to' | head -3 | tr '\n' ' ')"
done
