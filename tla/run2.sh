#!/bin/bash
# usage: tla/run2.sh <area> <Spec> <cfg>  -> writes tla/<area>/out/<cfg>.log ; prints a one-line summary
J=/Users/I074560/homebrew/Cellar/openjdk@21/21.0.11/libexec/openjdk.jdk/Contents/Home/bin
TOOLS=/Users/I074560/Downloads/experiments/redis-lean/tla/tools/tla2tools.jar
cd "$(dirname "$0")/$1" || exit 1; mkdir -p out states
$J/java -Xmx2g -XX:+UseParallelGC -cp $TOOLS tlc2.TLC -workers 2 -deadlock -metadir states/$3 -config $3.cfg $2 > out/$3.log 2>&1
echo "== $3: $(grep -E 'Invariant .* is violated|Temporal properties were violated|No error has been found|distinct states found|Error:' out/$3.log | grep -v 'The behavior up to' | head -3 | tr '\n' ' ')"
