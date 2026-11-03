#!/usr/bin/env bash
# Plain-java runner (same flags as the gradle run task): jrun.sh <icebergVersion> <mainClass> args...
# Requires: ./gw -q printClasspath -PicebergVersion=<v> > build/cp-<v>.txt and classes in build/alt.
cd "$(dirname "$0")"
v="$1"; shift
J=/Users/I074560/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home/bin/java
exec "$J" -Xmx2g -Dorg.slf4j.simpleLogger.defaultLogLevel=error \
  '-Dorg.slf4j.simpleLogger.log.org.apache.iceberg.util.LockManagers$InMemoryLockManager=off' \
  -DicebergVersion="$v" -cp "build/alt:$(cat build/cp-$v.txt)" "$@"
