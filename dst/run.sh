#!/usr/bin/env bash
# Replay one seed, or sweep a range, outside the test runner.
#   ./run.sh 42                              one seed, default faults
#   ./run.sh 42 quiet                        one seed, no faults
#   ./run.sh sweep 1000 aggressive           a seed sweep
#   ./run.sh sweep 1000 default RETRY_ON_UNKNOWN_STATE   with a pre-fix flag
# ICEBERG_VERSION picks the release to test against (default: gradle.properties).
set -euo pipefail
cd "$(dirname "$0")"
V="${ICEBERG_VERSION:-}"
GP=""
if [ -n "$V" ]; then GP="-PicebergVersion=$V"; fi
# shellcheck disable=SC2086
./gw -q compileJava $GP
# shellcheck disable=SC2086
./gw -q printClasspath $GP 2>/dev/null | tail -1 > build/cp.txt
# shellcheck disable=SC2086
./gw -q icebergVersion $GP 2>/dev/null | tail -1 > build/version.txt
# Guard against a half-switched build: the classpath and the compiled classes must both be the
# release that was asked for, or a sweep would silently measure the wrong jar.
WANT="$(cat build/version.txt)"
if ! grep -q "iceberg-core-$WANT.jar" build/cp.txt; then
  echo "run.sh: classpath does not contain iceberg-core-$WANT.jar; refusing to run" >&2
  exit 1
fi
JAVA_HOME="$(./gw -q javaHome 2>/dev/null | tail -1)"
export PATH="$JAVA_HOME/bin:$PATH"
exec java -Diceberg.worker.num-threads=1 \
  ${DST_COMMIT_UUID:+-Ddst.commitUuid=$DST_COMMIT_UUID} \
  ${DST_ROUND_TRIP:+-Ddst.metadataRoundTrip=$DST_ROUND_TRIP} \
  -Dorg.slf4j.simpleLogger.defaultLogLevel=error \
  -DicebergVersion="$(cat build/version.txt)" \
  -cp "$(cat build/cp.txt)" org.apache.iceberg.dst.Main "$@"
