#!/usr/bin/env bash
# Runs every row of the recall scorecard and writes the raw numbers to build/scorecard.txt.
# Each row is one sweep; the control row for a pre-fix flag is the same sweep with the flag off.
set -uo pipefail
cd "$(dirname "$0")"
SEEDS="${SEEDS:-1000}"
OUT=build/scorecard.txt
mkdir -p build
: > "$OUT"

row() {
  local label="$1"; shift
  echo "---- $label" | tee -a "$OUT"
  ./run.sh sweep "$@" 2>/dev/null | grep -E '^mode=' | tee -a "$OUT"
}

echo "iceberg=${ICEBERG_VERSION:-from gradle.properties}  seeds=$SEEDS  $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee -a "$OUT"

row "control: faithful workload, default faults"        "$SEEDS" default
row "control: faithful workload, aggressive faults"     "$SEEDS" aggressive
row "control: faithful workload, no faults"             "$SEEDS" quiet
row "A pre-fix: landed commit reported as plain failure" "$SEEDS" default RETRY_ON_UNKNOWN_STATE
row "A pre-fix: same, catalog faults only"              "$SEEDS" catalog-only RETRY_ON_UNKNOWN_STATE
row "control: catalog faults only"                     "$SEEDS" catalog-only
row "control: a table with history (faithful rewrite above replaced seq)" "$SEEDS" deephistory
row "control: same, no faults"                         "$SEEDS" deephistory-quiet
row "B pre-fix: cleanup on a non-cleanable failure"     "$SEEDS" nonstrict
row "C precondition: duplicate snapshot id handed out"  "$SEEDS" default DUPLICATE_SNAPSHOT_ID
row "D pre-fix: rewrite with no starting sequence number" "$SEEDS" default REWRITE_WITHOUT_SEQUENCE_NUMBER
row "E live: rewrite supplies a sequence number ahead"  "$SEEDS" seqahead
row "E live: same, no faults at all"                    "$SEEDS" seqahead-quiet
row "G pre-fix: duplicate-delete count across retries (needs ICEBERG_VERSION=1.11.0)" "$SEEDS" dupdelete
row "maintenance: snapshot expiry concurrent with writers" "$SEEDS" expiry
row "maintenance: same, no faults"                     "$SEEDS" expiry-quiet
row "maintenance: orphan clean, cutoff 0 steps"        "$SEEDS" orphan0
row "maintenance: orphan clean, cutoff 10 steps"       "$SEEDS" orphan10
row "maintenance: orphan clean, cutoff 20 steps"       "$SEEDS" orphan20
row "maintenance: orphan clean, cutoff 30 steps"       "$SEEDS" orphan30
row "maintenance: orphan clean, cutoff 200 steps"      "$SEEDS" orphan200
row "F pre-fix: catalog pointer is not atomic"          "$SEEDS" nonatomic
row "F pre-fix: same, no faults"                        "$SEEDS" nonatomic-quiet

echo "done -> $OUT"
