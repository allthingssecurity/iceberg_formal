#!/usr/bin/env bash
# Targeted write-path faults on 1.12.0 (each prints FAULT and CONTROL arms). Output capped per spec.
set -u
cd "$(dirname "$0")"
for spec in \
  "CreateMultipartUpload:DROP_RESP:data:1:1:0" \
  "CompleteMultipartUpload:DROP_RESP:data:1:1:0" \
  "CompleteMultipartUpload:CUT_REQ:data:1:1:100" \
  "UploadPart:CUT_REQ:data:1:8:1000000" \
  "UploadPart:CUT_REQ:data:1:8:1000000,AbortMultipartUpload:DROP_RESP:data:1:1:0" \
  "CreateMultipartUpload:CUT_REQ:data:1:8:0" \
  "PutObject:DROP_RESP:metadata:1:1:0" \
  "PutObject:CUT_REQ:manifest:1:8:1000" \
  "DeleteObjects:DROP_RESP:any:1:1:0" \
  "DeleteObjects:CUT_REQ:any:1:8:10"; do
  extra=""
  case "$spec" in DeleteObjects*) extra="-Dos.expire=true -Dos.commits=4";; esac
  ./gw -q harness -Dos.mode=target -Dos.spec="$spec" -Dos.runs=5 $extra 2>&1 | grep -v "^Note\|^details" | cut -c1-900 | head -8
done
