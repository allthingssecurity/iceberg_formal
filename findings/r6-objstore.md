# r6 objstore: real S3FileIO under connection drops (2026-11-03)

Harness: `r6/objstore/` (own Gradle project; `gw`/`gradlew`/`gradle/` copied from r5/catalogs). Released jars only:
`iceberg-{api,core,common,aws,aws-bundle,bundled-guava}` (1.12.0 unless stated), `org.xerial:sqlite-jdbc:3.53.4.0`,
and for `http-client.type=urlconnection` on 1.12.0 `software.amazon.awssdk:url-connection-client:2.54.17` (the 1.12.0
bundle ships SDK 2.54.17 with the apache5 client only). Releases before 1.4 get explicit SDK modules (build.gradle).
No binaries, no pip, no upstream writes, no commits.

| id | status | severity | decided by | one line |
|---|---|---|---|---|
| OS-1 | **confirmed** | **Major** | real JdbcCatalog + S3FileIO + S3Mock + fault proxy, clean oracle | With `http-client.type=urlconnection` a GET that the server closes early (FIN) reads as a clean EOF. Avro's `DataFileStream.hasNext` swallows the resulting EOFException. A manifest-merge commit then writes a merged manifest **without the entries it did not read** and commits it as OK. Committed files vanish from the table. Targeted: 5/5 at a block boundary, 20/20 at a random offset, 5/5 on 1.0.0 with its default client; also found by unguided random faults (1 of 40 runs). Controls: apache5 client 0/5 at the boundary and 0/20 at random offsets (the commit throws), fault off 0/5 and 0/20. |
| OS-1b | **confirmed** | **Major** (wrong results) | same | Same truncation during `planFiles()`: the scan silently returns fewer files. urlconnection 5/5; apache5 0/5 (scan throws); fault off 0/5. |
| OS-2 | confirmed; duplicate of #9674/#9679 (fix in **open** PR #15792) | Minor | same | A premature FIN on a sequential read raises `ConnectionClosedException`, which is not retried. A connection reset at the same offset is retried. FIN 5/5 threw, RESET 0/5 threw, control 0/5. |
| OS-3 | confirmed | Minor | same | `readFully`/`readTail` (RangeReadable) have no retry at all: a reset at the same offset 5/5 threw (seq read 0/5), control 0/5. |
| OS-4 | confirmed | Minor | same | When `CreateMultipartUpload` fails inside `write()`, try-with-resources `close()` sends a single PutObject with the partial staging content and returns normally. This leaves a **truncated object** at the target path (e.g. 5,318,079 of 8,298,831 B). 5/5; control 0/5. Not referenced by metadata. |
| OS-5 | confirmed | Minor | same | When the `CreateMultipartUpload` ack is lost, the SDK retry opens a second upload and the first is never aborted: 5/5 runs leave one incomplete upload (ListMultipartUploads); control 0/5. |
| OS-6 | confirmed (1 hit) | Minor | same | An abort whose ack is lost: the abort retry gets `NoSuchUploadException`, which replaces the original part-upload failure (1/1 runs where the abort fault fired; control 0/5). |
| OS-7 | refuted on 1.12.0 | none | same | No missing, truncated, or zero-length referenced file, and no lost or phantom commit, under PUT/UploadPart/Complete/Create/Delete cuts, lost acks, and stalls with the apache5 client: 80 random runs + 10 targeted specs × 5. Orphans appear only after failures. |

## Setup
- **S3 server**: Adobe S3Mock `com.adobe.testing:s3mock:5.2.3` (Spring Boot 4.1, Maven Central). It runs as a child JVM with its own classpath (`S3Server.java`). Its SDK/Spring therefore never shares a classloader with the Iceberg release under test, which lets one harness run 1.0.0 through 1.12.0. It is destroyed on exit and by a shutdown hook.
  - Used and working: PutObject, multipart Create/UploadPart/Complete/Abort, ListMultipartUploads, DeleteObjects, ranged GET, HEAD.
  - Ranged GETs return no `x-amz-checksum-*` (same as AWS), so SDK response-checksum validation is not active for S3InputStream reads.
  - Not covered: HTTP only, so the `SSLException` retry paths are not exercised. No 503/SlowDown throttling. Retried Complete returns 200 (same as AWS). Retried Abort returns NoSuchUpload (believed to match AWS, not verified).
- **Fault layer**: `FaultProxy.java`, an in-JVM HTTP/1.1-framing TCP proxy. It handles Content-Length, chunked, and `100-continue`. It classifies each request into an S3 op from method/path/query (op, key, uploadId, partNumber, Range).
  - A seeded `Decider` returns one of: CUT_REQ(K body bytes then RST), DROP_RESP (server applies the request, ack swallowed, RST), STALL_RESP (4.5 s, client socket timeout 3 s), RESET_RESP(K), FIN_RESP(K).
  - Each fault gets an id, recorded with the parsed op/key and the server status seen. Attribution uses these records, not exception text.
- **Workload** (`Run.java`): JdbcCatalog on SQLite, warehouse `s3://bucket/<run>`, format v2, merge min-count 3.
  - Each run does 4–7 appends and row deltas. Blobs go through `S3OutputStream` with `s3.multipart.part-size-bytes=5MiB`, `s3.multipart.threshold=1.0`, and sizes from 1 B to 11 MiB, so both single PUT and multipart are used.
  - 20 binary columns with 8–16 KiB bounds make manifests 0.3–7 MiB (single PUT >128 KiB, and multipart).
  - Checksum on/off and expireSnapshots on/off are randomised.
- **Oracle** (clean client straight to S3Mock): it walks every snapshot (manifest list → manifests → files). For each it checks existence, recorded length, full-content CRC against the write ledger, and the manifest entry count against the manifest-list counts.
  - Model check: an op that returned OK must have its files live; an op that threw must not.
  - Model-free check: snapshot `total-data-files` equals the live entries.
  - It also counts orphans by class, incomplete multipart uploads, and leftover staging files.
  - A read phase through the **faulty** IO compares `planFiles()`, sequential reads, `readFully` and `readTail` with the truth.

## Calibration
- `#5282` (single-PUT retry → "Resetting to invalid mark"; first in 1.1.0, compare=ahead; not in 1.0.0). CUT_REQ after 200 KiB on the first manifest PUT: **1.0.0 10/10 commits threw, 1.1.0 0/10**; controls 0/10 for both.
- `#10433`+`#11335` (S3InputStream read retry; first in 1.7.0, 1.6.1 diverged). RESET_RESP after 100 KiB on a data-file GET: **1.6.1 10/10 reads threw, 1.7.0 0/10**; controls 0/10.
- Oracle (1.12.0, 3 runs each). Flagged 3/3 for each of: truncated manifest, truncated data file, deleted data file, one flipped byte, zero-length manifest list. 0/3 with no tamper.

## OS-1 details
- Mechanism:
  1. `UrlConnectionHttpClient` (JDK HttpURLConnection) returns -1 when a Content-Length body is cut short.
  2. `S3InputStream.read` (S3InputStream.java:145-165) never compares bytes read with the GET's Content-Length.
  3. Avro 1.12.2 `DataFileStream.hasNext()` catches `EOFException` and returns false (exception table checked with javap).
  4. `ManifestReader` therefore yields a prefix of the entries. MergingSnapshotProducer copies that prefix into the merged manifest, and the commit succeeds.
- In the random-fault run (seed 100), one FIN cut 1 byte short of a 4,526-byte manifest-list GET. Two committed data files left the table. The snapshot summary says 9 data files; the manifests list 7. Both lost data objects are still in the bucket, unreferenced.
- Duplicate search:
  - Open PR #15792 ("AWS: handle premature connection close", unmerged, not in 1.12.0) names the urlconnection silent -1. It frames the problem as a Parquet read failure.
  - No issue or PR found that describes committed-metadata loss or a silently short scan ("manifest merge lost data files", "truncated manifest silently", "S3InputStream truncated", and others).
  - Status: root cause known upstream; the Major consequence appears unreported.
- Precondition: the user sets `http-client.type=urlconnection` (a documented option; the default before the switch to Apache, as the 1.0.0 run shows). A FIN-before-Content-Length is what S3 does to idle or stalled connections (#9679).

## Re-run
```
cd r6/objstore
./gw -q harness -PicebergVersion=1.0.0 -Dos.mode=calib-put -Dos.runs=10    # and -PicebergVersion=1.1.0
./gw -q harness -PicebergVersion=1.6.1 -Dos.mode=calib-read -Dos.runs=10   # and 1.7.0
./gw -q harness -Dos.mode=calib-oracle -Dos.runs=3
./gw -q harness -Dos.mode=fin-merge -Dos.client=urlconnection -Dos.runs=5 [-Dos.k=random -Dos.runs=20 -Dos.seed=50]
./gw -q harness -Dos.mode=fin-merge -Dos.client=default -Dos.runs=5      # control client
./gw -q harness -Dos.mode=fin-scan  -Dos.client=urlconnection -Dos.runs=5
./gw -q harness -Dos.mode=target-read -Dos.fault=FIN_RESP|RESET_RESP -Dos.nth=1|2 -Dos.runs=5 [-Dos.client=urlconnection]
./gw -q harness -Dos.mode=target -Dos.spec=CreateMultipartUpload:CUT_REQ:data:1:8:0 -Dos.runs=5   # see run-targets.sh
./gw -q harness -Dos.mode=explore -Dos.runs=40 -Dos.seed=100 [-Dos.client=urlconnection]
```
Unknown modes, ops, kinds, key classes, clients, and size modes throw. Summaries are in `results/*.txt`. Bounded
per-run details (at most 30 runs × 40 lines) are in `results/details/`. Work dirs and bucket prefixes are deleted after each run.

## Not done / limits
- No TLS: SSLException retries untested.
- No throttling or 5xx injection.
- Analytics-accelerator and CRT clients untested.
- Manifest-list truncation in the header was loud (EOFException); OS-1 via a manifest list was seen only in the random run.
- OS-6 fired once; no part-number-targeted repeat.
