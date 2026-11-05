# CR-13 — Phase 1 investigation record (evidence only)

Finding: "One DataWritten per table writer result is never split, so a large cycle exceeds
max.request.size and the task crash-loops" (source: code-review; no MC counterexample — record
sizes are not modeled).

Source checked: worktree HEAD `9fffeb6` (shallow clone, 1 commit). The worktree carries TLA
trace-harness edits; on this path they only add a `try/catch` around `send()` in `Worker.receive`
that calls an inert hook and rethrows, plus future collection in `Channel.send`. Line numbers below
are **pristine HEAD** (`git show HEAD:<file>`), which match the finding's citations.

## Step 1 — Code audit

### Cited code (pristine HEAD)

- `kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/channel/Worker.java:74`
  `SinkWriterResult results = sinkWriter.completeWrite();`
- `Worker.java:94-106` — one `Event(DataWritten(partitionStruct, commitId, tableReference,
  writeResult.dataFiles(), writeResult.deleteFiles()))` **per writer result**, carrying the
  result's complete file lists.
- `Worker.java:108-111` — appends `DataComplete(commitId, assignments)` and calls
  `send(events, results.sourceOffsets())`.
- `Channel.java:87-96` — each `Event` is `AvroUtil.encode`d into **one** `ProducerRecord` (key =
  producerId).
- `Channel.java:98-117` — single transaction: `beginTransaction`; `recordList.forEach(producer::send)`
  (`:103`); `sendOffsetsToTransaction(source offsets)` (`:104-107`); `commitTransaction` (`:108`);
  on any exception `abortTransaction` and rethrow (`:109-116`). No size check, no chunking.

### Where the writer results come from

- `data/SinkWriter.java:58-69` (pristine) — `completeWrite()` flattens `writer.complete()` of the
  per-table writers.
- `data/IcebergWriter.java:100-114` — `flush()` adds **one** `IcebergWriterResult(tableReference,
  Arrays.asList(writeResult.dataFiles()), Arrays.asList(writeResult.deleteFiles()),
  table.spec().partitionType())`. `flush()` runs only at `complete()` (once per commit cycle,
  `:117-124`) or at a schema-evolution boundary (`:86-92`). `partitionStruct` is the partition
  **type**, not a partition value, so a result is per table (per schema segment): all partitions'
  files of that table go into one `DataWritten`.
- `data/RecordUtils.java:110-114,163-178` — partitioned tables use `PartitionedAppendWriter`
  (`PartitionedFanoutWriter`): one open file per distinct partition value, rolled at
  `write.target-file-size-bytes`. Files per table per cycle >= distinct partition values the task
  touched in the cycle.
- `kafka-connect-events/.../DataWritten.java:97-118` — Avro schema = `data_files` /
  `delete_files` lists of `DataFile.getType(partitionType)` structs (path, partition, counts,
  `column_sizes`, `value_counts`, `null_value_counts`, `nan_value_counts`, `lower_bounds`,
  `upper_bounds`, `split_offsets`, ...). Record size grows with #files x per-file metadata
  (per-file metadata grows with the number of columns that keep metrics).

### Producer behaviour (kafka-clients 3.9.2, `gradle/libs.versions.toml:75`)

`KafkaProducer.doSend` -> `ensureValidRecordSize(uncompressed upper-bound size)`: if > 
`max.request.size` (default 1048576) it raises `RecordTooLargeException` (an `ApiException`),
returned as a failed future, and `TransactionManager.maybeTransitionToErrorState` puts the
transaction in abortable-error state; the next `sendOffsetsToTransaction` / `commitTransaction`
(or next `send`) throws `KafkaException("Cannot execute transactional method because we are in an
error state")` caused by `RecordTooLargeException`. Compression does not help (the check uses the
uncompressed estimate). If the producer limit is raised above the broker/topic
`message.max.bytes` / `max.message.bytes`, the broker rejects the batch with MESSAGE_TOO_LARGE —
same outcome.

Checked with `javap -c` on the cached `kafka-clients-3.9.2.jar`: `KafkaProducer.doSend` calls
`AbstractRecords.estimateSizeInBytesUpperBound` and then `ensureValidRecordSize`. For magic >= 2
the size comes from `DefaultRecordBatch.estimateBatchSizeUpperBound(key, value, headers)`, which
ignores the compression type. The `ApiException` path calls
`TransactionManager.maybeTransitionToErrorState` and returns a `FutureFailure`.

### Call chain (reachable in normal operation)

`WorkerSinkTask.deliverMessages` -> `IcebergSinkTask.put` (`IcebergSinkTask.java:86-90`) ->
`CommitterImpl.save` -> `processControlEvents` -> `Worker.process` -> `Channel.consumeAvailable`
-> `Worker.receive` (on `START_COMMIT`) -> `Channel.send` -> exception escapes `put()` ->
`WorkerSinkTask`: "Task threw an uncaught and unrecoverable exception. Task is being killed and
will not recover until manually restarted" -> task FAILED.

StartCommit fires every `iceberg.control.commit.interval-ms` (default 300000,
`IcebergSinkConfig.java:87`), so the path runs every cycle.

### Trigger scenario

1. Connector writes a partitioned table with high fan-out (for example hourly x ~30
   sub-partitions), many columns with metrics, and/or upsert mode (delete files too).
2. One task accumulates N files for that table in one cycle (fan-out, small target file size,
   long commit interval, or a catch-up backlog after downtime) so that the encoded `DataWritten`
   exceeds 1 MiB.
3. Coordinator sends StartCommit; the worker's `send()` fails; the transaction aborts (no
   DataWritten, no DataComplete, no source-offset commit); `put()` throws; the task FAILS.
4. On (manual or operator-driven) restart, records are re-delivered from the unchanged committed
   offsets, the same or a larger set of files is built, and the same failure recurs. After the
   failed task's consumer leaves the group, its partitions are also rebalanced onto surviving
   tasks, which then carry the extra backlog.

### Safeguards / masks noted (not judged here)

- Source offsets are committed only inside the aborted transaction -> no offset advance, no
  data loss (records re-delivered); the written data files are orphaned on storage.
- Coordinator: missing DataComplete -> commit times out (`commitTimeoutMs` default 30000) ->
  partial commit for the other tasks.
- Config workarounds only: `iceberg.kafka.max.request.size` plus broker/topic
  `message.max.bytes` / `max.message.bytes`; reduce `write.metadata.metrics.*`; shorter commit
  interval. None of them splits the message.
- A split would be protocol-compatible: `CommitState.tableCommitMap` groups DataWritten envelopes
  by `tableReference` (`CommitState.java:139-144`) and `Coordinator.commitToTable` merges files of
  all payloads (`Coordinator.java:277-301`).

## Step 2 — Developer-knowledge search

### apache/iceberg tracker (GitHub search API, 2026-11-05)

Queries: `RecordTooLargeException`, `"RecordTooLarge"`, `"max.request.size"`,
`"message.max.bytes"`, `"when serialized which is larger than"`,
`kafka connect control topic message too large`, `is:pr DataWritten split`,
`is:pr kafka connect "DataWritten"`, `kafka connect "message size"`,
`kafka connect "too many files" control`,
`"Cannot execute transactional method because we are in an error state"`, and the open
"Kafka Connect" PR list sorted by update. Result: no report and no PR (open, merged or closed)
about this mechanism. Only hit on `max.request.size` / `message.max.bytes`: #15293 (Kafka cluster
recreation; the strings appear only in the reporter's broker config). Commits to `Worker.java` /
`Channel.java` (#10351, #11599, #11780, #14979, #17933, #17552, #17713, #18039): none touches
event size.

### Predecessor tracker — same codebase

`tabular-io/iceberg-kafka-connect` (GitHub redirects to `databricks/iceberg-kafka-connect`). README:
"THIS REPOSITORY IS NOT MAINTAINED AS THE CODE HAS BEEN DONATED TO THE UPSTREAM APACHE ICEBERG
PROJECT". Apache PR #10351 ("Kafka Connect: Commit coordination", bryanck, merged 2024-07-11; it
introduced `Worker.java` / `Channel.java`) says: "the current sink implementation can be found at
https://github.com/tabular-io/iceberg-kafka-connect".

- **#149** (https://github.com/databricks/iceberg-kafka-connect/issues/149, closed 2023-11-07
  after a workaround answer): stack trace
  `Channel.send(Channel.java:96)` <- `Worker.receive(Worker.java:158)` <- `Channel.consumeAvailable`
  <- `Worker.process` <- `IcebergSinkTask.processControlEvents` <- `IcebergSinkTask.put` <-
  `WorkerSinkTask.deliverMessages`; "Task is being killed and will not recover until manually
  restarted. Error: Cannot execute transactional method because we are in an error state";
  "Caused by: org.apache.kafka.common.errors.RecordTooLargeException: The message is 1446709 bytes
  when serialized which is larger than 1048576, which is the value of the max.request.size
  configuration." Maintainer bryanck: "Try `iceberg.kafka.max.request.size`. The size of the
  message is somewhat concerning, are you creating many files per table per commit?"; reporter:
  hourly partitions x ~30 sub-partitions, 5-hour backlog after a stoppage; bryanck: "the control
  message includes the file path as well as metadata related to the file, such as column stats";
  advice: reduce `write.metadata.metrics.*`.
- **#285** (https://github.com/databricks/iceberg-kafka-connect/issues/285, OPEN, 2024-08-19):
  "RecordTooLargeException with multiple partitions" — "The request included a message larger
  than the max message size the server will accept"; comment: "this occurs due to very large
  control messages the connector is producing to the control topic, if the broker has a smaller
  limit it fails".
- **#297** (https://github.com/databricks/iceberg-kafka-connect/issues/297, OPEN, 2024-09-23):
  "RecordTooLargeException: When producing message to Kafka control topic" — "The message is
  7222162 bytes when serialized which is larger than 1048576"; two partition columns, upsert
  mode; broker limits cannot be raised; "huge blocker".

Same-site check (saved in `evidence/`): predecessor v0.6.0 `Worker.java:136-149` builds one
`COMMIT_RESPONSE` event (`CommitResponsePayload(partitionStruct, commitId, table, dataFiles,
deleteFiles)`) per writer result, `:158` `send(events, offsets);`; `Channel.java:96`
`recordList.forEach(producer::send);` inside the transaction. These are exactly the two frames in
#149's trace and the direct ancestors of Apache `Worker.java:94-111` / `Channel.java:87-103`
(event renamed `CommitResponse` -> `DataWritten`).

### Comments / docs / tests

- No comment at the site about message size.
- `docs/docs/kafka-connect.md:96,120-121` documents the `iceberg.kafka.*` client passthrough; it
  says nothing about control-message size.
- No test covers a large DataWritten or record size.
- Local git blame unavailable (shallow clone); the GitHub commits API was used instead.

## Step 3 — Known-status / pre-filter

- Same mechanism (one control-topic record per table per worker per cycle carrying every file and
  its column metrics, never split; it exceeds `max.request.size` / `message.max.bytes`; the
  worker transaction aborts; `put()` throws; the task fails; offsets do not advance, so a restart
  fails the same way) at the same site (`Worker.receive` -> `Channel.send`, donated code) is
  already reported: databricks/iceberg-kafka-connect #149, #285, #297.
- Fix status: **unfixed** (#285 and #297 open; Apache code unchanged; no upstream PR).
- Source: code-review (no counterexample).
- Pre-filter applies: **Status: DROPPED (code-review x known, cite:
  https://github.com/databricks/iceberg-kafka-connect/issues/149; also /issues/285, /issues/297;
  fix-status: unfixed)**. As the skill requires, no `repro/` test is written.
