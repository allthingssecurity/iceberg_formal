# CR-1: Phase 2 reproduction record

- **Test:** `repro/test_bugCR-1_control_topic_recreate.sh` (self-contained; it embeds the JUnit test, the shared catalog, and the Gradle source set).
- **Command:** `cd .specula-output && timeout 2700 bash repro/test_bugCR-1_control_topic_recreate.sh > repro/CR-1.log 2>&1`
- **Output files:**
  - `repro/CR-1.log`: summary
  - `repro/CR-1.gradle-output.log`: test console
  - `repro/CR-1.sink.log`: the unmodified sink's own slf4j log (`org.apache.iceberg.connect.channel` at DEBUG)
- **Final run:** exit 0, all 3 tests PASSED, BUILD SUCCESSFUL in 8m 32s. Earlier runs of tests 1-2 and of test 3 alone gave identical outcomes.
- **Escalation level reached: Level 0** (black box).

## Environment and preflight

- **System under test:** `git archive HEAD` (9fffeb63b1605a7c3acf218e58b7eeb43bdb1b01 = apache/iceberg main for `kafka-connect/`). The script verifies every file under `kafka-connect/kafka-connect/src/main` and `kafka-connect-events/src/main` byte-for-byte against git HEAD and refuses to run otherwise. Only a test-only Gradle source set (`src/crRepro`) is added.
- **Reused:**
  - the Specula harness recipe from `harness/run.sh` and the worktree `src/tlaHarness`: EmbeddedConnectCluster, a shared InMemoryCatalog root, the Gradle dependency block, `offset.flush.interval.ms=500`;
  - the Gradle build cache (cache hits keyed on the pristine inputs).
- **Rejected:** the instrumented worktree and `source/` builds. `TlaHooks.afterSend` waits for transaction markers and calls the Admin client after every send, which changes timing.
- **Topology:**
  - 1 KRaft broker (Kafka 3.9.2, `auto.create.topics.enable=false`, so the operator recreates the topic explicitly);
  - 2 Connect workers;
  - `IcebergSinkConnector` with `tasks.max=2`;
  - source topic `src` (2 partitions) and control topic `ctl` (1 partition);
  - commit interval 2 s, commit timeout 5 s;
  - an in-memory Iceberg catalog (Avro data files).
- **Actions (all through public interfaces):**
  - Kafka Admin `deleteTopics` / `createTopics` (operator);
  - a Kafka producer (source data);
  - Connect REST `POST /connectors/iceberg-sink/tasks/{id}/restart` (operator);
  - Iceberg reads (`IcebergGenerics`);
  - Admin `listConsumerGroupOffsets` / `listOffsets` / `describeConsumerGroups`.
  - There were no failpoints, no source edits and no injected state. The only timing choice was the operator's: delete and recreate right after a commit round completed (72 ms window).

## Results (final run)

| Phase | What happened | Observed |
|---|---|---|
| A, baseline | 6 data rounds plus idle rounds until the control log end is ≥ 400, then 1 data round | 70/70 records in table, committed `{0=35,1=35}`, 7 snapshots, floor `{"0":406}`, ctl log end 420 |
| B, recreate while RUNNING | `ctl` deleted and recreated (72 ms; new log end 0); 10 more records produced | 35 s later: 7 snapshots, 70 rows, committed unchanged, tasks RUNNING. Coordinator skipped 15 new-topic records, workers skipped 25; workers handled StartCommit 0 times; coordinator handled DataWritten 0 times; 7 timeouts, 7 rounds "committed to 0 table(s)". **STALL_HWM=true** |
| C, REST restart of non-leader task 1 (cooperative) | 10 more records produced | 0 new coordinators, 1 fresh commit worker. Workers handled StartCommit 20 times; coordinator skipped 61 records and handled DataWritten/DataComplete 0 times; "found nothing to commit" 0 times. Committed `{0=35,1=45}`, rows 70/90. **LOSS_HWM=true: 10 lost (ids 71..89 odd = partition 1)** |
| D, later | 3 more rounds | still 10 lost, 7 snapshots. Coordinator log: "local offsets {0=419} are less than or equal to committed offsets {0=419}", so no checkpoint on the new topic. **PERMANENT=true** |
| E, REST restart of leader (fresh coordinator) | 10 more records | new coordinator handled DataWritten 2 times, then "found nothing to commit to table db.tbl, skipping"; lost grows to 30. **LOSS_FLOOR_KNOWN_15293=true**; this is the known half, not claimed new |
| Control: same task restart, no recreation | 3 baseline rounds, records buffered at restart, more records | 50/50 rows, lost 0, snapshots 3→5, coordinator skips 0. **CONTROL_NO_RECREATE: allInTable=true, lost=0** |
| Default (eager) assignors | phases A-C | stall=true, **lost=20** (both partitions); `cannot determine leader` logged once at close, so the coordinator was neither stopped nor restarted; 2 fresh workers handled StartCommit 6 times; coordinator skipped 14 and handled DataWritten 0 times; floor filter hits 0. **mechanism=in-memory-HWM(Channel.java:129)** |

### Phase C causal chain (final run, `repro/CR-1.sink.log`)

```
03:00:51.444 [task-thread-iceberg-sink-1] INFO  CommitterImpl - Starting commit worker iceberg-sink-1
03:00:55.397 [iceberg-coord] INFO  Channel - Sending event of type: START_COMMIT
03:00:56.074 [task-thread-iceberg-sink-1] INFO  Channel - Sending event of type: DATA_WRITTEN
03:00:56.074 [task-thread-iceberg-sink-1] INFO  Channel - Sending event of type: DATA_COMPLETE
03:00:56.188 [task-thread-iceberg-sink-1] INFO  Channel - Handled event of type: START_COMMIT
03:00:56.189 [iceberg-coord] DEBUG Channel - Skipping already-consumed control topic offset 34 for partition 0
03:00:56.189 [iceberg-coord] DEBUG Channel - Skipping already-consumed control topic offset 35 for partition 0
03:01:01.200 [iceberg-coord] INFO  Coordinator - Coordinator iceberg-sink-0 completed commit 20a0df89-..., committed to 0 table(s), valid-through null
```

The fresh worker's DataWritten (offset 34) and DataComplete (offset 35) go out in the transaction that also commits partition 1's source offsets (35 → 45, read back via Admin). The surviving coordinator drops both at `Channel.java:129-134`, so `Coordinator.receive` never buffers them and `commitToTable` is never called with them. The floor filter is not involved (0 hits).

### Expected (correct) behavior

Per the target goal ("every record whose source offset the connector has committed is in the Iceberg table exactly once"), the 10 records would be in the table. At minimum, the sink would fail loudly rather than commit 0 tables while tasks stay RUNNING. The negative control shows that correct behavior on the same restart sequence when the control topic is not recreated.
