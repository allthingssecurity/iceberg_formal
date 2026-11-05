# CR-10 — Phase 2 reproduction record

Test: `.specula-output/repro/test_bugCR-10_txnid_fencing.sh` (self-contained). Run:
`timeout 3000 ./test_bugCR-10_txnid_fencing.sh > CR-10.log 2>&1` gave exit 0 in 2m42s. Exit 0 means
the harm was not triggered in any scenario.

Logs: `repro/CR-10.log` (driver), `repro/CR-10.<scenario>.gradle.log`,
`repro/CR-10.<scenario>.sink.log` (slf4j: sink, Connect runtime, Kafka TransactionManager),
`repro/CR-10.out/<scenario>.report.txt`.

## System under test

* `git archive HEAD` (9fffeb63b1) exported into `confirmation/CR-10/pristine-src`. The script checks
  that all 30 sink main sources are byte-identical to git HEAD. The Specula instrumentation in
  `worktree/` is not used, and the worktree was not modified.
* A test-only Gradle source set uses the same dependency recipe as `.specula-output/harness`
  (Kafka 3.9.2 `connect-runtime:test`, `kafka_2.13:test`, ...). It runs a real in-JVM KRaft broker and
  a real Connect cluster (`EmbeddedConnectCluster`, 1 worker) with the real `IcebergSinkConnector`
  (tasks.max=2, a 2-partition source topic, a 1-partition control topic) and one shared
  `InMemoryCatalog`.

## Levels

* Level 0 (mechanism). Ids are read from the herder's stored task configs (Connect REST
  `GET /connectors/cr10/tasks` = output of `IcebergSinkConnector.taskConfigs()`), and producer
  ids/epochs from the broker (`Admin.listTransactions` / `describeTransactions`). No hooks are
  involved.
* Level 1 (zombie). `StallInterceptor` is a Kafka client interceptor configured through
  `iceberg.kafka.interceptor.classes`. It delays one `producer.send()` of one worker: the first
  DataWritten of the armed transactional id. This stalls the task thread inside `Channel.send()` after
  `beginTransaction()` and before `sendOffsetsToTransaction()`. Nothing else changes. A connector
  config update triggers the real `taskConfigs()` regeneration. The herder's real graceful-stop timeout
  (2 s) turns the stalled task into an orphan ("Graceful stop of task cr10-1 failed", later "the orphan
  task threw an uncaught exception. A newer instance of this task might be already running").
* Coordinator scenario: a plain `KafkaProducer` with the 1.12.0 coordinator id
  (`transactionalPrefix()+"coordinator"+transactionalSuffix()`, 1.12.0 Channel.java:69,
  Coordinator.java:97) followed by `initTransactions()`. This is the request sequence a 1.12.0
  coordinator of the same task generation sends. It writes only to a scratch topic.
* Level 2/3 were not needed: Level 1 reached the exact window the finding describes. No sink source
  was modified.

## Results (verbatim lines are in `repro/CR-10.log`)

| Scenario | Mechanism observed | Zombie outcome | Table oracle |
|---|---|---|---|
| ZombieWorkerEvicted (reconfigured, zombie evicted at 8 s, resumes at 25 s) | Task 1's worker id `worker-txn-2fa7752d…-1` became `worker-txn-ab2eb02d…-1` in GEN-2. The old id kept producerId 6 / epoch 0 at the end, so it was never fenced by id. The stable coordinator id went from producerId 7 epoch 0 to epoch 1 (positive control). | `CommitFailedException: Transaction offset Commit failed due to consumer group metadata mismatch: The coordinator is not aware of this member.`, then `CompleteAbort`. The zombie's DataWritten was never visible to read_committed consumers. | produced=85, distinct=85, rows=85, no duplicates, no missing ids; committed offsets equal end offsets |
| ZombieWorkerNotEvicted (reconfigured, resumes at 6 s, still a member) | Ids differ across generations; the old id was untouched | Transaction accepted (`CompleteCommit`); DataWritten visible at offset 14, committed atomically with its source offsets; the next owner resumed after it | 85/85, no duplicates, no missing ids |
| ZombieWorkerNoReconfig (control, same generation) | src-1 moved to task 0 (`…-0`), a different id from the zombie's `…-1`, so even a per-task-stable id could not fence this zombie | Same KIP-447 rejection, then `CompleteAbort`; task 1 ends FAILED (Connect's normal unrecoverable-error behaviour) | 85/85, no duplicates, no missing ids |
| CoordinatorAcrossVersions | `connect-cr10-coordinator` vs the 1.12.0 id `coordinator-txn-4eb732ce…-0`. (1) The 1.12.0 init left the live coordinator untouched (producerId 6, epoch 0; it kept committing). (2) A current-version re-init bumped the stable id (epoch 0 to 1), and the 1.12.0 producer still COMMITTED afterwards. (3) Re-initializing the same 1.12.0 id fenced it (`ProducerFencedException`). | n/a | 40/40, no duplicates, no missing ids |

## Conclusion fed to the verdict

* The finding's mechanisms are real: worker ids change on every `taskConfigs()`, and the coordinator
  id namespace differs between 1.12.0 and the current version. Neither has a consequence attributable
  to the current code.
  * Worker: the designed zombie fence is KIP-447 ("The sink relies on KIP-447 for exactly-once
    semantics", docs/docs/kafka-connect.md:61-62). It fired every time the zombie was stale, and a
    zombie that was still a member committed legitimately. A stable per-task id would be neither
    necessary (KIP-447 covers it) nor sufficient (partitions move across task indices: the
    NoReconfig control).
  * Coordinator: the mixed-version gap is the 1.12.0 behaviour (its `-coord` offset commit is a
    non-transactional `commitSync`, so no producer epoch fences it under any id). It is already
    reported and documented in PR #18039 (review discussion_r3981219594 plus the PR's Release Notes).
* No persistent issue record: per references/persistent-findings.md, false positives are not
  persisted.
