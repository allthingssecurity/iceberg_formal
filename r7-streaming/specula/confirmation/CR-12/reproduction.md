# CR-12 — Phase 2 reproduction record

Test: `repro/test_bugCR-12_partial_commit_no_retry_budget.sh` (self-contained; embeds the JUnit sources).
Command: `cd .specula-output/repro && bash test_bugCR-12_partial_commit_no_retry_budget.sh`.
Logs: `repro/CR-12.log` (driver), `repro/CR-12.{PartialPathPermanentFailure,FullPathControl,FencedCoordinator}.log`.

Escalation level: **0** (pure black box). The sink is pristine `apache/iceberg @ 9fffeb6`, extracted
with `git archive HEAD`. The driver checks that `kafka-connect/kafka-connect/src/main` is
byte-identical to HEAD. It runs in a real Kafka Connect runtime (EmbeddedConnectCluster, real
`WorkerSinkTask`) against a real embedded Kafka 3.9.2 KRaft broker with real transactions and a
`HadoopCatalog` on the local filesystem. The only environment event is `chmod 555` on the table
metadata directory, the local analogue of a revoked catalog/storage privilege; the probe write
fails with AccessDeniedException. There is no source change, mock, failpoint or timing hook.

| Scenario | Setup | Observed (final run 06:20-06:22; A also in 2 earlier runs, identical) |
|---|---|---|
| A partial path | tasks.max=2, records only on src-1 (src-0's task idle, #17811) → 17× "Commit timeout reached", 0× "ready, received responses for all" | 14 consecutive swallowed failures (`RuntimeIOException`), one every ~3.05 s, 06:20:34.079 → 06:21:13.741. Configured max-consecutive-failures=3. Tasks `{0=RUNNING, 1=RUNNING}` throughout, coordinator thread alive, 0 CommitToTable, snapshots 1 → 1, source committed {1=48} versus 5 rows: 43 committed records not in the table. After repair: 50/50 rows, 0 duplicates, one healing snapshot with 15 data files / 45 records (the whole retained buffer). |
| B full path (control) | records on both partitions → full commits | Same failure: `CoordinatorThread` "Coordinator error during process, exiting thread" (RuntimeIOException) → `NotRunningException: Coordinator unexpectedly terminated on committer iceberg-sink-0` → task 0 FAILED 1.0 s after the revoke. |
| C fenced coordinator | partial steady state, interval 8 s / timeout 3 s; external `initTransactions()` on `connect-iceberg-sink-coordinator` mid-commit | The partial commit's `ProducerFencedException` was swallowed ("will retry", 06:22:00.035). The coordinator exited at its next StartCommit (06:22:05.047, ≈ interval − timeout) and was cleared as fenced. Task RUNNING (intended per #18039). Result: a bounded delay, not an endless loop. |
