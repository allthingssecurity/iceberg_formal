# CR-7 — Phase 2 reproduction record

Test: `repro/test_bugCR-7_zombie_coordinator_starves_new_coordinator.sh [range|dense|lowlatency|worstcase|sticky|injected]`
(`ATTEMPTS=N` overrides the number of hand-overs; the default is 10).

## Harness

- System under test: the **pristine** iceberg-kafka-connect sink. Its sources are extracted with
  `git archive HEAD` from upstream 9fffeb6 and compiled by the script. Blob hashes of Channel,
  Coordinator, CoordinatorThread and CommitterImpl are printed and match HEAD. The TLA
  instrumentation of the worktree is not on the classpath.
- Environment: an in-JVM Kafka 3.9.2 KRaft broker and one Kafka Connect worker
  (`EmbeddedConnectCluster`, connect-runtime test jar), a shared in-memory Iceberg catalog, and the
  default control topic `control-iceberg` with 1 partition (as in the docs). The dependency
  classpath is resolved offline by Gradle in the CR-7 worktree; the Iceberg module jars come from
  unmodified modules.
- Operations: Connect REST only (create; `restart?includeTasks=true`; stop/resume; delete), plus
  producers for the source topics. Observation is read-only: Admin `describeConsumerGroups` and
  `listConsumerGroupOffsets`, catalog snapshots and rows, and liveness and stack frames of the
  `iceberg-coord` threads.
- Traffic premise: N unrelated Iceberg sink connectors ("neighbours", commit interval 500 ms, fed
  continuously) share `control-iceberg`. A 20 s group-less read_committed reader measures
  inter-arrival gaps on `control-iceberg-0`.

## Escalation ladder

### Level 0/1: real APIs, legal configurations (timing chosen through public configs only)

| mode | neighbours | victim interval/timeout | assignor | control records / 20 s | max gap | gaps ≥1 s / 20 s | hand-overs where the zombie kept the partition | thread gone (latest) | DELETE: group empty after |
|---|---|---|---|---|---|---|---|---|---|
| range | 1 | 5 s / 2 s | default (range) | 82 | 1017 ms | 17 | 0/10 | 1.70 s | 768 ms |
| dense | 3 | 5 s / 2 s | default | 237 | 1012 ms | 16 | 0/10 | 1.96 s | 1571 ms |
| lowlatency | 3 | 1 s / 0.5 s | default | 243 | 1012 ms | 16 | 0/10 | 1.94 s | 892 ms |
| worstcase | 3 | 1 s / 0.5 s | CooperativeSticky | 243 | 1016 ms | 16 | 0/10 | 1.83 s | 261 ms |
| sticky | 1 | 5 s / 2 s | CooperativeSticky | 81 | 1015 ms | 16 | 0/20 | 1.81 s | 1105 ms |

In addition, a first range run (with an earlier driver; its log was overwritten) gave the same
0/10 result.

**Why it never triggers:** the control topic always contains a ≥1 s silence once per commit
cycle, even with three connectors sending ~12 records/s. Every live coordinator emits StartCommit
and handles commit timeouts only in `Coordinator.process()`. That method runs only after
`consumeAvailable` returned on an empty `POLL_DURATION = 1 s` poll (Coordinator.java:77, :126-141;
Channel.java:122-152). Coordinators sharing a topic all wait for the same global silence, so their
cycles synchronise. The terminated coordinator runs the same 1 s poll on the same partition, sees
that silence, and leaves the loop. Its thread was gone ≤1.96 s after every REST restart (60/60)
and the new coordinator always owned `control-iceberg-0`. At 100 ms sampling, the state "zombie
holds the partition in a settled group while the new coordinator has none" was never observed
(sticky run, 0/20).

Level 1 cannot widen the window: sleeps in the system under test only lengthen gaps, and a slower
catalog lengthens the committing coordinator's silence. Level 3 cannot either: lengthening
POLL_DURATION scales both the zombie's window and the coordinators' gating silence equally.

### Level 2: state injection, mask removed (NOT a reachable state; demonstrates the defect only)

Mode `injected`: no neighbours. A plain producer writes valid Iceberg control events (StartCommit
of an unrelated group) every 200 ms, so there is never a 1 s gap. No real Iceberg peer emits such
ungated traffic. See CR-7.injected.log.

- Phase 2b, with NO coordinator terminated: the healthy coordinator stays at
  `Channel.consumeAvailable:150 < Coordinator.process:136 < CoordinatorThread.run:51` for 30 s.
  Table rows 100→100 and source offsets 100→100 (stalled). It commits within 1.29 s after the
  injection stops. **The claimed commit stall happens without any zombie.**
- Hand-over under injection: the terminated coordinator (72d1a7da) kept `control-iceberg-0` for the
  whole 60 s window, at `Channel.consumeAvailable:150 < Coordinator.process:136 <
  CoordinatorThread.run:51` (C1=true). The new coordinator (76d915e9) was a member with no
  partition (C2=true). It initiated a commit every 5 s, timed out and committed 0 tables. Table
  rows stayed at 200. Source offsets did NOT advance (C3=false), because the restarted workers were
  themselves stuck in `Worker.start()` → `Channel.start()` → `consumeAvailable(1 s)` and answered
  StartCommit with empty DataComplete events.
- Control: when the injection stopped, the zombie left after 1 s, the table caught up to 300 rows
  in 5 s, and there were 300 distinct ids (no duplicates, no loss).
- Delete path under injection: 30 s after `DELETE /connectors/victim` the `-coord` group still had a
  member holding `control-iceberg-0`. `Admin.deleteConsumerGroups` → `GroupNotEmptyException`. The
  group emptied 1.18 s after the injection stopped.

## Verdict basis

The defect is real: `terminate()` does not stop the coordinator thread while records keep
arriving. This contradicts the intent in `TestCoordinatorThread.testRun` and the
`Coordinator.terminate()` comment. Its consequence is masked today by the 1 s commit-initiation
gate that every live coordinator applies (proven to fire: 60/60 hand-overs and 5/5 deletes
clean). Even with the mask removed, the commit stall the finding claims is not attributable to the
zombie: the healthy coordinator and the restarted workers stall under the same traffic. The
zombie-specific effect is the lingering `-coord` membership (phantom consumer, group not
deletable) and leaked clients for as long as gap-free traffic lasts. → **MASKED**.
