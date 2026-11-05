# r7 kafka-connect: Specula method on the Iceberg Kafka Connect sink (released 1.12.0)

Workspace: `r7/kafka-connect/`.

| directory or file | contents |
|---|---|
| `modeling-brief.md` | the modeling brief, with the audit trail in §8 |
| `spec/` | `base.tla`, `MC.tla`, `configs.py` → `*.cfg`, `Trace.tla`, `Trace.cfg`, `changelog.md`, `brief-coverage.md`, `instrumentation-spec.md`, `bug-report.md`, `output/` |
| `harness/` | the r6 harness plus trace emission and new scenarios |
| `traces/` | trace files |
| `repro/` | reproduction scripts |
| `r7-results.txt` | harness matrix |
| `r6-adopted/` | copy of the r6 model |

Constraints:
- Released jars from Maven Central only.
- Nothing posted upstream.
- No commits.
- r6/ was not touched.

**Overall result:**
- One reproduced Major on 1.12.0. **It is not new as a mechanism.** Upstream PR text names it as an out-of-scope limitation; no issue has been filed and it is unfixed.
- No new loss or duplicate mechanism was found in the areas the user prioritised (commit timeout with late DataWritten, multi-table partial failure, coordinator restart). The coverage for those areas is listed below.
- The main new contribution is the r6 model, now validated against traces of the real code: 2 mismatches were found by trace validation and 7 more while instrumenting. All are fixed.

## 1. Specula contribution: KcSink trace-validated against released 1.12.0

**Instrumentation**
- An emit-only shadow of `channel/Coordinator` (11 added `harness.Trace.emit` lines; diff in `harness/shadow-Coordinator-1.12.0.diff`). `Main` refuses to trace unless the shadow is the class that was loaded.
- Emits from FakeKafka, SharedCatalog and Emu.
- A read-only reflection of `CommitterImpl.coordinatorThread`.
- Mapping documented in `spec/instrumentation-spec.md`.

**Traces**
- 29 traces (`r7/kafka-connect/traces/*.ndjson`, 42-130 lines each) covering:
  - smoke and ICR runs (F1 and its controls);
  - coordinator failover (F2 and its controls);
  - catalog faults on 1 and 2 tables;
  - 10 timeout / late-DataWritten / multi-table runs;
  - 2 maintenance runs.
- **Result on `Trace.cfg`** (TraceMatched + TypeOK + NoDup):
  - 28 of 29 traces pass.
  - The 29th, `maint_csue` (the K1 trigger), stops with NoDup violated at the exact catalog commit that re-appends the files (state 66). Code and model agree on the duplicate. With TypeOK only, it matches end to end (`spec/output/trace-maint-typeonly.txt`).
- **With NoLoss added:** NoLoss is violated on exactly the 4 traces the harness oracle marks lost (`icr_nocommit` = F1; `coord_latest`, `fault_rte_before_pre0`, `timeout_t2_csue_after_move` = F2 family), and on no other trace.
- Six mutated traces (one field or one order changed each, `traces/negative/`) are all rejected.

**Spec-vs-code mismatches** (`spec/changelog.md`):
- **Found by trace validation:**
  - **M1:** `close()` keeps the coordinator alive when the Admin describe does not show partition 0 (eager / partial view). Same mechanism as #17637.
  - **M2:** a new coordinator's control-topic position is fixed when the `-coord` group actually assigns it the partition, after the old member's thread has left. It is not fixed at election. Under `latest` this widens r6 F2: DataWritten produced *after* the new coordinator was elected is also skipped (`timeout_t2_csue_after_move`). Same root cause and same fix (#18006), so it is **not a new finding**.
- **Found while instrumenting (C1-C7):**
  - CommitToTable and CommitComplete records shift control-topic offsets.
  - The cgo commit is an overwrite guarded by an instance-local cache.
  - `totalPartitionCount` comes from describe.
  - The 1.12.0 failure policy, including the terminated-but-non-null coordinator thread.
  - Per-table commit.
  - An empty timeout commit.
  - The initial state.
- **Harness defects fixed:**
  - **H1:** `done()` checked only the first table.
  - **H2:** the heal phase stopped before the coordinator had flushed replayed envelopes. It now runs 2 settle cycles. That change took `maint` from 0/3 to 3/3.
  - All r6 1.12.0 rows were re-run with the stricter oracle (below).

## 2. Calibration

The calibration was re-run on the r7 harness and on the converged spec.

**Harness**, 5 runs per config, from `r7-results.txt`:

| known bug | pre-fix release | fixed release | controls |
|---|---|---|---|
| #12372 `icr-close` | 1.8.1: 5/5 BAD | 1.9.0: 0/5 | — |
| #16282 `zombie-rewind replay=1` | 1.11.0: 5/5 BAD | 1.12.0: 0/5 | `replay=0`: 0/5 on both |

Smoke on 1.12.0: 0/5.

**TLC**:

| check | pre-fix config | result | fixed config | result |
|---|---|---|---|---|
| #12372 | `cal1_prefix` and `cal1_prefix_c1` | NoLoss violated | `cal1_fixed_c1` | no error, 348,041 distinct states, exhaustive |
| #16282 | `cal2_put_rewind` and `cal2_put_rewind_m0` | NoDup violated | `cal2_max_rewind_m0` | no error, 259,827 distinct states, exhaustive |

The full-bound fixed configs, `cal1_fixed` and `cal2_max_rewind`, hit the 25-minute budget at 51.5M and 26.6M distinct states with no violation. Those are bounded results, not exhaustive ones.

**Convergence** (`MC.cfg`: no faults, one rebalance, released logic with F1 and F2 fixed): no error over 5,729,843 distinct states. This includes MarkerMonotonic and SrcMonotonic.

**Witnesses**: W_AllCommitted, W_PartialCommit, W_Terminated and W_Expired are all reachable. ASSUME rejects an unknown knob.

**Regression checks of the known bugs in the evolved model**: `f1_seeknull_skip` and `f2_latest_fail` both violate NoLoss.

## 3. Finding

### K1 (confirmed on 1.12.0, Major; known as a limitation, not new): the table's stored-offset floor removed by routine maintenance during a replay window, causing re-appended data files (duplicate rows)

**Mechanism** (1.12.0 source, `org/apache/iceberg/connect/channel/`):

1. A table commit is applied, but the commit call fails with something other than CommitFailedException. Examples: CommitStateUnknownException on a REST/Glue timeout, or a crash right after apply.
   - In 1.12.0 this terminates the coordinator (`Coordinator.java:171-175`) before `commitConsumerOffsets()` (`Coordinator.java:211`).
   - The next `put()` throws NotRunningException (`CommitterImpl.java:198-201`).
   - Kafka Connect then kills the task ("Task is being killed and will not recover until manually restarted"; see the WorkerSinkTask log in #16016). The task stays FAILED until an operator restarts it.
   - The `<group>-coord` offset therefore stays below the already-applied DataWritten envelopes. Those envelopes are now protected only by the table's stored floor (snapshot summary `kafka.connect.offsets.<topic>.<group>`).
2. While the task is down, routine maintenance (`rewrite_manifests` or `rewrite_data_files`, then `expire_snapshots`) leaves no reachable snapshot that carries the key.
3. On restart, the coordinator re-reads from the old group offset (`Channel.java:195-200`).
4. `lastCommittedOffsetsForTable` walks the ancestry and finds nothing (`Coordinator.java:390-410`, `findFirst ... orElseGet(Map::of)`).
5. Every envelope passes the filter (`Coordinator.java:272-280`, `minOffset == null`).
6. The ancestry validator compares `{}` with `{}` and passes (`Coordinator.java:309, 362-367`).
7. The same data files are appended again (`Coordinator.java:319-320`).

**Reproduction** on released 1.12.0 jars. Harness scenario `maint`, 5 runs each:

```
cd /Users/I074560/Downloads/experiments/iceberg-verify/r7/kafka-connect/harness
./run.sh 1.12.0 maint 5 maint=1                    # trigger: CSUE after apply, FAILED task, rewrite_manifests + expire(retain_last=1), restart -> BAD 5/5 (dupRows=4)
./run.sh 1.12.0 maint 5 maint=1 kind=CRASH_AFTER   # trigger variant -> BAD 5/5
./run.sh 1.12.0 maint 5 maint=1 kind=RTE_AFTER     # trigger variant -> BAD 5/5
./run.sh 1.12.0 maint 5 maint=1 tables=2           # trigger variant -> BAD 5/5
./run.sh 1.12.0 maint 5 maint=0                    # control: no maintenance -> 0/5
./run.sh 1.12.0 maint 5 maint=2                    # control: rewrite only, floor still reachable -> 0/5
./run.sh 1.12.0 maint 5 maint=3                    # control: expire only, connector snapshot is current -> 0/5
./run.sh 1.12.0 maint 5 maint=1 kind=NONE          # control: maintenance with no replay window -> 0/5
../repro/test_bug1_floor_expiry.sh                 # all of the above with pass/fail checks
```

On 1.11.0 and 1.10.0 the same scenario gives 0/5, but not because those versions are immune. They log and retry instead of terminating (`1.11.0 Coordinator.java:150-161`), so the next commit cycle filters the buffered envelopes while the floor still exists, and the window closes before the scenario's maintenance step. TLC (`MC_hunt_expire`, NoDup violated at depth 14) shows the same re-append inside a running coordinator, without any restart:

1. A partial commit is applied but fails after apply ("will retry", buffer kept).
2. The floor expires.
3. The next commit re-appends.

**What is real and what is emulated in the trigger**

Real released code:
- the Iceberg 1.12.0 connector classes;
- Iceberg core `rewriteManifests()`, `expireSnapshots().retainLast(1)` and `SnapshotUtil.ancestorsOf` (unmodified, on the shared InMemoryCatalog).

Emulated:
- **Fault injection.** The injected CSUE is thrown *after* the catalog applied the commit. That is the documented meaning of CommitStateUnknownException.
- **Task stays down.** The FAILED task is kept down until the heal phase. This matches Connect semantics for task failure.
- **Restarted coordinator position.** It starts from the group's committed offset. That is standard Kafka consumer-group behaviour, and FakeKafka implements it.

The harness does not wait days. It expires with `older_than=now`.

**Realistic precondition.** A correct deployment can hit this. All of the following are needed:
- (a) a commit whose outcome is unknown, or a crash between the table commit and the cgo commit;
- (b) the coordinator not restarted for longer than the snapshot-expiry age. The default `history.expire.max-snapshot-age-ms` is 5 days, and typical maintenance jobs use 1-7 days. 1.12.0 makes (b) much likelier than earlier releases, because (a) now leaves the task FAILED instead of retrying;
- (c) at least one newer snapshot that does not carry the key, for example compaction, a manifest rewrite, or another writer;
- (d) `expire_snapshots` running in that window.

Consequences:
- If compaction also deleted the original data files, the re-appended entries point to files that no longer exist, and table reads fail. The harness used `rewrite_manifests`, which keeps the files, so it shows only duplicate rows.
- With PR #18006 applied (coordinator `auto.offset.reset=earliest`), every restart without a group offset replays the whole retained control topic. That makes the floor load-bearing on every restart.

**Overlap**
- Not F1, F2, #12372 or #16282.
- The mechanism is named as an out-of-scope limitation in the bodies of PR #18006 (https://github.com/apache/iceberg/pull/18006), PR #18322 (https://github.com/apache/iceberg/pull/18322) and PR #18012 (https://github.com/apache/iceberg/pull/18012), and in review comments on PR #17925 (https://github.com/apache/iceberg/pull/17925). In those comments, replaying "after that boundary is lost can register an already-committed file again".
- No issue exists and nothing is fixed in 1.12.0 or main.
- The new parts are the end-to-end reproduction on released 1.12.0 and the observation that 1.12.0's terminate-on-failure (#16237) holds the replay window open until an operator acts.

**Fix direction** (not tested):
- Do not let the dedup floor depend on snapshot retention. For example, persist it in table properties, or carry it forward into every snapshot the connector writes.
- Alternatively, before re-appending, skip data-file paths that are already live in the table.

## 4. Checked with no violation (coverage, all on 1.12.0)

**Scenario 2: commit timeout, partial commit, late DataWritten** (harness, checkpoint present, 5 runs each, all 0/5)

| `timeout precommit=1 …` variant | BAD |
|---|---|
| `after=none` | 0/5 |
| `after=move` | 0/5 |
| `after=crash` | 0/5 |
| `late=0` | 0/5 |
| `tables=2 kind=RTE_BEFORE after=crash` | 0/5 |
| `tables=2 kind=CSUE_AFTER after=move` | 0/5 |
| `tables=2 kind=RTE_AFTER` | 0/5 |
| `tables=2 kind=CFE_BEFORE count=5 maxcf=2` | 0/5 |

- Late files are committed once, in the next cycle, and the stored offset is past the late envelope. The traces are validated.
- TLC `MC_hunt_timeout`, simulation for 10 minutes: 3,458,381 traces (125.8M states, mean length 28), no violation.

**Scenario 3: multi-table with one table failing**

Harness `fault … precommit=1` with tables=1 and tables=2, kinds CSUE_AFTER/BEFORE, CRASH_AFTER/BEFORE, RTE_AFTER and CFE_BEFORE×5: all 0/5 with the stricter oracle.

TLC:
- `MC_hunt_multitable` BFS was stopped at the lead's disk request: 49,423,219 distinct states, depth 28, no violation. Not exhaustive.
- Simulations (`-simulate`, 7 minutes each, `spec/output/run-hunt-sim.txt`), none violated:

| config | traces | states |
|---|---|---|
| `MC_hunt_multitable` | 2.94M | 74M |
| `MC_hunt_multitable_cf2` | 3.71M | 80M |
| `MC_hunt_taskfail` | 4.71M | 127M |
| `MC_hunt_rel112` (crash + rewind + table failure + move + task failure, released knobs with F1/F2 fixed) | 4.07M | 132M |

These are random-walk coverage, not exhaustive.

**Refuted by reading Kafka source: a stale committed offset in the KIP-447 marker window**

Hypothesis: `KafkaUtils.seekToLastCommittedOffsets` uses the framework consumer (`read_uncommitted`), so it could read a stale committed offset during the KIP-447 marker window and rewind below an answered DataWritten, causing duplicates.

Refuted: kafka-clients 3.9.2 `ConsumerCoordinator.sendOffsetFetchRequest` always sends `requireStable=true` (bytecode `iconst_1`). Pending transactional offsets are therefore waited for.

**Out of scope, recorded as gaps**
- Two simultaneously live coordinators: known (#17637/#16016). The model keeps one.
- KIP-848 (`group.protocol=consumer`): needs a real broker. Its describe view during `close()` is the same "partition owned by nobody" case as #17637/F1.
- Worker `transactional.id` suffix regenerated per `taskConfigs()`: zombie workers are still fenced, by the KIP-447 group generation in `sendOffsetsToTransaction`. No loss or duplicate path was found.
- Dynamic routing, tables created mid-run, branch change, rollback: not modelled. Missing-table paths are known (#17641, #18012). For rollback, re-admitted envelopes restore rows that the operator rolled back, which is not a duplicate.

## 5. Known bugs re-observed (not re-reported)

| bug | trigger | control |
|---|---|---|
| F1 `icr-nocommit` | 5/5 | `precommit=1` 0/5 |
| F2 `coord-latest` | 5/5 | `reset=earliest` 0/5 |

## Lead re-run (2026-11-04, independent of the track)

- Source: `iceberg-kafka-connect-1.12.0-sources.jar` SHA-1 9cfc2025… matches Maven Central. K1 chain re-read in
  that source: Coordinator.commit rethrows non-CommitFailedException (170-174) before control-topic progress is
  saved; doCommit filters replayed DataWritten only against the floor from lastCommittedOffsetsForTable (265, 273-279),
  which walks snapshot ancestry for `kafka.connect.offsets.*` (390-410) and returns empty once those snapshots expire.
  Confirmed.
- `repro/test_bug1_floor_expiry.sh` (N=5): maint=1 BAD 5/5; maint=1 kind=CRASH_AFTER BAD 5/5; controls maint=0 0/5,
  maint=2 (rewrite only) 0/5, maint=1 kind=NONE 0/5. RESULT: REPRODUCED. Same as the track.
- Upstream: open PR #18006's description states that expiring the snapshots carrying the offset boundary "can permit
  duplicate file registration during historical replay"; #18322, #18012 and a #17925 comment list it as out of
  scope. **Lead verdict: K1 is REPRODUCED but KNOWN (documented limitation, no issue, no fix); not a new finding.**
- No new bug in the priority areas (timeouts with late responses, multi-table failures, coordinator failover):
  harness 0/5 everywhere, TLC simulation only (not exhaustive) at full bounds.
