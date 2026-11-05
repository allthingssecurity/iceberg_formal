# Brief Coverage Audit — iceberg-kafka-connect

Maps `modeling-brief.md` §2 (Scenarios), §5 (Proposed Invariants) and §6.1 (Model-Checkable Findings), plus the
target-specific priority questions Q1–Q5, to `base.tla` / `MC.tla` definitions and to the hunting configs.
The "enabled in" columns were filled by parsing the `INVARIANTS` / `PROPERTIES` sections of the cfg files on disk
(not from intent). All cfgs use `SPECIFICATION MCSpec` except `MC_hunt_s4_liveness.cfg` (`MCLiveSpec`).

Known issues are switched off by default in every hunt, so their traces cannot mask new mechanisms:
`FixNullSeek = TRUE` (#18767 / in-house null-offset seek), `CoordReset = "earliest"` (= PR #18006 applied),
`MaxRecreate = 0` (#15293 / F12 control-topic recreation), `RefVariants = {"v"}` except in the F1 hunt.
`Trace.cfg` uses the real defaults instead (`latest`, no null-seek fix, stale views possible).

## 1. Scenarios (brief §2)

| Scenario | Mechanism modeled by | Hunt cfg(s) | Fault / environment setup that makes it reachable |
|---|---|---|---|
| S1 Stored floor claims more than the commit contained (F1, #15293/F12) | `Groups = Tables \X RefVariants`; per-group `CoordReadFloor` (merged = max(floor, HWM)) and `CoordCasCommit` as separate steps; `CommitThreads` 1/2; `LegacyTasks`; `RecreateControlTopic` (switch) | `MC_hunt_s1_refsplit.cfg` (F1), `MC_hunt_s1_onevariant.cfg` (other paths) | refsplit: t1 legacy + t2 v, 2 source partitions, parallel groups, 1 restart for the replay; onevariant: CommitFailed budget 2 + CAS fault, `-coord` bounce (`MaxCgRebalances`), timeout (late DataWritten), checkpoint fault, crash/restart. #15293: `MaxRecreate` (0 = off, KNOWN) |
| S2 Dedup floor only reachable through the commit branch's ancestry (F3, F4) | `snaps`/`heads`, `FirstKeyFloor` walk stopping at expired parents, `ReadFloor` (`CoordHead`) vs `ValidatorFloor` (`ValidatorHead`, main fallback); `OtherWriterCommit`, `ExpireSnapshot`, `FastForwardDropBranch`, `LoseCoordOffsets`; `CoordReset` | `MC_hunt_s2_floor_reachability.cfg`, `MC_hunt_s2_multitable.cfg`, `MC_hunt_s2_branch.cfg` | reachability: other writer + 2 expiries + crash between CAS and checkpoint + checkpoint fault + `-coord` offset loss with `earliest`; multitable: 2 tables, 2 CAS faults, max-consecutive-failures 1, other writer + expiry; branch: `UseBranch` + fast-forward/drop |
| S3 `close()` discards all buffers before fallible rewind steps (F2) | `CloseDiscard` → `CloseElect` → `CloseSeek` with `*Fail` variants and `CloseSeekWakeup`; `stored`, `wakeup`, `PauseResume`, `PollRethrowWakeup` (batch dropped), `PollRethrowFatal`; eager / cooperative / lost (`RebalanceLose`, `PollLost`) | `MC_hunt_s3_close_discard.cfg` (cooperative), `MC_hunt_s3_eager.cfg` (eager), `MC_hunt_s3_faults_only.cfg` (other exception points) | close_discard: 1 churn rebalance + 1 lost member + 1 pause/resume + 1 close fault; eager: 1 rebalance + 1 pause/resume; faults_only: no pause/resume, 2 close/open faults, lost member, 2 restarts |
| S4 More than one or no coordinator; table commits unfenced | coordinator instances `CoordIds` with `cEpoch`/`coordEpoch` fencing; `terminate()` only sets `cTerm` (zombies keep consuming, `CoordReadFloor`/`CoordCasCommit` still possible); describe-based election views `CloseLeaderResults`/`OpenLeaderResults` (#17637 semantics); `-coord` group `cgOwner`/`cgCommitted` with eager `CgReassign`; partial path swallows fencing | `MC_hunt_s4_zombie.cfg`, `MC_hunt_s4_zombie_2ctl.cfg`, `MC_hunt_s4_multitable_unknown.cfg`, `MC_hunt_s4_stall.cfg`, `MC_hunt_s4_liveness.cfg` | zombie: 2 churn rebalances + lost member + CAS fault + `-coord` churn + timeout, 3 coordinator ids; 2ctl: 2 control partitions; multitable_unknown: 2 tables × 2 CAS faults (landed/unknown), budget 2, crash/restart; stall: `StaleViews` (F9) + 2 churn rebalances + lost member + crash; liveness: minimal F9 setup under `MCLiveSpec` fairness |
| S5 Completion accounting, partial commits, valid-through | `cCount` (summed, not de-duplicated), `cTotal` frozen at `StartCoordinator` (`TotalViews` = assigned count of the generation; since validation Round 1 a cooperative round-1 view, with moving partitions unowned, undercounts natively, #18322), `cDcParts` ghost, `CoordTimeout` → partial; DataWritten of any commit id buffered | `MC_hunt_s5_readiness.cfg`; timeouts/late DataWritten also in `MC_hunt_s4_zombie.cfg` | 2 partitions, `StaleViews`, cooperative move during a commit (1 rebalance), 2 timeouts, 3 commits. Smoke run: ValidThroughSound violated (≈2.7k states: total frozen at 1 from a partial view → full commit after one DataComplete; KNOWN family F11/#18322, META). Phase 3: that view was not reachable (fidelity gap, fixed: exact `TotalViews`, failed members stay in the described `owner`); final hunt: no violation in 30 min BFS, depth 43 (`bug-report.md`) |

No Scenario is merged away; S4's "two tables with unknown outcomes" (MC-4) has its own cfg
(`MC_hunt_s4_multitable_unknown.cfg`) because `MC_hunt_s2_multitable.cfg` also enables floor expiry, whose
KNOWN-reachable F3 duplicate would stop TLC before the predicted-safe part is explored.

## 2. Invariants (brief §5)

| Invariant | Type | Defined | Wired / used in MC | Enabled in (read from cfg files) |
|---|---|---|---|---|
| NoDuplicateRows | Safety | `base.tla` | inherited (`EXTENDS base`) | MC.cfg, s1_refsplit, s1_onevariant, s2_floor_reachability, s2_multitable, s2_branch, s3_close_discard, s3_eager, s3_faults_only, s4_zombie, s4_zombie_2ctl, s4_multitable_unknown, s5_readiness, Trace.cfg |
| SourceCommittedCovered | Safety | `base.tla` | inherited | MC.cfg, s1_refsplit, s1_onevariant, s3_close_discard, s3_eager, s3_faults_only, s4_zombie, s4_zombie_2ctl, s4_multitable_unknown |
| FloorSound | Safety | `base.tla` | inherited | s1_refsplit, s1_onevariant, s2_multitable, s4_zombie, s4_zombie_2ctl, s4_multitable_unknown, s5_readiness |
| CheckpointSound | Safety | `base.tla` | inherited | s1_refsplit, s1_onevariant, s2_multitable, s4_zombie, s4_zombie_2ctl, s4_multitable_unknown |
| NoStrandedDataWritten | Safety | `base.tla` (uses `Reachable`: buffered by / will be consumed by a live unfenced coordinator, or a future start position) | inherited | s1_refsplit, s1_onevariant, s2_multitable, s4_zombie, s4_zombie_2ctl, s4_multitable_unknown |
| AtMostOneCommitPerDataWritten | Safety | `base.tla` | inherited | MC.cfg, s2_floor_reachability, s2_multitable, s2_branch, s4_zombie, s4_zombie_2ctl, s4_multitable_unknown, Trace.cfg |
| FloorMonotone | Safety (action property `[][FloorMonotoneStep]_vars`) | `base.tla` | inherited | PROPERTIES of MC.cfg, base.cfg, s2_floor_reachability, s2_multitable, s2_branch |
| ValidThroughSound | Safety (secondary) | `base.tla` — checks the precondition of a sound valid-through: a FULL commit starts only after a DataComplete for the current commit id arrived for every source partition (timestamps not modeled) | inherited | s5_readiness |
| EventuallyCommitted | Liveness | `MC.tla` (`DWExists ~> (DWCommitted \/ TimeBudgetUsed)` under `MCLiveSpec`) | `MCLiveSpec` (WF reactive, SF time-driven, WF restart / join rebalance) | PROPERTIES of s4_liveness |

Additional (not in the brief, supporting): `FloorReadValidatorAgree` (F4 witness, `MC.tla`, enabled in
s2_branch — expected to be violated), `NoCoordinatorStall` (safety encoding of the MC-5 F8/F9 stall, `MC.tla`,
enabled in s4_stall), and the structural invariants `TypeOK`, `AtMostOneUnfencedCoordinator`,
`CoordOwnerIsMember`, `CoordRefConsistent`, `WorkerBufferAssigned`, `NonLostBufferAboveCommitted` (MC.cfg,
base.cfg, Trace.cfg). Every §5 Safety invariant is enabled in at least one hunt cfg.

## 3. Findings (brief §6.1)

| ID | Trigger mechanism in the model | Expected violation | Hunt cfg | Reachability notes |
|---|---|---|---|---|
| MC-1 (a) F1 rolling upgrade | t1 legacy (`uuid` null) and t2 v DataWritten for T1 in one cycle → two groups; group 1's CAS stores max(floor, HWM) covering group 2; sequential: group 2 filtered ("nothing to commit") then checkpoint+clear; parallel: group 2 `CoordCasValidationFail` → full commit throws → task FAILED → restart → replay filtered | FloorSound, NoStrandedDataWritten (end-to-end SourceCommittedCovered) | `MC_hunt_s1_refsplit.cfg` | smoke run: FloorSound violated at depth 18 (group legacy committed floor 5 over the v DataWritten at offset 3) |
| MC-1 (b) one variant, other paths | retained buffer after Iceberg CommitFailed (budget 2, CAS fault), `-coord` partition bounce (`CgRebalance` eager rewind + HWM), late DataWritten (timeout), checkpoint fault, crash/restart | FloorSound, NoStrandedDataWritten, CheckpointSound | `MC_hunt_s1_onevariant.cfg` | smoke run: no violation in 4 min (2.5M distinct states, depth 28) — full hunt pending |
| MC-2 F3 replay windows | (i) crash between `CoordCasCommit` and `CoordCheckpoint`; (ii) two tables, one CAS fails, max-consecutive-failures 1; (iii) `LoseCoordOffsets` with `CoordReset = "earliest"` — each combined with `OtherWriterCommit` + `ExpireSnapshot` making the key-bearing snapshot unreachable | NoDuplicateRows, AtMostOneCommitPerDataWritten | (i)+(iii) `MC_hunt_s2_floor_reachability.cfg`, (ii) `MC_hunt_s2_multitable.cfg` | smoke runs: (i) NoDuplicateRows violated at depth 31 (crash after CAS, other writer, expiry, replay from `earliest`, `ReadFloor = ValidatorFloor = {}` → second CAS); (ii) NoDuplicateRows violated at depth 33 (T1 committed, T2 failed → task FAILED → restart → T1 floor expired → T1 re-committed) |
| MC-3 F2 | wakeup raised during `close()` (between `CloseDiscard` and `CloseSeek`) → `CloseSeekWakeup` (no rewind) → `PollRethrowWakeup` drops the returned batch; eager, cooperative and lost; exception points other than `committed()`: `CloseDiscardFail`, `CloseElectFail`, `CloseSeekFail`, `OpenFail` | SourceCommittedCovered | `MC_hunt_s3_close_discard.cfg`, `MC_hunt_s3_eager.cfg`, `MC_hunt_s3_faults_only.cfg` (other exception points, F2 off) | smoke runs: SourceCommittedCovered violated (eager depth 15; cooperative 736k states, wakeup between CloseElect and CloseSeek). faults_only (F2 off): no violation in 3 min (2.2M distinct states, depth 19) — full hunt pending |
| MC-4 zombie safety (predicted safe) | old coordinators not stopped by close-time describe (eager: PreparingRebalance or old view; cooperative: round-1 view without the moved partition; lost), task FAILED without join → `cTerm` only; successor `initTransactions` fences sends/checkpoints, not `CoordReadFloor`/`CoordCasCommit`; partial path swallows fenced/validation errors; `CgRebalance` with 1 and 2 control partitions; two tables with unknown outcomes; timeouts with late DataWritten | NoDuplicateRows, FloorSound, NoStrandedDataWritten | `MC_hunt_s4_zombie.cfg`, `MC_hunt_s4_zombie_2ctl.cfg`, `MC_hunt_s4_multitable_unknown.cfg` | any violation is a new bug (brief). Smoke runs: s4_multitable_unknown **completed exhaustively, no violation** (17,309 distinct states, depth 47); s4_zombie no violation in 4 min (2.85M distinct states, depth 21) — full hunt pending |
| MC-5 liveness | F8: fenced coordinator cleared (`ClearFencedCoordinator`) while `startCoordinator()` was a no-op; F9: `StaleViews` empty describe at `Open` → no election, never retried; F4: branch missing while main carries the key | EventuallyCommitted (F8/F9, plus the safety encoding NoCoordinatorStall); F4 via FloorReadValidatorAgree | F8/F9 `MC_hunt_s4_liveness.cfg` and `MC_hunt_s4_stall.cfg`; F4 `MC_hunt_s2_branch.cfg` | smoke runs: NoCoordinatorStall violated (≈57k states: t1 kicked out, its lost-close stops its coordinator, t2's `open()` sees a non-Stable describe → no election, never retried) and EventuallyCommitted violated (minimal F9 cfg, ≈1 min). F4 in the model is an endless partial-commit retry loop that consumes the bounded time budget, so the budget escape of `EventuallyCommitted` hides it; it is witnessed instead by `FloorReadValidatorAgree` (smoke run: violated at depth 14), whose violation means every CAS fails validation until a successful CAS recreates the branch — which cannot happen |

## 4. Target-specific priority questions → model

| Q | Covered by |
|---|---|
| Q1 task rebalance / crash / restart (eager, cooperative, KIP-848 ≈ cooperative) | S3 cfgs (F2 + other exception points), `Crash`/`Restart` in s1_onevariant, s2, s4 cfgs; KIP-447 guard `Kip447Ok` + `WorkerAnswerRejected`; requireStable via `InitPos`/`SeekPos`; structural `NonLostBufferAboveCommitted` |
| Q2 coordinator move / crash / zombie | S4 cfgs; `AtMostOneUnfencedCoordinator`; stall s4_stall, liveness s4_liveness |
| Q3 timeout + late DataWritten | `CoordTimeout` (partial) + DataWritten of any commit id buffered; s1_onevariant, s4_zombie, s5_readiness |
| Q4 multi-table failure / unknown outcome | `CoordCasFault` (landedUnknown / commitFailed / unknownFailed), `Tasks.runParallel` stopOnFailure + throwOne first-class (`FirstErr`); s2_multitable, s4_multitable_unknown; F1 (s1_refsplit) |
| Q5 offsets missing / older than floors | `LoseCoordOffsets`, `CoordReset`, `StartPos`; floor reachability (s2 cfgs); control-topic recreation switch `MaxRecreate` (KNOWN, 0) |
| Additional exploration (source offsets ahead of the table) | `SourceCommittedCovered` + `NoStrandedDataWritten` in every S1/S3/S4 hunt |

## 5. Intentionally not modeled (brief §3.2) and other gaps

* Data-file encoding, RowDelta/equality deletes (writers are append-only), SMT routing, schema evolution.
* NoSuchTable / table-UUID-mismatch skip (#18012, C-1, code-review-only): no table replacement is modeled, so
  `CheckpointSound` needs no "intentional skip" exception.
* CommitToTable / CommitComplete records are not appended to the control topic; their only protocol effect (a
  fenced send after a landed commit) is in `CoordCasCommit` / `CoordCheckpointFencedPartial`.
* Valid-through timestamp values (#17077 earlier/null) are not modeled; `ValidThroughSound` checks the completeness
  precondition only.
* Liveness defects F5 (hang after `shutdownNow`) and F6 (zombie stuck in `consumeAvailable`) are represented as
  behaviours (queued table tasks are never started after `terminate()` with one commit thread; no terminated check in
  `CoordConsume`) but no dedicated invariant targets them (brief: tests T-4/T-5). F7/F10 appear as
  `CoordCheckpointFail*` / thread-exit classes.
* A wakeup pending during `onPartitionsAssigned` (it would skip `task.open`) is not modeled; F9 covers the same
  "election missed" outcome.
* Duplicate DataComplete counting (the other F11 trigger) needs a task that answers a StartCommit after gaining a
  partition without revoking any: a revoke always restarts the Worker, whose new control consumer starts at
  `latest` and never sees the older StartCommit. In the model that confines duplicates to add-only moves.

## 6. Smoke-run evidence (spec generation phase; not a substitute for the hunting phase)

| Config | Result |
|---|---|
| MC.cfg | completed, no violation: 5,564,018 distinct states, depth 57, 8 min (standard safety + structural + FloorMonotone) |
| base.cfg (simulation) | no TypeOK / structural violation (20–30k traces, depth 100) |
| s1_refsplit | FloorSound violated, depth 18 (F1) |
| s2_floor_reachability | NoDuplicateRows violated, depth 31 (F3, crash window) |
| s2_multitable | NoDuplicateRows violated, depth 33 (F3, multi-table partial success) |
| s2_branch | FloorReadValidatorAgree violated, depth 14 (F4 witness) |
| s3_eager / s3_close_discard | SourceCommittedCovered violated (F2, eager and cooperative) |
| s4_stall / s4_liveness | NoCoordinatorStall / EventuallyCommitted violated (F9) |
| s5_readiness | ValidThroughSound violated (F11 / #18322 family) |
| s4_multitable_unknown | completed, no violation (17,309 states) |
| s1_onevariant, s3_faults_only, s4_zombie, s4_zombie_2ctl | no violation within 3–4 min smoke budgets (full runs pending) |
| Trace.cfg (synthetic 14-event trace) | TraceMatched holds; a corrupted stored floor is rejected at event 13 |
