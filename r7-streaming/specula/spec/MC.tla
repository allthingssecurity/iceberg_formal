--------------------------------- MODULE MC ---------------------------------
(***************************************************************************)
(* Model-checking specification for the Iceberg Kafka Connect sink.        *)
(*                                                                         *)
(* Wraps base with counter-bounded actions.  Bounded = actions that        *)
(* INTRODUCE nondeterminism: abstract time (commit interval, commit        *)
(* timeout), faults (crash, lost member, exceptions in close/open, table   *)
(* commit outcomes, offset-commit failures), operator/environment actions  *)
(* (restart, pause/resume, rebalance churn, -coord churn, other writers,   *)
(* snapshot expiry, branch drop, offset loss, control-topic recreation).   *)
(* Unbounded = reactive steps (task callbacks, put, worker answers, the    *)
(* coordinator loop, per-table commit steps, checkpoint) and the "join"    *)
(* rebalance that reassigns partitions left unowned by a departed member.  *)
(***************************************************************************)
EXTENDS base

\* Access to the original (un-wrapped) base operators.
kc == INSTANCE base

CONSTANTS
    MaxCommits,       \* StartCommit firings (commit interval elapsed)        -- all
    MaxTimeouts,      \* commit timeouts -> partial commits                   -- S5, Q3
    MaxRebalances,    \* Connect-group churn rebalances                       -- S3, Q1
    MaxLost,          \* members kicked out (onPartitionsLost)                -- S3, S4
    MaxCrashes,       \* task process crashes                                 -- Q1, Q2
    MaxRestarts,      \* restarts of FAILED / crashed tasks (rolling upgrade) -- S1
    MaxPauseResume,   \* pause/resume wakeups of the Connect consumer         -- S3 (F2)
    MaxCloseFaults,   \* exceptions in close()/open() steps                   -- S3
    MaxCasFaults,     \* non-success table-commit outcomes                    -- Q4, S2
    MaxCkptFaults,    \* -coord offset transaction failures                   -- S2, S4
    MaxCgRebalances,  \* -coord membership churn                              -- S4
    MaxOtherWrites,   \* other writers' snapshots                             -- S2 (F3)
    MaxExpires,       \* snapshot expirations                                 -- S2 (F3)
    MaxBranchDrops,   \* fast-forward main + drop commit branch               -- S2 (F4)
    MaxOffsetLoss,    \* -coord committed offsets lost                        -- S2, Q5
    MaxRecreate,      \* control topic recreated (KNOWN #15293; keep 0)       -- S1
    MaxCtlLen         \* state constraint: control-topic length

VARIABLE faults
faultVars == <<faults>>
mc_vars   == <<vars, faultVars>>

Limit == [commits      |-> MaxCommits,     timeouts    |-> MaxTimeouts,
          rebalances   |-> MaxRebalances,  lost        |-> MaxLost,
          crashes      |-> MaxCrashes,     restarts    |-> MaxRestarts,
          pauses       |-> MaxPauseResume, closeFaults |-> MaxCloseFaults,
          casFaults    |-> MaxCasFaults,   ckptFaults  |-> MaxCkptFaults,
          cgRebalances |-> MaxCgRebalances, otherWrites |-> MaxOtherWrites,
          expires      |-> MaxExpires,     branchDrops |-> MaxBranchDrops,
          offsetLoss   |-> MaxOffsetLoss,  recreates   |-> MaxRecreate]

\* Guard and increment on the same transition.
Bounded(key, A) ==
    /\ faults[key] < Limit[key]
    /\ A
    /\ faults' = [faults EXCEPT ![key] = @ + 1]

-----------------------------------------------------------------------------
(* Counter-bounded wrappers *)

\* abstract time: commit interval elapsed (also for a fenced instance, whose
\* StartCommit send fails) / commit timeout elapsed
MCStartCommit(k) == Bounded("commits", kc!CoordStartCommit(k) \/ kc!CoordStartCommitFenced(k))
MCTimeout(k)     == Bounded("timeouts", kc!CoordTimeout(k))

\* Connect group
MCRebalance       == Bounded("rebalances", kc!Rebalance)
MCRebalanceLose(x) == Bounded("lost", kc!RebalanceLose(x))
MCCrash(t)        == Bounded("crashes", kc!Crash(t))
MCRestart(t)      == Bounded("restarts", kc!Restart(t))
MCPauseResume(t)  == Bounded("pauses", kc!PauseResume(t))
MCCloseFault(t)   == Bounded("closeFaults", \/ kc!CloseDiscardFail(t)
                                            \/ kc!CloseElectFail(t)
                                            \/ kc!CloseSeekFail(t)
                                            \/ kc!OpenFail(t))

\* coordinator / catalog / Kafka faults
MCCasFault(k, g) == Bounded("casFaults", kc!CoordCasFault(k, g))
MCCkptFault(k)   == Bounded("ckptFaults", kc!CoordCheckpointFailPartial(k)
                                          \/ kc!CoordCheckpointFailFull(k))
MCCgRebalance    == Bounded("cgRebalances", kc!CgRebalance)

\* environment
MCOtherWrite(T)  == Bounded("otherWrites", kc!OtherWriterCommit(T))
MCExpire(T, i)   == Bounded("expires", kc!ExpireSnapshot(T, i))
MCBranchDrop(T)  == Bounded("branchDrops", kc!FastForwardDropBranch(T))
MCLoseOffsets(c) == Bounded("offsetLoss", kc!LoseCoordOffsets(c))
MCRecreate       == Bounded("recreates", kc!RecreateControlTopic)

-----------------------------------------------------------------------------
(* Unbounded (reactive) actions *)

\* The group rebalances whenever a partition is in no live member's assignment
\* (initial join, a FAILED / crashed / kicked-out owner left -- its stale
\* `owner` entry stays until this generation --, the follow-up generation of a
\* cooperative move): reactive, not churn.  Sticky: owned partitions stay
\* with their owners (moving them is churn, bounded by MaxRebalances), so
\* reactive generations cannot chain forever.
NeedsRebalance ==
    \E p \in SrcParts : IF owner[p] \in Members THEN p \notin assign[owner[p]] ELSE TRUE
MCJoinRebalance == NeedsRebalance /\ kc!RebalanceWith({}, TRUE) /\ UNCHANGED faultVars

MCTaskReactive(t) == kc!TaskNext(t) /\ UNCHANGED faultVars

MCCoordReactive(k) ==
    /\ \/ kc!CoordStart(k) \/ kc!CoordSkipStartCommit(k) \/ kc!CoordExit(k)
       \/ kc!CoordPollEmpty(k)
       \/ \E c \in CtlParts : kc!CoordConsume(k, c) \/ kc!CoordOutOfRange(k, c)
       \/ \E g \in Groups : \/ kc!CoordReadFloor(k, g) \/ kc!CoordCasCommit(k, g)
                            \/ kc!CoordCasValidationFail(k, g)
       \/ kc!CoordCommitSwallow(k) \/ kc!CoordCommitRetry(k) \/ kc!CoordCommitThrow(k)
       \/ kc!CoordCheckpoint(k) \/ kc!CoordCheckpointFencedPartial(k)
       \/ kc!CoordCheckpointFencedFull(k)
    /\ UNCHANGED faultVars

-----------------------------------------------------------------------------
(* Initial state and next-state relation *)

MCInit ==
    /\ Init
    /\ faults = [x \in DOMAIN Limit |-> 0]

MCEnv ==
    \/ MCRebalance \/ MCCgRebalance \/ MCRecreate
    \/ \E T \in Tables : \/ MCOtherWrite(T) \/ MCBranchDrop(T)
                         \/ \E i \in 1..Len(snaps[T]) : MCExpire(T, i)
    \/ \E c \in CtlParts : MCLoseOffsets(c)

MCTaskSide(t) ==
    \/ MCTaskReactive(t)
    \/ MCCrash(t) \/ MCRestart(t) \/ MCPauseResume(t) \/ MCCloseFault(t)
    \/ MCRebalanceLose(t)

MCCoordFaults(k) == MCCkptFault(k) \/ \E g \in Groups : MCCasFault(k, g)

MCNext ==
    \/ \E t \in Tasks : MCTaskSide(t)
    \/ \E k \in CoordIds : MCCoordReactive(k) \/ MCStartCommit(k) \/ MCTimeout(k)
                           \/ MCCoordFaults(k)
    \/ MCJoinRebalance
    \/ MCEnv

MCSpec == MCInit /\ [][MCNext]_mc_vars

-----------------------------------------------------------------------------
(* Symmetry, view, state constraint *)

\* Tasks are interchangeable when they all run the same worker version
\* (LegacyTasks = {} or = Tasks); do not use with mixed versions.
Symmetry == Permutations(Tasks)

\* Excludes fault counters.  Merging states with different remaining budgets
\* under-approximates; used only in MC.cfg, never in hunting configs.
ModelView == vars

MCConstraint == \A c \in CtlParts : Len(ctl[c]) <= MaxCtlLen

-----------------------------------------------------------------------------
(* Liveness (S4/S2 -- MC-5): EventuallyCommitted                           *)
(*                                                                         *)
(* Time-driven actions are strongly fair; StartCommit is only taken when   *)
(* there is something to commit, so that a finite budget suffices.  A      *)
(* violation is a behavior in which a visible DataWritten never reaches    *)
(* its table although the time budget (commits, timeouts) is not used up: *)
(* a stall that more time cannot fix (F8/F9 no coordinator, a FAILED task  *)
(* after the restart budget).  An endless retry loop (F4 with partial      *)
(* commits) consumes the time budget and is therefore NOT reported here;   *)
(* F4 is witnessed by the state predicate FloorReadValidatorAgree below.   *)
(***************************************************************************)
PendingWork ==
    \/ \E d \in VisibleDWs : ~InTable(DWAt(d).tbl, d)
    \/ \E t \in Tasks : wBuf[t] # {}
    \/ \E t \in Tasks : \E p \in assign[t] : pos[t][p] >= 0 /\ pos[t][p] < NumRecs

MCStartCommitLive(k) == PendingWork /\ MCStartCommit(k)

MCLiveNext ==
    \/ \E t \in Tasks : MCTaskSide(t)
    \/ \E k \in CoordIds : MCCoordReactive(k) \/ MCStartCommitLive(k) \/ MCTimeout(k)
                           \/ MCCoordFaults(k)
    \/ MCJoinRebalance
    \/ MCEnv

MCLiveSpec ==
    /\ MCInit
    /\ [][MCLiveNext]_mc_vars
    /\ \A t \in Tasks : WF_mc_vars(MCTaskReactive(t)) /\ WF_mc_vars(MCRestart(t))
    /\ \A k \in CoordIds : /\ WF_mc_vars(MCCoordReactive(k))
                           /\ SF_mc_vars(MCStartCommitLive(k))
                           /\ SF_mc_vars(MCTimeout(k))
    /\ WF_mc_vars(MCJoinRebalance)

DWExists(c, i)    == i \in (ctlBase[c] + 1)..Len(ctl[c]) /\ ctl[c][i].type = "DW"
DWCommitted(c, i) == i <= Len(ctl[c]) /\ ctl[c][i].type = "DW" /\ InTable(ctl[c][i].tbl, <<c, i>>)
TimeBudgetUsed    == \/ MaxCommits > 0 /\ faults.commits >= MaxCommits
                     \/ MaxTimeouts > 0 /\ faults.timeouts >= MaxTimeouts

EventuallyCommitted ==
    \A c \in CtlParts : \A i \in 1..MaxCtlLen :
        DWExists(c, i) ~> (DWCommitted(c, i) \/ TimeBudgetUsed)

\* F4 diagnostic (MC-5): the floor the coordinator reads and the floor the
\* validator checks disagree, so every commit to T fails validation.
FloorReadValidatorAgree == \A T \in Tables : ReadFloor(T) = ValidatorFloor(T)

\* MC-5 (F8/F9) safety encoding: every task is up, an uncommitted DataWritten
\* is visible, no coordinator can still run a commit cycle, no election is
\* pending and no reactive (join) rebalance is due -- only an external
\* rebalance could restore progress (F8: fenced coordinator cleared while
\* startCoordinator() was a no-op; F9: election only from ADDED partitions).
CoordinatorStall ==
    /\ \A t \in Tasks : tUp[t]
    /\ \E d \in VisibleDWs : ~InTable(DWAt(d).tbl, d)
    /\ \A k \in CoordIds : ~LiveCoord(k)
    /\ ~NeedsRebalance
    /\ \A t \in Members : tStep[t] = "run" /\ pendAdd[t] = {}
NoCoordinatorStall == ~CoordinatorStall

=============================================================================
