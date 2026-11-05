-------------------------------- MODULE Trace --------------------------------
(***************************************************************************)
(* Trace validation for the Iceberg Kafka Connect sink (Category A: one    *)
(* totally ordered NDJSON trace; see instrumentation-spec.md).             *)
(*                                                                         *)
(* Every trace event names exactly one base action (event.name), its actor *)
(* (event.actor = "task" | "coord" | "env", event.task / event.coord), the *)
(* action arguments (event.args) and the actor's post-state snapshot       *)
(* (event.state).  Each wrapper calls the base action and validates the    *)
(* post-state; global effects (control topic, Connect offsets, catalog,    *)
(* -coord offsets) are validated by the events that cause them.           *)
(*                                                                         *)
(* Offsets in the trace are record ORDINALS per control partition (Kafka   *)
(* transaction markers removed by the harness post-processor), so a        *)
(* DataWritten at ordinal o is the spec record <<c, o + 1>>.               *)
(***************************************************************************)
EXTENDS base, Json, IOUtils, Sequences, TLC

-----------------------------------------------------------------------------
(* Trace loading *)

JsonFile ==
    IF "JSON" \in DOMAIN IOEnv THEN IOEnv.JSON
    ELSE "../traces/trace.ndjson"

TraceLog == TLCEval(
    LET all == ndJsonDeserialize(JsonFile)
    IN SelectSeq(all, LAMBDA x : /\ "tag" \in DOMAIN x
                                 /\ x.tag = "trace"
                                 /\ "event" \in DOMAIN x))

ASSUME Len(TraceLog) > 0

VARIABLE l           \* cursor into TraceLog (1-indexed)
traceVars == <<l>>

logline == TraceLog[l]
ev      == logline.event
A       == ev.args
S       == ev.state

-----------------------------------------------------------------------------
(* JSON helpers: arrays are sequences; maps are arrays of [key, value]     *)

SetOf(s)  == {s[i] : i \in DOMAIN s}
PartSet(s) == SetOf(s)
RecSet(s)  == {<<x[1], x[2]>> : x \in SetOf(s)}            \* [[p, off], ...]
DWIdSet(s) == {<<x[1], x[2] + 1>> : x \in SetOf(s)}        \* [[c, ordinal], ...]
PairMap(s, dom, dflt) ==
    [x \in dom |-> LET m == {pr \in SetOf(s) : pr[1] = x}
                   IN IF m = {} THEN dflt ELSE (CHOOSE pr \in m : TRUE)[2]]
GroupArg == <<A.tbl, A.var>>

IsEvent(name) == l <= Len(TraceLog) /\ ev.name = name
IsTaskEvent   == l <= Len(TraceLog) /\ ev.actor = "task"
IsCoordEvent  == l <= Len(TraceLog) /\ ev.actor = "coord"
IsEnvEvent    == l <= Len(TraceLog) /\ ev.actor = "env"

StepTrace == l' = l + 1

-----------------------------------------------------------------------------
(* Bootstrap: a fresh cluster -- empty control topic, empty tables, tasks  *)
(* started but not assigned (same as base Init).                           *)
TraceInit == Init /\ l = 1

-----------------------------------------------------------------------------
(***************************************************************************)
(* Post-state validation (strong unless noted)                             *)
(***************************************************************************)

\* Task actor: the full CommitterImpl / Worker / consumer snapshot.
\* Weak on purpose: taskGen is not compared (the collapsed cooperative rounds
\* number generations differently), tStep is implied by the event name.
ValidateTask(t) ==
    IF ~S.up
    THEN tUp'[t] = FALSE
    ELSE /\ tUp'[t] = TRUE
         /\ tVariant'[t] = S.variant
         /\ assign'[t] = PartSet(S.assign)
         /\ pos'[t] = PairMap(S.pos, SrcParts, -1)
         /\ stored'[t] = S.stored
         /\ worker'[t] = S.worker
         /\ wBuf'[t] = RecSet(S.buf)
         /\ coordRef'[t] = S.coord
         /\ (worker'[t] => wcPart'[t] = S.wpart)
         /\ (worker'[t] => wcPos'[t] = PairMap(S.wpos, CtlParts, 0))

\* Coordinator actor.  After the thread exits only pc and the error class are
\* compared (the spec canonicalises a retired instance).  The first-failure
\* class cErr is not compared inside a commit (no implementation field).
ValidateCoord(k) ==
    IF S.pc \in {"exited", "dead"}
    THEN /\ cPc'[k] = S.pc
         /\ (S.pc = "exited" => cErr'[k] = S.err)
    ELSE /\ cPc'[k] = S.pc
         /\ cTerm'[k] = S.terminated
         /\ cPos'[k] = PairMap(S.pos, CtlParts, -1)
         /\ cHwm'[k] = PairMap(S.hwm, CtlParts, -1)
         /\ cCkpt'[k] = PairMap(S.ckpt, CtlParts, -1)
         /\ cBuf'[k] = DWIdSet(S.buf)
         /\ cCid'[k] = S.cid
         /\ cCount'[k] = S.count
         /\ cTotal'[k] = S.total
         /\ cFails'[k] = S.fails
         /\ cMode'[k] = S.mode

\* Per-table commit task state for group events.
ValidateGroup(k) ==
    LET g == GroupArg IN
    /\ cGrp'[k][g].st = A.st
    /\ (A.st = "read" =>
          /\ cGrp'[k][g].exp = PairMap(A.exp, CtlParts, -1)
          /\ cGrp'[k][g].merged = PairMap(A.merged, CtlParts, -1)
          /\ cGrp'[k][g].dws = DWIdSet(A.files))

\* Global effects, checked by the event that causes them.
\* (arguments are bound before priming: A depends on the cursor l)
ValidateWorkerTxn ==
    LET c == A.part IN
    /\ \A pr \in SetOf(A.offsets) : srcCommitted'[pr[1]] = pr[2]
    /\ Len(ctl'[c]) - ctlBase'[c] = A.end

ValidateLanded ==
    LET T == A.tbl IN
    /\ Len(snaps'[T]) = A.snapCount
    /\ snaps'[T][Len(snaps'[T])].floor = PairMap(A.floor, CtlParts, -1)
    /\ snaps'[T][Len(snaps'[T])].dws = DWIdSet(A.files)

ValidateNotLanded == snaps' = snaps

ValidateCheckpoint == \A pr \in SetOf(A.committed) : cgCommitted'[pr[1]] = pr[2]

ValidateStartCommit ==
    LET c == A.part
        o == A.end - 1
    IN /\ Len(ctl'[c]) - ctlBase'[c] = A.end
       /\ ctl'[c][ctlBase'[c] + o + 1].cid = S.cid

-----------------------------------------------------------------------------
(***************************************************************************)
(* Action wrappers: event name -> base action (1:1 with                    *)
(* instrumentation-spec.md section 2)                                      *)
(***************************************************************************)
TaskAction(t) ==
    LET n == ev.name IN
    CASE n = "Put"                    -> Put(t, A.p)
      [] n = "WorkerAnswer"           -> WorkerAnswer(t, A.c) /\ ValidateWorkerTxn
      [] n = "WorkerAnswerRejected"   -> WorkerAnswerRejected(t, A.c)
      [] n = "WorkerOutOfRange"       -> WorkerOutOfRange(t, A.c)
      [] n = "PollLost"               -> PollLost(t)
      [] n = "PollRethrowWakeup"      -> PollRethrowWakeup(t)
      [] n = "PollRethrowFatal"       -> PollRethrowFatal(t)
      [] n = "PollWakeup"             -> PollWakeup(t)
      [] n = "PauseResume"            -> PauseResume(t)
      [] n = "CloseDiscard"           -> CloseDiscard(t)
      [] n = "CloseDiscardFail"       -> CloseDiscardFail(t)
      [] n = "CloseElect"             -> CloseElect(t)
      [] n = "CloseElectFail"         -> CloseElectFail(t)
      [] n = "CloseSeek"              -> CloseSeek(t)
      [] n = "CloseSeekWakeup"        -> CloseSeekWakeup(t)
      [] n = "CloseSeekFail"          -> CloseSeekFail(t)
      [] n = "Open"                   -> Open(t)
                                         \* a coordinator was started iff the event
                                         \* carries its producer partition / total
                                         /\ IF coordRef'[t] # coordRef[t]
                                            THEN /\ "cpart" \in DOMAIN A
                                                 /\ cPart'[coordRef'[t]] = A.cpart
                                                 /\ cTotal'[coordRef'[t]] = A.total
                                            ELSE ~("cpart" \in DOMAIN A)
      [] n = "OpenFail"               -> OpenFail(t)
      [] n = "ClearFencedCoordinator" -> ClearFencedCoordinator(t)
      [] n = "CoordinatorNotRunning"  -> CoordinatorNotRunning(t)
      [] n = "Crash"                  -> Crash(t)
      [] n = "Restart"                -> Restart(t)
      [] OTHER                        -> FALSE

CoordAction(k) ==
    LET n == ev.name IN
    CASE n = "CoordStart"                   -> CoordStart(k)
      [] n = "CoordStartCommit"             -> CoordStartCommit(k) /\ ValidateStartCommit
      [] n = "CoordStartCommitFenced"       -> CoordStartCommitFenced(k)
      [] n = "CoordExit"                    -> CoordExit(k)
      [] n = "CoordConsume"                 -> CoordConsume(k, A.c)
      [] n = "CoordOutOfRange"              -> CoordOutOfRange(k, A.c)
      [] n = "CoordTimeout"                 -> CoordTimeout(k)
      [] n = "CoordReadFloor"               -> CoordReadFloor(k, GroupArg) /\ ValidateGroup(k)
      [] n = "CoordCasCommit"               -> CoordCasCommit(k, GroupArg) /\ ValidateGroup(k)
                                               /\ ValidateLanded
      [] n = "CoordCasValidationFail"       -> CoordCasValidationFail(k, GroupArg)
                                               /\ ValidateGroup(k) /\ ValidateNotLanded
      [] n = "CoordCasFault"                -> CoordCasFault(k, GroupArg) /\ ValidateGroup(k)
                                               /\ cErr'[k] = FirstErr(k, A.cls)
                                               /\ IF A.landed THEN ValidateLanded
                                                  ELSE ValidateNotLanded
      [] n = "CoordCommitSwallow"           -> CoordCommitSwallow(k)
      [] n = "CoordCommitRetry"             -> CoordCommitRetry(k)
      [] n = "CoordCommitThrow"             -> CoordCommitThrow(k)
      [] n = "CoordCheckpoint"              -> CoordCheckpoint(k) /\ ValidateCheckpoint
      [] n = "CoordCheckpointFencedPartial" -> CoordCheckpointFencedPartial(k)
      [] n = "CoordCheckpointFencedFull"    -> CoordCheckpointFencedFull(k)
      [] n = "CoordCheckpointFailPartial"   -> CoordCheckpointFailPartial(k)
      [] n = "CoordCheckpointFailFull"      -> CoordCheckpointFailFull(k)
      [] OTHER                              -> FALSE

\* Environment events are emitted by the harness when it injects them.
\* Rebalance: inserted by the trace post-processor once per Connect-group
\* generation (each cooperative round is its own generation), before the
\* first callback event of the generation -- or earlier, before a
\* WorkerAnswerRejected that proves the broker already moved past the
\* member's generation -- with the assignment of the generation (moving
\* partitions unowned) and the kicked-out member, if any.
EnvAction ==
    LET n == ev.name IN
    CASE n = "Rebalance"             -> RebalanceWith(SetOf(A.lost), FALSE)
                                        /\ owner' = PairMap(A.owner, SrcParts, NoTask)
      [] n = "OtherWriterCommit"     -> OtherWriterCommit(A.tbl)
                                        /\ Len(snaps'[A.tbl]) = A.snapCount
      [] n = "ExpireSnapshot"        -> ExpireSnapshot(A.tbl, A.idx)
      [] n = "FastForwardDropBranch" -> FastForwardDropBranch(A.tbl)
      [] n = "LoseCoordOffsets"      -> LoseCoordOffsets(A.c)
      [] n = "RecreateControlTopic"  -> RecreateControlTopic
      [] n = "CgRebalance"           -> CgRebalance
      [] OTHER                       -> FALSE

\* Every event whose action reassigns the -coord group (CoordStart, thread
\* exits, Crash, CgRebalance) carries the resulting assignment.
ValidateCg == IF "cgOwner" \in DOMAIN A
              THEN cgOwner' = PairMap(A.cgOwner, CtlParts, 0) ELSE TRUE

TaskEventIfLogged ==
    /\ IsTaskEvent
    /\ LET t == ev.task IN
       /\ t \in Tasks
       /\ TaskAction(t)
       /\ ValidateTask(t)
       /\ ValidateCg
       /\ StepTrace

CoordEventIfLogged ==
    /\ IsCoordEvent
    /\ LET k == ev.coord IN
       /\ k \in CoordIds
       /\ CoordAction(k)
       /\ ValidateCoord(k)
       /\ ValidateCg
       /\ StepTrace

EnvEventIfLogged ==
    /\ IsEnvEvent
    /\ EnvAction
    /\ ValidateCg
    /\ StepTrace

-----------------------------------------------------------------------------
(***************************************************************************)
(* Silent actions (no trace event), each enabled only while the NEXT       *)
(* event needs it and disabled once it fired (no silent cycles).           *)
(***************************************************************************)

\* The coordinator loop is not instrumented per iteration: move k to the pc
\* the next k-event starts from (PollEmpty: consume -> loop, SkipStartCommit:
\* loop -> consume).
NeedsLoop    == {"CoordStartCommit", "CoordStartCommitFenced", "CoordTimeout", "CoordExit"}
NeedsConsume == {"CoordConsume"}

SilentCoordLoop ==
    /\ IsCoordEvent
    /\ LET k == ev.coord IN
       /\ k \in CoordIds
       /\ \/ ev.name \in NeedsLoop /\ cPc[k] = "consume" /\ CoordPollEmpty(k)
          \/ ev.name \in NeedsConsume /\ cPc[k] = "loop" /\ CoordSkipStartCommit(k)
    /\ UNCHANGED l

\* Connect skips task.open for an empty assignment: a member that only
\* revoked (or an eager member with nothing to open) finishes the callback
\* without an event (deterministic, one step).
SilentRejoin ==
    /\ l <= Len(TraceLog)
    /\ \E t \in Tasks :
         /\ tUp[t] /\ tStep[t] = "open" /\ pendAdd[t] = {}
         /\ Open(t)
    /\ UNCHANGED l

\* A member whose assignment did not change in a generation learns the new
\* generation in a later poll, without a callback (no event).  The KIP-447
\* check of an accepted WorkerAnswer is the only reader of taskGen, so the
\* sync is placed right before such an event (deterministic, one step).
SilentGenSync ==
    /\ IsEvent("WorkerAnswer")
    /\ LET t == ev.task IN
       /\ t \in Tasks
       /\ Rejoin(t)
    /\ UNCHANGED l

-----------------------------------------------------------------------------
TraceNext ==
    \/ TaskEventIfLogged
    \/ CoordEventIfLogged
    \/ EnvEventIfLogged
    \/ SilentCoordLoop
    \/ SilentRejoin
    \/ SilentGenSync

TraceSpec == TraceInit /\ [][TraceNext]_<<l, vars>> /\ WF_<<l, vars>>(TraceNext)

TraceView == <<vars, l>>

TraceMatched == <>(l > Len(TraceLog))

TraceAlias ==
    [ l        |-> l,
      len      |-> Len(TraceLog),
      event    |-> IF l <= Len(TraceLog) THEN ev.name ELSE "DONE",
      actor    |-> IF l <= Len(TraceLog) THEN ev.actor ELSE "DONE",
      assign   |-> assign,
      pos      |-> pos,
      wBuf     |-> wBuf,
      tStep    |-> tStep,
      coordRef |-> coordRef,
      cPc      |-> cPc,
      cPos     |-> cPos,
      cHwm     |-> cHwm,
      cBuf     |-> cBuf,
      cCid     |-> cCid,
      ctlEnd   |-> [c \in CtlParts |-> End(c)],
      srcComm  |-> srcCommitted,
      cgComm   |-> cgCommitted,
      floors   |-> [T \in Tables |-> ReadFloor(T)] ]

=============================================================================
