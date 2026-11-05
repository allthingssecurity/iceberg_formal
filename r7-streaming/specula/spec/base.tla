-------------------------------- MODULE base --------------------------------
(***************************************************************************)
(* Apache Iceberg Kafka Connect sink -- exactly-once commit protocol.      *)
(*                                                                         *)
(* Source: apache/iceberg @ 9fffeb6 (2026-11-02).  Path abbreviations used *)
(* in every annotation below:                                              *)
(*   CI = kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/    *)
(*        connect/channel/                                                 *)
(*   KC = kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/    *)
(*        connect/                                                         *)
(*   EV = kafka-connect/kafka-connect-events/src/main/java/org/apache/     *)
(*        iceberg/connect/events/                                          *)
(*   CORE = core/src/main/java/org/apache/iceberg/                         *)
(*   Environment semantics (Kafka 3.9.2 / Connect runtime) are cited by    *)
(*   class:line as in analysis-report.md section 3.1 (E1-E7).              *)
(*                                                                         *)
(* System category: A (distributed / message-passing), modeling-brief §1. *)
(*                                                                         *)
(* Actors:                                                                 *)
(*  - Connect sink tasks t (IcebergSinkTask + CommitterImpl + Worker): a   *)
(*    Connect consumer over source partitions, a SinkWriter buffer, and a  *)
(*    Worker control consumer/producer.                                    *)
(*  - Coordinator instances k (Coordinator + CoordinatorThread), one per   *)
(*    election, each with its own -coord consumer and a transactional      *)
(*    producer whose epoch fences older instances (#18039).                *)
(*  - Environment: Kafka control topic (read_committed log), the Connect   *)
(*    consumer group (generations, KIP-447), the connect-<name>-coord      *)
(*    group (committed offsets, eager reassignment), and the Iceberg       *)
(*    catalog (CAS table commits, snapshot ancestry, other writers,       *)
(*    snapshot expiry, branch refs).                                       *)
(*                                                                         *)
(* Abstractions (each justified in brief §3.2 or noted inline):            *)
(*  - A source record is <<p, off>>; "files" are the set of records a      *)
(*    worker wrote for a table; every record is routed to every table      *)
(*    (static routing, KC/data/SinkWriter.java:96-105).                    *)
(*  - CommitToTable / CommitComplete events are not appended to the log    *)
(*    (informational, brief §3.2); the only protocol-relevant effect of    *)
(*    the CommitToTable send -- failing with ProducerFencedException after *)
(*    a landed table commit -- is modeled in CoordCasCommit.               *)
(*  - Time is abstract: commit interval / commit timeout are               *)
(*    nondeterministic (bounded in MC).  Kafka txn markers are not         *)
(*    modeled (offsets are contiguous).                                    *)
(*  - One RebalanceWith step = one generation of the Connect group (join   *)
(*    completed at the broker).  Cooperative: a partition another member  *)
(*    still owns is only revoked in that generation and assigned in the   *)
(*    follow-up one (ConsumerCoordinator.onJoinComplete, requestRejoin);  *)
(*    eager: every member revokes everything, the new owner's open is     *)
(*    ordered after the revoke by CanAdd.  A member whose assignment does *)
(*    not change learns the new generation only in a later poll (Rejoin). *)
(***************************************************************************)
EXTENDS Naturals, Integers, FiniteSets, Sequences, TLC

CONSTANTS
    Tasks,             \* Connect sink task ids (model values)
    NoTask,            \* "no task" marker (model value)
    SrcParts,          \* source partitions, naturals 0..n-1 (ordered: the
                       \*   election uses the minimum, CI/CommitterImpl.java:122-128)
    NumRecs,           \* records per source partition (offsets 0..NumRecs-1)
    Tables,            \* Iceberg tables (model values)
    NoTable,           \* "no table" marker (model value)
    CtlParts,          \* control-topic partitions, naturals
    MaxCoord,          \* coordinator instance ids are 1..MaxCoord (one per election)
    RefVariants,       \* TableReference variants: subset of {"legacy","v"} (S1/F1)
    LegacyTasks,       \* tasks running <=1.10 worker code at Init (uuid-less refs)
    Protocol,          \* "eager" | "cooperative"  (S3; KIP-848 ~ cooperative)
    CoordReset,        \* "latest" | "earliest" auto.offset.reset of the -coord
                       \*   consumer (CI/KafkaClientFactory.java:55; "earliest" = PR #18006)
    FixNullSeek,       \* TRUE = assume the fix for the KNOWN null-offset seek (#18767)
    StaleViews,        \* TRUE = describe at open() may see a non-Stable group (F9)
    CommitThreads,     \* 1 = per-table commits run one at a time, 2 = in parallel
                       \*   (CI/Coordinator.java:111-121, CORE/util/Tasks.java:297-402)
    MaxConsecFailures, \* iceberg.control.commit.max-consecutive-failures (default 1,
                       \*   KC/IcebergSinkConfig.java:112)
    UseBranch          \* iceberg.tables.default-commit-branch set (S2/F4)

ASSUME Protocol \in {"eager", "cooperative"}
ASSUME CoordReset \in {"latest", "earliest"}
ASSUME RefVariants \subseteq {"legacy", "v"} /\ RefVariants # {}
ASSUME LegacyTasks \subseteq Tasks
ASSUME LegacyTasks # {} => "legacy" \in RefVariants
ASSUME LegacyTasks # Tasks => "v" \in RefVariants
ASSUME CommitThreads \in {1, 2}
ASSUME MaxConsecFailures \in Nat \ {0}
ASSUME SrcParts \subseteq Nat /\ CtlParts \subseteq Nat /\ CtlParts # {}
ASSUME NumRecs \in Nat \ {0} /\ MaxCoord \in Nat \ {0}
ASSUME FixNullSeek \in BOOLEAN /\ StaleViews \in BOOLEAN /\ UseBranch \in BOOLEAN

-----------------------------------------------------------------------------
(* Derived constants *)

Records  == SrcParts \X (0..(NumRecs - 1))
Groups   == Tables \X RefVariants          \* CommitState.tableCommitMap keys
CoordIds == 1..MaxCoord
NoFloor  == [c \in CtlParts |-> -1]         \* Map.of(): -1 encodes "key absent"

\* CI/CommitState.java:139-144 groups envelopes by the FULL TableReference;
\* EV/TableReference.java:144-160 includes uuid in equals/hashCode, so a
\* legacy (uuid == null, <=1.10 worker) and a v (uuid set, >=1.11 worker)
\* DataWritten for the same table are two groups (F1).

TaskSteps  == {"run", "closeDiscard", "closeElect", "closeSeek", "open"}
CloseSteps == {"closeDiscard", "closeElect", "closeSeek"}
CoordPcs   == {"idle", "start", "loop", "consume", "commit", "exited", "dead"}
ErrClasses == {"none", "commitFailed", "fenced", "other"}
GrpStates  == {"idle", "todo", "read", "done", "failed", "skipped"}

-----------------------------------------------------------------------------
VARIABLES
    \* ---- Connect consumer group / source offsets (environment, durable)
    srcCommitted,  \* [SrcParts -> Int]: connect-<name> committed offset (-1 = null);
                   \*   written ONLY by Worker transactions (CI/Channel.java:104-107;
                   \*   preCommit returns {} KC/IcebergSinkTask.java:99-104)
    owner,         \* [SrcParts -> Tasks \cup {NoTask}]: assignment of the current
                   \*   generation, as describeConsumerGroups reports it while the group is
                   \*   Stable; a FAILED / crashed member keeps its entries until the next
                   \*   generation (its LeaveGroup / session timeout triggers that one)
    gen,           \* Connect group generation (KIP-447 fencing, E1)
    \* ---- Connect task / CommitterImpl (in-memory, lost on crash)
    tUp,           \* [Tasks -> BOOLEAN]: task running (FALSE = FAILED or crashed)
    tVariant,      \* [Tasks -> RefVariants]: version of the worker code (S1)
    assign,        \* [Tasks -> SUBSET SrcParts]: consumer.assignment()
    pos,           \* [Tasks -> [SrcParts -> Int]]: fetch position (-1 = none)
    taskGen,       \* [Tasks -> Nat]: generation the task's consumer joined
    tStep,         \* [Tasks -> TaskSteps]: rebalance-callback program counter (S3)
    closing,       \* [Tasks -> SUBSET SrcParts]: argument of the running close()
    pendAdd,       \* [Tasks -> SUBSET SrcParts]: partitions to open() (assigned)
    lostParts,     \* [Tasks -> SUBSET SrcParts]: kicked out, onPartitionsLost pending
    stored,        \* [Tasks -> {"none","fatal","wakeup"}]: WorkerSinkTask
                   \*   rebalanceException (E3, WorkerSinkTask.java:786-790)
    wakeup,        \* [Tasks -> BOOLEAN]: pending consumer.wakeup() from pause/resume
                   \*   (WorkerSinkTask.java:204-207)
    coordRef,      \* [Tasks -> 0..MaxCoord]: CommitterImpl.coordinatorThread (0 = null)
    \* ---- Worker (CI/Worker.java) and SinkWriter buffer
    worker,        \* [Tasks -> BOOLEAN]: CommitterImpl.worker != null
    wBuf,          \* [Tasks -> SUBSET Records]: SinkWriter writers + sourceOffsets
    wcPos,         \* [Tasks -> [CtlParts -> Nat]]: Worker control consumer position
    wcPart,        \* [Tasks -> CtlParts]: partition of the worker producer key
    \* ---- Coordinator instances (CI/Coordinator.java, CI/CommitState.java)
    cPc,           \* [CoordIds -> CoordPcs]
    cHost,         \* [CoordIds -> Tasks \cup {NoTask}]
    cEpoch,        \* [CoordIds -> Nat]: producer epoch after initTransactions()
    cTerm,         \* [CoordIds -> BOOLEAN]: terminate() called (both terminated flags)
    cErr,          \* [CoordIds -> ErrClasses]: first failure of doCommit / exit error
    cPos,          \* [CoordIds -> [CtlParts -> Int]]: -coord consumer position
    cHwm,          \* [CoordIds -> [CtlParts -> Int]]: Channel.controlTopicOffsets
    cCkpt,         \* [CoordIds -> [CtlParts -> Int]]: Channel.committedOffsets
    cBuf,          \* [CoordIds -> SUBSET DW ids]: CommitState.commitBuffer
    cCid,          \* [CoordIds -> Nat]: currentCommitId (0 = null)
    cCount,        \* [CoordIds -> Nat]: receivedPartitionCount
    cDcParts,      \* [CoordIds -> SUBSET SrcParts]: ghost, partitions whose
                   \*   DataComplete for cCid was received (S5, ValidThroughSound)
    cTotal,        \* [CoordIds -> Nat]: totalPartitionCount frozen at construction
    cFails,        \* [CoordIds -> Nat]: consecutiveCommitFailures
    cMode,         \* [CoordIds -> {"none","full","partial"}]
    cGrp,          \* [CoordIds -> [Groups -> group record]]: per-table commit tasks
    cPart,         \* [CoordIds -> CtlParts]: partition of the coordinator producer key
    \* ---- connect-<name>-coord group and coordinator fencing (environment)
    cgOwner,       \* [CtlParts -> 0..MaxCoord]: -coord partition owner (0 = none)
    cgCommitted,   \* [CtlParts -> Int]: -coord committed offset (-1 = none)
    coordEpoch,    \* latest epoch of the stable coordinator transactional.id
    \* ---- control topic (environment)
    ctl,           \* [CtlParts -> Seq(Event)]: all records ever appended
    ctlBase,       \* [CtlParts -> Nat]: index where the current incarnation starts
    nextCid,       \* next commit id (UUID abstracted as a counter)
    \* ---- Iceberg catalog (environment)
    snaps,         \* [Tables -> Seq(Snapshot)]: every snapshot ever committed
    heads          \* [Tables -> [main: Nat, b: Nat]]: ref heads (0 = null / missing)

srcVars    == <<srcCommitted, owner, gen>>
taskVars   == <<tUp, tVariant, assign, pos, taskGen, tStep, closing, pendAdd,
                lostParts, stored, wakeup, coordRef>>
workerVars == <<worker, wBuf, wcPos, wcPart>>
coordVars  == <<cPc, cHost, cEpoch, cTerm, cErr, cPos, cHwm, cCkpt, cBuf, cCid,
                cCount, cDcParts, cTotal, cFails, cMode, cGrp, cPart>>
cgVars     == <<cgOwner, cgCommitted, coordEpoch>>
ctlVars    == <<ctl, ctlBase, nextCid>>
catVars    == <<snaps, heads>>
vars == <<srcVars, taskVars, workerVars, coordVars, cgVars, ctlVars, catVars>>

-----------------------------------------------------------------------------
(* Generic helpers *)

Max2(a, b)  == IF a >= b THEN a ELSE b
MinSet(S)   == CHOOSE x \in S : \A y \in S : x <= y
MaxSet(S)   == CHOOSE x \in S : \A y \in S : x >= y
FirstOf(S)  == IF S = {} THEN -1 ELSE MinSet(S)

RECURSIVE SetToSeq(_)
SetToSeq(S) == IF S = {} THEN <<>>
               ELSE LET x == CHOOSE y \in S : TRUE IN <<x>> \o SetToSeq(S \ {x})

TableSeq == SetToSeq(Tables)   \* order of SinkWriter writer results (HashMap order)

-----------------------------------------------------------------------------
(* Control topic helpers.  An event is a uniform record                    *)
(*   [type: {"SC","DW","DC"}, cid, tbl, var, rows, parts, off]            *)
(* SC = StartCommit, DW = DataWritten, DC = DataComplete (EV classes). A DW id*)
(* is <<c, i>>, its index in ctl[c]; e.off is its Kafka offset.            *)

End(c)     == Len(ctl[c]) - ctlBase[c]          \* log-end offset
Rec(c, o)  == ctl[c][ctlBase[c] + o + 1]        \* record at offset o (current incarnation)
DWAt(d)    == ctl[d[1]][d[2]]
DOff(d)    == ctl[d[1]][d[2]].off
GroupOf(d) == <<DWAt(d).tbl, DWAt(d).var>>

\* DataWritten records readable in the current incarnation of the topic
VisibleDWs == UNION {{<<c, i>> : i \in {j \in (ctlBase[c] + 1)..Len(ctl[c]) :
                                          ctl[c][j].type = "DW"}} : c \in CtlParts}

SCEvent(cid, o) == [type |-> "SC", cid |-> cid, tbl |-> NoTable, var |-> "none",
                    rows |-> {}, parts |-> {}, off |-> o]

-----------------------------------------------------------------------------
(* Catalog helpers.  A snapshot is                                         *)
(*   [parent: Nat (0 = none), hasKey: BOOLEAN, floor: [CtlParts -> Int],   *)
(*    dws: SUBSET DW ids, live: BOOLEAN, writer: {"conn","other"}]         *)
(* hasKey = summary contains kafka.connect.offsets.<topic>.<group>         *)
(* (CI/Coordinator.java:108-110); floor = its JSON map.                    *)

\* Coordinator.lastCommittedOffsets over SnapshotUtil.ancestorsOf
\* (CI/Coordinator.java:402-415, CORE/util/SnapshotUtil.java:243-254):
\* the first ancestor carrying the key; the walk stops at an expired parent.
RECURSIVE FirstKeyFloor(_, _)
FirstKeyFloor(T, i) ==
    IF i = 0 THEN NoFloor
    ELSE IF ~snaps[T][i].live THEN NoFloor
    ELSE IF snaps[T][i].hasKey THEN snaps[T][i].floor
    ELSE FirstKeyFloor(T, snaps[T][i].parent)

\* Coordinator.latestSnapshot (CI/Coordinator.java:388-393): table.snapshot(branch)
\* is null when the branch ref is missing (api Table.java:380-387).
CoordHead(T) == IF UseBranch THEN heads[T].b ELSE heads[T].main

\* SnapshotUtil.latestSnapshot(base, branch) used by SnapshotProducer.apply
\* (CORE/util/SnapshotUtil.java:478-489): a missing ref falls back to main (F4).
ValidatorHead(T) == IF UseBranch /\ heads[T].b # 0 THEN heads[T].b ELSE heads[T].main

ReadFloor(T)      == FirstKeyFloor(T, CoordHead(T))      \* CI/Coordinator.java:270,395-405
ValidatorFloor(T) == FirstKeyFloor(T, ValidatorHead(T))  \* CI/Coordinator.java:367-372,
                                                        \* CORE/SnapshotProducer.java:377-389

InTable(T, d) == \E i \in 1..Len(snaps[T]) : d \in snaps[T][i].dws

-----------------------------------------------------------------------------
(* Task helpers *)

BufParts(t)  == {r[1] : r \in wBuf[t]}
\* SinkWriter.save keeps offset+1 of the last record per partition
\* (KC/data/SinkWriter.java:85-87); records arrive in offset order.
BufOff(t, p) == MaxSet({r[2] : r \in {x \in wBuf[t] : x[1] = p}}) + 1

Members == {t \in Tasks : tUp[t] /\ lostParts[t] = {}}

\* Consumer position for a newly assigned partition: committed offset, else
\* auto.offset.reset (Connect sink default "earliest" = 0); requireStable (E2).
InitPos(p) == IF srcCommitted[p] # -1 THEN srcCommitted[p] ELSE 0

\* The task thread is outside poll with no pending callback / stored exception.
TaskIdle(t) == /\ tUp[t] /\ tStep[t] = "run" /\ pendAdd[t] = {}
               /\ stored[t] = "none"

\* Cooperative round 2 / eager join: an added partition is opened only after
\* every other (non-lost) owner finished revoking it.
CanAdd(t) == \A t2 \in Tasks \ {t} :
                (tStep[t2] \in CloseSteps /\ lostParts[t2] = {})
                    => closing[t2] \cap pendAdd[t] = {}

-----------------------------------------------------------------------------
(* Leader election views: CommitterImpl.hasLeaderPartition                 *)
(* (CI/CommitterImpl.java:77-128) describes the Connect group through      *)
(* Admin; a classic group that is not Stable is described with empty       *)
(* assignments (analysis-report §3.2 F9, #17637).                          *)

AssignedParts(view) == {p \in SrcParts : view[p] # NoTask}
InTransit  == UNION {closing[x] : x \in {y \in Tasks : tStep[y] \in CloseSteps
                                                       /\ lostParts[y] = {}}}
Round1View == [p \in SrcParts |-> IF p \in InTransit THEN NoTask ELSE owner[p]]
OldAssigned == UNION {assign[x] : x \in Tasks}

\* Possible results of hasLeaderPartition(closedPartitions) inside close():
\*  - lost: the group is Stable without this member, or still rebalancing;
\*  - eager: onJoinPrepare runs before JoinGroup: the group is still Stable
\*    with the old assignment, or already PreparingRebalance (empty);
\*  - cooperative: onJoinComplete of round 1: Stable with the round-1
\*    assignment in which the revoked partitions are unassigned.
CloseLeaderResults(t) ==
    LET S == closing[t] IN
    IF lostParts[t] # {} THEN {FALSE, FirstOf(AssignedParts(owner)) \in S}
    ELSE IF Protocol = "eager" THEN {FALSE, FirstOf(OldAssigned) \in S}
    ELSE {FirstOf(AssignedParts(Round1View)) \in S}

\* hasLeaderPartition(addedPartitions) inside open(): Stable new assignment;
\* with StaleViews also a not-yet-Stable (empty) view (F9).
OpenLeaderResults(t) ==
    LET accurate == FirstOf(AssignedParts(owner)) \in pendAdd[t] IN
    IF StaleViews THEN {FALSE, accurate} ELSE {accurate}

-----------------------------------------------------------------------------
(* Coordinator helpers *)

Fenced(k) == cEpoch[k] < coordEpoch     \* producer epoch superseded (#18039)

CgMembers == {k \in CoordIds : cPc[k] \in {"loop", "consume", "commit"}}

\* Position of a -coord partition after (re)assignment: committed offset, else
\* auto.offset.reset (CI/KafkaClientFactory.java:55, putIfAbsent "latest").
StartPos(c) == IF cgCommitted[c] # -1 /\ cgCommitted[c] <= End(c)
               THEN cgCommitted[c]
               ELSE IF CoordReset = "latest" THEN End(c) ELSE 0

\* Eager (range) reassignment of the -coord group among members M: every
\* assigned partition restarts from the committed offset (the rewind of
\* #16282 that the high-water mark at CI/Channel.java:126-139 neutralises).
CgAssignments(M) == IF M = {} THEN {[c \in CtlParts |-> 0]} ELSE [CtlParts -> M]
CgReassign(M) ==
    \E na \in CgAssignments(M) :
        /\ cgOwner' = na
        /\ cPos' = [k \in CoordIds |-> [c \in CtlParts |->
                        IF na[c] = k THEN StartPos(c) ELSE -1]]

IdleGrp == [st |-> "idle", exp |-> NoFloor, merged |-> NoFloor, dws |-> {}]
GroupsIn(buf) == {GroupOf(d) : d \in buf}
GroupFailed(k) == \E g \in Groups : cGrp[k][g].st = "failed"
SomeReading(k) == \E g \in Groups : cGrp[k][g].st = "read"
FirstErr(k, cls) == IF cErr[k] = "none" THEN cls ELSE cErr[k]

\* Channel.commitConsumerOffsets: partitions whose HWM advanced past this
\* instance's last committed value (CI/Channel.java:171-184).
ToCommit(k) == {c \in CtlParts : cHwm[k][c] # -1 /\
                   (cCkpt[k][c] = -1 \/ cHwm[k][c] > cCkpt[k][c])}

\* coordinator fields other than cPc / cPos (which CgReassign rewrites)
coordLocal == <<cHost, cEpoch, cTerm, cErr, cHwm, cCkpt, cBuf, cCid, cCount,
                cDcParts, cTotal, cFails, cMode, cGrp, cPart>>

-----------------------------------------------------------------------------
(***************************************************************************)
(* Initial state: tasks started (IcebergSinkTask.start, KC/IcebergSinkTask *)
(* .java:48-55) but not yet assigned; no coordinator; empty control topic; *)
(* empty tables.                                                           *)
(***************************************************************************)
Init ==
    /\ srcCommitted = [p \in SrcParts |-> -1]
    /\ owner        = [p \in SrcParts |-> NoTask]
    /\ gen          = 0
    /\ tUp          = [t \in Tasks |-> TRUE]
    /\ tVariant     = [t \in Tasks |-> IF t \in LegacyTasks THEN "legacy" ELSE "v"]
    /\ assign       = [t \in Tasks |-> {}]
    /\ pos          = [t \in Tasks |-> [p \in SrcParts |-> -1]]
    /\ taskGen      = [t \in Tasks |-> 0]
    /\ tStep        = [t \in Tasks |-> "run"]
    /\ closing      = [t \in Tasks |-> {}]
    /\ pendAdd      = [t \in Tasks |-> {}]
    /\ lostParts    = [t \in Tasks |-> {}]
    /\ stored       = [t \in Tasks |-> "none"]
    /\ wakeup       = [t \in Tasks |-> FALSE]
    /\ coordRef     = [t \in Tasks |-> 0]
    /\ worker       = [t \in Tasks |-> FALSE]
    /\ wBuf         = [t \in Tasks |-> {}]
    /\ wcPos        = [t \in Tasks |-> [c \in CtlParts |-> 0]]
    /\ wcPart       = [t \in Tasks |-> MinSet(CtlParts)]
    /\ cPc          = [k \in CoordIds |-> "idle"]
    /\ cHost        = [k \in CoordIds |-> NoTask]
    /\ cEpoch       = [k \in CoordIds |-> 0]
    /\ cTerm        = [k \in CoordIds |-> FALSE]
    /\ cErr         = [k \in CoordIds |-> "none"]
    /\ cPos         = [k \in CoordIds |-> NoFloor]
    /\ cHwm         = [k \in CoordIds |-> NoFloor]
    /\ cCkpt        = [k \in CoordIds |-> NoFloor]
    /\ cBuf         = [k \in CoordIds |-> {}]
    /\ cCid         = [k \in CoordIds |-> 0]
    /\ cCount       = [k \in CoordIds |-> 0]
    /\ cDcParts     = [k \in CoordIds |-> {}]
    /\ cTotal       = [k \in CoordIds |-> 0]
    /\ cFails       = [k \in CoordIds |-> 0]
    /\ cMode        = [k \in CoordIds |-> "none"]
    /\ cGrp         = [k \in CoordIds |-> [g \in Groups |-> IdleGrp]]
    /\ cPart        = [k \in CoordIds |-> MinSet(CtlParts)]
    /\ cgOwner      = [c \in CtlParts |-> 0]
    /\ cgCommitted  = [c \in CtlParts |-> -1]
    /\ coordEpoch   = 0
    /\ ctl          = [c \in CtlParts |-> <<>>]
    /\ ctlBase      = [c \in CtlParts |-> 0]
    /\ nextCid      = 1
    /\ snaps        = [T \in Tables |-> <<>>]
    /\ heads        = [T \in Tables |-> [main |-> 0, b |-> 0]]

-----------------------------------------------------------------------------
(***************************************************************************)
(* Shared state transformers                                               *)
(***************************************************************************)

\* Task process stops: WorkerSinkTask failure path closeAllPartitions ->
\* CommitterImpl.close(all) and IcebergSinkTask.stop -> close(List.of())
\* (KC/IcebergSinkTask.java:67-83,106-109; CI/CommitterImpl.java:161-176):
\* stopWorker discards every buffer; the consumer is closed (leaves the group).
\* `owner` is not changed: until the next generation the broker still
\* describes the old assignment (or, once the LeaveGroup is processed, a
\* non-Stable group without assignments -- StaleViews).
ResetTaskLocal(t) ==
    /\ tUp'       = [tUp EXCEPT ![t] = FALSE]
    /\ assign'    = [assign EXCEPT ![t] = {}]
    /\ pos'       = [pos EXCEPT ![t] = [p \in SrcParts |-> -1]]
    /\ tStep'     = [tStep EXCEPT ![t] = "run"]
    /\ closing'   = [closing EXCEPT ![t] = {}]
    /\ pendAdd'   = [pendAdd EXCEPT ![t] = {}]
    /\ lostParts' = [lostParts EXCEPT ![t] = {}]
    /\ stored'    = [stored EXCEPT ![t] = "none"]
    /\ wakeup'    = [wakeup EXCEPT ![t] = FALSE]
    /\ coordRef'  = [coordRef EXCEPT ![t] = 0]
    /\ worker'    = [worker EXCEPT ![t] = FALSE]
    /\ wBuf'      = [wBuf EXCEPT ![t] = {}]
    /\ UNCHANGED <<owner, tVariant, taskGen, wcPos, wcPart>>

\* Task FAILED (exception out of put()/poll()): the coordinator is only
\* terminate()d -- CoordinatorThread.terminate sets the flags and returns
\* without joining (CI/CoordinatorThread.java:83-86, CI/Coordinator.java:434-447;
\* CI/CommitterImpl.java:240-245), so the thread may linger (S4, F5/F6).
FailTask(t) ==
    /\ ResetTaskLocal(t)
    /\ cTerm' = IF coordRef[t] # 0 THEN [cTerm EXCEPT ![coordRef[t]] = TRUE] ELSE cTerm
    /\ UNCHANGED <<srcCommitted, gen, cPc, cHost, cEpoch, cErr, cPos, cHwm, cCkpt,
                   cBuf, cCid, cCount, cDcParts, cTotal, cFails, cMode, cGrp, cPart,
                   cgVars, ctlVars, catVars>>

\* End of a close(...) rebalance callback.  Lost: onPartitionsLost then the
\* member rejoins with no partitions (ConsumerCoordinator.onJoinPrepare);
\* revoke: the revoked partitions leave consumer.assignment() after the
\* callback (eager onJoinPrepare / cooperative onJoinComplete), then the
\* assign callback runs (tStep = "open").
CloseFinish(t, newPos) ==
    IF lostParts[t] # {} THEN
        /\ assign'    = [assign EXCEPT ![t] = {}]
        /\ pos'       = [pos EXCEPT ![t] = [p \in SrcParts |-> -1]]
        /\ lostParts' = [lostParts EXCEPT ![t] = {}]
        /\ taskGen'   = [taskGen EXCEPT ![t] = gen]
        /\ tStep'     = [tStep EXCEPT ![t] = "run"]
        /\ closing'   = [closing EXCEPT ![t] = {}]
    ELSE
        /\ assign'    = [assign EXCEPT ![t] = @ \ closing[t]]
        /\ pos'       = [pos EXCEPT ![t] = [p \in SrcParts |->
                            IF p \in closing[t] THEN -1 ELSE newPos[p]]]
        /\ tStep'     = [tStep EXCEPT ![t] = "open"]
        /\ closing'   = [closing EXCEPT ![t] = {}]
        /\ UNCHANGED <<lostParts, taskGen>>

-----------------------------------------------------------------------------
(***************************************************************************)
(* Connect group rebalance (environment).  One step = one generation: the  *)
(* broker completes the join once every (non-lost) member has rejoined,   *)
(* i.e. every member is inside poll() with no callback in progress, and   *)
(* bumps the generation (GroupMetadata.initNextGeneration).  L = member   *)
(* kicked out (poll / session timeout): it learns this only at its next   *)
(* poll (PollLost) and may still answer a StartCommit with its stale      *)
(* generation (KIP-447, E1).                                               *)
(* Eager: every member revokes everything (onJoinPrepare) and re-opens its *)
(* new assignment.  Cooperative (ConsumerCoordinator.java:336-433): a      *)
(* partition still owned by another member is only revoked by it in this  *)
(* generation (it stays unowned, "moving"); the revoker then requests a   *)
(* rejoin and the follow-up generation assigns it (the CooperativeSticky  *)
(* assignor never hands out a partition another member still owns).      *)
(* Unowned partitions are assigned directly.  A member with nothing to    *)
(* revoke or add gets no callback: it keeps running with its old          *)
(* generation until a later poll processes the join response (Rejoin).    *)
(* sticky = the assignment keeps every owned partition with its owner     *)
(* (only unowned partitions are handed out).                               *)
(***************************************************************************)
Assignments(M2, sticky) ==
    IF M2 = {} THEN {[p \in SrcParts |-> NoTask]}
    ELSE {na \in [SrcParts -> M2] :
            sticky => \A p \in SrcParts : \A y \in M2 : p \in assign[y] => na[p] = y}

RebalanceWith(L, sticky) ==
    /\ Members # {}
    /\ \A x \in Members : tStep[x] = "run" /\ pendAdd[x] = {} /\ stored[x] = "none"
    /\ L \subseteq Members
    /\ LET M2 == Members \ L IN
       \E na \in Assignments(M2, sticky) :
         LET mine(x) == {p \in SrcParts : na[p] = x}
             moving  == {p \in SrcParts : Protocol = "cooperative" /\
                                          \E y \in M2 : p \in assign[y] /\ na[p] # y}
             rev(x)  == IF Protocol = "eager" THEN assign[x] ELSE assign[x] \ mine(x)
             add(x)  == IF Protocol = "eager" THEN mine(x)
                        ELSE (mine(x) \ assign[x]) \ moving
         IN
         /\ gen'       = gen + 1
         /\ owner'     = [p \in SrcParts |-> IF p \in moving THEN NoTask ELSE na[p]]
         /\ closing'   = [x \in Tasks |-> IF x \in M2 THEN rev(x) ELSE closing[x]]
         /\ pendAdd'   = [x \in Tasks |-> IF x \in M2 THEN add(x) ELSE pendAdd[x]]
         /\ tStep'     = [x \in Tasks |->
                            IF x \in M2 /\ rev(x) # {} THEN "closeDiscard"
                            ELSE IF x \in M2 /\ add(x) # {} THEN "open"
                            ELSE tStep[x]]
         /\ lostParts' = [x \in Tasks |-> IF x \in L THEN assign[x] ELSE lostParts[x]]
    /\ UNCHANGED <<srcCommitted, tUp, tVariant, assign, pos, taskGen, stored, wakeup,
                   coordRef, workerVars, coordVars, cgVars, ctlVars, catVars>>

\* Membership change / churn without a lost member.
Rebalance == RebalanceWith({}, FALSE)

\* Member x is kicked out (poll or session timeout) while it is outside poll().
RebalanceLose(x) == x \in Members /\ assign[x] # {} /\ RebalanceWith({x}, FALSE)

\* A member whose assignment did not change in a generation processes the
\* join response in a later poll(): onJoinComplete updates its group metadata
\* (ConsumerCoordinator.java:351) with nothing revoked or added (Connect skips
\* task.open for an empty set, WorkerSinkTask.java:745-747).  Until then its
\* Worker's sendOffsetsToTransaction carries the old generation, which the
\* broker rejects with ILLEGAL_GENERATION (GroupCoordinator.scala:1022-1043:
\* no group-state exemption for transactional offset commits) --
\* WorkerAnswerRejected, the healthy task FAILS (harness gap 7).
Rejoin(t) ==
    /\ TaskIdle(t) /\ lostParts[t] = {} /\ ~wakeup[t]
    /\ taskGen[t] # gen
    /\ taskGen' = [taskGen EXCEPT ![t] = gen]
    /\ UNCHANGED <<srcVars, tUp, tVariant, assign, pos, tStep, closing, pendAdd,
                   lostParts, stored, wakeup, coordRef, workerVars, coordVars, cgVars,
                   ctlVars, catVars>>

\* A kicked-out member's next poll: onPartitionsLost -> closePartitions(lost)
\* -> IcebergSinkTask.close (WorkerSinkTask.java:774-794; KC/IcebergSinkTask.java:62-65).
PollLost(t) ==
    /\ tUp[t] /\ lostParts[t] # {} /\ tStep[t] = "run" /\ stored[t] = "none"
    /\ closing' = [closing EXCEPT ![t] = lostParts[t]]
    /\ tStep'   = [tStep EXCEPT ![t] = "closeDiscard"]
    /\ UNCHANGED <<srcVars, tUp, tVariant, assign, pos, taskGen, pendAdd, lostParts,
                   stored, wakeup, coordRef, workerVars, coordVars, cgVars, ctlVars,
                   catVars>>

-----------------------------------------------------------------------------
(***************************************************************************)
(* CommitterImpl.close(closedPartitions) (CI/CommitterImpl.java:160-187),  *)
(* split at its fallible calls (S3).  An exception escaping close() is     *)
(* stored by WorkerSinkTask and rethrown after poll() returns (E3).        *)
(***************************************************************************)

\* A wakeup already pending when poll() starts is raised by KafkaConsumer.poll
\* (client.maybeTriggerWakeup) before any callback runs (PollWakeup); only a
\* wakeup raised DURING the callback reaches committed() (F2 window).
\* Step 1 -- stopWorker() (CI/CommitterImpl.java:162-163,233-238): Worker.stop
\* (CI/Worker.java:116-120) drops the SinkWriter buffer of EVERY partition,
\* not only the closed ones.  isInitialized is always true here (open
\* precedes every close in this model, CI/CommitterImpl.java:165-169).
CloseDiscard(t) ==
    /\ tUp[t] /\ tStep[t] = "closeDiscard" /\ ~wakeup[t]
    /\ worker' = [worker EXCEPT ![t] = FALSE]
    /\ wBuf'   = [wBuf EXCEPT ![t] = {}]
    /\ tStep'  = [tStep EXCEPT ![t] = "closeElect"]
    /\ UNCHANGED <<srcVars, tUp, tVariant, assign, pos, taskGen, closing, pendAdd,
                   lostParts, stored, wakeup, coordRef, wcPos, wcPart, coordVars,
                   cgVars, ctlVars, catVars>>

\* Fault: Worker.stop / SinkWriter.close throws (e.g. writer close I/O):
\* `worker = null` and the rest of close() are skipped; stored => FAILED.
CloseDiscardFail(t) ==
    /\ tUp[t] /\ tStep[t] = "closeDiscard" /\ ~wakeup[t]
    /\ stored' = [stored EXCEPT ![t] = "fatal"]
    /\ wBuf'   = [wBuf EXCEPT ![t] = {}]
    /\ CloseFinish(t, pos[t])
    /\ UNCHANGED <<srcVars, tUp, tVariant, pendAdd, wakeup, coordRef, worker, wcPos,
                   wcPart, coordVars, cgVars, ctlVars, catVars>>

\* Step 2 -- if (hasLeaderPartition(closedPartitions)) stopCoordinator()
\* (CI/CommitterImpl.java:178-182,77-91,240-245).  stopCoordinator only sets
\* the terminated flags (no join).
CloseElect(t) ==
    /\ tUp[t] /\ tStep[t] = "closeElect"
    /\ \E isLeader \in CloseLeaderResults(t) :
         IF isLeader /\ coordRef[t] # 0
         THEN /\ cTerm'    = [cTerm EXCEPT ![coordRef[t]] = TRUE]
              /\ coordRef' = [coordRef EXCEPT ![t] = 0]
         ELSE UNCHANGED <<cTerm, coordRef>>
    /\ tStep' = [tStep EXCEPT ![t] = "closeSeek"]
    /\ UNCHANGED <<srcVars, tUp, tVariant, assign, pos, taskGen, closing, pendAdd,
                   lostParts, stored, wakeup, workerVars, cPc, cHost, cEpoch, cErr,
                   cPos, cHwm, cCkpt, cBuf, cCid, cCount, cDcParts, cTotal, cFails,
                   cMode, cGrp, cPart, cgVars, ctlVars, catVars>>

\* Fault: Admin describeConsumerGroups fails -> ConnectException
\* (CI/KafkaUtils.java:44-54); the seek is skipped (#16843); stored => FAILED.
CloseElectFail(t) ==
    /\ tUp[t] /\ tStep[t] = "closeElect"
    /\ stored' = [stored EXCEPT ![t] = "fatal"]
    /\ CloseFinish(t, pos[t])
    /\ UNCHANGED <<srcVars, tUp, tVariant, pendAdd, wakeup, coordRef, workerVars,
                   coordVars, cgVars, ctlVars, catVars>>

\* Step 3 -- KafkaUtils.seekToLastCommittedOffsets (CI/KafkaUtils.java:60-85):
\* committed(consumer.assignment()) then seek every partition whose committed
\* offset is non-null (:72-76).  consumer.assignment() still contains the
\* closed partitions during the callback, so retained (cooperative)
\* partitions are rewound here.  Null committed offset: no seek (KNOWN
\* #18767) unless FixNullSeek (seek to the auto.offset.reset position 0).
SeekPos(t) == [p \in SrcParts |->
                 IF p \in assign[t]
                 THEN IF srcCommitted[p] # -1 THEN srcCommitted[p]
                      ELSE IF FixNullSeek THEN 0 ELSE pos[t][p]
                 ELSE pos[t][p]]

CloseSeek(t) ==
    /\ tUp[t] /\ tStep[t] = "closeSeek" /\ ~wakeup[t]
    /\ CloseFinish(t, SeekPos(t))
    /\ UNCHANGED <<srcVars, tUp, tVariant, pendAdd, stored, wakeup, coordRef,
                   workerVars, coordVars, cgVars, ctlVars, catVars>>

\* F2: a wakeup pending from pause/resume (WorkerSinkTask.java:204-207) makes
\* consumer.committed() throw WakeupException (ConsumerNetworkClient.java:
\* 252-295,527-532), which is outside the IllegalStateException catch
\* (CI/KafkaUtils.java:77); no partition is rewound; Connect stores it.
CloseSeekWakeup(t) ==
    /\ tUp[t] /\ tStep[t] = "closeSeek" /\ wakeup[t]
    /\ stored' = [stored EXCEPT ![t] = "wakeup"]
    /\ wakeup' = [wakeup EXCEPT ![t] = FALSE]
    /\ CloseFinish(t, pos[t])
    /\ UNCHANGED <<srcVars, tUp, tVariant, pendAdd, coordRef, workerVars, coordVars,
                   cgVars, ctlVars, catVars>>

\* Fault: committed() fails otherwise (TimeoutException, reflection error
\* CI/KafkaUtils.java:87-97): no seek, stored => FAILED.
CloseSeekFail(t) ==
    /\ tUp[t] /\ tStep[t] = "closeSeek"
    /\ stored' = [stored EXCEPT ![t] = "fatal"]
    /\ CloseFinish(t, pos[t])
    /\ UNCHANGED <<srcVars, tUp, tVariant, pendAdd, wakeup, coordRef, workerVars,
                   coordVars, cgVars, ctlVars, catVars>>

-----------------------------------------------------------------------------
(***************************************************************************)
(* onPartitionsAssigned -> IcebergSinkTask.open -> CommitterImpl.open      *)
(* (WorkerSinkTask.java:712-756; KC/IcebergSinkTask.java:57-60;            *)
(* CI/CommitterImpl.java:140-151).  Positions of added partitions come     *)
(* from the committed offsets (requireStable, E2).  Election only from     *)
(* the ADDED partitions and never retried (F9).                           *)
(***************************************************************************)
FreeIds == {k \in CoordIds : cPc[k] = "idle"}

\* startCoordinator (CI/CommitterImpl.java:223-231): new Coordinator ->
\* Channel ctor -> createProducer(coordinatorTransactionalId).initTransactions()
\* bumps the epoch and fences every older coordinator (CI/KafkaClientFactory
\* .java:43-51, KC/IcebergSinkConfig.java:463-465); totalPartitionCount is
\* frozen from the describe snapshot (CI/Coordinator.java:106-107).
\* The producer key (producerId) fixes the control partition (CI/Channel.java:94).
\* totalPartitionCount = sum of the described members' assignment sizes.  The
\* describe that elected this task saw a Stable group (a non-Stable classic
\* group is described without assignments, so it never elects -- F9 is the
\* FALSE branch of OpenLeaderResults), i.e. exactly the assigned partitions of
\* the current generation: fewer than SrcParts while a cooperative move is in
\* flight (owner = NoTask, F11, #18322).
TotalViews == {Cardinality(AssignedParts(owner))}

StartCoordinator(t) ==
    /\ FreeIds # {}
    /\ LET k == MinSet(FreeIds) IN
       /\ coordRef'   = [coordRef EXCEPT ![t] = k]
       /\ cPc'        = [cPc EXCEPT ![k] = "start"]
       /\ cHost'      = [cHost EXCEPT ![k] = t]
       /\ cEpoch'     = [cEpoch EXCEPT ![k] = coordEpoch + 1]
       /\ coordEpoch' = coordEpoch + 1
       /\ \E tot \in TotalViews : cTotal' = [cTotal EXCEPT ![k] = tot]
       /\ \E c \in CtlParts : cPart' = [cPart EXCEPT ![k] = c]
    /\ UNCHANGED <<cTerm, cErr, cPos, cHwm, cCkpt, cBuf, cCid, cCount, cDcParts,
                   cFails, cMode, cGrp, cgOwner, cgCommitted>>

OpenAssign(t) ==
    /\ assign'  = [assign EXCEPT ![t] = @ \cup pendAdd[t]]
    /\ pos'     = [pos EXCEPT ![t] = [p \in SrcParts |->
                     IF p \in pendAdd[t] THEN InitPos(p) ELSE pos[t][p]]]
    /\ taskGen' = [taskGen EXCEPT ![t] = gen]
    /\ tStep'   = [tStep EXCEPT ![t] = "run"]
    /\ pendAdd' = [pendAdd EXCEPT ![t] = {}]

\* Connect skips task.open for an empty set (WorkerSinkTask.java:745-747);
\* startCoordinator is a no-op if coordinatorThread != null (F8).
\* Only the current (9fffeb6) coordinator code is modeled: a task running
\* <=1.10 code ("legacy", S1) would start a <=1.10 coordinator, whose
\* TableReference has no uuid and whose commit logic is out of scope, so a
\* legacy leader starts no coordinator here (rolling upgrade with the
\* coordinator upgraded first, analysis-report F1).
Open(t) ==
    /\ tUp[t] /\ tStep[t] = "open" /\ CanAdd(t)
    /\ OpenAssign(t)
    /\ \E isLeader \in (IF pendAdd[t] = {} THEN {FALSE} ELSE OpenLeaderResults(t)) :
         IF isLeader /\ coordRef[t] = 0 /\ tVariant[t] = "v"
         THEN StartCoordinator(t)
         ELSE UNCHANGED <<coordRef, coordVars, cgVars>>
    /\ UNCHANGED <<srcVars, tUp, tVariant, closing, lostParts, stored, wakeup,
                   workerVars, ctlVars, catVars>>

\* Fault: describe fails inside open(): stored by onPartitionsAssigned's catch
\* (WorkerSinkTask.java:748-754) => FAILED at the end of poll.
OpenFail(t) ==
    /\ tUp[t] /\ tStep[t] = "open" /\ CanAdd(t) /\ pendAdd[t] # {}
    /\ OpenAssign(t)
    /\ stored' = [stored EXCEPT ![t] = "fatal"]
    /\ UNCHANGED <<srcVars, tUp, tVariant, closing, lostParts, wakeup, coordRef,
                   workerVars, coordVars, cgVars, ctlVars, catVars>>

-----------------------------------------------------------------------------
(***************************************************************************)
(* WorkerSinkTask.poll / pollConsumer / iteration (E3: WorkerSinkTask.java *)
(* :248-262,497-505).                                                      *)
(***************************************************************************)

\* F2: the stored WakeupException is rethrown after consumer.poll() RETURNED
\* records (positions already advanced); iteration() treats it as a
\* pause/resume wakeup and continues -- the polled batch is never delivered.
PollRethrowWakeup(t) ==
    /\ tUp[t] /\ tStep[t] = "run" /\ pendAdd[t] = {} /\ stored[t] = "wakeup"
    /\ \E D \in SUBSET {p \in assign[t] : pos[t][p] >= 0 /\ pos[t][p] < NumRecs} :
         pos' = [pos EXCEPT ![t] = [p \in SrcParts |->
                    IF p \in D THEN pos[t][p] + 1 ELSE pos[t][p]]]
    /\ stored' = [stored EXCEPT ![t] = "none"]
    /\ UNCHANGED <<srcVars, tUp, tVariant, assign, taskGen, tStep, closing, pendAdd,
                   lostParts, wakeup, coordRef, workerVars, coordVars, cgVars,
                   ctlVars, catVars>>

\* Any other stored exception escapes iteration(): the task is FAILED.
PollRethrowFatal(t) ==
    /\ tUp[t] /\ tStep[t] = "run" /\ pendAdd[t] = {} /\ stored[t] = "fatal"
    /\ FailTask(t)

\* A pending wakeup consumed by a plain poll (or by the join requests before
\* any callback) throws before records are returned: benign.
PollWakeup(t) ==
    /\ tUp[t] /\ wakeup[t] /\ stored[t] = "none"
    /\ \/ tStep[t] = "run" /\ pendAdd[t] = {}
       \/ tStep[t] = "closeDiscard"
    /\ wakeup' = [wakeup EXCEPT ![t] = FALSE]
    /\ UNCHANGED <<srcVars, tUp, tVariant, assign, pos, taskGen, tStep, closing,
                   pendAdd, lostParts, stored, coordRef, workerVars, coordVars,
                   cgVars, ctlVars, catVars>>

\* Environment: connector pause/resume -> WorkerSinkTask.transitionTo ->
\* consumer.wakeup() from the herder thread (WorkerSinkTask.java:204-207).
PauseResume(t) ==
    /\ tUp[t] /\ ~wakeup[t]
    /\ wakeup' = [wakeup EXCEPT ![t] = TRUE]
    /\ UNCHANGED <<srcVars, tUp, tVariant, assign, pos, taskGen, tStep, closing,
                   pendAdd, lostParts, stored, coordRef, workerVars, coordVars,
                   cgVars, ctlVars, catVars>>

\* poll() returns record <<p, pos>> and put() buffers it: IcebergSinkTask.put
\* (KC/IcebergSinkTask.java:85-90) -> CommitterImpl.save -> startWorker
\* (CI/CommitterImpl.java:189-196,214-221) -> SinkWriter.save
\* (KC/data/SinkWriter.java:75-94).  A new Worker's control consumer joins a
\* fresh group with auto.offset.reset=latest (CI/Worker.java:50-56,
\* CI/Channel.java:228-233, CI/KafkaClientFactory.java:55).
Put(t, p) ==
    /\ TaskIdle(t) /\ lostParts[t] = {} /\ ~wakeup[t]
    /\ p \in assign[t] /\ pos[t][p] >= 0 /\ pos[t][p] < NumRecs
    /\ wBuf' = [wBuf EXCEPT ![t] = @ \cup {<<p, pos[t][p]>>}]
    /\ pos'  = [pos EXCEPT ![t][p] = @ + 1]
    /\ IF worker[t]
       THEN UNCHANGED <<worker, wcPos, wcPart>>
       ELSE /\ worker' = [worker EXCEPT ![t] = TRUE]
            /\ wcPos'  = [wcPos EXCEPT ![t] = [c \in CtlParts |-> End(c)]]
            /\ \E c \in CtlParts : wcPart' = [wcPart EXCEPT ![t] = c]
    /\ UNCHANGED <<srcVars, tUp, tVariant, assign, taskGen, tStep, closing, pendAdd,
                   lostParts, stored, wakeup, coordRef, coordVars, cgVars, ctlVars,
                   catVars>>

-----------------------------------------------------------------------------
(***************************************************************************)
(* Worker answers a StartCommit inside put() -> processControlEvents ->    *)
(* Worker.process -> consumeAvailable(ZERO) (CI/CommitterImpl.java:198-212,*)
(* CI/Worker.java:63-65, CI/Channel.java:122-152).  Non-StartCommit events *)
(* are ignored (CI/Worker.java:70-72).  The answer is ONE Kafka            *)
(* transaction {DataWritten per table, DataComplete, source offsets with   *)
(* the consumer's group metadata} (CI/Worker.java:74-111, CI/Channel.java  *)
(* :82-118).  completeWrite() clears the buffer first (:74, C-5).          *)
(***************************************************************************)
CoordCheckOk(t) == IF coordRef[t] = 0 THEN TRUE ELSE cPc[coordRef[t]] # "exited"

WorkerCanAnswer(t) == TaskIdle(t) /\ worker[t] /\ CoordCheckOk(t)

NextSC(t, c) == {o \in wcPos[t][c]..(End(c) - 1) :
                   /\ Rec(c, o).type = "SC"
                   /\ \A o2 \in wcPos[t][c]..(o - 1) : Rec(c, o2).type # "SC"}

\* KIP-447: TxnOffsetCommit is validated against the group generation and
\* member id (GroupCoordinator.scala, E1); without source offsets no
\* sendOffsetsToTransaction happens (CI/Channel.java:104-107).
Kip447Ok(t) == wBuf[t] = {} \/ (taskGen[t] = gen /\ lostParts[t] = {})

\* DataWritten per writer result (KC/data/SinkWriter.java:58-69; one per table
\* for unpartitioned writers) then DataComplete(commitId, context.assignment())
\* (CI/Worker.java:79-109).
AnswerEvents(t, cid, o0) ==
    LET ts  == IF wBuf[t] = {} THEN <<>> ELSE TableSeq
        n   == Len(ts)
        dws == [j \in 1..n |-> [type |-> "DW", cid |-> cid, tbl |-> ts[j],
                                var |-> tVariant[t], rows |-> wBuf[t],
                                parts |-> {}, off |-> o0 + j - 1]]
        dc  == [type |-> "DC", cid |-> cid, tbl |-> NoTable, var |-> "none",
                rows |-> {}, parts |-> assign[t], off |-> o0 + n]
    IN dws \o <<dc>>

WorkerAnswer(t, c) ==
    /\ WorkerCanAnswer(t)
    /\ wcPos[t][c] <= End(c)
    /\ \E o \in NextSC(t, c) :
         /\ Kip447Ok(t)
         /\ ctl' = [ctl EXCEPT ![wcPart[t]] =
                      @ \o AnswerEvents(t, Rec(c, o).cid, End(wcPart[t]))]
         /\ srcCommitted' = [p \in SrcParts |->
                               IF p \in BufParts(t) THEN BufOff(t, p) ELSE srcCommitted[p]]
         /\ wBuf'  = [wBuf EXCEPT ![t] = {}]
         /\ wcPos' = [wcPos EXCEPT ![t][c] = o + 1]
    /\ UNCHANGED <<owner, gen, taskVars, worker, wcPart, coordVars, cgVars, ctlBase,
                   nextCid, catVars>>

\* KIP-447 rejects the stale generation: commitTransaction fails, abort,
\* rethrow out of put() (CI/Channel.java:109-116) => task FAILED (F15).
WorkerAnswerRejected(t, c) ==
    /\ WorkerCanAnswer(t)
    /\ wcPos[t][c] <= End(c)
    /\ NextSC(t, c) # {}
    /\ ~Kip447Ok(t)
    /\ FailTask(t)

\* Control topic recreated: OffsetOutOfRange -> latest (only reachable with
\* RecreateControlTopic).
WorkerOutOfRange(t, c) ==
    /\ tUp[t] /\ worker[t] /\ wcPos[t][c] > End(c)
    /\ wcPos' = [wcPos EXCEPT ![t][c] = End(c)]
    /\ UNCHANGED <<srcVars, taskVars, worker, wBuf, wcPart, coordVars, cgVars,
                   ctlVars, catVars>>

\* processControlEvents (CI/CommitterImpl.java:198-208): a coordinator thread
\* that exited because it was fenced is cleared WITHOUT re-election (F8);
\* any other exit throws NotRunningException => task FAILED.
ClearFencedCoordinator(t) ==
    /\ TaskIdle(t) /\ coordRef[t] # 0
    /\ cPc[coordRef[t]] = "exited" /\ cErr[coordRef[t]] = "fenced"
    /\ coordRef' = [coordRef EXCEPT ![t] = 0]
    /\ UNCHANGED <<srcVars, tUp, tVariant, assign, pos, taskGen, tStep, closing,
                   pendAdd, lostParts, stored, wakeup, workerVars, coordVars, cgVars,
                   ctlVars, catVars>>

CoordinatorNotRunning(t) ==
    /\ TaskIdle(t) /\ coordRef[t] # 0
    /\ cPc[coordRef[t]] = "exited" /\ cErr[coordRef[t]] # "fenced"
    /\ FailTask(t)

-----------------------------------------------------------------------------
(***************************************************************************)
(* Coordinator instance k: CoordinatorThread.run loop (CI/CoordinatorThread *)
(* .java:39-64) around Coordinator.process (CI/Coordinator.java:126-141).   *)
(* pc "loop" = loop top / end of process(); "consume" = inside             *)
(* consumeAvailable; "commit" = inside commit()/doCommit.                  *)
(***************************************************************************)

\* Coordinator.commit retired paths reset the instance (it never runs again).
RetireCoords(D) ==
    /\ cBuf'     = [k \in CoordIds |-> IF k \in D THEN {} ELSE cBuf[k]]
    /\ cCid'     = [k \in CoordIds |-> IF k \in D THEN 0 ELSE cCid[k]]
    /\ cCount'   = [k \in CoordIds |-> IF k \in D THEN 0 ELSE cCount[k]]
    /\ cDcParts' = [k \in CoordIds |-> IF k \in D THEN {} ELSE cDcParts[k]]
    /\ cMode'    = [k \in CoordIds |-> IF k \in D THEN "none" ELSE cMode[k]]
    /\ cGrp'     = [k \in CoordIds |-> IF k \in D THEN [g \in Groups |-> IdleGrp]
                                       ELSE cGrp[k]]
    /\ cHwm'     = [k \in CoordIds |-> IF k \in D THEN NoFloor ELSE cHwm[k]]
    /\ cCkpt'    = [k \in CoordIds |-> IF k \in D THEN NoFloor ELSE cCkpt[k]]
    /\ cFails'   = [k \in CoordIds |-> IF k \in D THEN 0 ELSE cFails[k]]

\* Thread leaves the loop and runs coordinator.stop() -> Channel.stop closes
\* the -coord consumer, i.e. leaves the group (CI/CoordinatorThread.java:59-63,
\* CI/Channel.java:235-240); err is CoordinatorThread.error's class.
CoordExitWith(k, err) ==
    /\ cPc'  = [cPc EXCEPT ![k] = "exited"]
    /\ cErr' = [cErr EXCEPT ![k] = err]
    /\ RetireCoords({k})
    /\ CgReassign(CgMembers \ {k})

exitUnchanged == <<cHost, cEpoch, cTerm, cTotal, cPart, cgCommitted, coordEpoch>>

\* coordinator.start() = Channel.start: subscribe to the control topic and the
\* initial consumeAvailable (CI/CoordinatorThread.java:41-47, CI/Channel.java
\* :228-233).  Joining the -coord group triggers an (eager) reassignment.
CoordStart(k) ==
    /\ cPc[k] = "start"
    /\ cPc' = [cPc EXCEPT ![k] = "consume"]
    /\ CgReassign(CgMembers \cup {k})
    /\ UNCHANGED <<srcVars, taskVars, workerVars, coordLocal, cgCommitted, coordEpoch,
                   ctlVars, catVars>>

\* process(): isCommitIntervalReached (no commit in progress, interval
\* elapsed -- abstract time) -> startNewCommit -> send(StartCommit) in its own
\* transaction (CI/Coordinator.java:127-134, CI/CommitState.java:81-93,
\* CI/Channel.java:78-118).
CoordStartCommit(k) ==
    /\ cPc[k] = "loop" /\ ~cTerm[k] /\ cCid[k] = 0 /\ ~Fenced(k)
    /\ ctl'     = [ctl EXCEPT ![cPart[k]] = Append(@, SCEvent(nextCid, End(cPart[k])))]
    /\ cCid'    = [cCid EXCEPT ![k] = nextCid]
    /\ nextCid' = nextCid + 1
    /\ cPc'     = [cPc EXCEPT ![k] = "consume"]
    /\ UNCHANGED <<srcVars, taskVars, workerVars, cHost, cEpoch, cTerm, cErr, cPos,
                   cHwm, cCkpt, cBuf, cCount, cDcParts, cTotal, cFails, cMode, cGrp,
                   cPart, cgVars, ctlBase, catVars>>

\* The StartCommit send of a fenced instance throws ProducerFencedException;
\* the thread records it and exits (CI/CoordinatorThread.java:50-56).
CoordStartCommitFenced(k) ==
    /\ cPc[k] = "loop" /\ ~cTerm[k] /\ cCid[k] = 0 /\ Fenced(k)
    /\ CoordExitWith(k, "fenced")
    /\ UNCHANGED <<srcVars, taskVars, workerVars, exitUnchanged, ctlVars, catVars>>

\* Interval not reached or a commit already in progress: go to consumeAvailable.
CoordSkipStartCommit(k) ==
    /\ cPc[k] = "loop" /\ ~cTerm[k]
    /\ cPc' = [cPc EXCEPT ![k] = "consume"]
    /\ UNCHANGED <<srcVars, taskVars, workerVars, cHost, cEpoch, cTerm, cErr, cPos,
                   cHwm, cCkpt, cBuf, cCid, cCount, cDcParts, cTotal, cFails, cMode,
                   cGrp, cPart, cgVars, ctlVars, catVars>>

\* while (!terminated) -- terminate() was called (CI/CoordinatorThread.java:49).
CoordExit(k) ==
    /\ cPc[k] = "loop" /\ cTerm[k]
    /\ CoordExitWith(k, "none")
    /\ UNCHANGED <<srcVars, taskVars, workerVars, exitUnchanged, ctlVars, catVars>>

\* Coordinator.commit(partialCommit) -> doCommit: commitMap = tableCommitMap()
\* (CI/Coordinator.java:159-161,203-213; CI/CommitState.java:139-144).
\* After terminate(), exec is shut down (CI/Coordinator.java:437): the first
\* submit throws RejectedExecutionException, so no table task runs.
CommitBegin(k, mode, buf) ==
    LET G == GroupsIn(buf) IN
    /\ cPc'   = [cPc EXCEPT ![k] = "commit"]
    /\ cMode' = [cMode EXCEPT ![k] = mode]
    /\ cGrp'  = [cGrp EXCEPT ![k] = [g \in Groups |->
                   IF g \in G
                   THEN [IdleGrp EXCEPT !.st = IF cTerm[k] THEN "skipped" ELSE "todo"]
                   ELSE IdleGrp]]
    /\ cErr'  = [cErr EXCEPT ![k] = IF cTerm[k] /\ G # {} THEN "other" ELSE "none"]

\* One record of consumeAvailable (CI/Channel.java:122-152): skip offsets
\* below the in-memory high-water mark (:126-135), else advance it (:139) and
\* receive (CI/Coordinator.java:143-157): DataWritten -> addResponse (any
\* commit id, CI/CommitState.java:51-59); DataComplete -> addReady counts
\* assignments only for the current commit id (:61-71), then isCommitReady
\* against the frozen total (:117-137) -> commit(false) synchronously.
\* No terminated check here (F6).
CoordConsume(k, c) ==
    /\ cPc[k] = "consume" /\ cgOwner[c] = k
    /\ cPos[k][c] >= 0 /\ cPos[k][c] < End(c)
    /\ LET o     == cPos[k][c]
           e     == Rec(c, o)
           id    == <<c, ctlBase[c] + o + 1>>
           skip  == cHwm[k][c] # -1 /\ o < cHwm[k][c]
           isCur == cCid[k] # 0 /\ e.cid = cCid[k]
           cnt   == IF ~skip /\ e.type = "DC" /\ isCur
                    THEN cCount[k] + Cardinality(e.parts) ELSE cCount[k]
           ready == ~skip /\ e.type = "DC" /\ cCid[k] # 0 /\ cnt >= cTotal[k]
           buf   == IF ~skip /\ e.type = "DW" THEN cBuf[k] \cup {id} ELSE cBuf[k]
       IN
       /\ cPos'     = [cPos EXCEPT ![k][c] = o + 1]
       /\ cHwm'     = IF skip THEN cHwm ELSE [cHwm EXCEPT ![k][c] = Max2(@, o + 1)]
       /\ cBuf'     = [cBuf EXCEPT ![k] = buf]
       /\ cCount'   = [cCount EXCEPT ![k] = cnt]
       /\ cDcParts' = IF ~skip /\ e.type = "DC" /\ isCur
                      THEN [cDcParts EXCEPT ![k] = @ \cup e.parts] ELSE cDcParts
       /\ IF ready THEN CommitBegin(k, "full", buf)
                   ELSE UNCHANGED <<cPc, cMode, cGrp, cErr>>
    /\ UNCHANGED <<srcVars, taskVars, workerVars, cHost, cEpoch, cTerm, cCkpt, cCid,
                   cTotal, cFails, cPart, cgVars, ctlVars, catVars>>

\* Control topic recreated: OffsetOutOfRange -> auto.offset.reset.
CoordOutOfRange(k, c) ==
    /\ cPc[k] \in {"loop", "consume", "commit"} /\ cgOwner[c] = k
    /\ cPos[k][c] > End(c)
    /\ cPos' = [cPos EXCEPT ![k][c] = IF CoordReset = "latest" THEN End(c) ELSE 0]
    /\ UNCHANGED <<srcVars, taskVars, workerVars, coordLocal, cPc, cgVars, ctlVars,
                   catVars>>

\* poll(1s) returned nothing on every owned partition: consumeAvailable ends.
CoordPollEmpty(k) ==
    /\ cPc[k] = "consume"
    /\ \A c \in CtlParts : cgOwner[c] = k => cPos[k][c] = End(c)
    /\ cPc' = [cPc EXCEPT ![k] = "loop"]
    /\ UNCHANGED <<srcVars, taskVars, workerVars, coordLocal, cPos, cgVars, ctlVars,
                   catVars>>

\* isCommitTimedOut -> commit(true): partial commit with whatever DataWritten
\* are buffered (CI/Coordinator.java:138-140, CI/CommitState.java:105-115).
CoordTimeout(k) ==
    /\ cPc[k] = "loop" /\ cCid[k] # 0
    /\ CommitBegin(k, "partial", cBuf[k])
    /\ UNCHANGED <<srcVars, taskVars, workerVars, cHost, cEpoch, cTerm, cPos, cHwm,
                   cCkpt, cBuf, cCid, cCount, cDcParts, cTotal, cFails, cPart, cgVars,
                   ctlVars, catVars>>

-----------------------------------------------------------------------------
(***************************************************************************)
(* Per-table commit task commitToTable(ref, envelopes, controlTopicOffsets)*)
(* (CI/Coordinator.java:242-359), split into the read step (a) and the CAS *)
(* step (c) of analysis-report §1.4: their TOCTOU gap is closed only by    *)
(* the SnapshotAncestryValidator.                                          *)
(***************************************************************************)

\* (a) loadTable (:247-254), [uuid check :256-263 always passes here: no
\* table replacement is modeled, null uuid passes], committedOffsets =
\* lastCommittedOffsetsForTable (:270, :388-415), merged = max(committed,
\* controlTopicOffsets) (:271-275), filter offset >= stored floor (:277-285),
\* terminated check (:303-306), nothing to commit (:308-310).
\* Task start honours stopOnFailure (CORE/util/Tasks.java:314); with one
\* commit thread queued tasks run FIFO and are drained by shutdownNow (F5).
CoordReadFloor(k, g) ==
    /\ cPc[k] = "commit" /\ cGrp[k][g].st = "todo"
    /\ ~GroupFailed(k)
    /\ CommitThreads = 1 => (~SomeReading(k) /\ ~cTerm[k])
    /\ LET T      == g[1]
           exp    == ReadFloor(T)
           merged == [c \in CtlParts |-> Max2(exp[c], cHwm[k][c])]
           envs   == {d \in cBuf[k] : GroupOf(d) = g}
           kept   == {d \in envs : exp[d[1]] = -1 \/ DOff(d) >= exp[d[1]]}
       IN
       /\ cGrp' = [cGrp EXCEPT ![k][g] =
                    IF cTerm[k] THEN [IdleGrp EXCEPT !.st = "failed"]
                    ELSE IF kept = {} THEN [IdleGrp EXCEPT !.st = "done"]
                    ELSE [st |-> "read", exp |-> exp, merged |-> merged, dws |-> kept]]
       /\ cErr' = IF cTerm[k] THEN [cErr EXCEPT ![k] = FirstErr(k, "other")] ELSE cErr
    /\ UNCHANGED <<srcVars, taskVars, workerVars, cPc, cHost, cEpoch, cTerm, cPos,
                   cHwm, cCkpt, cBuf, cCid, cCount, cDcParts, cTotal, cFails, cMode,
                   cPart, cgVars, ctlVars, catVars>>

\* The new snapshot: parent = SnapshotUtil.latestSnapshot(base, branch)
\* (CORE/SnapshotProducer.java:297-304), summary key = merged floor
\* (CI/Coordinator.java:318), files = kept DataWritten (:287-293,324);
\* setBranchSnapshot(newSnapshot, targetBranch) (CORE/SnapshotProducer.java:517)
\* creates the branch if it is missing.
NewConnSnap(T, r) == [parent |-> ValidatorHead(T), hasKey |-> TRUE, floor |-> r.merged,
                      dws |-> r.dws, live |-> TRUE, writer |-> "conn"]

Land(k, g) ==
    LET T == g[1] IN
    /\ snaps' = [snaps EXCEPT ![T] = Append(@, NewConnSnap(T, cGrp[k][g]))]
    /\ heads' = [heads EXCEPT ![T] = IF UseBranch THEN [@ EXCEPT !.b = Len(snaps[T]) + 1]
                                     ELSE [@ EXCEPT !.main = Len(snaps[T]) + 1]]

\* (c) appendOp.commit(): validator re-run on the refreshed base on every
\* attempt (CORE/SnapshotProducer.java:377-389,491-536; E5), CAS succeeds;
\* then send(CommitToTable) (CI/Coordinator.java:343-349) -- a Kafka send
\* that fails with ProducerFencedException for a fenced instance AFTER the
\* table commit landed (table commits are not Kafka-fenced, CI/Channel.java
\* :165-169).
CoordCasCommit(k, g) ==
    /\ cPc[k] = "commit" /\ cGrp[k][g].st = "read"
    /\ ValidatorFloor(g[1]) = cGrp[k][g].exp
    /\ Land(k, g)
    /\ IF Fenced(k)
       THEN /\ cGrp' = [cGrp EXCEPT ![k][g].st = "failed"]
            /\ cErr' = [cErr EXCEPT ![k] = FirstErr(k, "fenced")]
       ELSE /\ cGrp' = [cGrp EXCEPT ![k][g].st = "done"]
            /\ UNCHANGED cErr
    /\ UNCHANGED <<srcVars, taskVars, workerVars, cPc, cHost, cEpoch, cTerm, cPos,
                   cHwm, cCkpt, cBuf, cCid, cCount, cDcParts, cTotal, cFails, cMode,
                   cPart, cgVars, ctlVars>>

\* Validator: expectedOffsets.equals(lastCommittedOffsets(ancestry))
\* (CI/Coordinator.java:367-372) fails -> ValidationException (not retryable).
CoordCasValidationFail(k, g) ==
    /\ cPc[k] = "commit" /\ cGrp[k][g].st = "read"
    /\ ValidatorFloor(g[1]) # cGrp[k][g].exp
    /\ cGrp' = [cGrp EXCEPT ![k][g].st = "failed"]
    /\ cErr' = [cErr EXCEPT ![k] = FirstErr(k, "other")]
    /\ UNCHANGED <<srcVars, taskVars, workerVars, cPc, cHost, cEpoch, cTerm, cPos,
                   cHwm, cCkpt, cBuf, cCid, cCount, cDcParts, cTotal, cFails, cMode,
                   cPart, cgVars, ctlVars, catVars>>

\* Fault: catalog outcome other than success (brief §3.1):
\*  landedUnknown  CommitStateUnknownException although the CAS landed
\*                 (also: CommitFailed reported, retry fails validation, E5)
\*  commitFailed   Iceberg CommitFailedException, retries exhausted, not landed
\*  unknownFailed  CommitStateUnknownException / other error, not landed
\* (CORE/SnapshotProducer.java:538-546; CI/Coordinator.java:176-179).
CoordCasFault(k, g) ==
    /\ cPc[k] = "commit" /\ cGrp[k][g].st = "read"
    /\ ValidatorFloor(g[1]) = cGrp[k][g].exp
    /\ \E out \in {"landedUnknown", "commitFailed", "unknownFailed"} :
         /\ IF out = "landedUnknown" THEN Land(k, g) ELSE UNCHANGED catVars
         /\ cGrp' = [cGrp EXCEPT ![k][g].st = "failed"]
         /\ cErr' = [cErr EXCEPT ![k] =
                       FirstErr(k, IF out = "commitFailed" THEN "commitFailed" ELSE "other")]
    /\ UNCHANGED <<srcVars, taskVars, workerVars, cPc, cHost, cEpoch, cTerm, cPos,
                   cHwm, cCkpt, cBuf, cCid, cCount, cDcParts, cTotal, cFails, cMode,
                   cPart, cgVars, ctlVars>>

-----------------------------------------------------------------------------
(***************************************************************************)
(* End of doCommit / commit() (CI/Coordinator.java:159-231).  Tasks.run    *)
(* waits for every task and rethrows one failure (throwFailureWhenFinished,*)
(* CORE/util/Tasks.java:361,396-400); stopOnFailure skips tasks that had   *)
(* not started.                                                            *)
(***************************************************************************)
CommitDone(k) ==
    /\ cPc[k] = "commit" /\ ~SomeReading(k)
    /\ cErr[k] # "none" \/ \A g \in Groups : cGrp[k][g].st # "todo"

\* CommitState.endCurrentCommit in the finally block (CI/Coordinator.java
\* :198-200, CI/CommitState.java:95-99).
EndCommit(k) ==
    /\ cCid'     = [cCid EXCEPT ![k] = 0]
    /\ cCount'   = [cCount EXCEPT ![k] = 0]
    /\ cDcParts' = [cDcParts EXCEPT ![k] = {}]
    /\ cMode'    = [cMode EXCEPT ![k] = "none"]
    /\ cGrp'     = [cGrp EXCEPT ![k] = [g \in Groups |-> IdleGrp]]
    /\ cErr'     = [cErr EXCEPT ![k] = "none"]

\* Partial commit: every exception swallowed, buffer kept (:165-174, C-2).
CoordCommitSwallow(k) ==
    /\ CommitDone(k) /\ cErr[k] # "none" /\ cMode[k] = "partial"
    /\ EndCommit(k)
    /\ cPc' = [cPc EXCEPT ![k] = "loop"]
    /\ UNCHANGED <<srcVars, taskVars, workerVars, cHost, cEpoch, cTerm, cPos, cHwm,
                   cCkpt, cBuf, cTotal, cFails, cPart, cgVars, ctlVars, catVars>>

\* Full commit, Iceberg CommitFailedException within the budget: warn and
\* retry in a later cycle, buffer kept (:176-197).
CoordCommitRetry(k) ==
    /\ CommitDone(k) /\ cErr[k] = "commitFailed" /\ cMode[k] = "full"
    /\ cFails[k] + 1 < MaxConsecFailures
    /\ EndCommit(k)
    /\ cFails' = [cFails EXCEPT ![k] = @ + 1]
    /\ cPc'    = [cPc EXCEPT ![k] = "consume"]
    /\ UNCHANGED <<srcVars, taskVars, workerVars, cHost, cEpoch, cTerm, cPos, cHwm,
                   cCkpt, cBuf, cTotal, cPart, cgVars, ctlVars, catVars>>

\* Full commit, anything else (or budget exhausted): rethrown out of receive /
\* consumeAvailable / process; the thread exits (:176-190).
CoordCommitThrow(k) ==
    /\ CommitDone(k) /\ cErr[k] # "none" /\ cMode[k] = "full"
    /\ cErr[k] # "commitFailed" \/ cFails[k] + 1 >= MaxConsecFailures
    /\ CoordExitWith(k, IF cErr[k] = "fenced" THEN "fenced" ELSE "other")
    /\ UNCHANGED <<srcVars, taskVars, workerVars, exitUnchanged, ctlVars, catVars>>

\* All table tasks succeeded: commitConsumerOffsets (CI/Coordinator.java:216,
\* CI/Channel.java:171-213: only partitions advancing past this instance's
\* last commit; transactional, consumer.groupMetadata()), clearResponses
\* (:217); CommitComplete (:219-223) is abstracted.
CoordCheckpoint(k) ==
    /\ CommitDone(k) /\ cErr[k] = "none" /\ ~Fenced(k)
    /\ LET TC == ToCommit(k) IN
       /\ cgCommitted' = [c \in CtlParts |-> IF c \in TC THEN cHwm[k][c] ELSE cgCommitted[c]]
       /\ cCkpt' = [cCkpt EXCEPT ![k] = [c \in CtlParts |->
                      IF c \in TC THEN cHwm[k][c] ELSE cCkpt[k][c]]]
    /\ cBuf' = [cBuf EXCEPT ![k] = {}]
    /\ EndCommit(k)
    /\ cFails' = IF cMode[k] = "full" THEN [cFails EXCEPT ![k] = 0] ELSE cFails
    /\ cPc'    = [cPc EXCEPT ![k] = IF cMode[k] = "full" THEN "consume" ELSE "loop"]
    /\ UNCHANGED <<srcVars, taskVars, workerVars, cHost, cEpoch, cTerm, cPos, cHwm,
                   cTotal, cPart, cgOwner, coordEpoch, ctlVars, catVars>>

\* Fenced instance: the offsets transaction (or, if nothing advanced, the
\* CommitComplete send after clearResponses) throws ProducerFencedException
\* (CI/Channel.java:199-207).  Partial: swallowed (R8); full: thread exits.
CoordCheckpointFencedPartial(k) ==
    /\ CommitDone(k) /\ cErr[k] = "none" /\ Fenced(k) /\ cMode[k] = "partial"
    /\ cBuf' = IF ToCommit(k) = {} THEN [cBuf EXCEPT ![k] = {}] ELSE cBuf
    /\ EndCommit(k)
    /\ cPc' = [cPc EXCEPT ![k] = "loop"]
    /\ UNCHANGED <<srcVars, taskVars, workerVars, cHost, cEpoch, cTerm, cPos, cHwm,
                   cCkpt, cTotal, cFails, cPart, cgVars, ctlVars, catVars>>

CoordCheckpointFencedFull(k) ==
    /\ CommitDone(k) /\ cErr[k] = "none" /\ Fenced(k) /\ cMode[k] = "full"
    /\ CoordExitWith(k, "fenced")
    /\ UNCHANGED <<srcVars, taskVars, workerVars, exitUnchanged, ctlVars, catVars>>

\* Fault: the offsets transaction fails otherwise (Kafka CommitFailedException
\* of a stale -coord member after a long commit -- F7 --, timeouts).
CoordCheckpointFailPartial(k) ==
    /\ CommitDone(k) /\ cErr[k] = "none" /\ ~Fenced(k) /\ cMode[k] = "partial"
    /\ ToCommit(k) # {}
    /\ EndCommit(k)
    /\ cPc' = [cPc EXCEPT ![k] = "loop"]
    /\ UNCHANGED <<srcVars, taskVars, workerVars, cHost, cEpoch, cTerm, cPos, cHwm,
                   cCkpt, cBuf, cTotal, cFails, cPart, cgVars, ctlVars, catVars>>

CoordCheckpointFailFull(k) ==
    /\ CommitDone(k) /\ cErr[k] = "none" /\ ~Fenced(k) /\ cMode[k] = "full"
    /\ ToCommit(k) # {}
    /\ CoordExitWith(k, "other")
    /\ UNCHANGED <<srcVars, taskVars, workerVars, exitUnchanged, ctlVars, catVars>>

-----------------------------------------------------------------------------
(***************************************************************************)
(* Process crash / restart (brief §3.1): a crashed process sends nothing   *)
(* further; its coordinator thread dies with it (no zombie in this case).  *)
(***************************************************************************)
Crash(t) ==
    /\ tUp[t]
    /\ LET D == {k \in CoordIds : cHost[k] = t /\ cPc[k] \notin {"idle", "exited", "dead"}}
       IN
       /\ ResetTaskLocal(t)
       /\ cPc' = [k \in CoordIds |-> IF k \in D THEN "dead" ELSE cPc[k]]
       /\ RetireCoords(D)
       /\ IF D \cap CgMembers = {} THEN UNCHANGED <<cgOwner, cPos>>
          ELSE CgReassign(CgMembers \ D)
    /\ UNCHANGED <<srcCommitted, gen, cHost, cEpoch, cTerm, cErr, cTotal, cPart,
                   cgCommitted, coordEpoch, ctlVars, catVars>>

\* Operator / runtime restarts a FAILED or crashed task (new process: fresh
\* in-memory state).  A rolling upgrade may change the worker version (S1).
Restart(t) ==
    /\ ~tUp[t]
    /\ tUp' = [tUp EXCEPT ![t] = TRUE]
    /\ \E v \in RefVariants : tVariant' = [tVariant EXCEPT ![t] = v]
    /\ taskGen' = [taskGen EXCEPT ![t] = gen]
    /\ UNCHANGED <<srcVars, assign, pos, tStep, closing, pendAdd, lostParts, stored,
                   wakeup, coordRef, workerVars, coordVars, cgVars, ctlVars, catVars>>

-----------------------------------------------------------------------------
(***************************************************************************)
(* Environment: -coord group churn, catalog maintenance, offset loss,      *)
(* control-topic recreation (KNOWN #15293, default off in MC).             *)
(***************************************************************************)

\* Membership churn of connect-<name>-coord (session/poll timeouts, F7):
\* eager reassignment among the open -coord consumers.
CgRebalance ==
    /\ CgMembers # {}
    /\ CgReassign(CgMembers)
    /\ UNCHANGED <<srcVars, taskVars, workerVars, coordLocal, cPc, cgCommitted,
                   coordEpoch, ctlVars, catVars>>

\* Another writer (compaction, another job) commits to main; custom summary
\* properties are not carried forward (CORE/SnapshotProducer.java:394-473).
OtherWriterCommit(T) ==
    /\ snaps' = [snaps EXCEPT ![T] = Append(@, [parent |-> heads[T].main,
                    hasKey |-> FALSE, floor |-> NoFloor, dws |-> {}, live |-> TRUE,
                    writer |-> "other"])]
    /\ heads' = [heads EXCEPT ![T].main = Len(snaps[T]) + 1]
    /\ UNCHANGED <<srcVars, taskVars, workerVars, coordVars, cgVars, ctlVars>>

\* expire_snapshots: any snapshot that is not a ref head (min-snapshots-to-keep
\* = 1).  Data files stay referenced by the current state (append-only).
ExpireSnapshot(T, i) ==
    /\ i \in 1..Len(snaps[T]) /\ snaps[T][i].live
    /\ i # heads[T].main /\ i # heads[T].b
    /\ snaps' = [snaps EXCEPT ![T][i].live = FALSE]
    /\ UNCHANGED <<srcVars, taskVars, workerVars, coordVars, cgVars, ctlVars, heads>>

\* WAP: fast-forward main to the commit branch, then drop the branch (F4).
FastForwardDropBranch(T) ==
    /\ UseBranch /\ heads[T].b # 0
    /\ heads' = [heads EXCEPT ![T] = [main |-> heads[T].b, b |-> 0]]
    /\ UNCHANGED <<srcVars, taskVars, workerVars, coordVars, cgVars, ctlVars, snaps>>

\* -coord committed offsets lost (offsets.retention of an empty group,
\* operator reset): the next owner starts at auto.offset.reset (#18006, Q5).
LoseCoordOffsets(c) ==
    /\ cgCommitted[c] # -1
    /\ cgCommitted' = [cgCommitted EXCEPT ![c] = -1]
    /\ UNCHANGED <<srcVars, taskVars, workerVars, coordVars, cgOwner, coordEpoch,
                   ctlVars, catVars>>

\* Control topic deleted and recreated: offsets restart at 0, group offsets of
\* the deleted partitions are removed (KNOWN #15293, F12).
RecreateControlTopic ==
    /\ \E c \in CtlParts : End(c) > 0
    /\ ctlBase'     = [c \in CtlParts |-> Len(ctl[c])]
    /\ cgCommitted' = [c \in CtlParts |-> -1]
    /\ UNCHANGED <<srcVars, taskVars, workerVars, coordVars, cgOwner, coordEpoch,
                   ctl, nextCid, catVars>>

-----------------------------------------------------------------------------
(***************************************************************************)
(* Next-state relation                                                     *)
(***************************************************************************)
TaskNext(t) ==
    \/ \E p \in SrcParts : Put(t, p)
    \/ \E c \in CtlParts : WorkerAnswer(t, c) \/ WorkerAnswerRejected(t, c)
                           \/ WorkerOutOfRange(t, c)
    \/ PollLost(t) \/ PollRethrowWakeup(t) \/ PollRethrowFatal(t) \/ PollWakeup(t)
    \/ CloseDiscard(t) \/ CloseElect(t) \/ CloseSeek(t) \/ CloseSeekWakeup(t)
    \/ Open(t) \/ Rejoin(t)
    \/ ClearFencedCoordinator(t) \/ CoordinatorNotRunning(t)

CoordNext(k) ==
    \/ CoordStart(k) \/ CoordStartCommit(k) \/ CoordStartCommitFenced(k)
    \/ CoordSkipStartCommit(k) \/ CoordExit(k) \/ CoordPollEmpty(k) \/ CoordTimeout(k)
    \/ \E c \in CtlParts : CoordConsume(k, c) \/ CoordOutOfRange(k, c)
    \/ \E g \in Groups : CoordReadFloor(k, g) \/ CoordCasCommit(k, g)
                         \/ CoordCasValidationFail(k, g)
    \/ CoordCommitSwallow(k) \/ CoordCommitRetry(k) \/ CoordCommitThrow(k)
    \/ CoordCheckpoint(k) \/ CoordCheckpointFencedPartial(k)
    \/ CoordCheckpointFencedFull(k)

FaultNext ==
    \/ Rebalance
    \/ \E x \in Tasks : RebalanceLose(x)
    \/ \E t \in Tasks : \/ Crash(t) \/ Restart(t) \/ PauseResume(t)
                        \/ CloseDiscardFail(t) \/ CloseElectFail(t)
                        \/ CloseSeekFail(t) \/ OpenFail(t)
    \/ \E k \in CoordIds : \/ \E g \in Groups : CoordCasFault(k, g)
                           \/ CoordCheckpointFailPartial(k)
                           \/ CoordCheckpointFailFull(k)

EnvNext ==
    \/ CgRebalance
    \/ \E T \in Tables : \/ OtherWriterCommit(T) \/ FastForwardDropBranch(T)
                         \/ \E i \in 1..Len(snaps[T]) : ExpireSnapshot(T, i)
    \/ \E c \in CtlParts : LoseCoordOffsets(c)
    \/ RecreateControlTopic

Next ==
    \/ \E t \in Tasks : TaskNext(t)
    \/ \E k \in CoordIds : CoordNext(k)
    \/ FaultNext
    \/ EnvNext

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(***************************************************************************)
(* Ghost views (derived from state; no extra variables)                    *)
(***************************************************************************)
RowsOf(d) == DWAt(d).rows

\* how many (snapshot, DataWritten) pairs put record r into table T
RowCount(T, r) ==
    Cardinality(UNION {{<<i, d>> : d \in {x \in snaps[T][i].dws : r \in RowsOf(x)}} :
                       i \in 1..Len(snaps[T])})

RowInTable(T, r) == \E i \in 1..Len(snaps[T]) : \E d \in snaps[T][i].dws : r \in RowsOf(d)

\* d would be filtered by the floor the coordinator reads now (CI/Coordinator.java:277-285)
BelowFloor(d) == LET f == ReadFloor(DWAt(d).tbl) IN f[d[1]] # -1 /\ DOff(d) < f[d[1]]

ReadFloorAt(T, c) == ReadFloor(T)[c]

\* A coordinator that will still run commit cycles that can checkpoint.
LiveCoord(k) == cPc[k] \in {"start", "loop", "consume", "commit"} /\ ~cTerm[k] /\ ~Fenced(k)

\* the owner of d's control partition has not consumed d yet and will not skip it
WillConsume(k, d) ==
    LET c == d[1] IN
    /\ cgOwner[c] = k /\ cPos[k][c] # -1 /\ cPos[k][c] <= DOff(d)
    /\ (cHwm[k][c] = -1 \/ cHwm[k][c] <= DOff(d))

\* a coordinator (re)assigned d's partition later starts at or before d
FutureStart(d) ==
    LET c == d[1] IN
    IF cgCommitted[c] # -1 THEN cgCommitted[c] <= DOff(d) ELSE CoordReset = "earliest"

Reachable(d) ==
    \/ \E k \in CoordIds : LiveCoord(k) /\ (d \in cBuf[k] \/ WillConsume(k, d))
    \/ FutureStart(d)

-----------------------------------------------------------------------------
(***************************************************************************)
(* Safety invariants (modeling-brief §5)                                   *)
(***************************************************************************)

\* Q1-Q4, S2, S4: each source record at most once in each table it is routed to.
NoDuplicateRows == \A T \in Tables : \A r \in Records : RowCount(T, r) <= 1

\* S2, S4: no DataWritten contributes files to the same table twice.
AtMostOneCommitPerDataWritten ==
    \A T \in Tables : \A i, j \in 1..Len(snaps[T]) :
        i # j => snaps[T][i].dws \cap snaps[T][j].dws = {}

\* Q1, S3: every record below the Connect committed offset is in a visible
\* DataWritten for each routed table, or already in that table.
SourceCommittedCovered ==
    \A p \in SrcParts : \A o \in 0..(srcCommitted[p] - 1) : \A T \in Tables :
        \/ \E d \in VisibleDWs : DWAt(d).tbl = T /\ <<p, o>> \in RowsOf(d)
        \/ RowInTable(T, <<p, o>>)

\* S1, Q3: every visible DataWritten for T below T's readable floor is in T.
FloorSound == \A d \in VisibleDWs : BelowFloor(d) => InTable(DWAt(d).tbl, d)

\* S1, S4, Q5: every DataWritten below the -coord checkpoint is in its table
\* (no intentional skips are modeled: NoSuchTable / UUID mismatch, C-1).
CheckpointSound ==
    \A d \in VisibleDWs :
        (cgCommitted[d[1]] # -1 /\ DOff(d) < cgCommitted[d[1]]) => InTable(DWAt(d).tbl, d)

\* S1, S2, Q5: every visible DataWritten not yet in its table is above its
\* table's floor and can still be consumed or is buffered by a live,
\* unfenced, non-terminated coordinator.
NoStrandedDataWritten ==
    \A d \in VisibleDWs : ~InTable(DWAt(d).tbl, d) => (~BelowFloor(d) /\ Reachable(d))

\* S5 (secondary): a FULL commit is only started once a DataComplete for the
\* current commit id was received for every source partition -- the
\* precondition of a sound valid-through timestamp (CI/CommitState.java
\* :146-165; F11, #18322, #17925).
ValidThroughSound ==
    \A k \in CoordIds : (cPc[k] = "commit" /\ cMode[k] = "full") => SrcParts \subseteq cDcParts[k]

\* S2: the readable floor never decreases through a connector commit
\* (environment actions -- expiry, other writers, branch ops -- excepted).
FloorMonotoneStep ==
    \A T \in Tables :
        (Len(snaps'[T]) > Len(snaps[T]) /\ snaps'[T][Len(snaps'[T])].writer = "conn")
            => \A c \in CtlParts : ReadFloorAt(T, c)' >= ReadFloorAt(T, c)
FloorMonotone == [][FloorMonotoneStep]_vars

-----------------------------------------------------------------------------
(***************************************************************************)
(* Type and structural invariants                                          *)
(***************************************************************************)
TypeOK ==
    /\ srcCommitted \in [SrcParts -> -1..NumRecs]
    /\ owner \in [SrcParts -> Tasks \cup {NoTask}]
    /\ gen \in Nat
    /\ tUp \in [Tasks -> BOOLEAN]
    /\ tVariant \in [Tasks -> RefVariants]
    /\ assign \in [Tasks -> SUBSET SrcParts]
    /\ pos \in [Tasks -> [SrcParts -> -1..NumRecs]]
    /\ taskGen \in [Tasks -> Nat]
    /\ tStep \in [Tasks -> TaskSteps]
    /\ closing \in [Tasks -> SUBSET SrcParts]
    /\ pendAdd \in [Tasks -> SUBSET SrcParts]
    /\ lostParts \in [Tasks -> SUBSET SrcParts]
    /\ stored \in [Tasks -> {"none", "fatal", "wakeup"}]
    /\ wakeup \in [Tasks -> BOOLEAN]
    /\ coordRef \in [Tasks -> 0..MaxCoord]
    /\ worker \in [Tasks -> BOOLEAN]
    /\ wBuf \in [Tasks -> SUBSET Records]
    /\ \A t \in Tasks : \A c \in CtlParts : wcPos[t][c] \in Nat
    /\ wcPart \in [Tasks -> CtlParts]
    /\ cPc \in [CoordIds -> CoordPcs]
    /\ cHost \in [CoordIds -> Tasks \cup {NoTask}]
    /\ cTerm \in [CoordIds -> BOOLEAN]
    /\ cErr \in [CoordIds -> ErrClasses]
    /\ cMode \in [CoordIds -> {"none", "full", "partial"}]
    /\ \A k \in CoordIds : \A g \in Groups : cGrp[k][g].st \in GrpStates
    /\ cgOwner \in [CtlParts -> 0..MaxCoord]
    /\ \A c \in CtlParts : cgCommitted[c] >= -1
    /\ \A c \in CtlParts : \A i \in 1..Len(ctl[c]) : ctl[c][i].type \in {"SC", "DW", "DC"}
    /\ \A T \in Tables : \A i \in 1..Len(snaps[T]) : snaps[T][i].parent < i

\* #18039: initTransactions of the newest instance fences every older one.
AtMostOneUnfencedCoordinator ==
    Cardinality({k \in CoordIds : cPc[k] \in {"start", "loop", "consume", "commit"}
                                  /\ ~Fenced(k)}) <= 1

CoordOwnerIsMember == \A c \in CtlParts : cgOwner[c] # 0 => cgOwner[c] \in CgMembers

CoordRefConsistent ==
    /\ \A t \in Tasks : coordRef[t] # 0 => cHost[coordRef[t]] = t
    /\ \A t1, t2 \in Tasks : (t1 # t2 /\ coordRef[t1] # 0) => coordRef[t1] # coordRef[t2]

\* The SinkWriter buffer only holds records of currently assigned partitions.
WorkerBufferAssigned == \A t \in Tasks : \A r \in wBuf[t] : r[1] \in assign[t]

\* A member's buffer never holds records below the committed offset (KIP-447
\* plus revoke-before-assign keep a single committing owner per partition).
NonLostBufferAboveCommitted ==
    \A t \in Tasks : lostParts[t] = {} =>
        \A r \in wBuf[t] : srcCommitted[r[1]] = -1 \/ r[2] >= srcCommitted[r[1]]

\* State constraint for base.cfg only (smoke run of the unbounded base spec).
BaseConstraint ==
    /\ nextCid <= 3
    /\ gen <= 2
    /\ \A c \in CtlParts : Len(ctl[c]) <= 6
    /\ \A T \in Tables : Len(snaps[T]) <= 3

=============================================================================
