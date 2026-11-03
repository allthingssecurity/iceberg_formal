------------------------------- MODULE KcSink -------------------------------
(***************************************************************************)
(* Iceberg Kafka Connect sink: control-topic commit protocol, exactly-once *)
(* safety only.                                                             *)
(*                                                                         *)
(* Source topic: partitions Parts (0 is the leader partition), MaxOff      *)
(* records each.  Sink tasks run a Worker (buffer of written records,       *)
(* answers StartCommit with DataWritten + DataComplete and commits source  *)
(* offsets in the same Kafka transaction) and, on the leader, a            *)
(* Coordinator (reads the control topic from its group offset, buffers     *)
(* DataWritten of ANY commit id, counts DataComplete of the current one,   *)
(* commits the files of buffered envelopes whose control offset is >= the  *)
(* table's stored offset, stores max(stored, consumed position), then     *)
(* commits its own control-topic group offset).                            *)
(*                                                                         *)
(* Knobs (each one a released behaviour or a candidate fix):               *)
(*   CLOSE      "prefix" = 1.8.1 open/close (close tears the committer      *)
(*              down, put() drops records until the next open, open on a   *)
(*              live committer fails the task); "fixed" = 1.9.0+ (#12372)  *)
(*   SEEKNULL   what close() does for a retained partition that has no     *)
(*              committed source offset: "skip" (released: no seek),       *)
(*              "earliest" (candidate fix)                                 *)
(*   CRESET     coordinator control consumer auto.offset.reset when its    *)
(*              group has no offset: "latest" (released default) /         *)
(*              "earliest" (#18006, open)                                  *)
(*   REPLAY     coordinator position tracking: "put" (<= 1.11.0, plain     *)
(*              put, regresses on re-read) / "max" (1.12.0, #17933 + skip) *)
(*   MaxRewind  eager rebalances of <group>-coord that send the live       *)
(*              coordinator back to the group offset (zombie join/leave)   *)
(*   FAIL       table-commit failure handling: "retry" (<= 1.11.0: logged,  *)
(*              buffer kept) / "terminate" (1.12.0: coordinator dies,      *)
(*              task fails and is restarted)                               *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS Tasks, NP, MaxOff, MaxCommits, MaxCtl, MaxCrash, MaxMoves, MaxRewind, MaxFail,
          CLOSE, SEEKNULL, CRESET, REPLAY, FAIL

ASSUME CLOSE \in {"prefix", "fixed"}
ASSUME SEEKNULL \in {"skip", "earliest"}
ASSUME CRESET \in {"latest", "earliest"}
ASSUME REPLAY \in {"put", "max"}
ASSUME FAIL \in {"retry", "terminate"}

Parts == 0..(NP - 1)
Recs == Parts \X (0..(MaxOff - 1))
NONE == 0
NULL == -1
Max(a, b) == IF a > b THEN a ELSE b

VARIABLES
  owner,      \* p -> task owning it in the connect group (NONE while unassigned)
  pos,        \* t -> p -> framework fetch position
  srcC,       \* p -> committed source offset of the connect group (NULL = none)
  up,         \* t -> process running
  committer,  \* t -> CommitterImpl exists (1.8.1 nulls it in close)
  worker,     \* t -> Worker exists
  wbuf,       \* t -> records written, not yet announced
  woff,       \* t -> p -> next offset of the last buffered record (NULL = none)
  wseen,      \* t -> control-topic entries the worker has looked at
  ctl,        \* the control topic (committed transactions only)
  coord,      \* task hosting the coordinator, NONE if none
  cpos,       \* coordinator fetch position (index of the next entry to read)
  cto,        \* coordinator controlTopicOffsets (next index after the last read)
  cbuf,       \* indices of buffered DataWritten envelopes
  ccur,       \* current commit id (0 = none)
  ccnt,       \* partitions counted for ccur
  cphase,     \* "idle" | "table" | "offsets"
  cgo,        \* <group>-coord committed offset (0 = none)
  table,      \* r -> number of rows in the table
  tOff,       \* control offset stored in the latest snapshot (0 = none)
  nextCid, crashes, moves, rewinds, fails

vars == <<owner, pos, srcC, up, committer, worker, wbuf, woff, wseen, ctl, coord, cpos,
          cto, cbuf, ccur, ccnt, cphase, cgo, table, tOff, nextCid, crashes, moves,
          rewinds, fails>>

NoOff == [p \in Parts |-> NULL]

Init ==
  /\ owner = [p \in Parts |-> CHOOSE t \in Tasks : TRUE]   \* initial: first task owns all
  /\ pos = [t \in Tasks |-> [p \in Parts |-> 0]]
  /\ srcC = [p \in Parts |-> NULL]
  /\ up = [t \in Tasks |-> TRUE]
  /\ committer = [t \in Tasks |-> TRUE]
  /\ worker = [t \in Tasks |-> FALSE]
  /\ wbuf = [t \in Tasks |-> {}]
  /\ woff = [t \in Tasks |-> NoOff]
  /\ wseen = [t \in Tasks |-> 0]
  /\ ctl = << >>
  /\ coord = CHOOSE t \in Tasks : TRUE
  /\ cpos = 1 /\ cto = 0 /\ cbuf = {} /\ ccur = 0 /\ ccnt = 0 /\ cphase = "idle"
  /\ cgo = 0
  /\ table = [r \in Recs |-> 0]
  /\ tOff = 0
  /\ nextCid = 1 /\ crashes = 0 /\ moves = 0 /\ rewinds = 0 /\ fails = 0

Owned(t) == {p \in Parts : owner[p] = t}
StartPos(p) == IF srcC[p] = NULL THEN 0 ELSE srcC[p]     \* sink default auto.offset.reset=earliest
CoordResume == IF cgo # 0 THEN cgo ELSE IF CRESET = "latest" THEN Len(ctl) + 1 ELSE 1

\* --------------------------------------------------------------- worker side

\* WorkerSinkTask.poll -> put(record).  1.8.1: committer == null -> record silently dropped.
Poll(t, p) ==
  /\ up[t] /\ owner[p] = t /\ pos[t][p] < MaxOff
  /\ pos' = [pos EXCEPT ![t][p] = @ + 1]
  /\ IF committer[t]
       THEN /\ wbuf' = [wbuf EXCEPT ![t] = @ \cup {<<p, pos[t][p]>>}]
            /\ woff' = [woff EXCEPT ![t][p] = pos[t][p] + 1]
            /\ worker' = [worker EXCEPT ![t] = TRUE]
            /\ wseen' = IF worker[t] THEN wseen ELSE [wseen EXCEPT ![t] = Len(ctl)]  \* latest
       ELSE UNCHANGED <<wbuf, woff, worker, wseen>>
  /\ UNCHANGED <<owner, srcC, up, committer, ctl, coord, cpos, cto, cbuf, ccur, ccnt, cphase,
                 cgo, table, tOff, nextCid, crashes, moves, rewinds, fails>>

\* Worker sees the next StartCommit: DataWritten (if any data) + DataComplete + source offsets,
\* one Kafka transaction.
Respond(t) ==
  /\ up[t] /\ worker[t]
  /\ \E i \in (wseen[t] + 1)..Len(ctl) :
       /\ ctl[i].k = "SC"
       /\ \A j \in (wseen[t] + 1)..(i - 1) : ctl[j].k # "SC"
       /\ LET dw == IF wbuf[t] = {} THEN << >> ELSE << [k |-> "DW", recs |-> wbuf[t], cid |-> ctl[i].cid] >>
              dc == << [k |-> "DC", n |-> Cardinality(Owned(t)), cid |-> ctl[i].cid] >>
          IN ctl' = ctl \o dw \o dc
       /\ wseen' = [wseen EXCEPT ![t] = i]
  /\ srcC' = [p \in Parts |-> IF woff[t][p] # NULL THEN woff[t][p] ELSE srcC[p]]
  /\ wbuf' = [wbuf EXCEPT ![t] = {}]
  /\ woff' = [woff EXCEPT ![t] = NoOff]
  /\ UNCHANGED <<owner, pos, up, committer, worker, coord, cpos, cto, cbuf, ccur, ccnt, cphase,
                 cgo, table, tOff, nextCid, crashes, moves, rewinds, fails>>

\* --------------------------------------------------------------- coordinator

CoordAlive == coord # NONE /\ up[coord]

StartCommit ==
  /\ CoordAlive /\ ccur = 0 /\ cphase = "idle" /\ nextCid <= MaxCommits
  /\ ctl' = Append(ctl, [k |-> "SC", cid |-> nextCid])
  /\ ccur' = nextCid /\ ccnt' = 0 /\ nextCid' = nextCid + 1
  /\ UNCHANGED <<owner, pos, srcC, up, committer, worker, wbuf, woff, wseen, coord, cpos, cto,
                 cbuf, cphase, cgo, table, tOff, crashes, moves, rewinds, fails>>

Read ==
  /\ CoordAlive /\ cphase = "idle" /\ cpos <= Len(ctl)
  /\ cpos' = cpos + 1
  /\ IF REPLAY = "max" /\ cpos < cto
       THEN UNCHANGED <<cto, cbuf, ccnt, cphase>>             \* 1.12.0: skip already consumed
       ELSE LET e == ctl[cpos] IN
            /\ cto' = IF REPLAY = "max" THEN Max(cto, cpos + 1) ELSE cpos + 1
            /\ cbuf' = IF e.k = "DW" THEN cbuf \cup {cpos} ELSE cbuf
            /\ ccnt' = IF e.k = "DC" /\ e.cid = ccur /\ ccur # 0 THEN ccnt + e.n ELSE ccnt
            /\ cphase' = IF e.k = "DC" /\ e.cid = ccur /\ ccur # 0 /\ ccnt + e.n >= NP
                           THEN "table" ELSE "idle"
  /\ UNCHANGED <<owner, pos, srcC, up, committer, worker, wbuf, woff, wseen, ctl, coord, ccur,
                 cgo, table, tOff, nextCid, crashes, moves, rewinds, fails>>

\* commit.timeout-ms: partial commit with whatever is buffered
Timeout ==
  /\ CoordAlive /\ ccur # 0 /\ cphase = "idle"
  /\ cphase' = "table"
  /\ UNCHANGED <<owner, pos, srcC, up, committer, worker, wbuf, woff, wseen, ctl, coord, cpos,
                 cto, cbuf, ccur, ccnt, cgo, table, tOff, nextCid, crashes, moves, rewinds, fails>>

Eligible == {i \in cbuf : tOff = 0 \/ i >= tOff}
Files == {ctl[i] : i \in Eligible}   \* one data file per DataWritten envelope

CommitTable ==
  /\ CoordAlive /\ cphase = "table"
  /\ IF Files = {}
       THEN UNCHANGED <<table, tOff>>                        \* "nothing to commit": no snapshot
       ELSE /\ table' = [r \in Recs |-> table[r] + Cardinality({f \in Files : r \in f.recs})]
            /\ tOff' = Max(tOff, cto)
  /\ cphase' = "offsets"
  /\ UNCHANGED <<owner, pos, srcC, up, committer, worker, wbuf, woff, wseen, ctl, coord, cpos,
                 cto, cbuf, ccur, ccnt, cgo, nextCid, crashes, moves, rewinds, fails>>

\* the catalog rejects the commit (nothing applied)
CommitTableFails ==
  /\ CoordAlive /\ cphase = "table" /\ fails < MaxFail /\ Files # {}
  /\ fails' = fails + 1
  /\ IF FAIL = "retry"
       THEN /\ cphase' = "idle" /\ ccur' = 0 /\ ccnt' = 0
            /\ UNCHANGED <<coord, cbuf>>
       ELSE /\ coord' = NONE /\ cbuf' = {} /\ cphase' = "idle" /\ ccur' = 0 /\ ccnt' = 0
  /\ UNCHANGED <<owner, pos, srcC, up, committer, worker, wbuf, woff, wseen, ctl, cpos, cto,
                 cgo, table, tOff, nextCid, crashes, moves, rewinds>>

CommitOffsets ==
  /\ CoordAlive /\ cphase = "offsets"
  /\ cgo' = Max(cgo, cto)
  /\ cbuf' = {} /\ ccur' = 0 /\ ccnt' = 0 /\ cphase' = "idle"
  /\ UNCHANGED <<owner, pos, srcC, up, committer, worker, wbuf, woff, wseen, ctl, coord, cpos,
                 cto, table, tOff, nextCid, crashes, moves, rewinds, fails>>

\* eager rebalance of <group>-coord (a zombie/second coordinator joins or leaves): the live
\* coordinator keeps its in-memory state but fetches from the group offset again
Rewind ==
  /\ CoordAlive /\ cphase = "idle" /\ rewinds < MaxRewind /\ cpos > CoordResume
  /\ cpos' = CoordResume /\ rewinds' = rewinds + 1
  /\ UNCHANGED <<owner, pos, srcC, up, committer, worker, wbuf, woff, wseen, ctl, coord, cto,
                 cbuf, ccur, ccnt, cphase, cgo, table, tOff, nextCid, crashes, moves, fails>>

\* --------------------------------------------------------------- rebalances, crashes

StartCoord(t) ==
  /\ coord' = t /\ cpos' = CoordResume /\ cto' = 0 /\ cbuf' = {} /\ ccur' = 0 /\ ccnt' = 0
  /\ cphase' = "idle"

NoCoordChange == UNCHANGED <<coord, cpos, cto, cbuf, ccur, ccnt, cphase>>

\* cooperative revocation of p from t: close(t, [p]) while p is still assigned
Revoke(t, p) ==
  /\ up[t] /\ owner[p] = t /\ moves < MaxMoves
  /\ moves' = moves + 1
  /\ owner' = [owner EXCEPT ![p] = NONE]
  /\ wbuf' = [wbuf EXCEPT ![t] = {}]
  /\ woff' = [woff EXCEPT ![t] = NoOff]
  /\ worker' = [worker EXCEPT ![t] = FALSE]
  /\ IF CLOSE = "prefix"
       THEN /\ committer' = [committer EXCEPT ![t] = FALSE]          \* committer = null
            /\ IF coord = t THEN coord' = NONE /\ cbuf' = {} /\ ccur' = 0 /\ ccnt' = 0
                                /\ cphase' = "idle" /\ UNCHANGED <<cpos, cto>>
                            ELSE NoCoordChange
            /\ UNCHANGED pos                                          \* no seek
       ELSE /\ UNCHANGED committer
            /\ IF coord = t /\ p = 0 THEN coord' = NONE /\ cbuf' = {} /\ ccur' = 0 /\ ccnt' = 0
                                         /\ cphase' = "idle" /\ UNCHANGED <<cpos, cto>>
                                     ELSE NoCoordChange
            /\ pos' = [pos EXCEPT ![t] = [q \in Parts |->
                         IF owner[q] # t THEN pos[t][q]
                         ELSE IF srcC[q] # NULL THEN srcC[q]
                         ELSE IF SEEKNULL = "earliest" THEN 0 ELSE pos[t][q]]]
  /\ UNCHANGED <<srcC, up, wseen, ctl, cgo, table, tOff, nextCid, crashes, rewinds, fails>>

\* incremental assignment of an unowned p to t, then open(t, [p])
Assign(t, p) ==
  /\ up[t] /\ owner[p] = NONE
  /\ owner' = [owner EXCEPT ![p] = t]
  /\ pos' = [pos EXCEPT ![t][p] = StartPos(p)]
  /\ IF CLOSE = "prefix"
       THEN IF committer[t]
              THEN \* 1.8.1 open(): Preconditions "Committer already open" -> the task fails:
                   \* WorkerSinkTask closes all, stops, the operator restarts it
                   /\ committer' = [committer EXCEPT ![t] = FALSE]
                   /\ worker' = [worker EXCEPT ![t] = FALSE]
                   /\ wbuf' = [wbuf EXCEPT ![t] = {}]
                   /\ woff' = [woff EXCEPT ![t] = NoOff]
                   /\ UNCHANGED wseen
                   /\ IF coord = t THEN coord' = NONE /\ cbuf' = {} /\ ccur' = 0 /\ ccnt' = 0
                                       /\ cphase' = "idle" /\ UNCHANGED <<cpos, cto>>
                                   ELSE NoCoordChange
              ELSE \* new committer: worker + coordinator if t's whole assignment holds partition 0
                   /\ committer' = [committer EXCEPT ![t] = TRUE]
                   /\ worker' = [worker EXCEPT ![t] = TRUE]
                   /\ wseen' = [wseen EXCEPT ![t] = Len(ctl)]
                   /\ UNCHANGED <<wbuf, woff>>
                   /\ IF (p = 0 \/ owner[0] = t) /\ coord = NONE THEN StartCoord(t) ELSE NoCoordChange
       ELSE /\ UNCHANGED <<committer, worker, wbuf, woff, wseen>>
            /\ IF p = 0 /\ coord = NONE THEN StartCoord(t) ELSE NoCoordChange
  /\ UNCHANGED <<srcC, up, ctl, cgo, table, tOff, nextCid, crashes, moves, rewinds, fails>>

\* process crash: nothing runs, partitions are released after the session timeout
Crash(t) ==
  /\ up[t] /\ crashes < MaxCrash
  /\ crashes' = crashes + 1
  /\ up' = [up EXCEPT ![t] = FALSE]
  /\ owner' = [p \in Parts |-> IF owner[p] = t THEN NONE ELSE owner[p]]
  /\ worker' = [worker EXCEPT ![t] = FALSE]
  /\ wbuf' = [wbuf EXCEPT ![t] = {}]
  /\ woff' = [woff EXCEPT ![t] = NoOff]
  /\ IF coord = t THEN coord' = NONE /\ cbuf' = {} /\ ccur' = 0 /\ ccnt' = 0 /\ cphase' = "idle"
                       /\ UNCHANGED <<cpos, cto>>
                  ELSE NoCoordChange
  /\ UNCHANGED <<pos, srcC, committer, wseen, ctl, cgo, table, tOff, nextCid, moves, rewinds, fails>>

\* 1.12.0: a terminated coordinator fails its task (NotRunningException); same as a crash for
\* the protocol, except that WorkerSinkTask closes and stops it.  Restart brings it back.
Restart(t) ==
  /\ ~up[t]
  /\ up' = [up EXCEPT ![t] = TRUE]
  /\ committer' = [committer EXCEPT ![t] = (CLOSE = "fixed")]
  /\ UNCHANGED <<owner, pos, srcC, worker, wbuf, woff, wseen, ctl, coord, cpos, cto, cbuf, ccur,
                 ccnt, cphase, cgo, table, tOff, nextCid, crashes, moves, rewinds, fails>>

\* the coordinator died (FAIL = terminate) while its task still holds partition 0: the task
\* fails, all its partitions are released (and reassigned later)
CoordDeadTaskFails(t) ==
  /\ up[t] /\ coord = NONE /\ owner[0] = t /\ CLOSE = "fixed" /\ crashes < MaxCrash
  /\ crashes' = crashes + 1
  /\ up' = [up EXCEPT ![t] = FALSE]
  /\ owner' = [p \in Parts |-> IF owner[p] = t THEN NONE ELSE owner[p]]
  /\ worker' = [worker EXCEPT ![t] = FALSE]
  /\ wbuf' = [wbuf EXCEPT ![t] = {}]
  /\ woff' = [woff EXCEPT ![t] = NoOff]
  /\ UNCHANGED <<pos, srcC, committer, wseen, ctl, cgo, table, tOff, nextCid, moves, rewinds,
                 fails, coord, cpos, cto, cbuf, ccur, ccnt, cphase>>

Next ==
  \/ \E t \in Tasks, p \in Parts : Poll(t, p) \/ Revoke(t, p) \/ Assign(t, p)
  \/ \E t \in Tasks : Respond(t) \/ Crash(t) \/ Restart(t) \/ CoordDeadTaskFails(t)
  \/ StartCommit \/ Read \/ Timeout \/ CommitTable \/ CommitTableFails \/ CommitOffsets \/ Rewind

Spec == Init /\ [][Next]_vars

\* --------------------------------------------------------------- properties

TypeOK ==
  /\ owner \in [Parts -> Tasks \cup {NONE}]
  /\ srcC \in [Parts -> (NULL..MaxOff)]
  /\ cphase \in {"idle", "table", "offsets"}
  /\ table \in [Recs -> 0..4]

\* every DataWritten that still has a path into the table
Pending(i) ==
  IF CoordAlive THEN i \in cbuf \/ i >= cpos
  ELSE (cgo # 0 /\ i >= cgo) \/ (cgo = 0 /\ CRESET = "earliest")
Recoverable(r) ==
  \E i \in 1..Len(ctl) : /\ ctl[i].k = "DW" /\ r \in ctl[i].recs
                         /\ (tOff = 0 \/ i >= tOff) /\ Pending(i)

\* a record whose source offset is committed is in the table or still recoverable
NoLoss == \A r \in Recs : (srcC[r[1]] # NULL /\ r[2] < srcC[r[1]]) => (table[r] >= 1 \/ Recoverable(r))
NoDup == \A r \in Recs : table[r] <= 1

\* state constraint (bounds the control topic)
CtlBound == Len(ctl) <= MaxCtl

\* non-vacuity witnesses: these must be VIOLATED (reachable)
W_AllCommitted == ~(\A r \in Recs : table[r] = 1)
W_Rewound == rewinds = 0
=============================================================================
