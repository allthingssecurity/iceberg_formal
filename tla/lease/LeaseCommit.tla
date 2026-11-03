---------------------------- MODULE LeaseCommit ----------------------------
(***************************************************************************)
(* Lease-protected catalog commits without fencing (r5: C-3/C-8, C-11/C-13) *)
(*                                                                         *)
(* Two writers A, B commit to one table. The catalog holds a pointer ptr   *)
(* and the table content (the set of writers whose commits are in the      *)
(* current lineage). A commit that writes metadata derived from a stale    *)
(* base drops every commit made since that base: a lost update.            *)
(*                                                                         *)
(* LOCK = "hms"    (hive-metastore MetastoreLock + HiveTableOperations)    *)
(*   lock(): server lock, ACQUIRED if free            MetastoreLock:140-151*)
(*   heartbeat every HB ticks; a failed beat sets encounteredException and *)
(*     stops the task                                 MetastoreLock:468-499*)
(*   server reaper drops a lock whose last beat is older than T            *)
(*     (hive.txn.timeout; AcidHouseKeeperService -> TxnHandler.timeOutLocks)*)
(*   doCommit: check loc == base (HTO:288-293), ensureActive (HTO:329),    *)
(*     persistTable with expected location null when the lock is on        *)
(*     (HTO:332-333), ensureActive (HTO:334) -> CommitStateUnknown         *)
(* LOCK = "dynamo" (aws DynamoDbLockManager + GlueTableOperations)         *)
(*   acquire: row absent -> put-if-absent; else observe version v, wait    *)
(*     LEASE, put-if-version==v                   DynamoDbLockManager:175-236*)
(*   heartbeat every HB: put new version if owner==me; failures only logged *)
(*     (377-386); release: delete if owner==me (254-278)                   *)
(*   Glue: checkMetadataLocation (GTO:158) then updateTable WITHOUT        *)
(*     VersionId when a lock manager is set (GTO:328-330)                  *)
(* CAS = TRUE: the write itself is conditional on ptr = base (Hive with    *)
(*   expected location / Glue VersionId). This is the candidate fix.       *)
(* ALLOW_PAUSE: a writer process may freeze (GC, VM suspend, partition):   *)
(*   neither its commit thread nor its heartbeat thread steps meanwhile.   *)
(* Time: Tick advances a global clock, but never past a heartbeat that is  *)
(*   due for a running (non-paused) process: heartbeats of live processes  *)
(*   are prompt, so a lease can lapse only through a pause.                *)
(* Not modelled: lock-acquire timeouts, lost lock responses (C-6), the     *)
(*   in-JVM lock, more than two writers, the commit-status check.          *)
(* This is a DESIGN model. It cannot confirm anything about the code.      *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS LOCK, CAS, ALLOW_PAUSE, HB, T, LEASE, MaxTime, MaxRetry

ASSUME LOCK \in {"hms", "dynamo"}          \* an unknown lock name fails here
ASSUME CAS \in BOOLEAN /\ ALLOW_PAUSE \in BOOLEAN
ASSUME HB \in Nat \ {0} /\ T \in Nat /\ LEASE \in Nat /\ MaxTime \in Nat /\ MaxRetry \in Nat

W == {"A", "B"}
None == "none"

VARIABLES now, pc, status, tries, base, baseC, ptr, content,
          paused, npause, holder, lastHb, hbOn, hbFailed, ver, obsVer, obsAt

vars == <<now, pc, status, tries, base, baseC, ptr, content,
          paused, npause, holder, lastHb, hbOn, hbFailed, ver, obsVer, obsAt>>

Init ==
    /\ now = 0
    /\ pc = [w \in W |-> "start"]
    /\ status = [w \in W |-> "running"]
    /\ tries = [w \in W |-> 0]
    /\ base = [w \in W |-> 0]
    /\ baseC = [w \in W |-> {}]
    /\ ptr = 0
    /\ content = {}
    /\ paused = [w \in W |-> FALSE]
    /\ npause = [w \in W |-> 0]
    /\ holder = None                 \* HMS lock holder / DynamoDB row owner
    /\ lastHb = [w \in W |-> 0]
    /\ hbOn = [w \in W |-> FALSE]
    /\ hbFailed = [w \in W |-> FALSE]
    /\ ver = 0                       \* DynamoDB row version (a fresh uuid per put)
    /\ obsVer = [w \in W |-> 0]
    /\ obsAt = [w \in W |-> 0]

Running(w) == ~paused[w] /\ status[w] = "running"

\* --- process freeze -------------------------------------------------------
Pause(w) == /\ ALLOW_PAUSE /\ ~paused[w] /\ npause[w] < 1
            /\ paused' = [paused EXCEPT ![w] = TRUE]
            /\ npause' = [npause EXCEPT ![w] = @ + 1]
            /\ UNCHANGED <<now, pc, status, tries, base, baseC, ptr, content, holder,
                           lastHb, hbOn, hbFailed, ver, obsVer, obsAt>>
Resume(w) == /\ paused[w]
             /\ paused' = [paused EXCEPT ![w] = FALSE]
             /\ UNCHANGED <<now, pc, status, tries, base, baseC, ptr, content, npause, holder,
                            lastHb, hbOn, hbFailed, ver, obsVer, obsAt>>

\* --- clock ------------------------------------------------------------------
HbDue(w) == hbOn[w] /\ now - lastHb[w] >= HB
Tick == /\ now < MaxTime
        /\ \A w \in W : (hbOn[w] /\ ~paused[w]) => now + 1 - lastHb[w] <= HB
        /\ now' = now + 1
        /\ UNCHANGED <<pc, status, tries, base, baseC, ptr, content, paused, npause, holder,
                       lastHb, hbOn, hbFailed, ver, obsVer, obsAt>>

\* --- heartbeats ---------------------------------------------------------------
Heartbeat(w) ==
    /\ ~paused[w] /\ HbDue(w)
    /\ lastHb' = [lastHb EXCEPT ![w] = now]
    /\ IF LOCK = "hms"
       THEN IF holder = w
            THEN UNCHANGED <<hbOn, hbFailed>>
            ELSE /\ hbFailed' = [hbFailed EXCEPT ![w] = TRUE]   \* NoSuchLock: task stops
                 /\ hbOn' = [hbOn EXCEPT ![w] = FALSE]
       ELSE UNCHANGED <<hbOn, hbFailed>>                       \* dynamo: logged, keeps going
    /\ ver' = IF LOCK = "dynamo" /\ holder = w THEN ver + 1 ELSE ver
    /\ UNCHANGED <<now, pc, status, tries, base, baseC, ptr, content, paused, npause, holder,
                   obsVer, obsAt>>

\* HMS server reaper (hive.txn.timeout). lastHb of the holder is the server-side last beat.
Reap == /\ LOCK = "hms" /\ holder # None /\ now - lastHb[holder] > T
        /\ holder' = None
        /\ UNCHANGED <<now, pc, status, tries, base, baseC, ptr, content, paused, npause,
                       lastHb, hbOn, hbFailed, ver, obsVer, obsAt>>

\* --- writer -----------------------------------------------------------------------
Goto(w, l) == pc' = [pc EXCEPT ![w] = l]

\* refresh: the new metadata will be derived from this base
Start(w) == /\ Running(w) /\ pc[w] = "start"
            /\ base' = [base EXCEPT ![w] = ptr]
            /\ baseC' = [baseC EXCEPT ![w] = content]
            /\ Goto(w, IF LOCK = "hms" THEN "acq" ELSE "dacq")
            /\ UNCHANGED <<now, status, tries, ptr, content, paused, npause, holder,
                           lastHb, hbOn, hbFailed, ver, obsVer, obsAt>>

Locked(w) == /\ holder' = w
             /\ lastHb' = [lastHb EXCEPT ![w] = now]
             /\ hbOn' = [hbOn EXCEPT ![w] = TRUE]
             /\ hbFailed' = [hbFailed EXCEPT ![w] = FALSE]
             /\ Goto(w, "check")

\* HMS lock(): ACQUIRED when free (the WAITING/checkLock loop collapses to this guard)
Acq(w) == /\ Running(w) /\ pc[w] = "acq" /\ holder = None
          /\ Locked(w)
          /\ UNCHANGED <<now, status, tries, base, baseC, ptr, content, paused, npause,
                         ver, obsVer, obsAt>>

\* DynamoDB acquireOnce: put-if-absent, else observe the version and sleep LEASE
DAcq(w) == /\ Running(w) /\ pc[w] = "dacq"
           /\ IF holder = None
              THEN /\ Locked(w) /\ ver' = ver + 1 /\ UNCHANGED <<obsVer, obsAt>>
              ELSE /\ obsVer' = [obsVer EXCEPT ![w] = ver]
                   /\ obsAt' = [obsAt EXCEPT ![w] = now]
                   /\ Goto(w, "dwait")
                   /\ UNCHANGED <<holder, lastHb, hbOn, hbFailed, ver>>
           /\ UNCHANGED <<now, status, tries, base, baseC, ptr, content, paused, npause>>
DWait(w) == /\ Running(w) /\ pc[w] = "dwait" /\ now - obsAt[w] >= LEASE
            /\ IF holder = None \/ ver = obsVer[w]      \* put conditional on the observed version
               THEN /\ Locked(w) /\ ver' = ver + 1
               ELSE /\ Goto(w, "dacq") /\ UNCHANGED <<holder, lastHb, hbOn, hbFailed, ver>>
            /\ UNCHANGED <<now, status, tries, base, baseC, ptr, content, paused, npause,
                           obsVer, obsAt>>

Release(w) == /\ holder' = IF holder = w THEN None ELSE holder   \* unlock(id) / delete-if-owner
              /\ hbOn' = [hbOn EXCEPT ![w] = FALSE]

\* CommitFailedException: release and retry from a fresh base, or give up
Fail(w) == /\ Release(w)
           /\ IF tries[w] < MaxRetry
              THEN /\ tries' = [tries EXCEPT ![w] = @ + 1] /\ Goto(w, "start")
                   /\ UNCHANGED status
              ELSE /\ status' = [status EXCEPT ![w] = "failed"] /\ Goto(w, "end")
                   /\ UNCHANGED tries

\* check-then-write: HTO:288-293 / GTO:158 checkMetadataLocation
Check(w) == /\ Running(w) /\ pc[w] = "check"
            /\ IF ptr = base[w]
               THEN /\ Goto(w, IF LOCK = "hms" THEN "ens1" ELSE "write")
                    /\ UNCHANGED <<status, tries, holder, hbOn>>
               ELSE Fail(w)
            /\ UNCHANGED <<now, base, baseC, ptr, content, paused, npause, lastHb, hbFailed,
                           ver, obsVer, obsAt>>

\* HTO:329 ensureActive: a LOCAL point check of the heartbeat task's state
Ens1(w) == /\ Running(w) /\ pc[w] = "ens1"
           /\ IF hbFailed[w]
              THEN /\ status' = [status EXCEPT ![w] = "failed"] /\ Release(w) /\ Goto(w, "end")
              ELSE /\ Goto(w, "write") /\ UNCHANGED <<status, holder, hbOn>>
           /\ UNCHANGED <<now, tries, base, baseC, ptr, content, paused, npause, lastHb, hbFailed,
                          ver, obsVer, obsAt>>

\* the metastore write: no fencing token; conditional on ptr = base only when CAS
Write(w) == /\ Running(w) /\ pc[w] = "write"
            /\ IF CAS /\ ptr # base[w]
               THEN /\ Fail(w) /\ UNCHANGED <<ptr, content>>
               ELSE /\ ptr' = ptr + 1
                    /\ content' = baseC[w] \cup {w}
                    /\ IF LOCK = "hms"
                       THEN /\ Goto(w, "ens2") /\ UNCHANGED <<status, holder, hbOn>>
                       ELSE /\ status' = [status EXCEPT ![w] = "success"]
                            /\ Release(w) /\ Goto(w, "end")
                    /\ UNCHANGED tries
            /\ UNCHANGED <<now, base, baseC, paused, npause, lastHb, hbFailed, ver, obsVer, obsAt>>

\* HTO:334 ensureActive after the write -> CommitStateUnknown, else SUCCESS; then unlock
Ens2(w) == /\ Running(w) /\ pc[w] = "ens2"
           /\ status' = [status EXCEPT ![w] = IF hbFailed[w] THEN "unknown" ELSE "success"]
           /\ Release(w) /\ Goto(w, "end")
           /\ UNCHANGED <<now, tries, base, baseC, ptr, content, paused, npause, lastHb, hbFailed,
                          ver, obsVer, obsAt>>

Next == \/ Tick \/ Reap
        \/ \E w \in W : \/ Pause(w) \/ Resume(w) \/ Heartbeat(w)
                        \/ Start(w) \/ Acq(w) \/ DAcq(w) \/ DWait(w)
                        \/ Check(w) \/ Ens1(w) \/ Write(w) \/ Ens2(w)

Spec == Init /\ [][Next]_vars

\* ---------------------------------------------------------------------------
TypeOK == /\ holder \in W \cup {None} /\ content \subseteq W /\ ptr \in Nat
\* a writer told SUCCESS has its commit in the current table
NoLostCommit == \A w \in W : status[w] = "success" => w \in content
\* lease safety in the abstract: two writes never land from overlapping critical sections
\* (checked through its consequence above; this one just says the lock is single-holder)
SingleHolder == holder \in W \cup {None}
\* witness (violated on purpose): both writers can be told SUCCESS
W_BothSuccess == ~(status["A"] = "success" /\ status["B"] = "success")
\* witness (violated on purpose): the lease of a writer can lapse while it is inside its
\* critical section (LOCK = hms: reaped; dynamo: stolen)
W_LeaseLapse == ~(\E w \in W : pc[w] \in {"ens1", "write"} /\ holder # w)
=============================================================================
