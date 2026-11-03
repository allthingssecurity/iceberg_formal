--------------------------- MODULE IcebergCommit ---------------------------
(***************************************************************************)
(* Apache Iceberg commit protocol: two writers (think Spark, Flink, Trino) *)
(* committing to one table through one catalog pointer, with optimistic    *)
(* concurrency, conflict validation and retry.                             *)
(*                                                                         *)
(* Spec anchors (Iceberg table spec v2/v3 and the Java core):              *)
(*   the catalog holds one pointer to the current metadata file; a commit  *)
(*     is a compare-and-set of that pointer                                *)
(*     (TableOperations.commit(base, metadata), BaseTransaction)           *)
(*   every commit gets the next sequence number; a data file carries the    *)
(*     sequence number of the commit that added it                         *)
(*   a delete file applies ONLY to data files whose data sequence number   *)
(*     is strictly lower than the delete file's own sequence number        *)
(*     (table spec, "Scan Planning" / sequence number inheritance)         *)
(*   RewriteFiles replaces data files and may inherit the replaced files'  *)
(*     sequence number, which is what keeps existing deletes applying      *)
(*   RowDelta/OverwriteFiles validate at commit time:                      *)
(*     validateNoConflictingDeletes  -> snapshot isolation                 *)
(*     validateNoConflictingDataFiles -> serializable                      *)
(*   a commit whose answer is lost raises CommitStateUnknownException and   *)
(*     the client must not clean up, because the commit may have landed    *)
(*                                                                         *)
(* Modelled: the pointer, the snapshot tree, sequence numbers, data files  *)
(* and equality delete files, the retry loop, both validations, an atomic  *)
(* and a non-atomic catalog, and a lost commit answer.                     *)
(* Not modelled: Parquet, manifest layout, partitioning, more than two     *)
(* writers. Files are opaque sets of row ids.                              *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    OpA, OpB,                   \* "append" | "delete" | "rewrite"  per writer
    PayA, PayB,                 \* rows appended, or rows the delete predicate matches
    InitRows,                   \* rows in the table at snapshot 0
    CATALOG,                    \* "cas"    atomic compare-and-set (REST, JDBC, Nessie, Polaris)
                                \* "rename" read version then write version+1 (filesystem catalog)
    ISOLATION,                  \* "snapshot" | "serializable"
    REWRITE_KEEPS_SEQ,          \* TRUE: a rewrite inherits the replaced files' sequence number
    VALIDATE_REWRITE_DELETES,   \* TRUE: a rewrite fails if a delete file arrived since it planned
    LOSE_ACK,                   \* TRUE: the catalog's answer may be lost (CommitStateUnknown)
    IDEMPOTENCY_TOKEN,          \* candidate fix: the writer can recognise its own snapshot
    MaxRetry, MaxSnap, MaxSeq

Rows    == {1, 2, 3}
Writers == {"A", "B"}

OpOf(w)  == IF w = "A" THEN OpA  ELSE OpB
PayOf(w) == IF w = "A" THEN PayA ELSE PayB

SnapIds   == 0 .. MaxSnap
Root      == [data |-> {[rows |-> InitRows, seq |-> 0]}, dels |-> {}, parent |-> 0, by |-> "none"]
EmptySnap == [data |-> {},                               dels |-> {}, parent |-> 0, by |-> "none"]

VARIABLES
    snaps,      \* snapshot id -> [data, dels, parent, by]; immutable once written
    nsnap,      \* next free snapshot id
    pointer,    \* the snapshot id the catalog points at
    cver,       \* catalog version counter (only the "rename" catalog reads it)
    nseq,       \* next sequence number
    pc,         \* writer -> "plan"|"write"|"prepare"|"cas"|"put"|"ack"|"unknown"|"done"|"abort"
    base,       \* writer -> the snapshot it planned against
    cand,       \* writer -> the snapshot its pending metadata is built on
    candver,    \* writer -> the catalog version it read at prepare time
    tries,      \* writer -> retries so far
    ok,         \* writer -> the catalog reported success at least once
    mine,       \* writer -> the snapshot id it created
    landed      \* ghost: writer -> how many times its payload reached a snapshot

vars == <<snaps, nsnap, pointer, cver, nseq, pc, base, cand, candver, tries, ok, mine, landed>>

(***************************************************************************)
(* Reading the table                                                       *)
(***************************************************************************)

\* A row is visible when some data file holds it and no delete file with a
\* HIGHER sequence number covers it. This is the v2 rule, and it is the
\* reason a concurrently appended row can survive an earlier delete.
Vis(s) ==
    { r \in Rows :
        \E d \in snaps[s].data :
            /\ r \in d.rows
            /\ ~ \E k \in snaps[s].dels : r \in k.rows /\ d.seq < k.seq }

RECURSIVE Anc(_)
Anc(s) == IF s = 0 THEN {0} ELSE {s} \cup Anc(snaps[s].parent)

MaxSeqOf(fs) == IF fs = {} THEN 0
                           ELSE CHOOSE m \in {d.seq : d \in fs} : \A d \in fs : d.seq <= m

(***************************************************************************)
(* Building and validating a commit                                        *)
(***************************************************************************)

\* The metadata writer w produces on top of snapshot cur. Its payload was
\* computed against base[w], which after a retry is no longer the current
\* snapshot: that gap is where the anomalies live.
Built(cur, w) ==
    LET s == snaps[cur]
        b == snaps[base[w]]
    IN CASE OpOf(w) = "append" ->
                [ data   |-> s.data \cup {[rows |-> PayOf(w), seq |-> nseq]},
                  dels   |-> s.dels,
                  parent |-> cur, by |-> w ]
         [] OpOf(w) = "delete" ->
                [ data   |-> s.data,
                  dels   |-> s.dels \cup {[rows |-> PayOf(w), seq |-> nseq]},
                  parent |-> cur, by |-> w ]
         [] OpOf(w) = "rewrite" ->
                \* compaction: replace the files it read with one file holding the
                \* rows visible at plan time, and drop the deletes it applied
                [ data   |-> (s.data \ b.data)
                                \cup {[ rows |-> Vis(base[w]),
                                        seq  |-> IF REWRITE_KEEPS_SEQ
                                                   THEN MaxSeqOf(b.data)
                                                   ELSE nseq ]},
                  dels   |-> s.dels \ b.dels,
                  parent |-> cur, by |-> w ]

Valid(cur, w) ==
    CASE OpOf(w) = "append" -> TRUE          \* appends never conflict
      [] OpOf(w) = "delete" ->
           IF ISOLATION = "serializable"
             \* validateNoConflictingDataFiles
             THEN ~ \E d \in (snaps[cur].data \ snaps[base[w]].data) :
                        d.rows \cap PayOf(w) # {}
             \* validateNoConflictingDeletes
             ELSE ~ \E k \in (snaps[cur].dels \ snaps[base[w]].dels) :
                        k.rows \cap PayOf(w) # {}
      [] OpOf(w) = "rewrite" ->
           IF VALIDATE_REWRITE_DELETES
             THEN snaps[cur].dels \ snaps[base[w]].dels = {}
             ELSE TRUE

(***************************************************************************)
(* Actions                                                                 *)
(***************************************************************************)

Init ==
    /\ snaps   = [i \in SnapIds |-> IF i = 0 THEN Root ELSE EmptySnap]
    /\ nsnap   = 1
    /\ pointer = 0
    /\ cver    = 0
    /\ nseq    = 1
    /\ pc      = [w \in Writers |-> "plan"]
    /\ base    = [w \in Writers |-> 0]
    /\ cand    = [w \in Writers |-> 0]
    /\ candver = [w \in Writers |-> 0]
    /\ tries   = [w \in Writers |-> 0]
    /\ ok      = [w \in Writers |-> FALSE]
    /\ mine    = [w \in Writers |-> 0]
    /\ landed  = [w \in Writers |-> 0]

\* read the current table metadata
Plan(w) ==
    /\ pc[w] = "plan"
    /\ base' = [base EXCEPT ![w] = pointer]
    /\ pc'   = [pc   EXCEPT ![w] = "write"]
    /\ UNCHANGED <<snaps, nsnap, pointer, cver, nseq, cand, candver, tries, ok, mine, landed>>

\* write the data files to the object store; the catalog is untouched, so a
\* failure here leaves garbage and never corruption
WriteFiles(w) ==
    /\ pc[w] = "write"
    /\ pc' = [pc EXCEPT ![w] = "prepare"]
    /\ UNCHANGED <<snaps, nsnap, pointer, cver, nseq, base, cand, candver, tries, ok, mine, landed>>

\* refresh, run the validations, build the new metadata
Prepare(w) ==
    /\ pc[w] = "prepare"
    /\ IF Valid(pointer, w)
         THEN /\ cand'    = [cand    EXCEPT ![w] = pointer]
              /\ candver' = [candver EXCEPT ![w] = cver]
              /\ pc'      = [pc      EXCEPT ![w] = "cas"]
         ELSE /\ pc' = [pc EXCEPT ![w] = "abort"]     \* ValidationException
              /\ UNCHANGED <<cand, candver>>
    /\ UNCHANGED <<snaps, nsnap, pointer, cver, nseq, base, tries, ok, mine, landed>>

Retreat(w) ==
    IF tries[w] < MaxRetry
      THEN /\ tries' = [tries EXCEPT ![w] = tries[w] + 1]
           /\ pc'    = [pc    EXCEPT ![w] = "prepare"]
      ELSE /\ pc' = [pc EXCEPT ![w] = "abort"]
           /\ UNCHANGED tries

\* atomic catalog: one compare-and-set
Cas(w) ==
    /\ CATALOG = "cas"
    /\ pc[w] = "cas"
    /\ IF pointer = cand[w]
         THEN /\ nsnap < MaxSnap /\ nseq < MaxSeq
              /\ snaps'   = [snaps  EXCEPT ![nsnap] = Built(cand[w], w)]
              /\ nsnap'   = nsnap + 1
              /\ nseq'    = nseq + 1
              /\ pointer' = nsnap
              /\ cver'    = cver + 1
              /\ mine'    = [mine   EXCEPT ![w] = nsnap]
              /\ landed'  = [landed EXCEPT ![w] = landed[w] + 1]
              /\ ok'      = [ok     EXCEPT ![w] = TRUE]
              /\ pc'      = [pc     EXCEPT ![w] = "ack"]
              /\ UNCHANGED <<base, cand, candver, tries>>
         ELSE \* CommitFailedException: somebody moved the pointer first
              /\ Retreat(w)
              /\ UNCHANGED <<snaps, nsnap, pointer, cver, nseq, base, cand, candver, ok, mine, landed>>

\* non-atomic catalog: read the version, then write version+1. Two writers can
\* pass the check at the same version and both write.
CheckVer(w) ==
    /\ CATALOG = "rename"
    /\ pc[w] = "cas"
    /\ IF cver = candver[w]
         THEN /\ pc' = [pc EXCEPT ![w] = "put"]
              /\ UNCHANGED tries
         ELSE Retreat(w)
    /\ UNCHANGED <<snaps, nsnap, pointer, cver, nseq, base, cand, candver, ok, mine, landed>>

Put(w) ==
    /\ CATALOG = "rename"
    /\ pc[w] = "put"
    /\ nsnap < MaxSnap /\ nseq < MaxSeq
    /\ snaps'   = [snaps  EXCEPT ![nsnap] = Built(cand[w], w)]
    /\ nsnap'   = nsnap + 1
    /\ nseq'    = nseq + 1
    /\ pointer' = nsnap
    /\ cver'    = candver[w] + 1          \* computed from what it read, not from cver
    /\ mine'    = [mine   EXCEPT ![w] = nsnap]
    /\ landed'  = [landed EXCEPT ![w] = landed[w] + 1]
    /\ ok'      = [ok     EXCEPT ![w] = TRUE]
    /\ pc'      = [pc     EXCEPT ![w] = "ack"]
    /\ UNCHANGED <<base, cand, candver, tries>>

\* the catalog answered; the answer may be lost on the way back
Ack(w) ==
    /\ pc[w] = "ack"
    /\ \/ pc' = [pc EXCEPT ![w] = "done"]
       \/ /\ LOSE_ACK
          /\ pc' = [pc EXCEPT ![w] = "unknown"]
    /\ UNCHANGED <<snaps, nsnap, pointer, cver, nseq, base, cand, candver, tries, ok, mine, landed>>

\* CommitStateUnknownException: the commit may or may not have landed
Unknown(w) ==
    /\ pc[w] = "unknown"
    /\ IF IDEMPOTENCY_TOKEN
         \* refresh and look for our own snapshot: the outcome becomes decidable
         THEN /\ pc' = [pc EXCEPT ![w] = IF mine[w] \in Anc(pointer) THEN "done" ELSE "prepare"]
              /\ UNCHANGED tries
         \* nothing to compare against, so the client commits again
         ELSE Retreat(w)
    /\ UNCHANGED <<snaps, nsnap, pointer, cver, nseq, base, cand, candver, ok, mine, landed>>

Step(w) ==
    \/ Plan(w) \/ WriteFiles(w) \/ Prepare(w)
    \/ Cas(w)  \/ CheckVer(w)   \/ Put(w)
    \/ Ack(w)  \/ Unknown(w)

Next == \E w \in Writers : Step(w)
Spec == Init /\ [][Next]_vars /\ \A w \in Writers : WF_vars(Step(w))

(***************************************************************************)
(* Invariants                                                              *)
(***************************************************************************)

Done      == \A w \in Writers : pc[w] \in {"done", "abort"}
Committed == {w \in Writers : ok[w] /\ mine[w] \in Anc(pointer)}

\* every commit the catalog accepted is in the table's history
NoLostCommit == \A w \in Writers : ok[w] => mine[w] \in Anc(pointer)

\* a writer's payload reaches the table at most once, even when it never
\* learned the outcome of its first attempt
NoDoubleApply == \A w \in Writers : landed[w] <= 1

\* snapshots are immutable: the root still reads as it did
ReadStability == Vis(0) = InitRows

\* the final table equals the result of running the committed writers in SOME
\* serial order. This is the real correctness property.
ApplyRows(R, w) ==
    CASE OpOf(w) = "append"  -> R \cup PayOf(w)
      [] OpOf(w) = "delete"  -> R \ PayOf(w)
      [] OpOf(w) = "rewrite" -> R

SerialOutcomes ==
    IF Committed = {} THEN {InitRows}
    ELSE IF Cardinality(Committed) = 1
      THEN { ApplyRows(InitRows, w) : w \in Committed }
      ELSE { ApplyRows(ApplyRows(InitRows, "A"), "B"),
             ApplyRows(ApplyRows(InitRows, "B"), "A") }

Serializable == Done => Vis(pointer) \in SerialOutcomes

\* what a user expects of DELETE: after it commits, the rows it named are gone.
\* Only checked when another writer committed from the same base, so a
\* violation is a concurrency effect and not a legitimate later insert.
Overlapped(w) == \E v \in Writers \ {w} : ok[v] /\ base[v] = base[w]

DeleteHolds ==
    \A w \in Writers :
        (OpOf(w) = "delete" /\ ok[w] /\ Overlapped(w) /\ mine[w] \in Anc(pointer))
            => Vis(pointer) \cap PayOf(w) = {}

\* witnesses: deliberately violated, to show the interleaving is reachable
W_BothCommit == ~ (\A w \in Writers : ok[w])
W_Done       == ~ Done        \* violated: both writers reach a terminal state
W_Retry      == \A w \in Writers : tries[w] = 0

Termination == <>[](\A w \in Writers : pc[w] \in {"done", "abort"})
=============================================================================
