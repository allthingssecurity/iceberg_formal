---- MODULE IcebergCommit_TTrace_1793620964 ----
EXTENDS Sequences, TLCExt, Toolbox, IcebergCommit, Naturals, TLC

_expression ==
    LET IcebergCommit_TEExpression == INSTANCE IcebergCommit_TEExpression
    IN IcebergCommit_TEExpression!expression
----

_trace ==
    LET IcebergCommit_TETrace == INSTANCE IcebergCommit_TETrace
    IN IcebergCommit_TETrace!trace
----

_inv ==
    ~(
        TLCGet("level") = Len(_TETrace)
        /\
        mine = ([A |-> 1, B |-> 2])
        /\
        snaps = ((0 :> [data |-> {[rows |-> {1}, seq |-> 0]}, dels |-> {}, parent |-> 0, by |-> "none"] @@ 1 :> [data |-> {[rows |-> {1}, seq |-> 0]}, dels |-> {[rows |-> {1, 2}, seq |-> 1]}, parent |-> 0, by |-> "A"] @@ 2 :> [data |-> {[rows |-> {1}, seq |-> 0], [rows |-> {2}, seq |-> 2]}, dels |-> {[rows |-> {1, 2}, seq |-> 1]}, parent |-> 1, by |-> "B"] @@ 3 :> [data |-> {}, dels |-> {}, parent |-> 0, by |-> "none"] @@ 4 :> [data |-> {}, dels |-> {}, parent |-> 0, by |-> "none"]))
        /\
        tries = ([A |-> 0, B |-> 0])
        /\
        nseq = (3)
        /\
        pointer = (2)
        /\
        candver = ([A |-> 0, B |-> 1])
        /\
        cver = (2)
        /\
        pc = ([A |-> "ack", B |-> "ack"])
        /\
        cand = ([A |-> 0, B |-> 1])
        /\
        nsnap = (3)
        /\
        ok = ([A |-> TRUE, B |-> TRUE])
        /\
        landed = ([A |-> 1, B |-> 1])
        /\
        base = ([A |-> 0, B |-> 0])
    )
----

_init ==
    /\ mine = _TETrace[1].mine
    /\ landed = _TETrace[1].landed
    /\ cand = _TETrace[1].cand
    /\ cver = _TETrace[1].cver
    /\ candver = _TETrace[1].candver
    /\ nseq = _TETrace[1].nseq
    /\ pointer = _TETrace[1].pointer
    /\ nsnap = _TETrace[1].nsnap
    /\ ok = _TETrace[1].ok
    /\ pc = _TETrace[1].pc
    /\ snaps = _TETrace[1].snaps
    /\ base = _TETrace[1].base
    /\ tries = _TETrace[1].tries
----

_next ==
    /\ \E i,j \in DOMAIN _TETrace:
        /\ \/ /\ j = i + 1
              /\ i = TLCGet("level")
        /\ mine  = _TETrace[i].mine
        /\ mine' = _TETrace[j].mine
        /\ landed  = _TETrace[i].landed
        /\ landed' = _TETrace[j].landed
        /\ cand  = _TETrace[i].cand
        /\ cand' = _TETrace[j].cand
        /\ cver  = _TETrace[i].cver
        /\ cver' = _TETrace[j].cver
        /\ candver  = _TETrace[i].candver
        /\ candver' = _TETrace[j].candver
        /\ nseq  = _TETrace[i].nseq
        /\ nseq' = _TETrace[j].nseq
        /\ pointer  = _TETrace[i].pointer
        /\ pointer' = _TETrace[j].pointer
        /\ nsnap  = _TETrace[i].nsnap
        /\ nsnap' = _TETrace[j].nsnap
        /\ ok  = _TETrace[i].ok
        /\ ok' = _TETrace[j].ok
        /\ pc  = _TETrace[i].pc
        /\ pc' = _TETrace[j].pc
        /\ snaps  = _TETrace[i].snaps
        /\ snaps' = _TETrace[j].snaps
        /\ base  = _TETrace[i].base
        /\ base' = _TETrace[j].base
        /\ tries  = _TETrace[i].tries
        /\ tries' = _TETrace[j].tries

\* Uncomment the ASSUME below to write the states of the error trace
\* to the given file in Json format. Note that you can pass any tuple
\* to `JsonSerialize`. For example, a sub-sequence of _TETrace.
    \* ASSUME
    \*     LET J == INSTANCE Json
    \*         IN J!JsonSerialize("IcebergCommit_TTrace_1793620964.json", _TETrace)

=============================================================================

 Note that you can extract this module `IcebergCommit_TEExpression`
  to a dedicated file to reuse `expression` (the module in the 
  dedicated `IcebergCommit_TEExpression.tla` file takes precedence 
  over the module `IcebergCommit_TEExpression` below).

---- MODULE IcebergCommit_TEExpression ----
EXTENDS Sequences, TLCExt, Toolbox, IcebergCommit, Naturals, TLC

expression == 
    [
        \* To hide variables of the `IcebergCommit` spec from the error trace,
        \* remove the variables below.  The trace will be written in the order
        \* of the fields of this record.
        mine |-> mine
        ,landed |-> landed
        ,cand |-> cand
        ,cver |-> cver
        ,candver |-> candver
        ,nseq |-> nseq
        ,pointer |-> pointer
        ,nsnap |-> nsnap
        ,ok |-> ok
        ,pc |-> pc
        ,snaps |-> snaps
        ,base |-> base
        ,tries |-> tries
        
        \* Put additional constant-, state-, and action-level expressions here:
        \* ,_stateNumber |-> _TEPosition
        \* ,_mineUnchanged |-> mine = mine'
        
        \* Format the `mine` variable as Json value.
        \* ,_mineJson |->
        \*     LET J == INSTANCE Json
        \*     IN J!ToJson(mine)
        
        \* Lastly, you may build expressions over arbitrary sets of states by
        \* leveraging the _TETrace operator.  For example, this is how to
        \* count the number of times a spec variable changed up to the current
        \* state in the trace.
        \* ,_mineModCount |->
        \*     LET F[s \in DOMAIN _TETrace] ==
        \*         IF s = 1 THEN 0
        \*         ELSE IF _TETrace[s].mine # _TETrace[s-1].mine
        \*             THEN 1 + F[s-1] ELSE F[s-1]
        \*     IN F[_TEPosition - 1]
    ]

=============================================================================



Parsing and semantic processing can take forever if the trace below is long.
 In this case, it is advised to uncomment the module below to deserialize the
 trace from a generated binary file.

\*
\*---- MODULE IcebergCommit_TETrace ----
\*EXTENDS IOUtils, IcebergCommit, TLC
\*
\*trace == IODeserialize("IcebergCommit_TTrace_1793620964.bin", TRUE)
\*
\*=============================================================================
\*

---- MODULE IcebergCommit_TETrace ----
EXTENDS IcebergCommit, TLC

trace == 
    <<
    ([mine |-> [A |-> 0, B |-> 0],snaps |-> (0 :> [data |-> {[rows |-> {1}, seq |-> 0]}, dels |-> {}, parent |-> 0, by |-> "none"] @@ 1 :> [data |-> {}, dels |-> {}, parent |-> 0, by |-> "none"] @@ 2 :> [data |-> {}, dels |-> {}, parent |-> 0, by |-> "none"] @@ 3 :> [data |-> {}, dels |-> {}, parent |-> 0, by |-> "none"] @@ 4 :> [data |-> {}, dels |-> {}, parent |-> 0, by |-> "none"]),tries |-> [A |-> 0, B |-> 0],nseq |-> 1,pointer |-> 0,candver |-> [A |-> 0, B |-> 0],cver |-> 0,pc |-> [A |-> "plan", B |-> "plan"],cand |-> [A |-> 0, B |-> 0],nsnap |-> 1,ok |-> [A |-> FALSE, B |-> FALSE],landed |-> [A |-> 0, B |-> 0],base |-> [A |-> 0, B |-> 0]]),
    ([mine |-> [A |-> 0, B |-> 0],snaps |-> (0 :> [data |-> {[rows |-> {1}, seq |-> 0]}, dels |-> {}, parent |-> 0, by |-> "none"] @@ 1 :> [data |-> {}, dels |-> {}, parent |-> 0, by |-> "none"] @@ 2 :> [data |-> {}, dels |-> {}, parent |-> 0, by |-> "none"] @@ 3 :> [data |-> {}, dels |-> {}, parent |-> 0, by |-> "none"] @@ 4 :> [data |-> {}, dels |-> {}, parent |-> 0, by |-> "none"]),tries |-> [A |-> 0, B |-> 0],nseq |-> 1,pointer |-> 0,candver |-> [A |-> 0, B |-> 0],cver |-> 0,pc |-> [A |-> "write", B |-> "plan"],cand |-> [A |-> 0, B |-> 0],nsnap |-> 1,ok |-> [A |-> FALSE, B |-> FALSE],landed |-> [A |-> 0, B |-> 0],base |-> [A |-> 0, B |-> 0]]),
    ([mine |-> [A |-> 0, B |-> 0],snaps |-> (0 :> [data |-> {[rows |-> {1}, seq |-> 0]}, dels |-> {}, parent |-> 0, by |-> "none"] @@ 1 :> [data |-> {}, dels |-> {}, parent |-> 0, by |-> "none"] @@ 2 :> [data |-> {}, dels |-> {}, parent |-> 0, by |-> "none"] @@ 3 :> [data |-> {}, dels |-> {}, parent |-> 0, by |-> "none"] @@ 4 :> [data |-> {}, dels |-> {}, parent |-> 0, by |-> "none"]),tries |-> [A |-> 0, B |-> 0],nseq |-> 1,pointer |-> 0,candver |-> [A |-> 0, B |-> 0],cver |-> 0,pc |-> [A |-> "write", B |-> "write"],cand |-> [A |-> 0, B |-> 0],nsnap |-> 1,ok |-> [A |-> FALSE, B |-> FALSE],landed |-> [A |-> 0, B |-> 0],base |-> [A |-> 0, B |-> 0]]),
    ([mine |-> [A |-> 0, B |-> 0],snaps |-> (0 :> [data |-> {[rows |-> {1}, seq |-> 0]}, dels |-> {}, parent |-> 0, by |-> "none"] @@ 1 :> [data |-> {}, dels |-> {}, parent |-> 0, by |-> "none"] @@ 2 :> [data |-> {}, dels |-> {}, parent |-> 0, by |-> "none"] @@ 3 :> [data |-> {}, dels |-> {}, parent |-> 0, by |-> "none"] @@ 4 :> [data |-> {}, dels |-> {}, parent |-> 0, by |-> "none"]),tries |-> [A |-> 0, B |-> 0],nseq |-> 1,pointer |-> 0,candver |-> [A |-> 0, B |-> 0],cver |-> 0,pc |-> [A |-> "prepare", B |-> "write"],cand |-> [A |-> 0, B |-> 0],nsnap |-> 1,ok |-> [A |-> FALSE, B |-> FALSE],landed |-> [A |-> 0, B |-> 0],base |-> [A |-> 0, B |-> 0]]),
    ([mine |-> [A |-> 0, B |-> 0],snaps |-> (0 :> [data |-> {[rows |-> {1}, seq |-> 0]}, dels |-> {}, parent |-> 0, by |-> "none"] @@ 1 :> [data |-> {}, dels |-> {}, parent |-> 0, by |-> "none"] @@ 2 :> [data |-> {}, dels |-> {}, parent |-> 0, by |-> "none"] @@ 3 :> [data |-> {}, dels |-> {}, parent |-> 0, by |-> "none"] @@ 4 :> [data |-> {}, dels |-> {}, parent |-> 0, by |-> "none"]),tries |-> [A |-> 0, B |-> 0],nseq |-> 1,pointer |-> 0,candver |-> [A |-> 0, B |-> 0],cver |-> 0,pc |-> [A |-> "prepare", B |-> "prepare"],cand |-> [A |-> 0, B |-> 0],nsnap |-> 1,ok |-> [A |-> FALSE, B |-> FALSE],landed |-> [A |-> 0, B |-> 0],base |-> [A |-> 0, B |-> 0]]),
    ([mine |-> [A |-> 0, B |-> 0],snaps |-> (0 :> [data |-> {[rows |-> {1}, seq |-> 0]}, dels |-> {}, parent |-> 0, by |-> "none"] @@ 1 :> [data |-> {}, dels |-> {}, parent |-> 0, by |-> "none"] @@ 2 :> [data |-> {}, dels |-> {}, parent |-> 0, by |-> "none"] @@ 3 :> [data |-> {}, dels |-> {}, parent |-> 0, by |-> "none"] @@ 4 :> [data |-> {}, dels |-> {}, parent |-> 0, by |-> "none"]),tries |-> [A |-> 0, B |-> 0],nseq |-> 1,pointer |-> 0,candver |-> [A |-> 0, B |-> 0],cver |-> 0,pc |-> [A |-> "cas", B |-> "prepare"],cand |-> [A |-> 0, B |-> 0],nsnap |-> 1,ok |-> [A |-> FALSE, B |-> FALSE],landed |-> [A |-> 0, B |-> 0],base |-> [A |-> 0, B |-> 0]]),
    ([mine |-> [A |-> 1, B |-> 0],snaps |-> (0 :> [data |-> {[rows |-> {1}, seq |-> 0]}, dels |-> {}, parent |-> 0, by |-> "none"] @@ 1 :> [data |-> {[rows |-> {1}, seq |-> 0]}, dels |-> {[rows |-> {1, 2}, seq |-> 1]}, parent |-> 0, by |-> "A"] @@ 2 :> [data |-> {}, dels |-> {}, parent |-> 0, by |-> "none"] @@ 3 :> [data |-> {}, dels |-> {}, parent |-> 0, by |-> "none"] @@ 4 :> [data |-> {}, dels |-> {}, parent |-> 0, by |-> "none"]),tries |-> [A |-> 0, B |-> 0],nseq |-> 2,pointer |-> 1,candver |-> [A |-> 0, B |-> 0],cver |-> 1,pc |-> [A |-> "ack", B |-> "prepare"],cand |-> [A |-> 0, B |-> 0],nsnap |-> 2,ok |-> [A |-> TRUE, B |-> FALSE],landed |-> [A |-> 1, B |-> 0],base |-> [A |-> 0, B |-> 0]]),
    ([mine |-> [A |-> 1, B |-> 0],snaps |-> (0 :> [data |-> {[rows |-> {1}, seq |-> 0]}, dels |-> {}, parent |-> 0, by |-> "none"] @@ 1 :> [data |-> {[rows |-> {1}, seq |-> 0]}, dels |-> {[rows |-> {1, 2}, seq |-> 1]}, parent |-> 0, by |-> "A"] @@ 2 :> [data |-> {}, dels |-> {}, parent |-> 0, by |-> "none"] @@ 3 :> [data |-> {}, dels |-> {}, parent |-> 0, by |-> "none"] @@ 4 :> [data |-> {}, dels |-> {}, parent |-> 0, by |-> "none"]),tries |-> [A |-> 0, B |-> 0],nseq |-> 2,pointer |-> 1,candver |-> [A |-> 0, B |-> 1],cver |-> 1,pc |-> [A |-> "ack", B |-> "cas"],cand |-> [A |-> 0, B |-> 1],nsnap |-> 2,ok |-> [A |-> TRUE, B |-> FALSE],landed |-> [A |-> 1, B |-> 0],base |-> [A |-> 0, B |-> 0]]),
    ([mine |-> [A |-> 1, B |-> 2],snaps |-> (0 :> [data |-> {[rows |-> {1}, seq |-> 0]}, dels |-> {}, parent |-> 0, by |-> "none"] @@ 1 :> [data |-> {[rows |-> {1}, seq |-> 0]}, dels |-> {[rows |-> {1, 2}, seq |-> 1]}, parent |-> 0, by |-> "A"] @@ 2 :> [data |-> {[rows |-> {1}, seq |-> 0], [rows |-> {2}, seq |-> 2]}, dels |-> {[rows |-> {1, 2}, seq |-> 1]}, parent |-> 1, by |-> "B"] @@ 3 :> [data |-> {}, dels |-> {}, parent |-> 0, by |-> "none"] @@ 4 :> [data |-> {}, dels |-> {}, parent |-> 0, by |-> "none"]),tries |-> [A |-> 0, B |-> 0],nseq |-> 3,pointer |-> 2,candver |-> [A |-> 0, B |-> 1],cver |-> 2,pc |-> [A |-> "ack", B |-> "ack"],cand |-> [A |-> 0, B |-> 1],nsnap |-> 3,ok |-> [A |-> TRUE, B |-> TRUE],landed |-> [A |-> 1, B |-> 1],base |-> [A |-> 0, B |-> 0]])
    >>
----


=============================================================================

---- CONFIG IcebergCommit_TTrace_1793620964 ----
CONSTANTS
    OpA = "delete"
    OpB = "append"
    PayA = { 1 , 2 }
    PayB = { 2 }
    InitRows = { 1 }
    CATALOG = "cas"
    ISOLATION = "snapshot"
    REWRITE_KEEPS_SEQ = FALSE
    VALIDATE_REWRITE_DELETES = FALSE
    LOSE_ACK = FALSE
    IDEMPOTENCY_TOKEN = FALSE
    MaxRetry = 1
    MaxSnap = 4
    MaxSeq = 5

INVARIANT
    _inv

CHECK_DEADLOCK
    \* CHECK_DEADLOCK off because of PROPERTY or INVARIANT above.
    FALSE

INIT
    _init

NEXT
    _next

CONSTANT
    _TETrace <- _trace

ALIAS
    _expression
=============================================================================
\* Generated on Mon Nov 02 17:32:45 IST 2026