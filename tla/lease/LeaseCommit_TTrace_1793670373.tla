---- MODULE LeaseCommit_TTrace_1793670373 ----
EXTENDS Sequences, TLCExt, Toolbox, Naturals, TLC, LeaseCommit

_expression ==
    LET LeaseCommit_TEExpression == INSTANCE LeaseCommit_TEExpression
    IN LeaseCommit_TEExpression!expression
----

_trace ==
    LET LeaseCommit_TETrace == INSTANCE LeaseCommit_TETrace
    IN LeaseCommit_TETrace!trace
----

_inv ==
    ~(
        TLCGet("level") = Len(_TETrace)
        /\
        baseC = ([A |-> {}, B |-> {"A"}])
        /\
        tries = ([A |-> 0, B |-> 0])
        /\
        paused = ([A |-> FALSE, B |-> FALSE])
        /\
        ver = (2)
        /\
        npause = ([A |-> 0, B |-> 0])
        /\
        obsVer = ([A |-> 0, B |-> 0])
        /\
        lastHb = ([A |-> 0, B |-> 0])
        /\
        hbOn = ([A |-> FALSE, B |-> FALSE])
        /\
        holder = ("none")
        /\
        content = ({"A", "B"})
        /\
        ptr = (2)
        /\
        hbFailed = ([A |-> FALSE, B |-> FALSE])
        /\
        pc = ([A |-> "end", B |-> "end"])
        /\
        now = (0)
        /\
        obsAt = ([A |-> 0, B |-> 0])
        /\
        base = ([A |-> 0, B |-> 1])
        /\
        status = ([A |-> "success", B |-> "success"])
    )
----

_init ==
    /\ hbOn = _TETrace[1].hbOn
    /\ paused = _TETrace[1].paused
    /\ content = _TETrace[1].content
    /\ obsAt = _TETrace[1].obsAt
    /\ now = _TETrace[1].now
    /\ pc = _TETrace[1].pc
    /\ npause = _TETrace[1].npause
    /\ obsVer = _TETrace[1].obsVer
    /\ holder = _TETrace[1].holder
    /\ baseC = _TETrace[1].baseC
    /\ ver = _TETrace[1].ver
    /\ ptr = _TETrace[1].ptr
    /\ base = _TETrace[1].base
    /\ lastHb = _TETrace[1].lastHb
    /\ hbFailed = _TETrace[1].hbFailed
    /\ status = _TETrace[1].status
    /\ tries = _TETrace[1].tries
----

_next ==
    /\ \E i,j \in DOMAIN _TETrace:
        /\ \/ /\ j = i + 1
              /\ i = TLCGet("level")
        /\ hbOn  = _TETrace[i].hbOn
        /\ hbOn' = _TETrace[j].hbOn
        /\ paused  = _TETrace[i].paused
        /\ paused' = _TETrace[j].paused
        /\ content  = _TETrace[i].content
        /\ content' = _TETrace[j].content
        /\ obsAt  = _TETrace[i].obsAt
        /\ obsAt' = _TETrace[j].obsAt
        /\ now  = _TETrace[i].now
        /\ now' = _TETrace[j].now
        /\ pc  = _TETrace[i].pc
        /\ pc' = _TETrace[j].pc
        /\ npause  = _TETrace[i].npause
        /\ npause' = _TETrace[j].npause
        /\ obsVer  = _TETrace[i].obsVer
        /\ obsVer' = _TETrace[j].obsVer
        /\ holder  = _TETrace[i].holder
        /\ holder' = _TETrace[j].holder
        /\ baseC  = _TETrace[i].baseC
        /\ baseC' = _TETrace[j].baseC
        /\ ver  = _TETrace[i].ver
        /\ ver' = _TETrace[j].ver
        /\ ptr  = _TETrace[i].ptr
        /\ ptr' = _TETrace[j].ptr
        /\ base  = _TETrace[i].base
        /\ base' = _TETrace[j].base
        /\ lastHb  = _TETrace[i].lastHb
        /\ lastHb' = _TETrace[j].lastHb
        /\ hbFailed  = _TETrace[i].hbFailed
        /\ hbFailed' = _TETrace[j].hbFailed
        /\ status  = _TETrace[i].status
        /\ status' = _TETrace[j].status
        /\ tries  = _TETrace[i].tries
        /\ tries' = _TETrace[j].tries

\* Uncomment the ASSUME below to write the states of the error trace
\* to the given file in Json format. Note that you can pass any tuple
\* to `JsonSerialize`. For example, a sub-sequence of _TETrace.
    \* ASSUME
    \*     LET J == INSTANCE Json
    \*         IN J!JsonSerialize("LeaseCommit_TTrace_1793670373.json", _TETrace)

=============================================================================

 Note that you can extract this module `LeaseCommit_TEExpression`
  to a dedicated file to reuse `expression` (the module in the 
  dedicated `LeaseCommit_TEExpression.tla` file takes precedence 
  over the module `LeaseCommit_TEExpression` below).

---- MODULE LeaseCommit_TEExpression ----
EXTENDS Sequences, TLCExt, Toolbox, Naturals, TLC, LeaseCommit

expression == 
    [
        \* To hide variables of the `LeaseCommit` spec from the error trace,
        \* remove the variables below.  The trace will be written in the order
        \* of the fields of this record.
        hbOn |-> hbOn
        ,paused |-> paused
        ,content |-> content
        ,obsAt |-> obsAt
        ,now |-> now
        ,pc |-> pc
        ,npause |-> npause
        ,obsVer |-> obsVer
        ,holder |-> holder
        ,baseC |-> baseC
        ,ver |-> ver
        ,ptr |-> ptr
        ,base |-> base
        ,lastHb |-> lastHb
        ,hbFailed |-> hbFailed
        ,status |-> status
        ,tries |-> tries
        
        \* Put additional constant-, state-, and action-level expressions here:
        \* ,_stateNumber |-> _TEPosition
        \* ,_hbOnUnchanged |-> hbOn = hbOn'
        
        \* Format the `hbOn` variable as Json value.
        \* ,_hbOnJson |->
        \*     LET J == INSTANCE Json
        \*     IN J!ToJson(hbOn)
        
        \* Lastly, you may build expressions over arbitrary sets of states by
        \* leveraging the _TETrace operator.  For example, this is how to
        \* count the number of times a spec variable changed up to the current
        \* state in the trace.
        \* ,_hbOnModCount |->
        \*     LET F[s \in DOMAIN _TETrace] ==
        \*         IF s = 1 THEN 0
        \*         ELSE IF _TETrace[s].hbOn # _TETrace[s-1].hbOn
        \*             THEN 1 + F[s-1] ELSE F[s-1]
        \*     IN F[_TEPosition - 1]
    ]

=============================================================================



Parsing and semantic processing can take forever if the trace below is long.
 In this case, it is advised to uncomment the module below to deserialize the
 trace from a generated binary file.

\*
\*---- MODULE LeaseCommit_TETrace ----
\*EXTENDS IOUtils, TLC, LeaseCommit
\*
\*trace == IODeserialize("LeaseCommit_TTrace_1793670373.bin", TRUE)
\*
\*=============================================================================
\*

---- MODULE LeaseCommit_TETrace ----
EXTENDS TLC, LeaseCommit

trace == 
    <<
    ([baseC |-> [A |-> {}, B |-> {}],tries |-> [A |-> 0, B |-> 0],paused |-> [A |-> FALSE, B |-> FALSE],ver |-> 0,npause |-> [A |-> 0, B |-> 0],obsVer |-> [A |-> 0, B |-> 0],lastHb |-> [A |-> 0, B |-> 0],hbOn |-> [A |-> FALSE, B |-> FALSE],holder |-> "none",content |-> {},ptr |-> 0,hbFailed |-> [A |-> FALSE, B |-> FALSE],pc |-> [A |-> "start", B |-> "start"],now |-> 0,obsAt |-> [A |-> 0, B |-> 0],base |-> [A |-> 0, B |-> 0],status |-> [A |-> "running", B |-> "running"]]),
    ([baseC |-> [A |-> {}, B |-> {}],tries |-> [A |-> 0, B |-> 0],paused |-> [A |-> FALSE, B |-> FALSE],ver |-> 0,npause |-> [A |-> 0, B |-> 0],obsVer |-> [A |-> 0, B |-> 0],lastHb |-> [A |-> 0, B |-> 0],hbOn |-> [A |-> FALSE, B |-> FALSE],holder |-> "none",content |-> {},ptr |-> 0,hbFailed |-> [A |-> FALSE, B |-> FALSE],pc |-> [A |-> "dacq", B |-> "start"],now |-> 0,obsAt |-> [A |-> 0, B |-> 0],base |-> [A |-> 0, B |-> 0],status |-> [A |-> "running", B |-> "running"]]),
    ([baseC |-> [A |-> {}, B |-> {}],tries |-> [A |-> 0, B |-> 0],paused |-> [A |-> FALSE, B |-> FALSE],ver |-> 1,npause |-> [A |-> 0, B |-> 0],obsVer |-> [A |-> 0, B |-> 0],lastHb |-> [A |-> 0, B |-> 0],hbOn |-> [A |-> TRUE, B |-> FALSE],holder |-> "A",content |-> {},ptr |-> 0,hbFailed |-> [A |-> FALSE, B |-> FALSE],pc |-> [A |-> "check", B |-> "start"],now |-> 0,obsAt |-> [A |-> 0, B |-> 0],base |-> [A |-> 0, B |-> 0],status |-> [A |-> "running", B |-> "running"]]),
    ([baseC |-> [A |-> {}, B |-> {}],tries |-> [A |-> 0, B |-> 0],paused |-> [A |-> FALSE, B |-> FALSE],ver |-> 1,npause |-> [A |-> 0, B |-> 0],obsVer |-> [A |-> 0, B |-> 0],lastHb |-> [A |-> 0, B |-> 0],hbOn |-> [A |-> TRUE, B |-> FALSE],holder |-> "A",content |-> {},ptr |-> 0,hbFailed |-> [A |-> FALSE, B |-> FALSE],pc |-> [A |-> "write", B |-> "start"],now |-> 0,obsAt |-> [A |-> 0, B |-> 0],base |-> [A |-> 0, B |-> 0],status |-> [A |-> "running", B |-> "running"]]),
    ([baseC |-> [A |-> {}, B |-> {}],tries |-> [A |-> 0, B |-> 0],paused |-> [A |-> FALSE, B |-> FALSE],ver |-> 1,npause |-> [A |-> 0, B |-> 0],obsVer |-> [A |-> 0, B |-> 0],lastHb |-> [A |-> 0, B |-> 0],hbOn |-> [A |-> FALSE, B |-> FALSE],holder |-> "none",content |-> {"A"},ptr |-> 1,hbFailed |-> [A |-> FALSE, B |-> FALSE],pc |-> [A |-> "end", B |-> "start"],now |-> 0,obsAt |-> [A |-> 0, B |-> 0],base |-> [A |-> 0, B |-> 0],status |-> [A |-> "success", B |-> "running"]]),
    ([baseC |-> [A |-> {}, B |-> {"A"}],tries |-> [A |-> 0, B |-> 0],paused |-> [A |-> FALSE, B |-> FALSE],ver |-> 1,npause |-> [A |-> 0, B |-> 0],obsVer |-> [A |-> 0, B |-> 0],lastHb |-> [A |-> 0, B |-> 0],hbOn |-> [A |-> FALSE, B |-> FALSE],holder |-> "none",content |-> {"A"},ptr |-> 1,hbFailed |-> [A |-> FALSE, B |-> FALSE],pc |-> [A |-> "end", B |-> "dacq"],now |-> 0,obsAt |-> [A |-> 0, B |-> 0],base |-> [A |-> 0, B |-> 1],status |-> [A |-> "success", B |-> "running"]]),
    ([baseC |-> [A |-> {}, B |-> {"A"}],tries |-> [A |-> 0, B |-> 0],paused |-> [A |-> FALSE, B |-> FALSE],ver |-> 2,npause |-> [A |-> 0, B |-> 0],obsVer |-> [A |-> 0, B |-> 0],lastHb |-> [A |-> 0, B |-> 0],hbOn |-> [A |-> FALSE, B |-> TRUE],holder |-> "B",content |-> {"A"},ptr |-> 1,hbFailed |-> [A |-> FALSE, B |-> FALSE],pc |-> [A |-> "end", B |-> "check"],now |-> 0,obsAt |-> [A |-> 0, B |-> 0],base |-> [A |-> 0, B |-> 1],status |-> [A |-> "success", B |-> "running"]]),
    ([baseC |-> [A |-> {}, B |-> {"A"}],tries |-> [A |-> 0, B |-> 0],paused |-> [A |-> FALSE, B |-> FALSE],ver |-> 2,npause |-> [A |-> 0, B |-> 0],obsVer |-> [A |-> 0, B |-> 0],lastHb |-> [A |-> 0, B |-> 0],hbOn |-> [A |-> FALSE, B |-> TRUE],holder |-> "B",content |-> {"A"},ptr |-> 1,hbFailed |-> [A |-> FALSE, B |-> FALSE],pc |-> [A |-> "end", B |-> "write"],now |-> 0,obsAt |-> [A |-> 0, B |-> 0],base |-> [A |-> 0, B |-> 1],status |-> [A |-> "success", B |-> "running"]]),
    ([baseC |-> [A |-> {}, B |-> {"A"}],tries |-> [A |-> 0, B |-> 0],paused |-> [A |-> FALSE, B |-> FALSE],ver |-> 2,npause |-> [A |-> 0, B |-> 0],obsVer |-> [A |-> 0, B |-> 0],lastHb |-> [A |-> 0, B |-> 0],hbOn |-> [A |-> FALSE, B |-> FALSE],holder |-> "none",content |-> {"A", "B"},ptr |-> 2,hbFailed |-> [A |-> FALSE, B |-> FALSE],pc |-> [A |-> "end", B |-> "end"],now |-> 0,obsAt |-> [A |-> 0, B |-> 0],base |-> [A |-> 0, B |-> 1],status |-> [A |-> "success", B |-> "success"]])
    >>
----


=============================================================================

---- CONFIG LeaseCommit_TTrace_1793670373 ----
CONSTANTS
    LOCK = "dynamo"
    CAS = FALSE
    ALLOW_PAUSE = FALSE
    HB = 1
    T = 2
    LEASE = 2
    MaxTime = 7
    MaxRetry = 1

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
\* Generated on Tue Nov 03 07:16:13 IST 2026