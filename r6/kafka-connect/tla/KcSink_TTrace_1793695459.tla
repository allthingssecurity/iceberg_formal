---- MODULE KcSink_TTrace_1793695459 ----
EXTENDS Sequences, TLCExt, Toolbox, Naturals, TLC, KcSink

_expression ==
    LET KcSink_TEExpression == INSTANCE KcSink_TEExpression
    IN KcSink_TEExpression!expression
----

_trace ==
    LET KcSink_TETrace == INSTANCE KcSink_TETrace
    IN KcSink_TETrace!trace
----

_inv ==
    ~(
        TLCGet("level") = Len(_TETrace)
        /\
        rewinds = (0)
        /\
        ccnt = (0)
        /\
        cgo = (0)
        /\
        crashes = (0)
        /\
        coord = (1)
        /\
        ccur = (1)
        /\
        committer = (<<TRUE, TRUE>>)
        /\
        pos = (<<(0 :> 2 @@ 1 :> 0), (0 :> 0 @@ 1 :> 0)>>)
        /\
        ctl = (<<[k |-> "SC", cid |-> 1], [k |-> "DW", recs |-> {<<0, 1>>}, cid |-> 1], [k |-> "DC", cid |-> 1, n |-> 1]>>)
        /\
        up = (<<TRUE, TRUE>>)
        /\
        worker = (<<TRUE, FALSE>>)
        /\
        nextCid = (2)
        /\
        cbuf = ({})
        /\
        fails = (0)
        /\
        table = ((<<0, 0>> :> 0 @@ <<0, 1>> :> 0 @@ <<1, 0>> :> 0 @@ <<1, 1>> :> 0))
        /\
        cto = (0)
        /\
        owner = ((0 :> 1 @@ 1 :> 0))
        /\
        srcC = ((0 :> 2 @@ 1 :> -1))
        /\
        wbuf = (<<{}, {}>>)
        /\
        tOff = (0)
        /\
        woff = (<<(0 :> -1 @@ 1 :> -1), (0 :> -1 @@ 1 :> -1)>>)
        /\
        wseen = (<<1, 0>>)
        /\
        cphase = ("idle")
        /\
        moves = (1)
        /\
        cpos = (1)
    )
----

_init ==
    /\ cgo = _TETrace[1].cgo
    /\ ctl = _TETrace[1].ctl
    /\ cto = _TETrace[1].cto
    /\ ccnt = _TETrace[1].ccnt
    /\ cbuf = _TETrace[1].cbuf
    /\ moves = _TETrace[1].moves
    /\ fails = _TETrace[1].fails
    /\ woff = _TETrace[1].woff
    /\ srcC = _TETrace[1].srcC
    /\ pos = _TETrace[1].pos
    /\ cphase = _TETrace[1].cphase
    /\ wseen = _TETrace[1].wseen
    /\ table = _TETrace[1].table
    /\ tOff = _TETrace[1].tOff
    /\ crashes = _TETrace[1].crashes
    /\ ccur = _TETrace[1].ccur
    /\ rewinds = _TETrace[1].rewinds
    /\ committer = _TETrace[1].committer
    /\ up = _TETrace[1].up
    /\ wbuf = _TETrace[1].wbuf
    /\ cpos = _TETrace[1].cpos
    /\ coord = _TETrace[1].coord
    /\ worker = _TETrace[1].worker
    /\ owner = _TETrace[1].owner
    /\ nextCid = _TETrace[1].nextCid
----

_next ==
    /\ \E i,j \in DOMAIN _TETrace:
        /\ \/ /\ j = i + 1
              /\ i = TLCGet("level")
        /\ cgo  = _TETrace[i].cgo
        /\ cgo' = _TETrace[j].cgo
        /\ ctl  = _TETrace[i].ctl
        /\ ctl' = _TETrace[j].ctl
        /\ cto  = _TETrace[i].cto
        /\ cto' = _TETrace[j].cto
        /\ ccnt  = _TETrace[i].ccnt
        /\ ccnt' = _TETrace[j].ccnt
        /\ cbuf  = _TETrace[i].cbuf
        /\ cbuf' = _TETrace[j].cbuf
        /\ moves  = _TETrace[i].moves
        /\ moves' = _TETrace[j].moves
        /\ fails  = _TETrace[i].fails
        /\ fails' = _TETrace[j].fails
        /\ woff  = _TETrace[i].woff
        /\ woff' = _TETrace[j].woff
        /\ srcC  = _TETrace[i].srcC
        /\ srcC' = _TETrace[j].srcC
        /\ pos  = _TETrace[i].pos
        /\ pos' = _TETrace[j].pos
        /\ cphase  = _TETrace[i].cphase
        /\ cphase' = _TETrace[j].cphase
        /\ wseen  = _TETrace[i].wseen
        /\ wseen' = _TETrace[j].wseen
        /\ table  = _TETrace[i].table
        /\ table' = _TETrace[j].table
        /\ tOff  = _TETrace[i].tOff
        /\ tOff' = _TETrace[j].tOff
        /\ crashes  = _TETrace[i].crashes
        /\ crashes' = _TETrace[j].crashes
        /\ ccur  = _TETrace[i].ccur
        /\ ccur' = _TETrace[j].ccur
        /\ rewinds  = _TETrace[i].rewinds
        /\ rewinds' = _TETrace[j].rewinds
        /\ committer  = _TETrace[i].committer
        /\ committer' = _TETrace[j].committer
        /\ up  = _TETrace[i].up
        /\ up' = _TETrace[j].up
        /\ wbuf  = _TETrace[i].wbuf
        /\ wbuf' = _TETrace[j].wbuf
        /\ cpos  = _TETrace[i].cpos
        /\ cpos' = _TETrace[j].cpos
        /\ coord  = _TETrace[i].coord
        /\ coord' = _TETrace[j].coord
        /\ worker  = _TETrace[i].worker
        /\ worker' = _TETrace[j].worker
        /\ owner  = _TETrace[i].owner
        /\ owner' = _TETrace[j].owner
        /\ nextCid  = _TETrace[i].nextCid
        /\ nextCid' = _TETrace[j].nextCid

\* Uncomment the ASSUME below to write the states of the error trace
\* to the given file in Json format. Note that you can pass any tuple
\* to `JsonSerialize`. For example, a sub-sequence of _TETrace.
    \* ASSUME
    \*     LET J == INSTANCE Json
    \*         IN J!JsonSerialize("KcSink_TTrace_1793695459.json", _TETrace)

=============================================================================

 Note that you can extract this module `KcSink_TEExpression`
  to a dedicated file to reuse `expression` (the module in the 
  dedicated `KcSink_TEExpression.tla` file takes precedence 
  over the module `KcSink_TEExpression` below).

---- MODULE KcSink_TEExpression ----
EXTENDS Sequences, TLCExt, Toolbox, Naturals, TLC, KcSink

expression == 
    [
        \* To hide variables of the `KcSink` spec from the error trace,
        \* remove the variables below.  The trace will be written in the order
        \* of the fields of this record.
        cgo |-> cgo
        ,ctl |-> ctl
        ,cto |-> cto
        ,ccnt |-> ccnt
        ,cbuf |-> cbuf
        ,moves |-> moves
        ,fails |-> fails
        ,woff |-> woff
        ,srcC |-> srcC
        ,pos |-> pos
        ,cphase |-> cphase
        ,wseen |-> wseen
        ,table |-> table
        ,tOff |-> tOff
        ,crashes |-> crashes
        ,ccur |-> ccur
        ,rewinds |-> rewinds
        ,committer |-> committer
        ,up |-> up
        ,wbuf |-> wbuf
        ,cpos |-> cpos
        ,coord |-> coord
        ,worker |-> worker
        ,owner |-> owner
        ,nextCid |-> nextCid
        
        \* Put additional constant-, state-, and action-level expressions here:
        \* ,_stateNumber |-> _TEPosition
        \* ,_cgoUnchanged |-> cgo = cgo'
        
        \* Format the `cgo` variable as Json value.
        \* ,_cgoJson |->
        \*     LET J == INSTANCE Json
        \*     IN J!ToJson(cgo)
        
        \* Lastly, you may build expressions over arbitrary sets of states by
        \* leveraging the _TETrace operator.  For example, this is how to
        \* count the number of times a spec variable changed up to the current
        \* state in the trace.
        \* ,_cgoModCount |->
        \*     LET F[s \in DOMAIN _TETrace] ==
        \*         IF s = 1 THEN 0
        \*         ELSE IF _TETrace[s].cgo # _TETrace[s-1].cgo
        \*             THEN 1 + F[s-1] ELSE F[s-1]
        \*     IN F[_TEPosition - 1]
    ]

=============================================================================



Parsing and semantic processing can take forever if the trace below is long.
 In this case, it is advised to uncomment the module below to deserialize the
 trace from a generated binary file.

\*
\*---- MODULE KcSink_TETrace ----
\*EXTENDS IOUtils, TLC, KcSink
\*
\*trace == IODeserialize("KcSink_TTrace_1793695459.bin", TRUE)
\*
\*=============================================================================
\*

---- MODULE KcSink_TETrace ----
EXTENDS TLC, KcSink

trace == 
    <<
    ([rewinds |-> 0,ccnt |-> 0,cgo |-> 0,crashes |-> 0,coord |-> 1,ccur |-> 0,committer |-> <<TRUE, TRUE>>,pos |-> <<(0 :> 0 @@ 1 :> 0), (0 :> 0 @@ 1 :> 0)>>,ctl |-> <<>>,up |-> <<TRUE, TRUE>>,worker |-> <<FALSE, FALSE>>,nextCid |-> 1,cbuf |-> {},fails |-> 0,table |-> (<<0, 0>> :> 0 @@ <<0, 1>> :> 0 @@ <<1, 0>> :> 0 @@ <<1, 1>> :> 0),cto |-> 0,owner |-> (0 :> 1 @@ 1 :> 1),srcC |-> (0 :> -1 @@ 1 :> -1),wbuf |-> <<{}, {}>>,tOff |-> 0,woff |-> <<(0 :> -1 @@ 1 :> -1), (0 :> -1 @@ 1 :> -1)>>,wseen |-> <<0, 0>>,cphase |-> "idle",moves |-> 0,cpos |-> 1]),
    ([rewinds |-> 0,ccnt |-> 0,cgo |-> 0,crashes |-> 0,coord |-> 1,ccur |-> 0,committer |-> <<TRUE, TRUE>>,pos |-> <<(0 :> 1 @@ 1 :> 0), (0 :> 0 @@ 1 :> 0)>>,ctl |-> <<>>,up |-> <<TRUE, TRUE>>,worker |-> <<TRUE, FALSE>>,nextCid |-> 1,cbuf |-> {},fails |-> 0,table |-> (<<0, 0>> :> 0 @@ <<0, 1>> :> 0 @@ <<1, 0>> :> 0 @@ <<1, 1>> :> 0),cto |-> 0,owner |-> (0 :> 1 @@ 1 :> 1),srcC |-> (0 :> -1 @@ 1 :> -1),wbuf |-> <<{<<0, 0>>}, {}>>,tOff |-> 0,woff |-> <<(0 :> 1 @@ 1 :> -1), (0 :> -1 @@ 1 :> -1)>>,wseen |-> <<0, 0>>,cphase |-> "idle",moves |-> 0,cpos |-> 1]),
    ([rewinds |-> 0,ccnt |-> 0,cgo |-> 0,crashes |-> 0,coord |-> 1,ccur |-> 0,committer |-> <<TRUE, TRUE>>,pos |-> <<(0 :> 1 @@ 1 :> 0), (0 :> 0 @@ 1 :> 0)>>,ctl |-> <<>>,up |-> <<TRUE, TRUE>>,worker |-> <<FALSE, FALSE>>,nextCid |-> 1,cbuf |-> {},fails |-> 0,table |-> (<<0, 0>> :> 0 @@ <<0, 1>> :> 0 @@ <<1, 0>> :> 0 @@ <<1, 1>> :> 0),cto |-> 0,owner |-> (0 :> 1 @@ 1 :> 0),srcC |-> (0 :> -1 @@ 1 :> -1),wbuf |-> <<{}, {}>>,tOff |-> 0,woff |-> <<(0 :> -1 @@ 1 :> -1), (0 :> -1 @@ 1 :> -1)>>,wseen |-> <<0, 0>>,cphase |-> "idle",moves |-> 1,cpos |-> 1]),
    ([rewinds |-> 0,ccnt |-> 0,cgo |-> 0,crashes |-> 0,coord |-> 1,ccur |-> 0,committer |-> <<TRUE, TRUE>>,pos |-> <<(0 :> 2 @@ 1 :> 0), (0 :> 0 @@ 1 :> 0)>>,ctl |-> <<>>,up |-> <<TRUE, TRUE>>,worker |-> <<TRUE, FALSE>>,nextCid |-> 1,cbuf |-> {},fails |-> 0,table |-> (<<0, 0>> :> 0 @@ <<0, 1>> :> 0 @@ <<1, 0>> :> 0 @@ <<1, 1>> :> 0),cto |-> 0,owner |-> (0 :> 1 @@ 1 :> 0),srcC |-> (0 :> -1 @@ 1 :> -1),wbuf |-> <<{<<0, 1>>}, {}>>,tOff |-> 0,woff |-> <<(0 :> 2 @@ 1 :> -1), (0 :> -1 @@ 1 :> -1)>>,wseen |-> <<0, 0>>,cphase |-> "idle",moves |-> 1,cpos |-> 1]),
    ([rewinds |-> 0,ccnt |-> 0,cgo |-> 0,crashes |-> 0,coord |-> 1,ccur |-> 1,committer |-> <<TRUE, TRUE>>,pos |-> <<(0 :> 2 @@ 1 :> 0), (0 :> 0 @@ 1 :> 0)>>,ctl |-> <<[k |-> "SC", cid |-> 1]>>,up |-> <<TRUE, TRUE>>,worker |-> <<TRUE, FALSE>>,nextCid |-> 2,cbuf |-> {},fails |-> 0,table |-> (<<0, 0>> :> 0 @@ <<0, 1>> :> 0 @@ <<1, 0>> :> 0 @@ <<1, 1>> :> 0),cto |-> 0,owner |-> (0 :> 1 @@ 1 :> 0),srcC |-> (0 :> -1 @@ 1 :> -1),wbuf |-> <<{<<0, 1>>}, {}>>,tOff |-> 0,woff |-> <<(0 :> 2 @@ 1 :> -1), (0 :> -1 @@ 1 :> -1)>>,wseen |-> <<0, 0>>,cphase |-> "idle",moves |-> 1,cpos |-> 1]),
    ([rewinds |-> 0,ccnt |-> 0,cgo |-> 0,crashes |-> 0,coord |-> 1,ccur |-> 1,committer |-> <<TRUE, TRUE>>,pos |-> <<(0 :> 2 @@ 1 :> 0), (0 :> 0 @@ 1 :> 0)>>,ctl |-> <<[k |-> "SC", cid |-> 1], [k |-> "DW", recs |-> {<<0, 1>>}, cid |-> 1], [k |-> "DC", cid |-> 1, n |-> 1]>>,up |-> <<TRUE, TRUE>>,worker |-> <<TRUE, FALSE>>,nextCid |-> 2,cbuf |-> {},fails |-> 0,table |-> (<<0, 0>> :> 0 @@ <<0, 1>> :> 0 @@ <<1, 0>> :> 0 @@ <<1, 1>> :> 0),cto |-> 0,owner |-> (0 :> 1 @@ 1 :> 0),srcC |-> (0 :> 2 @@ 1 :> -1),wbuf |-> <<{}, {}>>,tOff |-> 0,woff |-> <<(0 :> -1 @@ 1 :> -1), (0 :> -1 @@ 1 :> -1)>>,wseen |-> <<1, 0>>,cphase |-> "idle",moves |-> 1,cpos |-> 1])
    >>
----


=============================================================================

---- CONFIG KcSink_TTrace_1793695459 ----
CONSTANTS
    Tasks = { 1 , 2 }
    NP = 2
    MaxOff = 2
    MaxCommits = 2
    MaxCtl = 7
    MaxCrash = 0
    MaxMoves = 2
    MaxRewind = 0
    MaxFail = 0
    CLOSE = "fixed"
    SEEKNULL = "skip"
    CRESET = "earliest"
    REPLAY = "max"
    FAIL = "retry"

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
\* Generated on Tue Nov 03 14:14:20 IST 2026