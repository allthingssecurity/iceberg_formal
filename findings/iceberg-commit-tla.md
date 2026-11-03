# Iceberg commit protocol in TLA+: four findings (2026-11-02)

Spec: `tla/commit/IcebergCommit.tla` (308 lines). Configs: `tla/commit/configs.py` -> 15 generated `.cfg`.
Runner: `tla/run2.sh commit IcebergCommit <cfg>` (TLC, 2 workers, -deadlock). Logs in `tla/commit/out/`,
verdicts in `tla/commit/results.txt`, traces via `python3 tla/trace.py commit <cfg> pc pointer nseq ok landed`.
The whole suite runs in under 15 seconds; the largest config is 130 distinct states.

Two writers, rows `{1,2,3}`, three operations (append, equality delete, rewrite/compaction), the catalog
pointer, sequence numbers, the retry loop, both conflict validations, an atomic and a non-atomic catalog, and a
lost commit answer. Files are opaque sets of row ids. Parquet, manifest layout and partitioning are abstracted
away. State counts and step numbers for the violating configs depend on TLC's worker scheduling and shift a
little between runs; the verdicts do not.

## Results

| Config | Claim | Verdict | States |
|---|---|---|---|
| wit_both | both writers can commit from the same base | reachable (W_BothCommit violated) | 50 |
| wit_retry | a retry happens | reachable (W_Retry violated) | 49 |
| da_wit_done | both writers reach a terminal state | reachable (W_Done violated) | 70 |
| rw_serializable | compaction over a concurrent delete is serializable | **violated** | 79 |
| rw_deleteholds | the delete still covers its rows | **violated** | 55 |
| rw_fix_seq | same, rewrite inherits the replaced files' sequence number | holds, all 5 invariants + Termination | 80 |
| rw_fix_validate | same, rewrite calls validateNoConflictingDeletes | holds, all 5 invariants + Termination | 72 |
| da_deleteholds | the delete still covers its rows, versus a concurrent append | **violated** | 55 |
| da_serializable | that outcome is serializable | holds (+ Termination) | 80 |
| da_iso_serial | serializable isolation prevents it | **does not**: DeleteHolds still violated | 50 |
| cat_rename | no accepted commit is lost, filesystem catalog | **violated** | 72 |
| cat_cas | no accepted commit is lost, atomic catalog | holds, all 5 invariants + Termination | 80 |
| unk_double | a payload lands at most once after a lost answer | **violated** | 85 |
| unk_fix_token | same, with an idempotency token in the catalog | holds, all 5 invariants + Termination | 130 |
| all_fix | every guard on, all 5 invariants together | holds + Termination | 112 |

Invariants: `NoLostCommit`, `NoDoubleApply`, `ReadStability`, `Serializable`, `DeleteHolds`.
`Serializable` and `DeleteHolds` are guarded by a terminal state, so `da_wit_done` is there to show the guard is
satisfiable and the passing results are not vacuous. Every passing config that checks a guarded invariant also
checks `Termination`, which proves the terminal state is reached.

## Finding 1: compaction resurrects rows a concurrent delete removed

`rw_serializable`, 11 states. Writer A deletes row 1. Writer B compacts. Both plan at snapshot 0.

```
  2 Plan("B")        pointer=0
  3 Plan("A")        pointer=0
  6 Prepare("A")
  7 Cas("A")         pointer=1  ok=[A=TRUE]      snapshot 1 dels = {[{1}, seq 1]}
  9 Prepare("B")                                 B refreshes, its validation passes
 10 Cas("B")         pointer=2  ok=[A=TRUE,B=TRUE]
```

Final snapshot 2:

```
[ data |-> {[rows |-> {1, 2}, seq |-> 2]},
  dels |-> {[rows |-> {1}, seq |-> 1]}, parent |-> 1, by |-> "B" ]
```

The compacted file carries sequence number 2. The delete file carries 1. A delete file applies only to data
files with a strictly lower sequence number, so the delete no longer covers the compacted file and row 1 is
visible again. Final table `{1,2}`; both serial orders give `{2}`. A lost update, not an isolation preference.

Two independent guards each close it, and the model confirms both: inherit the replaced files' sequence number
in the rewrite (`rw_fix_seq`), which is what sequence number inheritance in the table spec is for, or call
`validateNoConflictingDeletes` on the rewrite so it aborts instead of committing stale (`rw_fix_validate`).
Both together is `all_fix`.

## Finding 2: an equality delete does not cover rows appended concurrently, and serializable isolation does not help

`da_deleteholds`, 9 states. The table holds row 1. A deletes rows matching a predicate covering 1 and 2.
B appends row 2. Both plan at snapshot 0; A commits first, then B.

```
  2 Plan("A")        pointer=0
  3 Plan("B")        pointer=0
  7 Cas("A")         pointer=1  ok=[A=TRUE]
  8 Prepare("B")
  9 Cas("B")         pointer=2  ok=[A=TRUE,B=TRUE]
```

Final snapshot 2:

```
[ data |-> {[rows |-> {1}, seq |-> 0], [rows |-> {2}, seq |-> 2]},
  dels |-> {[rows |-> {1, 2}, seq |-> 1]}, parent |-> 1, by |-> "B" ]
```

Row 1 is masked (0 < 1). Row 2 is not (2 > 1), so it survives a delete that named it.

The honest part: `da_serializable` shows this outcome **is** serializable. It equals the serial order A then B.
So it is not a bug and not an isolation violation. It is the gap between what `DELETE FROM t WHERE ...` means
to a user and what it means to the format: a delete is a statement about the rows that existed when it was
planned. `da_iso_serial` makes the point sharply. Setting serializable isolation does not change the outcome,
because the appender never validates and the deleter never retried, so there was no conflict to detect.

Consequence for a streaming lakehouse: a GDPR or correction delete running against a live Kafka to Iceberg
stream can leave matching rows behind, with no error anywhere. The guard is operational rather than a config
flag: fence the stream, or re-run the delete, or make it a predicate the ingestion path also enforces.

## Finding 3: a non-atomic catalog loses a commit it reported as successful

`cat_rename`, 11 states. Two appends against a filesystem style catalog that reads the version then writes
version+1.

```
  7 CheckVer("A")    both writers pass the check at version 0
  9 CheckVer("B")
 10 Put("A")         pointer=1  ok=[A=TRUE]
 11 Put("B")         pointer=2  ok=[A=TRUE,B=TRUE]
```

Snapshot 2's parent is 0, not 1. A's snapshot is not in the ancestry of the pointer, and A was told it
succeeded. `cat_cas` is the same scenario against an atomic compare-and-set and holds on all five invariants.

This is the formal argument for a real catalog in the path (REST, JDBC, Nessie, Polaris, BDC) rather than a
filesystem pointer, and it carries further than prose because the trace is eleven steps long.

Note that `Serializable` still **holds** here: a lost commit erases its writer's effect entirely, so the result
is a valid serial outcome of the remaining writer alone. Only `NoLostCommit`, which records that the catalog
said yes, catches it. The two invariants are complementary and both are needed.

## Finding 4: a commit whose answer is lost is applied twice, and the catalog API cannot prevent it

`unk_double`, 10 states. A commits, the answer is lost (`CommitStateUnknownException`), A retries.

```
  6 Cas("A")         pointer=1  landed=[A=1]     the commit DID land
  7 Ack("A")         pc=[A="unknown"]            the answer is lost
  8 Unknown("A")     pc=[A="prepare"]            nothing to compare against, so retry
 10 Cas("A")         pointer=2  landed=[A=2]     the payload is applied a second time
```

For an append that is duplicate rows. For a delete it is a second delete file. The writer did nothing wrong:
with no way to ask the catalog "did my commit land", refreshing and comparing snapshot summaries is guesswork.

`unk_fix_token` adds one thing: the writer can recognise its own snapshot in the refreshed ancestry, which is
what a client-generated commit id or idempotency key recorded by the catalog would give it. With that, all five
invariants hold and the suite terminates. **This is a concrete, checked recommendation for the Polaris and BDC
Catalog commit API**: accept a client commit id, record it in the metadata, and let a refresh decide the
outcome instead of guessing.

## Caveats

- This checks the model, not Iceberg's code. The engine level bugs (an engine calling the wrong validation
  method, a catalog whose compare-and-set is not really atomic) are for the DST harness, as the Camel work
  settled: the model picks the faults and the invariants, DST runs the real code against them.
- Two writers, three rows, one retry, bounded snapshot and sequence counters. Enough for every finding above.
  A third writer would raise the state count without, I expect, new behaviour at this level of abstraction.
- The object store is **not** in this spec. Crash before the pointer move, truncated manifest writes and the
  safety of orphan file cleanup are the next model, not this one. Finding 4 is the part of that story that
  surfaces at the catalog boundary.
- `DeleteHolds` is a user expectation, not a spec requirement, and findings 1 and 2 violate it for completely
  different reasons. Finding 1 also violates `Serializable` and is a real defect. Finding 2 does not.
- Row level deletes are modelled as equality deletes. Position deletes and v3 deletion vectors reference
  specific files, which is a different and probably safer shape; worth a separate config.
