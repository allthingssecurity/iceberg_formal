# Round 7: does streamed data land in Iceberg exactly once?

Rounds 5 and 6 looked at concurrent commits and failed writes. This round asks one question about streaming
ingestion: when records flow from Kafka (or Flink) into an Iceberg table, does every record end up in the
table exactly once? Not lost, not doubled, after crashes, restarts, rebalances, upgrades and the odd operator
action.

Two paths were checked, because both are open source and both are shipped by the Iceberg project:

- the Kafka Connect sink (`iceberg-kafka-connect`), the closest open equivalent to Confluent Tableflow;
- the Flink sinks (`FlinkSink`, `IcebergSink`, the dynamic sink).

Tableflow itself is closed source and was not tested.

All results below were found on Iceberg `main` and, where marked, re-checked on the **released 1.12.0 jars
from Maven Central**. Nothing in this round has been reported upstream yet.

## The method: Specula

[Specula](https://github.com/specula-org/Specula) is a workflow for finding bugs with TLA+. Its key idea is
that a model is only worth something if it matches the code, so it records traces from the real code and
checks the model against them before trusting anything the model says.

It runs in five phases. Here is what each phase did in this round, for the Kafka Connect sink.

**1. Code analysis (70 min).** Read the sink's protocol code (about 1,500 lines) and its history: 178 commits,
22 of them bug fixes in the core files, plus the related issues and PRs. Grouped the risks into five
scenarios: the stored offset "floor" claiming more than a commit contained, the floor disappearing or being
read from the wrong place, `close()` throwing away buffered records, zero or two coordinators, and how a
commit decides it has heard from every worker. Output: a modeling brief.

**2. Model (2 h).** Wrote a TLA+ model of workers, coordinator, control topic, Kafka consumer groups and
the Iceberg table (76 KB), with the invariants "no record lost" and "no record committed twice", and 17
hunting configurations, each aimed at one scenario.

**2.5. Traces from the real code (1 h 50 min).** Added probes to the real sink code and ran eight scenarios
against it (normal running, coordinator crash, table commit failure, partial commit, three zombie-coordinator
cases, failures during a partition handover). Each run produced a trace of what the code actually did.

**3. Check the model, then hunt (2 h 40 min).**
- The model reproduced 7 of the 8 real traces. The eighth showed the model was wrong about how Kafka hands a
  partition from one task to another (it happens in two rounds, not one). The model was fixed; 8 of 8 pass.
- With no faults injected, the model was checked exhaustively: 18.7 million distinct states, no violation.
- Then the hunt. Some configurations broke an invariant within seconds. Three of those turned out to be
  mistakes in the model, not in the code (for example, the model let a coordinator see an out-of-date
  partition count that real Kafka never shows). Those were fixed, and every hunt was run again. What
  survived went into a bug report: five violations.

**4. Confirmation (about 7 h).** Every suspect, the five from the model plus sixteen from code review, got its
own agent. Each one searched upstream for an existing report and then tried to make the bug happen in the
real code: unit-level against the real classes, or end to end on an embedded Kafka broker and Connect cluster.
A bug only counts if it happens with a control run beside it where it does not happen.

The same method was followed by hand for the Flink sinks and, first, for the Kafka Connect sink starting from
the round-6 model. Those two tracks found the Flink bug below and confirmed the snapshot-expiry duplicate.

### How the results were checked

Specula's agents tested against Iceberg `main`. Each finding marked "checked on 1.12.0" was then run again,
separately, on the released jars, with old-version workers built from the released 1.10.0 or 1.10.2 jars
where the bug involves an upgrade, and with its own control.

Two mistakes were made and caught during that checking, and are recorded here because they show why it
matters. A control run first did not actually run (a setting did not reach the test), so it looked like the
bug also happened in the control; it was fixed and re-run. And one run passed an empty scenario name, ran
nothing, and still printed "REPRODUCED"; it was thrown away and run properly. Neither changed a result, but
either could have.

## What was found

Of the 21 suspects from Specula: 9 are new, real bugs; 5 are real but already reported upstream; 3 are real
flaws that cannot cause harm today because something else catches them; 1 was a false positive; 3 were
still being checked when this was written. The hand-run tracks added one Flink bug.

### Data loss and corruption

**Rolling upgrade loses records (Kafka Connect, MC-1). Checked on 1.12.0.**
Version 1.11 started sending a table ID with each "here are my files" message. During a rolling upgrade, old
workers (1.10 or earlier) still send messages without it. The coordinator treats "table T, no ID" and "table T,
ID x" as two different tables. It commits the first, records how far it has read, and then treats the second
group's files as already committed. The workers have already told Kafka those records are done, so they are
never sent again. In the test the table had 3 of 6 records, 18 runs out of 18. With the old worker's message
given the table ID and nothing else changed, 6 of 6, every time.

**Upgrade plus a re-created table corrupts the new table (Kafka Connect, CR-3). Checked on 1.12.0.**
If a table is dropped and created again under the same name, the coordinator is supposed to refuse files
meant for the old table. It can only do that when the message carries the table ID, and messages from old
workers do not, so the check is skipped on purpose. Files written for the old table are added to the new one.
In the test the new table pointed at files with the wrong column types (reads failed) or at files deleted with
the old table (reads failed with "file not found"). With current workers the commit is correctly refused.

**Coordinator crashes on old-version messages (Kafka Connect). Checked on 1.12.0.**
When the new coordinator commits a message that came from a 1.10 worker, it crashes while sending its own
"committed" message (`Invalid field position: 3`). The table commit has already happened, but the
coordinator stops on every commit that includes old messages, so ingestion stalls during an upgrade.

**Bounded Flink job loses a writer's last batch (Flink F1). Checked on 1.12.0.**
When a Flink job reads a fixed amount of data (a backfill, an initial load) with several writers, each writer
hands over its last batch under the same label, "end of input". If one writer finishes, a checkpoint happens,
and then the other writer finishes, the second batch replaces the first instead of joining it. The job reports
success. On a real Flink 2.3 job, 17 of 20 runs were missing exactly one input split (200 of 400 rows); with
one writer, 0 of 20. Open issue #15846 has the same cause, but it was believed to need unaligned checkpoints.
This happens with ordinary ones.

**Re-created control topic loses records (Kafka Connect, CR-1). Reproduced by Specula; not yet checked on 1.12.0.**
The connector uses an internal Kafka topic to coordinate. If someone deletes and re-creates that topic while
the connector runs (a cluster migration, fixing a setting), the running connector remembers its old reading
position and skips everything on the new topic. 10 of 80 records went missing, permanently. Issue #15293
reports the symptom; this particular cause comes from a safeguard added in 1.12.0 and is not reported.

**Control topic with several partitions loses records on restart (Kafka Connect, CR-4). Reproduced by Specula; not yet checked on 1.12.0.**
The coordinator only saves its position for control-topic partitions it has read. If a worker's message
lands on a partition it never read, and the coordinator is replaced before that round commits (a task restart
is enough), the new coordinator starts that partition at "latest" and never sees the message. 6 of 6 attempts
lost the records; with a one-partition control topic, none. The open PR #18006 would fix it; its own comment
assumes this case cannot happen.

**Partial commits retry forever while Kafka is told the records are done (Kafka Connect, CR-12). Reproduced by Specula; not yet checked on 1.12.0.**
When a commit round times out (for example, one task has no data and never answers), the coordinator does a
"partial" commit. If that commit fails for a permanent reason, such as a revoked permission, the error is
swallowed and retried every round, with no limit. The task stays RUNNING, nothing reaches the table, and
Kafka offsets keep moving, so consumer lag looks fine. In the test, 43 records were done in Kafka and not in
the table after 14 silent retries.

**Snapshot cleanup while the connector is down causes duplicates (K1, MC-3). Checked on 1.12.0. Already known.**
The connector finds "how far did I get" in the table's snapshot history. If that snapshot is expired while
the connector is stopped and a replay is pending, it re-adds data. Open PR #18006 documents this.

**Two failovers right after a writer finishes (Flink F2). Shown at operator level only.**
The table records an impossible progress marker, and the next restore skips pending data. Needs two failures
in a short window; not yet shown on a full Flink job.

### Ingestion stops

**Dropped or expired commit branch stops all commits (Kafka Connect, MC-4). Checked on 1.12.0.**
The connector can write to a branch, for example in Iceberg's audit-branch workflow. If the branch is dropped
after it was published, or expires, two parts of the code look for the progress marker in different places,
and every commit fails. In the test, 9 of 9 commits failed with the branch dropped; 9 of 9 succeeded with it
kept. Meanwhile Kafka keeps marking records as done, so a long stall risks loss once the control topic's
retention passes. A fix is ready (see below).

**Missed coordinator election is never retried (Kafka Connect, MC-5). Reproduced by Specula; not yet checked on 1.12.0.**
Only the task that owns the first partition runs the coordinator, and that check only happens when a task is
given new partitions. If the check runs at a bad moment, for example while the group is momentarily empty
after a restart with a slow catalog, the task decides it is not the coordinator and never asks again. Nothing
is committed, nothing errors, both tasks show RUNNING. It lasted over five minutes in the test, until a manual
restart.

**Coordinator thread hangs after a restart with commits queued (Kafka Connect, CR-6). Reproduced by Specula; not yet checked on 1.12.0.**
With more tables than commit threads, a restart during a commit leaves the old coordinator thread waiting
forever. It keeps its Kafka connection and blocks the new coordinator for about five minutes. No data lost.

**A stopped old coordinator fails its task (Kafka Connect, CR-9). Reproduced by Specula; not yet checked on 1.12.0.**
When an old coordinator is replaced, it is supposed to notice and exit quietly. If its last commit fails a
validation check at the same time, the code only looks at the outer error, misses that it was replaced, and
fails the whole task.

### Real, but not reported again

Already reported upstream: a table dropped while records are in flight (#18012), a long commit failing the
task (#17376), a commit declared complete too early (#17925, #18322), an oversized commit message (reported
in Databricks' fork). Real flaws with no harm today: an interrupted rewind during pause/resume (Kafka Connect
always restarts the task and re-reads), and two zombie-coordinator cases (caught by the offset filter and the
commit validator). One false positive: unstable transactional IDs.

## What this means in practice

- Do not do a rolling upgrade of the Iceberg Kafka Connect sink across 1.11. Stop the connector, let the
  pending commit finish, then upgrade.
- Use a one-partition control topic, and do not delete or re-create it under a running connector.
- If the connector writes to a branch, do not drop the branch or give it a `max-ref-age` while the connector
  targets it.
- Alert on the table falling behind, not only on Kafka consumer lag. Several of these bugs keep Kafka lag low
  while the table stops receiving data.
- For bounded Flink jobs (backfills, initial loads) with `FlinkSink`, check row counts afterwards, or use one
  writer, until F1 is fixed.

## Upstream

Fixes are being prepared for MC-4 (branch fallback), MC-1 together with the upgrade crash, and Flink F1, each
with a regression test that fails without the fix. CR-3 needs a design decision from the maintainers and will
be an issue. The bugs marked "not yet checked on 1.12.0" will be re-run on the released jars before anything
is filed.

## Files

- [`findings/r7-specula-kafka-connect.md`](../findings/r7-specula-kafka-connect.md): the Specula run and the
  checks on released jars, with exact commands.
- [`findings/r7-kafka-connect.md`](../findings/r7-kafka-connect.md): the hand-run Kafka Connect track
  (round-6 model checked against real traces, snapshot-expiry duplicate).
- [`findings/r7-flink.md`](../findings/r7-flink.md): the Flink track, F1 and F2.
- [`specula/`](specula/): Specula's modeling brief, TLA+ model and configurations, bug report, and its
  confirmation verdicts.
- [`verify/`](verify/): the tests used to re-check findings on the released 1.12.0 jars.
