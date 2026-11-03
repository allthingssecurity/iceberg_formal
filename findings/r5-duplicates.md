# r5: upstream duplicate search for the seven confirmed Majors (2026-11-03)

Method: `gh api search/issues` over apache/iceberg issues and PRs, open and closed, 4–6 keyword queries per finding
(class names, method names, symptoms, exception names), then the bodies, comments and timelines of every plausible
hit read in full. Raw hits: scratchpad `dup-search.tsv`. Keyword search proves presence, not absence: "no match" means
none found by these queries, not that the issue is new. Mailing list and Slack were not searched.

| id | upstream | upstream status | relation | what r5 adds |
|---|---|---|---|---|
| A2-28 JDBC replayed CAS | **#18743** (issue, 2026-11-01) + **#18745** (PR, Fixes #18743); general shape #18402 / #18407 | both open, unmerged | **duplicate**: same root cause; #18743 even names the concurrent-writer rewind | independent reproduction: 20/20 on each of 5 operations (incl. inside a transaction), control 0/20; single-writer duplicate-file prediction refuted 0/20 |
| D-31 add_files deletes manifests on CSUE | **#13443** "importSparkPartitions should not delete manifest files after getting CommitStateUnknownException" (reported on 1.8.1) | **closed by stale-bot, not_planned** (2026-01-14), never fixed | **duplicate, unfixed** | still present on 1.12.0: 10/10 on Spark 4.1 and 10/10 on Spark 3.5; consequence shown concretely (live snapshot → missing manifest → every read fails) |
| C-55 eq-delete executor cache ignores projection | **#15039** "Equality deletes ignored on executor cache hit with different query" (1.9.2); fix PR **#15564** (adds projected field ids to the key) | issue **open**; PR closed **unmerged** | **duplicate root cause, new trigger** (#15039 uses different projections; r5 uses int→long type promotion + time travel) | still present on 1.12.0 with executor cache on by default: 5–9/10 wrong, 0/10 with cache off and 0/10 for same-schema orderings |
| E-4 Flink re-commit after expiry | **#18374** (PR) describes exactly this (crash loop if manifests deleted, duplicates if not); related #18098 | PR **closed unmerged**; maintainer: "configure snapshot expiration so the snapshots are stored as long as they could be needed for job restarts" | **known; maintainers treat it as an operational requirement, not a bug** | reproduction numbers (20/20 dup with IcebergSink+post-commit maintenance, crash loop otherwise). Severity should be read against the maintainer position: arguably "won't fix / document". |
| C-8 Hive lock lease lapses, no expected-location CAS | **#14096** "Connection lost during persistTable with Hive Locks can cause overwritten metadata location"; #5036 (heartbeats); #6570 lock-free mode via HIVE-26882 | #14096 **closed by stale-bot, not_planned**; lock-free mode exists and is the documented alternative | **same root (no CAS when the lock is on), different trigger** (#14096: HMS-side retry of alter_table; r5: lease reaped during a client freeze) | real embedded HMS reproduction 10/10 + 10/10, three controls 0/10; lease-expiry trigger not found upstream |
| D-2 Spark streaming to a branch, replayed epoch duplicated | none found | — | **no match found** | candidate new: 5/5, control (main) 0/5 |
| E-45 EqualityConvertCommitter deletes DVs on non-cleanable failure | none found (#17630 is a different bug in the same task; the committer dates from #16874, June 2026) | — | **no match found** | candidate new: 20/20, core RowDelta with the same fault 0/20 |

## Reclassified tally

- **No upstream match found (2):** D-2, E-45.
- **New trigger of a known root cause (2):** C-55 (issue still open), C-8 (issue stale-closed; lock-free mode is the mitigation).
- **Known, unfixed (2):** D-31 (stale-closed, never fixed), A2-28 (issue + fix PR opened 2026-11-01, two days ago).
- **Known, maintainers consider it configuration (1):** E-4.

All seven are still reproducible on the released 1.12.0 jars; iceberg/ main @ 9fffeb63b16 has the same code at every cited site.
