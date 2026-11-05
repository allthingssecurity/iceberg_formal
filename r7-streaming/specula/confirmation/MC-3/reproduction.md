# MC-3 — Phase 2 reproduction record

System under test: PRISTINE `apache/iceberg@9fffeb6` sink. The per-finding worktree carried the
Specula trace instrumentation; it was saved (`notes/instrumentation.diff`,
`notes/instrumentation-untracked.tgz`) and the nine instrumented main sources were restored with
`git checkout HEAD --`, `TlaHooks.java` / `connect/tla/` removed. The runner aborts unless
`git status` is clean under `kafka-connect/*/src/main`, `core/src/main`, `api/src/main`; the compiled
`Coordinator.class` contains no `tla` reference. Only the test-only Gradle source set `tlaHarness`
(embedded Kafka 3.9.2 / Connect test jars, `kafka-connect/build.gradle`) is added.

Runner: `repro/test_bugMC-3_floor_expiry.sh` (sources in `repro/test_bugMC-3_floor_expiry/`),
full log `repro/MC-3.log`, per-variant reports + JUnit XML in `repro/test_bugMC-3_floor_expiry/out/`.

## Escalation ladder

| Level | Attempt | Outcome |
|---|---|---|
| 0 | `Mc3E2ELevel0NoFaultTest`: embedded KRaft broker + Connect worker, real sink, healthy commit S1, other writer + `expire_snapshots(retain_last=1)`, graceful REST restart, more data | floor becomes `{}` (latent), but the `-coord` checkpoint is past S1's DataWritten -> nothing replayed -> no duplicate. Not triggered: a replay needs a failure between table commit and checkpoint, which a fault-free in-JVM run cannot produce (no `kill -9` of a thread). |
| 1 | Same cluster + ONE environment failpoint from the declared failure model: the catalog applies the connector's commit and reports `CommitStateUnknownException` (`Mc3Catalog`, configured through the public `iceberg.catalog.catalog-impl`). Variants: other writer + expiry, `rewrite_manifests` + expiry, control without expiry. Unit harness (real `Coordinator`, Kafka `MockProducer`/`MockConsumer`): exact CE replay with the crash at `Coordinator.java:216` (checkpoint transaction never commits), control, and the in-process partial-commit retry. | **Triggered** in every floor-unreachable variant; controls clean. |
| 2 | not needed | — |
| 3 | not needed | — |

## Harness notes / artifacts found and handled
- First E2E attempt: the faulted commit was the coordinator's very first commit, so the `-coord`
  group had no checkpoint; the restarted coordinator then starts at the sink default
  `auto.offset.reset=latest` and re-reads nothing (no replay, no duplicate, also in the expiry
  variant). The CE has an earlier empty checkpoint (states 6-11); the harness now waits for the first
  checkpoint before arming the fault. Recorded as a reachability premise: a replay needs at least one
  earlier checkpoint (always true for a connector that has run more than one commit cycle).
- `MockConsumer.subscribe()` clears the mock's committed-offset map (mock artifact; a broker keeps
  group offsets), so the unit harness installs the `-coord` committed offset after `start()`.
- `MockConsumer.groupMetadata()` reports `dummy.group.id`; the unit harness reads the checkpoint from
  whatever group the coordinator's producer committed (production group: `<connect group>-coord`).
- Run 2 of the script exited 2 only because the script file was edited while bash was executing it
  (`syntax error near unexpected token do`); all 7 tests had passed; log archived as
  `repro/test_bugMC-3_floor_expiry/MC-3.run2-script-edited-midrun.log`. The definitive log is
  `repro/MC-3.log`.

## Definitive run (`repro/MC-3.log`, EXIT=0, 8/8 tests PASSED)

| Test | Level | Floor at the replayed commit | Result |
|---|---|---|---|
| `Mc3E2ELevel0NoFaultTest` | 0 | `{}` (S1 EXPIRED) but no replay (checkpoint 4 > S1 DW @2) | no duplicate — not triggered |
| `TestMc3CrashReplayFloor.crashBeforeCheckpointThenForeignCommitAndExpiryDuplicates` (CE) | 1 | `{}` | F1 x2, rows b1-* x2 |
| `TestMc3CrashReplayFloor.crashBeforeCheckpointFloorReachableNoDuplicates` (control) | 1 | `{"0":5}` | no duplicate |
| `TestMc3CrashReplayFloor.partialCommitUnknownLandedRetryAfterExpiryDuplicates` | 1 | `{}` | F1 x2 (no crash, no restart) |
| `Mc3E2EControlNoExpiryTest` (control) | 1 | `{"0":12}` | replay re-read (WARN logged) and filtered, no duplicate |
| `Mc3E2EForeignAppendExpiryTest` | 1 | `{}` | S1 file x2, rows b1-0..4 x2, source offsets {0=10} committed |
| `Mc3E2ERewriteManifestsExpiryTest` | 1 | `{}` (head = `replace`) | S1 file x2, rows b1-0..4 x2 |

Sink log of the restarted coordinator (E2E foreign-append): `WARN CommitState - Received commit
response when no commit in progress, this can happen during recovery. Commit ID:
336da702-b17e-4cbc-b2ce-b5fcc7d2f8fe` (= S1's `kafka.connect.commit-id`), then `completed commit to
table db.tbl, snapshot 1381464225237104975` (`added-files=2`: S1's file again + b2's file).
