package org.apache.iceberg.dst;

/** A command-line entry point, so one seed can be replayed without a build tool in the way. */
public final class Main {
  private Main() {}

  static FaultConfig configFor(String mode) {
    switch (mode) {
      case "quiet":
        return FaultConfig.quiet();
      case "aggressive":
        return FaultConfig.aggressive();
      case "orphan0":
        return FaultConfig.orphanClean(0);
      case "orphan10":
        return FaultConfig.orphanClean(10);
      case "orphan40":
        return FaultConfig.orphanClean(40);
      case "orphan15":
        return FaultConfig.orphanClean(15);
      case "orphan20":
        return FaultConfig.orphanClean(20);
      case "orphan30":
        return FaultConfig.orphanClean(30);
      case "orphan200":
        return FaultConfig.orphanClean(200);
      case "solo":
        {
          // One writer, several operations, ref actions on: no concurrency at all. If a violation
          // survives this, interleaving is not part of the cause.
          FaultConfig c = FaultConfig.quiet();
          c.writers = 1;
          c.operationsPerWriter = 8;
          c.refActions = true;
          c.rewriteManifestsWorkload = true;
          c.seedSnapshots = 2;
          return c;
        }
      case "txn":
        {
          // Appends and transactions only, two writers, no faults: the narrowest shape that still
          // shows the sequence-number flip, if transactions are the cause.
          FaultConfig c = FaultConfig.quiet();
          c.writers = 2;
          c.operationsPerWriter = 3;
          c.transactionsOnly = true;
          c.seedSnapshots = 2;
          return c;
        }
      case "duo":
        {
          // Two writers, no faults: the smallest shape that can still interleave.
          FaultConfig c = FaultConfig.quiet();
          c.writers = 2;
          c.operationsPerWriter = 4;
          c.refActions = true;
          c.rewriteManifestsWorkload = true;
          c.seedSnapshots = 2;
          return c;
        }
      case "manifests":
        return FaultConfig.manifests();
      case "manifests-quiet":
        return FaultConfig.manifestsQuiet();
      case "expiry":
        return FaultConfig.expiry();
      case "expiry-quiet":
        return FaultConfig.expiryQuiet();
      case "dupdelete":
        return FaultConfig.duplicateDeletes();
      case "deephistory":
        return FaultConfig.deepHistory();
      case "deephistory-quiet":
        {
          FaultConfig c = FaultConfig.quiet();
          c.seedSnapshots = 4;
          return c;
        }
      case "catalog-only":
        {
          // The catalog can lose an answer or reject, but the store never fails. Separates "the
          // bug needs a lost commit answer" from "the bug needs a lost answer and then a second
          // failure".
          FaultConfig c = FaultConfig.quiet();
          c.commitRejectPct = 6;
          c.commitUnknownAfterLandingPct = 6;
          c.commitUnknownWithoutLandingPct = 4;
          return c;
        }
      case "nonatomic":
        {
          FaultConfig c = new FaultConfig();
          c.selfCheck = FaultConfig.SelfCheck.NON_ATOMIC_POINTER;
          return c;
        }
      case "nonatomic-quiet":
        {
          FaultConfig c = FaultConfig.quiet();
          c.selfCheck = FaultConfig.SelfCheck.NON_ATOMIC_POINTER;
          return c;
        }
      case "nonstrict":
        {
          FaultConfig c = new FaultConfig();
          c.strictCleanup = false;
          return c;
        }
      case "seqahead":
        {
          FaultConfig c = new FaultConfig();
          c.sequenceAheadRewrite = true;
          return c;
        }
      case "seqahead-quiet":
        {
          FaultConfig c = FaultConfig.quiet();
          c.sequenceAheadRewrite = true;
          return c;
        }
      case "default":
        return new FaultConfig();
      default:
        // An unknown mode must not quietly fall back to the default config: that turns a typo in a
        // sweep command into a clean row that means nothing. One of these cost me a bogus
        // measurement already.
        throw new IllegalArgumentException(
            "unknown mode '" + mode + "'; see Main.configFor for the list");
    }
  }

  public static void main(String[] args) {
    if (args.length > 0 && args[0].equals("entries")) {
      // Every live manifest entry for one path, per snapshot: the only way to tell a real sequence
      // number change from two entries that a path-keyed map collapsed.
      long s0 = Long.parseLong(args[1]);
      String needle = args[2];
      RunResult r = Simulation.run(s0, FaultConfig.swarm(s0));
      org.apache.iceberg.io.FileIO view = ((FakeFileIO) r.ops.io()).observerView();
      for (org.apache.iceberg.Snapshot snap : r.finalMetadata.snapshots()) {
        StringBuilder line = new StringBuilder();
        for (org.apache.iceberg.ManifestFile m : snap.allManifests(view)) {
          if (m.content() != org.apache.iceberg.ManifestContent.DATA) {
            continue;
          }
          try (org.apache.iceberg.ManifestReader<org.apache.iceberg.DataFile> reader =
              org.apache.iceberg.ManifestFiles.read(m, view, r.finalMetadata.specsById())) {
            for (org.apache.iceberg.DataFile f : reader) {
              if (f.location().endsWith(needle)) {
                line.append("\n    [")
                    .append(m.path())
                    .append(" dataSeq=")
                    .append(f.dataSequenceNumber())
                    .append(" manifestSeq=")
                    .append(m.sequenceNumber())
                    .append(" addedBy=")
                    .append(m.snapshotId())
                    .append(" len=")
                    .append(m.length())
                    .append(']');
              }
            }
          } catch (Exception ex) {
            line.append(" [read failed: ").append(ex).append(']');
          }
        }
        if (line.length() > 0) {
          System.out.println("snap " + snap.snapshotId() + " seq=" + snap.sequenceNumber() + line);
        }
      }
      System.exit(0);
    }
    if (args.length > 0 && args[0].equals("triage")) {
      // One line per failing seed, with the invariants it broke, so a sweep's whole tail can be
      // looked at rather than only its first failure.
      int seeds = args.length > 1 ? Integer.parseInt(args[1]) : 1000;
      String triageMode = args.length > 2 ? args[2] : "swarm";
      for (long seed = 1; seed <= seeds; seed++) {
        FaultConfig config =
            triageMode.equals("swarm") ? FaultConfig.swarm(seed) : configFor(triageMode);
        RunResult result = Simulation.run(seed, config);
        if (!result.clean()) {
          java.util.Map<String, Integer> byInvariant = new java.util.LinkedHashMap<>();
          for (Violation v : result.unexplained()) {
            byInvariant.merge(v.invariant, 1, Integer::sum);
          }
          System.out.println(
              "seed=" + seed + " " + byInvariant + " config=" + config);
          for (Violation v : result.unexplained()) {
            System.out.println("    " + v);
          }
        }
      }
      System.exit(0);
    }
    if (args.length > 0 && args[0].equals("sweep")) {
      int seeds = args.length > 1 ? Integer.parseInt(args[1]) : 100;
      String mode = args.length > 2 ? args[2] : "default";
      FaultConfig.PreFix preFix =
          args.length > 3 ? FaultConfig.PreFix.valueOf(args[3]) : FaultConfig.PreFix.NONE;
      Sweep.Summary summary =
          mode.equals("swarm")
              ? Sweep.run(1L, seeds, (long seed) -> FaultConfig.swarm(seed).preFix(preFix))
              : Sweep.run(1L, seeds, () -> configFor(mode).preFix(preFix));
      System.out.println("mode=" + mode + " preFix=" + preFix + " " + summary);
      System.out.println("writer outcomes: " + summary.writerOutcomes);
      if (summary.firstFailureReport != null) {
        System.out.println(summary.firstFailureReport);
      }
      System.exit(0);
    }
    if (args.length > 1 && args[1].equals("dump")) {
      RunResult r = Simulation.run(Long.parseLong(args[0]), FaultConfig.aggressive());
      for (FakeStore.WriteRecord w : r.store.writes()) {
        System.out.println(
            Canon.normalize(w.path) + " declared=" + w.declaredBytes + " stored=" + w.storedBytes);
      }
      System.exit(0);
    }
    long seed = args.length > 0 ? Long.parseLong(args[0]) : 1L;
    String mode1 = args.length > 1 ? args[1] : "default";
    FaultConfig config = mode1.equals("swarm") ? FaultConfig.swarm(seed) : configFor(mode1);
    if (args.length > 2) {
      config.preFix(FaultConfig.PreFix.valueOf(args[2]));
    }
    Thread watchdog =
        new Thread(
            () -> {
              try {
                Thread.sleep(20_000);
              } catch (InterruptedException e) {
                return;
              }
              System.err.println("=== WATCHDOG: still running after 20s, dumping threads ===");
              Thread.getAllStackTraces()
                  .forEach(
                      (t, frames) -> {
                        System.err.println("--- " + t.getName() + " " + t.getState());
                        for (StackTraceElement f : frames) {
                          System.err.println("    " + f);
                        }
                      });
              Runtime.getRuntime().halt(2);
            });
    watchdog.setDaemon(true);
    watchdog.start();

    RunResult result = Simulation.run(seed, config);
    watchdog.interrupt();
    System.out.println(
        "seed="
            + seed
            + " steps="
            + result.scheduler.steps()
            + " trace="
            + result.trace.lineCount()
            + " sha="
            + result.trace.sha256()
            + " hung="
            + result.scheduler.hung()
            + " unmanaged="
            + result.scheduler.unmanagedCrossings()
            + " clean="
            + result.clean());
    if (!result.clean()) {
      System.out.println(result.report());
    } else {
      for (Workload.Outcome o : result.outcomes) {
        System.out.println("  " + o);
      }
      for (FakeTableOperations.CommitRecord c : result.ops.commits()) {
        System.out.println("  commit " + c);
      }
      for (org.apache.iceberg.Snapshot snapshot : result.finalMetadata.snapshots()) {
        System.out.println(
            "  summary "
                + snapshot.snapshotId()
                + " "
                + snapshot.operation()
                + " "
                + snapshot.summary());
      }
    }
    System.exit(0);
  }
}
