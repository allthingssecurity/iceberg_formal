package objstore;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.TreeMap;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Catalog;
import software.amazon.awssdk.core.sync.RequestBody;

final class Experiments {
  final Env env;
  final PrintStream det;
  final String client;
  final long seed0;
  final int runs;
  int runCounter = 0;
  int detailRuns = 0;

  Experiments(Env env, PrintStream det, String client, long seed0, int runs) {
    this.env = env;
    this.det = det;
    this.client = client;
    this.seed0 = seed0;
    this.runs = runs;
  }

  Run.Config base(long seed) {
    Run.Config c = new Run.Config();
    c.seed = seed;
    c.client = client;
    return c;
  }

  /** Runs one workload with the given decider active for the whole run; returns the run. */
  Run one(Run.Config cfg, FaultProxy.Decider d, String label) throws Exception {
    env.proxy.clearLog();
    env.proxy.setDecider(d == null ? r -> null : d);
    Run run = new Run(env, cfg, ++runCounter);
    run.execute(null);
    env.proxy.setDecider(r -> null);
    boolean major = run.v.keySet().stream().anyMatch(k -> k.startsWith("MAJOR_"));
    List<FaultProxy.Req> faults = env.proxy.faults();
    if ((major || !faults.isEmpty() || !run.details.isEmpty()) && detailRuns < 30) {
      detailRuns++;
      det.println("== " + label + " run=" + run.runId + " seed=" + cfg.seed + " client=" + cfg.client
          + " checksum=" + cfg.checksum + " expire=" + cfg.expire + " major=" + major);
      det.println("   ops: " + run.opOutcome);
      int n = 0;
      for (FaultProxy.Req r : faults) {
        if (n++ < 15) {
          det.println("   fault#" + r.fault.id + " " + r.fault.kind + " k=" + r.fault.k + " on " + r + " -> " + r.effect);
        }
      }
      for (String s : run.details) {
        det.println("   " + s);
      }
      det.println("   counts: " + run.v);
    }
    env.deletePrefix(run.prefix + "/");
    Env.deleteTree(run.dir);
    return run;
  }

  static void add(Map<String, Integer> agg, Map<String, Integer> v) {
    v.forEach((k, n) -> agg.merge(k, n, Integer::sum));
  }

  static Map<String, Integer> opHistogram(List<FaultProxy.Req> log) {
    Map<String, Integer> h = new TreeMap<>();
    for (FaultProxy.Req r : log) {
      h.merge(r.op + "/" + r.status + (r.range != null ? "/range" : "") + (r.respChecksum.isEmpty() ? "" : "/cks=" + r.respChecksum), 1, Integer::sum);
    }
    return h;
  }

  void smoke() throws Exception {
    Run.Config c = base(seed0);
    c.expire = true;
    env.proxy.clearLog();
    Run run = new Run(env, c, ++runCounter);
    run.execute(null);
    System.out.println("smoke ops=" + run.opOutcome);
    System.out.println("smoke counts=" + run.v);
    System.out.println("smoke proxy ops=" + opHistogram(env.proxy.log()));
    for (String s : run.details) {
      System.out.println("  " + s);
    }
    env.deletePrefix(run.prefix + "/");
  }

  void control() throws Exception {
    Map<String, Integer> agg = new TreeMap<>();
    int majorRuns = 0;
    for (int i = 0; i < runs; i++) {
      Run.Config c = base(seed0 + i);
      c.expire = i % 2 == 1;
      c.checksum = i % 3 == 2;
      Run r = one(c, null, "control");
      add(agg, r.v);
      majorRuns += r.v.keySet().stream().anyMatch(k -> k.startsWith("MAJOR_")) ? 1 : 0;
    }
    System.out.printf("control v%s client=%s runs=%d majorRuns=%d counts=%s%n", env.version, client, runs,
        majorRuns, agg);
  }

  // ------------------------------------------------------------------ calibrations

  /** #5282 (first in 1.1.0): single-PUT retry after a mid-body drop failed with "Resetting to invalid mark". */
  void calibPut() throws Exception {
    for (boolean fault : new boolean[] {true, false}) {
      int threw = 0;
      int ok = 0;
      int injected = 0;
      Map<String, Integer> agg = new TreeMap<>();
      for (int i = 0; i < runs; i++) {
        Run.Config c = base(seed0 + i);
        c.commits = 1;
        c.bigData = false;
        c.readPhase = false;
        Harness.Targeted d = new Harness.Targeted(
            r -> r.op.equals("PutObject") && r.key.contains("/metadata/") && r.key.endsWith("-m0.avro"),
            1, 1, FaultProxy.Kind.CUT_REQ, 200 * 1024, 0);
        Run r = one(c, fault ? d : null, "calib-put-" + (fault ? "fault" : "control"));
        for (FaultProxy.Req q : env.proxy.faults()) {
          if (q.faultBytes > 128 * 1024) {
            injected++;
          }
        }
        if (r.opOutcome.getOrDefault(0, "").startsWith("THREW")) {
          threw++;
        } else if ("OK".equals(r.opOutcome.get(0))) {
          ok++;
        }
        add(agg, r.v);
      }
      System.out.printf("calib-put v%s %s runs=%d injected(>128KiB sent)=%d commitThrew=%d commitOK=%d counts=%s%n",
          env.version, fault ? "FAULT  " : "CONTROL", runs, injected, threw, ok, agg);
    }
  }

  /** #10433/#11335 (first in 1.7.0): S3InputStream retries a read that hits a connection reset. */
  void calibRead() throws Exception {
    for (boolean fault : new boolean[] {true, false}) {
      int targetThrew = 0;
      int targetOk = 0;
      int injected = 0;
      for (int i = 0; i < runs; i++) {
        Run.Config c = base(seed0 + i);
        c.commits = 2;
        c.bigData = false;
        final String[] target = new String[1];
        c.beforeRead = (run, x) -> {
          for (Map.Entry<String, Run.FileRec> e : run.ledger.entrySet()) {
            if (e.getValue().len >= 200 * 1024 && run.committedData().contains(e.getKey())) {
              target[0] = run.key(e.getKey());
              break;
            }
          }
          if (target[0] != null && fault) {
            env.proxy.setDecider(new Harness.Targeted(
                r -> r.op.equals("GetObject") && r.key.equals(target[0]), 1, 1,
                FaultProxy.Kind.RESET_RESP, 100 * 1024, 0));
          }
        };
        Run r = one(c, null, "calib-read-" + (fault ? "fault" : "control"));
        if (target[0] == null) {
          continue;
        }
        injected += env.proxy.faults().size();
        boolean threw = r.details.stream().anyMatch(s -> s.startsWith("seq read threw " + target[0]));
        if (threw) {
          targetThrew++;
        } else {
          targetOk++;
        }
      }
      System.out.printf("calib-read v%s %s runs=%d injected=%d targetReadThrew=%d targetReadOK=%d%n",
          env.version, fault ? "FAULT  " : "CONTROL", runs, injected, targetThrew, targetOk);
    }
  }

  /** Oracle calibration: tamper with one referenced object; the oracle must flag it. */
  void calibOracle() throws Exception {
    String[] tampers = {"none", "truncManifest", "truncData", "deleteData", "flipData", "zeroManifestList"};
    for (String tamper : tampers) {
      int flagged = 0;
      Map<String, Integer> agg = new TreeMap<>();
      for (int i = 0; i < runs; i++) {
        Run.Config c = base(seed0 + i);
        c.commits = 3;
        c.readPhase = false;
        c.bigData = false;
        c.beforeOracle = (run, x) -> tamper(run, tamper);
        Run r = one(c, null, "calib-oracle-" + tamper);
        Map<String, Integer> majors = new TreeMap<>();
        r.v.forEach((k, n) -> {
          if (k.startsWith("MAJOR_")) {
            majors.put(k, n);
          }
        });
        if (!majors.isEmpty()) {
          flagged++;
        }
        add(agg, majors);
      }
      System.out.printf("calib-oracle v%s tamper=%-16s runs=%d flagged=%d majors=%s%n", env.version, tamper, runs,
          flagged, agg);
    }
  }

  void tamper(Run run, String how) throws Exception {
    if (how.equals("none")) {
      return;
    }
    Catalog clean = Env.jdbc(new java.io.File(run.dir, "cat.db"), run.warehouse,
        env.s3Props(false, "apache", run.staging));
    Table t = clean.loadTable(run.id);
    TableMetadata m = ((HasTableOperations) t).operations().current();
    String key;
    switch (how) {
      case "truncManifest": {
        ManifestFile mf = m.currentSnapshot().allManifests(t.io()).get(0);
        key = run.key(mf.path());
        byte[] b = get(key);
        put(key, java.util.Arrays.copyOf(b, b.length / 2));
        break;
      }
      case "zeroManifestList": {
        key = run.key(m.currentSnapshot().manifestListLocation());
        put(key, new byte[0]);
        break;
      }
      case "truncData":
      case "deleteData":
      case "flipData": {
        String loc = run.committedData().iterator().next();
        key = run.key(loc);
        byte[] b = get(key);
        if (how.equals("truncData")) {
          put(key, java.util.Arrays.copyOf(b, b.length / 2));
        } else if (how.equals("deleteData")) {
          env.clean.deleteObject(x -> x.bucket(Env.BUCKET).key(key));
        } else {
          b[b.length / 2] ^= 0x5a;
          put(key, b);
        }
        break;
      }
      default:
        throw new IllegalArgumentException("Unknown tamper: " + how);
    }
    Env.closeQuietly(clean);
  }

  byte[] get(String key) {
    return env.clean.getObjectAsBytes(b -> b.bucket(Env.BUCKET).key(key)).asByteArray();
  }

  void put(String key, byte[] b) {
    env.clean.putObject(x -> x.bucket(Env.BUCKET).key(key), RequestBody.fromBytes(b));
  }

  // ------------------------------------------------------------------ exploration

  void explore() throws Exception {
    String kindsProp = Harness.prop("os.faults", "CUT_REQ,DROP_RESP,STALL_RESP,RESET_RESP,FIN_RESP");
    List<FaultProxy.Kind> kinds = new ArrayList<>();
    for (String k : kindsProp.split(",")) {
      kinds.add(FaultProxy.Kind.parse(k.trim()));
    }
    Map<String, Integer> agg = new TreeMap<>();
    Map<String, Integer> faultAgg = new TreeMap<>();
    int majorRuns = 0;
    List<String> majorSeeds = new ArrayList<>();
    for (int i = 0; i < runs; i++) {
      long seed = seed0 + i;
      SplittableRandom r = new SplittableRandom(seed ^ 0x9e3779b97f4a7c15L);
      Run.Config c = base(seed);
      c.checksum = r.nextBoolean();
      c.expire = r.nextBoolean();
      c.commits = 4 + r.nextInt(4);
      c.boundBytes = r.nextBoolean() ? 8192 : 16384; // 16 KiB bounds push merged manifests over 5 MiB (multipart)
      double p = 0.03 + r.nextDouble() * 0.12;
      int maxFaults = 1 + r.nextInt(4);
      int maxBurst = 1 + r.nextInt(5);
      Run run = one(c, new Harness.RateDecider(seed, p, maxFaults, kinds, maxBurst), "explore");
      add(agg, run.v);
      for (FaultProxy.Req q : env.proxy.faults()) {
        faultAgg.merge(q.op + ":" + q.fault.kind, 1, Integer::sum);
      }
      if (run.v.keySet().stream().anyMatch(k -> k.startsWith("MAJOR_"))) {
        majorRuns++;
        majorSeeds.add(String.valueOf(seed));
      }
    }
    System.out.printf("explore v%s client=%s runs=%d majorRuns=%d majorSeeds=%s%n", env.version, client, runs,
        majorRuns, majorSeeds);
    System.out.println("  faults injected: " + faultAgg);
    System.out.println("  outcome counts : " + agg);
  }

  /** Targeted write/read faults during the workload (os.spec), fault-off control arm included. */
  void target() throws Exception {
    String spec = Harness.prop("os.spec", null);
    if (spec == null) {
      throw new IllegalArgumentException("set -Dos.spec=op:kind:keyClass:nth:burst:k");
    }
    String sizeMode = Harness.prop("os.size", "big");
    for (boolean fault : new boolean[] {true, false}) {
      Map<String, Integer> agg = new TreeMap<>();
      Map<String, Integer> eff = new TreeMap<>();
      int majorRuns = 0;
      for (int i = 0; i < runs; i++) {
        Run.Config c = base(seed0 + i);
        c.commits = Harness.intProp("os.commits", 3);
        c.sizeMode = sizeMode;
        c.checksum = Boolean.parseBoolean(Harness.prop("os.checksum", "false"));
        c.expire = Boolean.parseBoolean(Harness.prop("os.expire", "false"));
        Run r = one(c, fault ? Harness.parseSpec(spec) : null, "target-" + (fault ? "fault" : "control"));
        for (FaultProxy.Req q : env.proxy.faults()) {
          eff.merge(q.op + ":" + q.fault.kind + "->" + q.effect.replaceAll("[0-9]+", "N"), 1, Integer::sum);
        }
        add(agg, r.v);
        majorRuns += r.v.keySet().stream().anyMatch(k -> k.startsWith("MAJOR_")) ? 1 : 0;
      }
      System.out.printf("target v%s client=%s spec=%s %s runs=%d majorRuns=%d%n  effects=%s%n  counts=%s%n",
          env.version, client, spec, fault ? "FAULT" : "CONTROL", runs, majorRuns, eff, agg);
    }
  }

  /** One targeted response fault on the nth GET of one committed data file during the read phase. */
  void targetRead() throws Exception {
    FaultProxy.Kind kind = FaultProxy.Kind.parse(Harness.prop("os.fault", "RESET_RESP"));
    int nth = Harness.intProp("os.nth", 1);
    long k = Long.parseLong(Harness.prop("os.k", String.valueOf(100 * 1024)));
    for (boolean fault : new boolean[] {true, false}) {
      Map<String, Integer> agg = new TreeMap<>();
      int injected = 0;
      for (int i = 0; i < runs; i++) {
        Run.Config c = base(seed0 + i);
        c.commits = 2;
        c.sizeMode = "mid";
        c.beforeRead = (run, x) -> {
          String target = null;
          for (Map.Entry<String, Run.FileRec> e : run.ledger.entrySet()) {
            if (e.getValue().len >= 200 * 1024 && run.committedData().contains(e.getKey())) {
              target = run.key(e.getKey());
              break;
            }
          }
          if (target != null && fault) {
            final String t = target;
            env.proxy.setDecider(new Harness.Targeted(
                r -> r.op.equals("GetObject") && r.key.equals(t), nth, 1, kind, k, 4500));
          }
        };
        Run r = one(c, null, "target-read-" + (fault ? "fault" : "control"));
        injected += env.proxy.faults().size();
        add(agg, r.v);
      }
      System.out.printf("target-read v%s client=%s kind=%s nth=%d k=%d %s runs=%d injected=%d counts=%s%n",
          env.version, client, kind, nth, k, fault ? "FAULT" : "CONTROL", runs, injected, agg);
    }
  }

  // ------------------------------------------------------------------ targeted: premature FIN on a manifest GET

  /**
   * Arms a FIN_RESP on the first GET of an existing data manifest, cutting exactly at an Avro block
   * boundary. merge=true: during the commit that merges manifests; merge=false: during scan planning.
   */
  void finMerge(boolean merge) throws Exception {
    String label = merge ? "fin-merge" : "fin-scan";
    for (String arm : new String[] {"FAULT", "CONTROL"}) {
      Map<String, Integer> agg = new TreeMap<>();
      int majorRuns = 0;
      int armed = 0;
      int effective = 0;
      for (int i = 0; i < runs; i++) {
        Run.Config c = base(seed0 + i);
        c.bigData = false;
        c.rowDeltaProb = 0;
        c.boundBytes = 2048; // ~80 KiB/entry -> several Avro blocks (64 KB sync interval) per manifest
        if (merge) {
          c.commits = 3;
          c.mergeMinCount = 3;
          c.readPhase = false;
        } else {
          c.commits = 2;
          c.mergeMinCount = 1000;
          c.readPhase = true;
        }
        final boolean[] didArm = new boolean[1];
        Run.Hook arm0 = (run, x) -> {
          if (!"FAULT".equals(arm)) {
            return;
          }
          if (merge && x != 2) {
            return;
          }
          Object[] target = pickManifestBoundary(run);
          if (target == null) {
            return;
          }
          didArm[0] = true;
          String key = (String) target[0];
          long boundary = (Long) target[1];
          env.proxy.setDecider(new Harness.Targeted(r -> r.op.equals("GetObject") && r.key.equals(key), 1, 1000,
              FaultProxy.Kind.FIN_RESP, boundary, 0));
        };
        if (merge) {
          c.beforeOp = arm0;
        } else {
          c.beforeRead = arm0;
        }
        Run r = one(c, null, label + "-" + arm);
        armed += didArm[0] ? 1 : 0;
        for (FaultProxy.Req q : env.proxy.faults()) {
          if (q.effect.startsWith("fin-after-")) {
            effective++;
          }
        }
        add(agg, r.v);
        majorRuns += r.v.keySet().stream().anyMatch(k -> k.startsWith("MAJOR_")) ? 1 : 0;
      }
      System.out.printf("%s v%s client=%s %s runs=%d armed=%d finInjected=%d majorRuns=%d counts=%s%n", label,
          env.version, client, arm, runs, armed, effective, majorRuns, agg);
    }
  }

  /** Returns {key, boundaryOffset} for the first data manifest of the current snapshot with >= 2 blocks. */
  Object[] pickManifestBoundary(Run run) throws Exception {
    Catalog clean = Env.jdbc(new java.io.File(run.dir, "cat.db"), run.warehouse,
        env.s3Props(false, "apache", run.staging));
    try {
      Table t = clean.loadTable(run.id);
      if (t.currentSnapshot() == null) {
        return null;
      }
      for (ManifestFile mf : t.currentSnapshot().allManifests(t.io())) {
        if (mf.content() != ManifestContent.DATA) {
          continue;
        }
        String key = run.key(mf.path());
        byte[] bytes = get(key);
        long[] ends = AvroBlocks.blockEnds(bytes);
        String kmode = Harness.prop("os.k", "boundary");
        if (kmode.equals("random")) {
          return new Object[] {key, 1 + new SplittableRandom(run.cfg.seed).nextLong(bytes.length - 1)};
        } else if (!kmode.equals("boundary")) {
          throw new IllegalArgumentException("Unknown os.k for fin modes: " + kmode);
        }
        if (ends.length >= 2) {
          return new Object[] {key, ends[0]}; // keep header + first block, drop the rest
        }
      }
      return null;
    } finally {
      Env.closeQuietly(clean);
    }
  }
}
