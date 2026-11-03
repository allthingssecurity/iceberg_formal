package objstore;

import java.io.File;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.TreeMap;
import java.util.function.Predicate;

/**
 * Entry point. -Dos.mode=... selects the experiment; unknown modes/kinds/clients throw.
 *
 * <p>Output: one summary line per (mode, arm) on stdout, and a bounded detail file under
 * build/work/v<version>/<mode>-details.txt (at most 40 lines per run, at most 30 runs with details).
 */
public final class Harness {

  static String prop(String name, String def) {
    String v = System.getProperty(name);
    return v == null ? def : v;
  }

  static int intProp(String name, int def) {
    return Integer.parseInt(prop(name, String.valueOf(def)));
  }

  /** Fault on the nth..(nth+burst-1) matching request. */
  static final class Targeted implements FaultProxy.Decider {
    final Predicate<FaultProxy.Req> match;
    final int nth;
    final int burst;
    final FaultProxy.Kind kind;
    final long k;
    final long stallMs;
    int seen = 0;

    Targeted(Predicate<FaultProxy.Req> match, int nth, int burst, FaultProxy.Kind kind, long k, long stallMs) {
      this.match = match;
      this.nth = nth;
      this.burst = burst;
      this.kind = kind;
      this.k = k;
      this.stallMs = stallMs;
    }

    @Override
    public FaultProxy.Fault decide(FaultProxy.Req r) {
      if (!match.test(r)) {
        return null;
      }
      seen++;
      if (seen >= nth && seen < nth + burst) {
        return new FaultProxy.Fault(kind, k, stallMs);
      }
      return null;
    }
  }

  /** Seeded per-request faults over the write and read ops S3FileIO issues. */
  static final class RateDecider implements FaultProxy.Decider {
    static final Set<String> WRITE_OPS = Set.of("PutObject", "CreateMultipartUpload", "UploadPart",
        "CompleteMultipartUpload", "AbortMultipartUpload", "DeleteObjects", "DeleteObject");
    static final Set<String> READ_OPS = Set.of("GetObject", "HeadObject");
    final SplittableRandom rnd;
    final double p;
    final int maxFaults;
    final List<FaultProxy.Kind> kinds;
    final int maxBurst;
    int faults = 0;
    String burstKey = null;
    int burstLeft = 0;
    FaultProxy.Kind burstKind;

    RateDecider(long seed, double p, int maxFaults, List<FaultProxy.Kind> kinds, int maxBurst) {
      this.rnd = new SplittableRandom(seed * 7919 + 13);
      this.p = p;
      this.maxFaults = maxFaults;
      this.kinds = kinds;
      this.maxBurst = maxBurst;
    }

    @Override
    public FaultProxy.Fault decide(FaultProxy.Req r) {
      boolean write = WRITE_OPS.contains(r.op);
      boolean read = READ_OPS.contains(r.op);
      if (!write && !read) {
        return null;
      }
      String bk = r.op + " " + r.key + " " + r.partNumber;
      if (burstLeft > 0 && bk.equals(burstKey)) {
        burstLeft--;
        return make(burstKind, r);
      }
      if (faults >= maxFaults || rnd.nextDouble() >= p) {
        return null;
      }
      List<FaultProxy.Kind> ok = new ArrayList<>();
      for (FaultProxy.Kind k : kinds) {
        boolean respKind = k == FaultProxy.Kind.RESET_RESP || k == FaultProxy.Kind.FIN_RESP;
        if (respKind && !"GetObject".equals(r.op)) {
          continue;
        }
        ok.add(k);
      }
      if (ok.isEmpty()) {
        return null;
      }
      faults++;
      FaultProxy.Kind k = ok.get(rnd.nextInt(ok.size()));
      int b = 1 + rnd.nextInt(maxBurst);
      if (b > 1) {
        burstKey = bk;
        burstLeft = b - 1;
        burstKind = k;
      }
      return make(k, r);
    }

    FaultProxy.Fault make(FaultProxy.Kind k, FaultProxy.Req r) {
      long kb;
      if (k == FaultProxy.Kind.CUT_REQ) {
        kb = r.bodyLen > 1 ? rnd.nextLong(r.bodyLen) : 0;
      } else {
        kb = rnd.nextInt(4) == 0 ? rnd.nextInt(64) : rnd.nextInt(1 << 20);
      }
      return new FaultProxy.Fault(k, kb, 3500 + rnd.nextInt(1500));
    }
  }

  public static void main(String[] args) throws Exception {
    String mode = prop("os.mode", null);
    if (mode == null) {
      throw new IllegalArgumentException("set -Dos.mode");
    }
    int runs = intProp("os.runs", 5);
    long seed0 = Long.parseLong(prop("os.seed", "1"));
    String client = prop("os.client", "default");
    Set<String> clients = Set.of("default", "apache", "urlconnection");
    if (!clients.contains(client)) {
      throw new IllegalArgumentException("Unknown os.client: " + client);
    }
    Set<String> modes = Set.of("smoke", "control", "calib-put", "calib-read", "calib-oracle", "explore",
        "fin-merge", "fin-scan", "target", "target-read");
    if (!modes.contains(mode)) {
      throw new IllegalArgumentException("Unknown os.mode: " + mode + " (known: " + modes + ")");
    }
    try (Env env = new Env()) {
      File ddir = new File(System.getProperty("resultsdir"));
      ddir.mkdirs();
      String tag = String.join("_", prop("os.spec", ""), prop("os.fault", ""), prop("os.nth", ""))
          .replaceAll("[^A-Za-z0-9_]+", "-");
      File details = new File(ddir, "v" + env.version + "-" + mode + "-" + client + (tag.matches("_*") ? "" : "-" + tag)
          + ".txt");
      try (PrintStream det = new PrintStream(details, StandardCharsets.UTF_8)) {
        Experiments ex = new Experiments(env, det, client, seed0, runs);
        switch (mode) {
          case "smoke":
            ex.smoke();
            break;
          case "control":
            ex.control();
            break;
          case "calib-put":
            ex.calibPut();
            break;
          case "calib-read":
            ex.calibRead();
            break;
          case "calib-oracle":
            ex.calibOracle();
            break;
          case "explore":
            ex.explore();
            break;
          case "fin-merge":
            ex.finMerge(true);
            break;
          case "fin-scan":
            ex.finMerge(false);
            break;
          case "target":
            ex.target();
            break;
          case "target-read":
            ex.targetRead();
            break;
          default:
            throw new IllegalStateException(mode);
        }
      }
      System.out.println("details: " + details);
    }
  }

  static final Set<String> OPS = Set.of("PutObject", "CreateMultipartUpload", "UploadPart",
      "CompleteMultipartUpload", "AbortMultipartUpload", "DeleteObjects", "DeleteObject", "GetObject", "HeadObject");
  static final Set<String> KEYS = Set.of("data", "manifest", "manifestlist", "metadata", "any");

  static boolean keyClass(String cls, String key) {
    switch (cls) {
      case "data":
        return key.contains("/data/");
      case "manifest":
        return key.contains("/metadata/") && key.endsWith(".avro") && !key.contains("/snap-");
      case "manifestlist":
        return key.contains("/metadata/snap-");
      case "metadata":
        return key.endsWith(".metadata.json");
      case "any":
        return true;
      default:
        throw new IllegalArgumentException("Unknown key class: " + cls);
    }
  }

  /** os.spec = op:kind:keyClass:nth:burst:k[,more] ; each rule counts matches independently. */
  static FaultProxy.Decider parseSpec(String spec) {
    List<Targeted> rules = new ArrayList<>();
    for (String r : spec.split(",")) {
      String[] f = r.trim().split(":");
      if (f.length != 6) {
        throw new IllegalArgumentException("bad rule (op:kind:keyClass:nth:burst:k): " + r);
      }
      String op = f[0];
      if (!OPS.contains(op)) {
        throw new IllegalArgumentException("Unknown op: " + op);
      }
      FaultProxy.Kind kind = FaultProxy.Kind.parse(f[1]);
      String kc = f[2];
      if (!KEYS.contains(kc)) {
        throw new IllegalArgumentException("Unknown key class: " + kc);
      }
      rules.add(new Targeted(q -> q.op.equals(op) && keyClass(kc, q.key), Integer.parseInt(f[3]),
          Integer.parseInt(f[4]), kind, Long.parseLong(f[5]), 4500));
    }
    return q -> {
      for (Targeted t : rules) {
        FaultProxy.Fault x = t.decide(q);
        if (x != null) {
          return x;
        }
      }
      return null;
    };
  }

  static String fmt(Map<String, Integer> m) {
    return new TreeMap<>(m).toString();
  }

  static String lower(String s) {
    return s.toLowerCase(Locale.ROOT);
  }
}
