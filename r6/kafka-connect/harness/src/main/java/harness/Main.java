package harness;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.inmemory.SharedCatalog;

/**
 * usage: Main <scenario> <runs> [key=value ...]
 *
 * <p>Prints one line per run and a summary "BAD k/N". Unknown scenarios / options throw.
 */
public final class Main {
  interface Scenario {
    World run(Map<String, String> opt);
  }

  static final Map<String, Scenario> SCENARIOS = new HashMap<>();

  static {
    SCENARIOS.put("smoke", Scenarios::smoke);
    SCENARIOS.put("icr-close", Scenarios::icrClose);
    SCENARIOS.put("icr-nocommit", Scenarios::icrNoCommit);
    SCENARIOS.put("coord-latest", Scenarios::coordLatest);
    SCENARIOS.put("fault", Scenarios::fault);
    SCENARIOS.put("zombie-rewind", Scenarios::zombieRewind);
  }

  public static void main(String[] args) throws Exception {
    String name = args[0];
    int runs = Integer.parseInt(args[1]);
    Map<String, String> opt = new HashMap<>();
    for (int i = 2; i < args.length; i++) {
      String[] kv = args[i].split("=", 2);
      opt.put(kv[0], kv[1]);
    }
    Scenario sc = SCENARIOS.get(name);
    if (sc == null) {
      throw new IllegalArgumentException("unknown scenario " + name + " (known: " + SCENARIOS.keySet() + ")");
    }
    String cls = Class.forName("org.apache.iceberg.connect.channel.KafkaClientFactory")
        .getProtectionDomain().getCodeSource().getLocation().toString();
    if (cls.contains("iceberg-kafka-connect-")) {
      throw new IllegalStateException("shadow KafkaClientFactory not first on classpath: " + cls);
    }
    String ver = org.apache.iceberg.connect.IcebergSinkTask.class.getProtectionDomain().getCodeSource().getLocation().toString();
    ver = ver.substring(ver.lastIndexOf('/') + 1);
    int bad = 0;
    boolean verbose = Boolean.parseBoolean(opt.getOrDefault("verbose", "false"));
    for (int r = 1; r <= runs; r++) {
      World w = sc.run(opt);
      Map<String, World.Verdict> vs = w.verdicts();
      boolean b = vs.values().stream().anyMatch(World.Verdict::bad);
      if (b) {
        bad++;
      }
      System.out.println(
          "[" + ver + "] " + name + " run " + r + ": " + (b ? "BAD " : "ok  ") + vs
              + " committedSrc=" + w.committedSource() + " failures=" + w.emu.failures
              + " injected=" + SharedCatalog.faultLog.size());
      if (verbose || (b && r == 1) || (r == 1 && w.emu.firstFailure != null)) {
        dump(w);
      }
    }
    System.out.println("SUMMARY [" + ver + "] " + name + " " + opt + " BAD " + bad + "/" + runs);
    System.out.flush();
    Runtime.getRuntime().halt(0); // parked coordinator threads of crashed tasks never exit
  }

  static void dump(World w) {
    List<String> t = w.emu.trace;
    System.out.println("  -- emu trace (" + t.size() + ")");
    t.stream().limit(80).forEach(s -> System.out.println("     " + s));
    List<String> e = w.kafka.events;
    System.out.println("  -- kafka events (" + e.size() + ")");
    e.stream().filter(s -> !s.startsWith("initTransactions")).limit(80).forEach(s -> System.out.println("     " + s));
    System.out.println("  -- catalog faults " + SharedCatalog.faultLog);
    System.out.println("  -- first task failure:" + w.emu.firstFailure);
  }

  static int intOpt(Map<String, String> o, String k, int d) {
    return o.containsKey(k) ? Integer.parseInt(o.get(k)) : d;
  }

  static <T> T enumOpt(Map<String, String> o, String k, String d, Function<String, T> f) {
    return f.apply(o.getOrDefault(k, d));
  }
}
