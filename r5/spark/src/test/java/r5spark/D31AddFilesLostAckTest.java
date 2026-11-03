package r5spark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

/**
 * D-31: add_files (SparkTableUtil.importSparkPartitions) deletes the staged manifests on any
 * Throwable from append.commit(), including CommitStateUnknownException after a commit that landed.
 */
public class D31AddFilesLostAckTest {
  static final int ROWS = 100;
  static final long SUM = (long) ROWS * (ROWS - 1) / 2;

  record Outcome(String mode, int run, boolean threw, boolean csue, String chain, int injected,
      boolean hasSnapshot, int manifests, int missingManifests, String query) {}

  @Test
  public void addFilesUnderLostAck() throws Exception {
    int runs = Integer.parseInt(System.getProperty("r5.runs", "3"));
    Path wh = Spark.tmp("d31-wh");
    Path src = Spark.tmp("d31-src");
    SparkSession spark = Spark.session(wh, Map.of());
    String srcDir = src.resolve("parquet").toString();
    spark.range(ROWS).selectExpr("cast(id as int) id", "cast(id as string) data")
        .repartition(4).write().parquet(srcDir);
    spark.sql("CREATE NAMESPACE IF NOT EXISTS x.db");

    List<Outcome> outcomes = new ArrayList<>();
    String[] modes = {"v2-fault", "v2-nofault", "v1-fault", "v1-nofault"};
    for (int run = 0; run < runs; run++) {
      for (String mode : modes) {
        int fv;
        boolean fault;
        switch (mode) {
          case "v2-fault": fv = 2; fault = true; break;
          case "v2-nofault": fv = 2; fault = false; break;
          case "v1-fault": fv = 1; fault = true; break;
          case "v1-nofault": fv = 1; fault = false; break;
          default: throw new IllegalArgumentException("unknown mode " + mode);
        }
        String t = "db.t_" + mode.replace('-', '_') + "_" + run;
        spark.sql("CREATE TABLE x." + t + " (id int, data string) USING iceberg "
            + "TBLPROPERTIES ('format-version'='" + fv + "')");
        int before = FaultyHadoopCatalog.INJECTED.get();
        FaultyHadoopCatalog.ARMED.set(fault ? 1 : 0);
        boolean threw = false;
        boolean csue = false;
        String chain = "";
        try {
          spark.sql("CALL x.system.add_files(table => '" + t + "', source_table => '`parquet`.`"
              + srcDir + "`')").collectAsList();
        } catch (Throwable e) {
          threw = true;
          csue = Spark.causedBy(e, CommitStateUnknownException.class);
          chain = Spark.chain(e);
        }
        FaultyHadoopCatalog.ARMED.set(0);
        int injected = FaultyHadoopCatalog.INJECTED.get() - before;

        Table table = Spark3Util.loadIcebergTable(spark, "x." + t);
        table.refresh();
        Snapshot snap = table.currentSnapshot();
        int manifests = 0;
        int missing = 0;
        String missingName = "-";
        boolean listExists = false;
        if (snap != null) {
          listExists = table.io().newInputFile(snap.manifestListLocation()).exists();
          for (ManifestFile m : snap.allManifests(table.io())) {
            manifests++;
            if (!table.io().newInputFile(m.path()).exists()) {
              missing++;
              missingName = m.path().substring(m.path().lastIndexOf('/') + 1);
            }
          }
        }
        String query;
        try {
          Row r = spark.sql("SELECT count(*), sum(id) FROM x." + t).collectAsList().get(0);
          query = "count=" + r.getLong(0) + " sum=" + (r.isNullAt(1) ? "null" : r.getLong(1));
        } catch (Throwable e) {
          query = "FAILED " + Spark.chain(e);
        }
        outcomes.add(new Outcome(mode, run, threw, csue, chain, injected, snap != null, manifests,
            missing, query + " manifestListExists=" + listExists + " missingManifest=" + missingName));
      }
    }

    StringBuilder sb = new StringBuilder();
    sb.append("iceberg=").append(System.getProperty("icebergVersion"))
        .append(" spark=").append(System.getProperty("sparkVersion")).append('\n');
    sb.append("mode\trun\tthrew\tcsue\tinjected\tsnapshot\tmanifests\tmissing\tquery\tchain\n");
    for (Outcome o : outcomes) {
      sb.append(o.mode).append('\t').append(o.run).append('\t').append(o.threw).append('\t')
          .append(o.csue).append('\t').append(o.injected).append('\t').append(o.hasSnapshot)
          .append('\t').append(o.manifests).append('\t').append(o.missingManifests).append('\t')
          .append(o.query).append('\t').append(o.chain).append('\n');
    }
    String expected = "count=" + ROWS + " sum=" + SUM;
    int[] bad = new int[modes.length];
    int[] total = new int[modes.length];
    for (Outcome o : outcomes) {
      int i = List.of(modes).indexOf(o.mode);
      total[i]++;
      if (o.missingManifests > 0 || !o.query.startsWith(expected + " ")) bad[i]++;
    }
    for (int i = 0; i < modes.length; i++) {
      sb.append("SUMMARY ").append(modes[i]).append(": broken (missing manifest or wrong/failed query) ")
          .append(bad[i]).append('/').append(total[i]).append('\n');
    }
    Spark.report("d31-" + System.getProperty("sparkLine") + ".tsv", sb.toString());
    System.err.println(sb);
    spark.stop();

    // Harness sanity: faults injected exactly where armed.
    for (Outcome o : outcomes) {
      assertEquals(o.mode.endsWith("-fault") ? 1 : 0, o.injected, o.toString());
      assertTrue(o.hasSnapshot, "commit must have landed: " + o);
    }
  }
}
