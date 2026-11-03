package r5spark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

/**
 * D-2: streaming idempotency (SparkWrite.BaseStreamingWrite.commit) walks main's ancestry, while
 * the commit goes to a branch (option "branch") or WAP branch (spark.wap.branch). A driver failure
 * between the Iceberg commit and Spark's commits/N write is simulated by deleting commits/N.
 */
public class D2StreamingBranchReplayTest {
  static final int ROWS = 10;
  static final StringBuilder EPOCHS = new StringBuilder();

  record Outcome(String target, boolean replay, int run, boolean commitLogDeleted,
      boolean commitLogRewritten, int streamingSnapshots, long rowsOnTarget, long rowsOnMain) {}

  @Test
  public void replayedEpoch() throws Exception {
    int runs = Integer.parseInt(System.getProperty("r5.runs", "3"));
    Path wh = Spark.tmp("d2-wh");
    SparkSession spark = Spark.session(wh, Map.of());
    spark.sql("CREATE NAMESPACE IF NOT EXISTS x.db");
    StructType schema = new StructType().add("id", DataTypes.IntegerType).add("data", DataTypes.StringType);

    List<Outcome> outcomes = new ArrayList<>();
    String[] targets = {"start-branch-option", "start-wap-branch", "start-main", "toTable-branch-option", "toTable-wap-branch", "toTable-main"};
    for (int run = 0; run < runs; run++) {
      for (String target : targets) {
        for (boolean replay : new boolean[] {true, false}) {
          if (!List.of(targets).contains(target)) {
            throw new IllegalArgumentException("unknown target " + target);
          }
          String t = "x.db.s_" + target.replace('-', '_') + "_" + (replay ? "replay" : "noreplay") + "_" + run;
          spark.sql("CREATE TABLE " + t + " (id int, data string) USING iceberg "
              + "TBLPROPERTIES ('format-version'='2'"
              + (target.endsWith("wap-branch") ? ", 'write.wap.enabled'='true'" : "") + ")");
          // main gets a snapshot of its own so the ancestry walk has something to walk
          spark.sql("INSERT INTO " + t + " VALUES (-1, 'seed')");
          if (!target.endsWith("main")) {
            spark.sql("ALTER TABLE " + t + " CREATE BRANCH b");
          }
          Path in = Spark.tmp("d2-in");
          Path ckpt = Spark.tmp("d2-ckpt");
          spark.range(ROWS).selectExpr("cast(id as int) id", "cast(id as string) data")
              .coalesce(1).write().mode("overwrite").parquet(in.resolve("f0").toString());

          if (target.endsWith("wap-branch")) {
            spark.conf().set("spark.wap.branch", "b");
          }
          runOnce(spark, schema, in, ckpt, t, target);
          Path commit0 = ckpt.resolve("commits").resolve("0");
          assertTrue(Files.exists(commit0), "batch 0 must have been committed by Spark");
          boolean deleted = false;
          if (replay) {
            Files.delete(commit0);
            Files.deleteIfExists(ckpt.resolve("commits").resolve(".0.crc"));
            deleted = !Files.exists(commit0);
          }
          runOnce(spark, schema, in, ckpt, t, target);
          boolean rewritten = Files.exists(commit0);
          if (target.endsWith("wap-branch")) {
            spark.conf().unset("spark.wap.branch");
          }

          Table table = Spark3Util.loadIcebergTable(spark, t);
          table.refresh();
          int streaming = 0;
          StringBuilder epochs = new StringBuilder();
          for (Snapshot s : table.snapshots()) {
            if (s.summary().containsKey("spark.sql.streaming.queryId")) {
              streaming++;
              epochs.append(s.summary().get("spark.sql.streaming.queryId").substring(0, 8)).append('/')
                  .append(s.summary().get("spark.sql.streaming.epochId")).append(' ');
            }
          }
          if (EPOCHS.length() < 4000) EPOCHS.append(t).append(": ").append(epochs).append('\n');
          long onTarget = target.endsWith("main") ? -1 : spark.sql("SELECT count(*) FROM " + t
              + ".branch_b WHERE id >= 0").collectAsList().get(0).getLong(0);
          long onMain = spark.sql("SELECT count(*) FROM " + t + " WHERE id >= 0")
              .collectAsList().get(0).getLong(0);
          outcomes.add(new Outcome(target, replay, run, deleted, rewritten, streaming, onTarget, onMain));
        }
      }
    }

    StringBuilder sb = new StringBuilder();
    sb.append("iceberg=").append(System.getProperty("icebergVersion"))
        .append(" spark=").append(System.getProperty("sparkVersion")).append('\n');
    sb.append("target\treplay\trun\tcommitLogDeleted\tcommitLogRewritten\tstreamingSnapshots\trowsOnBranchB\trowsOnMain\n");
    for (Outcome o : outcomes) {
      sb.append(o.target).append('\t').append(o.replay).append('\t').append(o.run).append('\t')
          .append(o.commitLogDeleted).append('\t').append(o.commitLogRewritten).append('\t')
          .append(o.streamingSnapshots).append('\t').append(o.rowsOnTarget).append('\t')
          .append(o.rowsOnMain).append('\n');
    }
    for (String target : targets) {
      for (boolean replay : new boolean[] {true, false}) {
        long dup = outcomes.stream().filter(o -> o.target.equals(target) && o.replay == replay
            && (o.rowsOnTarget > ROWS || o.rowsOnMain > ROWS)).count();
        long n = outcomes.stream().filter(o -> o.target.equals(target) && o.replay == replay).count();
        sb.append("SUMMARY ").append(target).append(replay ? " replay" : " no-replay")
            .append(": duplicated rows (branch b or main) ").append(dup).append('/').append(n).append('\n');
      }
    }
    sb.append("streaming snapshots (queryId-prefix/epochId) per table:\n").append(EPOCHS);
    Spark.report("d2-" + System.getProperty("sparkLine") + ".tsv", sb.toString());
    System.err.println(sb);
    spark.stop();
    for (Outcome o : outcomes) {
      assertEquals(o.replay, o.commitLogDeleted, "fault must be injected exactly when asked: " + o);
      assertTrue(o.commitLogRewritten, "restart must re-commit batch 0 to Spark's log: " + o);
    }
  }

  private static void runOnce(SparkSession spark, StructType schema, Path in, Path ckpt, String table,
      String target) throws Exception {
    DataStreamWriter<org.apache.spark.sql.Row> w = spark.readStream().schema(schema)
        .parquet(in.toString() + "/*")
        .writeStream().format("iceberg").outputMode("append")
        .option("checkpointLocation", ckpt.toString())
        .trigger(Trigger.AvailableNow());
    StreamingQuery q;
    switch (target) {
      case "start-branch-option": q = w.option("branch", "b").start(table); break;
      case "start-wap-branch": case "start-main": q = w.start(table); break;
      case "toTable-branch-option": q = w.option("branch", "b").toTable(table); break;
      case "toTable-wap-branch": case "toTable-main": q = w.toTable(table); break;
      default: throw new IllegalArgumentException("unknown target " + target);
    }
    q.awaitTermination();
  }
}
