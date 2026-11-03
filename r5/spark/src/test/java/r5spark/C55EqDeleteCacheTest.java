package r5spark;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

/**
 * C-55 / D-36: the executor-wide equality-delete cache is keyed by table name + delete file
 * location, but the cached rows are materialized with the scan's projection types. A time-travel
 * scan (id:int) and a current scan (id:long after promotion) share one entry.
 */
public class C55EqDeleteCacheTest {
  // data ids 0..9 (int); eq deletes id in (3, 7); after promotion one more row id=100.
  static final String TT_EXPECTED = "count=8 sum=35";
  static final String CUR_EXPECTED = "count=9 sum=135";

  @Test
  public void cacheAcrossSchemaVersions() throws Exception {
    int runs = Integer.parseInt(System.getProperty("r5.runs", "3"));
    SparkSession spark = Spark.session(Spark.tmp("c55-wh"), Map.of());
    spark.sql("CREATE NAMESPACE IF NOT EXISTS x.db");
    StringBuilder sb = new StringBuilder("iceberg=" + System.getProperty("icebergVersion")
        + " spark=" + System.getProperty("sparkVersion") + "\n"
        + "ordering\tcache\trun\tq1\tq1result\tq2\tq2result\tcacheEntriesForTableBeforeQ2\n");
    String[] orderings = {"tt-then-current", "current-then-tt", "concurrent", "tt-then-tt", "current-then-current"};
    List<String> rows = new ArrayList<>();
    for (int run = 0; run < runs; run++) {
      for (String ordering : orderings) {
        for (boolean cache : new boolean[] {true, false}) {
          String t = "x.db.e_" + ordering.replace('-', '_') + "_" + (cache ? "on" : "off") + "_" + run;
          long ttSnapshot = setup(spark, t);
          spark.conf().set("spark.sql.iceberg.executor-cache.enabled", Boolean.toString(cache));
          String tt = "SELECT count(*), sum(id) FROM " + t + " VERSION AS OF " + ttSnapshot;
          String cur = "SELECT count(*), sum(id) FROM " + t;
          String q1;
          String q2;
          switch (ordering) {
            case "tt-then-current": case "concurrent": q1 = "tt"; q2 = "current"; break;
            case "current-then-tt": q1 = "current"; q2 = "tt"; break;
            case "tt-then-tt": q1 = "tt"; q2 = "tt"; break;
            case "current-then-current": q1 = "current"; q2 = "current"; break;
            default: throw new IllegalArgumentException("unknown ordering " + ordering);
          }
          String r1;
          String r2;
          int entriesBeforeQ2 = -1;
          if (ordering.equals("concurrent")) {
            ExecutorService pool = Executors.newFixedThreadPool(2);
            CountDownLatch go = new CountDownLatch(1);
            Future<String> f1 = pool.submit(() -> { go.await(); return run(spark, tt); });
            Future<String> f2 = pool.submit(() -> { go.await(); return run(spark, cur); });
            go.countDown();
            r1 = f1.get();
            r2 = f2.get();
            pool.shutdown();
          } else {
            r1 = run(spark, q1.equals("tt") ? tt : cur);
            entriesBeforeQ2 = cachedEntries(t);
            r2 = run(spark, q2.equals("tt") ? tt : cur);
          }
          boolean ok1 = r1.equals(q1.equals("tt") ? TT_EXPECTED : CUR_EXPECTED);
          boolean ok2 = r2.equals(q2.equals("tt") ? TT_EXPECTED : CUR_EXPECTED);
          String line = ordering + "\t" + cache + "\t" + run + "\t" + q1 + "\t" + r1 + (ok1 ? "" : " WRONG")
              + "\t" + q2 + "\t" + r2 + (ok2 ? "" : " WRONG") + "\t" + entriesBeforeQ2;
          rows.add(line);
          sb.append(line).append('\n');
        }
      }
    }
    for (String ordering : orderings) {
      for (String cache : new String[] {"true", "false"}) {
        long n = rows.stream().filter(l -> l.startsWith(ordering + "\t" + cache + "\t")).count();
        long bad = rows.stream().filter(l -> l.startsWith(ordering + "\t" + cache + "\t") && l.contains("WRONG")).count();
        sb.append("SUMMARY ").append(ordering).append(" cache=").append(cache).append(": wrong ")
            .append(bad).append('/').append(n).append('\n');
      }
    }
    Spark.report("c55-" + System.getProperty("sparkLine") + ".tsv", sb.toString());
    spark.stop();
  }

  /** Entries in the executor cache whose key starts with this table's group (reflection, read-only). */
  @SuppressWarnings("unchecked")
  private static int cachedEntries(String table) {
    try {
      Object cache = org.apache.iceberg.spark.SparkExecutorCache.get();
      if (cache == null) return 0;
      java.lang.reflect.Field f = cache.getClass().getDeclaredField("state");
      f.setAccessible(true);
      Object state = f.get(cache);
      if (state == null) return 0;
      java.util.Map<String, ?> m = (java.util.Map<String, ?>) Class.forName(
          "org.apache.iceberg.shaded.com.github.benmanes.caffeine.cache.Cache").getMethod("asMap").invoke(state);
      String prefix = table.substring(table.indexOf('.') + 1);
      int n = 0;
      for (String k : m.keySet()) {
        if (k.contains(prefix + "_")) n++;
      }
      return n;
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  private static String run(SparkSession spark, String sql) {
    try {
      Row r = spark.sql(sql).collectAsList().get(0);
      return "count=" + r.getLong(0) + " sum=" + (r.isNullAt(1) ? "null" : r.getLong(1));
    } catch (Throwable e) {
      return "FAILED " + Spark.chain(e);
    }
  }

  /** Returns the snapshot id that has the int schema and the eq deletes. */
  private static long setup(SparkSession spark, String t) throws Exception {
    spark.sql("CREATE TABLE " + t + " (id int, data string) USING iceberg "
        + "TBLPROPERTIES ('format-version'='2')");
    spark.range(10).selectExpr("cast(id as int) id", "cast(id as string) data").coalesce(1)
        .writeTo(t).append();
    Table table = Spark3Util.loadIcebergTable(spark, t);
    table.refresh();
    Schema deleteSchema = table.schema().select("id");
    int idField = table.schema().findField("id").fieldId();
    GenericFileWriterFactory factory = new GenericFileWriterFactory.Builder(table)
        .equalityFieldIds(new int[] {idField})
        .equalityDeleteRowSchema(deleteSchema)
        .build();
    String path = table.locationProvider().newDataLocation("eqdel-" + UUID.randomUUID() + ".parquet");
    EqualityDeleteWriter<Record> w = factory.newEqualityDeleteWriter(
        EncryptedFiles.plainAsEncryptedOutput(table.io().newOutputFile(path)), table.spec(), null);
    for (int id : new int[] {3, 7}) {
      Record rec = GenericRecord.create(deleteSchema);
      rec.setField("id", id);
      w.write(rec);
    }
    w.close();
    DeleteFile df = w.toDeleteFile();
    table.newRowDelta().addDeletes(df).commit();
    table.refresh();
    long ttSnapshot = table.currentSnapshot().snapshotId();
    assertEquals(table.schemas().get(table.currentSnapshot().schemaId()).findField("id").type(),
        Types.IntegerType.get());
    table.updateSchema().updateColumn("id", Types.LongType.get()).commit();
    spark.sql("INSERT INTO " + t + " VALUES (100L, 'new')");
    return ttSnapshot;
  }
}
