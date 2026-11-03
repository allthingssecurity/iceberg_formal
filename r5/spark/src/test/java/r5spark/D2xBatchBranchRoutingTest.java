package r5spark;

import java.nio.file.Path;
import java.util.Map;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

/** Side check for D-2: where do BATCH writes with option branch / spark.wap.branch land? */
public class D2xBatchBranchRoutingTest {
  @Test
  public void batchRouting() throws Exception {
    Path wh = Spark.tmp("d2x-wh");
    SparkSession spark = Spark.session(wh, Map.of());
    spark.sql("CREATE NAMESPACE IF NOT EXISTS x.db");
    StringBuilder sb = new StringBuilder("iceberg=" + System.getProperty("icebergVersion")
        + " spark=" + System.getProperty("sparkVersion") + "\nroute\trowsOnB\trowsOnMain\n");
    String[] routes = {"writeTo-option-branch", "insert-wap-branch"};
    for (String route : routes) {
      String t = "x.db.b_" + route.replace('-', '_');
      spark.sql("CREATE TABLE " + t + " (id int) USING iceberg TBLPROPERTIES ('format-version'='2', "
          + "'write.wap.enabled'='true')");
      spark.sql("INSERT INTO " + t + " VALUES (-1)");
      spark.sql("ALTER TABLE " + t + " CREATE BRANCH b");
      switch (route) {
        case "writeTo-option-branch":
          spark.range(5).selectExpr("cast(id as int) id").writeTo(t).option("branch", "b").append();
          break;
        case "insert-wap-branch":
          spark.conf().set("spark.wap.branch", "b");
          spark.sql("INSERT INTO " + t + " VALUES (0),(1),(2),(3),(4)");
          spark.conf().unset("spark.wap.branch");
          break;
        default:
          throw new IllegalArgumentException("unknown route " + route);
      }
      long b = spark.sql("SELECT count(*) FROM " + t + ".branch_b WHERE id >= 0").first().getLong(0);
      long m = spark.sql("SELECT count(*) FROM " + t + " WHERE id >= 0").first().getLong(0);
      sb.append(route).append('\t').append(b).append('\t').append(m).append('\n');
    }
    Spark.report("d2x-" + System.getProperty("sparkLine") + ".tsv", sb.toString());
    spark.stop();
  }
}
