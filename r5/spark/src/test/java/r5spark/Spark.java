package r5spark;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.apache.spark.sql.SparkSession;

final class Spark {
  private Spark() {}

  static SparkSession session(Path warehouse, Map<String, String> extra) {
    SparkSession.Builder b =
        SparkSession.builder()
            .master("local[2]")
            .appName("r5-spark")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.x", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.x.catalog-impl", FaultyHadoopCatalog.class.getName())
            .config("spark.sql.catalog.x.warehouse", warehouse.toUri().toString())
            .config("spark.sql.catalog.x.cache-enabled", "false");
    extra.forEach(b::config);
    SparkSession s = b.getOrCreate();
    s.sparkContext().setLogLevel("ERROR");
    return s;
  }

  static Path tmp(String prefix) {
    try {
      Path base = Path.of(System.getProperty("user.dir"), "build", "r5", "tmp");
      Files.createDirectories(base);
      return Files.createTempDirectory(base, prefix);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static void report(String name, String text) {
    try {
      Path dir = Path.of(System.getProperty("user.dir"), "build", "r5");
      Files.createDirectories(dir);
      Files.writeString(dir.resolve(name), text);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static boolean causedBy(Throwable t, Class<? extends Throwable> cls) {
    for (Throwable c = t; c != null; c = c.getCause()) {
      if (cls.isInstance(c)) {
        return true;
      }
      if (c.getCause() == c) {
        break;
      }
    }
    return false;
  }

  static String chain(Throwable t) {
    StringBuilder sb = new StringBuilder();
    for (Throwable c = t; c != null && sb.length() < 400; c = c.getCause()) {
      if (sb.length() > 0) sb.append(" <- ");
      sb.append(c.getClass().getSimpleName());
      if (c.getCause() == c) break;
    }
    return sb.toString();
  }
}
