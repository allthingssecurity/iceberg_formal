package objstore;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Object;

/** One S3Mock child JVM + one in-JVM fault proxy, shared by all runs of a harness invocation. */
final class Env implements Closeable {
  static final String BUCKET = "bucket";

  final File workdir;
  final S3Server server;
  final FaultProxy proxy;
  final S3Client clean;
  final String version;

  Env() throws Exception {
    this.version = System.getProperty("icebergVersion");
    this.workdir = new File(System.getProperty("workdir"), "v" + version);
    deleteTree(workdir);
    workdir.mkdirs();
    this.server = new S3Server(workdir, BUCKET);
    this.proxy = new FaultProxy(server.port);
    this.clean =
        S3Client.builder()
            .endpointOverride(URI.create("http://127.0.0.1:" + server.port))
            .region(Region.US_EAST_1)
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
            .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
            .httpClient(anyHttpClient())
            .build();
  }

  static SdkHttpClient anyHttpClient() {
    for (String c : new String[] {"software.amazon.awssdk.http.apache5.Apache5HttpClient",
        "software.amazon.awssdk.http.apache.ApacheHttpClient",
        "software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient"}) {
      try {
        return (SdkHttpClient) Class.forName(c).getMethod("create").invoke(null);
      } catch (ReflectiveOperationException e) {
        // try next
      }
    }
    throw new IllegalStateException("no SDK HTTP client on classpath");
  }

  /** S3FileIO / catalog properties. viaProxy=false gives the clean (oracle) view. */
  Map<String, String> s3Props(boolean viaProxy, String client, File staging) {
    Map<String, String> p = new LinkedHashMap<>();
    int port = viaProxy ? proxy.port() : server.port;
    p.put("s3.endpoint", "http://127.0.0.1:" + port);
    p.put("s3.path-style-access", "true");
    p.put("s3.access-key-id", "test");
    p.put("s3.secret-access-key", "test");
    p.put("client.region", "us-east-1");
    p.put("s3.multipart.part-size-bytes", String.valueOf(5 * 1024 * 1024));
    p.put("s3.multipart.threshold", "1.0");
    p.put("s3.staging-dir", staging.getAbsolutePath());
    switch (client) {
      case "apache":
        p.put("http-client.type", "apache");
        p.put("http-client.apache.socket-timeout-ms", "3000");
        break;
      case "urlconnection":
        p.put("http-client.type", "urlconnection");
        p.put("http-client.urlconnection.socket-timeout-ms", "3000");
        break;
      case "default":
        break;
      default:
        throw new IllegalArgumentException("Unknown os.client: " + client);
    }
    return p;
  }

  /** JdbcCatalog on SQLite, loaded via CatalogUtil so old releases (Configurable<hadoop Configuration>) compile. */
  static Catalog jdbc(File db, String warehouse, Map<String, String> s3) {
    Map<String, String> p = new LinkedHashMap<>(s3);
    p.put(CatalogProperties.URI, "jdbc:sqlite:" + db.getAbsolutePath());
    p.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);
    p.put(CatalogProperties.FILE_IO_IMPL, S3FileIO.class.getName());
    p.put("jdbc.schema-version", "V1");
    return CatalogUtil.loadCatalog("org.apache.iceberg.jdbc.JdbcCatalog", "objstore", p, null);
  }

  static void closeQuietly(Object o) {
    if (o instanceof AutoCloseable) {
      try {
        ((AutoCloseable) o).close();
      } catch (Exception ignored) {
        // closing
      }
    }
  }

  S3FileIO cleanIO(File staging) {
    S3FileIO io = new S3FileIO();
    io.initialize(s3Props(false, "apache", staging));
    return io;
  }

  /** All objects under prefix (key -> size), via the clean client. */
  Map<String, Long> list(String prefix) {
    Map<String, Long> out = new LinkedHashMap<>();
    String token = null;
    do {
      ListObjectsV2Request.Builder b = ListObjectsV2Request.builder().bucket(BUCKET).prefix(prefix);
      if (token != null) {
        b.continuationToken(token);
      }
      ListObjectsV2Response r = clean.listObjectsV2(b.build());
      for (S3Object o : r.contents()) {
        out.put(o.key(), o.size());
      }
      token = Boolean.TRUE.equals(r.isTruncated()) ? r.nextContinuationToken() : null;
    } while (token != null);
    return out;
  }

  void deletePrefix(String prefix) {
    List<ObjectIdentifier> ids = new ArrayList<>();
    for (String k : list(prefix).keySet()) {
      ids.add(ObjectIdentifier.builder().key(k).build());
      if (ids.size() == 500) {
        clean.deleteObjects(
            DeleteObjectsRequest.builder().bucket(BUCKET).delete(Delete.builder().objects(ids).build()).build());
        ids = new ArrayList<>();
      }
    }
    if (!ids.isEmpty()) {
      clean.deleteObjects(
          DeleteObjectsRequest.builder().bucket(BUCKET).delete(Delete.builder().objects(ids).build()).build());
    }
  }

  static void deleteTree(File f) {
    if (f.isDirectory()) {
      File[] kids = f.listFiles();
      if (kids != null) {
        for (File k : kids) {
          deleteTree(k);
        }
      }
    }
    f.delete();
  }

  @Override
  public void close() throws IOException {
    try {
      proxy.close();
    } finally {
      clean.close();
      server.close();
    }
  }
}
