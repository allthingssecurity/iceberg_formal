package objstore;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Adobe S3Mock (com.adobe.testing:s3mock, Maven Central) in a child JVM with its own classpath, so its
 * AWS SDK / Spring stack never shares a classloader with the Iceberg release under test. The child is
 * destroyed on close() and by a shutdown hook.
 */
final class S3Server implements Closeable {
  final int port;
  private final Process proc;
  final File root;

  S3Server(File workdir, String bucket) throws IOException, InterruptedException {
    String cp = System.getProperty("s3server.classpath");
    if (cp == null || cp.isEmpty()) {
      throw new IllegalStateException("s3server.classpath not set (run through ./gw harness)");
    }
    this.port = freePort();
    int httpsPort = freePort();
    this.root = new File(workdir, "s3root");
    root.mkdirs();
    File logFile = new File(workdir, "s3mock.log");
    String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
    List<String> cmd = new ArrayList<>();
    cmd.add(java);
    cmd.add("-Xmx1g");
    cmd.add("-cp");
    cmd.add(cp);
    cmd.add("com.adobe.testing.s3mock.S3MockApplication");
    cmd.add("--http.port=" + port);
    cmd.add("--server.port=" + httpsPort);
    cmd.add("--com.adobe.testing.s3mock.store.root=" + root.getAbsolutePath());
    cmd.add("--com.adobe.testing.s3mock.store.initial-buckets=" + bucket);
    cmd.add("--com.adobe.testing.s3mock.store.retain-files-on-exit=false");
    cmd.add("--com.adobe.testing.s3mock.max-payload-size=64MB");
    cmd.add("--logging.level.root=WARN");
    ProcessBuilder pb = new ProcessBuilder(cmd);
    pb.redirectErrorStream(true);
    pb.redirectOutput(logFile);
    this.proc = pb.start();
    Runtime.getRuntime().addShutdownHook(new Thread(proc::destroyForcibly));
    long deadline = System.currentTimeMillis() + 90_000;
    while (true) {
      if (!proc.isAlive()) {
        throw new IOException("S3Mock exited early; see " + logFile + ": " + tail(logFile));
      }
      try (Socket s = new Socket(InetAddress.getLoopbackAddress(), port)) {
        break;
      } catch (IOException e) {
        if (System.currentTimeMillis() > deadline) {
          throw new IOException("S3Mock did not start; " + tail(logFile));
        }
        Thread.sleep(200);
      }
    }
  }

  private static String tail(File f) {
    try {
      List<String> lines = Files.readAllLines(f.toPath());
      return String.join("\n", lines.subList(Math.max(0, lines.size() - 15), lines.size()));
    } catch (IOException e) {
      return "<no log>";
    }
  }

  static int freePort() throws IOException {
    try (ServerSocket s = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
      return s.getLocalPort();
    }
  }

  @Override
  public void close() {
    proc.destroy();
    try {
      if (!proc.waitFor(10, java.util.concurrent.TimeUnit.SECONDS)) {
        proc.destroyForcibly();
      }
    } catch (InterruptedException e) {
      proc.destroyForcibly();
      Thread.currentThread().interrupt();
    }
  }
}
