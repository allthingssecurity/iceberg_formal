package r5;

import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iceberg.Files;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

/** Plain local-disk FileIO (Iceberg's own Files.localInput/localOutput) that records every delete. */
public class LocalFileIO implements FileIO {
  /** Every path deleted through any LocalFileIO instance in this JVM, cleared per run. */
  public static final Set<String> DELETED = ConcurrentHashMap.newKeySet();

  static String strip(String path) {
    return path.startsWith("file:") ? path.substring("file:".length()) : path;
  }

  @Override
  public InputFile newInputFile(String path) {
    return Files.localInput(strip(path));
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return Files.localOutput(strip(path));
  }

  @Override
  public void deleteFile(String path) {
    DELETED.add(strip(path));
    new File(strip(path)).delete();
  }

  @Override
  public void initialize(Map<String, String> properties) {}

  public static boolean exists(String path) {
    return new File(strip(path)).exists();
  }
}
