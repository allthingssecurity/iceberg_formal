package objstore;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.zip.CRC32;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.types.Types;
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsRequest;
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsResponse;
import software.amazon.awssdk.services.s3.model.MultipartUpload;

/** One workload run: real Iceberg commits through S3FileIO via the fault proxy, then a clean oracle. */
final class Run {

  static final class Config {
    long seed;
    int commits = 6;
    String client = "default";
    boolean checksum = false;
    int cols = 20;
    int boundBytes = 8192;
    boolean expire = false;
    boolean readPhase = true;
    boolean bigData = true;
    /** "mixed" (default), "big" (every blob >= 5 MiB: multipart), "mid" (130-730 KiB single PUT). */
    String sizeMode = "mixed";
    double rowDeltaProb = 0.35;
    int mergeMinCount = 3;
    /** hook invoked before op i (lets targeted scenarios arm the proxy). */
    Hook beforeOp = (run, i) -> { };
    /** hook invoked before the read phase. */
    Hook beforeRead = (run, i) -> { };
    /** hook invoked before the oracle (e.g. oracle calibration tampers with the store). */
    Hook beforeOracle = (run, i) -> { };
  }

  interface Hook {
    void at(Run run, int i) throws Exception;
  }

  static final class FileRec {
    final long len;
    final long crc;
    final long contentSeed;
    final int op;

    FileRec(long len, long crc, long contentSeed, int op) {
      this.len = len;
      this.crc = crc;
      this.contentSeed = contentSeed;
      this.op = op;
    }
  }

  final Env env;
  final Config cfg;
  final int runId;
  final String prefix; // key prefix (no bucket)
  final String warehouse;
  final File dir;
  final File staging;
  final SplittableRandom rnd;
  final Map<String, FileRec> ledger = new LinkedHashMap<>(); // s3 location -> written content
  final Map<Integer, String> opOutcome = new TreeMap<>();
  final Map<Integer, Set<String>> opData = new HashMap<>();
  final Map<Integer, Set<String>> opDeletes = new HashMap<>();
  final Set<String> writeFailed = new LinkedHashSet<>();
  /** violation class -> count; Major classes are prefixed "MAJOR_". */
  final Map<String, Integer> v = new TreeMap<>();
  final List<String> details = new ArrayList<>();
  Catalog faultyCatalog;
  Table table;
  final TableIdentifier id = TableIdentifier.of(Namespace.of("db"), "t");

  Run(Env env, Config cfg, int runId) {
    this.env = env;
    this.cfg = cfg;
    this.runId = runId;
    this.prefix = "r" + runId + "-" + cfg.seed;
    this.warehouse = "s3://" + Env.BUCKET + "/" + prefix;
    this.dir = new File(env.workdir, prefix);
    Env.deleteTree(dir);
    dir.mkdirs();
    this.staging = new File(dir, "staging");
    staging.mkdirs();
    this.rnd = new SplittableRandom(cfg.seed);
  }

  void count(String k) {
    v.merge(k, 1, Integer::sum);
  }

  void detail(String s) {
    if (details.size() < 40) {
      details.add(s);
    }
  }

  Schema schema() {
    List<Types.NestedField> f = new ArrayList<>();
    f.add(Types.NestedField.required(1, "id", Types.LongType.get()));
    for (int i = 0; i < cfg.cols; i++) {
      f.add(Types.NestedField.optional(2 + i, "b" + i, Types.BinaryType.get()));
    }
    return new Schema(f);
  }

  Map<String, String> s3Extra() {
    Map<String, String> m = new LinkedHashMap<>();
    if (cfg.checksum) {
      m.put("s3.checksum-enabled", "true");
    }
    return m;
  }

  void setup() {
    File db = new File(dir, "cat.db");
    faultyCatalog = withExtra(db, true);
    ((SupportsNamespaces) faultyCatalog).createNamespace(Namespace.of("db"));
    Map<String, String> props = new LinkedHashMap<>();
    props.put("format-version", "2");
    props.put("commit.manifest-merge.enabled", "true");
    props.put("commit.manifest.min-count-to-merge", String.valueOf(cfg.mergeMinCount));
    props.put("commit.manifest.target-size-bytes", String.valueOf(16L * 1024 * 1024));
    table = faultyCatalog.createTable(id, schema(), PartitionSpec.unpartitioned(), props);
  }

  Catalog withExtra(File db, boolean viaProxy) {
    Map<String, String> p = new LinkedHashMap<>(env.s3Props(viaProxy, cfg.client, staging));
    p.putAll(s3Extra());
    return Env.jdbc(db, warehouse, p);
  }

  static byte[] content(long seed, int len) {
    byte[] b = new byte[len];
    SplittableRandom r = new SplittableRandom(seed);
    int i = 0;
    while (i < len) {
      long x = r.nextLong();
      for (int j = 0; j < 8 && i < len; j++) {
        b[i++] = (byte) (x >>> (8 * j));
      }
    }
    return b;
  }

  /** Writes a blob through table.io() (S3OutputStream). Returns location or null if the write threw. */
  String writeBlob(String name, int len, int op) {
    String loc = table.location() + "/data/" + name + "-" + UUID.randomUUID() + ".parquet";
    long seed = rnd.nextLong();
    byte[] data = content(seed, len);
    try {
      OutputFile of = table.io().newOutputFile(loc);
      try (PositionOutputStream out = of.create()) {
        int off = 0;
        while (off < len) {
          int n = Math.min(len - off, 1 + rnd.nextInt(256 * 1024));
          out.write(data, off, n);
          off += n;
        }
      }
      CRC32 c = new CRC32();
      c.update(data);
      ledger.put(loc, new FileRec(len, c.getValue(), seed, op));
      return loc;
    } catch (RuntimeException | IOException e) {
      writeFailed.add(loc);
      count("blobWriteThrew:" + rootName(e));
      detail("op" + op + " blob write threw " + chain(e) + " " + key(loc) + " intendedLen=" + len);
      return null;
    }
  }

  int dataSize() {
    switch (cfg.sizeMode) {
      case "big":
        return 5 * 1024 * 1024 + 1 + rnd.nextInt(6 * 1024 * 1024);
      case "mid":
        return 130 * 1024 + rnd.nextInt(600 * 1024);
      case "mixed":
        break;
      default:
        throw new IllegalArgumentException("Unknown sizeMode: " + cfg.sizeMode);
    }
    int r = rnd.nextInt(10);
    if (cfg.bigData && r < 3) {
      return 5 * 1024 * 1024 + rnd.nextInt(6 * 1024 * 1024); // multipart path (>= 5 MiB, threshold 1.0)
    } else if (r < 7) {
      return 130 * 1024 + rnd.nextInt(600 * 1024); // single PUT, > 128 KiB mark limit
    }
    return 1 + rnd.nextInt(16 * 1024);
  }

  Metrics metrics(long rows) {
    Map<Integer, ByteBuffer> lo = new HashMap<>();
    Map<Integer, ByteBuffer> hi = new HashMap<>();
    for (int i = 0; i < cfg.cols; i++) {
      byte[] a = new byte[cfg.boundBytes];
      byte[] b = new byte[cfg.boundBytes];
      for (int j = 0; j < a.length; j++) {
        a[j] = (byte) rnd.nextInt(256);
        b[j] = (byte) rnd.nextInt(256);
      }
      a[0] = 0;
      b[0] = (byte) 0xff;
      lo.put(2 + i, ByteBuffer.wrap(a));
      hi.put(2 + i, ByteBuffer.wrap(b));
    }
    return new Metrics(rows, null, null, null, null, lo, hi);
  }

  void workload() throws Exception {
    for (int i = 0; i < cfg.commits; i++) {
      cfg.beforeOp.at(this, i);
      boolean delta = i > 0 && rnd.nextDouble() < cfg.rowDeltaProb && !committedData().isEmpty();
      int n = 1 + rnd.nextInt(3);
      Set<String> data = new LinkedHashSet<>();
      List<DataFile> dfs = new ArrayList<>();
      for (int j = 0; j < n; j++) {
        int len = dataSize();
        String loc = writeBlob("op" + i + "-f" + j, len, i);
        if (loc != null) {
          data.add(loc);
          dfs.add(DataFiles.builder(PartitionSpec.unpartitioned())
              .withPath(loc)
              .withFileSizeInBytes(len)
              .withFormat(FileFormat.PARQUET)
              .withMetrics(metrics(100))
              .build());
        }
      }
      Set<String> dels = new LinkedHashSet<>();
      DeleteFile del = null;
      if (delta) {
        int len = 512 + rnd.nextInt(200 * 1024);
        String loc = writeBlob("op" + i + "-del", len, i);
        if (loc != null) {
          dels.add(loc);
          del = FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
              .ofPositionDeletes()
              .withPath(loc)
              .withFileSizeInBytes(len)
              .withFormat(FileFormat.PARQUET)
              .withRecordCount(3)
              .build();
        }
      }
      if (dfs.isEmpty() && del == null) {
        opOutcome.put(i, "SKIPPED(no blob written)");
        continue;
      }
      opData.put(i, data);
      opDeletes.put(i, dels);
      try {
        if (delta) {
          org.apache.iceberg.RowDelta rd = table.newRowDelta();
          dfs.forEach(rd::addRows);
          if (del != null) {
            rd.addDeletes(del);
          }
          rd.commit();
        } else {
          org.apache.iceberg.AppendFiles ap = table.newAppend();
          dfs.forEach(ap::appendFile);
          ap.commit();
        }
        opOutcome.put(i, "OK");
      } catch (RuntimeException e) {
        opOutcome.put(i, "THREW:" + e.getClass().getSimpleName() + "/" + rootName(e));
        count("commitThrew:" + e.getClass().getSimpleName() + "/" + rootName(e));
        detail("op" + i + " commit threw " + chain(e));
      }
    }
  }

  Set<String> committedData() {
    Set<String> s = new LinkedHashSet<>();
    opOutcome.forEach((i, o) -> {
      if ("OK".equals(o)) {
        s.addAll(opData.getOrDefault(i, Set.of()));
      }
    });
    return s;
  }

  void expirePhase() {
    try {
      table.refresh();
      table.expireSnapshots().expireOlderThan(System.currentTimeMillis() + 1).retainLast(1).commit();
      count("expireOK");
    } catch (RuntimeException e) {
      count("expireThrew:" + e.getClass().getSimpleName() + "/" + rootName(e));
      detail("expire threw " + chain(e));
    }
  }

  // ---------------------------------------------------------------- oracle

  static final class Truth {
    TableMetadata meta;
    final Set<String> reachableKeys = new LinkedHashSet<>();
    final Set<String> liveData = new LinkedHashSet<>();
    final Set<String> liveDeletes = new LinkedHashSet<>();
    final Map<String, Integer> manifestLive = new HashMap<>();
  }

  String key(String loc) {
    String p = "s3://" + Env.BUCKET + "/";
    if (!loc.startsWith(p)) {
      throw new IllegalArgumentException("not in bucket: " + loc);
    }
    return loc.substring(p.length());
  }

  /** Clean view of the committed state + Major checks on everything reachable from it. */
  Truth oracle() throws IOException {
    Truth t = new Truth();
    Map<String, Long> objects = env.list(prefix + "/");
    Catalog clean = withExtra(new File(dir, "cat.db"), false);
    Table ct = clean.loadTable(id);
    TableMetadata meta = ((HasTableOperations) ct).operations().current();
    t.meta = meta;
    FileIO io = ct.io();
    t.reachableKeys.add(key(meta.metadataFileLocation()));
    for (TableMetadata.MetadataLogEntry e : meta.previousFiles()) {
      String k = key(e.file());
      t.reachableKeys.add(k);
      if (!objects.containsKey(k)) {
        count("minor_prevMetadataMissing");
      }
    }
    Snapshot current = meta.currentSnapshot();
    Set<String> checkedManifests = new LinkedHashSet<>();
    Set<String> checkedFiles = new LinkedHashSet<>();
    for (Snapshot s : meta.snapshots()) {
      String ml = key(s.manifestListLocation());
      t.reachableKeys.add(ml);
      Long mlLen = objects.get(ml);
      if (mlLen == null) {
        count("MAJOR_missingManifestList");
        detail("missing manifest list " + ml);
        continue;
      }
      if (mlLen == 0) {
        count("MAJOR_zeroLengthManifestList");
      }
      List<ManifestFile> manifests;
      try {
        manifests = s.allManifests(io);
      } catch (RuntimeException e) {
        count("MAJOR_unreadableManifestList");
        detail("unreadable manifest list " + ml + " " + chain(e));
        continue;
      }
      boolean isCurrent = current != null && s.snapshotId() == current.snapshotId();
      for (ManifestFile m : manifests) {
        String mk = key(m.path());
        t.reachableKeys.add(mk);
        boolean first = checkedManifests.add(mk);
        Long len = objects.get(mk);
        if (len == null) {
          if (first) {
            count("MAJOR_missingManifest");
            detail("missing manifest " + mk + " snap " + s.snapshotId());
          }
          continue;
        }
        if (len != m.length()) {
          if (first) {
            count("MAJOR_manifestLengthMismatch");
            detail("manifest " + mk + " object=" + len + " recorded=" + m.length());
          }
          continue;
        }
        List<String> live = new ArrayList<>();
        try {
          if (m.content() == ManifestContent.DATA) {
            try (ManifestReader<DataFile> r = ManifestFiles.read(m, io, meta.specsById())) {
              for (DataFile f : r) {
                live.add(f.path().toString());
                checkFile(f.path().toString(), f.fileSizeInBytes(), objects, checkedFiles, t);
              }
            }
          } else {
            try (ManifestReader<DeleteFile> r = ManifestFiles.readDeleteManifest(m, io, meta.specsById())) {
              for (DeleteFile f : r) {
                live.add(f.path().toString());
                checkFile(f.path().toString(), f.fileSizeInBytes(), objects, checkedFiles, t);
              }
            }
          }
        } catch (RuntimeException | IOException e) {
          if (first) {
            count("MAJOR_unreadableManifest");
            detail("unreadable manifest " + mk + " " + chain(e));
          }
          continue;
        }
        Integer added = m.addedFilesCount();
        Integer existing = m.existingFilesCount();
        if (first && added != null && existing != null && added + existing != live.size()) {
          count("MAJOR_manifestEntryCountMismatch");
          detail("manifest " + mk + " live=" + live.size() + " recorded=" + (added + existing));
        }
        if (isCurrent) {
          if (m.content() == ManifestContent.DATA) {
            t.liveData.addAll(live);
          } else {
            t.liveDeletes.addAll(live);
          }
        }
        t.manifestLive.put(mk, live.size());
      }
    }
    // structural oracle (model-free): snapshot summary vs what the manifests actually list
    if (current != null && current.summary() != null && current.summary().get("total-data-files") != null) {
      long summ = Long.parseLong(current.summary().get("total-data-files"));
      if (summ != t.liveData.size()) {
        count("MAJOR_summaryVsManifestsMismatch");
        detail("current snapshot summary total-data-files=" + summ + " but manifests list " + t.liveData.size());
      }
    }
    // logical oracle: model vs committed state
    for (Map.Entry<Integer, String> e : opOutcome.entrySet()) {
      int op = e.getKey();
      Set<String> d = opData.getOrDefault(op, Set.of());
      Set<String> x = opDeletes.getOrDefault(op, Set.of());
      boolean anyPresent = false;
      boolean allPresent = true;
      for (String f : d) {
        boolean p = t.liveData.contains(f);
        anyPresent |= p;
        allPresent &= p;
      }
      for (String f : x) {
        boolean p = t.liveDeletes.contains(f);
        anyPresent |= p;
        allPresent &= p;
      }
      if ("OK".equals(e.getValue()) && !allPresent) {
        count("MAJOR_lostCommittedFiles");
        detail("op" + op + " returned OK but its files are not all live in current snapshot");
      } else if (e.getValue().startsWith("THREW") && anyPresent) {
        boolean unknown = e.getValue().contains("CommitStateUnknown");
        count(unknown ? "info_unknownStateButCommitted" : "MAJOR_threwButCommitted");
        detail("op" + op + " threw " + e.getValue() + " but its files are live");
      }
    }
    for (String f : t.liveData) {
      if (!committedData().contains(f) && !anyThrewOwns(f)) {
        count("MAJOR_unexpectedLiveFile");
        detail("unexpected live file " + f);
      }
    }
    // orphans, leaked uploads, staging leftovers
    for (Map.Entry<String, Long> o : objects.entrySet()) {
      String k = o.getKey();
      if (t.reachableKeys.contains(k)) {
        continue;
      }
      String loc = "s3://" + Env.BUCKET + "/" + k;
      String cls;
      if (k.contains("/data/")) {
        cls = ledger.containsKey(loc) ? "orphan_callerData" : "orphan_dataUnknown(writeThrew)";
        if (ledger.containsKey(loc) && committedData().contains(loc)) {
          cls = "orphan_committedDataNotReachable";
        }
      } else if (k.endsWith(".metadata.json")) {
        cls = "orphan_metadataJson";
      } else if (k.contains("/snap-")) {
        cls = "orphan_manifestList";
      } else if (k.endsWith(".avro")) {
        cls = "orphan_manifest";
      } else {
        cls = "orphan_other";
      }
      count(cls);
      if (o.getValue() == 0) {
        count("orphan_zeroLength");
      }
      if (!cls.equals("orphan_callerData")) {
        detail(cls + " " + k + " size=" + o.getValue());
      }
    }
    int uploads = leakedUploads();
    if (uploads > 0) {
      v.merge("leak_incompleteMultipartUploads", uploads, Integer::sum);
    }
    File[] left = staging.listFiles();
    if (left != null && left.length > 0) {
      v.merge("leak_stagingFiles", left.length, Integer::sum);
    }
    Env.closeQuietly(clean);
    return t;
  }

  private boolean anyThrewOwns(String f) {
    for (Map.Entry<Integer, Set<String>> e : opData.entrySet()) {
      if (e.getValue().contains(f) && opOutcome.getOrDefault(e.getKey(), "").startsWith("THREW")) {
        return true;
      }
    }
    return false;
  }

  int leakedUploads() {
    int n = 0;
    String keyMarker = null;
    String idMarker = null;
    while (true) {
      ListMultipartUploadsRequest.Builder b =
          ListMultipartUploadsRequest.builder().bucket(Env.BUCKET).prefix(prefix + "/");
      if (keyMarker != null) {
        b.keyMarker(keyMarker).uploadIdMarker(idMarker);
      }
      ListMultipartUploadsResponse r = env.clean.listMultipartUploads(b.build());
      for (MultipartUpload u : r.uploads()) {
        n++;
        detail("leaked upload " + u.key() + " " + u.uploadId());
      }
      if (Boolean.TRUE.equals(r.isTruncated())) {
        keyMarker = r.nextKeyMarker();
        idMarker = r.nextUploadIdMarker();
      } else {
        return n;
      }
    }
  }

  private void checkFile(String loc, long recorded, Map<String, Long> objects, Set<String> checked, Truth t) {
    String k = key(loc);
    t.reachableKeys.add(k);
    if (!checked.add(k)) {
      return;
    }
    Long len = objects.get(k);
    if (len == null) {
      count("MAJOR_missingDataOrDeleteFile");
      detail("missing referenced file " + k);
      return;
    }
    if (len != recorded) {
      count("MAJOR_fileLengthMismatch");
      detail("file " + k + " object=" + len + " recorded=" + recorded);
      return;
    }
    FileRec rec = ledger.get(loc);
    if (rec == null) {
      count("MAJOR_referencedFileNotInLedger");
      detail("referenced file not written successfully by workload: " + k);
      return;
    }
    // full content check through the clean client
    try (InputStream in = env.clean.getObject(b -> b.bucket(Env.BUCKET).key(k))) {
      long[] lc = crcOf(in);
      if (lc[0] != rec.len || lc[1] != rec.crc) {
        count("MAJOR_fileContentMismatch");
        detail("content mismatch " + k);
      }
    } catch (IOException e) {
      count("oracleReadError");
    }
  }

  static long[] crcOf(InputStream in) throws IOException {
    CRC32 c = new CRC32();
    byte[] buf = new byte[65536];
    long n = 0;
    int r;
    while ((r = in.read(buf)) >= 0) {
      c.update(buf, 0, r);
      n += r;
    }
    return new long[] {n, c.getValue()};
  }

  /** Reads through the FAULTY io and compares with the clean truth. Silent divergence is Major. */
  void readPhase(Truth truth) {
    try {
      table.refresh();
    } catch (RuntimeException e) {
      count("read_refreshThrew:" + rootName(e));
      detail("refresh threw " + chain(e));
      return;
    }
    Set<String> planned = new LinkedHashSet<>();
    Set<String> plannedDeletes = new LinkedHashSet<>();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        planned.add(task.file().path().toString());
        for (DeleteFile d : task.deletes()) {
          plannedDeletes.add(d.path().toString());
        }
      }
      if (!planned.equals(truth.liveData)) {
        count("MAJOR_silentScanMismatch");
        detail("scan planned " + planned.size() + " files, truth " + truth.liveData.size());
      } else {
        count("read_scanOK");
      }
    } catch (RuntimeException | IOException e) {
      count("read_scanThrew:" + rootName(e));
      detail("scan threw " + chain(e));
    }
    List<String> all = new ArrayList<>(truth.liveData);
    all.addAll(truth.liveDeletes);
    for (String loc : all) {
      FileRec rec = ledger.get(loc);
      if (rec == null) {
        continue;
      }
      try (InputStream in = table.io().newInputFile(loc).newStream()) {
        long[] lc = crcOf(in);
        if (lc[0] != rec.len || lc[1] != rec.crc) {
          count("MAJOR_silentBadSequentialRead");
          detail("sequential read of " + key(loc) + " returned " + lc[0] + "B (expected " + rec.len + ") w/o error");
        } else {
          count("read_seqOK");
        }
      } catch (RuntimeException | IOException e) {
        count("read_seqThrew:" + rootName(e));
        detail("seq read threw " + key(loc) + " " + chain(e));
      }
      rangeCheck(loc, rec);
    }
  }

  private static java.lang.reflect.Method rangeCheck;
  private static boolean rangeLooked = false;

  /** Range reads exist only from 1.4 (Ranges.java is excluded from older builds). */
  private void rangeCheck(String loc, FileRec rec) {
    if (!rangeLooked) {
      rangeLooked = true;
      try {
        Class<?> c = Class.forName("objstore.Ranges");
        if ((Boolean) c.getDeclaredMethod("supported").invoke(null)) {
          rangeCheck = c.getDeclaredMethod("check", Run.class, String.class, FileRec.class);
        }
      } catch (ReflectiveOperationException e) {
        rangeCheck = null;
      }
    }
    if (rangeCheck != null) {
      try {
        rangeCheck.invoke(null, this, loc, rec);
      } catch (ReflectiveOperationException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  // ---------------------------------------------------------------- utils

  static Throwable root(Throwable t) {
    Throwable r = t;
    while (r.getCause() != null && r.getCause() != r) {
      r = r.getCause();
    }
    return r;
  }

  static String rootName(Throwable t) {
    return root(t).getClass().getSimpleName();
  }

  static String chain(Throwable t) {
    StringBuilder sb = new StringBuilder();
    Throwable r = t;
    int n = 0;
    while (r != null && n < 6) {
      if (n > 0) {
        sb.append(" <- ");
      }
      String msg = String.valueOf(r.getMessage());
      sb.append(r.getClass().getSimpleName()).append(": ").append(msg, 0, Math.min(140, msg.length()));
      r = r.getCause() == r ? null : r.getCause();
      n++;
    }
    return sb.toString();
  }

  /** Runs setup + workload (+expire) + read phase + oracle. Never throws for workload failures. */
  Truth execute(Consumer<Run> afterWorkload) throws Exception {
    setup();
    workload();
    if (cfg.expire) {
      expirePhase();
    }
    if (afterWorkload != null) {
      afterWorkload.accept(this);
    }
    if (cfg.readPhase) {
      Truth pre = oracleQuiet();
      cfg.beforeRead.at(this, 0);
      readPhase(pre);
    }
    cfg.beforeOracle.at(this, 0);
    Truth t = oracle();
    Env.closeQuietly(faultyCatalog);
    return t;
  }

  /** Oracle pass whose counts are discarded (used to obtain the truth before the read phase). */
  Truth oracleQuiet() throws IOException {
    Map<String, Integer> saved = new TreeMap<>(v);
    List<String> savedD = new ArrayList<>(details);
    Truth t = oracle();
    v.clear();
    v.putAll(saved);
    details.clear();
    details.addAll(savedD);
    return t;
  }
}
