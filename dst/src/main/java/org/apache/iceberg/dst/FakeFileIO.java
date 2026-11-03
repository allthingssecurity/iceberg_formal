package org.apache.iceberg.dst;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Map;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;

/**
 * A {@link FileIO} over {@link FakeStore} that can fail a write in each of the four ways an object
 * store can: not at all, before any byte, after a prefix, and after every byte but with the answer
 * lost.
 *
 * <p>Which one happens is drawn from the seed at {@code create()}, so one write has one outcome.
 * Every crossing is a scheduler point, which is what lets two writers interleave inside a single
 * {@code SnapshotProducer.commit()}.
 */
public final class FakeFileIO implements FileIO {

  private final FakeStore store;
  private final Scheduler sched;
  private final FaultConfig config;

  /**
   * An observer view never parks and never faults. The invariants and the per-snapshot captures
   * read the store through one of these, so looking at the table does not change the schedule.
   */
  private final boolean observer;

  public FakeFileIO(FakeStore store, Scheduler sched, FaultConfig config) {
    this(store, sched, config, false);
  }

  public FakeFileIO(FakeStore store, Scheduler sched, FaultConfig config, boolean observer) {
    this.store = store;
    this.sched = sched;
    this.config = config;
    this.observer = observer;
  }

  /** A read-only, fault-free view of the same store. */
  public FakeFileIO observerView() {
    FakeFileIO view = new FakeFileIO(store, sched, config, true);
    view.liveLists = this.liveLists;
    return view;
  }

  private boolean active() {
    return !observer && sched.armed();
  }

  private void point(String label) {
    if (!observer) {
      sched.point(label);
    }
  }

  public FakeStore store() {
    return store;
  }

  /**
   * Tells the read fault which manifest lists belong to snapshots still in the catalog's current
   * metadata. Set by the simulation once the catalog exists; shared by every view of this store.
   */
  private static final class LiveLists {
    volatile java.util.function.Predicate<String> isLive = path -> true;
  }

  private LiveLists liveLists = new LiveLists();

  public void classifyManifestListsWith(java.util.function.Predicate<String> isLive) {
    this.liveLists.isLive = isLive;
  }

  private static final java.util.Set<String> CLEANUP_CLASSES =
      java.util.Set.of(
          "org.apache.iceberg.IncrementalFileCleanup", "org.apache.iceberg.ReachableFileCleanup");

  /** The first FileCleanupStrategy frame on the stack, as Class.method, or null if none. */
  private static String cleanupSite() {
    return StackWalker.getInstance()
        .walk(
            frames ->
                frames
                    .filter(f -> CLEANUP_CLASSES.contains(f.getClassName()))
                    .findFirst()
                    .map(
                        f ->
                            f.getClassName().substring(f.getClassName().lastIndexOf('.') + 1)
                                + "."
                                + f.getMethodName())
                    .orElse(null));
  }

  /** The read fault on manifest lists (FaultConfig.readFailManifestListPct). */
  private void maybeFailRead(String path) {
    if (!active() || !"mlist".equals(FakeStore.kindOf(path))) {
      return;
    }
    String site = cleanupSite();
    if (site != null) {
      // Counted whether or not the fault is on, so a control run can say which cleanup strategy
      // ran and which loops read what. Recording draws nothing, so the schedule is unchanged.
      store.recordCleanupRead(site, liveLists.isLive.test(path));
    }
    if (config.readFailManifestListPct <= 0) {
      return;
    }
    boolean retained = liveLists.isLive.test(path);
    switch (config.readFailScope) {
      case ANY:
        break;
      case CLEANUP_RETAINED:
        if (site == null || !retained) {
          return;
        }
        break;
      case CLEANUP_EXPIRED:
        if (site == null || retained) {
          return;
        }
        break;
      default:
        throw new IllegalStateException("unhandled scope " + config.readFailScope);
    }
    String label = site == null ? "other" : site;
    store.recordReadInScope(label, retained);
    if (sched.draw("read.mlist", 100) < config.readFailManifestListPct) {
      store.recordReadFault(path, label, retained, sched.currentName());
      throw new UncheckedIOException(
          new IOException("fault: read of " + path + " failed (injected)"));
    }
  }


  @Override
  public InputFile newInputFile(String path) {
    return new FakeInputFile(path);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return new FakeOutputFile(path);
  }

  @Override
  public void deleteFile(String path) {
    point("delete:" + FakeStore.kindOf(path));
    if (active()
        && config.deleteFailPct > 0
        && sched.draw("delete.fail", 100) < config.deleteFailPct) {
      throw new UncheckedIOException(new IOException("fault: delete of " + path + " failed"));
    }
    store.delete(path, sched.currentName());
  }

  @Override
  public Map<String, String> properties() {
    return Collections.emptyMap();
  }

  private final class FakeInputFile implements InputFile {
    private final String path;

    FakeInputFile(String path) {
      this.path = path;
    }

    @Override
    public long getLength() {
      point("len:" + FakeStore.kindOf(path));
      long len = store.length(path);
      if (len < 0) {
        throw new NotFoundException("No such file: %s", path);
      }
      return len;
    }

    @Override
    public SeekableInputStream newStream() {
      point("read:" + FakeStore.kindOf(path));
      maybeFailRead(path);
      byte[] bytes = store.get(path);
      if (bytes == null) {
        throw new NotFoundException("No such file: %s", path);
      }
      return new FakeSeekableStream(bytes);
    }

    @Override
    public String location() {
      return path;
    }

    @Override
    public boolean exists() {
      point("exists:" + FakeStore.kindOf(path));
      return store.exists(path);
    }

    @Override
    public String toString() {
      return path;
    }
  }

  private static final class FakeSeekableStream extends SeekableInputStream {
    private final byte[] bytes;
    private final ByteArrayInputStream in;
    private long pos;

    FakeSeekableStream(byte[] bytes) {
      this.bytes = bytes;
      this.in = new ByteArrayInputStream(bytes);
    }

    @Override
    public long getPos() {
      return pos;
    }

    @Override
    public void seek(long newPos) {
      in.reset();
      long skipped = in.skip(newPos);
      this.pos = skipped;
    }

    @Override
    public int read() {
      int b = in.read();
      if (b >= 0) {
        pos++;
      }
      return b;
    }

    @Override
    public int read(byte[] buf, int off, int len) {
      int n = in.read(buf, off, len);
      if (n > 0) {
        pos += n;
      }
      return n;
    }

    @Override
    public int available() {
      return in.available();
    }

    @Override
    public long skip(long n) {
      long skipped = in.skip(n);
      pos += skipped;
      return skipped;
    }

    int size() {
      return bytes.length;
    }
  }

  /** The four write outcomes, drawn once per create(). */
  private enum WriteOutcome {
    OK,
    FAIL_BEFORE_BYTES,
    TRUNCATE_AND_THROW,
    LOST_ACK,
    SILENT_TRUNCATE
  }

  private final class FakeOutputFile implements OutputFile {
    private final String path;

    FakeOutputFile(String path) {
      this.path = path;
    }

    @Override
    public PositionOutputStream create() {
      return open(false);
    }

    @Override
    public PositionOutputStream createOrOverwrite() {
      return open(true);
    }

    private PositionOutputStream open(boolean overwrite) {
      String kind = FakeStore.kindOf(path);
      point("create:" + kind);
      if (!overwrite && store.exists(path)) {
        throw new org.apache.iceberg.exceptions.AlreadyExistsException(
            "File already exists: %s", path);
      }
      WriteOutcome outcome = drawOutcome();
      if (outcome == WriteOutcome.FAIL_BEFORE_BYTES) {
        store.recordWrite(new FakeStore.WriteRecord(path, 0, 0, false, true, kind));
        throw new UncheckedIOException(new IOException("fault: create of " + path + " failed"));
      }
      return new FakeOutputStream(path, kind, outcome);
    }

    private WriteOutcome drawOutcome() {
      if (!active()) {
        return WriteOutcome.OK;
      }
      int roll = sched.draw("write." + FakeStore.kindOf(path), 100);
      int at = config.writeFailBeforeBytesPct;
      if (roll < at) {
        return WriteOutcome.FAIL_BEFORE_BYTES;
      }
      at += config.writeTruncateAndThrowPct;
      if (roll < at) {
        return WriteOutcome.TRUNCATE_AND_THROW;
      }
      at += config.writeLostAckPct;
      if (roll < at) {
        return WriteOutcome.LOST_ACK;
      }
      at += config.writeSilentTruncatePct;
      if (roll < at) {
        return WriteOutcome.SILENT_TRUNCATE;
      }
      return WriteOutcome.OK;
    }

    @Override
    public String location() {
      return path;
    }

    @Override
    public InputFile toInputFile() {
      return new FakeInputFile(path);
    }

    @Override
    public String toString() {
      return path;
    }
  }

  private final class FakeOutputStream extends PositionOutputStream {
    private final String path;
    private final String kind;
    private final WriteOutcome outcome;
    private final java.io.ByteArrayOutputStream buffer = new java.io.ByteArrayOutputStream();
    private boolean closed;

    FakeOutputStream(String path, String kind, WriteOutcome outcome) {
      this.path = path;
      this.kind = kind;
      this.outcome = outcome;
    }

    @Override
    public long getPos() {
      return buffer.size();
    }

    @Override
    public void write(int b) {
      buffer.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) {
      buffer.write(b, off, len);
    }

    @Override
    public void close() {
      if (closed) {
        return;
      }
      closed = true;
      byte[] bytes = buffer.toByteArray();
      point("close:" + kind);
      switch (outcome) {
        case OK:
          store.put(path, bytes);
          store.recordWrite(
              new FakeStore.WriteRecord(path, bytes.length, bytes.length, false, false, kind));
          return;

        case LOST_ACK:
          store.put(path, bytes);
          store.recordWrite(
              new FakeStore.WriteRecord(path, bytes.length, bytes.length, false, true, kind));
          throw new UncheckedIOException(
              new IOException("fault: close of " + path + " lost the answer"));

        case TRUNCATE_AND_THROW:
          {
            int kept = prefixLength(bytes.length);
            store.put(path, java.util.Arrays.copyOf(bytes, kept));
            store.recordWrite(
                new FakeStore.WriteRecord(path, bytes.length, kept, true, true, kind));
            throw new UncheckedIOException(
                new IOException("fault: close of " + path + " truncated at " + kept));
          }

        case SILENT_TRUNCATE:
          {
            int kept = prefixLength(bytes.length);
            store.put(path, java.util.Arrays.copyOf(bytes, kept));
            store.recordWrite(
                new FakeStore.WriteRecord(path, bytes.length, kept, true, false, kind));
            return;
          }

        default:
          throw new IllegalStateException("unreachable outcome " + outcome);
      }
    }

    private int prefixLength(int total) {
      if (total == 0) {
        return 0;
      }
      // The bound must not depend on the byte count. Drawing nextInt(total) would make the
      // amount of randomness consumed a function of how long the Avro object happened to be,
      // so one extra byte anywhere would send the whole rest of the run down another path.
      int percent = sched.draw("write.prefix.pct", 100);
      int kept = (int) ((long) total * percent / 100L);
      // Always lose at least one byte, so the fault is observable.
      return Math.max(0, Math.min(kept, total - 1));
    }
  }
}
