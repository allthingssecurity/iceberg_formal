package r5spark;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;

/**
 * HadoopCatalog whose table operations can be armed to "land the commit, then lose the ack":
 * the delegate commit runs to completion (new metadata is current), then
 * CommitStateUnknownException is thrown. Every injection is counted.
 */
public class FaultyHadoopCatalog extends HadoopCatalog {
  /** Number of upcoming snapshot-adding commits (any table) that will land and then throw. */
  public static final AtomicInteger ARMED = new AtomicInteger(0);
  /** Faults actually injected (delegate commit returned normally, then we threw). */
  public static final AtomicInteger INJECTED = new AtomicInteger(0);
  /** All commits that reached the delegate. */
  public static final AtomicInteger COMMITS = new AtomicInteger(0);

  @Override
  protected TableOperations newTableOps(TableIdentifier identifier) {
    return new LoseAckOps(super.newTableOps(identifier));
  }

  static final class LoseAckOps implements TableOperations {
    private final TableOperations delegate;

    LoseAckOps(TableOperations delegate) {
      this.delegate = delegate;
    }

    @Override
    public TableMetadata current() {
      return delegate.current();
    }

    @Override
    public TableMetadata refresh() {
      return delegate.refresh();
    }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
      delegate.commit(base, metadata);
      COMMITS.incrementAndGet();
      // Only commits that add a snapshot are faulted (structural: compare snapshot lists), so
      // property/schema commits (e.g. add_files setting the default name mapping) pass through.
      int baseSnaps = base == null ? 0 : base.snapshots().size();
      boolean addsSnapshot = metadata.snapshots().size() > baseSnaps;
      if (addsSnapshot && ARMED.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0) {
        INJECTED.incrementAndGet();
        throw new CommitStateUnknownException(new RuntimeException("r5 injected: ack lost after commit landed"));
      }
    }

    @Override
    public FileIO io() {
      return delegate.io();
    }

    @Override
    public EncryptionManager encryption() {
      return delegate.encryption();
    }

    @Override
    public String metadataFileLocation(String fileName) {
      return delegate.metadataFileLocation(fileName);
    }

    @Override
    public LocationProvider locationProvider() {
      return delegate.locationProvider();
    }

    @Override
    public TableOperations temp(TableMetadata uncommittedMetadata) {
      return delegate.temp(uncommittedMetadata);
    }

    @Override
    public long newSnapshotId() {
      return delegate.newSnapshotId();
    }

    @Override
    public boolean requireStrictCleanup() {
      return delegate.requireStrictCleanup();
    }
  }
}
