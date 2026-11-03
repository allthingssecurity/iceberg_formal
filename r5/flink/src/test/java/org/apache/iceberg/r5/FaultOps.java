package org.apache.iceberg.r5;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;

/**
 * Wraps a real TableOperations. When armed, the NEXT commit() is applied by the delegate (the new
 * metadata lands in the catalog) and then the armed exception is thrown to the caller: a lost
 * acknowledgement. One-shot; every injection is counted and the thrown instance is remembered so
 * the caller can attribute it by identity.
 */
public final class FaultOps implements TableOperations {
  private final TableOperations delegate;
  private final AtomicReference<Supplier<RuntimeException>> armed = new AtomicReference<>();
  public final AtomicInteger injected = new AtomicInteger();
  public final AtomicInteger commits = new AtomicInteger();
  public volatile RuntimeException lastThrown;

  /** When true, every FileIO.deleteFile through this table fails (and is counted). */
  public final java.util.concurrent.atomic.AtomicBoolean failDeletes =
      new java.util.concurrent.atomic.AtomicBoolean();
  public final AtomicInteger injectedDeleteFailures = new AtomicInteger();
  public final AtomicInteger deletes = new AtomicInteger();
  public final java.util.Set<String> failedDeletePaths =
      java.util.concurrent.ConcurrentHashMap.newKeySet();
  private final FileIO io;

  public FaultOps(TableOperations delegate) {
    this.delegate = delegate;
    this.io = new DeleteFaultIO(delegate.io());
  }

  /** FileIO wrapper: injects delete failures when failDeletes is set. */
  private final class DeleteFaultIO implements FileIO {
    private final FileIO d;

    DeleteFaultIO(FileIO d) {
      this.d = d;
    }

    @Override
    public org.apache.iceberg.io.InputFile newInputFile(String path) {
      return d.newInputFile(path);
    }

    @Override
    public org.apache.iceberg.io.OutputFile newOutputFile(String path) {
      return d.newOutputFile(path);
    }

    @Override
    public void deleteFile(String path) {
      deletes.incrementAndGet();
      if (failDeletes.get()) {
        injectedDeleteFailures.incrementAndGet();
        failedDeletePaths.add(path);
        throw new org.apache.iceberg.exceptions.RuntimeIOException(
            new java.io.IOException("injected delete failure"));
      }
      d.deleteFile(path);
    }

    @Override
    public java.util.Map<String, String> properties() {
      return d.properties();
    }
  }

  public void armLostAck(Supplier<RuntimeException> fault) {
    armed.set(fault);
  }

  public boolean isArmed() {
    return armed.get() != null;
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
    commits.incrementAndGet();
    Supplier<RuntimeException> fault = armed.getAndSet(null);
    if (fault != null) {
      RuntimeException e = fault.get();
      injected.incrementAndGet();
      lastThrown = e;
      throw e;
    }
  }

  @Override
  public FileIO io() {
    return io;
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
