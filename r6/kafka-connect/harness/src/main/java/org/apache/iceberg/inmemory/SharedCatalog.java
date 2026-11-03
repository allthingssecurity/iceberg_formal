package org.apache.iceberg.inmemory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;

/**
 * Catalog loaded by every sink task through iceberg.catalog.catalog-impl. All instances share one
 * InMemoryCatalog (the "metastore"); each instance has its own closed flag (a closed REST/Glue/JDBC
 * client throws, cf. #13593 "Connection pool shut down"). Table commits go through a fault plan.
 */
public class SharedCatalog extends BaseMetastoreCatalog {
  public static InMemoryCatalog DELEGATE;

  public enum Kind {
    CFE_BEFORE, // CommitFailedException, nothing applied (SnapshotProducer retries these)
    CSUE_BEFORE, // CommitStateUnknownException, nothing applied
    CSUE_AFTER, // applied, then CommitStateUnknownException
    RTE_BEFORE, // generic RuntimeException (e.g. Glue/REST client error), nothing applied
    RTE_AFTER, // applied, then generic RuntimeException (lost response, not typed as CSUE)
    CRASH_BEFORE, // the committing process dies before the catalog applies the commit
    CRASH_AFTER; // the catalog applies the commit, then the committing process dies

    public static Kind of(String s) {
      return Kind.valueOf(s); // unknown name -> IllegalArgumentException
    }
  }

  public static final class InjectedCFE extends CommitFailedException {
    public InjectedCFE(String m) {
      super(m);
    }
  }

  public static final class InjectedCSUE extends CommitStateUnknownException {
    public InjectedCSUE(String m) {
      super(new RuntimeException(m));
    }
  }

  public static final class InjectedRTE extends RuntimeException {
    public InjectedRTE(String m) {
      super(m);
    }
  }

  public static final class ClosedCatalogException extends IllegalStateException {
    public ClosedCatalogException(String m) {
      super(m);
    }
  }

  // fault plan: commits numbered from 1 (every TableOperations.commit attempt that adds metadata)
  public static Kind faultKind = null;
  public static int faultFrom = -1;
  public static int faultCount = 0;
  public static int commitAttempts = 0;
  public static final List<String> faultLog = new ArrayList<>();
  public static boolean closedThrows = true;
  /** hook for CRASH_*: marks the stepping coordinator's task crashed, returns the exception */
  public static java.util.function.Supplier<RuntimeException> crashHook = null;
  /** owner check: a crashed process cannot reach the catalog */
  public static java.util.function.IntPredicate isCrashed = t -> false;
  public static java.util.function.IntSupplier currentTask = () -> -1;

  public static synchronized void reset() {
    faultKind = null;
    faultFrom = -1;
    faultCount = 0;
    commitAttempts = 0;
    faultLog.clear();
    closedThrows = true;
  }

  private volatile boolean closed = false;
  private String name;
  private final int ownerTask = currentTask.getAsInt();

  public SharedCatalog() {}

  @Override
  public void initialize(String name, Map<String, String> properties) {
    this.name = name;
  }

  @Override
  public String name() {
    return name;
  }

  void checkOpen() {
    if (isCrashed.test(ownerTask)) {
      throw new IllegalStateException("catalog client of crashed task " + ownerTask);
    }
    if (closed && closedThrows) {
      throw new ClosedCatalogException("catalog instance closed (Connection pool shut down)");
    }
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier id) {
    checkOpen();
    return new Ops(DELEGATE.newTableOps(id));
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier id) {
    return DELEGATE.defaultWarehouseLocation(id);
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    checkOpen();
    return DELEGATE.listTables(namespace);
  }

  @Override
  public boolean dropTable(TableIdentifier id, boolean purge) {
    throw new UnsupportedOperationException("dropTable");
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    throw new UnsupportedOperationException("renameTable");
  }

  @Override
  public void close() {
    closed = true;
  }

  final class Ops implements TableOperations {
    final TableOperations d;

    Ops(TableOperations d) {
      this.d = d;
    }

    @Override
    public TableMetadata current() {
      checkOpen();
      return d.current();
    }

    @Override
    public TableMetadata refresh() {
      checkOpen();
      return d.refresh();
    }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
      checkOpen();
      Kind k = null;
      int n;
      synchronized (SharedCatalog.class) {
        n = ++commitAttempts;
        if (faultKind != null && n >= faultFrom && n < faultFrom + faultCount) {
          k = faultKind;
          faultLog.add("commit#" + n + " " + k + " on " + Thread.currentThread().getName());
        }
      }
      if (k == null) {
        d.commit(base, metadata);
        return;
      }
      switch (k) {
        case CFE_BEFORE:
          throw new InjectedCFE("injected CFE at commit#" + n);
        case CSUE_BEFORE:
          throw new InjectedCSUE("injected CSUE(before) at commit#" + n);
        case CSUE_AFTER:
          d.commit(base, metadata);
          throw new InjectedCSUE("injected CSUE(after) at commit#" + n);
        case RTE_BEFORE:
          throw new InjectedRTE("injected RTE(before) at commit#" + n);
        case RTE_AFTER:
          d.commit(base, metadata);
          throw new InjectedRTE("injected RTE(after) at commit#" + n);
        case CRASH_BEFORE:
          throw crashHook.get();
        case CRASH_AFTER:
          d.commit(base, metadata);
          throw crashHook.get();
        default:
          throw new IllegalStateException("unknown fault " + k);
      }
    }

    @Override
    public FileIO io() {
      return d.io();
    }

    @Override
    public EncryptionManager encryption() {
      return d.encryption();
    }

    @Override
    public String metadataFileLocation(String fileName) {
      return d.metadataFileLocation(fileName);
    }

    @Override
    public LocationProvider locationProvider() {
      return d.locationProvider();
    }

    @Override
    public TableOperations temp(TableMetadata uncommittedMetadata) {
      return d.temp(uncommittedMetadata);
    }

    @Override
    public long newSnapshotId() {
      return d.newSnapshotId();
    }

    @Override
    public boolean requireStrictCleanup() {
      return d.requireStrictCleanup();
    }
  }

  static {
    // keep the import used on every release
    LocationProviders.class.getName();
  }
}
