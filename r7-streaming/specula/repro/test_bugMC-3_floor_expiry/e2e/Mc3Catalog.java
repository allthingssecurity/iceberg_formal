/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.connect.mc3;

import java.util.Map;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;

/**
 * MC-3 reproduction catalog (environment, not system under test): one in-memory catalog shared by
 * the test and every sink task of the JVM (each task instantiates its own Mc3Catalog through
 * {@code iceberg.catalog.catalog-impl}; all table operations are routed to the shared root).
 *
 * <p>The only fault it can inject is the documented catalog behaviour "commit landed, but the
 * client cannot tell": the compare-and-swap is applied and then {@link
 * CommitStateUnknownException} is thrown to the caller (what a REST/Glue/HMS client reports when
 * the commit request times out after the server applied it). It fires once, on the next connector
 * commit (a snapshot carrying {@code kafka.connect.commit-id}), and only when armed.
 */
public class Mc3Catalog extends InMemoryCatalog {

  private static final String COMMIT_ID_PROP = "kafka.connect.commit-id";
  private static final Object LOCK = new Object();
  private static volatile Mc3Catalog root = null;
  private static boolean armedUnknownLanded = false;
  private static int fired = 0;

  public static Mc3Catalog initRoot(Map<String, String> properties) {
    synchronized (Mc3Catalog.class) {
      if (root == null) {
        Mc3Catalog catalog = new Mc3Catalog();
        catalog.initialize("iceberg", properties);
        root = catalog;
      }

      return root;
    }
  }

  public static Mc3Catalog root() {
    return root;
  }

  /** The next connector commit lands and then reports CommitStateUnknownException. */
  public static void armUnknownLanded() {
    synchronized (LOCK) {
      armedUnknownLanded = true;
    }
  }

  public static int firedCount() {
    synchronized (LOCK) {
      return fired;
    }
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    if (this == root) {
      return new FaultInjectingOps(super.newTableOps(tableIdentifier));
    }

    return root.newTableOps(tableIdentifier);
  }

  private static boolean isConnectorCommit(TableMetadata base, TableMetadata metadata) {
    Snapshot next = metadata.currentSnapshot();
    if (next == null || !next.summary().containsKey(COMMIT_ID_PROP)) {
      return false;
    }

    Snapshot previous = base != null ? base.currentSnapshot() : null;
    return previous == null || previous.snapshotId() != next.snapshotId();
  }

  private static final class FaultInjectingOps implements TableOperations {
    private final TableOperations delegate;

    private FaultInjectingOps(TableOperations delegate) {
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
      boolean unknownLanded;
      synchronized (LOCK) {
        unknownLanded = armedUnknownLanded && isConnectorCommit(base, metadata);
        if (unknownLanded) {
          armedUnknownLanded = false;
          fired += 1;
        }
      }

      delegate.commit(base, metadata); // the CAS is applied in every case
      if (unknownLanded) {
        throw new CommitStateUnknownException(
            new RuntimeException("MC-3 injected: commit request timed out after it was applied"));
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
      LocationProvider provider = delegate.locationProvider();
      return provider != null
          ? provider
          : LocationProviders.locationsFor(current().location(), current().properties());
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
