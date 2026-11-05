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
package org.apache.iceberg.connect.mc5;

import java.util.Map;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;

/**
 * MC-5 repro catalog (the catalog is the ENVIRONMENT of the system under test). Every sink task
 * instantiates its own Mc5Catalog through {@code iceberg.catalog.catalog-impl}; all table operations
 * are delegated to one JVM-wide root so every task, worker and coordinator sees the same table.
 *
 * <p>Level-1 timing assistance only (kick-out mode): {@link #armStall} makes the next {@code
 * loadTable} issued from one named thread sleep, i.e. a slow catalog round-trip on that task's
 * {@code put()} path (the way a real catalog/storage latency spike stalls a sink task past {@code
 * max.poll.interval.ms}). It does not change any result the catalog returns. Inert unless armed.
 */
public class Mc5Catalog extends InMemoryCatalog {

  private static volatile Mc5Catalog root = null;
  private static final Object STALL_LOCK = new Object();
  private static String stallThread = null;
  private static long stallMillis = 0L;
  private static volatile long stallStartedAt = 0L;
  private static volatile long stallEndedAt = 0L;

  public static Mc5Catalog initRoot() {
    synchronized (Mc5Catalog.class) {
      if (root == null) {
        Mc5Catalog catalog = new Mc5Catalog();
        catalog.initialize("iceberg", Map.of());
        root = catalog;
      }

      return root;
    }
  }

  public static Mc5Catalog root() {
    return root;
  }

  private static volatile long initDelayMillis = 0L;

  /**
   * Level-1 timing assistance: catalog initialization latency for every catalog a (re)started sink
   * task creates in {@code IcebergSinkTask.start()} -- a real REST/Glue/JDBC catalog performs
   * network round trips there, between the Connect consumer's subscribe() and its first poll().
   */
  public static void setInitDelay(long millis) {
    initDelayMillis = millis;
  }

  public static long initDelay() {
    return initDelayMillis;
  }

  @Override
  public void initialize(String name, Map<String, String> properties) {
    long delay = initDelayMillis;
    if (delay > 0 && root != null) {
      try {
        Thread.sleep(delay);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    super.initialize(name, properties);
  }

  /** One-shot: the next loadTable on thread {@code threadName} sleeps {@code millis}. */
  public static void armStall(String threadName, long millis) {
    synchronized (STALL_LOCK) {
      stallThread = threadName;
      stallMillis = millis;
      stallStartedAt = 0L;
      stallEndedAt = 0L;
    }
  }

  public static void disarmStall() {
    synchronized (STALL_LOCK) {
      stallThread = null;
    }
  }

  public static long stallStartedAt() {
    return stallStartedAt;
  }

  public static long stallEndedAt() {
    return stallEndedAt;
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    if (this == root) {
      return super.newTableOps(tableIdentifier);
    }

    return root.newTableOps(tableIdentifier);
  }

  @Override
  public Table loadTable(TableIdentifier identifier) {
    long sleep = 0L;
    synchronized (STALL_LOCK) {
      if (stallThread != null && stallThread.equals(Thread.currentThread().getName())) {
        sleep = stallMillis;
        stallThread = null;
      }
    }

    if (sleep > 0) {
      stallStartedAt = System.currentTimeMillis();
      try {
        Thread.sleep(sleep);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        stallEndedAt = System.currentTimeMillis();
      }
    }

    return super.loadTable(identifier);
  }
}
