package org.apache.iceberg.connect;

import org.apache.iceberg.catalog.Catalog;

/** CR-3 repro helper: exposes the sink task's own (package-private) catalog loader. */
public final class Cr3CurrentCatalogs {
  private Cr3CurrentCatalogs() {}

  public static Catalog load(IcebergSinkConfig config) {
    return CatalogUtils.loadCatalog(config);
  }
}
