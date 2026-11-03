package org.apache.iceberg.dst;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * The decisive experiment for the finding in findings/r2-dst-harness.md §6, run without patching
 * the Iceberg jar.
 *
 * <p>The proposed cause: {@code BaseTransaction.applyUpdates} (BaseTransaction.java:439-456)
 * re-commits the <em>same</em> {@code PendingUpdate} objects when the base has moved, so the same
 * {@code MergingSnapshotProducer} instances and the same {@code ManifestFilterManager} caches are
 * reused. {@code ManifestFilterManager.filterManifest} (line 369) returns
 * {@code filteredManifests.get(manifest)} straight away, and that map is keyed on
 * {@code ManifestFile} whose {@code equals}/{@code hashCode} compare the path and nothing else
 * (GenericManifestFile.java:406-419) — so a manifest at a <em>different</em> sequence number is a
 * cache hit. The cached value's entries were written with {@code writer.existing(entry)}, which
 * records each {@code data_sequence_number} explicitly as resolved on the first pass.
 * {@code invalidateFilteredCache()} is private with four callers, all filter-criteria mutators, so
 * nothing clears it when the base changes.
 *
 * <p>If that is the cause, clearing those caches at the moment the base is refreshed should make the
 * disagreement disappear with nothing else altered. This clears them on <em>every</em> refresh,
 * which is strictly more aggressive than the fix a reviewer would want and therefore a sound probe:
 * over-clearing costs performance, not correctness.
 *
 * <p>Off unless {@code -Ddst.invalidateFilterCache=true}.
 */
public final class FilterCacheProbe {

  /**
   * Which group of reused caches to clear: {@code filter}, {@code merge}, {@code new}, {@code all},
   * or {@code off}. Every one of these survives a {@code BaseTransaction} re-apply, and all three
   * record entry sequence numbers explicitly via {@code writer.existing(entry)}, so each is a
   * candidate for carrying a first-pass number into the committed snapshot.
   */
  private static volatile String MODE = System.getProperty("dst.invalidateFilterCache", "off");

  private static volatile boolean ENABLED = !"off".equals(MODE) && !"false".equals(MODE);

  /**
   * MODE may name several groups, comma-separated (e.g. {@code filter,withmeta}), so that two caches
   * that could each mask the other can be cleared together without also clearing {@code new}. An
   * unknown name throws: a silent fall-through to "nothing cleared" would produce a control row
   * that looks like a refutation.
   */
  private static volatile java.util.Set<String> GROUPS = parseGroups(MODE);

  /**
   * Sets the mode for the rest of this JVM, so one test can run several probe rows (r5: the
   * four-row cause table in MetastoreFlipTest). Same grammar as the system property; an unknown
   * group throws before anything changes. Returns the previous mode so the caller can restore it.
   */
  public static synchronized String setMode(String mode) {
    java.util.Set<String> groups = parseGroups(mode);
    String previous = MODE;
    MODE = mode;
    GROUPS = groups;
    ENABLED = !"off".equals(mode) && !"false".equals(mode);
    return previous;
  }

  private static java.util.Set<String> parseGroups(String mode) {
    java.util.Set<String> known = java.util.Set.of("filter", "merge", "withmeta", "new");
    // "rewrite" (r5, A1-29) is accepted by name only and deliberately not part of "all", so the
    // round-2 meaning of "all" is unchanged.
    java.util.Set<String> named = java.util.Set.of("filter", "merge", "withmeta", "new", "rewrite");
    java.util.Set<String> out = new java.util.HashSet<>();
    if ("off".equals(mode) || "false".equals(mode)) {
      return out;
    }
    if ("all".equals(mode) || "true".equals(mode)) {
      return known;
    }
    for (String part : mode.split(",")) {
      String name = part.trim();
      if (!named.contains(name)) {
        throw new IllegalArgumentException(
            "dst.invalidateFilterCache: unknown group '" + name + "', expected one of " + known);
      }
      out.add(name);
    }
    return out;
  }

  private static boolean group(String name) {
    return ENABLED && GROUPS.contains(name);
  }

  /** ManifestFilterManager instances held by MergingSnapshotProducer, and their caches. */
  private static final String[] FILTER_MANAGERS = {"filterManager", "deleteFilterManager"};
  private static final String[] FILTER_CACHES = {"filteredManifests", "filteredManifestResults"};

  /** ManifestMergeManager instances, and the cache inside them. */
  private static final String[] MERGE_MANAGERS = {"mergeManager", "deleteMergeManager"};
  private static final String[] MERGE_CACHES = {"mergedManifests"};

  /** The producer's own already-written manifests for the new files. */
  private static final String[] NEW_MANIFEST_FIELDS = {
    "cachedNewDataManifests", "cachedNewDeleteManifests"
  };

  /**
   * {@code SnapshotProducer.manifestsWithMetadata} (SnapshotProducer.java:103), a final
   * path-keyed Caffeine cache built once per producer. Its loader returns the key object verbatim
   * when {@code file.snapshotId() != null} (:139-141), so a manifest read from a pass-1 manifest
   * list is stored under its path and returned to a pass-2 lookup at :330.
   */
  private static final String WITH_METADATA_FIELD = "manifestsWithMetadata";

  private static final List<Object> producers = Collections.synchronizedList(new ArrayList<>());

  /** The thread that registered each producer, for groups that may only fire on its own refresh. */
  private static final Map<Object, Thread> owners =
      Collections.synchronizedMap(new java.util.IdentityHashMap<>());
  private static volatile int cleared;
  private static volatile long withMetadataEvicted;

  private FilterCacheProbe() {}

  public static boolean enabled() {
    return ENABLED;
  }

  public static String mode() {
    return MODE;
  }

  public static void reset() {
    producers.clear();
    owners.clear();
    cleared = 0;
    withMetadataEvicted = 0;
  }

  /** How many caches were cleared in this run; 0 means the probe never fired. */
  public static int cleared() {
    return cleared;
  }

  /** How many entries were actually evicted from {@code manifestsWithMetadata}. */
  public static long withMetadataEvicted() {
    return withMetadataEvicted;
  }

  /** Remembers a producer created inside a transaction, so its caches can be reached later. */
  public static void register(Object producer) {
    if (ENABLED && producer != null) {
      producers.add(producer);
      owners.put(producer, Thread.currentThread());
    }
  }

  /** Clears every registered producer's filter caches. Called when the base metadata is refreshed. */
  public static void invalidateAll() {
    if (!ENABLED) {
      return;
    }
    List<Object> snapshot;
    synchronized (producers) {
      snapshot = new ArrayList<>(producers);
    }
    for (Object producer : snapshot) {
      if (group("filter")) {
        clearVia(producer, FILTER_MANAGERS, FILTER_CACHES);
      }
      if (group("merge")) {
        clearVia(producer, MERGE_MANAGERS, MERGE_CACHES);
      }
      if (group("withmeta")) {
        Object cache = read(producer, WITH_METADATA_FIELD);
        if (cache != null) {
          // Count only when the cache actually held something. "Cleared an empty cache" and
          // "cleared a populated cache and it did not help" are completely different conclusions,
          // and the first means this hook point cannot test the hypothesis at all.
          long size = estimatedSize(cache);
          if (invalidateAll(cache) && size > 0) {
            cleared++;
            withMetadataEvicted += size;
          }
        }
      }
      if (group("rewrite") && owners.get(producer) == Thread.currentThread()) {
        // Only on the owning writer's own refresh (BaseTransaction.commitSimpleTransaction refreshes
        // right before it re-applies). Clearing it during another writer's refresh would land in the
        // middle of this producer's own apply() and break its file-count validation, which is the
        // lever's doing, not Iceberg's.
        // BaseRewriteManifests.rewrittenManifests: emptying it makes requiresRewrite() true
        // (BaseRewriteManifests.java:212-214), so the next apply rewrites from the current base
        // instead of reusing the pass-1 newManifests.
        Object value = read(producer, "rewrittenManifests");
        if (value instanceof java.util.Collection) {
          java.util.Collection<?> collection = (java.util.Collection<?>) value;
          if (!collection.isEmpty()) {
            collection.clear();
            cleared++;
          }
        }
      }
      if (group("new")) {
        for (String field : NEW_MANIFEST_FIELDS) {
          Object value = read(producer, field);
          if (value instanceof java.util.Collection) {
            java.util.Collection<?> collection = (java.util.Collection<?>) value;
            if (!collection.isEmpty()) {
              collection.clear();
              cleared++;
            }
          }
        }
      }
    }
  }

  private static long estimatedSize(Object cache) {
    try {
      java.lang.reflect.Method method = cache.getClass().getMethod("estimatedSize");
      method.setAccessible(true);
      Object value = method.invoke(cache);
      return value instanceof Long ? (Long) value : 0L;
    } catch (ReflectiveOperationException | RuntimeException e) {
      return 0L;
    }
  }

  /** Caffeine's Cache.invalidateAll(), reached reflectively. */
  private static boolean invalidateAll(Object cache) {
    try {
      java.lang.reflect.Method method = cache.getClass().getMethod("invalidateAll");
      method.setAccessible(true);
      method.invoke(cache);
      return true;
    } catch (ReflectiveOperationException | RuntimeException e) {
      return false;
    }
  }

  private static void clearVia(Object producer, String[] managerFields, String[] cacheFields) {
    for (String managerField : managerFields) {
      Object manager = read(producer, managerField);
      if (manager == null) {
        continue;
      }
      for (String cacheField : cacheFields) {
        Object cache = read(manager, cacheField);
        if (cache instanceof Map) {
          Map<?, ?> map = (Map<?, ?>) cache;
          if (!map.isEmpty()) {
            map.clear();
            cleared++;
          }
        }
      }
    }
  }

  private static Object read(Object target, String name) {
    for (Class<?> c = target.getClass(); c != null; c = c.getSuperclass()) {
      try {
        Field field = c.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
      } catch (NoSuchFieldException e) {
        // keep walking up
      } catch (ReflectiveOperationException | RuntimeException e) {
        return null;
      }
    }
    return null;
  }
}
