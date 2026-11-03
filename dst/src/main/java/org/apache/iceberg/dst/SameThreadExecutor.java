package org.apache.iceberg.dst;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Runs every task on the caller's thread.
 *
 * <p>Injected into every producer through {@code scanManifestsWith} and {@code writeManifestsWith},
 * so Iceberg's own worker pool never starts and every boundary crossing belongs to a fiber the
 * scheduler owns. If a future release adds a pool the harness does not inject, the crossings from
 * it land in {@link Scheduler#unmanagedCrossings()} and the self-check fails.
 */
public final class SameThreadExecutor extends AbstractExecutorService {

  /**
   * Injects this executor into a producer's scan pool and, where the release has one, its manifest
   * write pool. {@code writeManifestsWith} only exists from Iceberg 1.12; calling it reflectively
   * is what lets one harness run against 1.8 through 1.12, which is how the recall scorecard gets
   * real pre-fix bytecode instead of a flag that imitates it.
   */
  /**
   * Whether this release has a manifest-write pool the harness can inject. Releases before 1.12.0
   * do not: {@code SnapshotUpdate.writeManifestsWith} was added there. Without it, manifest writes
   * run on Iceberg's own worker pool, which the scheduler does not own, and the escape counter says
   * so instead of pretending otherwise.
   */
  private static volatile Boolean writePoolInjectable;

  public static boolean writePoolInjectable() {
    return writePoolInjectable == null || writePoolInjectable;
  }

  public static <T extends org.apache.iceberg.SnapshotUpdate<T>> T inject(T update) {
    SameThreadExecutor inline = new SameThreadExecutor();
    T result = update.scanManifestsWith(inline);
    try {
      java.lang.reflect.Method m =
          result
              .getClass()
              .getMethod(
                  "writeManifestsWith", java.util.concurrent.ExecutorService.class, int.class);
      // The producer classes (MergeAppend, BaseOverwriteFiles, ...) are package-private, so the
      // Method handle has to be opened even though the method itself is public.
      m.setAccessible(true);
      writePoolInjectable = Boolean.TRUE;
      Object out = m.invoke(result, inline, 1);
      @SuppressWarnings("unchecked")
      T cast = (T) out;
      return cast;
    } catch (NoSuchMethodException e) {
      // Releases before 1.12.0 have no injection point for the manifest write pool.
      writePoolInjectable = Boolean.FALSE;
      return result;
    } catch (ReflectiveOperationException e) {
      if (e.getCause() instanceof UnsupportedOperationException) {
        return result;
      }
      throw new IllegalStateException(
          "could not inject the manifest write pool: " + e.getCause(), e);
    }
  }


  private volatile boolean shutdown;

  @Override
  public void execute(Runnable command) {
    command.run();
  }

  @Override
  public void shutdown() {
    shutdown = true;
  }

  @Override
  public List<Runnable> shutdownNow() {
    shutdown = true;
    return Collections.emptyList();
  }

  @Override
  public boolean isShutdown() {
    return shutdown;
  }

  @Override
  public boolean isTerminated() {
    return shutdown;
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) {
    return true;
  }

  @Override
  public <T> List<java.util.concurrent.Future<T>> invokeAll(
      Collection<? extends java.util.concurrent.Callable<T>> tasks) {
    List<java.util.concurrent.Future<T>> out = new ArrayList<>(tasks.size());
    for (java.util.concurrent.Callable<T> task : tasks) {
      out.add(submit(task));
    }
    return out;
  }
}
