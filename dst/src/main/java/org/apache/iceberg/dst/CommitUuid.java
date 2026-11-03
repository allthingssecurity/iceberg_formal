package org.apache.iceberg.dst;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Replaces the random commit UUID of a producer with a counter.
 *
 * <p>{@code SnapshotProducer} holds {@code private final String commitUUID =
 * UUID.randomUUID().toString()} and names every manifest after it. Iceberg writes manifests and
 * manifest lists as Avro with the gzip codec by default, so those random bytes change the
 * <em>compressed length</em> of a manifest list by a byte or two between two runs of the same seed.
 * The schedule is unaffected, but the byte lengths in a report are not, and a report that differs
 * between two replays is a report nobody can check.
 *
 * <p>So the field is overwritten with a counter. The counter advances once per producer, in
 * scheduler order, which is a function of the seed. If the field ever disappears the harness says
 * so once and carries on; {@link DeterminismTest} is then the thing that fails, which is the right
 * place to find out.
 */
public final class CommitUuid {

  private static final AtomicInteger NEXT = new AtomicInteger();
  private static volatile boolean warned;

  private CommitUuid() {}

  /** Resets the counter. Called at the start of every run, so seeds do not drift apart. */
  public static void reset() {
    NEXT.set(0);
  }

  /** Overwrites {@code commitUUID} on a producer, if the field is still there. */
  /**
   * Set {@code -Ddst.commitUuid=random} to leave Iceberg's own random UUID alone. Byte lengths in
   * reports then stop replaying exactly, but it is the switch that tells you whether a finding
   * depends on this reflection or not.
   */
  private static final boolean ENABLED = !"random".equals(System.getProperty("dst.commitUuid"));

  public static void makeDeterministic(Object producer) {
    if (!ENABLED) {
      return;
    }
    Field field = commitUuidField(producer.getClass());
    if (field == null) {
      if (!warned) {
        warned = true;
        System.err.println(
            "[dst] SnapshotProducer.commitUUID not found; manifest paths will be random and"
                + " byte lengths in reports will not replay exactly");
      }
      return;
    }
    try {
      field.setAccessible(true);
      field.set(producer, String.format("00000000-0000-4000-8000-%012d", NEXT.incrementAndGet()));
    } catch (ReflectiveOperationException | RuntimeException e) {
      if (!warned) {
        warned = true;
        System.err.println("[dst] could not fix SnapshotProducer.commitUUID: " + e);
      }
    }
  }

  private static Field commitUuidField(Class<?> type) {
    for (Class<?> c = type; c != null; c = c.getSuperclass()) {
      try {
        return c.getDeclaredField("commitUUID");
      } catch (NoSuchFieldException ignored) {
        // keep walking up
      }
    }
    return null;
  }
}
