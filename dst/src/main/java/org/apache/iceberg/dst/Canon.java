package org.apache.iceberg.dst;

import java.util.regex.Pattern;

/**
 * Normalises the random names Iceberg generates.
 *
 * <p>A manifest is named after a {@code UUID.randomUUID()} drawn inside the producer, which the
 * harness cannot seed. The schedule does not depend on it, but it leaks into exception messages,
 * and a message that differs between two runs of one seed makes the run look non-deterministic
 * when it is not. Anything the harness keeps or compares goes through here first.
 */
public final class Canon {

  private static final Pattern UUID =
      Pattern.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");

  private Canon() {}

  public static String normalize(String text) {
    return text == null ? null : UUID.matcher(text).replaceAll("<uuid>");
  }
}
