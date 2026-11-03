package org.apache.iceberg.dst;

/** Writes a small report under build/dst/, never to stdout (Gradle would keep stdout on disk). */
final class Reports {
  private static final int MAX_CHARS = 64 * 1024;

  private Reports() {}

  static void write(String name, String text) {
    String capped =
        text.length() <= MAX_CHARS ? text : text.substring(0, MAX_CHARS) + "\n... (capped)\n";
    try {
      java.nio.file.Path dir = java.nio.file.Paths.get("build", "dst");
      java.nio.file.Files.createDirectories(dir);
      java.nio.file.Files.write(
          dir.resolve(name), capped.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
  }
}
