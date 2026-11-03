package org.apache.iceberg.dst;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * The decision log of one run.
 *
 * <p>Every line is a decision the harness made: which fiber the scheduler released, and what each
 * fault draw returned. Nothing else goes in, so the log is a function of the seed alone. The
 * SHA-256 is computed over every line ever appended; only the last {@link #KEEP} lines are held in
 * memory, so a run that explodes into millions of steps cannot fill the disk or the heap. That is
 * the lesson from the Camel harness, where a looping test once wrote a 31 GB log.
 */
public final class Trace {
  /** Lines kept for the failure report. The hash still covers the whole run. */
  public static final int KEEP = 200;

  private final Deque<String> tail = new ArrayDeque<>();
  private final MessageDigest digest;
  private long lines;

  public Trace() {
    try {
      this.digest = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is required", e);
    }
  }

  public void line(String text) {
    digest.update(text.getBytes(StandardCharsets.UTF_8));
    digest.update((byte) '\n');
    lines++;
    tail.addLast(text);
    if (tail.size() > KEEP) {
      tail.removeFirst();
    }
  }

  public long lineCount() {
    return lines;
  }

  /** Hash of the whole decision log, not just the kept tail. */
  public String sha256() {
    byte[] out;
    try {
      out = ((MessageDigest) digest.clone()).digest();
    } catch (CloneNotSupportedException e) {
      throw new IllegalStateException(e);
    }
    StringBuilder sb = new StringBuilder(64);
    for (byte b : out) {
      sb.append(Character.forDigit((b >> 4) & 0xf, 16)).append(Character.forDigit(b & 0xf, 16));
    }
    return sb.toString();
  }

  public List<String> tail() {
    return new ArrayList<>(tail);
  }
}
