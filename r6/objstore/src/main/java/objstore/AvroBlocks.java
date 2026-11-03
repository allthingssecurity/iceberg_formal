package objstore;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Minimal Avro object-container parser: returns the byte offset just past each data block's sync marker. */
final class AvroBlocks {
  private final byte[] b;
  private int pos = 0;

  private AvroBlocks(byte[] b) {
    this.b = b;
  }

  private long readLong() {
    long n = 0;
    int shift = 0;
    while (true) {
      int x = b[pos++] & 0xff;
      n |= (long) (x & 0x7f) << shift;
      if ((x & 0x80) == 0) {
        break;
      }
      shift += 7;
    }
    return (n >>> 1) ^ -(n & 1);
  }

  static long[] blockEnds(byte[] data) {
    AvroBlocks p = new AvroBlocks(data);
    if (data.length < 4 || data[0] != 'O' || data[1] != 'b' || data[2] != 'j' || data[3] != 1) {
      throw new IllegalArgumentException("not an Avro container");
    }
    p.pos = 4;
    while (true) {
      long count = p.readLong();
      if (count == 0) {
        break;
      }
      if (count < 0) {
        count = -count;
        p.readLong(); // block byte size
      }
      for (long i = 0; i < count; i++) {
        long kl = p.readLong();
        p.pos += (int) kl;
        long vl = p.readLong();
        p.pos += (int) vl;
      }
    }
    byte[] sync = Arrays.copyOfRange(data, p.pos, p.pos + 16);
    p.pos += 16;
    List<Long> ends = new ArrayList<>();
    while (p.pos < data.length) {
      p.readLong(); // object count
      long size = p.readLong();
      p.pos += (int) size;
      if (!Arrays.equals(sync, 0, 16, data, p.pos, p.pos + 16)) {
        throw new IllegalStateException("sync marker mismatch at " + p.pos);
      }
      p.pos += 16;
      ends.add((long) p.pos);
    }
    return ends.stream().mapToLong(Long::longValue).toArray();
  }

}
