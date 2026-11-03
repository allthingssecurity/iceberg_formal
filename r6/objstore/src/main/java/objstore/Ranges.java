package objstore;

import java.io.IOException;
import java.util.Arrays;
import java.util.SplittableRandom;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;

/** Range reads (RangeReadable.readFully / readTail) through the faulty IO; only on releases that have it. */
final class Ranges {
  private static final boolean SUPPORTED;

  static {
    boolean ok;
    try {
      Class.forName("org.apache.iceberg.io.RangeReadable");
      ok = true;
    } catch (ClassNotFoundException e) {
      ok = false;
    }
    SUPPORTED = ok;
  }

  static boolean supported() {
    return SUPPORTED;
  }

  static void check(Run run, String loc, Run.FileRec rec) {
    if (rec.len < 2) {
      return;
    }
    byte[] expected = Run.content(rec.contentSeed, (int) rec.len);
    SplittableRandom r = new SplittableRandom(rec.crc);
    try (SeekableInputStream in = run.table.io().newInputFile(loc).newStream()) {
      if (!(in instanceof RangeReadable)) {
        return;
      }
      RangeReadable rr = (RangeReadable) in;
      for (int i = 0; i < 2; i++) {
        long pos = r.nextLong(rec.len - 1);
        int len = (int) Math.min(rec.len - pos, 1 + r.nextInt(512 * 1024));
        byte[] buf = new byte[len];
        rr.readFully(pos, buf, 0, len);
        if (!Arrays.equals(buf, 0, len, expected, (int) pos, (int) pos + len)) {
          run.count("MAJOR_silentBadRangeRead");
          run.detail("readFully mismatch " + run.key(loc) + " pos=" + pos + " len=" + len);
        } else {
          run.count("read_rangeOK");
        }
      }
      int tl = (int) Math.min(rec.len, 1 + r.nextInt(64 * 1024));
      byte[] tail = new byte[tl];
      int n = rr.readTail(tail, 0, tl);
      if (n != tl || !Arrays.equals(tail, 0, tl, expected, (int) rec.len - tl, (int) rec.len)) {
        run.count("MAJOR_silentBadTailRead");
        run.detail("readTail mismatch " + run.key(loc) + " n=" + n + " want=" + tl);
      } else {
        run.count("read_tailOK");
      }
    } catch (RuntimeException | IOException e) {
      run.count("read_rangeThrew:" + Run.rootName(e));
      run.detail("range read threw " + run.key(loc) + " " + Run.chain(e));
    }
  }

  private Ranges() {}
}
