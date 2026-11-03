package org.apache.iceberg.dst;

/**
 * One broken invariant.
 *
 * <p>A violation that a fault of the harness fully explains carries an {@link #excuse} and is not
 * counted as a defect. The distinction is the whole point: the harness is allowed to make the store
 * lose the tail of a closed write, and no code in Iceberg can keep a table readable after that. A
 * violation with no excuse is a claim about Iceberg, and has to be defended.
 */
public final class Violation {
  public final String invariant;
  public final String detail;
  public final String excuse;

  public Violation(String invariant, String detail) {
    this(invariant, detail, null);
  }

  public Violation(String invariant, String detail, String excuse) {
    this.invariant = invariant;
    this.detail = detail;
    this.excuse = excuse;
  }

  public boolean excused() {
    return excuse != null;
  }

  @Override
  public String toString() {
    return "[" + invariant + (excuse == null ? "" : " EXCUSED") + "] "
        + detail
        + (excuse == null ? "" : " -- " + excuse);
  }
}
