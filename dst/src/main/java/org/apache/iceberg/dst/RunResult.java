package org.apache.iceberg.dst;

import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.TableMetadata;

/** Everything one seed produced. */
public final class RunResult {
  public final long seed;
  public final FaultConfig config;
  public final Trace trace;
  public final Scheduler scheduler;
  public final FakeStore store;
  public final FakeTableOperations ops;
  public final List<Workload.Outcome> outcomes;
  public final TableMetadata finalMetadata;
  public final List<Violation> violations = new ArrayList<>();
  public Throwable harnessFailure;

  RunResult(
      long seed,
      FaultConfig config,
      Trace trace,
      Scheduler scheduler,
      FakeStore store,
      FakeTableOperations ops,
      List<Workload.Outcome> outcomes,
      TableMetadata finalMetadata) {
    this.seed = seed;
    this.config = config;
    this.trace = trace;
    this.scheduler = scheduler;
    this.store = store;
    this.ops = ops;
    this.outcomes = outcomes;
    this.finalMetadata = finalMetadata;
  }

  /** Violations the harness's own faults do not explain. These are the claims about Iceberg. */
  public List<Violation> unexplained() {
    List<Violation> out = new ArrayList<>();
    for (Violation violation : violations) {
      if (!violation.excused()) {
        out.add(violation);
      }
    }
    return out;
  }

  public boolean clean() {
    return unexplained().isEmpty() && harnessFailure == null;
  }

  /** The failure report: seed, hash, replay hint, violations and the tail of the decision log. */
  public String report() {
    StringBuilder sb = new StringBuilder();
    sb.append("=== DST failure ===\n");
    sb.append("seed            : ").append(seed).append('\n');
    sb.append("iceberg          : ")
        .append(System.getProperty("icebergVersion", "unknown"))
        .append('\n');
    sb.append("config          : ").append(config).append('\n');
    sb.append("trace sha-256   : ").append(trace.sha256()).append('\n');
    sb.append("trace lines     : ").append(trace.lineCount()).append('\n');
    sb.append("replay          : Simulation.run(").append(seed).append("L, config)\n");
    if (harnessFailure != null) {
      sb.append("harness failure : ").append(harnessFailure).append('\n');
    }
    sb.append("writers:\n");
    for (Workload.Outcome outcome : outcomes) {
      sb.append("  ").append(outcome).append('\n');
    }
    sb.append("catalog attempts:\n");
    for (FakeTableOperations.CommitRecord record : ops.commits()) {
      sb.append("  ").append(record).append('\n');
    }
    sb.append("store writes:\n");
    for (FakeStore.WriteRecord write : store.writes()) {
      sb.append("  ")
          .append(store.alias(write.path))
          .append(" by=")
          .append(write.fiber)
          .append(" declared=")
          .append(write.declaredBytes)
          .append(" stored=")
          .append(write.storedBytes)
          .append(write.truncated ? " TRUNCATED" : "")
          .append(write.reportedFailure ? " reportedFailure" : "")
          .append('\n');
    }
    sb.append("store deletes:\n");
    for (String deleted : store.deletes()) {
      sb.append("  ").append(store.alias(deleted)).append('\n');
    }
    sb.append("snapshots captured at commit:\n");
    for (java.util.Map.Entry<Long, TableView.SnapshotFiles> e : ops.capturedAtCommit().entrySet()) {
      TableView.SnapshotFiles f = e.getValue();
      sb.append("  ")
          .append(e.getKey())
          .append(" mlist=")
          .append(f.manifestList == null ? "-" : store.alias(f.manifestList))
          .append(" manifests=");
      for (String m : f.manifests.keySet()) {
        sb.append(store.alias(m)).append(' ');
      }
      sb.append(f.unreadable == null ? "" : " UNREADABLE: " + f.unreadable).append('\n');
    }
    sb.append("snapshot graph (id, parent, seq, operation, data files with their seq):\n");
    org.apache.iceberg.io.FileIO view = ((FakeFileIO) ops.io()).observerView();
    for (org.apache.iceberg.Snapshot snapshot : finalMetadata.snapshots()) {
      sb.append("  ")
          .append(snapshot.snapshotId())
          .append(" parent=")
          .append(snapshot.parentId())
          .append(" seq=")
          .append(snapshot.sequenceNumber())
          .append(' ')
          .append(snapshot.operation())
          .append(" -> ");
      TableView.SnapshotFiles files = TableView.read(finalMetadata, snapshot.snapshotId(), view);
      if (files.unreadable != null) {
        sb.append("UNREADABLE");
      } else {
        for (TableView.FileFact fact : files.facts.values()) {
          if (fact.content == org.apache.iceberg.FileContent.DATA) {
            sb.append(store.alias(fact.path)).append('@').append(fact.dataSequenceNumber).append(' ');
          }
        }
      }
      sb.append('\n');
    }
    sb.append("refs: ").append(finalMetadata.refs()).append('\n');
    sb.append("snapshot summaries:\n");
    for (org.apache.iceberg.Snapshot snapshot : finalMetadata.snapshots()) {
      sb.append("  ")
          .append(snapshot.snapshotId())
          .append(' ')
          .append(snapshot.operation())
          .append(' ')
          .append(snapshot.summary())
          .append('\n');
    }
    sb.append("violations (").append(violations.size()).append("):\n");
    for (Violation violation : violations) {
      sb.append("  ").append(violation).append('\n');
    }
    sb.append("last ").append(Trace.KEEP).append(" decisions:\n");
    for (String line : trace.tail()) {
      sb.append("  ").append(line).append('\n');
    }
    return sb.toString();
  }
}
