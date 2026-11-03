package org.apache.iceberg.flink.maintenance.operator;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.EventReceivingTasks;
import org.apache.flink.runtime.operators.coordination.MockOperatorCoordinatorContext;
import org.junit.jupiter.api.Test;

/**
 * E-33: BaseCoordinator keeps LOCK_RELEASE_CONSUMERS and PENDING_RELEASE_EVENTS in statics.
 * Coordinators are driven only through their public OperatorCoordinator API
 * (handleEventFromOperator, executionAttemptReady, close), so the real coordinator-thread path and
 * context.failJob are exercised.
 */
class E33BaseCoordinatorTest {

  enum Scenario {
    // (a) two tables in one classloader: B's release arrives before B registers, then A registers.
    CME_TWO_TABLES,
    // control for (a): only one table; its release arrives before its own registration.
    CONTROL_ONE_TABLE,
    // (b) shared classloader: job X's coordinator closes while job Y is running; Y then releases.
    CLOSE_OTHER_JOB,
    // control for (b): no other coordinator closes.
    CONTROL_NO_CLOSE
  }

  static final AtomicLong IDS = new AtomicLong(1);

  static final class Coord {
    final MockOperatorCoordinatorContext ctx;
    final BaseCoordinator c;
    final EventReceivingTasks tasks = EventReceivingTasks.createForRunningTasks();

    Coord(boolean trigger, String name) throws Exception {
      this.ctx = new MockOperatorCoordinatorContext(new OperatorID(IDS.incrementAndGet(), 7L), 1);
      this.c =
          trigger ? new TriggerManagerCoordinator(name, ctx) : new LockRemoverCoordinator(name, ctx);
      c.start();
      c.executionAttemptReady(0, 0, tasks.createGatewayForSubtask(0, 0));
      flush();
    }

    /** Waits until everything queued on the coordinator thread so far has run. */
    void flush() throws Exception {
      CompletableFuture<byte[]> f = new CompletableFuture<>();
      c.checkpointCoordinator(1L, f);
      f.get(5, TimeUnit.SECONDS);
    }
  }

  static String run(Scenario s) throws Exception {
    String a = "db.A";
    String b = "db.B";
    String result;
    switch (s) {
      case CME_TWO_TABLES:
        {
          Coord tmA = new Coord(true, "TM-A");
          Coord lrB = new Coord(false, "LR-B");
          lrB.c.handleEventFromOperator(0, 0, new LockReleaseEvent(b, 1L));
          lrB.flush();
          tmA.c.handleEventFromOperator(0, 0, new LockRegisterEvent(a));
          tmA.flush();
          result =
              String.format(
                  "jobFailed=%s cause=%s pendingAfter=%d",
                  tmA.ctx.isJobFailed(),
                  tmA.ctx.isJobFailed()
                      ? tmA.ctx.getJobFailureReason().getClass().getSimpleName()
                      : "-",
                  tmA.c.pendingReleaseEvents().size());
          tmA.c.close();
          lrB.c.close();
          break;
        }
      case CONTROL_ONE_TABLE:
        {
          Coord tmA = new Coord(true, "TM-A");
          Coord lrA = new Coord(false, "LR-A");
          lrA.c.handleEventFromOperator(0, 0, new LockReleaseEvent(a, 1L));
          lrA.flush();
          tmA.c.handleEventFromOperator(0, 0, new LockRegisterEvent(a));
          tmA.flush();
          result =
              String.format(
                  "jobFailed=%s pendingAfter=%d deliveredToTM-A=%d",
                  tmA.ctx.isJobFailed(),
                  tmA.c.pendingReleaseEvents().size(),
                  tmA.tasks.getSentEventsForSubtask(0).size());
          tmA.c.close();
          lrA.c.close();
          break;
        }
      case CLOSE_OTHER_JOB:
      case CONTROL_NO_CLOSE:
        {
          Coord tmY = new Coord(true, "TM-Y");
          Coord lrY = new Coord(false, "LR-Y");
          tmY.c.handleEventFromOperator(0, 0, new LockRegisterEvent(b));
          tmY.flush();
          Coord lrX = new Coord(false, "LR-X");
          if (s == Scenario.CLOSE_OTHER_JOB) {
            lrX.c.close(); // job X is cancelled / finishes
          }
          lrY.c.handleEventFromOperator(0, 0, new LockReleaseEvent(b, 2L));
          lrY.flush();
          tmY.flush();
          result =
              String.format(
                  "jobFailed=%s deliveredToTM-Y=%d pendingAfter=%d",
                  tmY.ctx.isJobFailed() || lrY.ctx.isJobFailed(),
                  tmY.tasks.getSentEventsForSubtask(0).size(),
                  lrY.c.pendingReleaseEvents().size());
          tmY.c.close();
          lrY.c.close();
          if (s == Scenario.CONTROL_NO_CLOSE) {
            lrX.c.close();
          }
          break;
        }
      default:
        throw new IllegalArgumentException("unknown scenario " + s);
    }
    return result;
  }

  @Test
  void e33() throws Exception {
    String v = System.getProperty("r5.runs");
    int n = v == null ? 10 : Integer.parseInt(v);
    Path out = Paths.get(System.getProperty("r5.out", "build/r5"));
    Files.createDirectories(out);
    Map<String, Map<String, Integer>> results = new TreeMap<>();
    for (Scenario s : Scenario.values()) {
      Map<String, Integer> counts = new TreeMap<>();
      for (int i = 0; i < n; i++) {
        counts.merge(run(s), 1, Integer::sum);
      }
      results.put(s.name(), counts);
    }
    try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(out.resolve("e33.txt")))) {
      w.println("E-33 iceberg " + System.getProperty("icebergVersion") + " runs=" + n);
      results.forEach(
          (k, c) -> c.forEach((o, cnt) -> w.printf("%-18s | %d/%d | %s%n", k, cnt, n, o)));
    }
    for (String o : results.get("CONTROL_ONE_TABLE").keySet()) {
      assertTrue(o.startsWith("jobFailed=false"), o);
    }
    for (String o : results.get("CONTROL_NO_CLOSE").keySet()) {
      assertTrue(o.contains("deliveredToTM-Y=1"), o);
    }
  }
}
