package org.apache.iceberg.dst;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * A seeded scheduler that puts the writer threads into one total order.
 *
 * <p>Writers run on real threads, so the real Iceberg code runs unmodified, with its real locks and
 * its real {@code ThreadLocal}s. Determinism comes from a baton: every call a writer makes across
 * the fake boundary ({@link FakeFileIO}, {@link FakeTableOperations}) first parks the thread in
 * {@link #point}. The scheduler, running on the test thread, waits until <em>every</em> live fiber
 * is parked, then picks one with the seeded RNG and releases it. Exactly one fiber runs at a time,
 * and the sequence of picks is a function of the seed.
 *
 * <p>Fault choices are drawn from the same RNG through {@link #draw}. A fiber draws only while it
 * is the one running fiber (the scheduler is blocked in {@code wait()} meanwhile), so the draw
 * order is fixed by the schedule.
 *
 * <p>The unit of interleaving is one fake-boundary call. A race between two lines inside a single
 * Iceberg method is not explored; that is the job of a model checker or jcstress, and the Promela
 * and TLA+ models in this project cover the protocol level.
 *
 * <p>Threads the harness did not spawn are counted in {@link #unmanagedCrossings()} rather than
 * parked, and the self-check asserts that count is zero. That is the escape-point report: if a
 * future Iceberg release starts a pool the harness does not inject, the number goes up and says so
 * instead of quietly losing determinism.
 */
public final class Scheduler {

  /** How long to wait for all fibers to park before calling the run hung. */
  private static final long HANG_WAIT_MS = 10_000;

  private static final class Fiber {
    final String name;
    volatile boolean alive = true;
    boolean parked;
    boolean released;
    String label = "spawn";
    Throwable failure;
    Thread thread;

    Fiber(String name) {
      this.name = name;
    }
  }

  private final Object lock = new Object();
  private final Random rng;
  private final Trace trace;
  private final long seed;
  private final long stepBudget;
  private final List<Fiber> fibers = new ArrayList<>();
  private final ThreadLocal<Fiber> self = new ThreadLocal<>();

  private volatile boolean armed;
  private long step;
  private long progress;
  private int unmanagedCrossings;
  private String firstUnmanagedStack;
  private final java.util.Set<String> unmanagedLabels = new java.util.LinkedHashSet<>();
  private int unmanagedDraws;
  private boolean hung;

  public Scheduler(long seed, Trace trace, long stepBudget) {
    this.seed = seed;
    this.rng = new Random(seed);
    this.trace = trace;
    this.stepBudget = stepBudget;
  }

  public long seed() {
    return seed;
  }

  public Trace trace() {
    return trace;
  }

  public boolean hung() {
    return hung;
  }

  public long steps() {
    return step;
  }

  public int unmanagedCrossings() {
    return unmanagedCrossings;
  }

  /** Which boundary labels an unmanaged thread crossed. */
  public java.util.Set<String> unmanagedLabels() {
    synchronized (lock) {
      return new java.util.LinkedHashSet<>(unmanagedLabels);
    }
  }

  /** Where the first unmanaged boundary crossing came from, or null if there were none. */
  public String firstUnmanagedStack() {
    synchronized (lock) {
      return firstUnmanagedStack;
    }
  }

  public int unmanagedDraws() {
    return unmanagedDraws;
  }

  /** Starts a writer. The fiber parks at once and runs nothing until the scheduler picks it. */
  public void spawn(String name, Runnable body) {
    Fiber fiber = new Fiber(name);
    synchronized (lock) {
      fibers.add(fiber);
    }
    Thread thread =
        new Thread(
            () -> {
              self.set(fiber);
              try {
                point("spawn");
                body.run();
              } catch (Throwable t) {
                fiber.failure = t;
              } finally {
                synchronized (lock) {
                  fiber.alive = false;
                  fiber.parked = false;
                  progress++;
                  lock.notifyAll();
                }
              }
            },
            "dst-" + name);
    thread.setDaemon(true);
    fiber.thread = thread;
    thread.start();
  }

  /**
   * A fake boundary. Parks the calling fiber until the scheduler releases it, and records the
   * release in the trace.
   */
  /**
   * Whether the scheduler is controlling the run. Setup work (creating the table, seeding it with
   * files) runs before the writers start and must neither park nor draw faults, or the seed would
   * buy a different schedule depending on how the table was built.
   */
  public boolean armed() {
    return armed;
  }

  public void arm() {
    this.armed = true;
  }

  public void disarm() {
    this.armed = false;
  }

  public String currentName() {
    Fiber me = self.get();
    return me == null ? "unmanaged" : me.name;
  }

  public void point(String label) {
    if (!armed) {
      return;
    }
    Fiber me = self.get();
    if (me == null) {
      synchronized (lock) {
        unmanagedCrossings++;
        unmanagedLabels.add(label);
        if (firstUnmanagedStack == null) {
          // Name the call site. An escape count on its own says the harness lost control but not
          // where, and the whole point of the counter is to be actionable.
          StringBuilder sb = new StringBuilder(label).append(" from ")
              .append(Thread.currentThread().getName()).append('\n');
          StackTraceElement[] frames = Thread.currentThread().getStackTrace();
          for (int i = 0; i < Math.min(frames.length, 18); i++) {
            sb.append("      ").append(frames[i]).append('\n');
          }
          firstUnmanagedStack = sb.toString();
        }
      }
      return;
    }
    synchronized (lock) {
      me.parked = true;
      me.label = label;
      progress++;
      lock.notifyAll();
      while (!me.released) {
        try {
          lock.wait();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException("fiber " + me.name + " interrupted while parked", e);
        }
      }
      me.released = false;
    }
  }

  /**
   * Draws a fault choice. Callable only by the fiber the scheduler has released; an unmanaged
   * thread gets 0 and is counted, so it cannot perturb the sequence.
   */
  public int draw(String what, int bound) {
    Fiber me = self.get();
    synchronized (lock) {
      if (me == null) {
        // A thread from a pool the harness could not inject. It runs while the fiber that
        // submitted its task is blocked in Future.get(), so nothing else is running and the draw
        // is still in schedule order. Returning a constant here instead would push every such
        // write into the first fault bucket, which would be a lie about the fault distribution.
        unmanagedDraws++;
      }
      int value = rng.nextInt(bound);
      trace.line(
          "      draw " + (me == null ? "(unmanaged) " : "") + what + " -> " + value + "/" + bound);
      return value;
    }
  }

  /** Runs the schedule until every fiber has finished, or the run hangs or runs away. */
  public void runToCompletion() {
    synchronized (lock) {
      while (true) {
        List<Fiber> ready = new ArrayList<>();
        int aliveCount = 0;
        for (Fiber f : fibers) {
          if (f.alive) {
            aliveCount++;
            if (f.parked) {
              ready.add(f);
            }
          }
        }
        if (aliveCount == 0) {
          return;
        }
        if (ready.size() < aliveCount) {
          long before = progress;
          try {
            lock.wait(HANG_WAIT_MS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("scheduler interrupted", e);
          }
          if (progress == before) {
            // Nothing moved for HANG_WAIT_MS while a fiber was neither parked nor finished:
            // the run is stuck outside the harness's control. That is itself a finding.
            hung = true;
            trace.line("HUNG after " + step + " steps");
            return;
          }
          continue;
        }
        if (step >= stepBudget) {
          hung = true;
          trace.line("STEP BUDGET " + stepBudget + " exhausted");
          return;
        }
        step++;
        Fiber pick = ready.get(rng.nextInt(ready.size()));
        trace.line(pad(step) + " " + pick.name + " " + pick.label);
        pick.parked = false;
        pick.released = true;
        progress++;
        lock.notifyAll();
      }
    }
  }

  /** The throwable a fiber died with, if any (the writers record their own outcomes). */
  public List<Throwable> fiberFailures() {
    List<Throwable> out = new ArrayList<>();
    synchronized (lock) {
      for (Fiber f : fibers) {
        if (f.failure != null) {
          out.add(f.failure);
        }
      }
    }
    return out;
  }

  /** Names of fibers still alive, for a hang report. */
  public List<String> stuckFibers() {
    List<String> out = new ArrayList<>();
    synchronized (lock) {
      for (Fiber f : fibers) {
        if (f.alive) {
          out.add(f.name + "@" + f.label + (f.parked ? " (parked)" : " (running)"));
        }
      }
    }
    return out;
  }

  private static String pad(long n) {
    String s = Long.toString(n);
    return "00000".substring(Math.min(5, s.length())) + s;
  }
}
