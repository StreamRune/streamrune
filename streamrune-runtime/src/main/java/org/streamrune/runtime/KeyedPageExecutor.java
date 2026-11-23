package org.streamrune.runtime;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.tracing.StreamRuneTracing;

/**
 * Runs a page of items in parallel across keys and in list order within a key.
 *
 * <p>Items with equal keys run one after the other, in list order, and never at the same time.
 * Items of different keys run concurrently, at most {@code parallelism} steps at a time. A page of
 * one key, or a parallelism of one, runs on the calling thread and ends at its first failure.
 * Otherwise virtual worker threads take the keys in the order of their first item; each worker runs
 * under the request context and dispatch cause of the calling thread, and under its trace context
 * as far as the executor's tracing carries it ({@link StreamRuneTracing#propagating}; {@link
 * StreamRuneTracing#NOOP} carries none).
 *
 * <p>A failed item ends its key; the {@link PageStopRule} decides whether it also ends the page. An
 * {@link Error} always does. {@link #run} never throws a step's failure: it is in the {@link
 * PageResult}.
 */
final class KeyedPageExecutor {

  static final int MAX_SUPPRESSED = 8;

  /**
   * The position of a failure that belongs to no item: the interrupt of the waiting caller, or a
   * worker whose task never returned.
   */
  private static final int NO_ITEM_POSITION = -1;

  /** The position of the interrupt of the waiting caller. */
  private static final int INTERRUPTED_POSITION = NO_ITEM_POSITION;

  /** Creates the worker threads of a page; a seam for a test that makes a start fail. */
  @FunctionalInterface
  interface WorkerThreads {
    /** The n-th worker of a page, unstarted; n counts from 0 in every page. */
    Thread unstarted(int n, Runnable task);
  }

  /**
   * Told of every failure a page records, before it is recorded; a seam for a test that makes the
   * recording itself fail, as an out-of-memory error would.
   */
  @FunctionalInterface
  interface FailureRecording {
    /** Called with each failure the page is about to record. */
    void recording(int position, Throwable failure);

    /** Records every failure. */
    FailureRecording NONE = (position, failure) -> {};
  }

  private final WorkerThreads threads;
  private final StreamRuneTracing tracing;
  private final FailureRecording recording;

  KeyedPageExecutor(String threadNamePrefix, StreamRuneTracing tracing) {
    this(named(Objects.requireNonNull(threadNamePrefix, "threadNamePrefix is required")), tracing);
  }

  KeyedPageExecutor(WorkerThreads threads, StreamRuneTracing tracing) {
    this(threads, tracing, FailureRecording.NONE);
  }

  KeyedPageExecutor(WorkerThreads threads, StreamRuneTracing tracing, FailureRecording recording) {
    this.threads = Objects.requireNonNull(threads, "threads is required");
    this.tracing = Objects.requireNonNull(tracing, "tracing is required");
    this.recording = Objects.requireNonNull(recording, "recording is required");
  }

  private static WorkerThreads named(String prefix) {
    return (n, task) -> Thread.ofVirtual().name(prefix + n).unstarted(task);
  }

  /**
   * Runs the page and returns when every started item has ended. A worker thread that cannot be
   * started is thrown, after the workers that did start have ended.
   *
   * @param items the items, in the order that decides the order within a key and the lowest
   *     position
   * @param keyOf the key of an item, compared by {@code equals}
   * @param parallelism the most steps in flight at once (at least 1)
   * @param step the work of one item
   * @param stopRule decides whether a failure stops the page; not asked on the inline paths
   * @param observer told when a step starts and ends
   * @param <K> the key type
   * @param <T> the item type
   * @return what the page came to; a step's failure is in it, never thrown
   */
  <K, T> PageResult run(
      List<T> items,
      Function<T, K> keyOf,
      int parallelism,
      Consumer<T> step,
      PageStopRule stopRule,
      PageObserver observer) {
    Objects.requireNonNull(items, "items is required");
    Objects.requireNonNull(keyOf, "keyOf is required");
    Objects.requireNonNull(step, "step is required");
    Objects.requireNonNull(stopRule, "stopRule is required");
    Objects.requireNonNull(observer, "observer is required");
    if (parallelism < 1) {
      throw new IllegalArgumentException("parallelism must be at least 1: " + parallelism);
    }
    long start = System.nanoTime();
    Map<K, List<Integer>> groups = new LinkedHashMap<>();
    for (int i = 0; i < items.size(); i++) {
      groups.computeIfAbsent(keyOf.apply(items.get(i)), k -> new ArrayList<>()).add(i);
    }
    Run<T> run = new Run<>(items, step, stopRule, observer, recording);
    if (items.isEmpty()) {
      return run.result(0, false, start);
    }
    List<List<Integer>> groupList = List.copyOf(groups.values());
    if (parallelism == 1 || groupList.size() <= 1) {
      run.inline();
      return run.result(groupList.size(), false, start);
    }
    int workers = Math.min(parallelism, groupList.size());
    AtomicInteger nextGroup = new AtomicInteger();
    Runnable worker = () -> run.work(groupList, nextGroup);
    List<Thread> created = new ArrayList<>(workers);
    for (int n = 0; n < workers; n++) {
      Runnable task = tracing.propagating(StreamRuneContext.wrap(worker));
      created.add(threads.unstarted(n, task));
    }
    List<Thread> started = new ArrayList<>(workers);
    try {
      for (Thread thread : created) {
        thread.start();
        started.add(thread);
      }
    } catch (Throwable t) {
      // A worker that could not start: the ones that did must end before run() reports anything.
      run.stopPage();
      if (join(started, run)) {
        Thread.currentThread().interrupt();
      }
      throw t;
    }
    boolean interrupted = join(started, run);
    int returned = run.workersReturned.get();
    if (returned < started.size()) {
      // A worker thread ended without its task returning normally: the task died while it
      // recorded a failure (an out-of-memory error), or a wrapper of the tracing or the context
      // dropped the task, or handed it to a thread that still runs. Its items did not run, or are
      // not done, or failed unrecorded: the page must fail, or they would pass for done. A
      // handed-off task that runs later starts no item of this page.
      run.stopPage();
      run.record(
          NO_ITEM_POSITION,
          new IllegalStateException(
              (started.size() - returned)
                  + " of "
                  + started.size()
                  + " worker(s) of the page ended without running their items to the end: a"
                  + " worker died while it recorded a failure, or the task a worker thread runs"
                  + " was dropped or handed off (a StreamRuneTracing whose propagating(Runnable)"
                  + " wrapper does not run its task)"));
    }
    if (interrupted) {
      run.record(
          INTERRUPTED_POSITION,
          new IllegalStateException("Interrupted while a page of saga steps was in flight"));
      Thread.currentThread().interrupt();
    }
    return run.result(groupList.size(), interrupted, start);
  }

  /** Waits for every thread, however often the caller is interrupted; returns whether it was. */
  private static boolean join(List<Thread> threads, Run<?> run) {
    boolean interrupted = false;
    for (Thread thread : threads) {
      while (true) {
        try {
          thread.join();
          break;
        } catch (InterruptedException e) {
          interrupted = true;
          run.stopPage();
        }
      }
    }
    return interrupted;
  }

  private record Failed(int position, Throwable failure) {}

  /** The state of one page. */
  private static final class Run<T> {
    private final List<T> items;
    private final Consumer<T> step;
    private final PageStopRule stopRule;
    private final PageObserver observer;
    private final FailureRecording recording;
    private final List<Failed> failures = new ArrayList<>();
    private final AtomicLong stepNanos = new AtomicLong();

    /**
     * The workers whose task returned normally; a worker whose task ended with a throwable is not
     * counted.
     */
    final AtomicInteger workersReturned = new AtomicInteger();

    private final Object counts = new Object();
    private int inFlight;
    private int started;
    private Throwable stoppedBy;
    private volatile boolean stopped;

    Run(
        List<T> items,
        Consumer<T> step,
        PageStopRule stopRule,
        PageObserver observer,
        FailureRecording recording) {
      this.items = items;
      this.step = step;
      this.stopRule = stopRule;
      this.observer = observer;
      this.recording = recording;
    }

    void stopPage() {
      stopped = true;
    }

    /** Runs the items in list order on the calling thread, ending at the first failure. */
    void inline() {
      int at = INTERRUPTED_POSITION;
      try {
        for (int i = 0; i < items.size(); i++) {
          at = i;
          if (!execute(i, false)) {
            return;
          }
        }
      } catch (Throwable t) {
        // not a step: the observer failed
        record(at, t);
      }
    }

    void work(List<List<Integer>> groups, AtomicInteger nextGroup) {
      workGroups(groups, nextGroup);
      // Counted on a normal return only: a worker that died — its recording of a failure threw —
      // left a failure unrecorded, and must fail the page as a worker that never ran does.
      workersReturned.incrementAndGet();
    }

    private void workGroups(List<List<Integer>> groups, AtomicInteger nextGroup) {
      int at = INTERRUPTED_POSITION;
      try {
        while (!stopped) {
          int g = nextGroup.getAndIncrement();
          if (g >= groups.size()) {
            return;
          }
          for (int position : groups.get(g)) {
            if (stopped) {
              break;
            }
            at = position;
            if (!execute(position, true)) {
              break;
            }
          }
        }
      } catch (Throwable t) {
        // Not a step: the observer or the stop rule failed, and this worker ends here. The page
        // must fail, or its items that never ran would pass for done.
        record(at, t);
        stopOn(t);
      }
    }

    private void stopOn(Throwable t) {
      synchronized (counts) {
        if (stoppedBy == null) {
          stoppedBy = t;
        }
      }
      stopped = true;
    }

    /** Runs one item; returns whether it succeeded. The inline paths do not ask the rule. */
    private boolean execute(int position, boolean asksRule) {
      starts();
      long begin = System.nanoTime();
      try {
        step.accept(items.get(position));
        return true;
      } catch (Throwable t) {
        int n = record(position, t);
        boolean stops;
        try {
          stops = asksRule && (t instanceof Error || stopRule.stopsPage(t, n));
        } catch (Throwable ruleFailed) {
          // the rule failed: the page fails and stops here, before the end of the step is
          // reported, as for a rule that says stop
          record(position, ruleFailed);
          stopOn(ruleFailed);
          return false;
        }
        if (stops) {
          stopOn(t);
        }
        return false;
      } finally {
        stepNanos.addAndGet(System.nanoTime() - begin);
        ends();
      }
    }

    private void starts() {
      synchronized (counts) {
        inFlight++;
        started++;
        try {
          observer.stepsChanged(inFlight, items.size() - started);
        } catch (Throwable t) {
          // the step never runs: it is neither in flight nor started
          inFlight--;
          started--;
          throw t;
        }
      }
    }

    private void ends() {
      synchronized (counts) {
        inFlight--;
        observer.stepsChanged(inFlight, items.size() - started);
      }
    }

    int record(int position, Throwable failure) {
      recording.recording(position, failure);
      synchronized (failures) {
        failures.add(new Failed(position, failure));
        return failures.size();
      }
    }

    PageResult result(int groups, boolean interrupted, long start) {
      List<Failed> failed;
      synchronized (failures) {
        failed = new ArrayList<>(failures);
      }
      Throwable winner = null;
      int winnerPosition = INTERRUPTED_POSITION;
      if (!failed.isEmpty()) {
        Comparator<Failed> order =
            Comparator.comparing((Failed f) -> !(f.failure() instanceof Error))
                .thenComparingInt(Failed::position);
        failed.sort(order);
        winner = failed.getFirst().failure();
        winnerPosition = failed.getFirst().position();
        for (int i = 1; i < failed.size() && i <= MAX_SUPPRESSED; i++) {
          if (failed.get(i).failure() != winner) {
            winner.addSuppressed(failed.get(i).failure());
          }
        }
      }
      int notStarted;
      Throwable stopper;
      synchronized (counts) {
        notStarted = items.size() - started;
        stopper = stoppedBy;
      }
      return new PageResult(
          items.size(),
          groups,
          (int)
              failed.stream()
                  .map(Failed::position)
                  .filter(p -> p != INTERRUPTED_POSITION)
                  .distinct()
                  .count(),
          notStarted,
          winner,
          winnerPosition,
          stopper,
          interrupted,
          stepNanos.get(),
          System.nanoTime() - start);
    }
  }
}
