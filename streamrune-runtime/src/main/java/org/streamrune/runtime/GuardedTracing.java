package org.streamrune.runtime;

import java.lang.ref.WeakReference;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.Internal;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.tracing.SpanAttributes;
import org.streamrune.core.tracing.SpanBaggage;
import org.streamrune.core.tracing.SpanKind;
import org.streamrune.core.tracing.SpanLink;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.tracing.TraceScope;
import org.streamrune.core.tracing.TracingArea;

/**
 * A {@link StreamRuneTracing} that keeps the seam's promise for the tracing it wraps: nothing it or
 * its scopes do can fail, or change the outcome of, the work being traced.
 *
 * <p>A {@link StreamRuneTracing} must not throw, but one can: a hand-written implementation with a
 * defect, or {@link OpenTelemetryTracing} over an OpenTelemetry SDK whose sampler or span processor
 * throws. Every StreamRune component that takes a tracing wraps it with {@link #of}, and {@link
 * OpenTelemetryTracing#of} returns its tracing already wrapped, so the rule holds whichever tracing
 * an application passes:
 *
 * <ul>
 *   <li>{@link #enabled} that throws answers {@code false};
 *   <li>a {@code start} or {@code startChild} that throws, or returns {@code null}, gives {@link
 *       TraceScope#NOOP}: the work runs without the span;
 *   <li>a scope method that throws does nothing; {@link TraceScope#link()} then gives {@link
 *       SpanLink#NONE};
 *   <li>a {@link #propagating} that throws, or returns {@code null}, gives the task itself, and a
 *       wrapped task whose wrapper throws before it runs the task still runs it, once, without the
 *       caller's trace context; what the wrapper throws after the task returned is dropped. What
 *       the task itself throws passes through unchanged.
 * </ul>
 *
 * <p><b>Propagation.</b> The guard assumes what the seam asks of {@code propagating}: the wrapper
 * runs the task on the thread that runs the wrapper. It never runs a task a second time on its own
 * account, so a wrapper that returns normally is taken at its word. A wrapper that returns without
 * ever running the task has dropped it: the guard cannot tell that from a hand-off to another
 * thread, and the {@code executeAsync} future of such a command is never completed, as with an
 * {@code asyncTaskWrapper} that drops its task. A wrapper that hands the task to another thread and
 * then throws is not followed by a second run once that thread has started the task.
 *
 * <p>Whatever is thrown is caught, an {@link Error} included: a span that cannot be recorded is
 * never a reason to fail a command, lose a lock or stall a subscription. An {@link
 * InterruptedException} the tracing lets out is caught too, and the thread's interrupt status is
 * set again. The {@code NullPointerException} of a {@code null} argument the seam's contract
 * rejects is the caller's own and is still thrown.
 *
 * <p>A failure is logged with one WARN, and after that at most once per {@value
 * #LOG_INTERVAL_MINUTES} minutes with the number of failures since, per guard; {@link
 * #failureCount()} counts them all, and a guard made with {@link #of(StreamRuneTracing,
 * StreamRuneMetrics)} counts each in {@link StreamRuneMetrics#recordTracingFailure(String)}, by the
 * operation that failed.
 *
 * <p><b>One guard per tracing.</b> {@link #of} returns {@link StreamRuneTracing#NOOP} and an
 * already guarded tracing as they are, so an untraced component pays nothing for the guard; and for
 * a tracing it has guarded before it returns that same guard, for as long as a component still
 * holds it. The components of an application that share one tracing therefore share one guard: one
 * failure count, one WARN stream and one metrics binding. The guards are remembered by the identity
 * of the tracing, through weak references, so a guard no component holds any more is forgotten.
 *
 * @hidden
 */
@Internal
public final class GuardedTracing implements StreamRuneTracing {

  private static final Logger LOG = LoggerFactory.getLogger(GuardedTracing.class);

  static final long LOG_INTERVAL_MINUTES = 5;

  private static final long LOG_INTERVAL_NANOS = Duration.ofMinutes(LOG_INTERVAL_MINUTES).toNanos();

  /**
   * The invocation of a propagated task running on this thread, so the guard can tell a wrapper
   * that failed before it ran the task from one that failed after.
   */
  private static final ThreadLocal<PropagatedRun> CURRENT_RUN = new ThreadLocal<>();

  /**
   * The guards in use, to find the guard of a tracing by the tracing's identity (its {@code equals}
   * is never called: it is the code being guarded). Read and written under its own lock, when a
   * component is built; a cleared reference is dropped on the next scan.
   */
  private static final List<WeakReference<GuardedTracing>> GUARDS = new ArrayList<>();

  private final StreamRuneTracing delegate;

  /** Where failures are counted; {@link StreamRuneMetrics#NOOP} until an integration binds one. */
  private volatile StreamRuneMetrics metrics = StreamRuneMetrics.NOOP;

  private final AtomicLong failures = new AtomicLong();
  private final AtomicLong unloggedFailures = new AtomicLong();

  /** {@code System.nanoTime()} of the last WARN; meaningful once {@link #failures} is positive. */
  private final AtomicLong lastLogNanos = new AtomicLong();

  private GuardedTracing(StreamRuneTracing delegate) {
    this.delegate = delegate;
  }

  /**
   * {@code tracing} guarded so that it can never fail the work it traces. {@link
   * StreamRuneTracing#NOOP} and a tracing that is already guarded are returned as they are, and a
   * tracing that was guarded before gets the guard it already has.
   *
   * @param tracing the tracing to guard
   * @return the guarded tracing
   * @throws NullPointerException if {@code tracing} is {@code null}
   */
  public static StreamRuneTracing of(StreamRuneTracing tracing) {
    Objects.requireNonNull(tracing, "tracing is required");
    if (tracing == StreamRuneTracing.NOOP || tracing instanceof GuardedTracing) {
      return tracing;
    }
    return guardOf(tracing, null);
  }

  /**
   * {@code tracing} guarded so that it can never fail the work it traces, with each failure counted
   * in {@code metrics} ({@link StreamRuneMetrics#recordTracingFailure(String)}). The integrations
   * call it where they have the application's tracing and its metrics; the guard of the tracing —
   * the one every component of the application holds, whether it was built before this call or is
   * built after it — then counts there. {@link StreamRuneTracing#NOOP} is returned as it is. A
   * guard that already counts in other metrics keeps them, and {@code tracing} gets a second guard
   * for {@code metrics}: two applications in one JVM that share a tracing each count their own.
   *
   * @param tracing the tracing to guard
   * @param metrics where the failures are counted
   * @return the guarded tracing
   * @throws NullPointerException if an argument is {@code null}
   */
  public static StreamRuneTracing of(StreamRuneTracing tracing, StreamRuneMetrics metrics) {
    Objects.requireNonNull(tracing, "tracing is required");
    Objects.requireNonNull(metrics, "metrics is required");
    if (tracing == StreamRuneTracing.NOOP) {
      return tracing;
    }
    if (metrics == StreamRuneMetrics.NOOP) {
      return of(tracing);
    }
    synchronized (GUARDS) {
      if (tracing instanceof GuardedTracing guarded) {
        if (guarded.bind(metrics)) {
          return guarded;
        }
        return guardOf(guarded.delegate, metrics);
      }
      return guardOf(tracing, metrics);
    }
  }

  /**
   * The guard of {@code tracing}: the remembered one that counts in {@code metrics} (or can be
   * bound to them; any remembered one when {@code metrics} is {@code null}), else a new one.
   */
  private static GuardedTracing guardOf(StreamRuneTracing tracing, StreamRuneMetrics metrics) {
    synchronized (GUARDS) {
      GuardedTracing found = null;
      for (Iterator<WeakReference<GuardedTracing>> it = GUARDS.iterator(); it.hasNext(); ) {
        GuardedTracing guard = it.next().get();
        if (guard == null) {
          it.remove();
        } else if (found == null
            && guard.delegate == tracing
            && (metrics == null || guard.bind(metrics))) {
          found = guard;
        }
      }
      if (found == null) {
        found = new GuardedTracing(tracing);
        if (metrics != null) {
          found.metrics = metrics;
        }
        GUARDS.add(new WeakReference<>(found));
      }
      return found;
    }
  }

  /** Whether this guard counts in {@code metrics}: it already does, or it counted nowhere yet. */
  private boolean bind(StreamRuneMetrics metrics) {
    StreamRuneMetrics bound = this.metrics;
    if (bound == metrics) {
      return true;
    }
    if (bound == StreamRuneMetrics.NOOP) {
      this.metrics = metrics;
      return true;
    }
    return false;
  }

  /**
   * How many times the guarded tracing, or one of its scopes, has thrown since this guard was made.
   *
   * @return the number of failures the guard caught
   */
  public long failureCount() {
    return failures.get();
  }

  @Override
  public boolean enabled(TracingArea area) {
    try {
      return delegate.enabled(area);
    } catch (Throwable t) {
      failed("enabled", t);
      return false;
    }
  }

  @Override
  public TraceScope start(
      TracingArea area, String spanName, SpanKind kind, SpanAttributes attributes) {
    try {
      return guarded(delegate.start(area, spanName, kind, attributes));
    } catch (Throwable t) {
      failed("start", t);
      return TraceScope.NOOP;
    }
  }

  @Override
  public TraceScope start(
      TracingArea area,
      String spanName,
      SpanKind kind,
      SpanAttributes attributes,
      Instant startTime) {
    Objects.requireNonNull(startTime, "startTime is required");
    try {
      return guarded(delegate.start(area, spanName, kind, attributes, startTime));
    } catch (Throwable t) {
      failed("start", t);
      return TraceScope.NOOP;
    }
  }

  @Override
  public TraceScope startChild(
      TracingArea area,
      String spanName,
      SpanKind kind,
      SpanAttributes attributes,
      Instant startTime) {
    Objects.requireNonNull(startTime, "startTime is required");
    try {
      return guarded(delegate.startChild(area, spanName, kind, attributes, startTime));
    } catch (Throwable t) {
      failed("startChild", t);
      return TraceScope.NOOP;
    }
  }

  @Override
  public TraceScope start(
      TracingArea area,
      String spanName,
      SpanKind kind,
      SpanAttributes attributes,
      SpanLink link,
      SpanBaggage baggage) {
    Objects.requireNonNull(link, "link is required; SpanLink.NONE for none");
    Objects.requireNonNull(baggage, "baggage is required; SpanBaggage.KEEP to keep it");
    try {
      return guarded(delegate.start(area, spanName, kind, attributes, link, baggage));
    } catch (Throwable t) {
      failed("start", t);
      return TraceScope.NOOP;
    }
  }

  @Override
  public Runnable propagating(Runnable task) {
    Objects.requireNonNull(task, "task is required");
    PropagatedTask tracked = new PropagatedTask(task);
    Runnable wrapped;
    try {
      wrapped = delegate.propagating(tracked);
    } catch (Throwable t) {
      failed("propagating", t);
      return task;
    }
    if (wrapped == null || wrapped == tracked || wrapped == task) {
      return task;
    }
    return () -> runPropagated(tracked, wrapped);
  }

  /**
   * Runs {@code wrapped}, the delegate's wrapper around {@code task}. The task runs exactly once: a
   * wrapper that throws before it reaches the task (the context it captured cannot be made current)
   * is replaced by the task itself, and one that throws after the task returned is dropped. A
   * throwable of the task itself passes through. A wrapper that returns normally is taken at its
   * word, whether the task ran on this thread or not: the guard never runs a task a second time.
   */
  private void runPropagated(PropagatedTask tracked, Runnable wrapped) {
    PropagatedRun outer = CURRENT_RUN.get();
    PropagatedRun run = new PropagatedRun();
    CURRENT_RUN.set(run);
    try {
      wrapped.run();
    } catch (Throwable t) {
      if (run.taskFailure == t) {
        throw t; // the task's own outcome
      }
      failed("propagating", t);
      if (run.taskFailure != null) {
        // The wrapper replaced the task's failure with its own: the task's is the outcome.
        throw sneaky(run.taskFailure);
      }
      if (!run.started && !tracked.startedElsewhere) {
        restore(outer);
        tracked.task.run();
      }
    } finally {
      restore(outer);
    }
  }

  private static void restore(PropagatedRun outer) {
    if (outer == null) {
      CURRENT_RUN.remove();
    } else {
      CURRENT_RUN.set(outer);
    }
  }

  @SuppressWarnings("unchecked")
  private static <T extends Throwable> RuntimeException sneaky(Throwable t) throws T {
    throw (T) t;
  }

  @Override
  public String toString() {
    try {
      return delegate.toString();
    } catch (Throwable t) {
      return "GuardedTracing";
    }
  }

  private TraceScope guarded(TraceScope scope) {
    return scope == null || scope == TraceScope.NOOP ? TraceScope.NOOP : new GuardedScope(scope);
  }

  /**
   * Counts a failure of the guarded tracing and logs it: the first one, then at most one per
   * interval. The log line names the operation and the exception; it never fails the caller.
   */
  private void failed(String operation, Throwable failure) {
    if (failure instanceof InterruptedException) {
      // Thrown past a signature that does not declare it; the JVM cleared the flag with it.
      Thread.currentThread().interrupt();
    }
    try {
      metrics.recordTracingFailure(operation);
    } catch (Throwable ignored) {
      // A metrics backend that throws must not fail the traced work either.
    }
    try {
      long count = failures.incrementAndGet();
      long unlogged = unloggedFailures.incrementAndGet();
      long now = System.nanoTime();
      long last = lastLogNanos.get();
      if (count != 1 && now - last < LOG_INTERVAL_NANOS) {
        return;
      }
      if (!lastLogNanos.compareAndSet(last, now)) {
        return; // another thread logs this interval
      }
      unloggedFailures.addAndGet(-unlogged);
      LOG.warn(
          "Tracing failed in {} ({} failure(s) since the last report); the traced work continues"
              + " without it. A StreamRuneTracing must not throw: check the tracing"
              + " implementation, or the sampler and span processors of the OpenTelemetry SDK."
              + " Further failures are reported at most once per {} minutes.",
          operation,
          unlogged,
          LOG_INTERVAL_MINUTES,
          failure);
    } catch (Throwable ignored) {
      // Reporting a tracing failure must not fail the traced work either.
    }
  }

  /** One invocation of a propagated task. */
  private static final class PropagatedRun {
    private boolean started;
    private Throwable taskFailure;
  }

  /** The task as the delegate's wrapper runs it: it marks the invocation the guard is watching. */
  private static final class PropagatedTask implements Runnable {
    private final Runnable task;

    /**
     * Whether a wrapper ever ran the task on a thread other than the one that ran the wrapper: the
     * guard then never falls back to running the task itself.
     */
    private volatile boolean startedElsewhere;

    PropagatedTask(Runnable task) {
      this.task = task;
    }

    @Override
    public void run() {
      PropagatedRun run = CURRENT_RUN.get();
      if (run == null) {
        // Not the thread that runs the guard's wrapper: the wrapper handed the task off.
        startedElsewhere = true;
        task.run();
        return;
      }
      run.started = true;
      try {
        task.run();
      } catch (Throwable t) {
        run.taskFailure = t;
        throw t;
      }
    }
  }

  /** A scope whose failures are counted and logged, and never reach the traced work. */
  private final class GuardedScope implements TraceScope {
    private final TraceScope scope;

    GuardedScope(TraceScope scope) {
      this.scope = scope;
    }

    @Override
    public void setAttribute(String key, String value) {
      try {
        scope.setAttribute(key, value);
      } catch (Throwable t) {
        failed("setAttribute", t);
      }
    }

    @Override
    public void setAttribute(String key, long value) {
      try {
        scope.setAttribute(key, value);
      } catch (Throwable t) {
        failed("setAttribute", t);
      }
    }

    @Override
    public void setAttribute(String key, boolean value) {
      try {
        scope.setAttribute(key, value);
      } catch (Throwable t) {
        failed("setAttribute", t);
      }
    }

    @Override
    public void recordSuccess() {
      try {
        scope.recordSuccess();
      } catch (Throwable t) {
        failed("recordSuccess", t);
      }
    }

    @Override
    public void recordError(Throwable error) {
      try {
        scope.recordError(error);
      } catch (Throwable t) {
        failed("recordError", t);
      }
    }

    @Override
    public SpanLink link() {
      try {
        SpanLink link = scope.link();
        return link == null ? SpanLink.NONE : link;
      } catch (Throwable t) {
        failed("link", t);
        return SpanLink.NONE;
      }
    }

    @Override
    public void close() {
      try {
        scope.close();
      } catch (Throwable t) {
        failed("close", t);
      }
    }
  }
}
