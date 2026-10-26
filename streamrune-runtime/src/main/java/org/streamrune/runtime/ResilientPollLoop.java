package org.streamrune.runtime;

import java.io.InterruptedIOException;
import java.nio.channels.ClosedByInterruptException;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Drives a background poll loop that survives transient failures instead of dying silently.
 *
 * <p>Runs the supplied poll task repeatedly on the calling thread while the {@code keepRunning}
 * condition holds. After a successful poll the loop sleeps for the configured idle delay; after a
 * failed poll (any {@link RuntimeException}) the failure is logged and the loop sleeps with capped
 * exponential backoff plus jitter before retrying. The loop exits only when the condition becomes
 * false or the thread is interrupted — a transient event-store or listener error never terminates
 * it.
 *
 * <p>The consecutive-failure counter is exposed via {@link #consecutiveFailures()} so owners can
 * report a degraded (but still running) status while the loop is in backoff-retry.
 *
 * <p>An {@link Error} is different: it is not retryable, so the loop logs it by name at ERROR and
 * rethrows — the loop stops, and the owner decides what a dead thread means.
 *
 * <p>A failure that ends the loop because its owner stopped it is no failure when the stop's own
 * interrupt caused it — the thread is interrupted, or the failure's cause chain holds an
 * interruption: a read in flight that the stop cut short. The loop logs it at INFO, on one line
 * without a stack trace. A failure after which the loop stops by itself (a task that cleared the
 * loop's condition before it threw, such as a terminal read poison) stays an ERROR.
 *
 * <p>Instances are reusable across {@link #run(PollTask)} invocations (e.g. pause/resume cycles
 * that spawn a fresh thread); the failure counter resets at the start of each run.
 */
final class ResilientPollLoop {

  private static final Logger logger = LoggerFactory.getLogger(ResilientPollLoop.class);

  /** Backoff never sleeps less than this, even if the configured initial backoff is tiny. */
  private static final Duration MIN_INITIAL_BACKOFF = Duration.ofMillis(10);

  /** How many links of a cause chain the stop check and the log summary follow. */
  private static final int MAX_CAUSE_DEPTH = 8;

  /** One poll cycle. Implementations may throw {@link RuntimeException} on transient failures. */
  @FunctionalInterface
  interface PollTask {
    void poll();
  }

  /**
   * Marks a poll-task exception that is an <em>expected control-flow signal</em> — e.g. a standby
   * losing (or never having had) leadership — rather than a genuine failure. The loop logs such a
   * signal at INFO as a one-line message <b>without</b> a stacktrace instead of ERROR-with-trace,
   * so an ordinary failover does not masquerade as an error in the logs. Everything else is the
   * same as for any other failure: the signal counts as a cycle failure and triggers the same
   * backoff, and the owning runner's own leadership check stops the loop — only the log line
   * differs.
   */
  interface BenignSignal {}

  private final String name;
  private final BooleanSupplier keepRunning;
  private final Supplier<Duration> idleDelay;
  private final Duration initialBackoff;
  private final Duration maxBackoff;
  private final AtomicInteger consecutiveFailures = new AtomicInteger(0);

  /**
   * Creates a resilient poll loop.
   *
   * @param name loop name used in log messages (required, non-blank)
   * @param keepRunning loop condition, checked before every cycle (required)
   * @param idleDelay sleep between successful polls, evaluated each cycle (required)
   * @param initialBackoff backoff after the first failure; doubles per consecutive failure
   *     (required; floored at 10 ms)
   * @param maxBackoff upper bound for the backoff delay (required; must be >= initialBackoff)
   */
  ResilientPollLoop(
      String name,
      BooleanSupplier keepRunning,
      Supplier<Duration> idleDelay,
      Duration initialBackoff,
      Duration maxBackoff) {
    if (name == null || name.isBlank()) {
      throw new IllegalArgumentException("name is required");
    }
    if (keepRunning == null) {
      throw new IllegalArgumentException("keepRunning is required");
    }
    if (idleDelay == null) {
      throw new IllegalArgumentException("idleDelay is required");
    }
    if (initialBackoff == null || initialBackoff.isNegative()) {
      throw new IllegalArgumentException("initialBackoff must be non-null and non-negative");
    }
    if (maxBackoff == null || maxBackoff.compareTo(initialBackoff) < 0) {
      throw new IllegalArgumentException("maxBackoff must be non-null and >= initialBackoff");
    }
    this.name = name;
    this.keepRunning = keepRunning;
    this.idleDelay = idleDelay;
    this.initialBackoff =
        initialBackoff.compareTo(MIN_INITIAL_BACKOFF) < 0 ? MIN_INITIAL_BACKOFF : initialBackoff;
    this.maxBackoff = maxBackoff;
  }

  /**
   * Runs the loop on the calling thread until {@code keepRunning} returns {@code false} or the
   * thread is interrupted.
   */
  void run(PollTask task) {
    consecutiveFailures.set(0);
    while (keepRunning.getAsBoolean()) {
      try {
        task.poll();
        if (consecutiveFailures.get() > 0) {
          logger.info(
              "Poll loop '{}' recovered after {} consecutive failure(s)",
              name,
              consecutiveFailures.get());
        }
        consecutiveFailures.set(0);
        Thread.sleep(idleDelay.get());
      } catch (InterruptedException _) {
        Thread.currentThread().interrupt();
        return;
      } catch (RuntimeException e) {
        int failures = consecutiveFailures.incrementAndGet();
        Duration backoff = backoffDelay(failures);
        // Read once, before logging, and reuse below for the retry decision. keepRunning may
        // already be false by the time we get here (e.g. a terminal read-poison signal sets it
        // before throwing — see the comment below) — an ERROR branch that unconditionally
        // says "retrying in N ms" would say it right before the loop returns WITHOUT retrying.
        boolean willRetry = keepRunning.getAsBoolean();
        if (e instanceof BenignSignal) {
          // Expected control-flow signal (e.g. leadership loss), not an error: one-line INFO, no
          // stacktrace. Backoff/counter behavior is deliberately unchanged (see BenignSignal) — the
          // owning runner's leadership check stops this loop shortly.
          logger.info("Poll loop '{}' pausing: {}", name, e.getMessage());
        } else if (willRetry) {
          logger.error(
              "Poll loop '{}' failed ({} consecutive failure(s)); retrying in {} ms",
              name,
              failures,
              backoff.toMillis(),
              e);
        } else if (cutShortByTheStop(e)) {
          // The owner ended the loop — a stop or a pause — and interrupted its thread, and the
          // interrupt cut the call in flight short: on a virtual thread it closes the socket of a
          // JDBC read, the driver throws, and the thread's interrupt status stays set. That is the
          // owner's doing, not a failure.
          logger.info(
              "Poll loop '{}' ended: the owner's interrupt cut a call in flight short ({})",
              name,
              summary(e));
        } else {
          logger.error(
              "Poll loop '{}' failed ({} consecutive failure(s)); stopping, not retrying",
              name,
              failures,
              e);
        }
        // keepRunning may already be false by the time we get here — e.g.
        // StreamEventSubscription.onReadFailure sets running=false (this loop's keepRunning)
        // BEFORE throwing the terminal read-poison exception that lands in this catch. Sleeping
        // the full backoff anyway (up to max(pollingInterval, 60s) at the 5th consecutive failure)
        // left the owning thread alive well past the point a caller's restart guard checks for it
        // — StreamEventSubscription.start() throws IllegalStateException("Previous polling thread
        // is still running") for that entire window, contradicting the documented remedy ("stop
        // the poll loop ... then start() the subscription again"). Skipping the sleep once the
        // loop is already told to stop benefits every consumer, not only the read-poison bound:
        // nothing will retry, so there is no reason to wait out a stale backoff first.
        if (!willRetry) {
          return;
        }
        try {
          Thread.sleep(backoff);
        } catch (InterruptedException _) {
          Thread.currentThread().interrupt();
          return;
        }
      } catch (Throwable t) {
        // A java.lang.Error (AssertionError, NoClassDefFoundError/LinkageError from a
        // missing optional dependency, StackOverflowError on pathological data, OutOfMemoryError)
        // is NOT retryable. Leaving this loop — and the owning virtual thread — without a single
        // log line naming the loop, it would let an owner without its own liveness accessor (the
        // retention sweepers) freeze silently, every gauge it feeds holding its last value.
        // Log the death BY NAME, then RETHROW: the loop stops terminally and the owner decides what
        // a dead thread means — PollingEventSubscription flips STOPPED in its own catch,
        // the retention sweepers report isStarted() && !isAlive() to
        // BackgroundRelayHealthContributor. Deliberately not swallowed: returning normally would
        // hide the death from owners whose liveness is "the thread is alive", and would break
        // the projection loop's catch.
        logger.error(
            "Poll loop '{}' terminated on an unrecoverable error — the owning thread is stopping;"
                + " its health/liveness view now reports it",
            name,
            t);
        throw t;
      }
    }
  }

  /**
   * Whether {@code failure} is a call the owner's stop or pause cut short: the thread is
   * interrupted (the owner's interrupt, which a socket read on a virtual thread leaves set), or the
   * failure's cause chain holds an interruption (a driver that consumed the flag kept its cause).
   */
  private static boolean cutShortByTheStop(RuntimeException failure) {
    if (Thread.currentThread().isInterrupted()) {
      return true;
    }
    Throwable cause = failure;
    for (int depth = 0; cause != null && depth < MAX_CAUSE_DEPTH; depth++) {
      if (cause instanceof InterruptedException
          || cause instanceof InterruptedIOException
          || cause instanceof ClosedByInterruptException) {
        return true;
      }
      cause = cause.getCause() == cause ? null : cause.getCause();
    }
    return false;
  }

  /** The failure and its causes on one line, for a log line without a stack trace. */
  private static String summary(Throwable failure) {
    StringBuilder text = new StringBuilder(failure.toString());
    Throwable cause = failure.getCause();
    for (int depth = 1; cause != null && cause != failure && depth < MAX_CAUSE_DEPTH; depth++) {
      text.append("; caused by ").append(cause);
      cause = cause.getCause() == cause ? null : cause.getCause();
    }
    return text.toString();
  }

  /** Number of consecutive poll failures; {@code 0} when the last poll succeeded. */
  int consecutiveFailures() {
    return consecutiveFailures.get();
  }

  /** Returns {@code true} while the loop is retrying after one or more consecutive failures. */
  boolean isDegraded() {
    return consecutiveFailures.get() > 0;
  }

  /**
   * Computes the backoff delay for the given consecutive-failure count (1-based): {@code initial *
   * 2^(failures-1)}, capped at {@code max}, with jitter spreading the result between 50% and 100%
   * of the computed delay to avoid thundering-herd retries.
   */
  Duration backoffDelay(int failures) {
    return backoffDelay(initialBackoff, maxBackoff, failures);
  }

  /**
   * Static variant of {@link #backoffDelay(int)} for callers whose loop shape does not fit {@link
   * #run(PollTask)} (e.g. cron-fired drains) but who want identical backoff behavior.
   */
  static Duration backoffDelay(Duration initialBackoff, Duration maxBackoff, int failures) {
    if (failures < 1) {
      throw new IllegalArgumentException("failures must be >= 1");
    }
    long initialMs = Math.max(initialBackoff.toMillis(), MIN_INITIAL_BACKOFF.toMillis());
    // Cap the exponent so the shift cannot overflow; the result is capped at maxBackoff anyway.
    long exponential = initialMs << Math.min(failures - 1, 32);
    long capped = Math.min(exponential, maxBackoff.toMillis());
    // Jitter: uniform in [capped/2, capped].
    long half = capped / 2;
    long jittered = half + ThreadLocalRandom.current().nextLong(capped - half + 1);
    return Duration.ofMillis(jittered);
  }
}
