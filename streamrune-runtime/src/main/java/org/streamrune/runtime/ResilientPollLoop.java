package org.streamrune.runtime;

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
 * <p>Instances are reusable across {@link #run(PollTask)} invocations (e.g. pause/resume cycles
 * that spawn a fresh thread); the failure counter resets at the start of each run.
 */
final class ResilientPollLoop {

  private static final Logger logger = LoggerFactory.getLogger(ResilientPollLoop.class);

  /** Backoff never sleeps less than this, even if the configured initial backoff is tiny. */
  private static final Duration MIN_INITIAL_BACKOFF = Duration.ofMillis(10);

  /** One poll cycle. Implementations may throw {@link RuntimeException} on transient failures. */
  @FunctionalInterface
  interface PollTask {
    void poll();
  }

  /**
   * Marks a poll-task exception that is an <em>expected control-flow signal</em> — e.g. a standby
   * losing (or never having had) leadership — rather than a genuine failure. The loop logs such a
   * signal at INFO as a one-line message <b>without</b> a stacktrace instead of ERROR-with-trace,
   * so an ordinary failover does not masquerade as an error in the logs. Everything else is
   * unchanged: the signal still counts as a cycle failure and still triggers the same backoff, so
   * the owning runner's own leadership check stops the loop exactly as before — only the log line
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
        // before throwing — see the comment below) — the ERROR branch used to unconditionally
        // say "retrying in N ms" immediately before the loop returned WITHOUT retrying.
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
        // is NOT retryable, and until now it left this loop — and the owning virtual thread —
        // without a single log line naming the loop. An owner without its own liveness accessor
        // (the retention sweepers) then froze silently, every gauge it fed holding its last value.
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
