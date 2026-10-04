package org.streamrune.core;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Exponential backoff retry policy for optimistic lock conflicts.
 *
 * @param maxAttempts maximum number of attempts (>= 1)
 * @param initialDelay delay before the first retry (required, must not be negative)
 * @param backoffMultiplier multiplier applied to the delay on each subsequent retry (>= 1.0)
 * @param jitterEnabled whether to add random jitter to prevent thundering herd (default: true)
 */
public record RetryPolicy(
    int maxAttempts, Duration initialDelay, double backoffMultiplier, boolean jitterEnabled) {

  public static final RetryPolicy DEFAULT = new RetryPolicy(3, Duration.ofMillis(50), 2.0, true);

  public RetryPolicy(int maxAttempts, Duration initialDelay, double backoffMultiplier) {
    this(maxAttempts, initialDelay, backoffMultiplier, true);
  }

  public RetryPolicy {
    if (maxAttempts < 1) {
      throw new IllegalArgumentException("maxAttempts must be >= 1");
    }
    if (initialDelay == null) {
      throw new IllegalArgumentException("initialDelay is required");
    }
    if (initialDelay.isNegative()) {
      throw new IllegalArgumentException("initialDelay must not be negative: " + initialDelay);
    }
    if (backoffMultiplier < 1.0) {
      throw new IllegalArgumentException("backoffMultiplier must be >= 1.0");
    }
  }

  /**
   * Computes the delay for the given attempt number (1-based). Attempt 1 returns {@code
   * initialDelay}, each subsequent attempt multiplies by {@code backoffMultiplier}.
   *
   * <p>When jitter is enabled (default), the base delay is multiplied by a random factor uniform in
   * {@code [0.5, 1.5)} ("equal jitter") to spread out concurrent retries.
   *
   * <p>The delay grows without an explicit cap — bound it by choosing {@code maxAttempts} and
   * {@code backoffMultiplier} accordingly. Pathological configurations saturate at {@code
   * Long.MAX_VALUE} nanoseconds instead of overflowing.
   */
  public Duration delayForAttempt(int attempt) {
    return backoffDelay(initialDelay, backoffMultiplier, jitterEnabled, attempt);
  }

  /**
   * Shared exponential-backoff computation for {@link RetryPolicy} and {@link
   * DeadLetterRetryPolicy}. The base delay is {@code initialDelay * backoffMultiplier^(attempt-1)},
   * computed in nanosecond precision so sub-millisecond initial delays are not truncated to zero.
   * When {@code jitterEnabled}, the result is multiplied by a random factor uniform in {@code [0.5,
   * 1.5)} ("equal jitter" — bounded around the base delay, unlike AWS-style decorrelated jitter).
   * The result saturates at {@code Long.MAX_VALUE} nanoseconds instead of overflowing.
   */
  static Duration backoffDelay(
      Duration initialDelay, double backoffMultiplier, boolean jitterEnabled, int attempt) {
    if (attempt < 1) {
      throw new IllegalArgumentException("attempt must be >= 1");
    }
    // Compute on doubles: exact for realistic delays, never throws on extreme ones.
    double initialNanos = initialDelay.getSeconds() * 1_000_000_000.0 + initialDelay.getNano();
    double delayNanos = initialNanos * Math.pow(backoffMultiplier, attempt - 1.0);
    if (jitterEnabled) {
      delayNanos *= 0.5 + ThreadLocalRandom.current().nextDouble();
    }
    // Math.round saturates at Long.MAX_VALUE for infinite/oversized doubles.
    return Duration.ofNanos(Math.round(delayNanos));
  }
}
