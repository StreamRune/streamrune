package org.streamrune.core;

import java.time.Duration;

/**
 * Configurable retry policy for dead letter queue entries. Controls how many times a DLQ entry is
 * retried, the backoff between retries, and whether exhausted entries are auto-discarded.
 *
 * @param maxRetries maximum number of DLQ retry attempts (>= 1)
 * @param initialDelay delay before the first DLQ retry (required, must not be negative)
 * @param backoffMultiplier multiplier applied to delay on each subsequent retry (>= 1.0)
 * @param discardAfterMaxRetries whether to auto-discard entries that exhaust all retries
 */
public record DeadLetterRetryPolicy(
    int maxRetries,
    Duration initialDelay,
    double backoffMultiplier,
    boolean discardAfterMaxRetries) {

  public static final DeadLetterRetryPolicy DEFAULT =
      new DeadLetterRetryPolicy(5, Duration.ofSeconds(30), 2.0, false);

  public DeadLetterRetryPolicy {
    if (maxRetries < 1) {
      throw new IllegalArgumentException("maxRetries must be >= 1");
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
   * Computes the delay for a given DLQ retry attempt (1-based). Jitter is always applied: the base
   * delay {@code initialDelay * backoffMultiplier^(attempt-1)} is multiplied by a random factor
   * uniform in {@code [0.5, 1.5)} ("equal jitter"). See {@link RetryPolicy#backoffDelay}.
   */
  public Duration delayForAttempt(int attempt) {
    return RetryPolicy.backoffDelay(initialDelay, backoffMultiplier, true, attempt);
  }
}
