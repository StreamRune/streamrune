package org.streamrune.core.subscription;

import java.time.Duration;

/**
 * Configuration for an event subscription's delivery mechanism.
 *
 * <p>A subscription sleeps whole milliseconds between two polls, so a value below one millisecond
 * would be slept as no pause at all: the interval must be at least {@code 1ms}, and the jitter zero
 * or at least {@code 1ms}. A shorter value is refused, as everywhere else in StreamRune where a
 * duration is slept in milliseconds.
 *
 * @param listenNotifyEnabled whether to use push-based notification (e.g. LISTEN/NOTIFY) for
 *     low-latency delivery
 * @param pollingInterval interval between catch-up polling cycles (at least {@code 1ms})
 * @param pollingJitter random jitter added to polling interval to avoid thundering herd (zero,
 *     which disables jitter, or at least {@code 1ms})
 */
public record SubscriptionConfig(
    boolean listenNotifyEnabled, Duration pollingInterval, Duration pollingJitter) {

  public SubscriptionConfig {
    if (pollingInterval == null) {
      throw new IllegalArgumentException("pollingInterval is required");
    }
    if (pollingInterval.compareTo(ONE_MILLISECOND) < 0) {
      throw new IllegalArgumentException(
          "pollingInterval (streamrune.polling-interval) must be at least 1ms, got: "
              + pollingInterval);
    }
    if (pollingJitter == null) {
      throw new IllegalArgumentException("pollingJitter is required");
    }
    if (pollingJitter.isNegative()) {
      throw new IllegalArgumentException(
          "pollingJitter (streamrune.polling-jitter) must not be negative, got: " + pollingJitter);
    }
    if (!pollingJitter.isZero() && pollingJitter.compareTo(ONE_MILLISECOND) < 0) {
      throw new IllegalArgumentException(
          "pollingJitter (streamrune.polling-jitter) must be zero or at least 1ms, got: "
              + pollingJitter);
    }
  }

  private static final Duration ONE_MILLISECOND = Duration.ofMillis(1);

  public static final SubscriptionConfig DEFAULT =
      new SubscriptionConfig(true, Duration.ofSeconds(5), Duration.ofSeconds(1));

  /**
   * Creates a config with LISTEN/NOTIFY disabled — pure polling mode. Jitter is one fifth of the
   * interval, matching the {@link #DEFAULT} ratio, in whole milliseconds (rounded down, so an
   * interval below {@code 5ms} polls without jitter).
   *
   * @param interval polling interval (at least {@code 1ms})
   * @return polling-only configuration
   */
  public static SubscriptionConfig pollingOnly(Duration interval) {
    if (interval == null) {
      throw new IllegalArgumentException("pollingInterval is required");
    }
    return new SubscriptionConfig(
        false, interval, interval.dividedBy(5).truncatedTo(java.time.temporal.ChronoUnit.MILLIS));
  }
}
