package org.streamrune.core.subscription;

import java.time.Duration;

/**
 * Configuration for an event subscription's delivery mechanism.
 *
 * @param listenNotifyEnabled whether to use push-based notification (e.g. LISTEN/NOTIFY) for
 *     low-latency delivery
 * @param pollingInterval interval between catch-up polling cycles (must be positive)
 * @param pollingJitter random jitter added to polling interval to avoid thundering herd (must not
 *     be negative; zero disables jitter)
 */
public record SubscriptionConfig(
    boolean listenNotifyEnabled, Duration pollingInterval, Duration pollingJitter) {

  public SubscriptionConfig {
    if (pollingInterval == null) throw new IllegalArgumentException("pollingInterval is required");
    if (pollingInterval.isZero() || pollingInterval.isNegative()) {
      throw new IllegalArgumentException("pollingInterval must be positive: " + pollingInterval);
    }
    if (pollingJitter == null) throw new IllegalArgumentException("pollingJitter is required");
    if (pollingJitter.isNegative()) {
      throw new IllegalArgumentException("pollingJitter must not be negative: " + pollingJitter);
    }
  }

  public static final SubscriptionConfig DEFAULT =
      new SubscriptionConfig(true, Duration.ofSeconds(5), Duration.ofSeconds(1));

  /**
   * Creates a config with LISTEN/NOTIFY disabled — pure polling mode. Jitter is one fifth of the
   * interval, matching the {@link #DEFAULT} ratio.
   *
   * @param interval polling interval (must be positive)
   * @return polling-only configuration
   */
  public static SubscriptionConfig pollingOnly(Duration interval) {
    if (interval == null) throw new IllegalArgumentException("pollingInterval is required");
    return new SubscriptionConfig(false, interval, interval.dividedBy(5));
  }
}
