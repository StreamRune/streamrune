package org.streamrune.core.subscription;

import java.util.Objects;

/**
 * Health status of a single subscription, including lifecycle state, lag, and error count.
 *
 * @param name the subscription name
 * @param state current lifecycle state
 * @param lag number of events behind the global head
 * @param errorCount number of consecutive delivery errors
 * @param status computed health status
 * @param role the subscription's role under its lease ({@link LeaseRole#NOT_REPORTED} when it
 *     reports none)
 */
public record SubscriptionHealth(
    String name,
    SubscriptionLifecycleState state,
    long lag,
    int errorCount,
    Status status,
    LeaseRole role) {

  /** Subscription health status. */
  public enum Status {
    UP,
    DOWN,
    DEGRADED
  }

  public SubscriptionHealth {
    Objects.requireNonNull(name, "name");
    Objects.requireNonNull(state, "state");
    Objects.requireNonNull(status, "status");
    Objects.requireNonNull(role, "role");
    if (lag < 0) throw new IllegalArgumentException("lag must be >= 0");
    if (errorCount < 0) throw new IllegalArgumentException("errorCount must be >= 0");
  }
}
