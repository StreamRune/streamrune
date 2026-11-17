package org.streamrune.core.subscription;

import java.util.Objects;
import org.streamrune.core.types.SubscriptionName;

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
 * @param store the name of the checkpoint target whose checkpoint {@code lag} is measured against —
 *     for a projection, the read-model store it is routed to, or {@code subscription_checkpoint}
 *     for one routed to the event store's subscription checkpoint; {@code null} when the
 *     registration names none (an event subscription, or a projection on its runner's default pair)
 */
public record SubscriptionHealth(
    SubscriptionName name,
    SubscriptionLifecycleState state,
    long lag,
    int errorCount,
    Status status,
    LeaseRole role,
    String store) {

  /** Subscription health status. */
  public enum Status {
    UP,
    DOWN,
    DEGRADED
  }

  /**
   * A health entry that names no checkpoint target ({@code store} is {@code null}).
   *
   * @param name the subscription name
   * @param state current lifecycle state
   * @param lag number of events behind the global head
   * @param errorCount number of consecutive delivery errors
   * @param status computed health status
   * @param role the subscription's role under its lease
   */
  public SubscriptionHealth(
      SubscriptionName name,
      SubscriptionLifecycleState state,
      long lag,
      int errorCount,
      Status status,
      LeaseRole role) {
    this(name, state, lag, errorCount, status, role, null);
  }

  public SubscriptionHealth {
    Objects.requireNonNull(name, "name");
    Objects.requireNonNull(state, "state");
    Objects.requireNonNull(status, "status");
    Objects.requireNonNull(role, "role");
    if (lag < 0) {
      throw new IllegalArgumentException("lag must be >= 0");
    }
    if (errorCount < 0) {
      throw new IllegalArgumentException("errorCount must be >= 0");
    }
  }
}
