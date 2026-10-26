package org.streamrune.runtime;

import org.streamrune.core.subscription.LeaseRole;
import org.streamrune.core.subscription.SubscriptionLeadership;

/**
 * The role a projection runner plays under its leadership, as its health entry reports it: the
 * runner takes the lease of its projection's name itself, so neither the runner's own view nor the
 * live subscription it builds holds a lease of its own to report.
 */
final class RunnerLeaseRole {

  private RunnerLeaseRole() {}

  /**
   * {@link LeaseRole#UNGATED} over {@link SubscriptionLeadership#NOOP} (the runner takes no lease
   * and delivers on every replica), {@link LeaseRole#LEADING} while this process holds the lease of
   * {@code name}, {@link LeaseRole#STANDBY} otherwise. A local read of the leadership: no
   * statement.
   */
  static LeaseRole of(SubscriptionLeadership leadership, String name) {
    if (leadership == SubscriptionLeadership.NOOP) {
      return LeaseRole.UNGATED;
    }
    return leadership.current(name).isPresent() ? LeaseRole.LEADING : LeaseRole.STANDBY;
  }
}
