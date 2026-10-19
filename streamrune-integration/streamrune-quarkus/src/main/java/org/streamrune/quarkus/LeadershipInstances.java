package org.streamrune.quarkus;

import jakarta.enterprise.inject.Instance;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.integration.SubscriptionLeadershipResolution;

/**
 * Resolves the one {@link SubscriptionLeadership} bean every framework runner shares. Every Quarkus
 * site that reads the leadership goes through {@link #resolve}, so an ambiguous leadership is
 * refused everywhere — not refused by one runner and read as absent by another.
 */
final class LeadershipInstances {

  private LeadershipInstances() {}

  /**
   * The resolvable leadership bean, or {@code null} when there is none.
   *
   * @throws IllegalStateException with two or more leadership beans and none the default; the
   *     message names their bean classes (see {@link SubscriptionLeadershipResolution#ambiguous})
   */
  static SubscriptionLeadership resolve(Instance<SubscriptionLeadership> instance) {
    if (instance.isUnsatisfied()) {
      return null;
    }
    if (instance.isAmbiguous()) {
      throw SubscriptionLeadershipResolution.ambiguous(
          instance.handlesStream().map(h -> h.getBean().getBeanClass().getName()).toList(),
          "@Alternative with a @Priority (or give the others a qualifier)");
    }
    return instance.get();
  }
}
