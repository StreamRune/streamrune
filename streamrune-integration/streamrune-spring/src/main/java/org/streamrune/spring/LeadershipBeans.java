package org.streamrune.spring;

import java.util.List;
import org.springframework.beans.factory.ObjectProvider;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.integration.SubscriptionLeadershipResolution;

/**
 * Resolves the one {@link SubscriptionLeadership} bean every framework runner shares. Every Spring
 * site that reads the leadership goes through {@link #resolve}, so none of them can read two beans
 * differently from another.
 */
final class LeadershipBeans {

  private LeadershipBeans() {}

  /**
   * The leadership bean: the only one, or the {@code @Primary} one of several; {@code null} when
   * there is none.
   *
   * @throws IllegalStateException with two or more leadership beans and none {@code @Primary}; the
   *     message names their classes (see {@link SubscriptionLeadershipResolution#ambiguous})
   */
  static SubscriptionLeadership resolve(ObjectProvider<SubscriptionLeadership> provider) {
    SubscriptionLeadership unique = provider.getIfUnique();
    if (unique != null) {
      return unique;
    }
    List<SubscriptionLeadership> candidates = provider.orderedStream().toList();
    if (candidates.isEmpty()) {
      return null;
    }
    throw SubscriptionLeadershipResolution.ambiguous(
        candidates.stream().map(candidate -> candidate.getClass().getName()).toList(), "@Primary");
  }
}
