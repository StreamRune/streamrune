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
          instance.handlesStream().map(h -> describe(h.getBean())).toList(),
          "@Alternative with a @Priority (or give the others a qualifier)");
    }
    return instance.get();
  }

  /**
   * Names a candidate: a class bean by its class; a producer by the class that declares it and the
   * qualifiers that set it apart (the bean class alone is the declaring class, the same for two
   * producers of one configuration class).
   */
  private static String describe(jakarta.enterprise.inject.spi.Bean<?> bean) {
    if (bean instanceof io.quarkus.arc.InjectableBean<?> injectable
        && injectable.getKind() != io.quarkus.arc.InjectableBean.Kind.CLASS) {
      String qualifiers =
          bean.getQualifiers().stream()
              .filter(
                  q ->
                      !(q instanceof jakarta.enterprise.inject.Default)
                          && !(q instanceof jakarta.enterprise.inject.Any))
              .map(Object::toString)
              .sorted()
              .collect(java.util.stream.Collectors.joining(" "));
      return "a producer of "
          + bean.getBeanClass().getName()
          + (qualifiers.isEmpty() ? "" : " " + qualifiers);
    }
    return bean.getBeanClass().getName();
  }
}
