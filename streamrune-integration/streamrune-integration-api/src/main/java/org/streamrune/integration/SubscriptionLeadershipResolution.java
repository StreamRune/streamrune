package org.streamrune.integration;

import java.util.List;

/**
 * The refusal the three integrations share when an application declares two or more {@code
 * SubscriptionLeadership} beans and marks none of them the default.
 *
 * <p>The framework threads one leadership into every projection runner, the dead-letter retry
 * runner and the saga sweepers. With two candidates it refuses to choose: picking one by bean order
 * could give two runners of one consumer different leases, and treating the pair as absent would
 * fall back to the always-leading {@code NOOP}, so every replica would lead. Spring and Quarkus
 * throw the exception built here; Micronaut's container already refuses the non-unique bean at the
 * injection point.
 */
public final class SubscriptionLeadershipResolution {

  private SubscriptionLeadershipResolution() {}

  /**
   * The startup failure for two or more leadership beans with none marked the default.
   *
   * @param candidates one description per candidate bean, typically its class name
   * @param howToChoose how this framework marks one bean the default, e.g. {@code @Primary}
   * @return the exception to throw
   */
  public static IllegalStateException ambiguous(List<String> candidates, String howToChoose) {
    return new IllegalStateException(
        candidates.size()
            + " SubscriptionLeadership beans and none is the default ("
            + String.join(", ", candidates)
            + "): StreamRune threads one leadership into every projection runner, the dead-letter"
            + " retry runner and the saga sweepers, and does not pick one, because two"
            + " leaderships let two replicas lead the same consumer. Keep one, or mark the one to"
            + " use "
            + howToChoose
            + ".");
  }
}
