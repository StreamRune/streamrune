package org.streamrune.runtime;

import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.subscription.SubscriptionLeadership;

/**
 * Startup checks of how saga runners are wired to the application's {@link SubscriptionLeadership}.
 * The integrations call it where they already look at every runner.
 */
public final class SagaLeaseWiring {

  private static final Logger LOG = LoggerFactory.getLogger(SagaLeaseWiring.class);

  private SagaLeaseWiring() {}

  /**
   * Logs one WARN per saga runner that carries no lease while {@code leadership} is a real one
   * (neither {@code null} nor {@link SubscriptionLeadership#NOOP}): its saga type is delivered on
   * every replica. Correct-but-noisy wiring is reported, not refused.
   *
   * @return how many runners were reported; {@code 0} for no runners or no real leadership
   */
  public static int warnUngatedUnderLeadership(
      Collection<? extends SagaRunner<?>> sagaRunners, SubscriptionLeadership leadership) {
    if (sagaRunners == null
        || sagaRunners.isEmpty()
        || leadership == null
        || leadership == SubscriptionLeadership.NOOP) {
      return 0;
    }
    int reported = 0;
    for (SagaRunner<?> runner : sagaRunners) {
      if (!runner.lease().isGated()) {
        LOG.warn(
            "Saga type {} is delivered on every replica: its SagaRunner carries no lease. Pass the"
                + " SubscriptionLeadership bean to SagaRunner.Builder.leadership(...) and build its"
                + " subscription with .sagaRunner(runner).",
            runner.sagaType().value());
        reported++;
      }
    }
    return reported;
  }
}
