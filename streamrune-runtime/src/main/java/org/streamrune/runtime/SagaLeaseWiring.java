package org.streamrune.runtime;

import java.util.Collection;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.Internal;
import org.streamrune.core.subscription.SubscriptionLeadership;

/**
 * Startup checks of how saga runners are wired to the application's {@link SubscriptionLeadership}.
 * The integrations call it where they already look at every runner.
 *
 * @hidden
 */
@Internal
public final class SagaLeaseWiring {

  private static final Logger LOG = LoggerFactory.getLogger(SagaLeaseWiring.class);

  private SagaLeaseWiring() {}

  /**
   * Logs one WARN per saga runner that carries no lease while {@code leadership} is a real one (not
   * {@link SubscriptionLeadership#NOOP}): its saga type is delivered on every replica.
   * Correct-but-noisy wiring is reported, not refused.
   *
   * @param sagaRunners the application's saga runners; {@code null} or empty for none
   * @param leadership the application's leadership; {@link SubscriptionLeadership#NOOP} when it has
   *     none, not {@code null}
   * @return how many runners were reported; {@code 0} for no runners or no real leadership
   * @throws NullPointerException if {@code leadership} is {@code null}
   */
  public static int warnUngatedUnderLeadership(
      Collection<? extends SagaRunner<?>> sagaRunners, SubscriptionLeadership leadership) {
    Objects.requireNonNull(leadership, "leadership is required");
    if (sagaRunners == null || sagaRunners.isEmpty() || leadership == SubscriptionLeadership.NOOP) {
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
