package org.streamrune.runtime;

import java.util.List;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.subscription.EventListener;
import org.streamrune.core.types.SubscriptionName;

/**
 * Decorates an {@link EventListener} to track delivery success and failure via {@link
 * SubscriptionHealthContributor}: a page the listener throws on is {@link
 * SubscriptionHealthContributor#recordError recorded as an error} and the exception is rethrown; a
 * page it returns from clears the subscription's consecutive errors ({@link
 * SubscriptionHealthContributor#recordSuccess recordSuccess}).
 *
 * <p>A subscription built with {@code healthContributor(...)} applies this itself (see {@link
 * #tracking}); wrap a listener by hand only for a subscription you register with {@link
 * SubscriptionHealthContributor#register} yourself.
 */
public final class HealthTrackingEventListener implements EventListener {

  private final EventListener delegate;
  private final SubscriptionHealthContributor healthContributor;
  private final SubscriptionName subscriptionName;

  public HealthTrackingEventListener(
      EventListener delegate,
      SubscriptionHealthContributor healthContributor,
      SubscriptionName subscriptionName) {
    this.delegate = delegate;
    this.healthContributor = healthContributor;
    this.subscriptionName = subscriptionName;
  }

  @Override
  public void onEvents(List<EventEnvelope> events) {
    try {
      delegate.onEvents(events);
      healthContributor.recordSuccess(subscriptionName.value());
    } catch (Throwable t) {
      healthContributor.recordError(subscriptionName.value());
      throw t;
    }
  }

  /**
   * The listener a subscription delivers to when it is registered with {@code healthContributor}
   * under {@code subscriptionName}: {@code listener} wrapped in a {@link
   * HealthTrackingEventListener} for that contributor and name, so each failed page is recorded and
   * a delivered one clears the errors. Returns {@code listener} itself when {@code
   * healthContributor} is {@code null}, and when {@code listener} already is a {@code
   * HealthTrackingEventListener} for the same contributor and name, so an application that wrapped
   * its listener itself is not counted twice. Only the outermost listener is inspected: a tracking
   * listener wrapped inside another decorator is not found, and would count each failure twice.
   */
  public static EventListener tracking(
      EventListener listener,
      SubscriptionHealthContributor healthContributor,
      SubscriptionName subscriptionName) {
    if (healthContributor == null) {
      return listener;
    }
    if (listener instanceof HealthTrackingEventListener tracking
        && tracking.healthContributor == healthContributor
        && tracking.subscriptionName.equals(subscriptionName)) {
      return listener;
    }
    return new HealthTrackingEventListener(listener, healthContributor, subscriptionName);
  }
}
