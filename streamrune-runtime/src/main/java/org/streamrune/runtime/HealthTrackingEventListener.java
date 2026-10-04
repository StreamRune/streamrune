package org.streamrune.runtime;

import java.util.List;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.subscription.EventListener;
import org.streamrune.core.types.SubscriptionName;

/**
 * Decorates an {@link EventListener} to track delivery success and failure via {@link
 * SubscriptionHealthContributor}.
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
}
