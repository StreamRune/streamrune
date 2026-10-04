package org.streamrune.core.subscription;

import java.util.List;
import org.streamrune.core.EventEnvelope;

/**
 * Callback for receiving batches of events from a subscription.
 *
 * <p><b>Failure semantics.</b> The delivering subscription invokes {@link #onEvents(List)} with a
 * non-empty batch in offset order. When the call returns normally, the subscription advances its
 * checkpoint past the batch. When the call <em>throws</em>, the checkpoint is NOT advanced: the
 * subscription stays running, records the failure (surfaced via {@link
 * SubscriptionHealth#errorCount()} where health tracking is wired), retries after a backoff, and
 * redelivers a batch starting at the same position (possibly extended with newer events). There is
 * no skip or dead-letter handling at this level — a listener that keeps throwing blocks the
 * subscription's progress. Use a projection runner with {@link
 * org.streamrune.core.projection.ProjectionErrorStrategy} when skip/DLQ semantics are needed.
 *
 * <p><b>At-least-once delivery.</b> Redelivery also happens after process restarts (checkpoint not
 * yet saved) and around pause/resume races. Implementations must therefore be idempotent:
 * processing the same event twice must leave the same result as processing it once.
 */
@FunctionalInterface
public interface EventListener {

  /**
   * Receives a batch of events. Returning normally acknowledges the batch (the subscription's
   * checkpoint advances); throwing causes the batch to be redelivered after a backoff.
   *
   * @param events non-empty batch of events in offset order
   */
  void onEvents(List<EventEnvelope> events);
}
