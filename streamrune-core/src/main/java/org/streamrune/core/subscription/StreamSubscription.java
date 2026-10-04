package org.streamrune.core.subscription;

import org.streamrune.core.types.StreamId;

/**
 * A stream-level subscription that delivers events for a specific aggregate stream. Unlike global
 * subscriptions that receive all events, stream subscriptions filter events by streamId.
 *
 * <p>This is useful for real-time updates to specific aggregates, such as SSE endpoints for a
 * single entity.
 *
 * <p>Delivery is at-least-once to the {@link EventListener} supplied at construction; failure
 * handling follows the {@link EventListener} contract (a throwing listener causes redelivery, never
 * a silent skip).
 */
public interface StreamSubscription extends AutoCloseable {

  /** Returns the stream ID this subscription is for. */
  StreamId streamId();

  /**
   * Starts delivering events to the listener supplied at construction.
   *
   * <p>{@link #close()} is terminal: a closed subscription is not restartable, and neither is one
   * whose previous delivery thread has not finished winding down. Build a new instance instead —
   * restarting over a still-live predecessor would put two pollers on one checkpoint, delivering
   * every batch twice.
   *
   * @throws IllegalStateException if the subscription is already running, has been closed, or its
   *     previous delivery thread is still finishing
   */
  void start();

  /** Returns true if the subscription is actively delivering events. */
  boolean isRunning();

  /**
   * Stops delivery and waits for the in-flight batch to finish before returning, so the caller may
   * safely tear down the resources the listener was using. Terminal and idempotent.
   */
  @Override
  void close();
}
