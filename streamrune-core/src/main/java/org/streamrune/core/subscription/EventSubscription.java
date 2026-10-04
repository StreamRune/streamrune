package org.streamrune.core.subscription;

/**
 * A subscription to the global event stream. Implementations deliver events to the {@link
 * EventListener} supplied at construction with at-least-once semantics; failure handling follows
 * the {@link EventListener} contract (a throwing listener causes redelivery, never a silent skip).
 *
 * <p><b>Lifecycle.</b> {@link #start()} begins delivery and may be called at most once: calling it
 * again — including after {@link #close()} — throws {@link IllegalStateException}. Subscriptions
 * are not restartable; create a new instance with the same subscription name to resume from the
 * last checkpoint. {@link #close()} stops delivery, releases resources, and is idempotent. See
 * {@link SubscriptionLifecycle} for the extended pause/resume state machine.
 */
public interface EventSubscription extends AutoCloseable {

  /**
   * Begins delivering events to the listener supplied at construction.
   *
   * @throws IllegalStateException if the subscription was already started or closed
   */
  void start();

  /** Returns {@code true} if this subscription is actively delivering events. */
  boolean isRunning();

  /** Stops event delivery and releases resources. Idempotent. */
  @Override
  void close();
}
