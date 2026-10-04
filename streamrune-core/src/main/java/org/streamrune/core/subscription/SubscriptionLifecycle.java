package org.streamrune.core.subscription;

/**
 * Extension of {@link EventSubscription} with pause/resume lifecycle control.
 *
 * <p>Valid state transitions:
 *
 * <pre>
 *   CREATED ──start()──▶ RUNNING ──pause()──▶ PAUSED
 *                           ▲                    │
 *                           └────resume()─────────┘
 *   RUNNING ──close()──▶ STOPPED
 *   PAUSED  ──close()──▶ STOPPED
 * </pre>
 *
 * <p>Implementations must be thread-safe. State transitions must be atomic: a concurrent call to
 * {@link #close()} while {@link #pause()} is in progress must result in either {@code PAUSED} or
 * {@code STOPPED}, never an inconsistent intermediate state.
 */
public interface SubscriptionLifecycle extends EventSubscription {

  /**
   * Pauses event delivery. The subscription retains its position; events accumulated during the
   * pause will be delivered on {@link #resume()} for offset-based implementations; push-based
   * implementations may behave differently.
   *
   * @throws IllegalStateException if the subscription is not in {@link
   *     SubscriptionLifecycleState#RUNNING} state
   */
  void pause();

  /**
   * Resumes event delivery from the position at which delivery was paused.
   *
   * @throws IllegalStateException if the subscription is not in {@link
   *     SubscriptionLifecycleState#PAUSED} state
   */
  void resume();

  /**
   * Returns the current lifecycle state.
   *
   * @return current state, never {@code null}
   */
  SubscriptionLifecycleState state();
}
