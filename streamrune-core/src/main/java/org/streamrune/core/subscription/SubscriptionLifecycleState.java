package org.streamrune.core.subscription;

/** Lifecycle states for a {@link SubscriptionLifecycle}. */
public enum SubscriptionLifecycleState {
  /** Created but {@link SubscriptionLifecycle#start()} not yet called. */
  CREATED,
  /** Actively polling / receiving events. */
  RUNNING,
  /**
   * Delivery suspended; position is retained and resumes on {@link SubscriptionLifecycle#resume()}.
   */
  PAUSED,
  /** Permanently stopped via {@link SubscriptionLifecycle#close()}. Cannot be resumed. */
  STOPPED
}
