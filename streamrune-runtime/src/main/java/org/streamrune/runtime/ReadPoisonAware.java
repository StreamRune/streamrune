package org.streamrune.runtime;

/**
 * A live event subscription that bounds deterministic read/deserialization poison: after its
 * configured bound of consecutive poison reads at the same checkpoint it stops itself terminally
 * and exposes the terminal {@link ProjectionReadPoisonException} here.
 *
 * <p>The owning {@link ContinuousProjectionRunner} reads this after observing a non-running live
 * subscription, so it can HALT with a distinct read-poison ERROR (health DOWN) instead of treating
 * the stop as a generic "subscription died unexpectedly". Implemented by {@link
 * PollingEventSubscription} <em>and</em> the production {@code HybridEventSubscription} (which
 * delegates to its internal polling subscription), so the runner recognizes a live read-poison on
 * BOTH the default-polling path and the documented factory/Hybrid path — not only when the live
 * subscription happens to be a {@link PollingEventSubscription}. {@link StreamEventSubscription}
 * implements it too: it has no owning runner, so its callers read this after observing {@code
 * isRunning() == false}.
 */
public interface ReadPoisonAware {

  /**
   * The terminal read-poison error that stopped this subscription, or {@code null} if it is still
   * running or stopped for another reason (a clean close, an unrelated error). Only meaningful once
   * the subscription reports {@code isRunning() == false}.
   */
  ProjectionReadPoisonException readPoisonError();
}
