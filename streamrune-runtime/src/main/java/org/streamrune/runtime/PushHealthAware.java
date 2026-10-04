package org.streamrune.runtime;

/**
 * A live subscription that delivers by BOTH a low-latency push path and a guaranteed polling path,
 * and can report whether its push path is still alive.
 *
 * <p>Implemented by the production {@code HybridEventSubscription}: PostgreSQL LISTEN/NOTIFY pushes
 * for millisecond delivery, periodic polling guarantees it. Losing the push path does not lose
 * events — polling still delivers — so it is never a reason to report the subscription DOWN. But it
 * is a real, standing degradation: every projection silently falls back from millisecond to
 * poll-interval latency for the rest of the process's life.
 *
 * <p><b>Why this exists.</b> That degradation had NO signal. {@code
 * streamrune.subscriptions.listener.reconnects} cannot express it: a listener that dies before its
 * first reconnect — its LISTEN data source cannot be created — never increments it, and a listener
 * that dies after exhausting a finite cap simply stops incrementing it. In both cases the counter
 * reads exactly what a perfectly healthy listener reads. No health component covered the listener
 * either, so {@code /health} reported UP and every metric read healthy over a push path that had
 * been dead since boot.
 *
 * <p>{@link SubscriptionHealthContributor} reads this through the same capability-interface
 * mechanism it uses for {@link ReadPoisonAware}: {@code streamrune-runtime} cannot depend on {@code
 * streamrune-postgres}, so the postgres subscription implements a runtime-owned interface.
 */
public interface PushHealthAware {

  /** Liveness of the low-latency push path, independent of delivery liveness. */
  enum PushPathStatus {
    /**
     * No push path is configured (a {@code pollingOnly} subscription). Expected, never a fault:
     * delivery runs via polling by design and there is nothing to be down.
     */
    NOT_CONFIGURED,
    /**
     * The push path is live and lowering delivery latency: it is connected, or has been
     * disconnected for less than a short reconnect grace (a routine reconnect, or the first connect
     * after start).
     */
    LIVE,
    /**
     * A configured push path is not delivering — it failed at startup, reconnected until a finite
     * cap was exhausted, or has been without a connection for longer than its reconnect grace (it
     * keeps reconnecting, and reports {@link #LIVE} again once connected). Delivery continues via
     * polling, at the poll interval.
     */
    DEAD
  }

  /**
   * Current push-path liveness. Only meaningful once the subscription has been started; a
   * not-yet-started subscription is covered by its lifecycle state instead.
   */
  PushPathStatus pushPathStatus();
}
