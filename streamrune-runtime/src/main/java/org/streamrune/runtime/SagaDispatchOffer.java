package org.streamrune.runtime;

import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.types.CausationId;
import org.streamrune.core.types.IdempotencyKey;

/**
 * The saga a dispatch is made for, handed from {@link SagaCommandDispatch} to {@link
 * VirtualThreadCommandBus} in a field of the dispatching thread, beside the scoped values the
 * dispatch binds.
 *
 * <p>The bus takes the offer at the start of the keyed dispatch it was made for, which empties the
 * field, so a command dispatched inside that command is not compared with it. It compares the offer
 * with the bindings it reads, and the events of the command then carry the offer's origin,
 * causation id and correlation id, and a fenced forward command is fenced against the offer's saga
 * ({@code VirtualThreadCommandBus.EventProvenance}, {@code VirtualThreadCommandBus.fenceOrigin}).
 * Saga dispatch withdraws an offer the bus did not take when the call returns (a bus of another
 * kind, or one that failed before it reached the dispatch), so no later dispatch of the thread
 * meets it.
 *
 * @param origin the saga the dispatch is made for
 * @param causationId the causation id of the dispatch's cause: the event that caused it, or {@code
 *     null}
 * @param key the idempotency key of the command
 * @param forwardFenced whether the command is a forward command fenced at its commit, so the
 *     forward marker of {@code key} is bound
 */
record SagaDispatchOffer(
    SagaOrigin origin, CausationId causationId, IdempotencyKey key, boolean forwardFenced) {

  private static final ThreadLocal<SagaDispatchOffer> OFFERED = new ThreadLocal<>();

  /** Offers this dispatch to the next keyed dispatch of the command bus on this thread. */
  void offer() {
    OFFERED.set(this);
  }

  /**
   * Takes the offer of this thread.
   *
   * @return the offer, which no later call returns; {@code null} when there is none
   */
  static SagaDispatchOffer take() {
    SagaDispatchOffer offer = OFFERED.get();
    if (offer != null) {
      OFFERED.remove();
    }
    return offer;
  }

  /** Withdraws an offer of this thread that was not taken. */
  static void withdraw() {
    OFFERED.remove();
  }

  /** Whether an offer of this thread is waiting; for tests. */
  static boolean pending() {
    return OFFERED.get() != null;
  }
}
